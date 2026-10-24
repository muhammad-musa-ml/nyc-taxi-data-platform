"""Command-line entry point: ``taxi-platform <stage> --month YYYY-MM``. Airflow tasks call exactly this."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from . import alerts, db, pipeline, storage
from .config import Settings, month_label, month_start
from .freshness import check_freshness

EXIT_FAILED, EXIT_CONTRACT, EXIT_NOT_PUBLISHED, EXIT_FRESHNESS = 1, 2, 3, 4


def _run_id(args) -> str:
    return args.run_id or "manual-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="taxi-platform")
    sub = parser.add_subparsers(dest="command", required=True)

    def month_cmd(name: str, help_: str):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--month", required=True, help="YYYY-MM")
        p.add_argument("--run-id", default=None)
        return p

    sub.add_parser("init-db", help="create schemas and tables")
    p = sub.add_parser("init-storage", help="create the landing and validated buckets")
    p.add_argument("--wait", type=int, default=0, help="seconds to wait for the object store")
    p = sub.add_parser("load-zones", help="land and load the taxi zone lookup")
    p.add_argument("--run-id", default=None)
    p = month_cmd("fetch", "land the month's raw file")
    p.add_argument("--force", action="store_true", help="download again even if already landed")
    month_cmd("validate", "validate the landed file")
    month_cmd("load", "replace the month in the warehouse")
    month_cmd("reconcile", "check the month's counts and freshness")
    month_cmd("run-month", "zones, fetch, validate and load in one process")
    p = sub.add_parser("freshness", help="check that every month in the window is loaded and recent")
    p.add_argument("--through", default=None, help="last month that must be loaded (default: expected latest)")
    sub.add_parser("status", help="print the ingestion ledger")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings()
    try:
        return _dispatch(args, settings)
    except pipeline.ContractViolation as err:
        print(f"CONTRACT VIOLATION: {err}", file=sys.stderr)
        return EXIT_CONTRACT
    except pipeline.NotPublished as err:
        print(f"NOT PUBLISHED: {err}", file=sys.stderr)
        return EXIT_NOT_PUBLISHED


def _dispatch(args, settings: Settings) -> int:
    cmd = args.command
    if cmd == "init-db":
        db.init_db(settings)
        print("schema ready")
    elif cmd == "init-storage":
        storage.ensure_buckets(settings, wait_seconds=args.wait)
        print(f"buckets ready: {settings.landing_bucket}, {settings.validated_bucket}")
    elif cmd == "load-zones":
        _print(pipeline.load_zones(settings, _run_id(args)))
    elif cmd == "freshness":
        with db.connect(settings) as conn:
            findings = check_freshness(conn, settings, through_month=args.through)
        for f in findings:
            alerts.emit(settings.alert_sink, "freshness", "warning", f.message, code=f.code)
        _print({"findings": [f.__dict__ for f in findings]})
        return EXIT_FRESHNESS if findings else 0
    elif cmd == "status":
        with db.connect(settings) as conn:
            for row in conn.execute(
                    "SELECT source_month, rows_in, rows_loaded, rows_quarantined, loaded_at, load_count"
                    " FROM ops.ingestion_ledger ORDER BY 1"):
                print(*row, sep="\t")
    else:
        month = month_start(args.month)
        run_id = _run_id(args)
        if cmd == "fetch":
            _print(pipeline.fetch(settings, month, run_id, force=args.force))
        elif cmd == "validate":
            _print(pipeline.validate_month(settings, month, run_id))
        elif cmd == "load":
            _print(pipeline.load_month(settings, month, run_id))
        elif cmd == "reconcile":
            _print(pipeline.reconcile(settings, month, run_id))
        elif cmd == "run-month":
            out = pipeline.run_month(settings, month, run_id)
            print(f"{month_label(month)}: in={out['load']['rows_in']} loaded={out['load']['rows_loaded']} "
                  f"quarantined={out['load']['rows_quarantined']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
