"""Inject known faults into a real month, run the pipeline on it, and score what was caught.

    python scripts/experiments/injection.py --source data/mirror --month 2025-03 --per-fault 200 --seeds 1 2 3 4 5

For each seed a copy of the month is written with ``per-fault`` rows changed for each of the faults in
``taxi_platform.inject.FAULTS`` (chosen only among rows that passed every rule in the original), then the
whole pipeline (``taxi-platform run-month``) runs on it in the scheduler container against its own scratch
database and buckets. The quarantine table is read back and scored against the injector's manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

from common import ROOT, RESULTS, connect, log, pipeline_cli, run, utc_now, write_json

from taxi_platform import inject


def scratch_db(name: str, create: bool) -> None:
    with connect("postgresql://taxi:taxi@localhost:18232/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        if create:
            conn.execute(f'CREATE DATABASE "{name}"')


def one_seed(seed: int, args) -> dict:
    lab = ROOT / "data" / "lab" / f"inject-s{seed}"
    db = f"warehouse_inject_s{seed}"
    env = {"TAXI_SOURCE_BASE": f"/data/lab/inject-s{seed}",
           "TAXI_PG_DSN": f"postgresql://taxi:taxi@postgres:5432/{db}",
           "TAXI_LANDING_BUCKET": f"lab-landing-s{seed}", "TAXI_VALIDATED_BUCKET": f"lab-validated-s{seed}",
           "TAXI_VALIDATE_WORKERS": "4"}
    log(f"seed {seed}: writing the injected month")
    run([sys.executable, "-m", "taxi_platform.inject", "make", "--source", args.source, "--month", args.month,
                "--out", str(lab), "--per-fault", str(args.per_fault), "--seed", str(seed),
                "--natural-cache", str(ROOT / "data" / "lab" / f"natural-{Path(args.source).name}-{args.month}.npy")])
    manifest_path = lab / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    scratch_db(db, create=True)
    pipeline_cli("init-db", env=env)
    pipeline_cli("init-storage", env=env)
    t0 = time.perf_counter()
    log(f"seed {seed}: running the pipeline on {manifest['rows_after']} rows")
    result = pipeline_cli("run-month", "--month", args.month, "--run-id", f"inject-s{seed}", env=env, check=False)
    seconds = time.perf_counter() - t0
    if result.returncode != 0:
        return {"seed": seed, "error": result.stderr[-2000:] + result.stdout[-2000:]}

    month_first = args.month + "-01"
    with connect(f"postgresql://taxi:taxi@localhost:18232/{db}") as conn:
        quarantined = {int(r): list(codes) for r, codes in conn.execute(
            "SELECT source_row, reason_codes FROM quarantine.yellow_trips WHERE source_month = %s", (month_first,))}
        ledger = conn.execute(
            "SELECT rows_in, rows_loaded, rows_quarantined FROM ops.ingestion_ledger WHERE source_month = %s",
            (month_first,)).fetchone()
        loaded_rows = {int(r[0]) for r in conn.execute(
            "SELECT source_row FROM raw.yellow_trips WHERE source_month = %s AND source_row = ANY(%s)",
            (month_first, [e["row"] for e in manifest["entries"]]))}
    scored = inject.evaluate(manifest, quarantined)
    injected_rows = {e["row"] for e in manifest["entries"]}
    out = {
        "seed": seed, "month": args.month, "per_fault": args.per_fault, "finished_at": utc_now(),
        "source_file_sha256_of_injected_file": hashlib.sha256(
            (lab / "trip-data" / f"yellow_tripdata_{args.month}.parquet").read_bytes()).hexdigest(),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "manifest_entries": len(manifest["entries"]),
        "rows_before_injection": manifest["rows_before"], "rows_after_injection": manifest["rows_after"],
        "ledger": {"rows_in": ledger[0], "rows_loaded": ledger[1], "rows_quarantined": ledger[2]},
        "natural_quarantined_rows_in_original": manifest["natural_quarantined_rows"],
        "injected_rows_found_in_raw_table": len(loaded_rows & injected_rows),
        "pipeline_seconds_not_a_headline": round(seconds, 1),
        "score": scored,
    }
    # Every row is either loaded or quarantined: loaded injected rows must equal the score's `loaded` total.
    scored_loaded = scored["detectable_total"]["loaded"] + scored["undetectable_total"]["loaded"]
    out["loaded_count_agrees_with_score"] = out["injected_rows_found_in_raw_table"] == scored_loaded
    write_json(Path(args.out) / f"seed-{seed}.json", out)
    scratch_db(db, create=False)
    shutil.rmtree(lab, ignore_errors=True)
    log(f"seed {seed}: detectable {scored['detectable_total']}, undetectable {scored['undetectable_total']}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/mirror")
    parser.add_argument("--month", default="2025-03")
    parser.add_argument("--per-fault", type=int, default=200)
    parser.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3, 4, 5])
    parser.add_argument("--out", default=str(RESULTS / "injection"))
    args = parser.parse_args()
    for seed in args.seeds:
        one_seed(seed, args)


if __name__ == "__main__":
    main()
