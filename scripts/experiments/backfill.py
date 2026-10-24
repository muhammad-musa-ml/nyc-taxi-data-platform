"""One timed backfill of all six months from an empty stack, optionally followed by a replay of the range.

    python scripts/experiments/backfill.py --run 1 [--replay]

A run is: stop the stack and delete its volumes, start it again, record the conditions, run
``airflow dags backfill`` for 2025-01 .. 2025-06 (real downloads from the TLC site into an empty landing
bucket), then read back what happened: wall time, the DAG-run and task durations from Airflow's own
metadata, the stage timings the pipeline wrote, row counts per month and reason code, and a snapshot of the
warehouse. With --replay the same range is run again with ``--reset-dagruns`` and compared with the snapshot
taken before it; every row count and content hash must be unchanged.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (ROOT, RESULTS, compare, compose, connect, in_scheduler, log, measurement_lock,
                    record_environment, snapshot, utc_now, wait_healthy, write_json)

FIRST, LAST = "2025-01-01", "2025-06-01"


def airflow_durations(run_id_prefix: str = "backfill__") -> dict:
    dsn = "postgresql://taxi:taxi@localhost:18232/airflow"
    with connect(dsn) as conn:
        runs = conn.execute(
            "SELECT run_id, state, start_date, end_date, extract(epoch FROM end_date - start_date)"
            " FROM dag_run WHERE dag_id = 'taxi_monthly' AND run_id LIKE %s ORDER BY execution_date",
            (run_id_prefix + "%",)).fetchall()
        tasks = conn.execute(
            "SELECT run_id, task_id, state, try_number, extract(epoch FROM end_date - start_date)"
            " FROM task_instance WHERE dag_id = 'taxi_monthly' AND run_id LIKE %s ORDER BY run_id, start_date",
            (run_id_prefix + "%",)).fetchall()
    out: dict = {}
    for run_id, state, start, end, seconds in runs:
        month = run_id.split("__")[1][:7]
        out[month] = {"run_id": run_id, "state": state, "started": start.isoformat(), "ended": end.isoformat(),
                      "dag_run_seconds": float(seconds), "tasks": {}}
    for run_id, task_id, state, tries, seconds in tasks:
        out[run_id.split("__")[1][:7]]["tasks"][task_id] = {"state": state, "tries": tries,
                                                            "seconds": float(seconds) if seconds is not None else None}
    return out


def stage_timings() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT run_id, source_month, stage, seconds, rows, detail FROM ops.stage_timings"
            " WHERE run_id LIKE 'backfill__%' ORDER BY id").fetchall()
    return [{"run_id": r, "month": m.isoformat() if m else None, "stage": s, "seconds": float(sec), "rows": n,
             "detail": d} for r, m, s, sec, n, d in rows]


def validation_reports() -> dict:
    """Rows in / loaded / quarantined per month and per reason code, from the ledger and the checks table."""
    with connect() as conn:
        snap = snapshot(conn)
    return {"ledger": snap["ledger"], "checks": snap["checks"]}


def fresh_stack() -> None:
    log("stopping the stack and deleting its volumes")
    compose("down", "-v", "--remove-orphans")
    compose("up", "-d")
    wait_healthy()
    log("stack is healthy")


def backfill(extra: list[str], log_path: Path) -> tuple[float, int]:
    started = time.perf_counter()
    result = in_scheduler("airflow", "dags", "backfill", "taxi_monthly", "-s", FIRST, "-e", LAST, "-y", *extra,
                          check=False)
    seconds = time.perf_counter() - started
    log_path.write_text(result.stdout + "\n--- stderr ---\n" + result.stderr, encoding="utf-8")
    return seconds, result.returncode


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=int, required=True)
    parser.add_argument("--replay", action="store_true", help="after the backfill, run the same range again")
    parser.add_argument("--out", default=str(RESULTS / "backfill"))
    args = parser.parse_args()
    out = Path(args.out) / f"run-{args.run}"
    out.mkdir(parents=True, exist_ok=True)

    with measurement_lock(f"P2 backfill run {args.run}"):
        fresh_stack()
        record_environment(out, f"backfill run {args.run}, before the timed section")
        alert_path = ROOT / "alert-sink" / "alerts.jsonl"
        alerts_before = len(alert_path.read_text().splitlines()) if alert_path.exists() else 0
        started_at = utc_now()
        seconds, code = backfill([], out / "backfill.log")
        ended_at = utc_now()
        log(f"backfill finished in {seconds:.1f} s, exit {code}")
        record_environment(out, "after the backfill", prefix="end-")
        result = {
            "run": args.run, "started_at": started_at, "ended_at": ended_at, "wall_seconds": seconds,
            "exit_code": code, "months": FIRST + ".." + LAST,
            "dag_runs": airflow_durations(), "stage_timings": stage_timings(),
        }
        with connect() as conn:
            before = snapshot(conn)
        result["state"] = before
        alerts_after =len(alert_path.read_text().splitlines()) if alert_path.exists() else 0
        result["alert_lines_written"] = alerts_after - alerts_before
        write_json(out / "result.json", result)

        if args.replay and code == 0:
            replay_seconds, replay_code = backfill(["--reset-dagruns"], out / "replay.log")
            log(f"replay finished in {replay_seconds:.1f} s, exit {replay_code}")
            with connect() as conn:
                after = snapshot(conn)
            replay = {"wall_seconds": replay_seconds, "exit_code": replay_code,
                      "comparison_with_state_before_replay": compare(before, after),
                      "load_count_per_month": {m: v["load_count"] for m, v in after["ledger"].items()},
                      "dag_runs": airflow_durations(), "state_after": after}
            write_json(out / "replay.json", replay)
            record_environment(out, "after the replay", prefix="replay-end-")
    log(f"wrote {out}")


if __name__ == "__main__":
    main()
