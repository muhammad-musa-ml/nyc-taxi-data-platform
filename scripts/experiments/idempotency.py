"""Re-run every loaded month through the pipeline and show that nothing changes.

    python scripts/experiments/idempotency.py --reruns 5

Needs the six months loaded (run a backfill first). For each month: snapshot the raw and quarantine layers
and the ledger, run ``taxi-platform run-month`` again (zones, fetch from the landing bucket, validate, load),
snapshot again and compare. Row counts and a content hash of every row must be identical and no key may
appear twice. The re-run is the same command Airflow's tasks call; it is repeated ``--reruns`` times per month.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import RESULTS, compare, connect, log, pipeline_cli, snapshot, utc_now, write_json

MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reruns", type=int, default=5)
    parser.add_argument("--out", default=str(RESULTS / "idempotency"))
    args = parser.parse_args()
    out = Path(args.out)
    started = utc_now()
    runs, any_change, total = [], False, 0
    for month in MONTHS:
        first_day = month + "-01"
        with connect() as conn:
            baseline = snapshot(conn, [first_day])
        previous = baseline
        for k in range(1, args.reruns + 1):
            t0 = time.perf_counter()
            result = pipeline_cli("run-month", "--month", month, "--run-id", f"idem-{month}-{k}", check=False,
                                  env={"TAXI_VALIDATE_WORKERS": "2"})  # fewer workers: this run is not timed
            seconds = time.perf_counter() - t0
            with connect() as conn:
                now = snapshot(conn, [first_day])
            versus_baseline = compare(baseline, now)
            entry = {
                "month": month, "rerun": k, "exit_code": result.returncode, "seconds_not_a_headline": round(seconds, 1),
                "error_output_tail": (result.stderr + result.stdout)[-2500:] if result.returncode else "",
                "identical_to_state_before_the_first_rerun": versus_baseline["identical"],
                "deltas": versus_baseline["deltas"],
                "duplicate_keys": now["duplicates"],
                "load_count": now["ledger"][first_day]["load_count"],
                "rows": {t: m[first_day]["rows"] for t, m in now["tables"].items() if first_day in m},
            }
            runs.append(entry)
            total += 1
            any_change |= not versus_baseline["identical"] or result.returncode != 0
            log(f"{month} rerun {k}: exit {result.returncode}, identical={versus_baseline['identical']}, "
                f"load_count={entry['load_count']}")
            previous = now
        del previous
    summary = {
        "started_at": started, "ended_at": utc_now(), "months": MONTHS, "reruns_per_month": args.reruns,
        "total_reruns": total, "reruns_with_any_difference_or_failure": sum(
            1 for r in runs if not r["identical_to_state_before_the_first_rerun"] or r["exit_code"] != 0),
        "reruns_with_duplicate_keys": sum(1 for r in runs if any(r["duplicate_keys"].values())),
        "note": "Timings in this file were taken without the machine lock and are not headline results.",
        "runs": runs,
    }
    write_json(out / "result.json", summary)
    log(f"{total} re-runs, {summary['reruns_with_any_difference_or_failure']} with a difference or failure")


if __name__ == "__main__":
    main()
