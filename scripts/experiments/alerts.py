"""Failure and freshness alerts, exercised through Airflow.

    python scripts/experiments/alerts.py --source data/mirror --repeats 5

Part A, file-level faults. A real-sized month with a column renamed, a column dropped, a column of the wrong
type, or no rows at all is offered to the monthly DAG as January..April. Each must fail at the validate task
without a retry, load nothing, and write a task-failure record and a DAG-failure record to the alert sink.
The script reports how long after the failing task ended the record appeared, and how long after the task
started the failure was detected.

Part B, freshness. With the six months loaded, the freshness DAG runs three ways: as configured (no
findings), expecting a month that was never loaded, and with a load-age limit shorter than the age of the newest load.

The alert sink is a file, so "alert delay" is the gap between two timestamps in the same record.
"""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

from common import (ROOT, RESULTS, connect, in_scheduler, log, measurement_lock, pipeline_cli, record_environment,
                    utc_now, write_json)

from taxi_platform import inject
from taxi_platform.config import month_start

SINK = ROOT / "alert-sink" / "alerts.jsonl"
FAULT_MONTHS = {"RENAMED_REQUIRED_COLUMN": "2025-01", "DROPPED_REQUIRED_COLUMN": "2025-02",
                "WRONG_COLUMN_TYPE": "2025-03", "EMPTY_FILE": "2025-04"}
AIRFLOW_DSN = "postgresql://taxi:taxi@localhost:18232/airflow"


def sink_lines() -> list[dict]:
    if not SINK.exists():
        return []
    return [json.loads(line) for line in SINK.read_text(encoding="utf-8").splitlines() if line.strip()]


def when(text: str) -> datetime:
    return datetime.fromisoformat(text)


def task_times(dag_id: str, run_prefix: str) -> dict:
    with connect(AIRFLOW_DSN) as conn:
        rows = conn.execute(
            "SELECT run_id, task_id, state, try_number, start_date, end_date FROM task_instance"
            " WHERE dag_id = %s AND run_id LIKE %s", (dag_id, run_prefix + "%")).fetchall()
    return {(run_id, task): {"state": state, "tries": tries, "start": start, "end": end}
            for run_id, task, state, tries, start, end in rows}


def build_fault_files(source: Path, month: str) -> Path:
    lab = ROOT / "data" / "lab" / "faults"
    scratch = ROOT / "data" / "lab" / "faults-build"
    shutil.rmtree(lab, ignore_errors=True)
    inject.write_file_faults(source / "trip-data" / f"yellow_tripdata_{month}.parquet",
                             source / "misc" / "taxi_zone_lookup.csv", scratch, month_start(month))
    (lab / "trip-data").mkdir(parents=True)
    (lab / "misc").mkdir(parents=True)
    shutil.copyfile(source / "misc" / "taxi_zone_lookup.csv", lab / "misc" / "taxi_zone_lookup.csv")
    for fault, target_month in FAULT_MONTHS.items():
        shutil.copyfile(scratch / fault / "trip-data" / f"yellow_tripdata_{month}.parquet",
                        lab / "trip-data" / f"yellow_tripdata_{target_month}.parquet")
    shutil.rmtree(scratch, ignore_errors=True)
    return lab


def contract_faults(args) -> dict:
    build_fault_files(Path(args.source), args.month)
    db = "warehouse_alert_lab"
    with connect("postgresql://taxi:taxi@localhost:18232/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{db}"')
    env = {"TAXI_SOURCE_BASE": "/data/lab/faults", "TAXI_PG_DSN": f"postgresql://taxi:taxi@postgres:5432/{db}",
           "TAXI_LANDING_BUCKET": "lab-landing-faults", "TAXI_VALIDATED_BUCKET": "lab-validated-faults"}
    pipeline_cli("init-db", env=env)
    pipeline_cli("init-storage", env=env)
    repeats = []
    for i in range(1, args.repeats + 1):
        before = len(sink_lines())
        result = in_scheduler("airflow", "dags", "backfill", "taxi_monthly", "-s", "2025-01-01", "-e", "2025-04-01",
                              "--reset-dagruns", "-y", env=env, check=False)
        new = sink_lines()[before:]
        times = task_times("taxi_monthly", "backfill__")
        per_fault = {}
        for fault, month in FAULT_MONTHS.items():
            run_id = f"backfill__{month}-01T00:00:00+00:00"
            validate = times[(run_id, "validate_with_great_expectations")]
            failure = [r for r in new if r["kind"] == "task_failure" and r.get("source_month") == month
                       and r["task_id"] == "validate_with_great_expectations"]
            dag_failure = [r for r in new if r["kind"] == "dag_run_failed" and month in r["run_id"]]
            downstream = {t: s["state"] for (rid, t), s in times.items() if rid == run_id and t not in
                          ("load_zones", "fetch_to_landing", "validate_with_great_expectations")}
            entry = {"validate_state": validate["state"], "validate_tries": validate["tries"],
                     "downstream_states": downstream, "task_failure_records": len(failure),
                     "dag_failure_records": len(dag_failure)}
            if failure:
                rec = failure[0]
                entry["alert_delay_after_task_ended_s"] = (when(rec["emitted_at"]) - when(rec["task_ended_at"])).total_seconds()
                entry["detected_after_task_started_s"] = (when(rec["emitted_at"]) - validate["start"]).total_seconds()
                entry["error"] = rec["error"]
            per_fault[fault] = entry
        with connect(f"postgresql://taxi:taxi@localhost:18232/{db}") as conn:
            loaded = {"ledger_rows": conn.execute("SELECT count(*) FROM ops.ingestion_ledger").fetchone()[0],
                      "raw_rows": conn.execute("SELECT count(*) FROM raw.yellow_trips").fetchone()[0]}
        repeats.append({"repeat": i, "backfill_exit_code": result.returncode, "faults": per_fault,
                        "loaded_into_warehouse": loaded, "alert_lines_written": len(new)})
        log(f"contract faults repeat {i}: exit {result.returncode}, {len(new)} alert lines, loaded {loaded}")
    with connect("postgresql://taxi:taxi@localhost:18232/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
    shutil.rmtree(ROOT / "data" / "lab" / "faults", ignore_errors=True)
    return {"month_used_as_the_source": args.month, "fault_to_dag_month": FAULT_MONTHS, "repeats": repeats}


def freshness_dag(env: dict, label: str, repeats: int) -> dict:
    runs = []
    for i in range(1, repeats + 1):
        before = len(sink_lines())
        result = in_scheduler("airflow", "dags", "backfill", "taxi_freshness", "-s", "2026-10-20", "-e", "2026-10-20",
                              "--reset-dagruns", "-y", env=env, check=False)
        new = sink_lines()[before:]
        times = task_times("taxi_freshness", "backfill__")
        t = times[("backfill__2026-10-20T00:00:00+00:00", "check_freshness")]
        findings = [r for r in new if r["kind"] == "freshness"]
        failure = [r for r in new if r["kind"] == "task_failure"]
        entry = {"repeat": i, "backfill_exit_code": result.returncode, "task_state": t["state"],
                 "findings": sorted(r["code"] for r in findings), "finding_messages": [r["message"] for r in findings],
                 "task_failure_records": len(failure)}
        if findings:
            entry["first_finding_after_task_started_s"] = (when(findings[0]["emitted_at"]) - t["start"]).total_seconds()
        if failure:
            entry["failure_alert_delay_after_task_ended_s"] = (
                when(failure[0]["emitted_at"]) - when(failure[0]["task_ended_at"])).total_seconds()
        runs.append(entry)
        log(f"freshness [{label}] repeat {i}: state {t['state']}, findings {entry['findings']}")
    return {"label": label, "environment": env, "runs": runs}


def freshness(args) -> dict:
    ok = pipeline_cli("freshness", check=False)
    scenarios = [
        freshness_dag({}, "as configured (all six months loaded and recent)", args.repeats),
        freshness_dag({"TAXI_EXPECTED_LATEST_MONTH": "2025-07"}, "a seventh month is expected but was never loaded",
                      args.repeats),
        freshness_dag({"TAXI_MAX_LOAD_AGE_HOURS": "0.05"}, "the newest load is older than the allowed 0.05 h (3 min)",
                      args.repeats),
    ]
    return {"cli_freshness_as_configured_exit_code": ok.returncode, "scenarios": scenarios}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/mirror")
    parser.add_argument("--month", default="2025-03")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--part", choices=["contract", "freshness", "both"], default="both")
    parser.add_argument("--out", default=str(RESULTS / "alerts"))
    args = parser.parse_args()
    out = {"started_at": utc_now()}
    with measurement_lock("P2 alerts experiment"):  # the delays are timings, so nothing else may be running
        record_environment(Path(args.out), "alerts experiment, before the timed section")
        if args.part in ("contract", "both"):
            out["contract_faults"] = contract_faults(args)
            write_json(Path(args.out) / "contract-faults.json", out["contract_faults"])
        if args.part in ("freshness", "both"):
            out["freshness"] = freshness(args)
            write_json(Path(args.out) / "freshness.json", out["freshness"])
        record_environment(Path(args.out), "after the alerts experiment", prefix="end-")
    log("done")


if __name__ == "__main__":
    main()
