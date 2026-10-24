"""Shared helpers for the DAGs: running pipeline commands as subprocesses and alerting on failure."""
from __future__ import annotations

import os
import subprocess

from airflow.exceptions import AirflowFailException

from taxi_platform.alerts import emit

SINK = os.environ.get("TAXI_ALERT_SINK", "/opt/airflow/alert-sink/alerts.jsonl")
PIPELINE_BIN = "/opt/pipeline-venv/bin"
DBT_DIR = "/opt/airflow/dbt"

# Exit codes of `taxi-platform` that retrying cannot fix: a file that breaks the contract, or one the
# source has not published.
NO_RETRY_EXIT_CODES = {2, 3}


def run(*command: str, cwd: str | None = None) -> None:
    """Run a command, stream its output into the task log, and fail the task if it fails."""
    print("+", " ".join(command), flush=True)
    # Airflow replaces sys.stdout with a logger that has no file descriptor, so read the pipe and print.
    proc = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        print(line.rstrip("\n"), flush=True)
    code = proc.wait()
    if code in NO_RETRY_EXIT_CODES:
        raise AirflowFailException(f"{command[0]} exited {code}; retrying cannot fix this")
    if code != 0:
        raise RuntimeError(f"{command[0]} exited {code}")


def taxi(*args: str) -> None:
    run(f"{PIPELINE_BIN}/taxi-platform", *args)


def dbt(*args: str) -> None:
    run(f"{PIPELINE_BIN}/dbt", "--no-use-colors", *args, "--project-dir", DBT_DIR, cwd=DBT_DIR)


def task_failure_alert(context) -> None:
    """Task-level failure callback: runs once the task has failed for good (after its retries)."""
    ti = context["task_instance"]
    interval = context.get("data_interval_start")
    emit(
        SINK, "task_failure", "critical",
        f"{ti.dag_id}.{ti.task_id} failed",
        dag_id=ti.dag_id, task_id=ti.task_id, run_id=context["run_id"],
        source_month=interval.strftime("%Y-%m") if interval else None,
        try_number=ti.try_number, task_ended_at=str(ti.end_date),
        error=str(context.get("exception"))[:500],
    )


def dag_run_failure_alert(context) -> None:
    dag_run = context["dag_run"]
    emit(SINK, "dag_run_failed", "critical", f"{dag_run.dag_id} run {dag_run.run_id} failed",
         dag_id=dag_run.dag_id, run_id=dag_run.run_id)
