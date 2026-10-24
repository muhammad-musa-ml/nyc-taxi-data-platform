"""Daily check that every month in the window is loaded and the newest load is recent."""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from taxi_common import dag_run_failure_alert, task_failure_alert, taxi


def check():
    # Exits 4 (and writes a `freshness` record to the alert sink) when a month is missing, the newest
    # month is behind or the newest load is older than the allowed age.
    taxi("freshness")


with DAG(
    dag_id="taxi_freshness",
    description="Freshness and completeness of the taxi warehouse",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0, "execution_timeout": timedelta(minutes=10),
                  "on_failure_callback": task_failure_alert},
    on_failure_callback=dag_run_failure_alert,
    tags=["taxi", "monitoring"],
) as dag:
    PythonOperator(task_id="check_freshness", python_callable=check)
