"""One run per month of TLC yellow-taxi data: land, validate, load, model, test, reconcile."""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from taxi_common import dag_run_failure_alert, dbt, task_failure_alert, taxi

FIRST_MONTH = os.environ.get("TAXI_FIRST_MONTH", "2025-01")
LAST_MONTH = os.environ.get("TAXI_LAST_MONTH", "2025-06")

# A run's logical date is the first day of the month it processes; Airflow starts it when that month
# has ended. The last scheduled run is the one for LAST_MONTH.
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
MONTH_FIRST_DAY = "{{ data_interval_start.strftime('%Y-%m-01') }}"
RUN_ID = "{{ run_id }}"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": task_failure_alert,
}


def _task(task_id: str, fn, *args):
    return PythonOperator(task_id=task_id, python_callable=fn, op_args=list(args))


def load_zones(pipeline_run_id):
    taxi("load-zones", "--run-id", pipeline_run_id)


def fetch(month, pipeline_run_id):
    taxi("fetch", "--month", month, "--run-id", pipeline_run_id)


def validate(month, pipeline_run_id):
    taxi("validate", "--month", month, "--run-id", pipeline_run_id)


def load(month, pipeline_run_id):
    taxi("load", "--month", month, "--run-id", pipeline_run_id)


def dbt_run(month_first_day):
    dbt("seed")
    dbt("run", "--vars", f'{{source_month: "{month_first_day}"}}')


def dbt_test(month_first_day):
    dbt("test", "--vars", f'{{source_month: "{month_first_day}"}}')


def reconcile(month, pipeline_run_id):
    taxi("reconcile", "--month", month, "--run-id", pipeline_run_id)


with DAG(
    dag_id="taxi_monthly",
    description="Monthly incremental load of NYC TLC yellow-taxi trips",
    schedule="@monthly",
    start_date=pendulum.from_format(FIRST_MONTH, "YYYY-MM", tz="UTC"),
    end_date=pendulum.from_format(LAST_MONTH, "YYYY-MM", tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=dag_run_failure_alert,
    tags=["taxi", "batch"],
) as dag:
    t_zones = _task("load_zones", load_zones, RUN_ID)
    t_fetch = _task("fetch_to_landing", fetch, MONTH, RUN_ID)
    t_validate = _task("validate_with_great_expectations", validate, MONTH, RUN_ID)
    t_load = _task("load_warehouse", load, MONTH, RUN_ID)
    t_dbt_run = _task("dbt_run", dbt_run, MONTH_FIRST_DAY)
    t_dbt_test = _task("dbt_test", dbt_test, MONTH_FIRST_DAY)
    t_reconcile = _task("reconcile_and_check_freshness", reconcile, MONTH, RUN_ID)

    t_zones >> t_fetch >> t_validate >> t_load >> t_dbt_run >> t_dbt_test >> t_reconcile
