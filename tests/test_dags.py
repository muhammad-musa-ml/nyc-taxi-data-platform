"""The DAG files import cleanly under Airflow and have the shape the README describes.

Needs Apache Airflow in the interpreter that runs the test (CI installs it with the 2.10.5 constraints file;
the project's own virtual environment does not carry Airflow, so these tests skip there).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def bag():
    sys.path.insert(0, str(DAGS))  # the scheduler puts the dags folder on the path; tests do it by hand
    try:
        yield DagBag(dag_folder=str(DAGS), include_examples=False)
    finally:
        sys.path.remove(str(DAGS))


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_both_dags_are_present(bag):
    assert sorted(bag.dag_ids) == ["taxi_freshness", "taxi_monthly"]


def test_monthly_dag_runs_the_seven_stages_in_order(bag):
    dag = bag.get_dag("taxi_monthly")
    order = ["load_zones", "fetch_to_landing", "validate_with_great_expectations", "load_warehouse",
             "dbt_run", "dbt_test", "reconcile_and_check_freshness"]
    assert sorted(dag.task_ids) == sorted(order)
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
        assert dag.get_task(upstream).downstream_task_ids == {downstream}


def test_monthly_dag_is_scheduled_monthly_with_catchup_and_one_run_at_a_time(bag):
    dag = bag.get_dag("taxi_monthly")
    assert dag.schedule_interval == "@monthly"
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.start_date.strftime("%Y-%m") == "2025-01"
    assert dag.end_date.strftime("%Y-%m") == "2025-06"


def test_every_task_alerts_on_failure_and_retries_once(bag):
    dag = bag.get_dag("taxi_monthly")
    assert dag.on_failure_callback is not None
    for task in dag.tasks:
        assert task.on_failure_callback, task.task_id
        assert task.retries == 1, task.task_id


def test_freshness_dag_is_daily_and_does_not_retry(bag):
    dag = bag.get_dag("taxi_freshness")
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert [t.retries for t in dag.tasks] == [0]
