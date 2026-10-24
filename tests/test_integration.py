"""The pipeline against a real PostgreSQL and a real S3-compatible store, on the small fixture month.

Skipped unless TAXI_TEST_PG_DSN and TAXI_TEST_S3_ENDPOINT are set (CI sets them; so does ``make test-integration``
in the README). The tests drop and recreate the raw, quarantine and ops schemas, so they refuse to run against a
database whose name does not contain "test".
"""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pytest

from taxi_platform import alerts, cli, db, inject, pipeline, storage
from taxi_platform.config import Settings
from taxi_platform.freshness import check_freshness

FIXTURE = Path(__file__).parent / "fixtures" / "source"
FIXTURE_INFO = json.loads((FIXTURE / "FIXTURE.json").read_text(encoding="utf-8"))
MONTH = date(2025, 3, 1)
RUN = "itest"

pytestmark = pytest.mark.skipif(
    not (os.environ.get("TAXI_TEST_PG_DSN") and os.environ.get("TAXI_TEST_S3_ENDPOINT")),
    reason="needs TAXI_TEST_PG_DSN and TAXI_TEST_S3_ENDPOINT (a throw-away PostgreSQL database and an S3 endpoint)")


def make_settings(tmp_path: Path, source: Path = FIXTURE) -> Settings:
    dsn = os.environ["TAXI_TEST_PG_DSN"]
    if "test" not in dsn.rsplit("/", 1)[-1]:
        pytest.fail("refusing to drop schemas in a database whose name does not contain 'test'")
    return Settings(
        pg_dsn=dsn, s3_endpoint=os.environ["TAXI_TEST_S3_ENDPOINT"],
        s3_access_key=os.environ.get("TAXI_TEST_S3_ACCESS_KEY", "taxi-local-key"),
        s3_secret_key=os.environ.get("TAXI_TEST_S3_SECRET_KEY", "taxi-local-secret"),
        landing_bucket="itest-landing", validated_bucket="itest-validated", source_base=str(source),
        alert_sink=str(tmp_path / "alerts.jsonl"), validate_workers=2,
        first_month="2025-01", expected_latest_month="2025-03")


def reset(settings: Settings) -> None:
    with db.connect(settings, autocommit=True) as conn:
        for schema in ("raw", "quarantine", "ops"):
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    db.init_db(settings)
    storage.ensure_buckets(settings, wait_seconds=60)
    s3 = storage.client(settings)
    for bucket in (settings.landing_bucket, settings.validated_bucket):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])


@pytest.fixture()
def settings(tmp_path):
    s = make_settings(tmp_path)
    reset(s)
    return s


def one(settings: Settings, sql: str, *params):
    with db.connect(settings) as conn:
        return conn.execute(sql, params).fetchone()


def state(settings: Settings) -> dict:
    """Everything a re-run must leave unchanged."""
    with db.connect(settings) as conn:
        return {
            "raw_rows": conn.execute("SELECT count(*) FROM raw.yellow_trips").fetchone()[0],
            "quarantine_rows": conn.execute("SELECT count(*) FROM quarantine.yellow_trips").fetchone()[0],
            "raw_checksum": conn.execute(
                "SELECT md5(string_agg(t::text, '|' ORDER BY source_row)) FROM raw.yellow_trips t").fetchone()[0],
            "quarantine_checksum": conn.execute(
                "SELECT md5(string_agg(t::text, '|' ORDER BY source_row)) FROM quarantine.yellow_trips t").fetchone()[0],
            "checks": conn.execute(
                "SELECT json_agg(c ORDER BY reason_code) FROM ops.ingestion_checks c").fetchone()[0],
        }


def test_fixture_month_loads_the_counts_the_fixture_records(settings):
    result = pipeline.run_month(settings, MONTH, RUN)["load"]
    assert result["rows_in"] == FIXTURE_INFO["fixture_rows"]
    assert result["rows_quarantined"] == FIXTURE_INFO["fixture_rows_quarantined_by_the_rules"]
    assert result["primary_rows"] == FIXTURE_INFO["fixture_primary_reason_counts"]
    s = state(settings)
    assert s["raw_rows"] == result["rows_loaded"] and s["quarantine_rows"] == result["rows_quarantined"]
    ledger = one(settings, "SELECT rows_in, rows_loaded, rows_quarantined, load_count FROM ops.ingestion_ledger")
    assert ledger == (result["rows_in"], result["rows_loaded"], result["rows_quarantined"], 1)


def test_quarantined_rows_carry_machine_readable_reason_codes(settings):
    pipeline.run_month(settings, MONTH, RUN)
    codes = one(settings, "SELECT array_agg(DISTINCT primary_reason ORDER BY primary_reason) FROM quarantine.yellow_trips")[0]
    assert codes == sorted(FIXTURE_INFO["fixture_primary_reason_counts"])
    assert one(settings, "SELECT count(*) FROM quarantine.yellow_trips WHERE NOT (primary_reason = ANY(reason_codes))")[0] == 0


def test_running_the_month_again_changes_nothing(settings):
    pipeline.run_month(settings, MONTH, RUN)
    first = state(settings)
    for attempt in range(2):
        pipeline.run_month(settings, MONTH, f"{RUN}-again-{attempt}")
        assert state(settings) == first
    dupes = one(settings, "SELECT count(*) FROM (SELECT source_month, source_row FROM raw.yellow_trips"
                          " GROUP BY 1, 2 HAVING count(*) > 1) d")[0]
    assert dupes == 0
    assert one(settings, "SELECT load_count FROM ops.ingestion_ledger")[0] == 3  # the load ran three times


def test_second_fetch_is_served_from_the_landing_bucket(settings):
    pipeline.fetch(settings, MONTH, RUN)
    meta = pipeline.fetch(settings, MONTH, RUN)
    timing = one(settings, "SELECT detail->>'cached' FROM ops.stage_timings WHERE stage = 'fetch' ORDER BY id DESC LIMIT 1")
    assert timing[0] == "true"
    assert meta["landing_key"] == settings.landing_key(MONTH)


def test_a_load_that_fails_halfway_leaves_the_previous_state_and_a_retry_succeeds(settings, monkeypatch):
    pipeline.run_month(settings, MONTH, RUN)
    before = state(settings)
    real_copy, calls = db.copy_table, []

    def crash_on_quarantine(conn, target, table, chunk_rows=250_000):
        calls.append(target)
        if target.startswith("quarantine."):
            raise RuntimeError("simulated crash after the good rows were copied")
        return real_copy(conn, target, table, chunk_rows)

    monkeypatch.setattr(db, "copy_table", crash_on_quarantine)
    with pytest.raises(RuntimeError, match="simulated crash"):
        pipeline.load_month(settings, MONTH, RUN + "-crash")
    assert calls[0].startswith("raw.") and calls[-1].startswith("quarantine.")  # it did get half way
    assert state(settings) == before
    assert one(settings, "SELECT load_count FROM ops.ingestion_ledger")[0] == 1

    monkeypatch.setattr(db, "copy_table", real_copy)
    pipeline.load_month(settings, MONTH, RUN + "-retry")
    assert state(settings) == before
    assert one(settings, "SELECT load_count FROM ops.ingestion_ledger")[0] == 2


def test_a_first_load_that_fails_halfway_leaves_no_rows_and_no_ledger_entry(settings, monkeypatch):
    pipeline.fetch(settings, MONTH, RUN)
    pipeline.load_zones(settings, RUN)
    pipeline.validate_month(settings, MONTH, RUN)
    real_copy = db.copy_table

    def crash(conn, target, table, chunk_rows=250_000):
        if target.startswith("quarantine."):
            raise RuntimeError("simulated crash")
        return real_copy(conn, target, table, chunk_rows)

    monkeypatch.setattr(db, "copy_table", crash)
    with pytest.raises(RuntimeError):
        pipeline.load_month(settings, MONTH, RUN)
    assert one(settings, "SELECT count(*) FROM raw.yellow_trips")[0] == 0
    assert one(settings, "SELECT count(*) FROM ops.ingestion_ledger")[0] == 0
    monkeypatch.setattr(db, "copy_table", real_copy)
    assert pipeline.load_month(settings, MONTH, RUN)["rows_in"] == FIXTURE_INFO["fixture_rows"]


@pytest.fixture()
def drift_source(tmp_path):
    return inject.write_file_faults(FIXTURE / "trip-data" / "yellow_tripdata_2025-03.parquet",
                                    FIXTURE / "misc" / "taxi_zone_lookup.csv", tmp_path / "faults", MONTH)


@pytest.mark.parametrize("fault", ["RENAMED_REQUIRED_COLUMN", "DROPPED_REQUIRED_COLUMN", "WRONG_COLUMN_TYPE", "EMPTY_FILE"])
def test_a_file_that_breaks_the_contract_is_rejected_whole(tmp_path, drift_source, fault):
    settings = make_settings(tmp_path, Path(drift_source[fault]))
    reset(settings)
    pipeline.fetch(settings, MONTH, RUN)
    pipeline.load_zones(settings, RUN)
    with pytest.raises(pipeline.ContractViolation):
        pipeline.validate_month(settings, MONTH, RUN)
    report = storage.get_json(storage.client(settings), settings.validated_bucket, "yellow/2025-03/report.json")
    assert report["contract_ok"] is False and report["contract_failures"]
    with pytest.raises(pipeline.ContractViolation):  # the loader refuses a report that says the contract failed
        pipeline.load_month(settings, MONTH, RUN)
    assert one(settings, "SELECT count(*) FROM raw.yellow_trips")[0] == 0
    assert one(settings, "SELECT count(*) FROM ops.ingestion_ledger")[0] == 0


def test_command_line_exit_codes_tell_a_contract_failure_from_a_missing_file(tmp_path, drift_source, monkeypatch):
    settings = make_settings(tmp_path, Path(drift_source["DROPPED_REQUIRED_COLUMN"]))
    reset(settings)
    for key, value in {"TAXI_PG_DSN": settings.pg_dsn, "TAXI_S3_ENDPOINT": settings.s3_endpoint,
                       "TAXI_LANDING_BUCKET": settings.landing_bucket, "TAXI_VALIDATED_BUCKET": settings.validated_bucket,
                       "TAXI_SOURCE_BASE": settings.source_base, "TAXI_ALERT_SINK": settings.alert_sink,
                       "TAXI_VALIDATE_WORKERS": "1"}.items():
        monkeypatch.setenv(key, value)
    assert cli.main(["load-zones", "--run-id", RUN]) == 0
    assert cli.main(["fetch", "--month", "2025-03", "--run-id", RUN]) == 0
    assert cli.main(["validate", "--month", "2025-03", "--run-id", RUN]) == cli.EXIT_CONTRACT
    assert cli.main(["fetch", "--month", "2031-01", "--run-id", RUN]) == cli.EXIT_NOT_PUBLISHED


def test_an_extra_column_in_the_file_is_accepted(tmp_path, drift_source):
    settings = make_settings(tmp_path, Path(drift_source["ADDED_UNKNOWN_COLUMN"]))
    reset(settings)
    assert pipeline.run_month(settings, MONTH, RUN)["load"]["rows_in"] == FIXTURE_INFO["fixture_rows"]


def test_reconcile_agrees_after_a_load_and_names_the_difference_after_a_row_goes_missing(settings):
    pipeline.run_month(settings, MONTH, RUN)
    result = pipeline.reconcile(settings, MONTH, RUN)
    assert result["problems"] == [] and result["raw"] + result["quarantine"] == result["rows_in"]
    with db.connect(settings) as conn:
        conn.execute("DELETE FROM raw.yellow_trips WHERE source_row = (SELECT min(source_row) FROM raw.yellow_trips)")
    with pytest.raises(RuntimeError, match="raw has"):
        pipeline.reconcile(settings, MONTH, RUN)


def test_freshness_names_the_months_that_were_never_loaded(settings):
    pipeline.run_month(settings, MONTH, RUN)
    with db.connect(settings) as conn:
        findings = check_freshness(conn, settings)
    assert [f.code for f in findings] == ["MISSING_MONTH", "MISSING_MONTH"]
    assert "2025-01" in findings[0].message and "2025-02" in findings[1].message
    pipeline.reconcile(settings, MONTH, RUN)  # reconcile raises the same findings into the alert sink
    sink = alerts.read_all(settings.alert_sink)
    assert [r["code"] for r in sink if r["kind"] == "freshness"] == ["MISSING_MONTH", "MISSING_MONTH"]
