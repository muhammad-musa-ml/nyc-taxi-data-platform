"""The alert sink and the freshness checks, with a stand-in for the database connection."""
from __future__ import annotations

import json
import sys
import threading
from datetime import date, datetime, timedelta, timezone

import pytest

from taxi_platform import alerts
from taxi_platform.config import Settings
from taxi_platform.freshness import check_freshness


class FakeConnection:
    """Answers the one query the freshness check makes."""

    def __init__(self, loaded: dict[date, datetime]):
        self.loaded = loaded

    def execute(self, sql: str, *_args):
        assert "ops.ingestion_ledger" in sql
        return self

    def fetchall(self):
        return list(self.loaded.items())


NOW = datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)


def settings(**overrides) -> Settings:
    base = dict(first_month="2025-01", expected_latest_month="2025-03", max_load_age_hours=36.0)
    return Settings(**{**base, **overrides})


def ledger(months: list[int], age_hours: float = 1.0) -> dict[date, datetime]:
    return {date(2025, m, 1): NOW - timedelta(hours=age_hours) for m in months}


def codes(findings) -> list[str]:
    return [f.code for f in findings]


def test_complete_and_recent_has_no_findings():
    assert check_freshness(FakeConnection(ledger([1, 2, 3])), settings(), now=NOW) == []


def test_a_missing_month_in_the_middle_is_named():
    findings = check_freshness(FakeConnection(ledger([1, 3])), settings(), now=NOW)
    assert codes(findings) == ["MISSING_MONTH"]
    assert "2025-02" in findings[0].message


def test_the_newest_load_being_too_old_is_reported_with_its_age():
    findings = check_freshness(FakeConnection(ledger([1, 2, 3], age_hours=50)), settings(), now=NOW)
    assert codes(findings) == ["LOAD_TOO_OLD"]
    assert "50.0 h" in findings[0].message


def test_one_old_month_does_not_matter_while_the_newest_load_is_recent():
    loaded = ledger([1, 2], age_hours=500)
    loaded[date(2025, 3, 1)] = NOW - timedelta(hours=2)
    assert check_freshness(FakeConnection(loaded), settings(), now=NOW) == []


def test_newest_loaded_month_behind_the_expected_one():
    findings = check_freshness(FakeConnection(ledger([1, 2])), settings(), now=NOW)
    assert codes(findings) == ["MISSING_MONTH", "LATEST_MONTH_BEHIND"]


def test_empty_ledger_reports_every_month_and_the_empty_ledger():
    findings = check_freshness(FakeConnection({}), settings(), now=NOW)
    assert codes(findings) == ["MISSING_MONTH"] * 3 + ["LOAD_TOO_OLD"]


def test_through_month_narrows_the_window():
    findings = check_freshness(FakeConnection(ledger([1, 2])), settings(), through_month="2025-02", now=NOW)
    assert findings == []


def test_emit_appends_one_json_line_per_record(tmp_path):
    sink = tmp_path / "nested" / "alerts.jsonl"
    first = alerts.emit(str(sink), "task_failure", "critical", "load failed", task_id="load", source_month="2025-03")
    alerts.emit(str(sink), "freshness", "warning", "month missing", code="MISSING_MONTH")
    lines = sink.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["kind"] == "task_failure" and parsed[0]["severity"] == "critical"
    assert parsed[0]["task_id"] == "load" and parsed[0]["source_month"] == "2025-03"
    assert parsed[1]["code"] == "MISSING_MONTH"
    assert first["emitted_at"] == parsed[0]["emitted_at"]
    assert datetime.fromisoformat(parsed[0]["emitted_at"]).tzinfo is not None


def test_read_all_returns_nothing_for_a_missing_sink(tmp_path):
    assert alerts.read_all(str(tmp_path / "none.jsonl")) == []


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="O_APPEND is atomic on POSIX only; the stack runs in Linux containers. On a Windows host six threads "
           "lost records (134 of 150 survived when this test was first run there).")
def test_concurrent_writers_never_split_a_line(tmp_path):
    sink = str(tmp_path / "alerts.jsonl")

    def writer(n: int):
        for i in range(25):
            alerts.emit(sink, "task_failure", "critical", f"writer {n} record {i}", padding="x" * 400)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    records = alerts.read_all(sink)  # json.loads on every line: a torn line would raise
    assert len(records) == 150


def test_non_serialisable_fields_are_stringified(tmp_path):
    sink = str(tmp_path / "alerts.jsonl")
    alerts.emit(sink, "task_failure", "critical", "x", when=date(2025, 3, 1))
    assert alerts.read_all(sink)[0]["when"] == "2025-03-01"
