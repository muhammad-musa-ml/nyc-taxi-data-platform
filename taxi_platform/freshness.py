"""Freshness and completeness checks on the ingestion ledger."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

from .config import Settings, month_label, month_range, month_start


@dataclass(frozen=True)
class Finding:
    code: str
    message: str


def check_freshness(conn: psycopg.Connection, settings: Settings, through_month: str | None = None,
                    now: datetime | None = None) -> list[Finding]:
    """Findings for the months ``first_month`` .. ``through_month`` (default: the expected latest month).

    - MISSING_MONTH: a month in the window has no ledger row.
    - LOAD_TOO_OLD: the newest load is older than the allowed age.
    """
    now = now or datetime.now(timezone.utc)
    through = through_month or settings.expected_latest_month
    expected = month_range(settings.first_month, through)
    rows = conn.execute("SELECT source_month, loaded_at FROM ops.ingestion_ledger").fetchall()
    loaded = {r[0]: r[1] for r in rows}
    findings = [Finding("MISSING_MONTH", f"no load recorded for {month_label(m)}") for m in expected if m not in loaded]
    if loaded:
        newest = max(loaded.values())
        age_hours = (now - newest).total_seconds() / 3600
        if age_hours > settings.max_load_age_hours:
            findings.append(Finding(
                "LOAD_TOO_OLD",
                f"newest load finished {age_hours:.1f} h ago; the allowed age is {settings.max_load_age_hours:g} h"))
    else:
        findings.append(Finding("LOAD_TOO_OLD", "the ledger is empty"))
    latest_expected = month_start(through)
    if loaded and max(loaded) < latest_expected:
        findings.append(Finding(
            "LATEST_MONTH_BEHIND",
            f"newest loaded month is {month_label(max(loaded))}; expected {month_label(latest_expected)}"))
    return findings
