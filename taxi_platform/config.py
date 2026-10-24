"""Settings, read from the environment so Airflow, the command line and CI share one code path."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_LOOKUP_URL = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"


def month_start(month: str | date) -> date:
    """Accept '2025-03' or a date inside the month and return the first day."""
    if isinstance(month, date):
        return month.replace(day=1)
    year, mon = month.split("-")[:2]
    return date(int(year), int(mon), 1)


def next_month(first: date) -> date:
    return date(first.year + (first.month == 12), first.month % 12 + 1, 1)


def month_label(first: date) -> str:
    return f"{first.year:04d}-{first.month:02d}"


def month_range(first: str, last: str) -> list[date]:
    out, cur, end = [], month_start(first), month_start(last)
    while cur <= end:
        out.append(cur)
        cur = next_month(cur)
    return out


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = field(default_factory=lambda: os.environ.get(
        "TAXI_PG_DSN", "postgresql://taxi:taxi@localhost:18232/warehouse"))
    s3_endpoint: str = field(default_factory=lambda: os.environ.get("TAXI_S3_ENDPOINT", "http://localhost:18290"))
    s3_access_key: str = field(default_factory=lambda: os.environ.get("TAXI_S3_ACCESS_KEY", "taxi-local-key"))
    s3_secret_key: str = field(default_factory=lambda: os.environ.get("TAXI_S3_SECRET_KEY", "taxi-local-secret"))
    landing_bucket: str = field(default_factory=lambda: os.environ.get("TAXI_LANDING_BUCKET", "landing"))
    validated_bucket: str = field(default_factory=lambda: os.environ.get("TAXI_VALIDATED_BUCKET", "validated"))
    # Where the TLC files come from: the public CloudFront URL, or a directory (tests, offline replays).
    source_base: str = field(default_factory=lambda: os.environ.get("TAXI_SOURCE_BASE", TLC_BASE_URL))
    alert_sink: str = field(default_factory=lambda: os.environ.get("TAXI_ALERT_SINK", "alert-sink/alerts.jsonl"))
    validate_workers: int = field(default_factory=lambda: int(os.environ.get("TAXI_VALIDATE_WORKERS", "4")))
    # Freshness: the month the window is expected to have reached and the allowed age of the last load.
    expected_latest_month: str = field(default_factory=lambda: os.environ.get("TAXI_EXPECTED_LATEST_MONTH", "2025-06"))
    first_month: str = field(default_factory=lambda: os.environ.get("TAXI_FIRST_MONTH", "2025-01"))
    max_load_age_hours: float = field(default_factory=lambda: float(os.environ.get("TAXI_MAX_LOAD_AGE_HOURS", "36")))
    # How long the loader waits for a lock on the month's partition before giving up, so a task never hangs
    # behind a transaction that a dead process left open.
    load_lock_timeout_s: int = field(default_factory=lambda: int(os.environ.get("TAXI_LOAD_LOCK_TIMEOUT_S", "180")))

    def source_url(self, month: date) -> str:
        label = month_label(month)
        return f"{self.source_base.rstrip('/')}/trip-data/yellow_tripdata_{label}.parquet"

    def zone_source_url(self) -> str:
        return f"{self.source_base.rstrip('/')}/misc/taxi_zone_lookup.csv"

    def landing_key(self, month: date) -> str:
        label = month_label(month)
        return f"yellow/{label}/yellow_tripdata_{label}.parquet"

    def validated_prefix(self, month: date) -> str:
        return f"yellow/{month_label(month)}"
