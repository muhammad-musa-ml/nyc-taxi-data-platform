"""Month arithmetic and the settings that every entry point shares."""
from __future__ import annotations

from datetime import date

import pytest

from taxi_platform.config import (
    Settings,
    month_label,
    month_range,
    month_start,
    next_month,
)


def test_month_start_accepts_labels_and_dates():
    assert month_start("2025-03") == date(2025, 3, 1)
    assert month_start("2025-03-17") == date(2025, 3, 1)
    assert month_start(date(2025, 3, 17)) == date(2025, 3, 1)


@pytest.mark.parametrize("first,expected", [
    (date(2025, 1, 1), date(2025, 2, 1)),
    (date(2025, 11, 1), date(2025, 12, 1)),
    (date(2025, 12, 1), date(2026, 1, 1)),
])
def test_next_month_rolls_over_the_year(first, expected):
    assert next_month(first) == expected


def test_month_label_pads_to_two_digits():
    assert month_label(date(2025, 3, 1)) == "2025-03"
    assert month_label(date(2025, 12, 1)) == "2025-12"


def test_month_range_is_inclusive_and_crosses_years():
    assert month_range("2025-01", "2025-06") == [date(2025, m, 1) for m in range(1, 7)]
    assert month_range("2025-11", "2026-02") == [date(2025, 11, 1), date(2025, 12, 1),
                                                 date(2026, 1, 1), date(2026, 2, 1)]
    assert month_range("2025-03", "2025-03") == [date(2025, 3, 1)]
    assert month_range("2025-04", "2025-03") == []


def test_settings_read_the_environment(monkeypatch):
    monkeypatch.setenv("TAXI_PG_DSN", "postgresql://u:p@db:5432/w")
    monkeypatch.setenv("TAXI_SOURCE_BASE", "/data/mirror/")
    monkeypatch.setenv("TAXI_VALIDATE_WORKERS", "2")
    monkeypatch.setenv("TAXI_MAX_LOAD_AGE_HOURS", "12.5")
    settings = Settings()
    assert settings.pg_dsn == "postgresql://u:p@db:5432/w"
    assert settings.validate_workers == 2
    assert settings.max_load_age_hours == 12.5
    # A trailing slash on the base must not double up in the URLs built from it.
    assert settings.source_url(date(2025, 3, 1)) == "/data/mirror/trip-data/yellow_tripdata_2025-03.parquet"
    assert settings.zone_source_url() == "/data/mirror/misc/taxi_zone_lookup.csv"


def test_settings_defaults_point_at_the_public_source(monkeypatch):
    monkeypatch.delenv("TAXI_SOURCE_BASE", raising=False)
    settings = Settings()
    assert settings.source_url(date(2025, 6, 1)) == \
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-06.parquet"


def test_landing_keys_and_prefixes_are_per_month():
    settings = Settings()
    month = date(2025, 4, 1)
    assert settings.landing_key(month) == "yellow/2025-04/yellow_tripdata_2025-04.parquet"
    assert settings.validated_prefix(month) == "yellow/2025-04"
