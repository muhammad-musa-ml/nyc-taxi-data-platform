"""Shared helpers: small in-memory months of trips with one known fault per row."""
from __future__ import annotations

from datetime import datetime, timedelta

import pyarrow as pa
import pytest

ZONE_IDS = list(range(1, 266))
MONTH_START = datetime(2025, 3, 1)

COLUMNS = {
    "VendorID": pa.int32(), "tpep_pickup_datetime": pa.timestamp("us"), "tpep_dropoff_datetime": pa.timestamp("us"),
    "passenger_count": pa.int64(), "trip_distance": pa.float64(), "RatecodeID": pa.int64(),
    "store_and_fwd_flag": pa.string(), "PULocationID": pa.int32(), "DOLocationID": pa.int32(),
    "payment_type": pa.int64(), "fare_amount": pa.float64(), "extra": pa.float64(), "mta_tax": pa.float64(),
    "tip_amount": pa.float64(), "tolls_amount": pa.float64(), "improvement_surcharge": pa.float64(),
    "total_amount": pa.float64(), "congestion_surcharge": pa.float64(), "Airport_fee": pa.float64(),
    "cbd_congestion_fee": pa.float64(),
}


def clean_row(i: int) -> dict:
    """A valid trip; ``i`` shifts the pickup minute so no two clean rows are duplicates."""
    pickup = MONTH_START + timedelta(days=9, hours=8, minutes=i)
    return {
        "VendorID": 1, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup + timedelta(minutes=20),
        "passenger_count": 1, "trip_distance": 3.2, "RatecodeID": 1, "store_and_fwd_flag": "N",
        "PULocationID": 100, "DOLocationID": 200, "payment_type": 1, "fare_amount": 18.5, "extra": 1.0,
        "mta_tax": 0.5, "tip_amount": 3.0, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
        "total_amount": 24.0, "congestion_surcharge": 2.5, "Airport_fee": 0.0, "cbd_congestion_fee": 0.75,
    }


def table_from_rows(rows: list[dict], drop: tuple[str, ...] = ()) -> pa.Table:
    cols = {name: pa.array([r[name] for r in rows], type=typ) for name, typ in COLUMNS.items() if name not in drop}
    return pa.table(cols)


@pytest.fixture()
def zone_ids():
    return ZONE_IDS
