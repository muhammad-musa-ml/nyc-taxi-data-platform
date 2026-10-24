"""Row rules and the file contract, declared as Great Expectations expectations.

Every row rule carries ``meta["reason_code"]``; that code is what lands in the quarantine table.
"""
from __future__ import annotations

import os
from datetime import date

# Telemetry stays off: the pipeline makes no outbound call other than the data download.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
os.environ.setdefault("TQDM_DISABLE", "1")  # no progress bars in task logs

import great_expectations.expectations as gxe  # noqa: E402
import pandas as pd  # noqa: E402

from .config import next_month  # noqa: E402

REQUIRED_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
    "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee",
]
OPTIONAL_COLUMNS = ["cbd_congestion_fee"]  # added by the TLC in 2025; older months do not have it
DATA_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

DERIVED_COLUMNS = ["trip_duration_s", "speed_mph", "min_amount", "max_amount", "max_abs_fee", "is_repeat_row"]
DUPLICATE_KEY = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid",
    "trip_distance", "fare_amount", "total_amount",
]
FEE_COLUMNS = ["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
               "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]

VALID_VENDORS = [1, 2, 4, 6, 7]
VALID_PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]
MAX_DISTANCE_MILES = 200
MAX_SPEED_MPH = 100
MAX_DURATION_S = 24 * 3600
MAX_FARE_OR_TOTAL = 5000
MAX_ABS_FEE = 5000

# Columns the TLC leaves null on purpose for flex-fare trips; reported, never quarantined.
SOMETIMES_NULL_COLUMNS = ["passenger_count", "ratecodeid", "store_and_fwd_flag", "congestion_surcharge", "airport_fee"]

# Every reason code the row rules can emit, in the order they are checked.
REASON_CODES = [
    "NULL_PICKUP_TS", "NULL_DROPOFF_TS", "NULL_ZONE", "NULL_AMOUNT", "NULL_VENDOR", "NULL_PAYMENT_TYPE",
    "NULL_DISTANCE", "INVALID_VENDOR", "INVALID_PAYMENT_TYPE",
    "INVALID_PASSENGER_COUNT", "UNKNOWN_PICKUP_ZONE", "UNKNOWN_DROPOFF_ZONE", "PICKUP_OUTSIDE_MONTH",
    "DROPOFF_BEFORE_PICKUP", "DURATION_OVER_24H", "NEGATIVE_AMOUNT", "EXCESSIVE_AMOUNT",
    "DISTANCE_OUT_OF_RANGE", "FEE_OUT_OF_RANGE", "IMPLAUSIBLE_SPEED", "DUPLICATE_TRIP",
]


def _m(code: str) -> dict:
    return {"reason_code": code}


def row_expectations(month: date, zone_ids: list[int]) -> list:
    """The row rules for one file. ``month`` is the first day of the file's calendar month."""
    start = pd.Timestamp(month)
    end = pd.Timestamp(next_month(month)) - pd.Timedelta(microseconds=1)
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime", meta=_m("NULL_PICKUP_TS")),
        gxe.ExpectColumnValuesToNotBeNull(column="tpep_dropoff_datetime", meta=_m("NULL_DROPOFF_TS")),
        gxe.ExpectColumnValuesToNotBeNull(column="pulocationid", meta=_m("NULL_ZONE")),
        gxe.ExpectColumnValuesToNotBeNull(column="dolocationid", meta=_m("NULL_ZONE")),
        gxe.ExpectColumnValuesToNotBeNull(column="fare_amount", meta=_m("NULL_AMOUNT")),
        gxe.ExpectColumnValuesToNotBeNull(column="total_amount", meta=_m("NULL_AMOUNT")),
        gxe.ExpectColumnValuesToNotBeNull(column="vendorid", meta=_m("NULL_VENDOR")),
        gxe.ExpectColumnValuesToNotBeNull(column="payment_type", meta=_m("NULL_PAYMENT_TYPE")),
        gxe.ExpectColumnValuesToNotBeNull(column="trip_distance", meta=_m("NULL_DISTANCE")),
        gxe.ExpectColumnValuesToBeInSet(column="vendorid", value_set=VALID_VENDORS, meta=_m("INVALID_VENDOR")),
        gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=VALID_PAYMENT_TYPES,
                                        meta=_m("INVALID_PAYMENT_TYPE")),
        gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=0, max_value=9,
                                          meta=_m("INVALID_PASSENGER_COUNT")),
        gxe.ExpectColumnValuesToBeInSet(column="pulocationid", value_set=zone_ids, meta=_m("UNKNOWN_PICKUP_ZONE")),
        gxe.ExpectColumnValuesToBeInSet(column="dolocationid", value_set=zone_ids, meta=_m("UNKNOWN_DROPOFF_ZONE")),
        gxe.ExpectColumnValuesToBeBetween(column="tpep_pickup_datetime", min_value=start, max_value=end,
                                          meta=_m("PICKUP_OUTSIDE_MONTH")),
        gxe.ExpectColumnValuesToBeBetween(column="trip_duration_s", min_value=0, meta=_m("DROPOFF_BEFORE_PICKUP")),
        gxe.ExpectColumnValuesToBeBetween(column="trip_duration_s", max_value=MAX_DURATION_S,
                                          meta=_m("DURATION_OVER_24H")),
        gxe.ExpectColumnValuesToBeBetween(column="min_amount", min_value=0, meta=_m("NEGATIVE_AMOUNT")),
        gxe.ExpectColumnValuesToBeBetween(column="max_amount", max_value=MAX_FARE_OR_TOTAL,
                                          meta=_m("EXCESSIVE_AMOUNT")),
        gxe.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0, max_value=MAX_DISTANCE_MILES,
                                          meta=_m("DISTANCE_OUT_OF_RANGE")),
        gxe.ExpectColumnValuesToBeBetween(column="max_abs_fee", max_value=MAX_ABS_FEE, meta=_m("FEE_OUT_OF_RANGE")),
        gxe.ExpectColumnValuesToBeBetween(column="speed_mph", max_value=MAX_SPEED_MPH, meta=_m("IMPLAUSIBLE_SPEED")),
        gxe.ExpectColumnValuesToBeInSet(column="is_repeat_row", value_set=[False], meta=_m("DUPLICATE_TRIP")),
    ]


def warn_expectations() -> list:
    """Null shares the TLC produces on purpose. Counted and reported, never quarantined."""
    return [gxe.ExpectColumnValuesToNotBeNull(column=c, meta={"warning": f"NULL_{c.upper()}"})
            for c in SOMETIMES_NULL_COLUMNS]


# pandas dtypes a column may arrive as. Integer columns that hold nulls come through as float64.
_INT_OR_FLOAT = ["int8", "int16", "int32", "int64", "float32", "float64", "Int32", "Int64"]
_TIMESTAMP = ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"]
_TEXT = ["object", "StringDtype", "str", "string"]  # GX reports pandas' string dtype by its class name
CONTRACT_TYPES = {
    "tpep_pickup_datetime": _TIMESTAMP,
    "tpep_dropoff_datetime": _TIMESTAMP,
    "store_and_fwd_flag": _TEXT,
    **{c: _INT_OR_FLOAT for c in REQUIRED_COLUMNS if c not in ("tpep_pickup_datetime", "tpep_dropoff_datetime",
                                                               "store_and_fwd_flag")},
}


def contract_expectations(min_rows: int = 1, max_rows: int = 50_000_000) -> list:
    exps = [
        gxe.ExpectTableColumnsToMatchSet(column_set=REQUIRED_COLUMNS, exact_match=False,
                                         meta={"contract": "REQUIRED_COLUMNS"}),
        gxe.ExpectTableRowCountToBeBetween(min_value=min_rows, max_value=max_rows,
                                           meta={"contract": "ROW_COUNT"}),
    ]
    exps += [gxe.ExpectColumnValuesToBeInTypeList(column=c, type_list=t, meta={"contract": f"TYPE_{c}"})
             for c, t in CONTRACT_TYPES.items()]
    return exps
