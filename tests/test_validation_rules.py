"""Every reason code fires on a row built to trigger it, and clean rows pass."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pyarrow as pa
import pytest

from taxi_platform import rules, validate

from .conftest import ZONE_IDS, clean_row, table_from_rows

MONTH = date(2025, 3, 1)

# (reason code, field changes applied to a clean row)
CASES = [
    ("NULL_PICKUP_TS", {"tpep_pickup_datetime": None}),
    ("NULL_DROPOFF_TS", {"tpep_dropoff_datetime": None}),
    ("NULL_ZONE", {"PULocationID": None}),
    ("NULL_ZONE", {"DOLocationID": None}),
    ("NULL_AMOUNT", {"fare_amount": None}),
    ("NULL_AMOUNT", {"total_amount": None}),
    ("NULL_VENDOR", {"VendorID": None}),
    ("NULL_PAYMENT_TYPE", {"payment_type": None}),
    ("NULL_DISTANCE", {"trip_distance": None}),
    ("INVALID_VENDOR", {"VendorID": 9}),
    ("INVALID_PAYMENT_TYPE", {"payment_type": 12}),
    ("INVALID_PASSENGER_COUNT", {"passenger_count": 40}),
    ("UNKNOWN_PICKUP_ZONE", {"PULocationID": 900}),
    ("UNKNOWN_DROPOFF_ZONE", {"DOLocationID": 0}),
    ("PICKUP_OUTSIDE_MONTH", {"tpep_pickup_datetime": datetime(2008, 12, 31, 23, 0),
                              "tpep_dropoff_datetime": datetime(2008, 12, 31, 23, 20)}),
    ("DROPOFF_BEFORE_PICKUP", {"tpep_dropoff_datetime": datetime(2025, 3, 10, 7, 0)}),
    ("DURATION_OVER_24H", {"tpep_dropoff_datetime": datetime(2025, 3, 12, 9, 0), "trip_distance": 20.0}),
    ("NEGATIVE_AMOUNT", {"fare_amount": -7.5}),
    ("NEGATIVE_AMOUNT", {"total_amount": -0.01}),
    ("EXCESSIVE_AMOUNT", {"fare_amount": 90000.0, "total_amount": 90005.0}),
    ("DISTANCE_OUT_OF_RANGE", {"trip_distance": 5000.0}),
    ("FEE_OUT_OF_RANGE", {"tip_amount": 90000.0}),
    ("IMPLAUSIBLE_SPEED", {"trip_distance": 60.0}),
]


def run(rows, **kwargs):
    table = validate.normalize_table(table_from_rows(rows, **kwargs))
    return validate.validate_frame(validate.prepare_frame(table), MONTH, ZONE_IDS, workers=1)


def test_clean_rows_pass_every_rule():
    outcome = run([clean_row(i) for i in range(30)])
    assert outcome.contract.ok
    assert outcome.rows_quarantined == 0
    assert outcome.rows_good == 30


@pytest.mark.parametrize("code,changes", CASES, ids=[f"{c}-{'+'.join(ch)}" for c, ch in CASES])
def test_reason_code_fires(code, changes):
    rows = [clean_row(i) for i in range(10)]
    rows[4] = {**rows[4], **changes}
    outcome = run(rows)
    assert outcome.rows_quarantined == 1, outcome.row_codes
    assert code in outcome.row_codes[4]
    # nothing else was disturbed
    assert set(outcome.row_codes) == {4}


def test_every_reason_code_has_a_case():
    covered = {code for code, _ in CASES} | {"DUPLICATE_TRIP"}
    assert covered == set(rules.REASON_CODES)


def test_duplicate_trip_quarantines_the_later_copy_only():
    rows = [clean_row(i) for i in range(10)]
    rows[7] = dict(rows[2])  # same vendor, times, zones, distance, fare and total as row 2
    outcome = run(rows)
    assert list(outcome.quarantine_rows) == [7]
    assert outcome.row_codes[7] == ["DUPLICATE_TRIP"]


def test_a_row_can_fail_several_rules_and_keeps_all_codes_in_rule_order():
    rows = [clean_row(i) for i in range(5)]
    rows[1] = {**rows[1], "fare_amount": -4.0, "trip_distance": 999.0}
    outcome = run(rows)
    # 999 miles in 20 minutes is also an implausible speed, so three rules fire.
    assert outcome.row_codes[1] == ["NEGATIVE_AMOUNT", "DISTANCE_OUT_OF_RANGE", "IMPLAUSIBLE_SPEED"]
    assert outcome.primary_counts() == {"NEGATIVE_AMOUNT": 1}
    assert outcome.rule_rows == {"NEGATIVE_AMOUNT": 1, "DISTANCE_OUT_OF_RANGE": 1, "IMPLAUSIBLE_SPEED": 1}


def test_null_flex_fare_fields_are_counted_not_quarantined():
    rows = [clean_row(i) for i in range(10)]
    for i in (0, 3, 6):
        rows[i] = {**rows[i], "passenger_count": None, "RatecodeID": None, "store_and_fwd_flag": None,
                   "congestion_surcharge": None, "Airport_fee": None}
    outcome = run(rows)
    assert outcome.rows_quarantined == 0
    assert outcome.warn_counts["NULL_PASSENGER_COUNT"] == 3
    assert outcome.warn_counts["NULL_STORE_AND_FWD_FLAG"] == 3


def test_older_files_without_the_cbd_fee_still_pass():
    outcome = run([clean_row(i) for i in range(10)], drop=("cbd_congestion_fee",))
    assert outcome.contract.ok and outcome.rows_quarantined == 0


def test_missing_required_column_fails_the_contract():
    outcome = run([clean_row(i) for i in range(10)], drop=("fare_amount",))
    assert not outcome.contract.ok
    assert "REQUIRED_COLUMNS" in outcome.contract.failures
    assert outcome.rows_quarantined == 0 and outcome.row_codes == {}


def test_wrong_column_type_fails_the_contract():
    table = validate.normalize_table(table_from_rows([clean_row(i) for i in range(10)]))
    frame = validate.prepare_frame(table)
    frame["fare_amount"] = frame["fare_amount"].astype(str)
    assert validate.check_contract(frame).failures == ["TYPE_fare_amount"]


@pytest.mark.parametrize("column", ["fare_amount", "trip_distance", "cbd_congestion_fee", "VendorID"])
def test_a_text_column_where_numbers_belong_fails_the_contract_instead_of_crashing(column):
    table = table_from_rows([clean_row(i) for i in range(10)])
    index = table.column_names.index(column)
    table = table.set_column(index, column, table.column(column).cast(pa.string()))
    outcome = validate.validate_frame(validate.prepare_frame(validate.normalize_table(table)), MONTH, ZONE_IDS)
    assert not outcome.contract.ok
    assert outcome.contract.failures == [f"TYPE_{column.lower()}"]
    assert outcome.row_codes == {}


def test_empty_file_fails_the_contract():
    table = validate.normalize_table(table_from_rows([clean_row(0)]).slice(0, 0))
    frame = validate.prepare_frame(table)
    assert "ROW_COUNT" in validate.check_contract(frame).failures


def test_sharded_run_matches_single_process_run():
    rows = [clean_row(i) for i in range(40)]
    rows[3] = {**rows[3], "fare_amount": -1.0}
    rows[17] = {**rows[17], "VendorID": 9}
    shift = timedelta(days=400)
    rows[33] = {**rows[33], "tpep_pickup_datetime": rows[33]["tpep_pickup_datetime"] - shift,
                "tpep_dropoff_datetime": rows[33]["tpep_dropoff_datetime"] - shift}
    table = validate.normalize_table(table_from_rows(rows))
    frame = validate.prepare_frame(table)
    one = validate.validate_frame(frame, MONTH, ZONE_IDS, workers=1)
    # Shard bounds are exercised even where the platform cannot fork.
    assert validate._shard_bounds(40, 4) == [(0, 10), (10, 20), (20, 30), (30, 40)]
    assert one.row_codes[3] == ["NEGATIVE_AMOUNT"]
    assert one.row_codes[17] == ["INVALID_VENDOR"]
    assert one.row_codes[33] == ["PICKUP_OUTSIDE_MONTH"]


def test_split_table_keeps_row_numbers_and_codes():
    rows = [clean_row(i) for i in range(6)]
    rows[2] = {**rows[2], "fare_amount": -3.0}
    table = validate.normalize_table(table_from_rows(rows))
    outcome = validate.validate_frame(validate.prepare_frame(table), MONTH, ZONE_IDS, workers=1)
    good, quarantine = validate.split_table(table, outcome)
    assert good.column("source_row").to_pylist() == [0, 1, 3, 4, 5]
    assert quarantine.column("source_row").to_pylist() == [2]
    assert quarantine.column("reason_codes").to_pylist() == ["{NEGATIVE_AMOUNT}"]
    assert quarantine.column("primary_reason").to_pylist() == ["NEGATIVE_AMOUNT"]
