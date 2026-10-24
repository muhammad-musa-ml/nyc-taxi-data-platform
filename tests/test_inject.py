"""The injector writes the faults it says it writes, and scoring against its manifest is exact."""
from __future__ import annotations

from datetime import date

import numpy as np
import pyarrow.parquet as pq
import pytest

from taxi_platform import inject, validate

from .conftest import ZONE_IDS, clean_row, table_from_rows

MONTH = date(2025, 3, 1)
PER_FAULT = 5


@pytest.fixture(scope="module")
def clean_table():
    return table_from_rows([clean_row(i) for i in range(600)])


@pytest.fixture(scope="module")
def injected(clean_table):
    return inject.inject(clean_table, MONTH, ZONE_IDS, np.empty(0, dtype=np.int64), per_fault=PER_FAULT, seed=11)


def run_rules(table):
    frame = validate.prepare_frame(validate.normalize_table(table))
    return validate.validate_frame(frame, MONTH, ZONE_IDS, workers=1)


@pytest.fixture(scope="module")
def scored(injected):
    table, manifest = injected
    return inject.evaluate(manifest, run_rules(table).row_codes)


def test_the_original_clean_table_has_nothing_to_quarantine(clean_table):
    assert run_rules(clean_table).rows_quarantined == 0


def test_every_detectable_fault_is_caught_by_its_own_reason_code(scored):
    for name, slot in scored["per_fault"].items():
        if slot["detectable"]:
            assert slot["injected"] == PER_FAULT, name
            assert slot["caught_by_expected_code"] == PER_FAULT, (name, slot)
            assert slot["quarantined_by_other_code_only"] == 0 and slot["loaded"] == 0, (name, slot)


def test_undetectable_faults_are_loaded_and_counted_apart(scored):
    names = {n for n, s in scored["per_fault"].items() if not s["detectable"]}
    assert names == {"PLAUSIBLE_FARE_TAMPER", "ZONE_SWAP"}
    for name in names:
        slot = scored["per_fault"][name]
        assert slot["loaded"] == PER_FAULT and slot["caught_by_expected_code"] == 0, (name, slot)
    assert scored["undetectable_total"]["injected"] == 2 * PER_FAULT
    assert scored["undetectable_total"]["loaded"] == 2 * PER_FAULT


def test_nothing_else_is_quarantined(scored):
    assert scored["quarantined_rows_not_injected"] == 0


def test_every_fault_has_a_row_in_the_manifest_and_every_code_has_a_fault(injected):
    _, manifest = injected
    assert {e["fault"] for e in manifest["entries"]} == {f.name for f in inject.FAULTS}
    from taxi_platform import rules
    assert {f.expected_code for f in inject.FAULTS if f.detectable} == set(rules.REASON_CODES)


def test_only_the_listed_rows_differ_from_the_original(clean_table, injected):
    table, manifest = injected
    changed = {e["row"] for e in manifest["entries"] if e["fault"] != inject.DUPLICATE}
    appended = [e["row"] for e in manifest["entries"] if e["fault"] == inject.DUPLICATE]
    assert table.num_rows == clean_table.num_rows + len(appended)
    assert appended == list(range(clean_table.num_rows, clean_table.num_rows + PER_FAULT))
    before, after = clean_table.to_pylist(), table.to_pylist()
    differing = {i for i in range(clean_table.num_rows) if before[i] != after[i]}
    assert differing == changed
    assert table.schema == clean_table.schema


def test_a_duplicate_is_an_exact_copy_of_an_untouched_row(clean_table, injected):
    table, manifest = injected
    rows = table.to_pylist()
    original = clean_table.to_pylist()
    for entry in manifest["entries"]:
        if entry["fault"] == inject.DUPLICATE:
            source = entry["changes"]["copy_of_row"][1]
            assert rows[entry["row"]] == rows[source] == original[source]


def test_same_seed_same_output_and_a_different_seed_picks_other_rows(clean_table, injected):
    again_table, again = inject.inject(clean_table, MONTH, ZONE_IDS, np.empty(0, dtype=np.int64),
                                       per_fault=PER_FAULT, seed=11)
    assert again == injected[1] and again_table.equals(injected[0])
    _, other = inject.inject(clean_table, MONTH, ZONE_IDS, np.empty(0, dtype=np.int64),
                             per_fault=PER_FAULT, seed=12)
    assert {e["row"] for e in other["entries"]} != {e["row"] for e in injected[1]["entries"]}


def test_rows_the_rules_already_reject_are_never_chosen(clean_table):
    natural = np.arange(0, 300, dtype=np.int64)
    _, manifest = inject.inject(clean_table, MONTH, ZONE_IDS, natural, per_fault=PER_FAULT, seed=3)
    used = {e["row"] for e in manifest["entries"] if e["fault"] != inject.DUPLICATE}
    used |= {e["changes"]["copy_of_row"][1] for e in manifest["entries"] if e["fault"] == inject.DUPLICATE}
    assert used and min(used) >= 300


def test_asking_for_more_rows_than_exist_is_an_error(clean_table):
    with pytest.raises(ValueError, match="candidate rows"):
        inject.inject(clean_table, MONTH, ZONE_IDS, np.empty(0, dtype=np.int64), per_fault=500, seed=1)


def test_scoring_separates_expected_other_and_loaded():
    manifest = {
        "rows_before": 10, "natural_quarantined_rows": 0,
        "faults": [{"name": "A", "expected_reason_code": "X", "detectable": True},
                   {"name": "B", "expected_reason_code": None, "detectable": False}],
        "entries": [{"row": 1, "fault": "A", "expected_reason_code": "X"},
                    {"row": 2, "fault": "A", "expected_reason_code": "X"},
                    {"row": 3, "fault": "A", "expected_reason_code": "X"},
                    {"row": 4, "fault": "B", "expected_reason_code": None}],
    }
    result = inject.evaluate(manifest, {1: ["X", "Y"], 2: ["Y"], 7: ["Z"]})
    assert result["per_fault"]["A"] == {"expected_reason_code": "X", "detectable": True, "injected": 3,
                                        "caught_by_expected_code": 1, "quarantined_by_other_code_only": 1, "loaded": 1}
    assert result["per_fault"]["B"]["loaded"] == 1
    assert result["quarantined_rows_not_injected"] == 1  # row 7 was never touched by the injector


# ---------------------------------------------------------------------------- file-level faults

@pytest.fixture()
def drift_files(tmp_path, clean_table):
    src = tmp_path / "src.parquet"
    pq.write_table(clean_table, src)
    zones = tmp_path / "zones.csv"
    zones.write_text("LocationID,Borough,Zone,service_zone\n1,EWR,Newark Airport,EWR\n", encoding="utf-8")
    return inject.write_file_faults(src, zones, tmp_path / "out", MONTH)


def contract_of(directory: str):
    table = pq.read_table(f"{directory}/trip-data/yellow_tripdata_2025-03.parquet")
    return validate.check_contract(validate.prepare_frame(validate.normalize_table(table)))


@pytest.mark.parametrize("fault,failure", [
    ("RENAMED_REQUIRED_COLUMN", "REQUIRED_COLUMNS"),
    ("DROPPED_REQUIRED_COLUMN", "REQUIRED_COLUMNS"),
    ("WRONG_COLUMN_TYPE", "TYPE_fare_amount"),
    ("EMPTY_FILE", "ROW_COUNT"),
])
def test_broken_files_fail_the_contract(drift_files, fault, failure):
    result = contract_of(drift_files[fault])
    assert not result.ok and failure in result.failures


def test_an_extra_column_does_not_stop_the_load(drift_files):
    assert contract_of(drift_files["ADDED_UNKNOWN_COLUMN"]).ok
