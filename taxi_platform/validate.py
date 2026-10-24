"""Validate one month of trips with Great Expectations and split it into good and quarantined rows.

The row number inside the file (``source_row``) is the DataFrame index, so Great Expectations returns
the failing row numbers directly. Naming an index column instead was measured at roughly 7x slower.
"""
from __future__ import annotations

import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date

from . import rules  # noqa: F401  (sets GX_ANALYTICS_ENABLED before great_expectations is imported)

import great_expectations as gx  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
from great_expectations.data_context.types.base import ProgressBarsConfig  # noqa: E402


class ValidationError(RuntimeError):
    """A rule could not be evaluated. Never treated as a pass."""


def normalize_table(table: pa.Table) -> pa.Table:
    """Lower-case the column names, add the optional columns a file may lack, keep the TLC column order."""
    table = table.rename_columns([c.lower() for c in table.column_names])
    for col in rules.OPTIONAL_COLUMNS:
        if col not in table.column_names:
            table = table.append_column(col, pa.nulls(table.num_rows, pa.float64()))
    return table


def prepare_frame(table: pa.Table) -> pd.DataFrame:
    """The DataFrame Great Expectations sees: the data columns plus the derived columns the rules use."""
    df = table.select([c for c in rules.DATA_COLUMNS if c in table.column_names]).to_pandas()
    # Great Expectations 1.24's type-list check does not recognise pandas 3's default string dtype.
    if "store_and_fwd_flag" in df.columns and isinstance(df["store_and_fwd_flag"].dtype, pd.StringDtype):
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(object)
    if any(c not in df.columns for c in rules.REQUIRED_COLUMNS):
        return df  # the file contract fails on the missing column; the derived columns need all of them
    stamps = ("tpep_pickup_datetime", "tpep_dropoff_datetime")
    numbers = [c for c in rules.DATA_COLUMNS if c not in stamps + ("store_and_fwd_flag",) and c in df.columns]
    if not all(pd.api.types.is_datetime64_any_dtype(df[c]) for c in stamps) \
            or not all(pd.api.types.is_numeric_dtype(df[c]) for c in numbers):
        return df  # a column has the wrong type: the contract names it, and arithmetic on it would crash
    duration = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    df["trip_duration_s"] = duration
    hours = duration.where(duration > 0) / 3600.0
    df["speed_mph"] = df["trip_distance"] / hours
    df["min_amount"] = df[["fare_amount", "total_amount"]].min(axis=1)
    df["max_amount"] = df[["fare_amount", "total_amount"]].max(axis=1)
    df["max_abs_fee"] = df[[c for c in rules.FEE_COLUMNS if c in df.columns]].abs().max(axis=1)
    df["is_repeat_row"] = df.duplicated(subset=rules.DUPLICATE_KEY, keep="first")
    return df


def _new_batch_definition():
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    source = context.data_sources.add_pandas("trips")
    asset = source.add_dataframe_asset("month")
    return asset.add_batch_definition_whole_dataframe("whole")


def _run_suite(frame: pd.DataFrame, expectations: list, result_format: dict, name: str):
    suite = gx.ExpectationSuite(name=name, expectations=expectations)
    batch = _new_batch_definition().get_batch(batch_parameters={"dataframe": frame})
    result = batch.validate(suite, result_format=result_format)
    if len(result.results) != len(expectations):
        raise ValidationError(f"{name}: {len(result.results)} results for {len(expectations)} expectations")
    for item in result.results:
        info = item.exception_info or {}
        if any(v.get("raised_exception") for v in info.values() if isinstance(v, dict)):
            message = next(v.get("exception_message") for v in info.values() if v.get("raised_exception"))
            raise ValidationError(f"{name}: expectation {item.expectation_config.type} raised: {message}")
    return result


# Shared with forked workers; set before the pool is created.
_SHARD_STATE: dict = {}


def _validate_shard(bounds: tuple[int, int]) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    start, stop = bounds
    frame = _SHARD_STATE["frame"].iloc[start:stop]
    expectations = rules.row_expectations(_SHARD_STATE["month"], _SHARD_STATE["zone_ids"])
    result = _run_suite(frame, expectations, {"result_format": "COMPLETE", "return_unexpected_index_query": False,
                                              "include_unexpected_rows": False}, "row_rules")
    failing: dict[str, list[np.ndarray]] = {}
    for item in result.results:
        code = item.expectation_config.meta["reason_code"]
        idx = item.result.get("unexpected_index_list") or []
        if item.result.get("unexpected_count", 0) != len(idx):
            raise ValidationError(f"{code}: {item.result.get('unexpected_count')} failures but {len(idx)} row numbers")
        if idx:
            failing.setdefault(code, []).append(np.asarray(idx, dtype=np.int64))
    merged = {code: np.unique(np.concatenate(parts)) for code, parts in failing.items()}

    warn = _run_suite(frame, rules.warn_expectations(), {"result_format": "SUMMARY"}, "null_shares")
    warn_counts = {item.expectation_config.meta["warning"]: int(item.result.get("unexpected_count", 0))
                   for item in warn.results}
    return merged, warn_counts


@dataclass
class ContractResult:
    ok: bool
    failures: list[str] = field(default_factory=list)


def check_contract(frame: pd.DataFrame) -> ContractResult:
    """File-level contract: required columns, plausible types, at least one row."""
    base = frame[[c for c in frame.columns if c in rules.DATA_COLUMNS]]
    first = _run_suite(base, rules.contract_expectations()[:2], {"result_format": "BASIC"}, "contract_shape")
    failures = [r.expectation_config.meta["contract"] for r in first.results if not r.success]
    if "REQUIRED_COLUMNS" in failures:
        return ContractResult(False, failures)
    typed = _run_suite(base, rules.contract_expectations()[2:], {"result_format": "BASIC"}, "contract_types")
    failures += [r.expectation_config.meta["contract"] for r in typed.results if not r.success]
    return ContractResult(not failures, failures)


@dataclass
class Outcome:
    rows_in: int
    contract: ContractResult
    quarantine_rows: np.ndarray  # sorted row numbers
    row_codes: dict[int, list[str]]  # row number -> reason codes, in rule order
    warn_counts: dict[str, int]
    rule_rows: dict[str, int]  # reason code -> rows that failed it (a row may fail several)

    @property
    def rows_quarantined(self) -> int:
        return int(len(self.quarantine_rows))

    @property
    def rows_good(self) -> int:
        return self.rows_in - self.rows_quarantined

    def primary_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for codes in self.row_codes.values():
            out[codes[0]] = out.get(codes[0], 0) + 1
        return out


def _shard_bounds(n: int, workers: int) -> list[tuple[int, int]]:
    workers = max(1, min(workers, n)) if n else 1
    step = -(-n // workers) if n else 0
    return [(i, min(i + step, n)) for i in range(0, n, step)] if n else []


def validate_frame(frame: pd.DataFrame, month: date, zone_ids: list[int], workers: int = 1) -> Outcome:
    contract = check_contract(frame)
    n = len(frame)
    if not contract.ok:
        return Outcome(n, contract, np.empty(0, dtype=np.int64), {}, {}, {})
    _SHARD_STATE.update(frame=frame, month=month, zone_ids=zone_ids)
    bounds = _shard_bounds(n, workers)
    can_fork = "fork" in multiprocessing.get_all_start_methods() and not sys.platform.startswith("win")
    try:
        if workers > 1 and can_fork and len(bounds) > 1:
            with ProcessPoolExecutor(max_workers=len(bounds), mp_context=multiprocessing.get_context("fork")) as pool:
                shard_results = list(pool.map(_validate_shard, bounds))
        else:
            shard_results = [_validate_shard(b) for b in bounds]
    finally:
        _SHARD_STATE.clear()

    by_code: dict[str, list[np.ndarray]] = {}
    warn_counts: dict[str, int] = {}
    for failing, warn in shard_results:
        for code, idx in failing.items():
            by_code.setdefault(code, []).append(idx)
        for key, value in warn.items():
            warn_counts[key] = warn_counts.get(key, 0) + value
    rule_rows: dict[str, int] = {}
    row_codes: dict[int, list[str]] = {}
    for code in rules.REASON_CODES:  # rule order decides the primary reason
        if code not in by_code:
            continue
        idx = np.unique(np.concatenate(by_code[code]))
        rule_rows[code] = int(len(idx))
        for i in idx.tolist():
            row_codes.setdefault(i, []).append(code)
    quarantine_rows = np.fromiter(sorted(row_codes), dtype=np.int64, count=len(row_codes))
    return Outcome(n, contract, quarantine_rows, row_codes, warn_counts, rule_rows)


def split_table(table: pa.Table, outcome: Outcome) -> tuple[pa.Table, pa.Table]:
    """Add ``source_row`` and split into (good rows, quarantined rows with reason columns)."""
    n = table.num_rows
    table = table.append_column("source_row", pa.array(np.arange(n, dtype=np.int64)))
    mask = np.zeros(n, dtype=bool)
    mask[outcome.quarantine_rows] = True
    good = table.filter(pa.array(~mask))
    quarantine = table.filter(pa.array(mask))
    codes = [outcome.row_codes[int(r)] for r in outcome.quarantine_rows]
    quarantine = quarantine.append_column("reason_codes", pa.array(["{" + ",".join(c) + "}" for c in codes],
                                                                   type=pa.string()))
    quarantine = quarantine.append_column("primary_reason", pa.array([c[0] for c in codes], type=pa.string()))
    return good, quarantine


def worker_default() -> int:
    return max(1, min(4, (os.cpu_count() or 1)))
