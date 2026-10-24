"""Inject known faults into real rows, keep a manifest of every change, and score what the pipeline caught.

The injector only touches rows that passed every row rule in the original file, so a quarantine decision on
one of them is caused by the injected fault and not by a defect the TLC data already had. Row numbers in
the manifest are positions in the injected file, the same ``source_row`` the pipeline records.

Two groups of faults:

* detectable: each one is aimed at one reason code (``expected_reason_code``);
* undetectable: values that are wrong but stay inside every range the rules enforce (a tampered fare, a
  swapped zone). The rules cannot see these, and they are reported separately so the number of caught
  rows is never read as "the data is right".

File-level faults (a renamed column, a dropped column, a wrong type, an empty file) are written by
:func:`write_file_faults`; the pipeline must reject each of those whole.
"""
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import validate
from .config import month_label, month_start


@dataclass(frozen=True)
class Fault:
    name: str
    expected_code: str | None  # None: no rule can see this fault
    mutate: Callable[[dict, dict], dict]  # (row, context) -> the fields to overwrite
    select: str | None = None  # name of the extra candidate filter, if the fault needs one

    @property
    def detectable(self) -> bool:
        return self.expected_code is not None


def _shift(days: int):
    def mutate(row, ctx):
        delta = timedelta(days=days)
        return {"tpep_pickup_datetime": row["tpep_pickup_datetime"] + delta,
                "tpep_dropoff_datetime": row["tpep_dropoff_datetime"] + delta}
    return mutate


def _set(**fields):
    def mutate(row, ctx):
        return dict(fields)
    return mutate


def _speed(row, ctx):
    hours = (row["tpep_dropoff_datetime"] - row["tpep_pickup_datetime"]).total_seconds() / 3600.0
    return {"trip_distance": round(130.0 * hours, 2)}  # 130 mph over the trip's own duration, under 200 miles


def _tamper(row, ctx):
    delta = round(row["fare_amount"] * 0.6, 2)
    return {"fare_amount": round(row["fare_amount"] + delta, 2), "total_amount": round(row["total_amount"] + delta, 2)}


def _swap_zone(row, ctx):
    zones = ctx["zone_ids"]
    nxt = zones[(zones.index(row["pulocationid"]) + 1) % len(zones)] if row["pulocationid"] in zones else zones[0]
    return {"pulocationid": nxt}


def _neg(row, ctx):
    return {"fare_amount": -row["fare_amount"]}


def _before(row, ctx):
    return {"tpep_dropoff_datetime": row["tpep_pickup_datetime"] - timedelta(minutes=10)}


def _over_day(row, ctx):
    return {"tpep_dropoff_datetime": row["tpep_pickup_datetime"] + timedelta(hours=26)}


FAULTS: list[Fault] = [
    Fault("NULL_PICKUP_TS", "NULL_PICKUP_TS", _set(tpep_pickup_datetime=None)),
    Fault("NULL_DROPOFF_TS", "NULL_DROPOFF_TS", _set(tpep_dropoff_datetime=None)),
    Fault("NULL_PICKUP_ZONE", "NULL_ZONE", _set(pulocationid=None)),
    Fault("NULL_DROPOFF_ZONE", "NULL_ZONE", _set(dolocationid=None)),
    Fault("NULL_FARE", "NULL_AMOUNT", _set(fare_amount=None)),
    Fault("NULL_TOTAL", "NULL_AMOUNT", _set(total_amount=None)),
    Fault("NULL_VENDOR", "NULL_VENDOR", _set(vendorid=None)),
    Fault("NULL_PAYMENT_TYPE", "NULL_PAYMENT_TYPE", _set(payment_type=None)),
    Fault("NULL_DISTANCE", "NULL_DISTANCE", _set(trip_distance=None)),
    Fault("INVALID_VENDOR", "INVALID_VENDOR", _set(vendorid=9)),
    Fault("INVALID_PAYMENT_TYPE", "INVALID_PAYMENT_TYPE", _set(payment_type=12)),
    Fault("INVALID_PASSENGER_COUNT", "INVALID_PASSENGER_COUNT", _set(passenger_count=40)),
    Fault("UNKNOWN_PICKUP_ZONE", "UNKNOWN_PICKUP_ZONE", _set(pulocationid=900)),
    Fault("UNKNOWN_DROPOFF_ZONE", "UNKNOWN_DROPOFF_ZONE", _set(dolocationid=0)),
    Fault("PICKUP_OUTSIDE_MONTH", "PICKUP_OUTSIDE_MONTH", _shift(-400)),
    Fault("DROPOFF_BEFORE_PICKUP", "DROPOFF_BEFORE_PICKUP", _before),
    Fault("DURATION_OVER_24H", "DURATION_OVER_24H", _over_day),
    Fault("NEGATIVE_AMOUNT", "NEGATIVE_AMOUNT", _neg, select="fare_at_least_1"),
    Fault("EXCESSIVE_AMOUNT", "EXCESSIVE_AMOUNT", _set(fare_amount=90000.0)),
    Fault("DISTANCE_OUT_OF_RANGE", "DISTANCE_OUT_OF_RANGE", _set(trip_distance=5000.0)),
    Fault("FEE_OUT_OF_RANGE", "FEE_OUT_OF_RANGE", _set(tip_amount=90000.0)),
    Fault("IMPLAUSIBLE_SPEED", "IMPLAUSIBLE_SPEED", _speed, select="duration_2_to_90_min"),
    Fault("DUPLICATE_TRIP", "DUPLICATE_TRIP", _set()),  # an exact copy appended after the original
    Fault("PLAUSIBLE_FARE_TAMPER", None, _tamper, select="fare_5_to_100"),
    Fault("ZONE_SWAP", None, _swap_zone, select="zone_known"),
]
FAULT_BY_NAME = {f.name: f for f in FAULTS}
DUPLICATE = "DUPLICATE_TRIP"


def _lower_map(table: pa.Table) -> dict[str, str]:
    return {name.lower(): name for name in table.column_names}


def natural_quarantine(table: pa.Table, month: date, zone_ids: list[int], workers: int = 1) -> np.ndarray:
    """Row numbers the rules already reject in the untouched file."""
    outcome = validate.validate_frame(validate.prepare_frame(validate.normalize_table(table)),
                                      month, zone_ids, workers=workers)
    return outcome.quarantine_rows


def _candidate_masks(table: pa.Table, month: date, zone_ids: list[int]) -> dict[str, np.ndarray]:
    names = _lower_map(table)
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "total_amount", "pulocationid"]
    frame = table.select([names[c] for c in cols]).to_pandas()
    frame.columns = cols
    seconds = (frame["tpep_dropoff_datetime"] - frame["tpep_pickup_datetime"]).dt.total_seconds()
    return {
        "fare_at_least_1": (frame["fare_amount"] >= 1).to_numpy(),
        "duration_2_to_90_min": ((seconds >= 120) & (seconds <= 5400)).to_numpy(),
        "fare_5_to_100": ((frame["fare_amount"] >= 5) & (frame["fare_amount"] <= 100)
                          & (frame["total_amount"] >= frame["fare_amount"])).to_numpy(),
        "zone_known": frame["pulocationid"].isin(zone_ids).to_numpy(),
    }


def _jsonable(value):
    return value.isoformat() if isinstance(value, (datetime, date)) else value


def inject(table: pa.Table, month: date, zone_ids: list[int], natural_rows: np.ndarray,
           per_fault: int = 50, seed: int = 7, faults: list[Fault] | None = None) -> tuple[pa.Table, dict]:
    """Return (the table with faults written in, the manifest). Nothing outside the chosen rows changes."""
    faults = faults if faults is not None else FAULTS
    n = table.num_rows
    names = _lower_map(table)
    rng = np.random.default_rng(seed)
    free = np.ones(n, dtype=bool)
    free[natural_rows] = False  # only rows that passed every rule are candidates
    extra = _candidate_masks(table, month, zone_ids)

    chosen: dict[int, Fault] = {}
    dup_sources: list[int] = []
    for fault in faults:
        ok = free.copy()
        if fault.select:
            ok &= extra[fault.select]
        pool = np.flatnonzero(ok)
        if len(pool) < per_fault:
            raise ValueError(f"{fault.name}: only {len(pool)} candidate rows for {per_fault} requested")
        picks = np.sort(rng.choice(pool, size=per_fault, replace=False))
        free[picks] = False  # a row carries at most one fault; a duplicate's source is left untouched
        if fault.name == DUPLICATE:
            dup_sources = picks.tolist()
        else:
            for p in picks.tolist():
                chosen[p] = fault

    positions = sorted(chosen)
    originals = table.take(pa.array(positions)).to_pylist() if positions else []
    entries, new_rows = [], []
    ctx = {"zone_ids": sorted(zone_ids)}
    for pos, original in zip(positions, originals, strict=True):
        lowered = {k.lower(): v for k, v in original.items()}
        changes = chosen[pos].mutate(lowered, ctx)
        updated = dict(original)
        for key, value in changes.items():
            updated[names[key]] = value
        new_rows.append(updated)
        entries.append({
            "row": pos, "fault": chosen[pos].name, "expected_reason_code": chosen[pos].expected_code,
            "changes": {k: [_jsonable(lowered[k]), _jsonable(v)] for k, v in changes.items()},
        })

    result = table
    if positions:
        replacement = pa.Table.from_pylist(new_rows, schema=table.schema)
        mask = np.zeros(n, dtype=bool)
        mask[positions] = True
        mask_arr = pa.array(mask)
        columns = [pc.replace_with_mask(table.column(i).combine_chunks(), mask_arr,
                                        replacement.column(i).combine_chunks())
                   for i in range(table.num_columns)]
        result = pa.table(columns, schema=table.schema)
    if dup_sources:
        copies = table.take(pa.array(dup_sources))
        for k, src in enumerate(dup_sources):
            entries.append({"row": n + k, "fault": DUPLICATE, "expected_reason_code": "DUPLICATE_TRIP",
                            "changes": {"copy_of_row": [None, src]}})
        result = pa.concat_tables([result, copies])

    manifest = {
        "month": month_label(month), "seed": seed, "per_fault": per_fault, "rows_before": n,
        "rows_after": result.num_rows, "natural_quarantined_rows": int(len(natural_rows)),
        "faults": [{"name": f.name, "expected_reason_code": f.expected_code, "detectable": f.detectable}
                   for f in faults],
        "entries": sorted(entries, key=lambda e: e["row"]),
    }
    return result, manifest


def evaluate(manifest: dict, quarantined: dict[int, list[str]]) -> dict:
    """Score the pipeline against the manifest.

    ``quarantined`` maps a row number to the reason codes the pipeline recorded for it. A detectable
    fault counts as caught when its expected code is among the row's codes. A row quarantined for some
    other code only is counted apart, and a row that was loaded is a miss.
    """
    per_fault: dict[str, dict] = {}
    for f in manifest["faults"]:
        per_fault[f["name"]] = {"expected_reason_code": f["expected_reason_code"], "detectable": f["detectable"],
                                "injected": 0, "caught_by_expected_code": 0,
                                "quarantined_by_other_code_only": 0, "loaded": 0}
    for e in manifest["entries"]:
        slot = per_fault[e["fault"]]
        slot["injected"] += 1
        codes = quarantined.get(e["row"])
        if codes is None:
            slot["loaded"] += 1
        elif e["expected_reason_code"] in codes:
            slot["caught_by_expected_code"] += 1
        else:
            slot["quarantined_by_other_code_only"] += 1

    def total(detectable: bool) -> dict:
        slots = [s for s in per_fault.values() if s["detectable"] == detectable]
        return {k: sum(s[k] for s in slots) for k in
                ("injected", "caught_by_expected_code", "quarantined_by_other_code_only", "loaded")}

    injected_rows = {e["row"] for e in manifest["entries"]}
    unexplained = sorted(r for r in quarantined if r not in injected_rows and r < manifest["rows_before"])
    return {
        "per_fault": per_fault,
        "detectable_total": total(True),
        "undetectable_total": total(False),
        "quarantined_rows_not_injected": len(unexplained),
        "natural_quarantined_rows": manifest["natural_quarantined_rows"],
        # rows the pipeline rejected that the injector never touched, beyond the ones the original file
        # already had rejected: false positives caused by the injection (0 is the expected value)
        "unexpected_extra_quarantined_rows": len(unexplained) - manifest["natural_quarantined_rows"],
    }


# ----------------------------------------------------------------------------------------- file-level faults

def write_file_faults(source_file: Path, zone_file: Path, out_root: Path, month: date) -> dict[str, str]:
    """Write one broken copy of the file per fault under ``out_root/<fault>/`` in the pipeline's layout."""
    label = month_label(month)
    table = pq.read_table(source_file)
    names = _lower_map(table)
    variants = {
        "RENAMED_REQUIRED_COLUMN": table.rename_columns(
            ["fare_amt" if c == names["fare_amount"] else c for c in table.column_names]),
        "DROPPED_REQUIRED_COLUMN": table.drop_columns([names["payment_type"]]),
        "WRONG_COLUMN_TYPE": table.set_column(
            table.column_names.index(names["fare_amount"]), names["fare_amount"],
            table.column(names["fare_amount"]).cast(pa.string())),
        "EMPTY_FILE": table.slice(0, 0),
        # Not a rejection case: an extra column the contract does not mention must not stop the load.
        "ADDED_UNKNOWN_COLUMN": table.append_column(
            "surge_multiplier", pa.array(np.ones(table.num_rows), pa.float64())),
    }
    written = {}
    for name, variant in variants.items():
        base = out_root / name
        (base / "trip-data").mkdir(parents=True, exist_ok=True)
        (base / "misc").mkdir(parents=True, exist_ok=True)
        pq.write_table(variant, base / "trip-data" / f"yellow_tripdata_{label}.parquet", compression="zstd")
        shutil.copyfile(zone_file, base / "misc" / "taxi_zone_lookup.csv")
        written[name] = str(base)
    return written


# ----------------------------------------------------------------------------------------------- command line

def _zone_ids(zone_file: Path) -> list[int]:
    return [int(z) for z in pd.read_csv(zone_file).iloc[:, 0]]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m taxi_platform.inject")
    sub = parser.add_subparsers(dest="command", required=True)
    mk = sub.add_parser("make", help="write a copy of a month with injected faults and a manifest")
    mk.add_argument("--source", required=True, help="directory holding trip-data/ and misc/")
    mk.add_argument("--month", default="2025-03")
    mk.add_argument("--out", required=True, help="output directory (same layout, plus manifest.json)")
    mk.add_argument("--per-fault", type=int, default=50)
    mk.add_argument("--seed", type=int, default=7)
    mk.add_argument("--workers", type=int, default=1)
    mk.add_argument("--natural-cache", default=None, help="a .npy file to read or write the natural quarantine rows")
    ff = sub.add_parser("file-faults", help="write broken copies of a month's file (schema drift and friends)")
    ff.add_argument("--source", required=True)
    ff.add_argument("--month", default="2025-03")
    ff.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    month = month_start(args.month)
    label = month_label(month)
    source = Path(args.source)
    src_file = source / "trip-data" / f"yellow_tripdata_{label}.parquet"
    zone_file = source / "misc" / "taxi_zone_lookup.csv"
    if args.command == "file-faults":
        print(json.dumps(write_file_faults(src_file, zone_file, Path(args.out), month), indent=2))
        return 0

    table = pq.read_table(src_file)
    zone_ids = _zone_ids(zone_file)
    cache = Path(args.natural_cache) if args.natural_cache else None
    if cache and cache.exists():
        natural = np.load(cache)
    else:
        natural = natural_quarantine(table, month, zone_ids, workers=args.workers)
        if cache:
            np.save(cache, natural)
    injected, manifest = inject(table, month, zone_ids, natural, per_fault=args.per_fault, seed=args.seed)
    out = Path(args.out)
    (out / "trip-data").mkdir(parents=True, exist_ok=True)
    (out / "misc").mkdir(parents=True, exist_ok=True)
    pq.write_table(injected, out / "trip-data" / f"yellow_tripdata_{label}.parquet", compression="zstd")
    shutil.copyfile(zone_file, out / "misc" / "taxi_zone_lookup.csv")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{label}: {manifest['rows_before']} rows -> {manifest['rows_after']}, "
          f"{len(manifest['entries'])} injected across {len(manifest['faults'])} faults, seed {args.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
