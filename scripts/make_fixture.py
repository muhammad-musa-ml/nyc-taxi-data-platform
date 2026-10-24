"""Regenerate the small CI fixture from a full TLC month.

The fixture holds REAL rows only: a seeded random sample of the month plus the first few real rows that fail
each row rule when the whole month is validated (so every rule that fires in that month has examples). The
result is written beside the zone lookup in the layout the pipeline's ``TAXI_SOURCE_BASE`` expects:

    tests/fixtures/source/trip-data/yellow_tripdata_<month>.parquet
    tests/fixtures/source/misc/taxi_zone_lookup.csv
    tests/fixtures/source/FIXTURE.json      (what it was cut from, so the cut can be checked)

Usage: python scripts/make_fixture.py --source data/mirror [--month 2025-03] [--rows 4000] [--per-rule 5]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from taxi_platform import validate
from taxi_platform.config import month_label, month_start

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "tests" / "fixtures" / "source"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="directory holding trip-data/ and misc/")
    parser.add_argument("--month", default="2025-03")
    parser.add_argument("--rows", type=int, default=4000, help="size of the random sample")
    parser.add_argument("--per-rule", type=int, default=5, help="real failing rows kept per reason code")
    parser.add_argument("--seed", type=int, default=2025)
    args = parser.parse_args()

    month = month_start(args.month)
    label = month_label(month)
    source = Path(args.source)
    month_file = source / "trip-data" / f"yellow_tripdata_{label}.parquet"
    zone_file = source / "misc" / "taxi_zone_lookup.csv"

    table = pq.read_table(month_file)
    n = table.num_rows
    rng = np.random.default_rng(args.seed)
    chosen = set(rng.choice(n, size=min(args.rows, n), replace=False).tolist())
    n_random = len(chosen)

    # Validate the whole month once to find real rows that fail each rule.
    zone_ids = [int(z) for z in pd.read_csv(zone_file).iloc[:, 0]]
    normal = validate.normalize_table(table)
    outcome = validate.validate_frame(validate.prepare_frame(normal), month, zone_ids, workers=1)
    taken: Counter = Counter()
    added: Counter = Counter()
    for row in sorted(outcome.row_codes):
        code = outcome.row_codes[row][0]
        if taken[code] < args.per_rule:
            taken[code] += 1
            if row not in chosen:
                added[code] += 1
                chosen.add(row)

    picked = sorted(chosen)  # keep the file's own order
    sample = table.take(picked)
    (OUT / "trip-data").mkdir(parents=True, exist_ok=True)
    (OUT / "misc").mkdir(parents=True, exist_ok=True)
    pq.write_table(sample, OUT / "trip-data" / f"yellow_tripdata_{label}.parquet", compression="zstd")
    shutil.copyfile(zone_file, OUT / "misc" / "taxi_zone_lookup.csv")

    local = validate.validate_frame(
        validate.prepare_frame(validate.normalize_table(sample)), month, zone_ids, workers=1)
    info = {
        "month": label,
        "source_file": month_file.name,
        "source_file_sha256": sha256(month_file),
        "source_rows": n,
        "seed": args.seed,
        "random_sample_rows": n_random,
        "failing_rows_added_per_reason_code": dict(sorted(added.items())),
        "fixture_rows": sample.num_rows,
        "fixture_rows_quarantined_by_the_rules": local.rows_quarantined,
        "fixture_primary_reason_counts": dict(sorted(local.primary_counts().items())),
        "note": "Real TLC rows only; no value was edited.",
    }
    (OUT / "FIXTURE.json").write_text(json.dumps(info, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(info, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
