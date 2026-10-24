"""Rebuild the aggregate marts after the fact table's rows have been physically shuffled, and require that they
come out identical.

    python scripts/check_marts_deterministic.py        # TAXI_PG_DSN names a database that dbt has already built

Floating-point addition depends on the order the rows are visited, and a month that is deleted and inserted
again comes back in a different physical order. Marts that sum double-precision columns therefore differ in the
last digits after a replay even though the data did not change. This script reproduces that on purpose: it
rewrites ``marts.fct_trips`` in random order, runs the two aggregate marts again and compares a hash of every
row before and after.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
DSN = os.environ.get("TAXI_PG_DSN", "postgresql://taxi:taxi@localhost:18232/warehouse")
MARTS = ["marts.mart_zone_hour_demand", "marts.mart_daily_summary"]


def fingerprint(conn: psycopg.Connection) -> dict:
    return {t: conn.execute(f"SELECT count(*), sum(hashtextextended(m::text, 0))::text FROM {t} m").fetchone()
            for t in MARTS}


def main() -> int:
    with psycopg.connect(DSN) as conn:
        before = fingerprint(conn)
        conn.execute("CREATE TEMP TABLE shuffled ON COMMIT DROP AS SELECT * FROM marts.fct_trips ORDER BY random()")
        conn.execute("TRUNCATE marts.fct_trips")
        conn.execute("INSERT INTO marts.fct_trips SELECT * FROM shuffled")
    dbt = [sys.executable, "-m", "dbt.cli.main"] if os.environ.get("DBT_VIA_MODULE") else ["dbt"]
    subprocess.run([*dbt, "run", "--profiles-dir", ".", "--select", "mart_zone_hour_demand", "mart_daily_summary"],
                   cwd=ROOT / "dbt", check=True)
    with psycopg.connect(DSN) as conn:
        after = fingerprint(conn)
    bad = [t for t in MARTS if before[t] != after[t]]
    for table in MARTS:
        print(f"{table}: rows {before[table][0]} -> {after[table][0]}, "
              f"{'identical' if table not in bad else 'DIFFERENT'} after the fact table was rewritten in random order")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
