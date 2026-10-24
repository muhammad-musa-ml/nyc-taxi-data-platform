"""PostgreSQL access: schema creation, month partitions, bulk COPY and stage timing."""
from __future__ import annotations

import io
import json
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from importlib import resources

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv

from .config import Settings, month_label


def connect(settings: Settings, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn, autocommit=autocommit)


def init_db(settings: Settings) -> None:
    sql = resources.files("taxi_platform").joinpath("schema.sql").read_text(encoding="utf-8")
    with connect(settings) as conn:
        conn.execute(sql)


def partition_name(table: str, month: date) -> str:
    schema, name = table.split(".")
    return f"{schema}.{name}_{month.year:04d}_{month.month:02d}"


def ensure_partition(conn: psycopg.Connection, table: str, month: date) -> str:
    part = partition_name(table, month)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {part} PARTITION OF {table} FOR VALUES IN ('{month.isoformat()}')")
    return part


_CSV_OPTIONS = pacsv.WriteOptions(include_header=False, quoting_style="needed")


def copy_table(conn: psycopg.Connection, target: str, table: pa.Table, chunk_rows: int = 250_000) -> int:
    """COPY an Arrow table into ``target`` as CSV; column order and names must match the table."""
    columns = ", ".join(table.column_names)
    with conn.cursor() as cur, cur.copy(f"COPY {target} ({columns}) FROM STDIN WITH (FORMAT csv)") as copy:
        for batch in table.to_batches(max_chunksize=chunk_rows):
            buf = io.BytesIO()
            pacsv.write_csv(pa.Table.from_batches([batch]), buf, _CSV_OPTIONS)
            copy.write(buf.getvalue())
    return table.num_rows


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def timed_stage(settings: Settings, run_id: str, month: date | None, stage: str, detail: dict | None = None):
    """Record one row in ops.stage_timings when the block ends, whether it succeeds or fails."""
    started = utcnow()
    t0 = time.perf_counter()
    info: dict = {"rows": None, "detail": dict(detail or {})}
    status = "ok"
    try:
        yield info
    except BaseException:
        status = "failed"
        raise
    finally:
        seconds = time.perf_counter() - t0
        info["detail"]["status"] = status
        try:
            with connect(settings, autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO ops.stage_timings"
                    " (run_id, source_month, stage, started_at, finished_at, seconds, rows, detail)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (run_id, month, stage, started, utcnow(), round(seconds, 3), info["rows"],
                     json.dumps(info["detail"], default=str)))
        except Exception:  # timing is evidence, never a reason to fail a stage
            pass


def month_value(month: date) -> str:
    return month_label(month)
