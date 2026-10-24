"""The stages of one monthly run: load_zones, fetch, validate, load, reconcile."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from . import db, rules, storage, validate
from .alerts import emit
from .config import Settings, month_label
from .freshness import check_freshness


class ContractViolation(RuntimeError):
    """The file does not match the contract; nothing was loaded."""


class NotPublished(RuntimeError):
    """The source has no file for this month (yet)."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _download(url: str, dest: Path) -> None:
    """Stream ``url`` (http or a local directory path) to ``dest``."""
    if url.startswith(("http://", "https://")):
        last: Exception | None = None
        for attempt in range(3):
            try:
                # Ask for the bytes as stored: the SHA-256 recorded in the ledger must be the file's own, and
                # CloudFront gzips the CSV when the client offers it.
                with requests.get(url, stream=True, timeout=(10, 120),
                                  headers={"Accept-Encoding": "identity"}) as resp:
                    if resp.status_code in (403, 404):
                        raise NotPublished(f"{url} answered HTTP {resp.status_code}")
                    resp.raise_for_status()
                    with dest.open("wb") as fh:
                        for block in resp.iter_content(chunk_size=1 << 20):  # decodes a content-encoding if sent
                            fh.write(block)
                return
            except NotPublished:
                raise
            except Exception as err:  # network trouble: retry, then fail the task
                last = err
                time.sleep(2 * (attempt + 1))
        raise RuntimeError(f"download failed after 3 attempts: {url}: {last}")
    src = Path(url[7:] if url.startswith("file://") else url)
    if not src.exists():
        raise NotPublished(f"{src} does not exist")
    shutil.copyfile(src, dest)


# --------------------------------------------------------------------------- zones

def load_zones(settings: Settings, run_id: str) -> dict:
    """Land the zone lookup and replace raw.taxi_zone_lookup in one transaction."""
    s3 = storage.client(settings)
    with db.timed_stage(settings, run_id, None, "load_zones") as info, tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "taxi_zone_lookup.csv"
        _download(settings.zone_source_url(), path)
        sha = _sha256(path)
        storage.put_file(s3, settings.landing_bucket, "zones/taxi_zone_lookup.csv", path)
        zones = pd.read_csv(path)
        zones.columns = ["locationid", "borough", "zone", "service_zone"]
        table = pa.Table.from_pandas(zones, preserve_index=False)
        with db.connect(settings) as conn:
            conn.execute("TRUNCATE raw.taxi_zone_lookup")
            db.copy_table(conn, "raw.taxi_zone_lookup", table)
        info["rows"] = len(zones)
        info["detail"]["sha256"] = sha
    return {"zones": len(zones), "sha256": sha}


def zone_ids(settings: Settings) -> list[int]:
    with db.connect(settings) as conn:
        ids = [int(r[0]) for r in conn.execute("SELECT locationid FROM raw.taxi_zone_lookup ORDER BY 1")]
    if not ids:
        raise RuntimeError("raw.taxi_zone_lookup is empty; run load-zones first")
    return ids


# --------------------------------------------------------------------------- fetch

def fetch(settings: Settings, month: date, run_id: str, force: bool = False) -> dict:
    """Put the month's raw file in the landing bucket unless it is already there."""
    s3 = storage.client(settings)
    key = settings.landing_key(month)
    meta_key = key + ".meta.json"
    with db.timed_stage(settings, run_id, month, "fetch") as info:
        if not force and storage.exists(s3, settings.landing_bucket, key) \
                and storage.exists(s3, settings.landing_bucket, meta_key):
            meta = storage.get_json(s3, settings.landing_bucket, meta_key)
            info["detail"]["cached"] = True
            info["rows"] = None
            return meta
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "month.parquet"
            url = settings.source_url(month)
            _download(url, path)
            meta = {
                "source_url": url,
                "landing_key": key,
                "file_bytes": path.stat().st_size,
                "file_sha256": _sha256(path),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            storage.put_file(s3, settings.landing_bucket, key, path)
            storage.put_json(s3, settings.landing_bucket, meta_key, meta)
        info["detail"].update(cached=False, file_bytes=meta["file_bytes"])
        return meta


# --------------------------------------------------------------------------- validate

def validate_month(settings: Settings, month: date, run_id: str) -> dict:
    """Validate the landed file; write good rows, quarantined rows and a report to the validated bucket."""
    s3 = storage.client(settings)
    ids = zone_ids(settings)
    prefix = settings.validated_prefix(month)
    with db.timed_stage(settings, run_id, month, "validate") as info, tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        landed = tmp_path / "month.parquet"
        storage.get_file(s3, settings.landing_bucket, settings.landing_key(month), landed)
        meta = storage.get_json(s3, settings.landing_bucket, settings.landing_key(month) + ".meta.json")
        t0 = time.perf_counter()
        table = validate.normalize_table(pq.read_table(landed))
        frame = validate.prepare_frame(table)
        t_prepare = time.perf_counter() - t0
        t0 = time.perf_counter()
        outcome = validate.validate_frame(frame, month, ids, workers=settings.validate_workers)
        t_rules = time.perf_counter() - t0
        del frame
        report = {
            "source_month": month.isoformat(),
            "run_id": run_id,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "landing_key": meta["landing_key"],
            "file_bytes": meta["file_bytes"],
            "file_sha256": meta["file_sha256"],
            "fetched_at": meta["fetched_at"],
            "contract_ok": outcome.contract.ok,
            "contract_failures": outcome.contract.failures,
            "rows_in": outcome.rows_in,
            "rows_loaded": outcome.rows_good,
            "rows_quarantined": outcome.rows_quarantined,
            "rule_rows": outcome.rule_rows,
            "primary_rows": outcome.primary_counts(),
            "warn_counts": outcome.warn_counts,
            "workers": settings.validate_workers,
            "seconds_prepare": round(t_prepare, 3),
            "seconds_rules": round(t_rules, 3),
        }
        info["rows"] = outcome.rows_in
        if not outcome.contract.ok:
            storage.put_json(s3, settings.validated_bucket, f"{prefix}/report.json", report)
            raise ContractViolation(
                f"{month_label(month)}: file contract failed: {', '.join(outcome.contract.failures)}")
        good, quarantine = validate.split_table(table, outcome)
        pq.write_table(good, tmp_path / "good.parquet", compression="zstd")
        pq.write_table(quarantine, tmp_path / "quarantine.parquet", compression="zstd")
        storage.put_file(s3, settings.validated_bucket, f"{prefix}/good.parquet", tmp_path / "good.parquet")
        storage.put_file(s3, settings.validated_bucket, f"{prefix}/quarantine.parquet",
                         tmp_path / "quarantine.parquet")
        storage.put_json(s3, settings.validated_bucket, f"{prefix}/report.json", report)
        info["detail"].update(rows_quarantined=outcome.rows_quarantined, seconds_rules=report["seconds_rules"])
        return report


# --------------------------------------------------------------------------- load

_GOOD_ORDER = ["source_month", "source_row"] + rules.DATA_COLUMNS
_QUARANTINE_ORDER = ["source_month", "source_row", "reason_codes", "primary_reason"] + rules.DATA_COLUMNS


def _ordered(table: pa.Table, month: date, order: list[str]) -> pa.Table:
    month_col = pa.array([month.isoformat()] * table.num_rows, type=pa.string())
    table = table.append_column("source_month", month_col)
    return table.select(order)


def load_month(settings: Settings, month: date, run_id: str) -> dict:
    """Replace the month in one transaction. Running it again for the same month changes nothing."""
    s3 = storage.client(settings)
    prefix = settings.validated_prefix(month)
    with db.timed_stage(settings, run_id, month, "load") as info, tempfile.TemporaryDirectory() as tmp:
        report = storage.get_json(s3, settings.validated_bucket, f"{prefix}/report.json")
        if not report["contract_ok"]:
            raise ContractViolation(f"{month_label(month)}: validated report says the contract failed")
        if report["source_month"] != month.isoformat():
            raise RuntimeError(f"report is for {report['source_month']}, not {month.isoformat()}")
        good_path, q_path = Path(tmp) / "good.parquet", Path(tmp) / "quarantine.parquet"
        storage.get_file(s3, settings.validated_bucket, f"{prefix}/good.parquet", good_path)
        storage.get_file(s3, settings.validated_bucket, f"{prefix}/quarantine.parquet", q_path)
        good = _ordered(pq.read_table(good_path), month, _GOOD_ORDER)
        quarantine = _ordered(pq.read_table(q_path), month, _QUARANTINE_ORDER)
        if good.num_rows != report["rows_loaded"] or quarantine.num_rows != report["rows_quarantined"]:
            raise RuntimeError(
                f"validated files hold {good.num_rows}/{quarantine.num_rows} rows but the report says "
                f"{report['rows_loaded']}/{report['rows_quarantined']}")
        if good.num_rows + quarantine.num_rows != report["rows_in"]:
            raise RuntimeError("rows_in does not equal rows_loaded + rows_quarantined")

        with db.connect(settings) as conn:  # one transaction: commit on exit, roll back on error
            conn.execute(f"SET LOCAL lock_timeout = '{int(settings.load_lock_timeout_s)}s'")
            good_part =db.ensure_partition(conn, "raw.yellow_trips", month)
            q_part = db.ensure_partition(conn, "quarantine.yellow_trips", month)
            conn.execute(f"TRUNCATE {good_part}")
            conn.execute(f"TRUNCATE {q_part}")
            db.copy_table(conn, good_part, good)
            db.copy_table(conn, q_part, quarantine)
            conn.execute("DELETE FROM ops.ingestion_checks WHERE source_month = %s", (month,))
            for code, failed in report["rule_rows"].items():
                conn.execute(
                    "INSERT INTO ops.ingestion_checks (source_month, reason_code, rows_failed, rows_primary)"
                    " VALUES (%s, %s, %s, %s)",
                    (month, code, failed, report["primary_rows"].get(code, 0)))
            conn.execute(
                """
                INSERT INTO ops.ingestion_ledger
                    (source_month, landing_key, file_bytes, file_sha256, fetched_at, rows_in, rows_loaded,
                     rows_quarantined, warn_counts, validated_at, loaded_at, load_count, run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), 1, %s)
                ON CONFLICT (source_month) DO UPDATE SET
                    landing_key = EXCLUDED.landing_key, file_bytes = EXCLUDED.file_bytes,
                    file_sha256 = EXCLUDED.file_sha256, fetched_at = EXCLUDED.fetched_at,
                    rows_in = EXCLUDED.rows_in, rows_loaded = EXCLUDED.rows_loaded,
                    rows_quarantined = EXCLUDED.rows_quarantined, warn_counts = EXCLUDED.warn_counts,
                    validated_at = EXCLUDED.validated_at, loaded_at = now(),
                    load_count = ops.ingestion_ledger.load_count + 1, run_id = EXCLUDED.run_id
                """,
                (month, report["landing_key"], report["file_bytes"], report["file_sha256"], report["fetched_at"],
                 report["rows_in"], report["rows_loaded"], report["rows_quarantined"],
                 json.dumps(report["warn_counts"]), report["validated_at"], run_id))
        info["rows"] = good.num_rows
        info["detail"].update(rows_quarantined=quarantine.num_rows)
        return report


# --------------------------------------------------------------------------- reconcile

def reconcile(settings: Settings, month: date, run_id: str) -> dict:
    """Check the month's numbers agree across the ledger, the raw layer and the marts; run freshness."""
    with db.timed_stage(settings, run_id, month, "reconcile") as info, db.connect(settings) as conn:
        ledger = conn.execute(
            "SELECT rows_in, rows_loaded, rows_quarantined FROM ops.ingestion_ledger WHERE source_month = %s",
            (month,)).fetchone()
        if ledger is None:
            raise RuntimeError(f"no ledger row for {month_label(month)}")
        raw_n = conn.execute("SELECT count(*) FROM raw.yellow_trips WHERE source_month = %s", (month,)).fetchone()[0]
        q_n = conn.execute("SELECT count(*) FROM quarantine.yellow_trips WHERE source_month = %s",
                           (month,)).fetchone()[0]
        fct_n = None
        if conn.execute("SELECT to_regclass('marts.fct_trips')").fetchone()[0] is not None:
            fct_n = conn.execute("SELECT count(*) FROM marts.fct_trips WHERE source_month = %s", (month,)).fetchone()[0]
        problems = []
        if raw_n != ledger[1]:
            problems.append(f"raw has {raw_n} rows, ledger says {ledger[1]} loaded")
        if q_n != ledger[2]:
            problems.append(f"quarantine has {q_n} rows, ledger says {ledger[2]}")
        if ledger[0] != raw_n + q_n:
            problems.append(f"rows_in {ledger[0]} != raw {raw_n} + quarantine {q_n}")
        if fct_n is not None and fct_n != raw_n:
            problems.append(f"fct_trips has {fct_n} rows for the month, raw has {raw_n}")
        findings = check_freshness(conn, settings, through_month=month_label(month))
        for f in findings:
            emit(settings.alert_sink, "freshness", "warning", f.message, code=f.code, source_month=month.isoformat())
        result = {"rows_in": ledger[0], "raw": raw_n, "quarantine": q_n, "fct_trips": fct_n,
                  "freshness": [f.code for f in findings], "problems": problems}
        info["detail"].update(result)
        if problems:
            raise RuntimeError("reconciliation failed: " + "; ".join(problems))
        return result


def run_month(settings: Settings, month: date, run_id: str, with_zones: bool = True) -> dict:
    """All pipeline-side stages in one process (CI and command-line use; Airflow runs them as tasks)."""
    out = {}
    if with_zones:
        out["zones"] = load_zones(settings, run_id)
    out["fetch"] = fetch(settings, month, run_id)
    out["validate"] = validate_month(settings, month, run_id)
    out["load"] = load_month(settings, month, run_id)
    return out

