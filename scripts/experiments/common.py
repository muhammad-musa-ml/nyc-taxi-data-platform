"""Shared pieces of the measurement scripts: the machine lock, the environment record, Compose helpers and
state snapshots of the warehouse.

Every figure in ``results/`` comes from one of the scripts in this folder; each writes its raw output
(JSON, logs, the container list) next to the figures derived from it.
"""
from __future__ import annotations

import contextlib
import json
import os
import platform
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "results"
PG_HOST_DSN = os.environ.get("TAXI_HOST_PG_DSN", "postgresql://taxi:taxi@localhost:18232/warehouse")
SCHEDULER = "airflow-scheduler"
PIPELINE_BIN = "/opt/pipeline-venv/bin/taxi-platform"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"{utc_now()} {message}", flush=True)


# ------------------------------------------------------------------------------------------------ lock

@contextlib.contextmanager
def measurement_lock(label: str, poll_seconds: float = 2.0):
    """Hold a machine-wide lock file while a timed batch runs.

    The path comes from TAXI_MEASURE_LOCK. The file is created exclusively, so only one batch holds it;
    anyone else waits. With the variable unset there is no lock (a machine used by one project).
    """
    path = os.environ.get("TAXI_MEASURE_LOCK")
    if not path:
        yield
        return
    waited = 0.0
    while True:
        try:
            with open(path, "x", encoding="utf-8") as fh:
                fh.write(f"{label} {utc_now()}\n")
            break
        except FileExistsError:
            if waited == 0:
                log(f"lock {path} is held; waiting")
            time.sleep(poll_seconds)
            waited += poll_seconds
    log(f"lock acquired after waiting {waited:.0f} s: {label}")
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        log("lock released")


# ------------------------------------------------------------------------------------------------ shell

def run(args: list[str], *, check: bool = True, capture: bool = True, env: dict | None = None,
        stdin_text: str | None = None, timeout: float | None = None) -> subprocess.CompletedProcess:
    full_env = {**os.environ, **(env or {})}
    # No inherited stdin: an attached `docker compose run` would otherwise wait on it forever.
    stdin = {"input": stdin_text} if stdin_text is not None else {"stdin": subprocess.DEVNULL}
    return subprocess.run(args, cwd=ROOT, check=check, capture_output=capture, text=True, env=full_env,
                          timeout=timeout, encoding="utf-8", errors="replace", **stdin)


def compose(*args: str, check: bool = True, env: dict | None = None, timeout: float | None = None):
    return run(["docker", "compose", *args], check=check, env=env, timeout=timeout)


def in_scheduler(*command: str, env: dict | None = None, check: bool = True, timeout: float | None = None):
    """Run a command in the scheduler container, optionally with extra environment variables."""
    flags = [x for k, v in (env or {}).items() for x in ("-e", f"{k}={v}")]
    return compose("exec", "-T", *flags, SCHEDULER, *command, check=check, timeout=timeout)


def pipeline_cli(*args: str, env: dict | None = None, check: bool = True, timeout: float | None = None):
    return in_scheduler(PIPELINE_BIN, *args, env=env, check=check, timeout=timeout)


def wait_healthy(services: tuple[str, ...] = ("postgres", "airflow-scheduler", "airflow-webserver"),
                 timeout: float = 600.0) -> None:
    deadline = time.time() + timeout
    for service in services:
        container = compose("ps", "-q", service).stdout.strip()
        while True:
            status = run(["docker", "inspect", "-f", "{{.State.Health.Status}}", container]).stdout.strip()
            if status == "healthy":
                break
            if time.time() > deadline:
                raise TimeoutError(f"{service} is {status} after {timeout:.0f} s")
            time.sleep(3)


def connect(dsn: str | None = None, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(dsn or PG_HOST_DSN, autocommit=autocommit)


# ------------------------------------------------------------------------------------------ environment

def tool_versions() -> dict:
    """Versions as seen inside the scheduler container, where the pipeline runs."""
    out: dict = {}
    freeze = in_scheduler("/opt/pipeline-venv/bin/pip", "freeze", check=False).stdout.splitlines()
    wanted = ("pandas==", "pyarrow==", "great_expectations==", "great-expectations==", "dbt-core==",
              "dbt-postgres==", "psycopg", "boto3==")
    out["pipeline_venv"] = sorted(line for line in freeze if line.lower().startswith(wanted))
    out["airflow"] = in_scheduler("airflow", "version", check=False).stdout.strip().splitlines()[-1:]
    out["python"] = in_scheduler("/opt/pipeline-venv/bin/python", "--version", check=False).stdout.strip()
    with connect() as conn:
        out["postgres"] = conn.execute("SHOW server_version").fetchone()[0]
    images = run(["docker", "compose", "images", "--format", "json"], check=False).stdout
    out["compose_images"] = images.strip().splitlines()
    return out


def sanitize_ps(table: str, own_prefix: str = "taxi-") -> str:
    """Keep this stack's containers by name; reduce every other container to its status only."""
    lines, others = [], 0
    for line in table.splitlines():
        if not line.strip() or line.startswith(("NAMES", own_prefix)):
            lines.append(line)
            continue
        others += 1
        lines.append("(other container)".ljust(41) + re.split(r"\s{2,}", line.strip())[-1])
    lines.append(f"# {others} containers outside this stack were running; their names and images are withheld")
    return "\n".join(lines) + "\n"


def record_environment(out_dir: Path, note: str = "", prefix: str = "") -> None:
    """The conditions a timed batch ran under: the container list, machine and tool versions.

    The container list in the results folder names only this stack's containers. If TAXI_FULL_PS_DIR is set the
    complete list is also written there (outside the repository).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ps = run(["docker", "ps", "--format", "table {{.Names}}\t{{.Image}}\t{{.Status}}"]).stdout
    header = f"# captured {utc_now()} {note}\n"
    (out_dir / f"{prefix}docker-ps.txt").write_text(header + sanitize_ps(ps), encoding="utf-8")
    private = os.environ.get("TAXI_FULL_PS_DIR")
    if private:
        target = Path(private) / (out_dir.relative_to(RESULTS) if out_dir.is_relative_to(RESULTS) else out_dir.name)
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{prefix}docker-ps-full.txt").write_text(header + ps, encoding="utf-8")
    info = run(["docker", "info", "--format", "{{.NCPU}} CPUs, {{.MemTotal}} bytes memory, server {{.ServerVersion}}"],
               check=False).stdout.strip()
    head = run(["git", "rev-parse", "HEAD"], check=False).stdout.strip()
    dirty = run(["git", "status", "--porcelain", "--", "taxi_platform", "dags", "dbt", "docker-compose.yml"],
                check=False).stdout.strip()
    env = {
        "captured_at": utc_now(), "note": note,
        "code": {"git_head": head, "uncommitted_changes_to_pipeline_dags_dbt_or_compose": bool(dirty)},
        "host": {"os": platform.platform(), "python": sys.version.split()[0], "docker": info,
                 "cpu_count_host": os.cpu_count()},
        "tools": tool_versions(),
    }
    (out_dir / f"{prefix}environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")


def rounded(obj, digits: int = 4):
    """Floats rounded to ``digits`` decimals (a timing is never meaningful past a tenth of a millisecond)."""
    if isinstance(obj, float):
        return round(obj, digits)
    if isinstance(obj, dict):
        return {k: rounded(v, digits) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [rounded(v, digits) for v in obj]
    return obj


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rounded(obj), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


# ------------------------------------------------------------------------------------------------ state

TABLES = ["raw.yellow_trips", "quarantine.yellow_trips", "marts.fct_trips", "marts.mart_zone_hour_demand",
          "marts.mart_daily_summary"]


def snapshot(conn: psycopg.Connection, months: list[str] | None = None) -> dict:
    """Row counts and an order-independent content hash per table and month, key duplicates, the ledger."""
    where = ""
    params: tuple = ()
    if months:
        where, params = " WHERE source_month = ANY(%s::date[])", (months,)
    out: dict = {"tables": {}, "duplicates": {}}
    for table in TABLES:
        if conn.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0] is None:
            continue
        rows = conn.execute(
            f"SELECT source_month, count(*), sum(hashtextextended(t::text, 0))::text FROM {table} t{where}"
            " GROUP BY 1 ORDER BY 1", params).fetchall()
        out["tables"][table] = {m.isoformat(): {"rows": n, "content_hash": h} for m, n, h in rows}
    for table in ("raw.yellow_trips", "quarantine.yellow_trips"):
        out["duplicates"][table] = conn.execute(
            f"SELECT count(*) FROM (SELECT 1 FROM {table}{where} GROUP BY source_month, source_row HAVING count(*) > 1) d",
            params).fetchone()[0]
    if conn.execute("SELECT to_regclass('marts.fct_trips')").fetchone()[0] is not None:
        out["duplicates"]["marts.fct_trips.trip_id"] = conn.execute(
            f"SELECT count(*) - count(DISTINCT trip_id) FROM marts.fct_trips{where}", params).fetchone()[0]
    out["ledger"] = {
        m.isoformat(): {"rows_in": a, "rows_loaded": b, "rows_quarantined": c, "file_sha256": d, "file_bytes": e,
                        "load_count": f}
        for m, a, b, c, d, e, f in conn.execute(
            f"SELECT source_month, rows_in, rows_loaded, rows_quarantined, file_sha256, file_bytes, load_count"
            f" FROM ops.ingestion_ledger{where} ORDER BY 1", params)}
    out["checks"] = {}
    for m, code, failed, primary in conn.execute(
            f"SELECT source_month, reason_code, rows_failed, rows_primary FROM ops.ingestion_checks{where}"
            " ORDER BY 1, 2", params):
        out["checks"].setdefault(m.isoformat(), {})[code] = {"rows_failed": failed, "rows_primary": primary}
    return out


def compare(before: dict, after: dict) -> dict:
    """What changed between two snapshots. ``load_count`` is expected to move; nothing else is."""
    deltas = []
    for table, months in after["tables"].items():
        for month, now in months.items():
            was = before["tables"].get(table, {}).get(month)
            if was is None:
                deltas.append({"table": table, "month": month, "change": "appeared", "rows": now["rows"]})
            elif was["rows"] != now["rows"] or was["content_hash"] != now["content_hash"]:
                deltas.append({"table": table, "month": month, "row_delta": now["rows"] - was["rows"],
                               "content_changed": was["content_hash"] != now["content_hash"]})
    for table, months in before["tables"].items():
        for month in months:
            if month not in after["tables"].get(table, {}):
                deltas.append({"table": table, "month": month, "change": "disappeared"})
    for month, now in after["ledger"].items():
        was = before["ledger"].get(month, {})
        for key in ("rows_in", "rows_loaded", "rows_quarantined", "file_sha256", "file_bytes"):
            if was.get(key) != now[key]:
                deltas.append({"ledger": month, "field": key, "before": was.get(key), "after": now[key]})
    if before["checks"] != after["checks"]:
        deltas.append({"checks_changed": True})
    return {"deltas": deltas, "duplicate_keys_after": after["duplicates"], "identical": not deltas}
