"""The local alert sink: one JSON object per line, appended to a file.

No external account is involved. Anything that can tail a file (or an Airflow callback) can consume it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def emit(sink: str, kind: str, severity: str, message: str, **fields) -> dict:
    record = {
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        "severity": severity,
        "message": message,
        **fields,
    }
    path = Path(sink)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, default=str, sort_keys=True) + "\n"
    # One write call per record keeps lines whole when two tasks alert at once (O_APPEND).
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)
    return record


def read_all(sink: str) -> list[dict]:
    path = Path(sink)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
