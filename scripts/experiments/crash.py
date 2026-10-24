"""Kill the loader in the middle of its transaction and show the warehouse is left as it was.

    python scripts/experiments/crash.py --month 2025-02 --delays 0 6 12 18 24

Needs the month loaded already. Each trial starts ``taxi-platform load`` in a throw-away container, waits until
PostgreSQL shows the loader's COPY running, waits the given number of extra seconds, then kills the whole
container with SIGKILL. It then checks that PostgreSQL rolled the transaction back (the month's rows, content
hash and ledger entry are exactly what they were), runs the load again, and checks the result is the same.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import RESULTS, compare, compose, connect, log, run, snapshot, utc_now, write_json

CONTAINER = "taxi-crash-trial"


def active_copy(conn) -> dict | None:
    row = conn.execute(
        "SELECT pid, state, left(query, 80), extract(epoch FROM now() - xact_start) FROM pg_stat_activity"
        " WHERE query ILIKE 'COPY %yellow_trips_%' AND state = 'active' AND pid <> pg_backend_pid() LIMIT 1").fetchone()
    return None if row is None else {"pid": row[0], "state": row[1], "query": row[2], "transaction_age_s": float(row[3])}


def still_there(conn, pid: int) -> bool:
    return conn.execute("SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (pid,)).fetchone()[0] > 0


def open_transactions(conn) -> int:
    return conn.execute(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
        " AND state LIKE 'idle in transaction%'").fetchone()[0]


def trial(month: str, delay: float, number: int) -> dict:
    first_day = month + "-01"
    with connect() as conn:
        before = snapshot(conn, [first_day])
    run(["docker", "rm", "-f", CONTAINER], check=False)
    compose("run", "-d", "--no-deps", "--name", CONTAINER, "--entrypoint", "/opt/pipeline-venv/bin/taxi-platform",
            "airflow-scheduler", "load", "--month", month, "--run-id", f"crash-{number}")
    seen, deadline = None, time.time() + 300
    with connect(autocommit=True) as watcher:
        while seen is None and time.time() < deadline:
            seen = active_copy(watcher)
            time.sleep(0.2)
        if seen is None:
            run(["docker", "rm", "-f", CONTAINER], check=False)
            return {"trial": number, "error": "the loader's COPY never appeared"}
        time.sleep(delay)
        at_kill = active_copy(watcher)  # None when the COPY already ended and a later statement is running
        killed_at = utc_now()
        run(["docker", "kill", "--signal=KILL", CONTAINER])
        killed_clock = time.perf_counter()
        run(["docker", "wait", CONTAINER], check=False)
        run(["docker", "rm", "-f", CONTAINER], check=False)
        # PostgreSQL rolls the transaction back when it notices the client is gone. A client that vanished
        # without closing its connection is noticed by the TCP keepalives (30 s idle, 10 s apart, 3 probes).
        while still_there(watcher, seen["pid"]):
            if time.perf_counter() - killed_clock > 600:
                break
            time.sleep(0.5)
        release_s = time.perf_counter() - killed_clock
        if still_there(watcher, seen["pid"]):  # not noticed in 10 minutes: clear it by hand so the run can go on
            watcher.execute("SELECT pg_terminate_backend(%s)", (seen["pid"],))
        leftover = open_transactions(watcher)
    with connect() as conn:
        after_crash = snapshot(conn, [first_day])
    crash_compare = compare(before, after_crash)

    retry = compose("run", "-T", "--rm", "--no-deps", "--entrypoint", "/opt/pipeline-venv/bin/taxi-platform",
                    "airflow-scheduler", "load", "--month", month, "--run-id", f"crash-{number}-retry", check=False)
    with connect() as conn:
        after_retry = snapshot(conn, [first_day])
    retry_compare = compare(before, after_retry)
    return {
        "trial": number, "extra_delay_s": delay, "copy_seen_first": seen, "copy_running_at_kill": at_kill,
        "killed_at": killed_at, "seconds_until_postgres_released_the_dead_loader": round(release_s, 1),
        "released_within_the_wait_limit": release_s <= 600, "open_transactions_left": leftover,
        "state_after_kill_identical_to_before": crash_compare["identical"], "deltas_after_kill": crash_compare["deltas"],
        "load_count_after_kill": after_crash["ledger"][first_day]["load_count"],
        "retry_exit_code": retry.returncode, "state_after_retry_identical_to_before": retry_compare["identical"],
        "deltas_after_retry": retry_compare["deltas"], "load_count_before": before["ledger"][first_day]["load_count"],
        "load_count_after_retry": after_retry["ledger"][first_day]["load_count"],
        "duplicate_keys_after_retry": after_retry["duplicates"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2025-02")
    parser.add_argument("--delays", type=float, nargs="+", default=[0, 6, 12, 18, 24])
    parser.add_argument("--out", default=str(RESULTS / "crash"))
    args = parser.parse_args()
    started = utc_now()
    trials = []
    for i, delay in enumerate(args.delays, start=1):
        result = trial(args.month, delay, i)
        trials.append(result)
        log(f"trial {i}: {result.get('copy_running_at_kill') and 'COPY still running' or 'past the COPY'}, "
            f"identical after kill={result.get('state_after_kill_identical_to_before')}, "
            f"identical after retry={result.get('state_after_retry_identical_to_before')}")
    ok = [t for t in trials if "error" not in t]
    write_json(Path(args.out) / "result.json", {
        "started_at": started, "ended_at": utc_now(), "month": args.month, "trials": trials,
        "trials_killed_mid_copy": sum(1 for t in ok if t["copy_running_at_kill"]),
        "trials_left_unchanged_by_the_kill": sum(1 for t in ok if t["state_after_kill_identical_to_before"]),
        "trials_identical_after_retry": sum(1 for t in ok if t["state_after_retry_identical_to_before"]),
    })


if __name__ == "__main__":
    main()
