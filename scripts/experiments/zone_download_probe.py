"""How the TLC CDN answers the zone lookup CSV, and whether the pipeline's download copes with it.

    python scripts/experiments/zone_download_probe.py --direct 150 --pipeline 50

Part A sends plain requests with the pipeline's own headers and tallies status, ``x-cache`` and body size. Part B
calls the pipeline's ``_download`` repeatedly against the same URL and records, for every call, the HTTP answers it
had to go through and whether the bytes it kept match the SHA-256 recorded when the source files were first fetched.
Not timed; no lock needed.
"""
from __future__ import annotations

import argparse
import collections
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import RESULTS, ROOT, log, utc_now, write_json  # noqa: E402

sys.path.insert(0, str(ROOT))
from taxi_platform import pipeline  # noqa: E402

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
HEADERS = {"Accept-Encoding": "identity"}


def known_sha() -> str:
    import json

    sources = json.loads((RESULTS / "source-files.json").read_text(encoding="utf-8"))
    return sources["zone_lookup"]["sha256"]


def direct(count: int) -> dict:
    answers, tally = [], collections.Counter()
    for i in range(count):
        t0 = time.perf_counter()
        try:
            resp = requests.get(URL, stream=True, timeout=(10, 120), headers=HEADERS)
            body = b"".join(resp.iter_content(chunk_size=1 << 20))
            row = {"status": resp.status_code, "x_cache": resp.headers.get("x-cache", ""),
                   "content_length_header": resp.headers.get("Content-Length"), "body_bytes": len(body)}
        except Exception as err:  # noqa: BLE001
            row = {"status": None, "error": type(err).__name__, "body_bytes": 0}
        row.update(index=i, at=utc_now(), seconds=round(time.perf_counter() - t0, 3))
        answers.append(row)
        tally[(row["status"], row.get("x_cache", ""), row["body_bytes"])] += 1
        time.sleep(0.3)
    return {
        "requests": count,
        "tally": [{"status": k[0], "x_cache": k[1], "body_bytes": k[2], "count": n} for k, n in tally.most_common()],
        "answers": answers,
    }


def through_pipeline(count: int, expected_sha: str) -> dict:
    seen: list[int] = []
    original = requests.get

    def counting_get(*args, **kwargs):
        resp = original(*args, **kwargs)
        seen.append(resp.status_code)
        return resp

    requests.get = counting_get
    calls = []
    try:
        for i in range(count):
            seen.clear()
            dest = ROOT / "data" / "probe-zone.csv"
            dest.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.perf_counter()
            error = ""
            try:
                pipeline._download(URL, dest)
            except Exception as err:  # noqa: BLE001
                error = f"{type(err).__name__}: {err}"
            sha = pipeline._sha256(dest) if dest.exists() and not error else ""
            calls.append({"index": i, "at": utc_now(), "http_answers": list(seen), "attempts": len(seen),
                          "seconds": round(time.perf_counter() - t0, 2), "error": error,
                          "sha256_matches_first_fetch": sha == expected_sha})
            dest.unlink(missing_ok=True)
            time.sleep(0.3)
    finally:
        requests.get = original
    return {
        "calls": count,
        "calls_that_ended_with_the_right_file": sum(1 for c in calls if c["sha256_matches_first_fetch"]),
        "calls_that_failed": sum(1 for c in calls if c["error"]),
        "calls_that_needed_a_retry": sum(1 for c in calls if c["attempts"] > 1),
        "most_attempts_one_call_needed": max(c["attempts"] for c in calls),
        "detail": calls,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--direct", type=int, default=150)
    parser.add_argument("--pipeline", type=int, default=50)
    parser.add_argument("--out", default=str(RESULTS / "zone-download-probe"))
    args = parser.parse_args()
    started = utc_now()
    expected = known_sha()
    log(f"part A: {args.direct} plain requests")
    part_a = direct(args.direct)
    log(f"part B: {args.pipeline} calls of the pipeline's download")
    part_b = through_pipeline(args.pipeline, expected)
    write_json(Path(args.out) / "result.json", {
        "url": URL, "started_at": started, "ended_at": utc_now(), "expected_sha256": expected,
        "note": "Not timed. What the CDN answers varies over time; this file records what it answered when run.",
        "part_a_plain_requests": part_a, "part_b_pipeline_download": part_b,
    })
    log(f"A: {[(t['status'], t['body_bytes'], t['count']) for t in part_a['tally']]}; "
        f"B: {part_b['calls_that_ended_with_the_right_file']}/{part_b['calls']} right, "
        f"{part_b['calls_that_needed_a_retry']} needed a retry")


if __name__ == "__main__":
    main()
