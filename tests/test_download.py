"""The download path over HTTP, including a server that gzips what it is allowed to."""
from __future__ import annotations

import gzip
import hashlib
import http.server
import threading

import pytest

from taxi_platform import pipeline

PAYLOAD = (b"LocationID,Borough,Zone,service_zone\n" + b"1,EWR,Newark Airport,EWR\n" * 200)


HITS: dict[str, int] = {}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        HITS[self.path] = HITS.get(self.path, 0) + 1
        if self.path.endswith("missing.csv") or (self.path.endswith("flaky-403.csv") and HITS[self.path] <= 2):
            self.send_response(403 if "403" in self.path else 404)
            self.end_headers()
            return
        # CloudFront was observed answering the zone CSV with HTTP 202 and no body for a stretch of requests.
        if self.path.endswith("always-202.csv") or (self.path.endswith("flaky-202.csv") and HITS[self.path] <= 3):
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path.endswith("empty-200.csv"):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body, headers = PAYLOAD, {}
        if "gzip" in self.headers.get("Accept-Encoding", "") and self.path.endswith("gzip-if-offered.csv"):
            body, headers = gzip.compress(PAYLOAD), {"Content-Encoding": "gzip"}
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):  # keep test output quiet
        pass


@pytest.fixture()
def server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


def test_plain_file_downloads_byte_for_byte(server, tmp_path):
    dest = tmp_path / "out.csv"
    pipeline._download(f"{server}/plain.csv", dest)
    assert dest.read_bytes() == PAYLOAD


def test_server_that_gzips_when_offered_still_yields_the_original_bytes(server, tmp_path):
    dest = tmp_path / "out.csv"
    pipeline._download(f"{server}/gzip-if-offered.csv", dest)
    assert dest.read_bytes() == PAYLOAD
    assert pipeline._sha256(dest) == hashlib.sha256(PAYLOAD).hexdigest()


def test_http_404_means_not_published_and_is_not_retried(server, tmp_path):
    with pytest.raises(pipeline.NotPublished):
        pipeline._download(f"{server}/missing.csv", tmp_path / "out.csv")


@pytest.fixture()
def no_sleep(monkeypatch):
    delays: list[float] = []
    monkeypatch.setattr(pipeline.time, "sleep", delays.append)
    HITS.clear()
    return delays


def test_http_202_with_an_empty_body_is_retried_until_the_file_arrives(server, tmp_path, no_sleep):
    dest = tmp_path / "out.csv"
    pipeline._download(f"{server}/flaky-202.csv", dest)
    assert dest.read_bytes() == PAYLOAD
    assert HITS["/flaky-202.csv"] == 4  # three empty answers, then the file
    assert no_sleep == [2, 4, 8]  # backs off between attempts


def test_http_202_that_never_clears_fails_the_download_instead_of_writing_an_empty_file(server, tmp_path, no_sleep):
    with pytest.raises(RuntimeError, match="download failed after 6 attempts"):
        pipeline._download(f"{server}/always-202.csv", tmp_path / "out.csv")
    assert HITS["/always-202.csv"] == 6
    assert len(no_sleep) == 5  # no sleep after the last attempt


def test_a_required_file_retries_a_403_but_a_month_file_reports_not_published_at_once(server, tmp_path, no_sleep):
    dest = tmp_path / "out.csv"
    with pytest.raises(pipeline.NotPublished):
        pipeline._download(f"{server}/flaky-403.csv", dest)  # a month file: 403 means not published
    assert HITS["/flaky-403.csv"] == 1 and not no_sleep
    HITS.clear()
    pipeline._download(f"{server}/flaky-403.csv", dest, required=True)  # the zone file: it exists, so retry
    assert dest.read_bytes() == PAYLOAD
    assert HITS["/flaky-403.csv"] == 3 and no_sleep == [2, 4]


def test_a_required_file_that_stays_missing_fails_after_every_attempt(server, tmp_path, no_sleep):
    with pytest.raises(RuntimeError, match="download failed after 6 attempts"):
        pipeline._download(f"{server}/missing.csv", tmp_path / "out.csv", required=True)
    assert HITS["/missing.csv"] == 6


def test_an_empty_200_answer_is_not_accepted_as_the_file(server, tmp_path, no_sleep):
    with pytest.raises(RuntimeError, match="empty"):
        pipeline._download(f"{server}/empty-200.csv", tmp_path / "out.csv")


def test_local_directory_source_copies_the_file(tmp_path):
    src = tmp_path / "src.csv"
    src.write_bytes(PAYLOAD)
    dest = tmp_path / "out.csv"
    pipeline._download(str(src), dest)
    assert dest.read_bytes() == PAYLOAD
    with pytest.raises(pipeline.NotPublished):
        pipeline._download(str(tmp_path / "absent.csv"), tmp_path / "x.csv")
