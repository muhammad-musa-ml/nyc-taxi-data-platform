"""The download path over HTTP, including a server that gzips what it is allowed to."""
from __future__ import annotations

import gzip
import hashlib
import http.server
import threading

import pytest

from taxi_platform import pipeline

PAYLOAD = (b"LocationID,Borough,Zone,service_zone\n" + b"1,EWR,Newark Airport,EWR\n" * 200)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path.endswith("missing.csv"):
            self.send_response(404)
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


def test_local_directory_source_copies_the_file(tmp_path):
    src = tmp_path / "src.csv"
    src.write_bytes(PAYLOAD)
    dest = tmp_path / "out.csv"
    pipeline._download(str(src), dest)
    assert dest.read_bytes() == PAYLOAD
    with pytest.raises(pipeline.NotPublished):
        pipeline._download(str(tmp_path / "absent.csv"), tmp_path / "x.csv")
