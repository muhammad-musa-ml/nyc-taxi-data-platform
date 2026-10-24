"""Request the zone CSV repeatedly with the pipeline's own headers and tally the HTTP answers."""
import collections
import sys
import time

import requests

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
tally = collections.Counter()
slow = []
for i in range(N):
    t0 = time.perf_counter()
    try:
        r = requests.get(URL, stream=True, timeout=(10, 120), headers={"Accept-Encoding": "identity"})
        body = b"".join(r.iter_content(chunk_size=1 << 20))
        key = f"{r.status_code} bytes={len(body)} server={r.headers.get('x-cache', '')[:20]}"
    except Exception as err:  # noqa: BLE001
        key = f"EXC {type(err).__name__}"
    dt = time.perf_counter() - t0
    tally[key] += 1
    if not key.startswith("200 bytes=12331"):
        print(f"{i}: {key} in {dt:.2f}s", flush=True)
    time.sleep(0.3)
for key, n in tally.most_common():
    print(n, key)
