"""After the CDN starts answering 403, how long until it answers 200 again? Poll every 5 s for up to 6 minutes."""
import time

import requests

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
t0 = time.time()
last = None
while time.time() - t0 < 360:
    try:
        r = requests.get(URL, timeout=(10, 60), headers={"Accept-Encoding": "identity"})
        status, size, xc = r.status_code, len(r.content), r.headers.get("x-cache", "")
    except Exception as err:  # noqa: BLE001
        status, size, xc = type(err).__name__, 0, ""
    state = (status, size, xc)
    if state != last:
        print(f"t+{time.time() - t0:6.1f}s {time.strftime('%H:%M:%S', time.gmtime())}Z -> {state}", flush=True)
        last = state
    if status == 200 and size == 12331:
        print("recovered", flush=True)
        break
    time.sleep(5)
