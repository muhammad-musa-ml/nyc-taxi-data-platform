# What running it for real turned up

Each of these was found by running the platform, not by reading it. All were fixed, and each fix has a
test that fails on the old code (checked by putting the old code back and running the test).

## 1. The zone lookup arrived gzip-compressed

The first run through Airflow failed in `load_zones` with `UnicodeDecodeError: 'utf-8' codec can't decode byte
0x8b`. CloudFront serves the CSV gzip-compressed to any client that offers `Accept-Encoding: gzip`, which
`requests` does by default, and the download copied the raw socket stream, so the compressed bytes landed
in the bucket. Every earlier run had read the files from a local copy, so this path had never been exercised.
The download now asks for the bytes as stored (`Accept-Encoding: identity`) and writes them through
`iter_content`, which decodes a content-encoding if one is sent anyway; the SHA-256 in the ledger is therefore
the published file's own. `tests/test_download.py` serves the same file from a local HTTP server that gzips
when asked.

## 2. dbt inside the container looked for PostgreSQL on localhost

`dbt_run` failed with `connection to server at "localhost" ... Connection refused`: the dbt profile defaults to
the host-published port, which only exists outside Compose. The Compose environment now sets `DBT_HOST` and
`DBT_PORT`. Nothing in the unit tests could have shown this; the DAG test only checks that the file imports.

## 3. A text column where numbers belong crashed the validator

Writing the injector's broken files (a renamed column, a dropped column, a wrong type, no rows) showed that
a file whose `fare_amount` was text did not fail the file contract as intended: deriving the minimum of fare
and total raised a `TypeError` first, so the task failed as an ordinary error and would have been retried.
The derived columns are now computed only when every column has a numeric or timestamp type; otherwise the
contract names the offending column (`TYPE_fare_amount`) and the task fails once, without a retry. The
optional `cbd_congestion_fee` column is now part of the type contract too. `tests/test_validation_rules.py`
covers four columns; `tests/test_integration.py` runs all four broken-file cases through the real stages.

## 4. A killed loader leaves its locks until PostgreSQL notices

In an early kill drill, the second of two SIGKILLs left the loader's backend waiting on a client that no longer
existed (`wait_event = ClientRead`), still holding the locks it took when it truncated the month's partition.
Nothing in the warehouse was half-written, but anything that touched the partition, a retry of the load
included, queued behind it: my own verification query was still waiting almost seven minutes later, and I
cleared the backend by hand. The server's keepalive settings were at their default of 0, which means the
operating system's default (about two hours on Linux). The first kill had been cleaned up within a couple of
seconds, so the stuck state is intermittent and I could not make it happen on demand. Two changes bound it:
PostgreSQL now runs with `tcp_keepalives_idle=30`, `tcp_keepalives_interval=10`, `tcp_keepalives_count=3` (a dead
client is dropped within about a minute), and the load sets `lock_timeout` (180 s, `TAXI_LOAD_LOCK_TIMEOUT_S`) so a
task fails instead of hanging and its retry runs after the cleanup. `tests/test_integration.py` holds an exclusive
lock from a second connection and checks the load gives up and succeeds afterwards. The kill drill in the README
was run after these changes; it times how long PostgreSQL took to release each killed loader.

## 5. The alert sink loses records on a Windows host

The alert sink appends one JSON line per write with `O_APPEND`, which is atomic for a single write on Linux. A
concurrency test with six threads on the Windows development machine kept 134 of 150 records; the same test on
Linux keeps all of them. The stack runs in Linux containers, so nothing changes there, but the test is skipped
on Windows with that reason rather than made to pass.

## 6. The first replay was not identical: floating-point sums depend on row order

The first full run (kept in `results/before-mart-fix/`) was followed by a replay of all six months. The raw
layer, the quarantine layer, `fct_trips`, the ledger and the check counts came back identical, row for row and
by content hash, with every load count at 2. The two aggregate marts did not: `mart_zone_hour_demand` and
`mart_daily_summary` had the same number of rows but a different content hash in all six months (12 of 12
tables-and-months in `replay.json`).

The marts summed and averaged `double precision` columns. Deleting a month and inserting it again returns its
rows in a different physical order, and floating-point addition is not associative. Aggregating one month of the
unchanged table with parallel workers and then without, or over the rows in reverse order, changed the daily
revenue by up to 6.1e-14 (relative) and the average speed by up to 3.4e-14, on 31 of 31 days
(`results/before-mart-fix/fp-order-diagnosis.txt`). The data had not changed; the arithmetic had.

The marts now aggregate over `numeric` (exact sums) and round to the precision the inputs carry (cents for
amounts). `scripts/check_marts_deterministic.py` rewrites `fct_trips` in random order, rebuilds the marts and
requires identical content; it fails on the old SQL (both marts differ) and passes on the new, and CI runs it
on every push. All official runs in `results/backfill/` were redone with the fixed marts.

## 7. The zone lookup download failed on some re-runs: the CDN sometimes does not answer with the file

The first idempotency experiment re-ran every month five times (30 re-runs, `results/idempotency-first-attempt/`).
Three of them (2025-03, re-runs 3 to 5, about a minute apart) exited with status 1 at `load_zones` after 0.14 to
0.44 s; the warehouse stayed identical to its state before the re-run each time, and a later single run of the
same month passed. I had not captured the error text, so the cause of those three is not directly observed.
Requesting the same URL directly, with the headers the pipeline uses, produced two CDN behaviours that fit it:

- **HTTP 202 with an empty body** (`x-cache: Error from cloudfront`): 23 of 150 requests, the first 23 in a row,
  then 127 plain 200s (`results/zone-download-probe/202-episode/`). The download only raised on 4xx/5xx, so it
  wrote a zero-byte file and `pandas.read_csv` failed about 0.2 s later.
- **HTTP 403 for every request for a minute or more**, after about 170 requests in 75 s from one address: 30
  consecutive calls of the pipeline's download failed (`results/zone-download-probe/run-1-with-the-202-fix-only/`).
  A poll every 5 s afterwards still got 403 at the first poll and 200 from the next, between 55 and 81 s after the
  episode began. The pipeline reads 403 and 404 as "this month is not published" (the CDN's storage origin answers 403
  for a missing key), so that call raised `NotPublished` straight away.

A re-run asked the CDN for a file it already held, which is why a quiet minute was enough to turn an idempotent
operation into a failing one. Three changes:

1. `_download` accepts only a 200 with a non-empty body whose length matches `Content-Length`; anything else is
   retried (six attempts, waiting 2, 4, 8, 16 and 32 s).
2. The zone lookup is a `required` file, so there 403 and 404 are retried instead of meaning "not published";
   month files keep that meaning (a 403 on a month file during a throttling episode would still be reported as
   "not published": the two cannot be told apart from the response).
3. If the download still fails and an earlier run landed `zones/taxi_zone_lookup.csv`, `load_zones` uses that copy,
   records `source = landed copy` and the download error in `ops.stage_timings`, and writes a `zone_download_fallback`
   warning to the alert sink. A first run with no landed copy still fails.

`tests/test_download.py` serves 202, 403, 404 and empty-200 answers from a local HTTP server and fails on the old
code (3 of the new tests); `tests/test_integration.py` covers the fallback with and without a landed copy. A second
probe on the fixed code (`run-2-final-code/`) saw no throttling at all (100 plain requests and 100 calls of the
pipeline's download, all 200), so it shows the happy path is unchanged, not the retry path; the retry path is shown
by the tests. The idempotency experiment was then rerun on the fixed code.

## 8. Two backfills of the same range collide

Starting a second `airflow dags backfill` for months that another backfill is already running makes both fail with
"task instance ... finished with state success, but the task instance's state attribute is running" (Airflow's
"task state changed externally"). Run one backfill at a time; `scripts/backfill.sh` is a thin wrapper and does
not guard against it.
