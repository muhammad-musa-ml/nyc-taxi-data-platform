# nyc-taxi-data-platform

A batch data platform on real New York City taxi trip records, built to run on a laptop for $0: an Airflow
DAG loads one month at a time into PostgreSQL, every row is checked at ingestion, rows that fail are kept
in a quarantine table under machine-readable reason codes, dbt builds tested models and an analyst mart on
top, and any month, or all of them, can be replayed without changing a row count.

I built it to practise the parts of data engineering that make a pipeline trustworthy rather than merely
working: loads that are safe to run twice, a precise definition of a bad row, alerts that say what failed,
and measurements of whether the checks catch what they should.

**The data is real; only the injected faults are synthetic.** The trips are the NYC Taxi and Limousine
Commission (TLC) yellow-taxi records for January to June 2025, downloaded from the TLC's public site on
2026-10-24 (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), plus its taxi zone lookup. The
only invented data is the set of bad rows that `taxi_platform/inject.py` writes into real rows to test the
checks; every place they appear says so.

## What it does

```
TLC CloudFront ─fetch─> S3 landing/ ─validate (Great Expectations)─> S3 validated/<month>/{good,quarantine}.parquet
                                                                              │
                          Airflow: one DAG run per month                     load (one transaction)
                                                                              ▼
                                  PostgreSQL 16: raw.yellow_trips (partition per month), quarantine.yellow_trips,
                                                 ops.ingestion_ledger, ops.ingestion_checks, ops.stage_timings
                                                                              │
                                                       dbt: staging ─> intermediate ─> marts (+ 32 tests)
                                                                              │
                                              failure callbacks and a freshness check ─> alert sink (JSON lines)
```

The monthly DAG `taxi_monthly` has seven tasks, each of which calls the `taxi-platform` command line (or dbt)
in a separate virtual environment inside the Airflow image:

| task | what it does |
|---|---|
| `load_zones` | lands the zone lookup and replaces `raw.taxi_zone_lookup` |
| `fetch_to_landing` | downloads the month's Parquet file into the landing bucket unless it is already there, with its SHA-256 |
| `validate_with_great_expectations` | checks the file's layout, then 23 row expectations under 21 reason codes; writes good rows, quarantined rows and a report |
| `load_warehouse` | replaces the month in `raw` and `quarantine` and writes the ledger row, in one transaction |
| `dbt_run`, `dbt_test` | rebuild the month's slice of the incremental models, then test it |
| `reconcile_and_check_freshness` | checks that ledger, raw layer and fact table agree, then runs the freshness check |

A second DAG, `taxi_freshness`, runs the same freshness check daily.

| service | port |
|---|---|
| Airflow UI (user `admin`, password `admin`) | 18280 |
| PostgreSQL 16 (user `taxi`, password `taxi`) | 18232 |
| S3 API (SeaweedFS; key `taxi-local-key`) | 18290 |

These credentials are local development values for containers that are not reachable from outside the machine.
Why PostgreSQL and not DuckDB, why tasks call a command line, and what the object store is:
[docs/design.md](docs/design.md).

## Results

Every figure here is copied from [results/SUMMARY.md](results/SUMMARY.md), which `scripts/experiments/summarize.py`
computes from the raw files in [`results/`](results/); [docs/measurements.md](docs/measurements.md) says how each one
was taken and on what. Medians, with the range and the number of runs (n) in brackets. Everything ran on one shared
laptop (see [Limits](#limits)).

**Rows.** The six months hold 24,083,384 trips. 22,750,875 were loaded and 1,332,509 (5.53%) were quarantined; the
counts were identical in all 5 backfills. NEGATIVE_AMOUNT is the largest group in every month, from 144,439 rows in
January to 325,804 in May (a row can fail several rules, and each rule's count is kept).

| month | rows in | quarantined | share | DAG run | validate | load | dbt run | dbt test |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2025-01 | 3,475,226 | 145,476 | 4.19% | 140.9 s | 25.6 s | 21.2 s | 54.7 s | 11.9 s |
| 2025-02 | 3,577,543 | 183,926 | 5.14% | 160.8 s | 26.3 s | 23.2 s | 71.7 s | 12.5 s |
| 2025-03 | 4,145,257 | 210,175 | 5.07% | 201.3 s | 28.1 s | 24.6 s | 80.3 s | 25.7 s |
| 2025-04 | 3,970,553 | 188,184 | 4.74% | 211.5 s | 29.1 s | 25.9 s | 84.9 s | 44 s |
| 2025-05 | 4,591,845 | 327,068 | 7.12% | 231.9 s | 34.8 s | 28 s | 92 s | 49.9 s |
| 2025-06 | 4,322,960 | 277,680 | 6.42% | 235.9 s | 32 s | 26.4 s | 92.5 s | 53.4 s |

**Backfill.** Starting from an empty stack and downloading all six files from the TLC site, the whole range took
1177 s (1162-1275, n=5). The table's times are medians over those 5 runs, per task; dbt's run is the longest task in
every month. A replay of the whole range afterwards took 1341.1 s (n=1), exited 0 and left every row count and
content hash of the raw, quarantine and fact tables and both marts unchanged (ledger load count 2 per month).
The downloaded files matched an independent `curl` download by SHA-256 and size in 5 of 5 runs.

**Re-running a loaded month.** 30 re-runs (5 per month): 0 changed any row count, content hash or ledger count, and
0 left a duplicate key. The first attempt at this experiment had 3 of 30 re-runs exit with status 1 (2025-03,
re-runs 3 to 5); the warehouse was identical after each. They failed at the step that downloads the zone lookup, and I
had not kept the error text. Probing that URL afterwards showed the CDN answering with no file, which the download
mishandled; the fix and the evidence are in [docs/findings.md](docs/findings.md) (section 7). The 30 above are the
rerun on the fixed code, in which the CDN did not misbehave, so the fix itself is shown by its tests and not by this run.

**Killing the loader.** Five times the loader's container was SIGKILLed while PostgreSQL showed its `COPY` running on
2025-02: the warehouse was identical to before in 5 of 5, and identical after running the load again in 5 of 5.
PostgreSQL released the dead loader's locks after 60.7 s (0.4-60.7, n=5): 0.4 s when the kill came 0 or 3 s into
the `COPY`, 60.7 s when it came 6, 9 or 12 s in.

**Catching bad rows.** Real March rows that had passed every rule were changed to carry each of 25 kinds of fault, 200
rows per kind, and the whole pipeline was run on the result, 5 times with different seeds. For the 23 kinds a rule
should see: 4600 rows injected per run, 4600 caught by the expected reason code (4600-4600, n=5), 0 caught only by
another code, 0 loaded. For the 2 kinds no rule can see (a fare raised to another plausible fare, and a pickup zone
replaced by another valid zone): 400 injected per run and 400 loaded. The pipeline rejected 0 rows beyond the file's
own rejects and the injected ones. The table by fault is in [results/SUMMARY.md](results/SUMMARY.md). The injected
faults are synthetic and I wrote them and the rules, so a perfect score here shows the rules do what they say, not that
they would catch every real defect.

**Alerts.** Four broken files (a renamed column, a dropped column, a wrong column type, no rows) were offered to the
monthly DAG, 5 times each. Every time the validate task failed in a single attempt, the later tasks did not run, nothing was
loaded, and the sink received one task-failure record and one DAG-failure record. The record appeared 0.0014 to
0.0016 s after the task ended (medians per fault) and the failure was detected 5.2 to 13.8 s after the task started
(medians per fault). The freshness DAG found nothing on six loaded months; expecting a seventh month it reported
LATEST_MONTH_BEHIND and MISSING_MONTH, 4.3 s (4.3-4.5, n=5) after the task started; with a 3-minute age limit it reported
LOAD_TOO_OLD, 4.2 s (4-4.4, n=5) after the task started.

**What the first attempts found.** The first replay was not identical (12 table-and-month differences in the two
aggregate marts, from floating-point sums over rows in a different order), a download that the CDN answered with HTTP
202 and no body, or with HTTP 403 to every request in a run, and a first alerts run that exercised one broken file of
four. Each is in
[docs/findings.md](docs/findings.md) and its files are kept under `results/`.

## Running it

Needs Docker with Compose. The Airflow image is 3.37 GB because it carries the data libraries.

```
docker compose up -d --build --wait
docker compose exec airflow-scheduler airflow dags unpause taxi_monthly
```

The scheduler then runs the six monthly runs by itself (the DAG's `end_date` comes from `TAXI_LAST_MONTH`,
default 2025-06). To run or replay a range on demand:

```
scripts/backfill.sh 2025-01 2025-06
scripts/backfill.sh 2025-03 2025-03 --reset      # replay a month that already ran
```

Look at what came out:

```
docker compose exec airflow-scheduler /opt/pipeline-venv/bin/taxi-platform status
docker compose exec postgres psql -U taxi -d warehouse -c \
  "SELECT primary_reason, count(*) FROM quarantine.yellow_trips WHERE source_month = '2025-03-01' GROUP BY 1 ORDER BY 2 DESC"
```

Operating notes, including what each alert means and what to do about it: [docs/operations.md](docs/operations.md).

## What counts as a bad row

The rules are Great Expectations expectations in [`taxi_platform/rules.py`](taxi_platform/rules.py); each carries the
reason code that lands in the quarantine table. A row can fail several rules: the table keeps every code
(`reason_codes`) and the first in rule order (`primary_reason`).

| reason code | the rule |
|---|---|
| NULL_PICKUP_TS, NULL_DROPOFF_TS, NULL_ZONE, NULL_AMOUNT, NULL_VENDOR, NULL_PAYMENT_TYPE, NULL_DISTANCE | the field is present |
| INVALID_VENDOR, INVALID_PAYMENT_TYPE | vendor in {1, 2, 4, 6, 7}; payment type 0 to 6 |
| INVALID_PASSENGER_COUNT | when present, 0 to 9 |
| UNKNOWN_PICKUP_ZONE, UNKNOWN_DROPOFF_ZONE | the id exists in the zone lookup |
| PICKUP_OUTSIDE_MONTH | the pickup falls inside the file's calendar month |
| DROPOFF_BEFORE_PICKUP, DURATION_OVER_24H | the trip ends after it starts and within 24 hours |
| NEGATIVE_AMOUNT, EXCESSIVE_AMOUNT | fare and total are between 0 and 5,000 |
| DISTANCE_OUT_OF_RANGE, IMPLAUSIBLE_SPEED | distance 0 to 200 miles; average speed at most 100 mph |
| FEE_OUT_OF_RANGE | no fee column is larger than 5,000 in absolute value |
| DUPLICATE_TRIP | the same vendor, times, zones, distance, fare and total appeared earlier in the file |

Fields the TLC leaves null on purpose (passenger count, rate code, store-and-forward flag and two fees on
flex-fare trips) are counted in the report and never quarantined.

A separate **file contract** is checked first: every required column present with a plausible type, and at
least one row. A file that breaks it is rejected whole before any row is loaded, the task fails without a retry,
and the failure alert is written.

## Tests

```
pip install -r requirements.txt pytest ruff && pip install --no-deps -e .
ruff check taxi_platform tests scripts dags
python -m pytest tests                                       # unit tests
```

The integration tests run the pipeline on the small fixture (`tests/fixtures/source/`, 4,031 real rows of March
2025, see `FIXTURE.json`) against a real PostgreSQL and an S3 endpoint, in a database whose name must contain
`test`:

```
docker compose up -d postgres s3
docker compose exec postgres psql -U taxi -d warehouse -c "CREATE DATABASE warehouse_test"
TAXI_TEST_PG_DSN=postgresql://taxi:taxi@localhost:18232/warehouse_test \
TAXI_TEST_S3_ENDPOINT=http://localhost:18290 python -m pytest tests/test_integration.py
```

They load the fixture month and check the counts, run it again twice and compare every row, kill the loader
halfway (a simulated crash, once on a first load and once on a month already loaded), hold a lock to make the
loader give up, feed broken files (a renamed column, a dropped one, a wrong type, no rows) and check the
command-line exit codes, and check reconciliation and freshness findings. `tests/test_dags.py` imports the DAG
files under Airflow and checks their shape (it needs Airflow in the interpreter, so CI runs it in its own job).

`.github/workflows/ci.yml` runs on every push with three jobs: lint and unit tests; the pipeline on the fixture
against a PostgreSQL service and a SeaweedFS container, then the integration tests, `dbt seed/run/test`, the
reconciliation, the lineage page and a second run that must leave the table byte-for-byte unchanged; and the DAG
import under Airflow 2.10.5 with its constraints file.

## dbt models

`staging` (views over the raw tables), `intermediate` (`int_trips_enriched`: duration, speed, tip share and a
`trip_id` hash), and `marts`: `fct_trips`, `dim_zone`, `mart_zone_hour_demand` (trips, fares, tips and speed by
pickup zone, date and hour), `mart_daily_summary` and `mart_ingestion_quality` (rows in, loaded and quarantined per
month and reason). The three big marts are incremental: each run deletes the month's rows and inserts them again,
so re-running a month cannot double it. The two aggregate marts sum and average over `numeric`, not `double
precision`: the first replay of all six months came back with the same rows but different last digits in them,
because floating-point sums depend on row order ([docs/findings.md](docs/findings.md)). Lineage, rendered from
dbt's manifest: [docs/lineage.md](docs/lineage.md).

## Limits

- **One laptop, shared.** Every timing is from a single Windows 11 machine running Docker Desktop that was also
  running other projects' containers. The runs were serialised with a lock file, but the ranges in the results are
  there because the machine was not otherwise idle. Conditions per run: [docs/measurements.md](docs/measurements.md).
- **SeaweedFS stands in for an S3 service.** The code uses the S3 API through `boto3` and an endpoint setting; I have
  not run it against Amazon S3 or MinIO. MinIO was the first choice and was not downloadable when I built this
  (details in [docs/design.md](docs/design.md)).
- **The rules are mine.** The thresholds (a fare between 0 and 5,000, at most 100 mph, a trip under 24 hours) come from
  looking at the data, not from a TLC definition. A quarantined row failed one of these rules; it is not
  necessarily a wrong record, and the largest group is negative fares, which the TLC publishes for refunds and
  disputes as well as for errors.
- **What the rules cannot see.** The injected plausible-value faults (a fare changed to another plausible fare) are
  loaded by design, and the results report them as missed. Checks against another source of truth, such as a fare
  recomputed from distance and rate code, are not built.
- **A 403 from the download looks like "not published".** For a month file the CDN's storage origin answers 403 for a
  key that does not exist, so a throttling 403 cannot be told from a month that is not out yet. The zone lookup, which
  must exist, retries it ([docs/findings.md](docs/findings.md), section 7).
- **A republished month is not picked up by itself.** A file already in the landing bucket is not downloaded again; a
  corrected file needs `taxi-platform fetch --month <month> --force`.
- **Alerts go to a local file.** Nothing pages anyone. The measured alert delay is the delay to that file on the
  same host, not to an inbox or a pager.
- **The crash drill kills the loader's container.** It does not lose the PostgreSQL volume or cut the power to the
  host; it shows the load is one transaction and that PostgreSQL releases a vanished client's locks.
- **One taxi type and six months.** Yellow-taxi trips from January to June 2025. Green taxis, for-hire vehicles and
  later months are not loaded, and the DAG runs one month at a time.
- **Local credentials.** The passwords in the Compose file are for containers published on `localhost` only.

## Layout

```
taxi_platform/   the pipeline: settings, S3 and PostgreSQL access, the reason-code rules and their Great Expectations
                 runner, the stages, alerts and freshness, the command line, and the bad-row injector
dags/            taxi_monthly, taxi_freshness and their shared callbacks
dbt/             staging, intermediate and mart models, seeds, generic and singular tests
docker/          the Airflow image, database initialisation and the SeaweedFS S3 configuration
scripts/         backfill.sh, lineage.py, check_marts_deterministic.py, make_fixture.py, and experiments/ (the measurements)
tests/           unit, integration and DAG tests; fixtures/source holds the small real-data fixture
results/         the raw result files of every experiment; SUMMARY.md and summary.json are computed from them
docs/            design.md, operations.md, findings.md, measurements.md, lineage.md
```
