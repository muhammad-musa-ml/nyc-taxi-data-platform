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
