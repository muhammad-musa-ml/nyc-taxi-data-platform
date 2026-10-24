# Design

This is the design of `nyc-taxi-data-platform`: a batch data platform that runs on a laptop
for $0. It loads the New York City Taxi and Limousine Commission (TLC) yellow-taxi trip records
month by month, checks every row at ingestion, keeps rows that fail in a quarantine table with
machine-readable reason codes, builds analysis tables with dbt, and can replay any month or all
months without changing a single row count.

Nothing here is synthetic except the bad rows the tests inject on purpose, and those are labelled
where they appear.

## Data

- Source: the TLC trip-record page, https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<YYYY>-<MM>.parquet`
  for 2025-01 through 2025-06 (six consecutive months), plus
  `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` (265 zones).
- Fetched on 2026-10-24 (UTC). The pipeline records each file's SHA-256, byte size and fetch time
  in the ingestion ledger, so a later run can tell whether the published file changed.
- The data is real and dirty: negative fares, trips that end before they start, pickups years
  outside the file's month, 100+ mph average speeds, and a large block of rows (the share grows from
  January to June) whose passenger count, rate code and fees are all null. The checks below are
  written against what these files contain.

## Shape

```
TLC CloudFront ──fetch──> S3 landing/    (raw file as published, SHA-256 beside it)
                              │
                         validate (Great Expectations)
                              │
              ┌───────────────┴────────────────┐
        S3 validated/<month>/good        S3 validated/<month>/quarantine
              │                                  │
              └──────────── load (one transaction per month) ────────────┐
                                                                         ▼
                  PostgreSQL 16   raw.yellow_trips (list-partitioned by month)
                                  quarantine.yellow_trips
                                  ops.ingestion_ledger, ops.ingestion_checks
                                         │
                                    dbt: staging ─> intermediate ─> marts  (+ tests)
                                         │
                                  freshness check, failure callbacks ──> alert sink
```

Everything is orchestrated by one Airflow DAG per month and run with Docker Compose.

| piece | choice | why |
|---|---|---|
| orchestrator | Apache Airflow 2.10.5, LocalExecutor | the monthly schedule, retries, per-task state, the `dags backfill` command and failure callbacks all come with it |
| landing storage | SeaweedFS (S3 API) | the raw file is kept untouched in an object store, as it would be on S3; a reload never re-downloads. MinIO was the first choice, but on 2026-10-24 its image on quay.io answered 401, its Docker Hub image refused the pull and its binary download answered 410, so SeaweedFS stands in; the code only uses the S3 API |
| validation | Great Expectations 1.x | the rules are declared as expectations; GX returns the row numbers that fail each one |
| warehouse | PostgreSQL 16 | partition-per-month makes "replace this month" one transaction; dbt has a first-class adapter |
| transforms | dbt-postgres | staging, intermediate and mart models with tests and generated lineage |
| task code | the `taxi_platform` Python package in its own virtual environment inside the Airflow image | tasks call `taxi-platform <stage> --month YYYY-MM`, so the same code runs from Airflow, from the command line and in CI |

Host ports (all in 18200-18299): Airflow UI 18280, PostgreSQL 18232, S3 API 18290.

## Stages and idempotency

Every stage takes a month and can be run again for the same month.

1. **fetch** downloads the month's file unless the landing bucket already holds it, and writes a
   `.meta.json` beside it with the source URL, size, SHA-256 and fetch time. A replay therefore never
   downloads again; `taxi-platform fetch --force` does. The bytes are requested as stored (no
   content-encoding), so the digest is the published file's own. The digest travels into the ledger.
2. **validate** checks the file contract (required columns and types), then runs the row rules.
   Output: `good.parquet`, `quarantine.parquet` and a `report.json` with a count per reason code.
3. **load** replaces the month inside one transaction: create the month's partition if missing,
   truncate it, copy the good rows in, delete the month's quarantine rows, copy the quarantined
   rows in, upsert the ledger row. Nothing is appended, so a re-run, or a re-run after a crash
   half way through, ends in the same state. Row identity is enforced by the database
   (`PRIMARY KEY (source_month, source_row)`), not by the loader; a trip that appears twice inside one
   file is a validation rule (`DUPLICATE_TRIP`), and dbt tests that `trip_id` is unique in the fact table.
4. **dbt run / dbt test** rebuild the month's slice of the incremental models (delete the month's
   rows, insert them again) and test it.
5. **reconcile** checks that rows loaded equal rows in the fact table and that
   `rows_in = rows_loaded + rows_quarantined`, then runs the freshness check.

## Row rules and reason codes

Each rule is a Great Expectations expectation whose `meta.reason_code` names the failure. A row can
fail several rules; the quarantine table keeps all of its codes and the first one as its primary reason.
Columns that the TLC leaves null on purpose (passenger count, rate code, store-and-forward flag and the
congestion and airport fees on flex-fare trips) are reported as a warning, not quarantined.

| reason code | rule |
|---|---|
| NULL_PICKUP_TS, NULL_DROPOFF_TS | timestamp is present |
| NULL_ZONE | pickup and drop-off zone ids are present |
| NULL_AMOUNT | fare and total amounts are present |
| INVALID_VENDOR | vendor id is in {1, 2, 4, 6, 7} |
| INVALID_PAYMENT_TYPE | payment type is in 0-6 |
| INVALID_PASSENGER_COUNT | when present, 0-9 |
| UNKNOWN_PICKUP_ZONE, UNKNOWN_DROPOFF_ZONE | id exists in the zone lookup |
| PICKUP_OUTSIDE_MONTH | pickup falls inside the file's calendar month |
| DROPOFF_BEFORE_PICKUP | drop-off is not earlier than pickup |
| DURATION_OVER_24H | trip lasts at most 24 hours |
| NEGATIVE_AMOUNT | fare and total amounts are not negative (refund and adjustment records) |
| DISTANCE_OUT_OF_RANGE | trip distance is between 0 and 200 miles |
| IMPLAUSIBLE_SPEED | average speed is at most 100 mph |
| DUPLICATE_TRIP | the same vendor, pickup, drop-off, zones, distance, fare and total appear earlier in the file |

The file contract is separate and stricter: a missing column, a column of the wrong type or an empty file
fails the whole month before any row is loaded, and raises an alert.

## dbt

`staging` renames and types the raw tables (views). `intermediate` derives duration, speed, tip share and
the `trip_id` hash (a view, evaluated inside the incremental `fct_trips`). `marts` hold the analyst-facing tables
(`fct_trips`, `mart_zone_hour_demand` and `mart_daily_summary` are incremental: each run deletes the month's rows
and inserts them again):
`fct_trips`, `dim_zone`, `mart_zone_hour_demand` (trips, fares, tips and speed by pickup zone, date
and hour), `mart_daily_summary` and `mart_ingestion_quality` (rows in, loaded and quarantined per month
and reason). Schema tests cover keys, not-null, accepted values and relationships, and singular tests
reconcile row counts and totals against the raw layer. The aggregate marts sum over `numeric` so that a replayed
month, whose rows come back in a different physical order, produces exactly the same numbers
(`scripts/check_marts_deterministic.py` proves it on every push). `docs/lineage.md` is rendered from dbt's
`manifest.json` by `scripts/lineage.py`.

## Operations

- **Monthly schedule, bounded window.** The DAG is scheduled `@monthly` with `catchup` on and an end date
  of 2025-06 (`TAXI_LAST_MONTH`), so the scheduler runs the six months on its own. Raise the variable
  to extend the window.
- **Backfill.** `scripts/backfill.sh` wraps `airflow dags backfill` for any range and can reset
  earlier runs, so a range is replayed from the landing bucket without downloading it again.
- **Failure alerts.** Every task's failure callback appends a JSON line to the alert sink
  (`alert-sink/alerts.jsonl`) with the DAG, task, month, attempt and error.
- **Freshness.** A second DAG (and the last task of every monthly run) checks that no month in the
  window is missing, that the latest loaded month is the expected one and that the last load is not older
  than the allowed age; a breach writes a `freshness` record to the same sink.

## What gets measured

Rows in, loaded and quarantined per month and per reason code; end-to-end DAG-run time per month;
backfill time for all six months from an empty warehouse and the time of a replay; idempotency (row
counts, key duplicates and a content hash of every row before and after re-running every month); a loader
killed in the middle of its transaction; detection of deliberately injected bad rows (how many were caught,
by which rule, how many were missed); and how long a failure or a freshness breach takes to reach the
alert sink. Timed runs repeat 5 times. Other containers were running on the machine, so the container list
is saved beside every timed result. Results live in `results/`, produced by the scripts in
`scripts/experiments/`, and every number in the README is copied from there.

## Decisions worth knowing

- PostgreSQL over DuckDB: a partitioned table with transactional replace is the usual warehouse load
  pattern, and it makes the idempotency claim checkable with ordinary SQL.
- Great Expectations is asked for row numbers through the DataFrame index; asking it for index
  columns by name was measured at 40 s against 6 s for the same rule on 4.3 million rows.
- Tasks call a command-line entry point rather than importing the package into the Airflow
  interpreter, so Airflow's pinned dependencies and the data libraries do not have to agree.
