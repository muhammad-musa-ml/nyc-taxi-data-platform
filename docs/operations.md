# Operating the platform

Everything below runs against the Compose stack in this repository. Host ports are 18280 (Airflow UI,
user `admin`, password `admin`), 18232 (PostgreSQL, user `taxi`, password `taxi`) and 18290 (S3 API, key
`taxi-local-key`). These are local development values for containers that are not reachable from outside
the machine.

## Start, run, replay

```
docker compose up -d --build --wait      # first start builds the Airflow image (3.37 GB with the data libraries)
docker compose exec airflow-scheduler airflow dags unpause taxi_monthly
```

With the DAG unpaused, the scheduler runs one DAG run per month between 2025-01 and `TAXI_LAST_MONTH`
(default 2025-06). Run a range on demand, or replay it, with the backfill helper:

```
scripts/backfill.sh 2025-01 2025-06              # run the range
scripts/backfill.sh 2025-03 2025-03 --reset      # run one month again from the landing bucket
```

A single stage can be run by hand with the same command Airflow's tasks use:

```
docker compose exec airflow-scheduler /opt/pipeline-venv/bin/taxi-platform validate --month 2025-03
docker compose exec airflow-scheduler /opt/pipeline-venv/bin/taxi-platform status
```

`status` prints the ingestion ledger: month, rows in, loaded, quarantined, when it loaded and how many times.

## Where a month's data is

| what | where |
|---|---|
| the file as published, with its SHA-256 | S3 `landing/yellow/<month>/` (`.parquet` and `.meta.json`) |
| rows that passed, rows rejected, and the report | S3 `validated/yellow/<month>/` (`good.parquet`, `quarantine.parquet`, `report.json`) |
| rows that passed | `raw.yellow_trips`, one partition per month |
| rows that failed, with every reason code | `quarantine.yellow_trips` (`reason_codes text[]`, `primary_reason`) |
| what the last load saw | `ops.ingestion_ledger`; rows failed per rule in `ops.ingestion_checks` |
| how long each stage took | `ops.stage_timings` (one row per attempt) |
| analyst tables | `marts.*`; lineage in [lineage.md](lineage.md) |

Why were rows rejected last month?

```sql
SELECT primary_reason, count(*) FROM quarantine.yellow_trips
WHERE source_month = '2025-03-01' GROUP BY 1 ORDER BY 2 DESC;
```

`source_row` is the row's position in the month's file, so a rejected row can be found in the original.

## Alerts

Every task's failure callback, and the DAG's, appends one JSON line to `alert-sink/alerts.jsonl`
(`/opt/airflow/alert-sink/alerts.jsonl` in the containers). The freshness check writes its own lines.
Anything that can tail a file can consume them; no external service is involved.

| `kind` | written when | first thing to look at |
|---|---|---|
| `task_failure` | a task failed after its retries (`error`, `try_number`, `source_month`, `task_ended_at`) | the task log in the Airflow UI |
| `dag_run_failed` | the monthly run ended in failure | the task alert for the same `run_id` |
| `freshness` | a month is missing (`MISSING_MONTH`), the newest load is older than `TAXI_MAX_LOAD_AGE_HOURS` (`LOAD_TOO_OLD`), or the newest loaded month is behind the expected one (`LATEST_MONTH_BEHIND`) | `taxi-platform status` |
| `zone_download_fallback` (warning) | the zone lookup could not be downloaded after six attempts, so the copy an earlier run landed was used (`run_id`, `error`); the task did not fail | the `error` field, then the `detail` of the `load_zones` row in `ops.stage_timings`; the CDN's answers are described in [findings.md](findings.md), section 7 |

Two exit codes of `taxi-platform` are not retried, because running the same file again cannot change the answer:

- **2, contract violation.** The file has a missing or renamed required column, a column of the wrong type, or no
  rows. Nothing was loaded. `validated/yellow/<month>/report.json` lists `contract_failures`. If the publisher
  changed the layout on purpose, change `taxi_platform/rules.py` (and the dbt models) and run the month again;
  otherwise wait for a corrected file and run `taxi-platform fetch --month <month> --force`.
- **3, not published.** The source answered 403/404 for that month. The month has not been released yet; run it
  again later. (A throttled CDN also answers 403, and for a month file the two cannot be told apart.)

Anything else is retried once, 20 seconds later.

## If a load is interrupted

The load replaces a month in one transaction, so killing it at any point leaves the previous state in place.
PostgreSQL notices a vanished client through TCP keepalives (30 s idle, 10 s apart, 3 probes, set in
`docker-compose.yml`); until then the dead loader's transaction holds its locks. A later load waits for those locks
at most `TAXI_LOAD_LOCK_TIMEOUT_S` (180 s) and then fails, and the task's retry runs after PostgreSQL has cleaned
up. To clear a stuck backend by hand:

```sql
SELECT pid, state, wait_event, left(query, 60) FROM pg_stat_activity WHERE datname = 'warehouse';
SELECT pg_terminate_backend(<pid>);
```

## Changing the window

`TAXI_FIRST_MONTH` / `TAXI_LAST_MONTH` bound the DAG's schedule, and `TAXI_EXPECTED_LATEST_MONTH` is the month the
freshness check expects to have loaded. For example, to add July 2025:

```
TAXI_LAST_MONTH=2025-07 TAXI_EXPECTED_LATEST_MONTH=2025-07 docker compose up -d
```
