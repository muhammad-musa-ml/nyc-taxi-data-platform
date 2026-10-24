# How the numbers were measured

Every figure in the README comes from [`results/SUMMARY.md`](../results/SUMMARY.md), which
`scripts/experiments/summarize.py` computes from the raw files in `results/`. Nothing is typed in. A quantity that no
file holds is printed as "not measured". Where an experiment was repeated, the summary gives the median and the
minimum and maximum over the runs, with the number of runs.

## Conditions

- **One machine.** A Windows 11 laptop running Docker Desktop with Linux containers. The CPU count and memory
  Docker offered, the server version, the package versions inside the containers, the PostgreSQL version and the git
  revision of the code are in the `environment.json` beside each timed result. A figure measured here says what
  this setup did and not what another one would.
- **A shared machine.** The laptop was also running other projects' containers. Timed runs were taken while holding a
  lock file that those projects also respected, so no other timed run overlapped, but their idle containers and
  ordinary work were not excluded. The container list at the start and end of each run is saved beside its results
  (`docker-ps.txt`, `end-docker-ps.txt`); other projects' containers appear there as "(other container)" with their
  status only. This is the main reason the ranges are reported.
- **A real network.** Every backfill downloads the six files and the zone lookup from the TLC's CDN into an empty
  landing bucket, so download time, which depends on the network and the CDN, is part of the wall time. Each file's
  SHA-256 is compared with an independent `curl` download taken before the first run (`results/source-files.json`).
- **Cold start.** Each timed backfill begins with `docker compose down -v` and `up`, so the database, the landing
  bucket, Airflow's metadata and the dbt state are all new. The container images were already on the machine.

## What was timed, and how

| experiment | repeats | timed | what is measured |
|---|---|---|---|
| Backfill of 2025-01 to 2025-06 from an empty stack (`scripts/experiments/backfill.py`) | 5 | yes, under the lock | wall time of `airflow dags backfill` as seen by the script; each DAG run's and task's duration from Airflow's metadata; each stage's duration from `ops.stage_timings`; rows in, loaded and quarantined per month and per reason from the ledger and `ops.ingestion_checks` |
| Replay of the whole range (same script, `--replay`) | 1 (run 1) | yes, under the lock | wall time, and whether every row count and content hash of the raw, quarantine, fact and mart tables, the ledger and the checks came back unchanged |
| Idempotency (`idempotency.py`) | 5 per month, 30 in all | no | after each re-run of `taxi-platform run-month` on a loaded month: row counts and a content hash of every table for that month, the ledger's load count, and duplicate keys |
| Loader killed mid-load (`crash.py`) | 5 trials | no, except the seconds PostgreSQL took to release the dead loader | SIGKILL to the container running `taxi-platform load` at a chosen moment after PostgreSQL showed its `COPY`, then the same comparison as above, then the load run again |
| Injected bad rows (`injection.py`) | 5 seeds | no | rows from a real month changed to break each of 25 rules (200 rows per rule per seed), run through the whole pipeline, scored against the injector's manifest |
| Alerts (`alerts.py`) | 5 per scenario | yes, under the lock | for four broken-file cases offered to the monthly DAG, and three freshness scenarios, the seconds from the task ending (or starting) to the record appearing in the alert sink |
| Zone-download probe (`zone_download_probe.py`) | 2 | no | what the CDN answers for the zone lookup, and whether the pipeline's download copes ([docs/findings.md](findings.md), section 7) |

The timed experiments take the lock when they start and release it as soon as they end. The untimed ones run with
two validation workers instead of four so they leave the machine usable; their durations are saved but are not
headline figures.

## How a result is judged

- **Identical** means identical for every table in the comparison, month by month: the row count and a content hash
  (the sum, over the month's rows, of PostgreSQL's 64-bit hash of each row's text, so it does not depend on row
  order; `scripts/experiments/common.py`, `snapshot` and `compare`) for the raw, quarantine and fact tables and the
  two aggregate marts, plus the ledger's counts and file digest and the per-reason check counts. A single difference in
  any of them is reported as a difference, with the table, month and numbers named in `deltas`. The ledger's
  `load_count` is the one value expected to move: it counts how many times the month was loaded.
- **Caught** means the injected row is in the quarantine table with the reason code the injector says it should have.
  A row quarantined only under some other code is counted separately, and a row that ended up in the raw table is
  counted as missed. The two plausible-value faults are expected to be missed; they show what the rules cannot see.
- **Alert delay** is the difference between two timestamps in the same sink record: when the record was written and
  when Airflow says the failing task ended. The sink is a file on the same host, so this measures the pipeline's own
  callback and not a delivery service.

## Stored floats

JSON result files hold floating-point values rounded to four decimals. A full-precision timing is not meaningful
beyond that, and the 12-digit fractions of a raw `float` trip the publish checks' account-number pattern.

## Which code produced which run

`environment.json` records the git revision and whether the pipeline code, DAGs, dbt project or compose file had
uncommitted changes. Run 1 and its replay were taken before the zone-download fix described in findings section 7 and
before the other changes listed there; the rest were taken after it. The fix changes only what happens when the CDN
does not answer with the file, so the happy path that a timing measures is the same code; the longest `load_zones` stage
in each run is reported in the summary so a retry (at least 2 s of waiting) would be visible.

## What was not measured

- Anything on a machine other than this one, or against another object store than SeaweedFS.
- More than six months of data, or months loaded concurrently (the DAG is configured for one run at a time).
- Memory use, CPU use or disk throughput. Only elapsed time and counts were recorded.
- The quality of the alert's content for a person on call; only that the record exists, names the task and month,
  and appears quickly.
- Recovery from a host crash or a lost volume. The crash drill kills the loader's container, not the machine.
