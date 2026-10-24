#!/usr/bin/env bash
# Replay a range of months through the monthly DAG.
#
#   scripts/backfill.sh 2025-01 2025-06            # run the six months, skipping none
#   scripts/backfill.sh 2025-03 2025-03 --reset    # replay one month that already ran
#
# The range is inclusive. Months whose raw file is already in the landing bucket are not downloaded again,
# and every load replaces its month in one transaction, so a replay changes no row count. The command runs
# inside the scheduler container, so the stack must be up (`docker compose up -d`).
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <first-month YYYY-MM> <last-month YYYY-MM> [--reset]" >&2
  exit 64
fi

first="$1"
last="$2"
reset=""
if [ "${3:-}" = "--reset" ]; then
  reset="--reset-dagruns"
fi

cd "$(dirname "$0")/.."
docker compose exec -T airflow-scheduler \
  airflow dags backfill taxi_monthly \
  --start-date "${first}-01" --end-date "${last}-01" \
  ${reset} --yes
