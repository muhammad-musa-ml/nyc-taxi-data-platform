-- Rows quarantined per primary reason equal the per-reason counts the loader recorded.
with actual as (
    select source_month, primary_reason as reason_code, count(*) as n
    from {{ source('quarantine', 'yellow_trips') }}
    where {{ month_filter() }}
    group by 1, 2
),
recorded as (
    select source_month, reason_code, rows_primary as n
    from {{ source('ops', 'ingestion_checks') }}
    where {{ month_filter() }} and rows_primary > 0
)
select
    coalesce(a.source_month, r.source_month) as source_month,
    coalesce(a.reason_code, r.reason_code) as reason_code,
    a.n as quarantined,
    r.n as recorded
from actual a
full join recorded r on r.source_month = a.source_month and r.reason_code = a.reason_code
where coalesce(a.n, 0) <> coalesce(r.n, 0)
