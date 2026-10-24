-- The ledger's counts agree with what is in the raw and quarantine tables.
with raw_rows as (
    select source_month, count(*) as n from {{ source('raw', 'yellow_trips') }}
    where {{ month_filter() }} group by 1
),
q_rows as (
    select source_month, count(*) as n from {{ source('quarantine', 'yellow_trips') }}
    where {{ month_filter() }} group by 1
)
select
    l.source_month,
    l.rows_loaded, r.n as raw_rows,
    l.rows_quarantined, q.n as quarantine_rows
from {{ source('ops', 'ingestion_ledger') }} l
left join raw_rows r on r.source_month = l.source_month
left join q_rows q on q.source_month = l.source_month
where {{ month_filter('l.source_month') }}
  and (l.rows_loaded <> coalesce(r.n, 0) or l.rows_quarantined <> coalesce(q.n, 0))
