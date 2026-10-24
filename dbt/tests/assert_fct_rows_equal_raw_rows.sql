-- Every loaded row reaches the fact table, and nothing else does.
with raw_rows as (
    select source_month, count(*) as n
    from {{ source('raw', 'yellow_trips') }}
    where {{ month_filter() }}
    group by 1
),
fct_rows as (
    select source_month, count(*) as n
    from {{ ref('fct_trips') }}
    where {{ month_filter() }}
    group by 1
)
select
    coalesce(r.source_month, f.source_month) as source_month,
    r.n as raw_rows,
    f.n as fct_rows
from raw_rows r
full join fct_rows f on f.source_month = r.source_month
where coalesce(r.n, 0) <> coalesce(f.n, 0)
