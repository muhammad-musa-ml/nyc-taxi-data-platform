-- The analysis mart neither drops nor double-counts trips or revenue.
with fct as (
    select source_month, count(*) as trips, sum(total_amount) as revenue
    from {{ ref('fct_trips') }}
    where {{ month_filter() }}
    group by 1
),
mart as (
    select source_month, sum(trips) as trips, sum(revenue) as revenue
    from {{ ref('mart_zone_hour_demand') }}
    where {{ month_filter() }}
    group by 1
)
select
    coalesce(f.source_month, m.source_month) as source_month,
    f.trips as fct_trips,
    m.trips as mart_trips,
    f.revenue as fct_revenue,
    m.revenue as mart_revenue
from fct f
full join mart m on m.source_month = f.source_month
where coalesce(f.trips, 0) <> coalesce(m.trips, 0)
   or abs(coalesce(f.revenue, 0) - coalesce(m.revenue, 0)) > 1.0
