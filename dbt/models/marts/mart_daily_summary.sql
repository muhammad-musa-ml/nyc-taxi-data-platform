{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='source_month') }}

-- Aggregates are taken over numeric, not double precision. Floating-point addition depends on the order the rows
-- are visited, and a month that is deleted and inserted again comes back in a different physical order, so a
-- replay would otherwise change the last digits. Numeric sums are exact, and the rounding is to the precision
-- the inputs carry (amounts are in cents).
select
    source_month,
    pickup_date,
    count(*)                                                         as trips,
    round(sum(total_amount::numeric), 2)                             as revenue,
    round(avg(fare_amount::numeric), 4)                              as avg_fare,
    round(avg(trip_distance_miles::numeric), 4)                      as avg_distance_miles,
    round(avg(tip_pct::numeric) filter (where is_card_trip), 6)      as avg_tip_pct,
    round((count(*) filter (where is_card_trip))::numeric / count(*), 6) as card_share,
    round(avg(avg_speed_mph::numeric), 4)                            as avg_speed_mph
from {{ ref('fct_trips') }}
where {{ month_filter('source_month') }}
group by 1, 2
