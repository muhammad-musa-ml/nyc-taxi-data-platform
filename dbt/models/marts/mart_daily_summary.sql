{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='source_month') }}

select
    source_month,
    pickup_date,
    count(*)                                       as trips,
    sum(total_amount)                              as revenue,
    avg(fare_amount)                               as avg_fare,
    avg(trip_distance_miles)                       as avg_distance_miles,
    avg(tip_pct) filter (where is_card_trip)       as avg_tip_pct,
    (count(*) filter (where is_card_trip))::double precision / count(*) as card_share,
    avg(avg_speed_mph)                             as avg_speed_mph
from {{ ref('fct_trips') }}
where {{ month_filter('source_month') }}
group by 1, 2
