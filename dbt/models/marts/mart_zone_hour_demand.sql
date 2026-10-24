{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='source_month') }}

-- Analyst-facing: how busy, how expensive and how generous each pickup zone is, hour by hour.
-- Tip share is taken over card trips only, because cash tips are not recorded.
select
    source_month,
    pickup_zone_id,
    pickup_borough,
    pickup_zone,
    pickup_date,
    pickup_hour,
    count(*)                                         as trips,
    sum(passenger_count)                             as passengers,
    sum(fare_amount)                                 as fare_total,
    avg(fare_amount)                                 as avg_fare,
    avg(trip_distance_miles)                         as avg_distance_miles,
    avg(trip_duration_min)                           as avg_duration_min,
    avg(avg_speed_mph)                               as avg_speed_mph,
    count(*) filter (where is_card_trip)             as card_trips,
    sum(tip_amount) filter (where is_card_trip)      as tip_total,
    avg(tip_pct) filter (where is_card_trip)         as avg_tip_pct,
    sum(total_amount)                                as revenue
from {{ ref('fct_trips') }}
where {{ month_filter('source_month') }}
group by 1, 2, 3, 4, 5, 6
