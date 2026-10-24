{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='source_month') }}

-- Analyst-facing: how busy, how expensive and how generous each pickup zone is, hour by hour.
-- Tip share is taken over card trips only, because cash tips are not recorded.
-- Aggregates are taken over numeric, not double precision, so that replaying a month cannot change the last digits
-- (floating-point sums depend on row order); amounts are in cents, so rounding to cents loses nothing.
select
    source_month,
    pickup_zone_id,
    pickup_borough,
    pickup_zone,
    pickup_date,
    pickup_hour,
    count(*)                                                         as trips,
    sum(passenger_count)                                             as passengers,
    round(sum(fare_amount::numeric), 2)                              as fare_total,
    round(avg(fare_amount::numeric), 4)                              as avg_fare,
    round(avg(trip_distance_miles::numeric), 4)                      as avg_distance_miles,
    round(avg(trip_duration_min::numeric), 4)                        as avg_duration_min,
    round(avg(avg_speed_mph::numeric), 4)                            as avg_speed_mph,
    count(*) filter (where is_card_trip)                             as card_trips,
    round(sum(tip_amount::numeric) filter (where is_card_trip), 2)   as tip_total,
    round(avg(tip_pct::numeric) filter (where is_card_trip), 6)      as avg_tip_pct,
    round(sum(total_amount::numeric), 2)                             as revenue
from {{ ref('fct_trips') }}
where {{ month_filter('source_month') }}
group by 1, 2, 3, 4, 5, 6
