{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='source_month') }}

select
    t.trip_id,
    t.source_month,
    t.source_row,
    t.vendor_id,
    t.pickup_at,
    t.dropoff_at,
    t.pickup_date,
    t.pickup_hour,
    t.pickup_isodow,
    t.pickup_zone_id,
    pz.borough                         as pickup_borough,
    pz.zone_name                       as pickup_zone,
    t.dropoff_zone_id,
    dz.borough                         as dropoff_borough,
    dz.zone_name                       as dropoff_zone,
    t.passenger_count,
    t.trip_distance_miles,
    t.trip_duration_min,
    t.avg_speed_mph,
    t.rate_code_id,
    coalesce(rc.rate_code_name, 'Unknown') as rate_code_name,
    t.payment_type_id,
    pt.payment_type_name,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.tip_pct,
    t.is_card_trip,
    t.is_zero_duration,
    t.is_zero_distance
from {{ ref('int_trips_enriched') }} t
left join {{ ref('dim_zone') }} pz on pz.zone_id = t.pickup_zone_id
left join {{ ref('dim_zone') }} dz on dz.zone_id = t.dropoff_zone_id
left join {{ ref('dim_payment_type') }} pt on pt.payment_type_id = t.payment_type_id
left join {{ ref('dim_rate_code') }} rc on rc.rate_code_id = t.rate_code_id
where {{ month_filter('t.source_month') }}
