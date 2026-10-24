-- date_part() returns double precision; extract() returns numeric, which is several times slower to
-- compute over millions of rows. Amounts are cast to double precision once, here.
select
    md5(concat_ws('|', vendor_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
                  trip_distance_miles, fare_amount, total_amount))           as trip_id,
    source_month,
    source_row,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_at::date                                                          as pickup_date,
    date_part('hour', pickup_at)::int                                        as pickup_hour,
    date_part('isodow', pickup_at)::int                                      as pickup_isodow,
    pickup_zone_id,
    dropoff_zone_id,
    passenger_count,
    trip_distance_miles::double precision                                    as trip_distance_miles,
    (date_part('epoch', dropoff_at) - date_part('epoch', pickup_at)) / 60.0  as trip_duration_min,
    case
        when dropoff_at > pickup_at
            then trip_distance_miles::double precision * 3600.0
                 / (date_part('epoch', dropoff_at) - date_part('epoch', pickup_at))
    end                                                                      as avg_speed_mph,
    rate_code_id,
    payment_type_id,
    fare_amount::double precision                                            as fare_amount,
    tip_amount::double precision                                             as tip_amount,
    tolls_amount::double precision                                           as tolls_amount,
    total_amount::double precision                                           as total_amount,
    payment_type_id = 1                                                      as is_card_trip,
    case when payment_type_id = 1 and fare_amount > 0
         then (tip_amount / fare_amount)::double precision end               as tip_pct,
    dropoff_at = pickup_at                                                   as is_zero_duration,
    trip_distance_miles = 0                                                  as is_zero_distance
from {{ ref('stg_yellow_trips') }}
where {{ month_filter('source_month') }}
