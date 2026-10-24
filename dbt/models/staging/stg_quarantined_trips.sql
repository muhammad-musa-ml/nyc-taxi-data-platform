select
    source_month,
    source_row,
    primary_reason,
    reason_codes,
    cardinality(reason_codes)    as reason_count,
    tpep_pickup_datetime         as pickup_at,
    tpep_dropoff_datetime        as dropoff_at,
    pulocationid::int            as pickup_zone_id,
    dolocationid::int            as dropoff_zone_id,
    payment_type::int            as payment_type_id,
    trip_distance                as trip_distance_miles,
    fare_amount,
    total_amount
from {{ source('quarantine', 'yellow_trips') }}
