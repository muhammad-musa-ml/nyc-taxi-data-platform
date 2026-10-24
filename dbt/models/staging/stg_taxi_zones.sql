select
    locationid::int                        as zone_id,
    coalesce(borough, 'Unknown')           as borough,
    coalesce(zone, 'Unknown')              as zone_name,
    coalesce(service_zone, 'Unknown')      as service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
