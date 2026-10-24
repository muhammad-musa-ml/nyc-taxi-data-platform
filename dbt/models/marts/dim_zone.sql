{{ config(materialized='table') }}

select
    zone_id,
    borough,
    zone_name,
    service_zone,
    borough = 'Manhattan'        as is_manhattan,
    service_zone in ('Airports', 'EWR') as is_airport
from {{ ref('stg_taxi_zones') }}
