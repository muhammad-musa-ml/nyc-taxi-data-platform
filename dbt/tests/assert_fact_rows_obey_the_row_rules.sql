-- Independent restatement of the main row rules over the finished fact table: if the validator let a
-- bad row through, or the models changed one, this finds it.
select source_month, source_row, pickup_at, dropoff_at, trip_distance_miles, fare_amount, total_amount
from {{ ref('fct_trips') }}
where {{ month_filter() }}
  and (
        pickup_at < source_month
     or pickup_at >= (source_month + interval '1 month')
     or dropoff_at < pickup_at
     or dropoff_at - pickup_at > interval '24 hours'
     or fare_amount < 0
     or total_amount < 0
     or trip_distance_miles < 0
     or trip_distance_miles > 200
     or avg_speed_mph > 100
  )
