-- mart__fact_all_trips.sql

with trips_renamed as (
    select 'bike' as type, started_at_ts as pickup_datetime, ended_at_ts as dropoff_datetime
    from {{ ref('stg__bike_data') }}
    union all
    select 'fhv' as type, pickup_datetime, dropoff_datetime
    from {{ ref('stg__fhv_tripdata') }}
    union all
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime
    from {{ ref('stg__fhvhv_tripdata') }}
    union all
    select 'green' as type, lpep_pickup_datetime as pickup_datetime, lpep_dropoff_datetime as dropoff_datetime
    from {{ ref('stg__green_tripdata') }}
    union all
    select 'yellow' as type, tpep_pickup_datetime as pickup_datetime, tpep_dropoff_datetime as dropoff_datetime
    from {{ ref('stg__yellow_tripdata') }}
)

select
    type,
    pickup_datetime as started_at_ts,
    dropoff_datetime as ended_at_ts,
    datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
    datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
from trips_renamed

