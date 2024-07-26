--mart__fact_trips_by_borough.sql

--number of trips grouped by borough.  Build from fact and dim models.  Count of all trips and dim model for boroughs.  Make a new fact table.


-- models/mart/mart__fact_trips_by_borough.sql

-- Step 1: Consolidate all trips into a single CTE
with all_trips as (
    select
        'bike' as type,
        started_at_ts as pickup_datetime,
        ended_at_ts as dropoff_datetime,
        start_station_id as pulocationid,
        end_station_id as dolocationid,
        datediff('minute', started_at_ts, ended_at_ts) as duration_min,
        datediff('second', started_at_ts, ended_at_ts) as duration_sec
    from {{ ref('stg__bike_data') }}
    union all
    select 'fhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
    from {{ ref('stg__fhv_tripdata') }}
    union all
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
    from {{ ref('stg__fhvhv_tripdata') }}
    union all
    select 'green' as type, lpep_pickup_datetime as pickup_datetime, lpep_dropoff_datetime as dropoff_datetime,
        pulocationid, dolocationid,
        datediff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_min,
        datediff('second', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_sec
    from {{ ref('stg__green_tripdata') }}
    union all
    select 'yellow' as type, tpep_pickup_datetime as pickup_datetime, tpep_dropoff_datetime as dropoff_datetime,
        pulocationid, dolocationid,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_min,
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_sec
    from {{ ref('stg__yellow_tripdata') }}
),

-- Step 2: Map location IDs to boroughs for pickup locations
pickup_boroughs as (
    select
        at.type,
        at.pickup_datetime,
        at.dropoff_datetime,
        at.pulocationid as locationid,
        dl.borough as pickup_borough,
        at.duration_min,
        at.duration_sec
    from all_trips at
    left join {{ ref('mart__dim_locations') }} dl
    on at.pulocationid = dl.locationid
),

-- Step 3: Map location IDs to boroughs for dropoff locations
dropoff_boroughs as (
    select
        at.type,
        at.pickup_datetime,
        at.dropoff_datetime,
        at.dolocationid as locationid,
        dl.borough as dropoff_borough,
        at.duration_min,
        at.duration_sec
    from all_trips at
    left join {{ ref('mart__dim_locations') }} dl
    on at.dolocationid = dl.locationid
),

-- Step 4: Consolidate borough information
borough_trips as (
    select
        pb.type,
        pb.pickup_datetime,
        pb.dropoff_datetime,
        pb.pickup_borough,
        db.dropoff_borough,
        pb.duration_min,
        pb.duration_sec
    from pickup_boroughs pb
    left join dropoff_boroughs db
    on pb.type = db.type
    and pb.pickup_datetime = db.pickup_datetime
    and pb.dropoff_datetime = db.dropoff_datetime
)

-- Step 5: Aggregate trips by borough
select
    pickup_borough as borough,
    count(*) as num_trips,
    avg(duration_min) as avg_duration_min,
    avg(duration_sec) as avg_duration_sec
from borough_trips
where pickup_borough is not null
group by pickup_borough
order by num_trips desc

-- End of the SQL model
