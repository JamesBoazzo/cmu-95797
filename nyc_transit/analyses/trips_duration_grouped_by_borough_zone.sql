-- This query processes trip data from various staging tables, maps their locations to boroughs and zones,
-- and calculates the number of trips, average duration in minutes, and average duration in seconds
-- by borough and zone. The results are written to a text file.
-- PLEASE NOTE: I used the stg tables b/c the mart_fact_all_trips_table did NOT include pulocationid or dolocationid, which was important in
-- my methodology to write this query.

COPY (
    -- Combine trip data from various sources into a single table with unified columns as explained above.
    with all_trips as (
        select
            pickup_datetime,
            dropoff_datetime,
            pulocationid,
            dolocationid,
            datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
            datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
        from (
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
            select 
                'fhv' as type, 
                pickup_datetime, 
                dropoff_datetime,
                pulocationid, 
                dolocationid,
                datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
                datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
            from {{ ref('stg__fhv_tripdata') }}
            union all
            select 
                'fhvhv' as type, 
                pickup_datetime, 
                dropoff_datetime,
                pulocationid, 
                dolocationid,
                datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
                datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
            from {{ ref('stg__fhvhv_tripdata') }}
            union all
            select 
                'green' as type, 
                lpep_pickup_datetime as pickup_datetime, 
                lpep_dropoff_datetime as dropoff_datetime,
                pulocationid, 
                dolocationid,
                datediff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_min,
                datediff('second', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_sec
            from {{ ref('stg__green_tripdata') }}
            union all
            select 
                'yellow' as type, 
                tpep_pickup_datetime as pickup_datetime, 
                tpep_dropoff_datetime as dropoff_datetime,
                pulocationid, 
                dolocationid,
                datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_min,
                datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_sec
            from {{ ref('stg__yellow_tripdata') }}
        ) as all_trips_data
    ),

    -- Map pickup locations to their respective boroughs and zones
    pickup_boroughs_zones as (
        select
            at.pickup_datetime,
            at.dropoff_datetime,
            dl.borough as pickup_borough,
            dl.zone as pickup_zone,
            at.duration_min,
            at.duration_sec
        from all_trips at
        left join {{ ref('mart__dim_locations') }} dl
        on at.pulocationid = dl.locationid
    ),

    --  Map dropoff locations to their respective boroughs and zones
    dropoff_boroughs_zones as (
        select
            at.pickup_datetime,
            at.dropoff_datetime,
            dl.borough as dropoff_borough,
            dl.zone as dropoff_zone,
            at.duration_min,
            at.duration_sec
        from all_trips at
        left join {{ ref('mart__dim_locations') }} dl
        on at.dolocationid = dl.locationid
    ),

    -- Combine the pickup and dropoff borough and zone information
    combined_boroughs_zones as (
        select
            coalesce(pbz.pickup_borough, dbz.dropoff_borough) as borough,
            coalesce(pbz.pickup_zone, dbz.dropoff_zone) as zone,
            pbz.duration_min,
            pbz.duration_sec
        from pickup_boroughs_zones pbz
        full join dropoff_boroughs_zones dbz
        on pbz.pickup_datetime = dbz.pickup_datetime
        and pbz.dropoff_datetime = dbz.dropoff_datetime
    )

    --  Aggregate the trip data by borough and zone as per instructions, and calculate the average duration
    select
        borough,
        zone,
        count(*) as num_trips,
        avg(duration_min) as avg_duration_min,
        avg(duration_sec) as avg_duration_sec
    from combined_boroughs_zones
    where borough is not null
    group by borough, zone
    order by borough, zone
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/trips_duration_grouped_by_borough_zone.txt' (HEADER, DELIMITER ',');
