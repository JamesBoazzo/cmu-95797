-- taxi_trips_no_valid_pickup_location_id.sql
-- This query combines taxi trip data from taxi trip data, checks for invalid pickup location IDs,
-- and counts the number of trips with invalid pickup locations. The results are written to a text file.

COPY (
    -- Combine taxi trip data from various sources into a single table with unified columns
    with taxi_trips as (
        select
            'fhv' as type, 
            pickup_datetime, 
            dropoff_datetime, 
            pulocationid, 
            dolocationid
        from {{ ref('stg__fhv_tripdata') }}
        union all
        select
            'fhvhv' as type, 
            pickup_datetime, 
            dropoff_datetime, 
            pulocationid, 
            dolocationid
        from {{ ref('stg__fhvhv_tripdata') }}
        union all
        select
            'green' as type, 
            lpep_pickup_datetime as pickup_datetime, 
            lpep_dropoff_datetime as dropoff_datetime, 
            pulocationid, 
            dolocationid
        from {{ ref('stg__green_tripdata') }}
        union all
        select
            'yellow' as type, 
            tpep_pickup_datetime as pickup_datetime, 
            tpep_dropoff_datetime as dropoff_datetime, 
            pulocationid, 
            dolocationid
        from {{ ref('stg__yellow_tripdata') }}
    ),

    -- Identify trips with invalid pickup location IDs
    invalid_pickup_location_trips as (
        select
            tt.*
        from taxi_trips tt
        left join {{ ref('mart__dim_locations') }} dl
        on tt.pulocationid = dl.locationid
        where dl.locationid is null
    )

    -- Count the number of trips with invalid pickup location IDs
    select
        count(*) as invalid_pickup_location_count
    from invalid_pickup_location_trips
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/taxi_trips_no_valid_pickup_location_id.txt' (HEADER, DELIMITER ',');