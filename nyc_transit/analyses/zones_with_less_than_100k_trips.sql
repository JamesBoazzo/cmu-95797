-- zones_with_less_than_100k_trips.sql 
-- This query combines taxi trip data from various sources, maps their locations to zones,
-- and identifies zones with fewer than 100,000 trips. The results are written to a text file.

COPY (
    -- ombine taxi trip data from relevant trip staging tables into a single table with unified columns
    with all_taxi_trips as (
        select
            'fhv' as type,
            pulocationid,
            dolocationid
        from {{ ref('stg__fhv_tripdata') }}
        union all
        select
            'fhvhv' as type,
            pulocationid,
            dolocationid
        from {{ ref('stg__fhvhv_tripdata') }}
        union all
        select
            'green' as type,
            pulocationid,
            dolocationid
        from {{ ref('stg__green_tripdata') }}
        union all
        select
            'yellow' as type,
            pulocationid,
            dolocationid
        from {{ ref('stg__yellow_tripdata') }}
    ),

    -- Map the pickup and dropoff locations to their respective zones
    all_zones as (
        select
            dl.zone,
            at.pulocationid,
            at.dolocationid
        from all_taxi_trips at
        left join {{ ref('mart__dim_locations') }} dl
        on at.pulocationid = dl.locationid
        or at.dolocationid = dl.locationid
    )

    -- identify zones with fewer than 100,000 trips and order by the number of trips
    select
        zone,
        count(*) as num_trips
    from all_zones
    group by zone
    having count(*) < 100000
    order by num_trips
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/zones_with_less_than_100k_trips.txt' (HEADER, DELIMITER ',');