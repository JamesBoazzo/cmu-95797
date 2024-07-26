-- nyc_transit/analyses/yellow_taxi_fare_comparison.sql
-- This query processes yellow taxi trip data, joins it with location information,
-- and compares fare amounts with overall, borough, and zone averages.
-- Assistance from https://mode.com/sql-tutorial/sql-window-functions for this problem.
-- Do not print as per instructions
-- Extract relevant fields from yellow taxi trip data and join with location data
with yellow_taxi_data as (
    select
        yt.fare_amount,              
        yt.tpep_pickup_datetime,     
        yt.pulocationid,             
        dl.zone,                    
        dl.borough                  
    from {{ ref('stg__yellow_tripdata') }} yt  -- Reference the yellow taxi trip staging table
    left join {{ ref('mart__dim_locations') }} dl  -- Join with the location dimension table
    on yt.pulocationid = dl.locationid            -- Match the pickup location ID with location ID in dimension table
)

-- Select data and calculate average fares
select
    tpep_pickup_datetime,           -- Select pickup datetime for the output
    fare_amount,                    -- Select fare amount for the output
    zone,                           -- Select zone information for the output
    borough,                        -- Select borough information for the output
    avg(fare_amount) over () as overall_avg_fare,               -- Calculate overall average fare across all trips
    avg(fare_amount) over (partition by borough) as borough_avg_fare,  -- Calculate average fare partitioned by borough
    avg(fare_amount) over (partition by zone) as zone_avg_fare  -- Calculate average fare partitioned by zone
from yellow_taxi_data              -- Use the CTE with joined yellow taxi data and location information

