
  

    -- models/staging/stg__green_tripdata.sql
-- This model cleans and renames columns from the source table 'green_tripdata'
-- It also eliminates irrelevant columns (ehail_fee) and handles NULL values appropriately.
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
--Despite querying slide 38 of week1 slides, I could not identify, for instance, what the 1 or 2 meant in trip_type.  Therefore, I kept it as is for my model.
with source as (
    -- Select all data from the source table 'green_tripdata' in the 'main' schema
    select * from "nyc_transit"."main"."green_tripdata"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Rename 'VendorID' to 'vendor_id' and ensure it is a string
        try_cast(VendorID as varchar) as vendor_id,
        
        -- Split 'lpep_pickup_datetime' into 'lpep_pickup_date' and 'lpep_pickup_time'
        try_cast(lpep_pickup_datetime as timestamp) as lpep_pickup_datetime, -- Original column as timestamp
        cast(lpep_pickup_datetime as date) as lpep_pickup_date, -- Extract date part and rename to lpep_pickup_date
        cast(lpep_pickup_datetime as time) as lpep_pickup_time, -- Extract time part and rename to lpep_pickup_time
        
        -- Split 'lpep_dropoff_datetime' into 'lpep_dropoff_date' and 'lpep_dropoff_time'
        try_cast(lpep_dropoff_datetime as timestamp) as lpep_dropoff_datetime, -- Original column as timestamp
        cast(lpep_dropoff_datetime as date) as lpep_dropoff_date, -- Extract date part and rename to lpep_dropoff_date
        cast(lpep_dropoff_datetime as time) as lpep_dropoff_time, -- Extract time part and rename to lpep_dropoff_time
        
        -- Ensure 'store_and_fwd_flag' is a string.  All listed as 'N' but chose to keep until further data exploration.
        try_cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,

        -- Rename 'RatecodeID' to 'rate_code_id' and ensure it is a string
        try_cast(RatecodeID as varchar) as rate_code_id,

        -- Rename 'PULocationID' to 'pickup_location_id' and ensure it is a string
        try_cast(PULocationID as varchar) as pickup_location_id,

        -- Rename 'DOLocationID' to 'dropoff_location_id' and ensure it is a string
        try_cast(DOLocationID as varchar) as dropoff_location_id,

        -- Ensure 'passenger_count' remains as a double
        try_cast(passenger_count as double) as passenger_count,

        -- Ensure 'trip_distance' remains as a double
        try_cast(trip_distance as double) as trip_distance,

        -- Ensure 'fare_amount' remains as a double
        try_cast(fare_amount as double) as fare_amount,

        -- Ensure 'extra' remains as a double
        try_cast(extra as double) as extra,

        -- Ensure 'mta_tax' remains as a double
        try_cast(mta_tax as double) as mta_tax,

        -- Ensure 'tip_amount' remains as a double
        try_cast(tip_amount as double) as tip_amount,

        -- Ensure 'tolls_amount' remains as a double
        try_cast(tolls_amount as double) as tolls_amount,
		
		--Eliminate ehail_fee due to NULLS in the entire column

        -- Ensure 'improvement_surcharge' remains as a double
        try_cast(improvement_surcharge as double) as improvement_surcharge,

        -- Ensure 'total_amount' remains as a double
        try_cast(total_amount as double) as total_amount,

        -- Ensure 'payment_type' remains as a double
        try_cast(payment_type as double) as payment_type,

        -- Ensure 'trip_type' remains as a double
        try_cast(trip_type as double) as trip_type,

        -- Ensure 'congestion_surcharge' remains as a double
        try_cast(congestion_surcharge as double) as congestion_surcharge,

        -- Rename 'filename' to 'source_filename' and ensure it is a string
        try_cast(filename as varchar) as source_filename
        
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed

