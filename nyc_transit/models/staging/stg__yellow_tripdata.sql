
-- models/staging/stg__yellow_tripdata.sql
-- This model cleans and renames columns from the source table 'yellow_tripdata'
-- It also eliminates irrelevant columns 'airport_fee' and handles NULL values appropriately.
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
--Despite querying slide 38 of week1 slides, I could not identify, for instance, what the 1,2, or 4 meant in payment_type.  Therefore, I kept it as is for my model.
with source as (
    -- Select all data from the source table 'yellow_tripdata' in the 'main' schema
    select * from "nyc_transit"."main"."yellow_tripdata"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Rename 'VendorID' to 'vendor_id' and ensure it is a string
        try_cast(VendorID as varchar) as vendor_id,
        
        -- Split 'tpep_pickup_datetime' into 'tpep_pickup_date' and 'tpep_pickup_time'
        try_cast(tpep_pickup_datetime as timestamp) as tpep_pickup_datetime, -- Original column as timestamp
        cast(tpep_pickup_datetime as date) as tpep_pickup_date, -- Extract date part and rename to tpep_pickup_date
        cast(tpep_pickup_datetime as time) as tpep_pickup_time, -- Extract time part and rename to tpep_pickup_time
        
        -- Split 'tpep_dropoff_datetime' into 'tpep_dropoff_date' and 'tpep_dropoff_time'
        try_cast(tpep_dropoff_datetime as timestamp) as tpep_dropoff_datetime, -- Original column as timestamp
        cast(tpep_dropoff_datetime as date) as tpep_dropoff_date, -- Extract date part and rename to tpep_dropoff_date
        cast(tpep_dropoff_datetime as time) as tpep_dropoff_time, -- Extract time part and rename to tpep_dropoff_time
        
        -- Ensure 'passenger_count' remains as a double
        try_cast(passenger_count as double) as passenger_count,
        
        -- Ensure 'trip_distance' remains as a double
        try_cast(trip_distance as double) as trip_distance,
        
        -- Rename 'RatecodeID' to 'rate_code_id' and ensure it is a string
        try_cast(RatecodeID as varchar) as rate_code_id,
        
        -- Ensure 'store_and_fwd_flag' is a string.  Although all listed as 'N' chose to keep until further data exploration.
        try_cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,

        -- Rename 'PULocationID' to 'pickup_location_id' and ensure it is a string
        try_cast(PULocationID as varchar) as pickup_location_id,

        -- Rename 'DOLocationID' to 'dropoff_location_id' and ensure it is a string
        try_cast(DOLocationID as varchar) as dropoff_location_id,

        -- Ensure 'payment_type' remains as a double
        try_cast(payment_type as double) as payment_type,

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

        -- Ensure 'improvement_surcharge' remains as a double
        try_cast(improvement_surcharge as double) as improvement_surcharge,

        -- Ensure 'total_amount' remains as a double
        try_cast(total_amount as double) as total_amount,

        -- Ensure 'congestion_surcharge' remains as a double
        try_cast(congestion_surcharge as double) as congestion_surcharge,
		
		--Eliminate 'airport_fee' due to NULLS in the entire column

        -- Rename 'filename' to 'source_filename' and ensure it is a string
        try_cast(filename as varchar) as source_filename
        
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
