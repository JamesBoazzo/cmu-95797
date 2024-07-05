
  
  create view "nyc_transit"."main_staging"."stg__fhvhv_tripdata__dbt_tmp" as (
    -- models/staging/stg__fhvhv_tripdata.sql
-- This model cleans and renames columns from the source table 'fhvhv_tripdata'
-- It also eliminates unnecessary columns ('airport_fee') and handles NULL values appropriately.
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
with source as (
    -- Select all data from the source table 'fhvhv_tripdata' in the 'main' schema
    select * from "nyc_transit"."main"."fhvhv_tripdata"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Rename 'hvfhs_license_num' to 'hvfs_license_number' and ensure it is a string
        try_cast(trim(hvfhs_license_num) as varchar) as hvfs_license_number,
        
        -- Rename 'dispatching_base_num' to 'dispatching_base_number' and ensure it is a string
        try_cast(trim(dispatching_base_num) as varchar) as dispatching_base_number,
        
        -- Rename 'originating_base_num' to 'originating_base_number', ensure it is a string, and handle NULLs
        try_cast(COALESCE(trim(originating_base_num), 'UNKNOWN') as varchar) as originating_base_number,
        
        -- Split 'request_datetime' into 'request_date' and 'request_time'
        try_cast(request_datetime as timestamp) as request_datetime, -- Original column as timestamp
        cast(request_datetime as date) as request_date, -- Extract date part and rename to request_date
        cast(request_datetime as time) as request_time, -- Extract time part and rename to request_time
        
        -- Handle NULLs for 'on_scene_datetime', split into 'on_scene_date' and 'on_scene_time'
        try_cast(COALESCE(on_scene_datetime, '1970-01-01 00:00:00') as timestamp) as on_scene_datetime, -- Original column as timestamp
        cast(on_scene_datetime as date) as on_scene_date, -- Extract date part and rename to on_scene_date
        cast(on_scene_datetime as time) as on_scene_time, -- Extract time part and rename to on_scene_time
        
        -- Split 'pickup_datetime' into 'pickup_date' and 'pickup_time'
        try_cast(pickup_datetime as timestamp) as pickup_datetime, -- Original column as timestamp
        cast(pickup_datetime as date) as pickup_date, -- Extract date part and rename to pickup_date
        cast(pickup_datetime as time) as pickup_time, -- Extract time part and rename to pickup_time
        
        -- Split 'dropoff_datetime' into 'dropoff_date' and 'dropoff_time'
        try_cast(dropoff_datetime as timestamp) as dropoff_datetime, -- Original column as timestamp
        cast(dropoff_datetime as date) as dropoff_date, -- Extract date part and rename to dropoff_date
        cast(dropoff_datetime as time) as dropoff_time, -- Extract time part and rename to dropoff_time
        
        -- Rename 'PULocationID' to 'pickup_location_id', ensure all lowercase, and cast to string
        try_cast(lower(trim(cast(PULocationID as varchar))) as varchar) as pickup_location_id,

        -- Rename 'DOLocationID' to 'dropoff_location_id', ensure all lowercase, and cast to string
        try_cast(lower(trim(cast(DOLocationID as varchar))) as varchar) as dropoff_location_id,
        
        -- Ensure 'trip_miles' remains as a double
        try_cast(trip_miles as double) as trip_miles,

        -- Ensure 'trip_time' remains as a bigint
        try_cast(trip_time as bigint) as trip_time,

        -- Ensure 'base_passenger_fare' remains as a double
        try_cast(base_passenger_fare as double) as base_passenger_fare,
        
        -- Ensure 'tolls' remains as a double
        try_cast(tolls as double) as tolls,

        -- Rename 'bcf' to 'black_car_fund' and ensure it remains as a double
        try_cast(bcf as double) as black_car_fund,

        -- Ensure 'sales_tax' remains as a double
        try_cast(sales_tax as double) as sales_tax,

        -- Ensure 'congestion_surcharge' remains as a double
        try_cast(congestion_surcharge as double) as congestion_surcharge,
		
		--Eliminate 'airport_fee' due to NULLS in the entire column
        
        -- Ensure 'tips' remains as a double
        try_cast(tips as double) as tips,

        -- Ensure 'driver_pay' remains as a double
        try_cast(driver_pay as double) as driver_pay,
        
        -- Rename 'shared_request_flag' to 'is_shared_request', handle blanks as 'UNKNOWN', and cast to string
        try_cast(COALESCE(trim(shared_request_flag), 'UNKNOWN') as varchar) as is_shared_request,

        -- Rename 'shared_match_flag' to 'is_shared_match', handle blanks as 'UNKNOWN', and cast to string
        try_cast(COALESCE(trim(shared_match_flag), 'UNKNOWN') as varchar) as is_shared_match,

        -- Rename 'access_a_ride_flag' to 'is_access_a_ride', handle blanks as 'UNKNOWN', and cast to string
        try_cast(COALESCE(trim(access_a_ride_flag), 'UNKNOWN') as varchar) as is_access_a_ride,

        -- Rename 'wav_request_flag' to 'is_wheelchair_accessible_request', handle blanks as 'UNKNOWN', and cast to string
        try_cast(COALESCE(trim(wav_request_flag), 'UNKNOWN') as varchar) as is_wheelchair_accessible_request,

        -- Rename 'wav_match_flag' to 'is_wheelchair_accessible_match', handle blanks as 'UNKNOWN', and cast to string
        try_cast(COALESCE(trim(wav_match_flag), 'UNKNOWN') as varchar) as is_wheelchair_accessible_match,

        -- Rename 'filename' to 'source_filename' and ensure it is a string
        try_cast(trim(filename) as varchar) as source_filename
        
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
  );
