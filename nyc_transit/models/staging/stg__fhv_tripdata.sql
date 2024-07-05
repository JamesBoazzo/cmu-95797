
  
  create view "nyc_transit"."main_staging"."stg__fhv_tripdata__dbt_tmp" as (
    -- models/staging/stg__fhv_tripdata.sql
-- This model cleans and renames columns from the source table 'fhv_tripdata'
-- It also eliminates the 'SR_Flag' column due to nulls throughout.
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
with source as (
    -- Select all data from the source table 'fhv_tripdata' in the 'main' schema
    select * from "nyc_transit"."main"."fhv_tripdata"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Ensure 'dispatching_base_num' is a string
        try_cast(dispatching_base_num as varchar) as dispatching_base_number,
        
        -- Split 'pickup_datetime' into 'pickup_date' and 'pickup_time'
        try_cast(pickup_datetime as timestamp) as pickup_datetime, -- Original column as timestamp
        cast(pickup_datetime as date) as pickup_date, -- Extract date part and rename to pickup_date
        cast(pickup_datetime as time) as pickup_time, -- Extract time part and rename to pickup_time
        
        -- Ensure 'dropoff_datetime' is lowercase and split into 'dropoff_date' and 'dropoff_time'
        try_cast(dropoff_datetime as timestamp) as dropoff_datetime, -- Original column as timestamp
        cast(dropoff_datetime as date) as dropoff_date, -- Extract date part and rename
        cast(dropoff_datetime as time) as dropoff_time, -- Extract time part and rename
        
        -- Rename 'PUlocationID' to 'pickup_location_id' and ensure it is a string
        try_cast(PUlocationID as varchar) as pickup_location_id,
        
        -- Rename 'DOlocationID' to 'dropoff_location_id' and ensure it is a string
        try_cast(DOlocationID as varchar) as dropoff_location_id,
		
		-- Drop 'SR_Flag' due to NULLS' in the entire column
        
        -- Ensure 'affiliated_base_number' is all lowercase and a string using try_cast, trim any whitespace if needed
        try_cast(lower(affiliated_base_number) as varchar) as affiliated_base_number,
		
		 -- Rename 'filename' to 'source_filename' and ensure it is a string
        try_cast(trim(filename) as varchar) as source_filename
        
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
  );
