-- models/staging/stg__fhvhv_tripdata.sql
-- The original column (originating_base_number) is preserved and included in the output as original_dispatching_base_number.  Same for original_originating_base_number
-- No data is deleted. Instead, new columns (originating_base_number_status and dispatching_base_number_status) are created to indicate the validity of the values ('unknown'if not in fhv_bases table).
-- Assistance from https://stackoverflow.com/questions/tagged/sql to build SQL code
WITH source AS (
    -- Select all data from the source table 'fhvhv_tripdata' in the 'main' schema
    SELECT * 
    FROM "nyc_transit"."main"."fhvhv_tripdata"
),

valid_bases AS (
    -- Select distinct base numbers from the stg__fhv_bases table
    SELECT DISTINCT trim(lower(base_number)) AS base_number
    FROM main_staging.stg__fhv_bases
),

renamed AS (
    -- Clean and rename columns, marking invalid base numbers
    SELECT 
        -- Rename 'hvfhs_license_num' to 'hvfs_license_number' and ensure it is a string
        try_cast(trim(hvfhs_license_num) AS varchar) AS hvfs_license_number,

        -- Original value of 'dispatching_base_num'
        lower(trim(dispatching_base_num)) AS original_dispatching_base_number,

        -- Mark invalid base numbers as 'unknown'
        CASE 
            WHEN lower(trim(dispatching_base_num)) IN (SELECT base_number FROM valid_bases)
            THEN lower(trim(dispatching_base_num))
            ELSE 'unknown'
        END AS dispatching_base_number_status,

        -- Original value of 'originating_base_num'
        lower(trim(originating_base_num)) AS original_originating_base_number,

        -- Mark invalid base numbers as 'unknown'
        CASE 
            WHEN lower(trim(originating_base_num)) IN (SELECT base_number FROM valid_bases)
            THEN lower(trim(originating_base_num))
            ELSE 'unknown'
        END AS originating_base_number_status,

        -- Split 'request_datetime' into 'request_date' and 'request_time', handle NULL values
        try_cast(COALESCE(request_datetime, '1970-01-01 00:00:00') AS timestamp) AS request_datetime, -- Original column as timestamp with default value for NULLs
        cast(COALESCE(request_datetime, '1970-01-01 00:00:00') AS date) AS request_date, -- Extract date part and rename to request_date with default value for NULLs
        cast(COALESCE(request_datetime, '1970-01-01 00:00:00') AS time) AS request_time, -- Extract time part and rename to request_time with default value for NULLs
        
        -- Handle NULLs for 'on_scene_datetime', split into 'on_scene_date' and 'on_scene_time'
        try_cast(COALESCE(on_scene_datetime, '1970-01-01 00:00:00') AS timestamp) AS on_scene_datetime, -- Original column as timestamp
        cast(on_scene_datetime AS date) AS on_scene_date, -- Extract date part and rename to on_scene_date
        cast(on_scene_datetime AS time) AS on_scene_time, -- Extract time part and rename to on_scene_time
        
        -- Split 'pickup_datetime' into 'pickup_date' and 'pickup_time'
        try_cast(pickup_datetime AS timestamp) AS pickup_datetime, -- Original column as timestamp
        cast(pickup_datetime AS date) AS pickup_date, -- Extract date part and rename to pickup_date
        cast(pickup_datetime AS time) AS pickup_time, -- Extract time part and rename to pickup_time
        
        -- Split 'dropoff_datetime' into 'dropoff_date' and 'dropoff_time'
        try_cast(dropoff_datetime AS timestamp) AS dropoff_datetime, -- Original column as timestamp
        cast(dropoff_datetime AS date) AS dropoff_date, -- Extract date part and rename to dropoff_date
        cast(dropoff_datetime AS time) AS dropoff_time, -- Extract time part and rename to dropoff_time
        
        -- Rename 'PULocationID' to 'pickup_location_id', ensure all lowercase, and cast to string
        try_cast(lower(trim(cast(PULocationID AS varchar))) AS varchar) AS pickup_location_id,

        -- Rename 'DOLocationID' to 'dropoff_location_id', ensure all lowercase, and cast to string
        try_cast(lower(trim(cast(DOLocationID AS varchar))) AS varchar) AS dropoff_location_id,
        
        -- Ensure 'trip_miles' remains as a double
        try_cast(trip_miles AS double) AS trip_miles,

        -- Ensure 'trip_time' remains as a bigint
        try_cast(trip_time AS bigint) AS trip_time,

        -- Ensure 'base_passenger_fare' remains as a double
        try_cast(base_passenger_fare AS double) AS base_passenger_fare,
        
        -- Ensure 'tolls' remains as a double
        try_cast(tolls AS double) AS tolls,

        -- Rename 'bcf' to 'black_car_fund' and ensure it remains as a double
        try_cast(bcf AS double) AS black_car_fund,

        -- Ensure 'sales_tax' remains as a double
        try_cast(sales_tax AS double) AS sales_tax,

        -- Ensure 'congestion_surcharge' remains as a double
        try_cast(congestion_surcharge AS double) AS congestion_surcharge,
        
        -- Eliminate 'airport_fee' due to NULLS in the entire column
        
        -- Ensure 'tips' remains as a double
        try_cast(tips AS double) AS tips,

        -- Ensure 'driver_pay' remains as a double
        try_cast(driver_pay AS double) AS driver_pay,
        
        -- Rename 'shared_request_flag' to 'is_shared_request', handle 'Y'/'N' as boolean, and cast to boolean
        CASE
            WHEN trim(shared_request_flag) = 'Y' THEN TRUE
            WHEN trim(shared_request_flag) = 'N' THEN FALSE
            ELSE 'unknown' -- Handle NULL values as 'unknown'
        END AS is_shared_request,

        -- Rename 'shared_match_flag' to 'is_shared_match', handle 'Y'/'N' as boolean, and cast to boolean
        CASE
            WHEN trim(shared_match_flag) = 'Y' THEN TRUE
            WHEN trim(shared_match_flag) = 'N' THEN FALSE
            ELSE 'unknown' -- Handle NULL values as 'unknown'
        END AS is_shared_match,

        -- Rename 'access_a_ride_flag' to 'is_access_a_ride', handle 'Y'/'N' as boolean, and cast to boolean
        CASE
            WHEN trim(access_a_ride_flag) = 'Y' THEN TRUE
            WHEN trim(access_a_ride_flag) = 'N' THEN FALSE
            ELSE 'unknown' -- Handle NULL values as 'unknown'
        END AS is_access_a_ride,

        -- Rename 'wav_request_flag' to 'is_wheelchair_accessible_request', handle 'Y'/'N' as boolean, and cast to boolean
        CASE
            WHEN trim(wav_request_flag) = 'Y' THEN TRUE
            WHEN trim(wav_request_flag) = 'N' THEN FALSE
            ELSE 'unknown' -- Handle NULL values as 'unknown'
        END AS is_wheelchair_accessible_request,

        -- Rename 'wav_match_flag' to 'is_wheelchair_accessible_match', handle 'Y'/'N' as boolean, and cast to boolean
        CASE
            WHEN trim(wav_match_flag) = 'Y' THEN TRUE
            WHEN trim(wav_match_flag) = 'N' THEN FALSE
            ELSE 'unknown' -- Handle NULL values as 'unknown'
        END AS is_wheelchair_accessible_match,

        -- Rename 'filename' to 'source_filename' and ensure it is a string
        try_cast(trim(filename) AS varchar) AS source_filename
        
    FROM source
)

-- Select all cleaned and renamed columns for the final output
SELECT 
    hvfs_license_number,
    original_dispatching_base_number,
    dispatching_base_number_status,
    original_originating_base_number,
    originating_base_number_status,
    request_datetime,
    request_date,
    request_time,
    on_scene_datetime,
    on_scene_date,
    on_scene_time,
    pickup_datetime,
    pickup_date,
    pickup_time,
    dropoff_datetime,
    dropoff_date,
    dropoff_time,
    pickup_location_id,
    dropoff_location_id,
    trip_miles,
    trip_time,
    base_passenger_fare,
    tolls,
    black_car_fund,
    sales_tax,
    congestion_surcharge,
    tips,
    driver_pay,
    is_shared_request,
    is_shared_match,
    is_access_a_ride,
    is_wheelchair_accessible_request,
    is_wheelchair_accessible_match,
    source_filename
FROM renamed
