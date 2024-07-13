

-- models/staging/stg__fhv_tripdata.sql
-- This model cleans and renames columns from the source table 'fhv_tripdata'
-- It also handles null values by replacing them with appropriate default values
-- The original column (originating_base_number) is preserved and included in the output as original_dispatching_base_number.
-- No data is deleted. Instead, new column (dispatching_base_number_status) is created to indicate the validity of the values ('unknown'if not in fhv_bases table).

WITH source AS (
    -- Select all data from the source table 'fhv_tripdata' in the 'main' schema
    SELECT * FROM "nyc_transit"."main"."fhv_tripdata"
),

valid_bases AS (
    -- Select distinct base numbers from the stg__fhv_bases table
    SELECT DISTINCT trim(lower(base_number)) AS base_number
    FROM main_staging.stg__fhv_bases
),

renamed AS (
    -- Clean and rename columns, marking invalid base numbers
    SELECT 
        -- Ensure 'dispatching_base_num' is trimmed and lowercase
        lower(trim(dispatching_base_num)) AS dispatching_base_number,

        -- Split 'pickup_datetime' into 'pickup_date' and 'pickup_time', replace nulls with default datetime
        try_cast(coalesce(pickup_datetime, '1970-01-01 00:00:00') AS timestamp) AS pickup_datetime, -- Original column as timestamp
        cast(coalesce(pickup_datetime, '1970-01-01 00:00:00') AS date) AS pickup_date, -- Extract date part and rename to pickup_date
        cast(coalesce(pickup_datetime, '1970-01-01 00:00:00') AS time) AS pickup_time, -- Extract time part and rename to pickup_time

        -- Ensure 'dropoff_datetime' is a timestamp and split into 'dropoff_date' and 'dropoff_time', replace nulls with default datetime
        try_cast(coalesce(dropoff_datetime, '1970-01-01 00:00:00') AS timestamp) AS dropoff_datetime, -- Original column as timestamp
        cast(coalesce(dropoff_datetime, '1970-01-01 00:00:00') AS date) AS dropoff_date, -- Extract date part and rename
        cast(coalesce(dropoff_datetime, '1970-01-01 00:00:00') AS time) AS dropoff_time, -- Extract time part and rename

        -- Rename 'PUlocationID' to 'pickup_location_id' and ensure it is a string, replace nulls with 'unknown'
        try_cast(coalesce(PUlocationID, 'unknown') AS varchar) AS pickup_location_id,

        -- Rename 'DOlocationID' to 'dropoff_location_id' and ensure it is a string, replace nulls with 'unknown'
        try_cast(coalesce(DOlocationID, 'unknown') AS varchar) AS dropoff_location_id,

        -- Ensure 'affiliated_base_number' is all lowercase and a string, replace nulls with 'unknown' and trim whitespace
        try_cast(lower(trim(coalesce(affiliated_base_number, 'unknown'))) AS varchar) AS affiliated_base_number,

        -- Rename 'filename' to 'source_filename' and ensure it is a string, replace nulls with 'unknown' and trim whitespace
        try_cast(trim(coalesce(filename, 'unknown')) AS varchar) AS source_filename,

        -- Original value of 'dispatching_base_num'
        lower(trim(dispatching_base_num)) AS original_dispatching_base_number,

        -- Mark invalid base numbers as 'unknown'
        CASE 
            WHEN lower(trim(dispatching_base_num)) IN (SELECT base_number FROM valid_bases)
            THEN lower(trim(dispatching_base_num))
            ELSE 'unknown'
        END AS dispatching_base_number_status

    FROM source
)

-- Select all cleaned and renamed columns for the final output
SELECT 
    original_dispatching_base_number,
    dispatching_base_number_status,
    pickup_datetime,
    pickup_date,
    pickup_time,
    dropoff_datetime,
    dropoff_date,
    dropoff_time,
    pickup_location_id,
    dropoff_location_id,
    affiliated_base_number,
    source_filename
FROM renamed

