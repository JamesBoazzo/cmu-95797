
-- models/staging/stg__bike_data.sql
-- Assistance from https://stackoverflow.com/questions/tagged/sql to build SQL code
-- Define the DBT model for cleaning and renaming columns-- Updated ride_id below to ensure data integrity (see comments below for updated SQL code and explanation).


WITH source AS (
    -- Select all data from the source table 'bike_data' in the 'main' schema
    SELECT * FROM "nyc_transit"."main"."bike_data"
),

renamed AS (
    -- Clean and rename columns
    SELECT 
        -- Ensure 'tripduration' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(tripduration) AS VARCHAR), '0') AS trip_duration,

        -- Ensure 'starttime' is a timestamp and handle empty cells
        COALESCE(TRY_CAST(starttime AS TIMESTAMP), '1970-01-01 00:00:00') AS start_time,

        -- Ensure 'stoptime' is a timestamp and handle empty cells
        COALESCE(TRY_CAST(stoptime AS TIMESTAMP), '1970-01-01 00:00:00') AS stop_time,

        -- Ensure 'start station id' is a string, trim whitespace, and handle empty cells
        COALESCE(TRIM("start station id"), TRIM(start_station_id), 'Unknown') AS start_station_id,

        -- Ensure 'start station name' is a string, trim whitespace, and handle empty cells
        COALESCE(TRIM("start station name"), TRIM(start_station_name), 'Unknown') AS start_station_name,

        -- Ensure 'start station latitude' is a double and handle empty cells
        COALESCE(TRY_CAST("start station latitude" AS DOUBLE), 0.0) AS start_station_latitude,

        -- Ensure 'start station longitude' is a double and handle empty cells
        COALESCE(TRY_CAST("start station longitude" AS DOUBLE), 0.0) AS start_station_longitude,

        -- Ensure 'end station id' is a string, trim whitespace, and handle empty cells
        COALESCE(TRIM("end station id"), TRIM(end_station_id), 'Unknown') AS end_station_id,

        -- Ensure 'end station name' is a string, trim whitespace, and handle empty cells
        COALESCE(TRIM("end station name"), TRIM(end_station_name), 'Unknown') AS end_station_name,

        -- Ensure 'end station latitude' is a double and handle empty cells
        COALESCE(TRY_CAST("end station latitude" AS DOUBLE), 0.0) AS end_station_latitude,

        -- Ensure 'end station longitude' is a double and handle empty cells
        COALESCE(TRY_CAST("end station longitude" AS DOUBLE), 0.0) AS end_station_longitude,

        -- Ensure 'bikeid' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(bikeid) AS VARCHAR), 'Unknown') AS bike_id,

        -- Ensure 'usertype' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(usertype) AS VARCHAR), 'Unknown') AS user_type,

        -- Ensure 'birth year' is a string, trim whitespace, set to NULL if < 1924, and handle empty cells
        CASE WHEN TRIM("birth year") < '1924' THEN NULL ELSE COALESCE(TRIM("birth year"), 'Unknown') END AS birth_year,

        -- Ensure 'gender' is a string, trim whitespace, convert numerical codes to descriptive text, and handle empty cells
        COALESCE(
            CASE 
                WHEN LOWER(TRIM(gender)) IN ('unknown', 'other') THEN '0'
                WHEN LOWER(TRIM(gender)) = 'male' THEN '1'
                WHEN LOWER(TRIM(gender)) = 'female' THEN '2'
            ELSE '0'
        END) AS gender,
       
        -- Ensure 'ride_id' is a string, trim whitespace, and handle empty cells by providing unique 'unknown_id' (e.g. 'unknown_1,' 'unknown_2', etc.)
        CASE 
            WHEN TRIM(ride_id) IS NULL OR TRIM(ride_id) = '' THEN 'Unknown_' || ROW_NUMBER() OVER () 
            ELSE TRIM(ride_id)
        END AS ride_id,

        -- Ensure 'rideable_type' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(rideable_type) AS VARCHAR), 'Unknown') AS rideable_type,

        -- Split 'started_at' into 'started_at_date' and 'started_at_time', ensure it is trimmed, and handle empty cells
        COALESCE(TRY_CAST(TRIM(started_at) AS TIMESTAMP), '1970-01-01 00:00:00') AS started_at,
        COALESCE(CAST(TRIM(started_at) AS DATE), '1970-01-01') AS started_at_date,
        COALESCE(CAST(TRIM(started_at) AS TIME), '00:00:00') AS started_at_time,

        -- Split 'ended_at' into 'ended_at_date' and 'ended_at_time', ensure it is trimmed, and handle empty cells
        COALESCE(TRY_CAST(TRIM(ended_at) AS TIMESTAMP), '1970-01-01 00:00:00') AS ended_at,
        COALESCE(CAST(TRIM(ended_at) AS DATE), '1970-01-01') AS ended_at_date,
        COALESCE(CAST(TRIM(ended_at) AS TIME), '00:00:00') AS ended_at_time,

        -- Ensure 'start_lat' is a double and handle empty cells
        COALESCE(TRY_CAST(start_lat AS DOUBLE), 0.0) AS start_latitude,

        -- Ensure 'start_lng' is a double and handle empty cells
        COALESCE(TRY_CAST(start_lng AS DOUBLE), 0.0) AS start_longitude,

        -- Ensure 'end_lat' is a double and handle empty cells
        COALESCE(TRY_CAST(end_lat AS DOUBLE), 0.0) AS end_latitude,

        -- Ensure 'end_lng' is a double and handle empty cells
        COALESCE(TRY_CAST(end_lng AS DOUBLE), 0.0) AS end_longitude,

        -- Ensure 'member_casual' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(member_casual) AS VARCHAR), 'Unknown') AS member_casual,

        -- Ensure 'filename' is a string, trim whitespace, and handle empty cells
        COALESCE(TRY_CAST(TRIM(filename) AS VARCHAR), 'Unknown') AS source_filename
    FROM source
)

SELECT * FROM renamed
