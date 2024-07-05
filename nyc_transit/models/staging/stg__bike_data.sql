
  
  create view "nyc_transit"."main_staging"."stg__bike_data__dbt_tmp" as (
    -- models/staging/stg__bike_data.sql
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
-- Define the DBT model for cleaning and renaming columns
--Possible duplicates but didn't remove as they do not appear to be PK per instructions.  Ride_id is the most likely candidate for PK.
with source as (
    -- Select all data from the source table 'bike_data' in the 'main' schema
    select * from "nyc_transit"."main"."bike_data"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Ensure 'tripduration' is a string and trim whitespace
		-- Numerous instances of very high values (e.g. >100,000); however, I opted to keep them as is b/c data is in seconds and may indicate long term rentals.  
		-- Also, I will perform querying on these cleaned values later.
        try_cast(trim(tripduration) as varchar) as trip_duration,

        -- Ensure 'starttime' is a timestamp
        try_cast(starttime as timestamp) as start_time,

        -- Ensure 'stoptime' is a timestamp
        try_cast(stoptime as timestamp) as stop_time,

        -- Ensure 'start station id' is a string and trim whitespace
        try_cast(trim("start station id") as varchar) as start_station_id,

        -- Ensure 'start station name' is a string and trim whitespace
        try_cast(trim("start station name") as varchar) as start_station_name,

        -- Ensure 'start station latitude' is a double
        try_cast("start station latitude" as double) as start_station_latitude,

        -- Ensure 'start station longitude' is a double
        try_cast("start station longitude" as double) as start_station_longitude,

        -- Ensure 'end station id' is a string and trim whitespace
        try_cast(trim("end station id") as varchar) as end_station_id,

        -- Ensure 'end station name' is a string and trim whitespace
        try_cast(trim("end station name") as varchar) as end_station_name,

        -- Ensure 'end station latitude' is a double
        try_cast("end station latitude" as double) as end_station_latitude,

        -- Ensure 'end station longitude' is a double
        try_cast("end station longitude" as double) as end_station_longitude,

        -- Ensure 'bikeid' is a string and trim whitespace
        try_cast(trim(bikeid) as varchar) as bike_id,

        -- Ensure 'usertype' is a string and trim whitespace
        try_cast(trim(usertype) as varchar) as user_type,

        -- Ensure 'birth year' is a string and trim whitespace, set to NULL if > 1924 (to handle ages >100)
        case when trim("birth year") > '1924' then NULL else trim("birth year") end as birth_year,

        -- Ensure 'gender' is a string, trim whitespace, and convert numerical codes to descriptive text
        case 
            when trim(gender) = '0' then 'unknown'
            when trim(gender) = '1' then 'male'
            when trim(gender) = '2' then 'female'
            else 'Other'
        end as gender,

        -- Ensure 'ride_id' is a string and trim whitespace
        try_cast(trim(ride_id) as varchar) as ride_id,

        -- Ensure 'rideable_type' is a string and trim whitespace
        try_cast(trim(rideable_type) as varchar) as rideable_type,

        -- Split 'started_at' into 'started_at_date' and 'started_at_time', ensure it is trimmed
        try_cast(trim(started_at) as timestamp) as started_at,
        cast(trim(started_at) as date) as started_at_date,
        cast(trim(started_at) as time) as started_at_time,

        -- Split 'ended_at' into 'ended_at_date' and 'ended_at_time', ensure it is trimmed
        try_cast(trim(ended_at) as timestamp) as ended_at,
        cast(trim(ended_at) as date) as ended_at_date,
        cast(trim(ended_at) as time) as ended_at_time,

        -- Ensure 'start_station_name' is a string and trim whitespace
        try_cast(trim(start_station_name) as varchar) as start_station_name,

        -- Ensure 'start_station_id' is a string and trim whitespace
        try_cast(trim(start_station_id) as varchar) as start_station_id,

        -- Ensure 'end_station_name' is a string and trim whitespace
        try_cast(trim(end_station_name) as varchar) as end_station_name,

        -- Ensure 'end_station_id' is a string and trim whitespace
        try_cast(trim(end_station_id) as varchar) as end_station_id,

        -- Ensure 'start_lat' is a double.  
        try_cast(start_lat as double) as start_latitude,

        -- Ensure 'start_lng' is a double
        try_cast(start_lng as double) as start_longitude,

        -- Ensure 'end_lat' is a double
        try_cast(end_lat as double) as end_latitude,

        -- Ensure 'end_lng' is a double
        try_cast(end_lng as double) as end_longitude,

        -- Ensure 'member_casual' is a string and trim whitespace
        try_cast(trim(member_casual) as varchar) as member_casual,

        -- Ensure 'filename' is a string and trim whitespace
        try_cast(trim(filename) as varchar) as source_filename
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
  );
