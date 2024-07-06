
-- models/staging/stg__central_park_weather_data.sql
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
with source as (
    -- Select all data from the source table 'central_park_weather' in the 'main' schema
    select * from "nyc_transit"."main"."central_park_weather"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Rename 'STATION' to 'station_id' and ensure it is a string, trimming whitespace
        try_cast(trim(STATION) as varchar) as station_id,

        -- Rename 'NAME' to 'station_name' and ensure it is a string, trimming whitespace
        try_cast(trim(NAME) as varchar) as station_name,

        -- Rename 'DATE' to 'date' and cast to date, trimming whitespace
        try_cast(trim(DATE) as date) as date,

        -- Rename 'AWND' to 'average_wind_speed', handle blank cells, and cast to double
        try_cast(trim(coalesce(AWND, '0')) as double) as average_wind_speed,

        -- Rename 'PRCP' to 'precipitation' and cast to double
        try_cast(PRCP as double) as precipitation,

        -- Rename 'SNOW' to 'snowfall', handle blank cells, and cast to double
        try_cast(trim(coalesce(SNOW, '0')) as double) as snowfall,

        -- Rename 'SNWD' to 'snow_depth' and cast to double
        try_cast(SNWD as double) as snow_depth,

        -- Rename 'TMAX' to 'max_temperature' and cast to int
        try_cast(TMAX as int) as max_temperature,

        -- Rename 'TMIN' to 'min_temperature' and cast to int
        try_cast(TMIN as int) as min_temperature,

        -- Rename 'FILENAME' to 'source_filename' and ensure it is a string, trimming whitespace
        try_cast(trim(FILENAME) as varchar) as source_filename
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
