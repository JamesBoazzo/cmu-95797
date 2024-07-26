-- days_before_precip_more_bike_trips.sql
-- This query calculates the average number of bike trips on days with precipitation or snow
-- and on the days immediately preceding such days. The results are written to a text file.

COPY (
    -- Identify days with precipitation or snow
    WITH weather_events AS (
        SELECT
            date,  -- Select the date
            CASE
                WHEN precipitation > 0 OR snowfall > 0 THEN 1  -- Mark days with precipitation or snow
                ELSE 0  -- Mark days without precipitation or snow
            END AS is_precip_or_snow  -- Alias the result as is_precip_or_snow for clarity
        FROM main_staging.stg__central_park_weather  -- Source table containing weather data
    ),
    
    -- Count the number of bike trips for each day
    bike_trips_by_day AS (
        SELECT
            CAST(started_at_ts AS DATE) AS trip_date,  -- Extract the date from the start timestamp
            COUNT(*) AS bike_trip_count  -- Count the number of bike trips for each day
        FROM {{ ref('mart__fact_all_bike_trips') }}  -- Source table containing bike trip data
        GROUP BY CAST(started_at_ts AS DATE)  -- Group by the extracted date
    ),
    
    -- Identify the days with precipitation or snow
    precip_or_snow_days AS (
        SELECT
            date AS precip_date  -- Rename the date column to precip_date for clarity
        FROM weather_events
        WHERE is_precip_or_snow = 1  -- Filter for days with precipitation or snow
    ),
    
    -- Identify the days immediately preceding the days with precipitation or snow
    preceding_days AS (
        SELECT
            date - INTERVAL 1 DAY AS preceding_date  -- Subtract one day from the date to get the preceding date
        FROM weather_events
        WHERE is_precip_or_snow = 1  -- Filter for days with precipitation or snow
    ),
    
    -- Calculate the average number of bike trips for both sets of days
    average_bike_trips AS (
        -- Calculate average bike trips on days with precipitation or snow
        SELECT
            'precip_or_snow' AS day_type,  -- Label these days as 'precip_or_snow'
            AVG(bike_trip_count) AS avg_bike_trips  -- Calculate the average number of bike trips
        FROM bike_trips_by_day
        WHERE trip_date IN (SELECT precip_date FROM precip_or_snow_days)  -- Filter for days with precipitation or snow
        
        UNION ALL
        
        -- Calculate average bike trips on days immediately preceding precipitation or snow
        SELECT
            'preceding' AS day_type,  -- Label these days as 'preceding'
            AVG(bike_trip_count) AS avg_bike_trips  -- Calculate the average number of bike trips
        FROM bike_trips_by_day
        WHERE trip_date IN (SELECT preceding_date FROM preceding_days)  -- Filter for the days immediately preceding precipitation or snow
    )
    SELECT *
    FROM average_bike_trips  -- Select all columns from the average_bike_trips CTE
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/days_before_precip_more_bike_trips.txt' (HEADER, DELIMITER ',');

