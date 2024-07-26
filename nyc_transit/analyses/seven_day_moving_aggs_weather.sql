-- seven_day_moving_aggs_weather.sql
-- This query calculates seven-day moving aggregates for precipitation and snowfall data.
-- The results are written to a text file.
-- https://mode.com/sql-tutorial/sql-window-functions#defining-a-window-alias for assistance
COPY (
    SELECT
        date,                                        
        precipitation,                                
        snowfall,                                    
        MIN(precipitation) OVER seven_days AS min_precipitation,  -- Calculate the minimum precipitation over a 7-day window
        MAX(precipitation) OVER seven_days AS max_precipitation,  -- Calculate the maximum precipitation over a 7-day window
        AVG(precipitation) OVER seven_days AS avg_precipitation,  -- Calculate the average precipitation over a 7-day window
        SUM(precipitation) OVER seven_days AS sum_precipitation,  -- Calculate the total precipitation over a 7-day window
        MIN(snowfall) OVER seven_days AS min_snow,                -- Calculate the minimum snowfall over a 7-day window
        MAX(snowfall) OVER seven_days AS max_snow,                -- Calculate the maximum snowfall over a 7-day window
        AVG(snowfall) OVER seven_days AS avg_snow,                -- Calculate the average snowfall over a 7-day window
        SUM(snowfall) OVER seven_days AS sum_snow                 -- Calculate the total snowfall over a 7-day window
    FROM main_staging.stg__central_park_weather                   -- Source table containing Central Park weather data
    WINDOW seven_days AS (                                        -- Define a window of 7 days
        ORDER BY date ASC                                         -- Order the window by date in ascending order
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING                  -- Include 3 days before and 3 days after the current row
    )
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/seven_day_moving_aggs_weather.txt' (HEADER, DELIMITER ',');
