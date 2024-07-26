-- nyc_transit/analyses/seven_day_moving_average_prcp.sql
-- This query calculates the seven-day moving average of precipitation (prcp) using lag and lead functions.
-- The results are written to a text file.
-- Used this for assistance https://mode.com/sql-tutorial/sql-window-functions#lag-and-lead
COPY (
    --  Calculate lag and lead values for precipitation
    WITH prcp_lag_lead AS (
        SELECT
            date,                                           -- Select the date
            precipitation AS prcp,                          -- Select the precipitation and alias it as prcp
            LAG(precipitation, 3) OVER (ORDER BY date) AS lag_3,    -- Get the precipitation value 3 days before the current date
            LAG(precipitation, 2) OVER (ORDER BY date) AS lag_2,    -- Get the precipitation value 2 days before the current date
            LAG(precipitation, 1) OVER (ORDER BY date) AS lag_1,    -- Get the precipitation value 1 day before the current date
            LEAD(precipitation, 1) OVER (ORDER BY date) AS lead_1,  -- Get the precipitation value 1 day after the current date
            LEAD(precipitation, 2) OVER (ORDER BY date) AS lead_2,  -- Get the precipitation value 2 days after the current date
            LEAD(precipitation, 3) OVER (ORDER BY date) AS lead_3   -- Get the precipitation value 3 days after the current date
        FROM main_staging.stg__central_park_weather          -- Source table containing weather data
    )

    -- Calculate the seven-day moving average of precipitation
    SELECT
        date,                                               
        prcp,                                               
        (
            prcp + 
            COALESCE(lag_1, 0) + COALESCE(lag_2, 0) + COALESCE(lag_3, 0) +  -- Sum the precipitation values for the current date and the 3 previous dates (if they exist)
            COALESCE(lead_1, 0) + COALESCE(lead_2, 0) + COALESCE(lead_3, 0) -- Sum the precipitation values for the 3 subsequent dates (if they exist)
        ) / 
        (
            1 +  -- Include the current day's precipitation in the average
            CASE WHEN lag_1 IS NOT NULL THEN 1 ELSE 0 END +  -- Add 1 for each of the 3 previous dates that have a precipitation value
            CASE WHEN lag_2 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN lag_3 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN lead_1 IS NOT NULL THEN 1 ELSE 0 END + -- Add 1 for each of the 3 subsequent dates that have a precipitation value
            CASE WHEN lead_2 IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN lead_3 IS NOT NULL THEN 1 ELSE 0 END
        ) AS seven_day_moving_avg_prcp  -- Calculate the seven-day moving average of precipitation
    FROM prcp_lag_lead                    -- Use the CTE with lag and lead values
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/seven_day_moving_average_prcp.txt' (HEADER, DELIMITER ',');





