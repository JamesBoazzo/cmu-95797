-- average_time_between_pickups.sql
-- This query calculates the average time between pickups for each pickup zone.
-- The results are written to a text file.
-- https://mode.com/sql-tutorial/sql-window-functions#lag-and-lead for assistance with this query 
COPY (
    -- Calculate the time difference between consecutive pickups
    WITH pickup_differences AS (
        SELECT
            z.zone AS pickup_zone,  -- Use zone name instead of pulocationid for clarity
            t.pickup_datetime,  -- Select the pickup datetime for each trip
            LEAD(t.pickup_datetime) OVER (PARTITION BY t.pulocationid ORDER BY t.pickup_datetime) AS next_pickup_datetime,  -- Get the next pickup datetime for the same zone
            datediff('minute', t.pickup_datetime, LEAD(t.pickup_datetime) OVER (PARTITION BY t.pulocationid ORDER BY t.pickup_datetime)) AS time_diff  -- Calculate the time difference in minutes between consecutive pickups
        FROM {{ ref('mart__fact_all_taxi_trips') }} t  -- Source table containing all taxi trips
        JOIN {{ ref('taxi+_zone_lookup') }} z ON t.pulocationid = z.locationid  -- Join with zone lookup table to get the zone name
    )

    -- Calculate the average time difference between pickups for each pickup zone
    SELECT
        pickup_zone,  -- Select the pickup zone
        AVG(time_diff) AS avg_time_between_pickups  -- Calculate the average time difference between pickups for each zone
    FROM pickup_differences  -- Use the CTE with calculated time differences
    WHERE time_diff IS NOT NULL  -- Exclude rows where the time difference is null
    GROUP BY pickup_zone  -- Group by pickup zone to calculate the average for each zone
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/average_time_between_pickups.txt' (HEADER, DELIMITER ',');



