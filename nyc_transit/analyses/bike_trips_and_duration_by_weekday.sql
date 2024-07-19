-- bike_trips_and_duration_by_weekday.sql
-- This query calculates the count and total duration of bike trips by weekday in both seconds and minutes.

select
    -- Extract the weekday from the start timestamp of the trip
    weekday(started_at_ts) as weekday,
    
    -- Count the total number of trips that started on each weekday
    count(*) as total_trips,
    
    -- Sum the total duration in seconds for all trips that started on each weekday
    sum(duration_sec) as total_trip_duration_secs,
    
    -- Sum the total duration in minutes for all trips that started on each weekday
    sum(duration_min) as total_trip_duration_mins
from "nyc_transit"."main"."mart__fact_all_bike_trips"
group by 
    -- Group the results by weekday
    weekday


