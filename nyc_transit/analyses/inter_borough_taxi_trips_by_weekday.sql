-- inter_borough_taxi_trips_by_weekday.sql
-- This query calculates the count of total taxi trips, trips starting and ending in different boroughs,
-- and the percentage of such trips by weekday.
-- Ensured the use of CTE (specifically inter-borough trips below) as instructed in HW.

-- Extract relevant fields from the fact table and add the weekday
with taxi_trips as (
    select
        type,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        weekday(pickup_datetime) as weekday
    from "nyc_transit"."main"."mart__fact_all_taxi_trips"
),

-- Select location IDs and their boroughs
locations as (
    select
        locationid,
        borough
    from "nyc_transit"."main"."mart__dim_locations"
),

-- Join trips with their pickup and dropoff boroughs
trips_with_boroughs as (
    select
        tt.type,
        tt.pickup_datetime,
        tt.dropoff_datetime,
        tt.pulocationid,
        tt.dolocationid,
        tt.weekday,
        pl.borough as pickup_borough,
        dl.borough as dropoff_borough
    from taxi_trips tt
    join locations pl on tt.pulocationid = pl.locationid
    join locations dl on tt.dolocationid = dl.locationid
),

-- Calculate total trips and inter-borough trips by weekday
-- Ensured to use this as CTE 
inter_borough_trips as (
    select
        weekday,
        count(*) as total_trips,
        -- Count the number of trips where the pickup and dropoff boroughs are different
        sum(case when pickup_borough != dropoff_borough then 1 else 0 end) as inter_borough_trips
    from trips_with_boroughs
    group by weekday
)

-- Calculate the percentage of inter-borough trips and order by weekday
select
    -- Convert numeric weekday values to their respective day names
	-- References inter-borough CTE above
    case
        when weekday = 0 then 'Sunday'
        when weekday = 1 then 'Monday'
        when weekday = 2 then 'Tuesday'
        when weekday = 3 then 'Wednesday'
        when weekday = 4 then 'Thursday'
        when weekday = 5 then 'Friday'
        when weekday = 6 then 'Saturday'
    end as weekday_name,
    total_trips,
    inter_borough_trips,
    (inter_borough_trips * 100.0 / total_trips) as percentage_inter_borough
from inter_borough_trips
order by weekday

