-- taxi_trips_ending_at_airports.sql

-- Create an expression to select airport zones
with airport_zones as (
    select 
        locationid,
        zone
    from "nyc_transit"."main"."mart__dim_locations"
    where service_zone in ('Airports', 'EWR')
    -- This CTE filters the locations to include only those in the 'Airports' or 'EWR' service zones
)

-- Count the total number of trips ending at the selected airport zones
select
    count(*) as total_trips
from "nyc_transit"."main"."mart__fact_all_taxi_trips" t
join airport_zones az on t.dolocationid = az.locationid
-- This join matches the drop-off location ID of each taxi trip with the location ID of the airport zones



