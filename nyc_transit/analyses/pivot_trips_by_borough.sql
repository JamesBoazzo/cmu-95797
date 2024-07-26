-- nyc_transit/analyses/pivot_trips_by_borough.sql
-- dbt_utils pivot function was consistently giving me errors.  Therefore, I wrote the SQL script manually.
COPY (
    -- Define CTE named trips_by_borough
    with trips_by_borough as (
        select
            dl.borough,       -- Select the borough from the dimension locations table
            count(*) as num_trips  -- Count the number of trips for each borough
        from {{ ref('mart__fact_all_taxi_trips') }} ft  -- Reference the fact table containing taxi trips
        left join {{ ref('mart__dim_locations') }} dl  -- Reference the dimension table containing location data
        on ft.pulocationid = dl.locationid  -- Join on pick-up location ID
        or ft.dolocationid = dl.locationid  -- Also join on drop-off location ID
        group by dl.borough  -- Group the results by borough
    )

    -- Select and pivot the results
    select
        sum(case when borough = 'Manhattan' then num_trips else 0 end) as Manhattan,  -- Sum the number of trips for Manhattan
        sum(case when borough = 'Brooklyn' then num_trips else 0 end) as Brooklyn,    -- Sum the number of trips for Brooklyn
        sum(case when borough = 'Queens' then num_trips else 0 end) as Queens,        -- Sum the number of trips for Queens
        sum(case when borough = 'Bronx' then num_trips else 0 end) as Bronx,          -- Sum the number of trips for the Bronx
        sum(case when borough = 'Staten Island' then num_trips else 0 end) as Staten_Island  -- Sum the number of trips for Staten Island
    from trips_by_borough  -- Use the CTE defined above as the source
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/pivot_trips_by_borough.txt' (HEADER, DELIMITER ',');  -- Export the results 







