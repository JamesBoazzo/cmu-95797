with source as (
    select * from {{ source('main', 'bike_data') }}
),

renamed as (
    select
        tripduration,
        starttime,
        stoptime,
        "start station id" as start_station_id,
        "start station name" as start_station_name,
        "start station latitude" as start_station_latitude,
        "start station longitude" as start_station_longitude,
        "end station id" as end_station_id,
        "end station name" as end_station_name,
        "end station latitude" as end_station_latitude,
        "end station longitude" as end_station_longitude,
        bikeid,
        usertype,
        "birth year" as birth_year,
        gender,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        filename
    from source
)

select
    coalesce(starttime, started_at)::timestamp as started_at_ts,
    coalesce(stoptime, ended_at)::timestamp as ended_at_ts,
    coalesce(try_cast(tripduration as int), datediff('second', started_at_ts, ended_at_ts)) as tripduration,
    start_station_id,
    start_station_name,
    coalesce(start_station_latitude::double, start_lat::double) as start_lat,
    coalesce(start_station_longitude::double, start_lng::double) as start_lng,
    end_station_id,
    end_station_name,
    coalesce(end_station_latitude::double, end_lat::double) as end_lat,
    coalesce(end_station_longitude::double, end_lng::double) as end_lng,
    filename
from renamed
