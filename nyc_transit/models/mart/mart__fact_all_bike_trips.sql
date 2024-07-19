-- mart__fact_all_bike_trips.sql
select
  started_at_ts,
  ended_at_ts,
  datediff('minute', started_at_ts, ended_at_ts) as duration_min,
  datediff('second', started_at_ts, ended_at_ts) as duration_sec,
  start_station_id,
  end_station_id
from {{ ref('stg__bike_data') }}
