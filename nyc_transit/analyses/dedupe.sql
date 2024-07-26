-- nyc_transit/analyses/dedupe.sql
-- https://mode.com/sql-tutorial/sql-window-functions for assistance
-- Use a subquery to emulate the QUALIFY clause
-- The subquery assigns a row number to each row within the partition of event_id,
-- ordered by event_timestamp in descending order. This ensures that the most recent
-- event gets the row number 1.

COPY (
    select
        event_id,
        event_timestamp,
        -- Include other relevant columns here
    from (
        -- This subquery uses the ROW_NUMBER window function to assign a row number
        -- to each row within the partition of event_id, ordered by event_timestamp
        -- in descending order. This ensures that the most recent event gets the row number 1.
        select
            *,
            row_number() over (partition by event_id order by event_timestamp desc) as row_num
        from {{ ref('events') }}
    ) as ranked_events
    -- The outer query filters the results to include only the rows where row_num = 1.
    -- This effectively removes duplicates, keeping only the most recent event for each event_id.
    where row_num = 1
) TO 'C:/Users/Jdboazzo/cmu-95797/answers/dedupe.txt' (HEADER, DELIMITER ',');

