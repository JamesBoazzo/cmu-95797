{{ config(
    materialized='table'
) }}

with test_data as (
    select * from (
        values 
            ('Manhattan', 100),
            ('Brooklyn', 200),
            ('Queens', 150),
            ('Bronx', 80),
            ('Staten Island', 60),
            ('Manhattan', 50),
            ('Brooklyn', 75),
            ('Queens', 25),
            ('Bronx', 40),
            ('Staten Island', 30)
    ) as t(borough, num_trips)
)

select *
from {{ pivot(
    'borough',
    ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'],
    'num_trips'
) }}

