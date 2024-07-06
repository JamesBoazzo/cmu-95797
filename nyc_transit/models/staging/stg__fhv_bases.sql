
  
 
-- models/staging/stg__fhv_bases.sql
-- This model cleans and renames columns from the source table 'fhv_bases'
-- It also replaces blank cells in the 'dba' column with 'Unknown'
--assistance from https://stackoverflow.com/questions/tagged/sql to build sql code
with source as (
    -- Select all data from the source table 'fhv_bases' in the 'main' schema
    select * from "nyc_transit"."main"."fhv_bases"
),

renamed as (
    -- Clean and rename columns
    select 
        -- Ensure 'base_number' is a string and trim whitespace
        try_cast(trim(base_number) as varchar) as base_number,

        -- Ensure 'base_name' is a string and trim whitespace
        try_cast(trim(base_name) as varchar) as base_name,

        -- Replace blank cells in 'dba' column with 'Unknown', ensure it is a string, and trim whitespace
        try_cast(trim(coalesce(dba, 'Unknown')) as varchar) as dba,

        -- Ensure 'dba_category' is a string and trim whitespace.  Although each cell says 'other' opted to keep this column as is until later data exploration.
        try_cast(trim(dba_category) as varchar) as dba_category,

        -- Rename 'filename' to 'source_filename', ensure it is a string, and trim whitespace
        try_cast(trim(filename) as varchar) as source_filename
    from source
)

-- Select all cleaned and renamed columns for the final output
select * from renamed
