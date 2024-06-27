-- Turn on echo to see the commands being run
.echo on

-- Show table names
-- List all the table names present in the current database to help ensure the tables ingested correctly.
SHOW TABLES;

-- Describe each table to get the schema
-- Retrieve the schema (column definitions) of the tables.
-- This provides details about the tables, such as column names, data types, and constraints.
DESCRIBE bike_data;
DESCRIBE central_park_weather;
DESCRIBE fhv_bases;
DESCRIBE fhv_tripdata;
DESCRIBE fhvhv_tripdata;
DESCRIBE green_tripdata;
DESCRIBE yellow_tripdata;


