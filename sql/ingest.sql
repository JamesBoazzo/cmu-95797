--Turn on echo to see commands being run.
.echo on

-- Drop existing tables if they exist to avoid any conflicts with old data.
DROP TABLE IF EXISTS bike_data;
DROP TABLE IF EXISTS central_park_weather;
DROP TABLE IF EXISTS fhv_tripdata;
DROP TABLE IF EXISTS fhvhv_tripdata;
DROP TABLE IF EXISTS green_tripdata;
DROP TABLE IF EXISTS yellow_tripdata;
DROP TABLE IF EXISTS fhv_bases;

-- Ingest bike_data.  Create the table by reading specified file path.
CREATE TABLE bike_data AS SELECT * FROM read_csv_auto('C:/Users/Jdboazzo/cmu-95797/data/citibike-tripdata.csv.gz');

-- Ingest central_park_weather.  Create the table by reading specified file path.
CREATE TABLE central_park_weather AS SELECT * FROM read_csv_auto('C:/Users/Jdboazzo/cmu-95797/data/central_park_weather.csv');

-- Ingest fhv_tripdata.  Create the table by reading specified file path.
CREATE TABLE fhv_tripdata AS SELECT * FROM read_parquet('C:/Users/Jdboazzo/cmu-95797/data/taxi/fhv_tripdata.parquet');

-- Ingest fhvhv_tripdata.  Create the table by reading specified file path.
CREATE TABLE fhvhv_tripdata AS SELECT * FROM read_parquet('C:/Users/Jdboazzo/cmu-95797/data/taxi/fhvhv_tripdata.parquet');

-- Ingest green_tripdata.  Create the table by reading specified file path.
CREATE TABLE green_tripdata AS SELECT * FROM read_parquet('C:/Users/Jdboazzo/cmu-95797/data/taxi/green_tripdata.parquet');

-- Ingest yellow_tripdata.  Create the table by reading specified file path.
CREATE TABLE yellow_tripdata AS SELECT * FROM read_parquet('C:/Users/Jdboazzo/cmu-95797/data/taxi/yellow_tripdata.parquet');

-- Ingest fhv_bases.  Create the table by reading specified file path.
CREATE TABLE fhv_bases AS SELECT * FROM read_csv_auto('C:/Users/Jdboazzo/cmu-95797/data/fhv_bases.csv');
