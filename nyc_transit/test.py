import duckdb

# Connect to the DuckDB database
conn = duckdb.connect('C:/Users/Jdboazzo/cmu-95797/main.db')

# Show schemas
schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
print("Schemas:", schemas)

# Show tables in each schema
for schema in schemas:
    schema_name = schema[0]
    tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'").fetchall()
    print(f"Tables in schema {schema_name}:", tables)


##(env) C:\Users\Jdboazzo\cmu-95797\nyc_transit>python test.py
##Schemas: [('information_schema',), ('main',), ('pg_catalog',), ('staging',), ('information_schema',), ('main',), ('pg_catalog',), ('information_schema',), ('main',), ('pg_catalog',)]
##Tables in schema information_schema: []
##Tables in schema main: [('bike_data',), ('central_park_weather',), ('fhvhv_tripdata',), ('fhv_bases',), ('fhv_tripdata',), ('green_tripdata',), ('yellow_tripdata',)]
##Tables in schema pg_catalog: []
##Tables in schema staging: []
##Tables in schema information_schema: []
##Tables in schema main: [('bike_data',), ('central_park_weather',), ('fhvhv_tripdata',), ('fhv_bases',), ('fhv_tripdata',), ('green_tripdata',), ('yellow_tripdata',)]
##Tables in schema pg_catalog: []
##Tables in schema information_schema: []
##Tables in schema main: [('bike_data',), ('central_park_weather',), ('fhvhv_tripdata',), ('fhv_bases',), ('fhv_tripdata',), ('green_tripdata',), ('yellow_tripdata',)]
##Tables in schema pg_catalog: []
