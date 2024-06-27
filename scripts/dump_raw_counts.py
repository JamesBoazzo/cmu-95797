import duckdb

# Connect to the DuckDB database
# Establish a connection to the DuckDB database file named 'main.db'. 
#https://duckdb.org/docs/api/python/overview.html was my source for this line of code.
# This connection allows us to execute SQL queries on the database.
con = duckdb.connect('main.db')

# Define the list of table names for which we want to retrieve the row counts.
# These tables were previously created and populated with data.
tables = [
    'bike_data',
    'central_park_weather',
    'fhv_bases',
    'fhv_tripdata',
    'fhvhv_tripdata',
    'green_tripdata',
    'yellow_tripdata'
]
# Print heading for the output
# Print a heading to indicate the following lines will show table names and their corresponding row counts.
print("Table Name and Total Row Count")

# Iterate over the list of table names to execute a SQL query for each table.
# The query retrieves the count of rows in each table.  https://duckdb.org/docs/api/python/overview.html for assistance with this for loop.
for table in tables:
    result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    
   # Print the table name and the corresponding row count.
    print(f"{table}: {result[0]}")
