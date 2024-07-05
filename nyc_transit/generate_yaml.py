import duckdb
import yaml

# Connect to DuckDB
con = duckdb.connect('C:/Users/Jdboazzo/cmu-95797/main.db')

# Fetch column information
result = con.execute("""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main';
""").fetchall()

# Organize data
data = {}
for table_name, column_name, data_type in result:
    if table_name not in data:
        data[table_name] = []
    data[table_name].append({'name': column_name, 'data_type': data_type})

# Generate YAML
sources = [{'name': 'main', 'tables': [{'name': table, 'columns': cols} for table, cols in data.items()]}]

# Write to a YAML file
with open('sources.yml', 'w') as file:
    yaml.dump({'sources': sources}, file, sort_keys=False)

print("YAML file 'sources.yml' generated successfully.")
