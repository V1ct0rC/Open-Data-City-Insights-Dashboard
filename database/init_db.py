from google.cloud import bigquery
from bigquery_schema import (
    raw_nyc_vehicle_collisions_crashes,
    raw_nyc_emergency_responses,
    raw_nyc_arrest_data,
    raw_la_crime_data,
    raw_la_traffic_collisions,
    processed_nyc_data,
    processed_la_data
)
from dotenv import load_dotenv


load_dotenv()

def init_local_db():
    # TODO: Implement local SQLite database initialization logic
    pass

def init_db():
    try:
        client = bigquery.Client()
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        print("Initializing Local SQLite database instead.")
        init_local_db()
        return
    
    dataset_id = "city_insights"
    dataset_ref = f"{client.project}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        # Create the dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your preferred location
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created at {dataset.location}")
    
    # Dictionary mapping table names to their schemas
    tables = {
        "raw_nyc_vehicle_collisions_crashes": raw_nyc_vehicle_collisions_crashes,
        "raw_nyc_emergency_responses": raw_nyc_emergency_responses,
        "raw_nyc_arrest_data": raw_nyc_arrest_data,
        "raw_la_crime_data": raw_la_crime_data,
        "raw_la_traffic_collisions": raw_la_traffic_collisions,
        "processed_nyc_data": processed_nyc_data,
        "processed_la_data": processed_la_data
    }
    
    # Create each table
    for table_name, schema in tables.items():
        table_id = f"{dataset_ref}.{table_name}"
        
        # Convert dictionary schema to SchemaField objects
        schema_fields = [
            bigquery.SchemaField(
                field["name"], 
                field["type"], 
                mode=field["mode"]
            ) for field in schema
        ]
        
        table = bigquery.Table(table_id, schema=schema_fields)
        
        try:
            table = client.create_table(table, exists_ok=True)
            print(f"Table {table_name} created or already exists.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")


if __name__ == "__main__":
    print ("Testing BigQuery connection client and query execution...")
    client = bigquery.Client()

    query = """
        SELECT name, SUM(number) as total_people
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        WHERE state = 'TX'
        GROUP BY name, state
        ORDER BY total_people DESC
        LIMIT 20
    """
    rows = client.query_and_wait(query)

    print("The query data:")
    for row in rows:
        print("name={}, count={}".format(row[0], row["total_people"]))

    print("Initializing the database...")
    init_db()
    print("Database initialization complete.")
