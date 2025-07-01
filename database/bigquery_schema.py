# Currently, BigQuery does not support JSON upload from Pandas, so we use STRING for raw data

raw_nyc_vehicle_collisions_crashes = [
    {"name": "collision_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "crash_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "crash_time", "type": "TIME", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
    {"name": "number_of_persons_injured", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "number_of_persons_killed", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

raw_nyc_emergency_responses = [
    {"name": "response_id", "type": "STRING", "mode": "REQUIRED"},  # Added primary key
    {"name": "creation_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "closed_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
    {"name": "incident_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

raw_nyc_arrest_data = [
    {"name": "arrest_key", "type": "STRING", "mode": "REQUIRED"},
    {"name": "arrest_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "arrest_boro", "type": "STRING", "mode": "NULLABLE"},
    {"name": "arrest_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

raw_la_crime_data = [
    {"name": "date_occ", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "time_occ", "type": "TIME", "mode": "NULLABLE"},
    {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

raw_la_traffic_collisions = [
    {"name": "date_occ", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "time_occ", "type": "TIME", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

processed_nyc_data = [
    {"name": "date_entry", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
    {"name": "type_of_entry", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]

processed_la_data = [
    {"name": "date_entry", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "type_of_entry", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_data", "type": "STRING", "mode": "NULLABLE"}
]