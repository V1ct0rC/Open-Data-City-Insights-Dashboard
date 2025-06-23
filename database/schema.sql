-- RAW tables
CREATE TABLE IF NOT EXISTS raw_nyc_vehicle_collisions_crashes (
    collision_id INTEGER PRIMARY KEY,
    crash_date TIMESTAMP,
    crash_time TIME,
    latitude FLOAT,
    longitude FLOAT,
    borough TEXT,
    number_of_persons_injured INTEGER,
    number_of_persons_killed INTEGER,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS raw_nyc_emergency_responses (
    creation_date TIMESTAMP,
    closed_date TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    borough TEXT,
    incident_type TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS raw_nyc_arrest_data (
    arrest_key TEXT PRIMARY KEY,
    arrest_date TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    arrest_boro TEXT,
    arrest_type TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS raw_la_crime_data (
    date_occ TIMESTAMP,
    time_occ TIME,
    lat FLOAT,
    lon FLOAT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS raw_la_traffic_collisions (
    date_occ TIMESTAMP,
    time_occ TIME,
    raw_data JSONB
);

-- Processed tables
CREATE TABLE IF NOT EXISTS processed_nyc_data (
    date_entry TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    borough TEXT,
    type_of_entry TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS processed_la_data (
    date_entry TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    type_of_entry TEXT,
    raw_data JSONB
);