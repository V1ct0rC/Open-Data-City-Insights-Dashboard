import pandas as pd
import json
import hashlib
from datetime import datetime


def transform_vehicle_collisions_crashes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw vehicle collisions data to match BigQuery schema. Any value that cannot be converted 
    to the target type (e.g., a string that is not a valid date or number) will be replaced with NaT 
    (for dates/times) or NaN (for numbers)
    """
    out = pd.DataFrame()
    out["collision_id"] = df["collision_id"].astype(int)
    out["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    out["crash_time"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce").dt.time
    out["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    out["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    out["borough"] = df.get("borough")
    out["number_of_persons_injured"] = pd.to_numeric(df.get("number_of_persons_injured"), errors="coerce").fillna(0).astype(int)
    out["number_of_persons_killed"] = pd.to_numeric(df.get("number_of_persons_killed"), errors="coerce").fillna(0).astype(int)
    out["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    return out


def transform_emergency_responses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw emergency response data to match BigQuery schema with a stable primary key.
    """
    out = pd.DataFrame()
    
    # Generate a stable primary key based on a combination of fields
    def generate_response_id(row):
        # Create a string with the key identifying fields
        # Using only fields that together should be unique for each incident
        key_data = f"{row.get('unique_key', '')}"
        
        # If there's no unique_key in the data, create a composite key
        if not key_data or key_data == 'nan':
            key_data = f"{row.get('creation_date', '')}-{row.get('incident_address', '')}-{row.get('incident_type', '')}"
            
            # Add coordinates if available for more uniqueness
            lat = row.get('latitude', '')
            lon = row.get('longitude', '')
            if lat and lon and lat != 'nan' and lon != 'nan':
                key_data += f"-{lat}-{lon}"
                
        # Hash the key data to create a stable ID
        return hashlib.md5(key_data.encode()).hexdigest()
    
    # Create the response_id field
    out["response_id"] = df.apply(generate_response_id, axis=1)
    out["creation_date"] = pd.to_datetime(df.get("creation_date"), errors="coerce")
    out["closed_date"] = pd.to_datetime(df.get("closed_date"), errors="coerce")
    out["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    out["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    out["borough"] = df.get("borough")
    out["incident_type"] = df.get("incident_type")
    out["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    
    return out


def transform_arrest_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw arrest data to match BigQuery schema. Any value that cannot be converted to the 
    target type (e.g., a string that is not a valid date or number) will be replaced with NaT (for 
    dates/times) or NaN (for numbers)
    """
    out = pd.DataFrame()
    out["arrest_key"] = df["arrest_key"].astype(str)
    out["arrest_date"] = pd.to_datetime(df.get("arrest_date"), errors="coerce")
    out["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    out["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    out["arrest_boro"] = df.get("arrest_boro")
    out["arrest_type"] = df.get("arrest_type")
    out["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    return out
