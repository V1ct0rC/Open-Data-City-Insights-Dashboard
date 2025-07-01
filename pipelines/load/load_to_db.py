import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


load_dotenv()

def load_to_bigquery_db(df: pd.DataFrame, table_name: str) -> None:
    """
    Load DataFrame to BigQuery table with deduplication to prevent adding duplicate rows
    
    Args:
        df (pd.DataFrame): DataFrame to load
        table_name (str): Name of the BigQuery table to load data into
 
    Returns:
        None
    """
    
    client = bigquery.Client()

    project_id = client.project
    dataset_id = "city_insights"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        table = client.get_table(table_id)
        print(f"Table '{table_name}' exists. Proceeding with data load.")
    except NotFound:
        raise Exception(f"Table '{table_name}' does not exist in the dataset.")
    except Exception as e:
        raise Exception(f"Error accessing table '{table_name}': {e}")
    
    schema_fields = [field.name for field in table.schema]
    
    # Add raw_data if not present
    if "raw_data" not in df.columns:
        df["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)

    # Filter columns to match schema
    df_filtered = df[[col for col in df.columns if col in schema_fields]]
    
    # Determine primary key field based on table name
    if "vehicle_collisions" in table_name:
        primary_key = "collision_id"
    elif "arrest_data" in table_name:
        primary_key = "arrest_key"
    elif "emergency_responses" in table_name:
        primary_key = "response_id"  # Use the new permanent primary key
    else:
        print(f"Warning: No known primary key for table {table_name}. Uploading all data.")
        primary_key = None
    
    # Query existing IDs to avoid duplicates
    if primary_key and primary_key in df_filtered.columns:
        existing_ids_query = f"""
            SELECT DISTINCT {primary_key} 
            FROM `{project_id}.{dataset_id}.{table_name}`
        """
        
        try:
            existing_ids_df = client.query(existing_ids_query).to_dataframe()
            existing_ids = set(existing_ids_df[primary_key].values)
            
            # Filter out rows that already exist in the database
            new_rows = df_filtered[~df_filtered[primary_key].isin(existing_ids)]
            
            print(f"Found {len(df_filtered)} total rows, {len(new_rows)} are new")
            
            if len(new_rows) == 0:
                print("No new data to upload.")
                return
                
            df_to_upload = new_rows
        except Exception as e:
            print(f"Error querying existing IDs: {e}")
            print("Proceeding with full upload (may contain duplicates)")
            df_to_upload = df_filtered
    else:
        print(f"Warning: Primary key '{primary_key}' not found in data. Uploading all rows.")
        df_to_upload = df_filtered
    
    # Load to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df_to_upload, table_id, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()
    
    print(f"{len(df_to_upload)} new rows loaded successfully into {table_name}.")


if __name__ == "__main__":
    # Example usage
    pass