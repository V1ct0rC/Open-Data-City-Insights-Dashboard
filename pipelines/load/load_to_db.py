import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

def load_to_bigquery_db(df: pd.DataFrame, table_name: str):
    """
    Load DataFrame to BigQuery table with schema validation
    """
    
    client = bigquery.Client()

    project_id = client.project
    dataset_id = "city_insights"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        table = client.get_table(table_id)
        print(f"Table '{table_name}' exists. Proceeding with data load.")
    except Exception:
        raise Exception(f"Table '{table_name}' does not exist in the dataset.")
    
    schema_fields = [field.name for field in table.schema]
    
    # Add raw_data if not present
    if "raw_data" not in df.columns:
        df["raw_data"] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)

    # Filter columns to match schema
    df_filtered = df[[col for col in df.columns if col in schema_fields]]
    
    # Load to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df_filtered, table_id, job_config=job_config
    )
    
    # Wait for the job to complete
    job.result()
    
    print(f"Data loaded successfully into {table_name}.")


if __name__ == "__main__":
    # Example usage
    pass