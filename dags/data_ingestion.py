# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipelines.extract.nyc_open_data import *
from pipelines.transform.nyc_open_data_transform import *
from pipelines.load.load_to_db import *


# default_args = {
#     'owner': 'victor',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'email_on_failure': False,
#     'email_on_retry': False,
# }

# with DAG(
#     'nyc_data_pipeline', default_args=default_args, description='Fetch and process NYC open data',
#     schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False,
# ) as dag:

#     # Vehicle Collisions Tasks
#     def fetch_and_store_collisions():
#         df = fetch_vehicle_collisions_crashes()
#         transformed_df = transform_vehicle_collisions_crashes(df)
#         load_to_bigquery_db(transformed_df, 'raw_nyc_vehicle_collisions_crashes')

#     task_process_collisions = PythonOperator(
#         task_id='fetch_and_store_collisions',
#         python_callable=fetch_and_store_collisions
#     )

#     # Emergency Responses Tasks
#     def fetch_and_store_emergency():
#         df = fetch_emergency_responses()
#         transformed_df = transform_emergency_responses(df)
#         load_to_bigquery_db(transformed_df, 'raw_nyc_emergency_responses')

#     task_process_emergency = PythonOperator(
#         task_id='fetch_and_store_emergency',
#         python_callable=fetch_and_store_emergency
#     )

#     # Arrest Data Tasks
#     def fetch_and_store_arrests():
#         df = fetch_arrest_data()
#         transformed_df = transform_arrest_data(df)
#         load_to_bigquery_db(transformed_df, 'raw_nyc_arrest_data')

#     task_process_arrests = PythonOperator(
#         task_id='fetch_and_store_arrests',
#         python_callable=fetch_and_store_arrests
#     )

#     [task_process_collisions, task_process_emergency, task_process_arrests]


if __name__ == "__main__":
    # For testing the pipeline locally
    print("Testing emergency responses pipeline...")
    df = fetch_arrest_data(offset=0, limit=500)
    print(df.head())

    transformed_df = transform_arrest_data(df)
    print(transformed_df.head())

    load_to_bigquery_db(transformed_df, 'raw_nyc_arrest_data')
