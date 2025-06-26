# from airflow.sdk import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipelines.extract.nyc_open_data import *
from pipelines.transform.nyc_open_data_transform import *
from pipelines.load.load_to_db import *


# # default_args = {
# #     'owner': 'victor',
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=5),
# # }

# # with DAG(
# #     'nyc_data_pipeline',
# #     default_args=default_args,
# #     description='Fetch and process NYC open data',
# #     schedule_interval='@daily',
# #     start_date=datetime(2024, 1, 1),
# #     catchup=False,
# # ) as dag:

# #     def fetch_and_store_crime():
# #         df = fetch_vehicle_collisions_crashes()
# #         load_to_bigquery_db(df, 'raw_nyc_vehicle_collisions_crashes')

# #     task_fetch_crime = PythonOperator(
# #         task_id='fetch_and_store_crime',
# #         python_callable=fetch_and_store_crime
# #     )

# #     task_fetch_crime

if __name__ == "__main__":
    # TODO: Implement DAG for task orchestration
    df = fetch_emergency_responses(offset=0, limit=500)
    print(df.head())

    transformed_df = transform_emergency_responses(df)
    print(transformed_df.head())

    load_to_bigquery_db(transformed_df, 'raw_nyc_emergency_responses')
