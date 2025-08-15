from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

def extract_transform():
    # Paths relative to project root
    input_path = '/opt/airflow/project/data.csv'  # Uses root data.csv
    output_path = '/opt/airflow/project/outputs/processed_data.csv'
    
    print(f"Reading from: {input_path}")
    print(f"Files in directory: {os.listdir('/opt/airflow/project')}")
    
    data = pd.read_csv(input_path)
    data[data["price"] > 1.00].to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

with DAG(
    'etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
) as dag:
    
    PythonOperator(
        task_id='transform_data',
        python_callable=extract_transform,
    )