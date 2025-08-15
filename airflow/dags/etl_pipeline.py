from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator  # Absolute import path
import pandas as pd

def process_data():
    df = pd.DataFrame({'test': [1, 2, 3]})
    print(df)

with DAG(
    dag_id='debug_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_task',
        python_callable=process_data
    )