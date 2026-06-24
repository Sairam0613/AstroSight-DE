from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_function():
    print("Airflow is working!")
    return "success"

with DAG(
    dag_id='test_dag',
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='test_task',
        python_callable=test_function
    )