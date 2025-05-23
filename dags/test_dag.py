from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from metashape import print_hi

def print_hello():
    print_hi()

with DAG(
        dag_id='test_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
