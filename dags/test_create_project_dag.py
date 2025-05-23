import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from metashape import print_hi,create_project
from config import Config


def print_hello():
    # activate(Config.METASHAPE_LICENSE)
    create_project(Config.NAS_ROOT + '/test.psx')
    print_hi()

with DAG(
        dag_id='test_create_project_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
