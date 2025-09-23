from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# from metashape import deactivate


def deactivate_license():
    """
    """
    # create_project(Config.NAS_ROOT + '/test.psx')
    # print_hi()


with DAG(
        dag_id='test_deactivate_license_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    task = PythonOperator(
        task_id='deactivate_license',
        python_callable=deactivate_license,
    )
