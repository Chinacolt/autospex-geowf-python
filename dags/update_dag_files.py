import logging
from datetime import datetime
import os
import subprocess

from airflow.decorators import task, dag
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'max_active_tis_per_dag': 1
}


@dag(
    dag_id='update_airflow_files',
    default_args=default_args,
    start_date=datetime(2022, 10, 14, 3),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
)
def update_airflow_files():
    @task
    def git_pull():

        git_branch = Variable.get("source_git_branch")
        if not git_branch:
            logging.warning("No git branch specified in Variable 'source_git_branch'. Using default branch 'master'.")
            git_branch = "master"

        logging.info("Pulling from git")
        airflow_home_folder = Variable.get("airflow_dags_folder_path")
        if not airflow_home_folder:
            airflow_home_folder = os.getenv("AIRFLOW__CORE__DAGS_FOLDER")
            if airflow_home_folder:
                logging.info("Using environment variable AIRFLOW__CORE__DAGS_FOLDER: %s", airflow_home_folder)
            else:
                logging.error("Variable 'airflow_dags_folder_path' is not set and environment variable AIRFLOW__CORE__DAGS_FOLDER is not set.")
                raise ValueError("Variable 'airflow_dags_folder_path' is not set. Please set it to the path of your Airflow DAGs folder.")

        logging.info("CWD: %s", airflow_home_folder)
        logging.info("Target git branch: %s", git_branch)

        try:
            logging.info("Switching to branch: %s", git_branch)
            checkout_process = subprocess.run(
                ["git", "checkout", git_branch],
                capture_output=True, text=True, cwd=airflow_home_folder, check=True
            )
            logging.info("Checkout stdout: %s", checkout_process.stdout)
            logging.info("Checkout stderr: %s", checkout_process.stderr)

            logging.info("Pulling from origin/%s", git_branch)
            pull_process = subprocess.run(
                ["git", "pull", "origin", git_branch],
                capture_output=True, text=True, cwd=airflow_home_folder, check=True
            )
            logging.info("Pull stdout: %s", pull_process.stdout)
            logging.info("Pull stderr: %s", pull_process.stderr)

        except subprocess.CalledProcessError as e:
            logging.error("Git command failed with return code %d", e.returncode)
            logging.error("Stdout: %s", e.stdout)
            logging.error("Stderr: %s", e.stderr)
            raise

    git_pull()


update_airflow_files()
