import logging
import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from common.config import get_variable, get_keycloak_token

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

@dag(
    dag_id="workflow_listener",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    tags=["listener"],
)
def workflow_listener():

    @task()
    def get_workflow_id() -> dict:
        logger = logging.getLogger("airflow.task")
        base_url = get_variable("WORKFLOW_API_URL_TEMPLATE")
        api_url = f"{base_url}/pending"
        token = get_keycloak_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                logger.info("[Fetch Task] No pending workflow found (empty response).")
                return {}

            if isinstance(data, str):
                logger.info(f"[Fetch Task] Returning workflow ID: {data}")
                return {"workflowId": data}

            if isinstance(data, dict) and "workflowId" in data:
                workflow_id = data["workflowId"]
                logger.info(f"[Fetch Task] Parsed workflow ID: {workflow_id}")
                return {"workflowId": workflow_id}

        except requests.exceptions.RequestException as e:
            logger.error(f"[Fetch Task] Error fetching workflow ID: {e}")
        return {}

    @task(trigger_rule="all_success")
    def trigger_pipeline(conf_payload: dict, **context):
        logging.info(f"[Trigger Task] Sending payload: {conf_payload}")
        trigger = TriggerDagRunOperator(
            task_id="trigger_metashape_pipeline_op",
            trigger_dag_id="metashape_chunk_pipeline",
            conf=conf_payload,
            wait_for_completion=False,
            reset_dag_run=True,
        )
        trigger.execute(context=context)

    workflow_id_payload = get_workflow_id()
    trigger_pipeline(workflow_id_payload)

workflow_listener()
