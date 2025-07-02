from airflow.utils.state import State
import logging
import json

from common.helpers import notify_task_completion, notify_task_failure


def task_success_callback(context):
    try:
        task_instance = context["task_instance"]
        task_name = task_instance.task_id
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"

        payload = task_instance.xcom_pull(task_ids=task_instance.task_id, key=task_name)

        notify_task_completion(
            workflow_id=workflow_id,
            task_name=task_instance.task_id,
            payload=payload
        )

        logging.info(f"[{task_instance.task_id}] Success callback triggered with payload: {json.dumps(payload)}")
    except Exception as e:
        logging.error(f"[Callback Error] Success callback failed: {str(e)}")

def task_failure_callback(context):
    logging.info("Task failure callback triggered")
    try:
        task_instance = context["task_instance"]
        task_name = task_instance.task_id
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logging.info(f"[{task_instance.task_id}] Task failure callback triggered for workflowId: {workflow_id}")


        payload = task_instance.xcom_pull(task_ids=task_instance.task_id, key=task_name)

        
        logging.info(f"[{task_instance.task_id}] XCom payload: {json.dumps(payload, indent=2)}")

        if payload:
            error_message = payload.get("errorMessage")

        notify_task_failure(
            workflow_id=workflow_id,
            task_name=task_instance.task_id,
            error_message=error_message
        )

        logging.info(f"[{task_instance.task_id}] Failure callback triggered with error: {error_message}")
    except Exception as e:
        logging.error(f"[Callback Error] Failure callback failed: {str(e)}")
