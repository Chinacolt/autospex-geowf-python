import logging
import time

from common.config import inject
from common.config import re_inject_param
from common.helpers import notify_task_completion

logger = logging.getLogger(__name__)


@inject(
    workflow_conf_key="workflowId",
    read_params=["review_alignment_and_georeferencing", "metashape_server_ip", "nas_root_path"],
    method="GET"
)
def review_alignment_and_georeferencing(**context):
    logger = logging.getLogger("airflow.task")
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id
    workflow_id = context["dag_run"].conf.get("workflowId")

    attempt = 0
    while True:
        response = re_inject_param(
            workflow_id=workflow_id,
            task_name=task_name,
            param_name="review_alignment_and_georeferencing"
        )

        logger.info(f"response in review_alignment_and_georeferencing = {response}")

        param_val = str(response).lower() != "false"

        logger.info(f"[{task_name}] Attempt {attempt + 1}: review_alignment_and_georeferencing = {param_val}")

        success_payload = {
            "review_alignment_and_georeferencing_message": "Completed review of alignment and georeferencing."
        }

        if param_val is True:
            logger.info(f"[{task_name}] Condition met. Exiting.")
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=success_payload
            )
            return

        logger.info(f"[{task_name}] Not met. Sleeping 10 seconds...")
        time.sleep(10)
        attempt += 1
