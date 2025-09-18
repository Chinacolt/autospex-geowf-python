import json
import logging
import time
from common.config import inject, re_inject_param
from common.config import inject
from common.helpers import notify_task_completion

from dags.lib.metashape import with_licence

logger = logging.getLogger(__name__)


@inject(
    workflow_conf_key="workflowId",
    read_params=["review_tie_point_removal_result"],
    method="GET"
)
@with_licence
def review_tie_point_removal_result(**context):
    logger = logging.getLogger("airflow.task")
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id
    workflow_id = context["dag_run"].conf.get("workflowId")

    attempt = 0
    while True:
        response = re_inject_param(
            workflow_id=workflow_id,
            task_name=task_name,
            param_name="review_tie_point_removal_result"
        )

        logger.info(f"response in review_tie_point_removal_result = {response}")

        param_val = str(response).lower() != "false"

        logger.info(f"[{task_name}] Attempt {attempt + 1}: review_tie_point_removal_result = {param_val}")
        
        success_payload = {
            "review_tie_point_removal_result_message": "Completed review tie point removal result."
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




