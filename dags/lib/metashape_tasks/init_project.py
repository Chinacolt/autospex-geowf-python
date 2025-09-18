import json
import logging
import os

import Metashape as ms
from common.config import inject
from common.helpers import notify_task_completion

from lib.metashape import with_licence

logger = logging.getLogger(__name__)


@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "country_code",
        "structure_code",
        "site_code",
        "organization_code",
        "survey_code",
        "nas_folder_path",
        "s3_location_bucket"
    ],
    method="GET"
)
@with_licence
def create_metashape_project(**context):
    task_instance = context.get('task_instance') or context.get('ti')
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId", "unknown") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting Metashape project creation with workflowId: {workflow_id}")

        country_code = context.get("country_code")
        structure_code = context.get("structure_code")
        site_code = context.get("site_code")
        organization_code = context.get("organization_code")
        survey_code = context.get("survey_code")
        nas_root_path = context.get("nas_root_path")

        nas_folder_path = context.get("nas_folder_path")
        s3_location_bucket = context.get("s3_location_bucket")

        project_code = f"{country_code}_{organization_code}_{site_code}_{structure_code}_{survey_code}"
        project_dir = os.path.join(
            nas_root_path,
            nas_folder_path,
            s3_location_bucket,
            organization_code,
            site_code,
            structure_code,
            survey_code,
            "3dprocessing",
            "metashape"
        )
        project_name = f"{project_code}_3dprocessing"
        project_file_path = os.path.normpath(os.path.join(project_dir, f"{project_name}.psx"))

        logger.info(f"==> project_code <== {project_code}")
        logger.info(f"==> project_dir <== {project_dir}")
        logger.info(f"==> project_name <== {project_name}")
        logger.info(f"==> project_file_path <== {project_file_path}")

        if not os.path.exists(project_dir):
            logger.warning(f"[INFO] Project directory not found. Creating: {project_dir}")
            os.makedirs(project_dir, exist_ok=True)

        if os.path.isdir(project_file_path):
            raise IsADirectoryError(f"[ERROR] '{project_file_path}' is a directory. It must be a file.")

        doc = ms.Document()
        doc.save(project_file_path)
        logger.info(f"[DEBUG] New Metashape project saved at: {project_file_path}")

        workflow_id = context["dag_run"].conf.get("workflowId")
        if not workflow_id:
            raise Exception("workflowId not found in dag_run.conf")

        windows_project_path = os.path.join(project_dir, f"{project_name}.psx")

        payload = {
            "project_code": project_code,
            "project_path": project_dir,
            "windows_project_path": windows_project_path,
            "project_name": project_name
        }

        logger.info(f"[DEBUG] JSON payload: {json.dumps(payload)}")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[SUCCESS] Task completed successfully. XCom payload pushed.")
        except Exception as notify_error:
            logger.error(f"[ERROR] Failed to notify task completion: {str(notify_error)}")
            raise

    except Exception as e:
        logger.error(f"[{task_name}] Task failed create metashape project step: {type(e).__name__} - {str(e)}")
        # workflow_id = context.get("dag_run", {}).get("conf", {}).get("workflowId", "unknown")

        error_payload = {
            "workflowId": workflow_id,
            "taskName": task_name,
            "errorMessage": str(e),
            "projectInfo": {
                "project_path": project_dir,
                "project_name": project_name
            }
        }
        task_instance.xcom_push(key="task_payload", value=error_payload)
        raise
