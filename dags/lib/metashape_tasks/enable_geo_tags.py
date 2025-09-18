import Metashape
import os
import logging
import json
from common.config import inject
from common.helpers import notify_task_completion

from lib.metashape import with_licence

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "chunk_label","metashape_server_ip", "nas_root_path"
    ],
    method="GET"
)
@with_licence
def enable_geo_tags(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting enable_geo_tags task with workflowId: {workflow_id}")

        project_path = context["project_path"]
        project_name = context["project_name"]
        chunk_label = context["chunk_label"]

        logger.info(f"[INFO] Starting enable_geo_tags task for chunk '{chunk_label}'")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at: {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)
        logger.info(f"[INFO] Metashape project opened from: {project_file_path}")

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found.")

        logger.info(f"[INFO] Processing {len(chunk.cameras)} cameras in chunk '{chunk.label}'")

        updated_count = 0
        for camera in chunk.cameras:
            if not camera.reference:
                logger.debug(f"[SKIP] Camera '{camera.label}' has no reference.")
                continue

            if not camera.reference.enabled:
                camera.reference.enabled = True
                updated_count += 1
                logger.debug(f"[UPDATE] GPS enabled for camera '{camera.label}'.")

        doc.save()
        logger.info("[INFO] Metashape project saved after enabling GPS tags.")

        payload = {
                "enabled_camera_count": updated_count
            }
        
        logger.info(f"[INFO] Enabled GPS tags for {updated_count} cameras.")
        logger.info(f"[INFO] Payload: {json.dumps(payload)}")

        try:
            
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Enabled {updated_count} cameras.")
        except Exception as notify_error:
            logger.error(f"[{task_name}] Notification failed: {type(notify_error).__name__} - {str(notify_error)}")
            raise


    except Exception as e:
        logger.error(f"[{task_name}] Task failed: {type(e).__name__} - {str(e)}")

        error_payload = {
            "workflowId": workflow_id,
            "taskName": task_name,
            "errorMessage": str(e),
            "projectInfo": {
                "project_path": project_path,
                "project_name": project_name
            }
        }
        task_instance.xcom_push(key=task_name, value=error_payload)
        raise
