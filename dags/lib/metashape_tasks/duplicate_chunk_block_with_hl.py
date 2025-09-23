import json
import logging
import os

import Metashape
from common.config import inject
from common.helpers import notify_task_completion

logger = logging.getLogger(__name__)


@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "chunk_label",
        "metashape_server_ip",
        "nas_root_path"
    ],
    method="GET"
)
def duplicate_chunk_block_with_hl(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting duplicate_chunk_block_with_hl task with workflowId: {workflow_id}")

        project_path = context["project_path"]
        project_name = context["project_name"]
        chunk_label = context["chunk_label"]

        logger.info(f"[INFO] Project: {project_name}, Chunk to Duplicate: {chunk_label}")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)

        original_chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not original_chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        duplicated_chunk = original_chunk.copy()
        duplicated_chunk.label = "Chunk_HL"
        doc.chunks.append(duplicated_chunk)
        logger.info(f"[INFO] Duplicated chunk created: {duplicated_chunk.label}")

        removed_count = 0
        for camera in duplicated_chunk.cameras[:]:
            if camera.group and "HR" in camera.group.label:
                duplicated_chunk.remove(camera)
                duplicated_chunk.remove(camera.group)
                removed_count += 1

        logger.info(f"[INFO] Removed {removed_count} 'HR' cameras/groups from duplicated chunk.")
        doc.save()
        logger.info(f"[INFO] Project saved at {project_file_path}")

        payload = {
            "chunk_label_HL": duplicated_chunk.label,
        }

        logger.info(f"[{task_name}] Payload prepared for notification: {json.dumps(payload, indent=2)}")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Duplicated chunk: {duplicated_chunk.label}")
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
