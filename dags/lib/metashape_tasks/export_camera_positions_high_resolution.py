import Metashape
import os
import logging
from common.config import inject
import json
from common.helpers import notify_task_completion

from lib.metashape import with_licence

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "chunk_label_HR",
        "project_code","metashape_server_ip", "nas_root_path"
    ],
    method="GET"
)
@with_licence
def export_camera_positions_high_resolution(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting export_camera_positions_high_resolution task with workflowId: {workflow_id}")

        project_path = context["project_path"]
        project_name = context["project_name"]
        chunk_label = context["chunk_label_HR"]
        project_code = context["project_code"]

        logger.info(f"[INFO] Starting camera export for chunk: '{chunk_label}'")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        camera_positions_xml_file_path = os.path.join(project_path, "export")
        if not os.path.exists(camera_positions_xml_file_path):
            os.makedirs(camera_positions_xml_file_path, exist_ok=True)

        camera_positions_xml_name = f"{project_code}_cameras_hr.xml"
        output_path = os.path.join(camera_positions_xml_file_path, camera_positions_xml_name)

        logger.info(f"[DEBUG] Output Path, Exporting camera positions to: {output_path}")


        chunk.exportCameras(
            path=output_path,
            format=Metashape.CamerasFormat.CamerasFormatBlocksExchange,
            save_points=True,
            save_markers=True
        )

        payload={
            "hr_output_path": output_path
        }

        logger.info(f"[INFO] JSON payload {json.dumps(payload)}")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully with payload: {payload}")
        except Exception as notify_error:
            logger.error(f"[{task_name}] Notification failed: {type(notify_error).__name__} - {str(notify_error)}")
            raise notify_error

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

        task_instance.xcom_push(key="task_payload", value=error_payload)
        raise
