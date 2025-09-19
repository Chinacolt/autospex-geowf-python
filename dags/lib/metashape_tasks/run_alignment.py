import json
import logging
import os

import Metashape as ms
from common.config import inject
from common.helpers import notify_task_completion
from common.utils import get_accuracy_value

logger = logging.getLogger(__name__)


@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "run_alignment_batch_id",
        "key_point_limit",
        "tie_point_limit",
        "generic_preselection",
        "reference_preselection",
        "adaptive_camera_model_fitting",
        "accuracy",
        "guided_image_matching",
        "reset_current_alignment",
        "exclude_stationary_tie_points",
        "metashape_server_ip",
        "nas_root_path"
    ],
    method="GET"
)
def run_alignment(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting run_alignment task with workflowId: {workflow_id}")

        nas_root_path = context.get("nas_root_path")

        # Get Params
        project_path = context["project_path"]
        project_name = context["project_name"]
        key_point_limit = int(context["key_point_limit"])
        tie_point_limit = int(context["tie_point_limit"])
        generic_preselection = not (context["generic_preselection"] == "False")
        reference_preselection = bool(context["reference_preselection"])
        adaptive_camera_model_fitting = not (context["adaptive_camera_model_fitting"] == "False")
        accuracy = context["accuracy"]
        guided_image_matching = not (context.get("guided_image_matching") == "False")
        reset_current_alignment = not (context.get("reset_current_alignment") == "False")
        exclude_stationary_tie_points = not (context.get("exclude_stationary_tie_points") == "False")

        logger.info("[run_alignment] project=%s | key/tie=%s/%s | accuracy=%s",
                    project_name, key_point_limit, tie_point_limit, accuracy)

        metashape_server_ip = context.get("metashape_server_ip")

        # Daha önceki batch varsa iptal et
        run_alignment_batch_id = context.get("run_alignment_batch_id")
        if run_alignment_batch_id:
            try:
                client = ms.NetworkClient()
                client.connect(metashape_server_ip)
                client.abortBatch(run_alignment_batch_id)
                logger.info(f"Previous batch {run_alignment_batch_id} aborted.")
                client.disconnect()
            except Exception as e:
                logger.warning(f"[WARNING] Could not abort previous batch: {e}")

        # Projeyi aç
        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        logger.info(f"Opening Metashape project: {project_file_path}")

        doc = ms.Document()
        doc.open(project_file_path, read_only=False)
        chunk = doc.chunks[0]

        # MatchPhotos görevi
        match_task = ms.Tasks.MatchPhotos()
        match_task.keypoint_limit = key_point_limit
        match_task.tiepoint_limit = tie_point_limit
        match_task.downscale = get_accuracy_value(accuracy)
        match_task.generic_preselection = generic_preselection
        match_task.reference_preselection = reference_preselection
        match_task.reference_preselection_mode = ms.ReferencePreselectionMode.ReferencePreselectionEstimated
        match_task.guided_matching = guided_image_matching
        match_task.reset_matches = reset_current_alignment
        match_task.filter_stationary_points = exclude_stationary_tie_points

        # AlignCameras görevi
        align_task = ms.Tasks.AlignCameras()
        align_task.adaptive_fitting = adaptive_camera_model_fitting

        # Batch'i oluştur ve başlat
        client = ms.NetworkClient()
        client.connect(metashape_server_ip)

        relative_path = os.path.join(project_path, f"{project_name}.psx")
        logger.info(f"[DEBUG] Relative project path for batch: {relative_path}")

        batch_id = client.createBatch(relative_path, [match_task.toNetworkTask(chunk), align_task.toNetworkTask(chunk)])
        client.setBatchPaused(batch_id, False)
        logger.info(f"[SUCCESS] Batch submitted and running. ID: {batch_id}")

        payload = {
            "run_alignment_batch_id": batch_id
        }

        logger.info(f"[{task_name}] Payload prepared for notification: {json.dumps(payload, indent=2)}")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name="wait_wf_run_alignment",
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Batch ID: {batch_id}")
        except Exception as notify_error:
            logger.error(f"[{task_name}] Notification failed: {type(notify_error).__name__} - {str(notify_error)}")
            raise notify_error

    except Exception as e:
        logger.exception(f"[{task_name}] Task failed: {e}")

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
