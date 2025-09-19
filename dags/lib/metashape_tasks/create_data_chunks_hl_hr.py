import glob
import logging
import os
from collections import defaultdict

from Metashape import Metashape as ms
from common.config import inject
from common.helpers import notify_task_completion

logger = logging.getLogger(__name__)


def normalize_section_name(name: str) -> str:
    return name.lower().replace(" ", "-")


@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "country_code",
        "project_path",
        "project_name",
        "__image_group_paths",
        "survey_code",
        "nas_folder_path",
        "s3_location_bucket",
        "metashape_server_ip",
        "nas_root_path"
    ],
    method="GET"
)
def create_data_chunks_hl_hr(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting create_data_chunks_hl_hr task with workflowId: {workflow_id}")

        project_path = context["project_path"]
        project_name = context["project_name"]
        image_group_paths = context.get("__image_group_paths", [])
        survey_code = context["survey_code"]
        nas_root_path = context.get("nas_root_path")
        nas_folder_path = context["nas_folder_path"]
        country_code = context["country_code"]
        s3_location_bucket = context["s3_location_bucket"]

        logger.info("[Context] project_path=%s | project_name=%s | image_group_paths=%s | "
                    "survey_code=%s | nas_root_path=%s | nas_folder_path=%s | country_code=%s",
                    project_path, project_name, image_group_paths,
                    survey_code, nas_root_path, nas_folder_path, country_code)

        grouped_by_s3 = defaultdict(list)
        for group in image_group_paths:
            s3_folder = group.get("s3FolderId")
            grouped_by_s3[s3_folder].append(group)

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found: {project_file_path}")

        doc = ms.Document()
        doc.open(project_file_path, read_only=False)

        chunk = doc.addChunk()
        chunk.label = "Chunk_HL_HR"

        created_camera_groups = {}

        for s3_folder, groups in grouped_by_s3.items():
            folder_path = os.path.join(nas_root_path, nas_folder_path, s3_location_bucket, s3_folder)
            logger.info(f"Checking folder: {folder_path}")

            photo_paths = []
            for ext in ["*.jpg", "*.JPG", "*.jpeg", "*.JPEG", "*.png", "*.PNG"]:
                photo_paths.extend(glob.glob(os.path.join(folder_path, ext)))

            if not photo_paths:
                logger.warning(f"No images found in {folder_path}")
                continue

            group = groups[0]
            section = group.get("section", "unknown")
            section_normalized = normalize_section_name(section)
            folder_suffix = s3_folder.rstrip("/").split("/")[-1]
            camera_group_label = f"{survey_code}_{folder_suffix}_{section_normalized}"

            if camera_group_label not in created_camera_groups:
                camera_group = chunk.addCameraGroup()
                camera_group.label = camera_group_label
                created_camera_groups[camera_group_label] = camera_group
                logger.info(f"Created camera group: {camera_group_label}")
            else:
                camera_group = created_camera_groups[camera_group_label]

            added_cameras = chunk.addPhotos(photo_paths, group=camera_group)
            if not added_cameras:
                logger.warning(f"No cameras added from: {folder_path}")
                continue

        doc.save()

        camera_group_labels = list(created_camera_groups.keys())

        payload = {
            "chunk_label": chunk.label,
            "camera_group_labels": camera_group_labels
        }

        logger.info(f"[{task_name}] Created chunk '{chunk.label}' with {len(camera_group_labels)} camera groups.")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(
                f"[{task_name}] Task completed successfully. Created chunk '{chunk.label}' with {len(camera_group_labels)} camera groups.")
        except Exception as notify_error:
            logger.error(f"[{task_name}] Notification failed: {type(notify_error).__name__} - {str(notify_error)}")
            raise
    except Exception as e:
        logger.error(f"[{task_name}] Task failed create data chunks hl hr: {type(e).__name__} - {str(e)}")

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
