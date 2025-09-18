import Metashape
import os
import logging
import json
from common.config import inject, get_variable
from common.helpers import notify_task_completion

from lib.metashape import with_licence

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "country_code",
        "project_path",
        "project_name",
        "__image_group_paths",
        "nas_root_path",
        "nas_folder_path",
        "chunk_label",
        "camera_group_labels"
    ],
    method="GET"
)
@with_licence
def disable_images(**context):
    task_instance = context.get('task_instance') or context.get('ti')
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting disable_images task with workflowId: {workflow_id}")

        project_path = context["project_path"]
        project_name = context["project_name"]
        chunk_label = context["chunk_label"]
        image_group_paths = context.get("__image_group_paths", [])

        logger.info(f"[INFO] Project: {project_name} | Chunk: {chunk_label} | Groups: {len(image_group_paths)}")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        disable_filenames = {
            group.get("fileName").lower()
            for group in image_group_paths
            if not group.get("includeFlag", True) and group.get("fileName")
        }

        logger.info(f"[INFO] Found {len(disable_filenames)} images to disable based on includeFlag=False")

        disabled_count = 0
        group_disable_map = {}

        for camera in chunk.cameras:
            if not (camera.photo and camera.photo.path and camera.group):
                continue

            filename = os.path.basename(camera.photo.path).lower()
            group_label = camera.group.label

            if filename in disable_filenames:
                camera.enabled = False
                disabled_count += 1
                group_disable_map.setdefault(group_label, []).append(filename)


        disabled_camera_group_labels = []
        for group in chunk.camera_groups:
            label = group.label
            group_cameras = [c for c in chunk.cameras if c.group and c.group.label == label]
            total = len(group_cameras)
            disabled = sum(1 for c in group_cameras if not c.enabled)

            if total == 0:
                continue

            if disabled == total:
                disabled_camera_group_labels.append({"name": label, "disable": True})
            elif label in group_disable_map:
                disabled_camera_group_labels.append({
                    "name": label,
                    "disable": group_disable_map[label]
                })

        doc.save()

        logger.info(f"[INFO] Total disabled images: {disabled_count}")

        payload = {
            "disabled_camera_group_labels": disabled_camera_group_labels
        }

        logger.info(f"[{task_name}] Disabled {disabled_count} images in chunk '{chunk_label}'.")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Disabled {disabled_count} images.")
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
