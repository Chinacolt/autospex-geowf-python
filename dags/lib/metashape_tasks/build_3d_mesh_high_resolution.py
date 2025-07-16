import Metashape
import os
import logging
from common.config import inject
from common.helpers import notify_task_completion

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "chunk_label_HR",
        "project_code"
    ],
    method="GET"
)
def build_3d_mesh_high_resolution(**context):

    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id
    dag_run = context.get("dag_run")
    workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
    project_path = context["project_path"]
    project_name = context["project_name"]

    try:


        logger.info(f"[{task_name}] Starting build_3d_mesh_high_resolution task with workflowId: {workflow_id}")
        chunk_label = context["chunk_label_HR"]
        project_code = context["project_code"]

        logger.info(f"[INFO] Starting build 3d mesh high resolution for chunk '{chunk_label}'")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")

        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at: {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path)

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        build_3d_mesh_high_resolution_filepath = os.path.join(project_path, "export")
        if not os.path.exists(build_3d_mesh_high_resolution_filepath):
            os.makedirs(build_3d_mesh_high_resolution_filepath, exist_ok=True)

        build_3d_mesh_high_resolution_name = f"{project_code}_3dmesh_hr.obj"
        output_path = os.path.join(build_3d_mesh_high_resolution_filepath, build_3d_mesh_high_resolution_name)

        logger.info(f"[DEBUG] Output Path, 3d mesh high resolution: {output_path}")

        if not chunk.depth_maps:
            logger.info("[INFO] Depth maps not found. Its Generating...")
            chunk.buildDepthMaps(
                downscale=2,
                filter_mode=Metashape.MildFiltering
            )
            logger.info("[INFO] Depth maps produced.")
        else:
            logger.info("[INFO] Depth maps is already exists. This will use.")

        logger.info("[INFO] Mesh model is creating...")
        chunk.buildModel(
            surface_type=Metashape.SurfaceType.Arbitrary,
            source_data=Metashape.DataSource.DepthMapsData,
            interpolation=Metashape.Interpolation.EnabledInterpolation,
            face_count=Metashape.FaceCount.MediumFaceCount,
            vertex_colors=True,
            reuse_depth=True
        )
        logger.info("[INFO] Mesh model created.")

        chunk.exportModel(
            path=output_path,
            binary=True,
            precision=6,
            texture_format=Metashape.ImageFormat.ImageFormatJPEG,
            texture=True,
            normals=True,
            colors=True,
            cameras=False,
            format=Metashape.ModelFormat.ModelFormatOBJ
        )

        logger.info(f"[INFO] Mesh model is exported: {output_path}")

        try:
            workflow_id = context["dag_run"].conf.get("workflowId")
            logger.info(f"[DEBUG] Received workflowId from triggering DAG: {workflow_id}")
        except Exception as e:
            raise Exception(f"[ERROR] workflowId could not be fetched from context: {e}")

        if not workflow_id:
            raise Exception("workflowId not found in dag_run.conf")


        payload={
            "3d_mesh_hr_output_path": output_path
        }

        try:

            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Build 3d mesh high resolution {output_path}")
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

        task_instance.xcom_push(key=task_name, value=error_payload)
        raise
