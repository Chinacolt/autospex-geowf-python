import Metashape
import os
import logging
import json
from common.config import inject, get_variable
from common.helpers import notify_task_completion

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        "project_path",
        "project_name",
        "chunk_label_HL",
        "project_code",
        "mesh_hl_batch_id"
    ],
    method="GET"
)
def build_3d_mesh_high_level(**context):

    task_instance = context.get("task_instance") or context.get("ti")
    task_name = task_instance.task_id
    dag_run = context.get("dag_run")
    workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
    project_path = context["project_path"]
    project_name = context["project_name"]
    mesh_hl_batch_id = context.get("mesh_hl_batch_id")

    try:
        
        nas_root_path = get_variable("nas_root_path")
        windows_root_path = get_variable("windows_nas_root_path")
        metashape_server_ip = get_variable("METASHAPE_SERVER_IP")

        logger.info(f"[{task_name}] Starting build_3d_mesh_high_level task with workflowId: {workflow_id}")
        chunk_label = context["chunk_label_HL"]
        project_code = context["project_code"]

        logger.info(f"[INFO] Starting build 3d mesh high level for chunk '{chunk_label}'")

        project_file_path = os.path.join(project_path, f"{project_name}.psx")

        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at: {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        build_3d_mesh_high_level_filepath = os.path.join(project_path, "export")
        if not os.path.exists(build_3d_mesh_high_level_filepath):
            os.makedirs(build_3d_mesh_high_level_filepath, exist_ok=True)

        build_3d_mesh_high_level_name = f"{project_code}_3dmesh_hl.obj"
        output_path = os.path.join(build_3d_mesh_high_level_filepath, build_3d_mesh_high_level_name)
        
        logger.info(f"[DEBUG] Output Path, 3d mesh high level: {output_path}")

        server_output_path = output_path.replace(nas_root_path, windows_root_path).replace("/", "\\")
        logger.info(f"[DEBUG] Server-side output path for 3d mesh high level: {server_output_path}")

        # Daha önceki batch varsa iptal et
        if mesh_hl_batch_id:
            try:
                client = Metashape.NetworkClient()
                client.connect(metashape_server_ip)
                client.abortBatch(mesh_hl_batch_id)
                logger.info(f"Previous batch {mesh_hl_batch_id} aborted.")
                client.disconnect()
            except Exception as e:
                logger.warning(f"[WARNING] Could not abort previous batch: {e}")

        # 1. DepthMaps Task
        build_depth_maps_task = Metashape.Tasks.BuildDepthMaps()
        build_depth_maps_task.downscale = 2
        build_depth_maps_task.filter_mode = Metashape.FilterMode.MildFiltering
        build_depth_maps_task.subdivide_task = True

        # 2. BuildModel Task
        build_model_task = Metashape.Tasks.BuildModel()
        build_model_task.source_data = Metashape.DataSource.DepthMapsData
        build_model_task.surface_type = Metashape.SurfaceType.Arbitrary
        build_model_task.face_count = Metashape.FaceCount.HighFaceCount
        build_model_task.interpolation = Metashape.Interpolation.EnabledInterpolation
        build_model_task.vertex_colors = True
        build_model_task.keep_depth = True 
        build_model_task.subdivide_task = True

        # 3. BuildUV Task
        build_uv_task = Metashape.Tasks.BuildUV()
        build_uv_task.mapping_mode = Metashape.MappingMode.GenericMapping
        build_uv_task.texture_size = 8192
        build_uv_task.page_count = 1

        # 4. BuildTexture Task
        build_texture_task = Metashape.Tasks.BuildTexture()
        build_texture_task.blending_mode = Metashape.BlendingMode.MosaicBlending
        build_texture_task.texture_size = 8192 # Should generally match the texture_size set in build_uv_task
        build_texture_task.fill_holes = True
        build_texture_task.ghosting_filter = True
        build_texture_task.source_data = Metashape.DataSource.ImagesData
        build_texture_task.texture_type = Metashape.Model.TextureType.DiffuseMap
        
        # 5. ExportModel Task
        export_model_task = Metashape.Tasks.ExportModel()
        export_model_task.path = server_output_path
        export_model_task.format = Metashape.ModelFormat.ModelFormatOBJ
        export_model_task.binary = False
        export_model_task.save_texture = True
        export_model_task.save_normals = True
        export_model_task.save_colors = True
        export_model_task.save_uv = True
        export_model_task.save_markers = True

        # Batch'i oluştur ve başlat
        client = Metashape.NetworkClient()
        client.connect(metashape_server_ip)

        relative_path = os.path.join(project_path, f"{project_name}.psx").replace(nas_root_path, windows_root_path).replace(
            "/", "\\")
        logger.info(f"[DEBUG] Relative project path for batch: {relative_path}")

        batch_id = client.createBatch(relative_path, [build_depth_maps_task.toNetworkTask(chunk), build_model_task.toNetworkTask(chunk), build_uv_task.toNetworkTask(chunk), build_texture_task.toNetworkTask(chunk), export_model_task.toNetworkTask(chunk)])
        client.setBatchPaused(batch_id, False)
        logger.info(f"[SUCCESS] Batch submitted and running. ID: {batch_id}")

        try:
            workflow_id = context["dag_run"].conf.get("workflowId")
            logger.info(f"[DEBUG] Received workflowId from triggering DAG: {workflow_id}")
        except Exception as e:
            raise Exception(f"[ERROR] workflowId could not be fetched from context: {e}")

        if not workflow_id:
            raise Exception("workflowId not found in dag_run.conf")


        payload={
            "mesh_hl_output_path": server_output_path,
            "mesh_hl_batch_id": batch_id
        }

        logger.info(f"[{task_name}] Payload prepared for notification: {json.dumps(payload, indent=2)}")

        try:

            notify_task_completion(
                workflow_id=workflow_id,
                task_name="wait_wf_3d_mesh_hl",
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Build 3d mesh high level {server_output_path}")
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
