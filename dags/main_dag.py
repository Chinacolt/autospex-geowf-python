from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.callbacks import task_failure_callback
from common.helpers import notify_task_completion
from lib.metashape_manager import manager as metashape_manager
from lib.metashape_tasks.build_3d_mesh_high_level import build_3d_mesh_high_level
from lib.metashape_tasks.build_3d_mesh_high_resolution import build_3d_mesh_high_resolution
from lib.metashape_tasks.copy_data_to_nas import copy_data_to_nas
from lib.metashape_tasks.create_data_chunks_hl_hr import create_data_chunks_hl_hr
from lib.metashape_tasks.disable_geo_tags import disable_geo_tags
from lib.metashape_tasks.disable_images import disable_images
from lib.metashape_tasks.duplicate_chunk_block_with_hl import duplicate_chunk_block_with_hl
from lib.metashape_tasks.duplicate_chunk_block_with_hr import duplicate_chunk_block_with_hr
from lib.metashape_tasks.enable_geo_tags import enable_geo_tags
from lib.metashape_tasks.export_camera_positions_high_level import export_camera_positions_high_level
from lib.metashape_tasks.export_camera_positions_high_resolution import export_camera_positions_high_resolution
from lib.metashape_tasks.init_project import create_metashape_project
from lib.metashape_tasks.reivew_alignment_and_georeferencing import review_alignment_and_georeferencing
from lib.metashape_tasks.review_tie_point_removal_result import review_tie_point_removal_result
from lib.metashape_tasks.run_alignment import run_alignment
from lib.metashape_tasks.run_tie_point_removal_reiteration import run_tie_point_removal_reiteration
from lib.metashape_tasks.wait_wf_3d_mesh_hl import wait_wf_3d_mesh_hl
from lib.metashape_tasks.wait_wf_3d_mesh_hr import wait_wf_3d_mesh_hr
from lib.metashape_tasks.wait_wf_run_alignment import wait_wf_run_alignment

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
        dag_id='metashape_chunk_pipeline',
        default_args=default_args,
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        on_failure_callback=task_failure_callback,
        tags=["metashape"],
) as dag:
    @task(
        task_id="create_cluster",
        executor_config={
            "overrides": {
                "cpu": "256",
                "memory": "512"
            }
        }
    )
    def t_create_cluster(**context):
        task_instance = context.get("task_instance") or context.get("ti")
        dag_run = context.get("dag_run")
        task_name = task_instance.task_id
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        if workflow_id is None:
            raise ValueError("workflowId is required!")

        server_instance_type = "c5.large"
        worker_instance_type = "g6.xlarge"
        worker_count = 1

        server_fqdn, worker_data_path = metashape_manager.create_cluster(uniq_id=workflow_id,
                                                                         server_instance_type=server_instance_type,
                                                                         worker_instance_type=worker_instance_type,
                                                                         worker_count=worker_count
                                                                         )
        # task_instance.xcom_push(key="metashape_server_ip", value=server_fqdn)
        # task_instance.xcom_push(key="nas_root_path", value=worker_data_path)

        payload = {
            "metashape_server_ip": server_fqdn,
            "nas_root_path": worker_data_path
        }

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            print(f"[SUCCESS] Task completed successfully. XCom payload pushed.")
        except Exception as notify_error:
            print(f"[ERROR] Failed to notify task completion: {str(notify_error)}")
            raise


    @task(task_id="copy_data_to_nas",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_copy_data_to_nas(**context):
        return copy_data_to_nas(**context)


    @task(task_id="create_metashape_project",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_create_metashape_project(**context):
        return create_metashape_project(**context)


    @task(task_id="create_data_chunks", on_failure_callback=task_failure_callback,
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_create_chunk(**context):
        return create_data_chunks_hl_hr(**context)


    @task(task_id="disable_images",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_disable_images(**context):
        return disable_images(**context)


    @task(task_id="disable_geo_tags",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_disable_geo_tags(**context):
        return disable_geo_tags(**context)


    @task(task_id="run_alignment",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_run_alignment(**context):
        return run_alignment(**context)


    @task(task_id="wait_wf_run_alignment",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_wait_wf_run_alignment(**context):
        return wait_wf_run_alignment(**context)


    @task(task_id="review_alignment_and_georeferencing",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_review_alignment_and_georeferencing(**context):
        return review_alignment_and_georeferencing(**context)


    @task(task_id="enable_geo_tags",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_enable_geo_tags(**context):
        return enable_geo_tags(**context)


    @task(task_id="run_tie_point_removal_reiteration",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_run_tie_point_removal_reiteration(**context):
        return run_tie_point_removal_reiteration(**context)


    @task(task_id="review_tie_point_removal_result",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_review_tie_point_removal_result(**context):
        return review_tie_point_removal_result(**context)


    @task(task_id="duplicate_chunk_block_with_hl",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_duplicate_chunk_block_with_hl(**context):
        return duplicate_chunk_block_with_hl(**context)


    @task(task_id="duplicate_chunk_block_with_hr",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_duplicate_chunk_block_with_hr(**context):
        return duplicate_chunk_block_with_hr(**context)


    @task(task_id="export_camera_positions_high_level",
          executor_config={
              "overrides": {
                  "cpu": "2048",
                  "memory": "4096"
              }
          })
    def t_export_camera_positions_high_level(**context):
        return export_camera_positions_high_level(**context)


    @task(task_id="export_camera_positions_high_resolution",
          executor_config={
              "overrides": {
                  "cpu": "8192",
                  "memory": "32768"
              }
          })
    def t_export_camera_positions_high_resolution(**context):
        return export_camera_positions_high_resolution(**context)


    @task(task_id="build_3d_mesh_high_level",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_build_3d_mesh_high_level(**context):
        return build_3d_mesh_high_level(**context)


    @task(task_id="wait_wf_3d_mesh_hl",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_wait_wf_3d_mesh_hl(**context):
        return wait_wf_3d_mesh_hl(**context)


    @task(task_id="build_3d_mesh_high_resolution",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_build_3d_mesh_high_resolution(**context):
        return build_3d_mesh_high_resolution(**context)


    @task(task_id="wait_wf_3d_mesh_hr",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_wait_wf_3d_mesh_hr(**context):
        return wait_wf_3d_mesh_hr(**context)


    @task(task_id="destroy_cluster",
          executor_config={
              "overrides": {
                  "cpu": "256",
                  "memory": "512"
              }
          })
    def t_destroy_cluster(**context):
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        metashape_manager.destroy_cluster(uniq_id=workflow_id)


    (
            t_create_cluster()
            >> t_copy_data_to_nas()
            >> t_create_metashape_project()
            >> t_create_chunk()
            >> t_disable_images()
            >> t_disable_geo_tags()
            >> t_run_alignment()
            >> t_wait_wf_run_alignment()
            >> t_review_alignment_and_georeferencing()
            >> t_enable_geo_tags()
            >> t_run_tie_point_removal_reiteration()
            >> t_review_tie_point_removal_result()
            >> t_duplicate_chunk_block_with_hl()
            >> t_duplicate_chunk_block_with_hr()
            >> t_export_camera_positions_high_level()
            >> t_export_camera_positions_high_resolution()
            >> t_build_3d_mesh_high_level()
            >> t_wait_wf_3d_mesh_hl()
            >> t_build_3d_mesh_high_resolution()
            >> t_wait_wf_3d_mesh_hr()
            >> t_destroy_cluster()
    )
