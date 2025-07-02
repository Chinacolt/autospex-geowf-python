import Metashape
import logging

from Metashape.Metashape import Tasks

from common.config import inject, get_variable
from common.helpers import notify_task_completion
import time

logger = logging.getLogger(__name__)

@inject(
    workflow_conf_key="workflowId",
    read_params=[
        'run_alignment_batch_id'
    ],
    method="GET"
)
def wait_wf_run_alignment(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    run_alignment_batch_id = context["run_alignment_batch_id"]
    metashape_server_ip = get_variable("METASHAPE_SERVER_IP")

    logger.info(f"run_alignment_batch_id: {run_alignment_batch_id}")
    try:
        client = Metashape.NetworkClient()
        client.connect(metashape_server_ip)
        while True:
            batch_info = client.batchInfo(run_alignment_batch_id)
            # {'aborted': False, 'failed': False, 'finished_at': 1749810377, 'max_worker_version': '2.2', 'meta': None, 'min_worker_version': '2.2', 'next_revision': 1, 'path': 'CAN/ABC/LAF/SPLW/2409/3dprocessing/metashape/CAN_ABC_LAF_SPLW_2409_3dprocessing.psx', 'paused': False, 'priority': 0, 'started_at': 1749806999, 'state': 'completed', 'tasks': [{'aborted': False, 'elapsed_time': 3.389535, 'errors': [], 'failed': False, 'gpu_support': True, 'name': 'MatchPhotos', 'output': [{'text': '2025-06-13 13:26:14 registration accepted\n', 'type': 0}, {'text': '2025-06-13 13:26:14 MatchPhotos: downscale = 1, generic_preselection = on, reference_preselection = on, filter_mask = off, mask_tiepoints = on, filter_stationary_points = on, keypoint_limit = 40000, keypoint_limit_per_mpx = 1000, tiepoint_limit = 4000, guided_matching = off, exclude_corners = off\n', 'type': 0}, {'text': '2025-06-13 13:26:15 Matching photos...\n', 'type': 0}, {'text': '2025-06-13 13:26:15 processing finished in 0.319001 sec\n', 'type': 0}, {'text': '2025-06-13 13:26:16 registration accepted\n', 'type': 0}, {'text': '2025-06-13 13:26:16 MatchPhotos.initialize (1/1): downscale = 1, subtask = initialize, generic_preselection = on, reference_preselection = on, filter_mask = off, mask_tiepoints = on, filter_stationary_points = on, keypoint_limit = 40000, keypoint_limit_per_mpx = 1000, tiepoint_limit = 4000, guided_matching = off, exclude_corners = off, data_folder = /home/guven/Documents/nas_root_test_path/autospex-geo/CAN/ABC/LAF/SPLW/2409/3dprocessing/metashape/CAN_ABC_LAF_SPLW_2409_3dprocessing.files/0/0/point_cloud\n', 'type': 0}, {'text': '2025-06-13 13:26:16 processing finished in 0.312262 sec\n', 'type': 0}, {'text': '2025-06-13 13:26:17 registration accepted\n', 'type': 0}, {'text': '2025-06-13 13:26:17 MatchPhotos.cleanup (1/1): downscale = 1, subtask = cleanup, generic_preselection = on, reference_preselection = on, filter_mask = off, mask_tiepoints = on, filter_stationary_points = on, keypoint_limit = 40000, keypoint_limit_per_mpx = 1000, tiepoint_limit = 4000, guided_matching = off, exclude_corners = off, data_folder = /home/guven/Documents/nas_root_test_path/autospex-geo/CAN/ABC/LAF/SPLW/2409/3dprocessing/metashape/CAN_ABC_LAF_SPLW_2409_3dprocessing.files/0/0/point_cloud\n', 'type': 0}, {'text': '2025-06-13 13:26:17 processing finished in 0.319426 sec\n', 'type': 0}], 'params': None, 'params_text': 'downscale = 1, generic_preselection = on, reference_preselection = on, filter_mask = off, mask_tiepoints = on, filter_stationary_points = on, keypoint_limit = 40000, keypoint_limit_per_mpx = 1000, tiepoint_limit = 4000, guided_matching = off, exclude_corners = off', 'processing_time': 3.376907, 'progress': 100.0, 'remaining_time': 0.0, 'state': 'completed', 'target_count': 1, 'targets_completed': 1, 'targets_failed': 0, 'title': 'Match Photos', 'workitems_completed': 3, 'workitems_failed': 0, 'workitems_inprogress': 0, 'workitems_waiting': 0}], 'username': 'guven', 'uuid': '34ed8715-277c-493d-819f-f8ce70d9b23f', 'worker_limit': 0, 'worker_version': '2.2.1.20641'}
            
            print(f"Batch Info: {batch_info}")
            # IF completed, break the loop
            if batch_info['state'] == 'completed':
                logger.info("Batch processing completed.")
                try:
                    workflow_id = context["dag_run"].conf.get("workflowId")
                    notify_task_completion(
                        workflow_id=workflow_id,
                        task_name="run_alignment",
                        payload={"run_alignment": "completed"}
                    )
                except Exception as notify_error:
                    logger.error(f"[ERROR] Failed to notify task completion: {str(notify_error)}")            
                break
            elif batch_info['state'] in ['pending', 'queued', 'inprogress']:
                print(f"Batch state is {batch_info['state']}. Waiting for completion...")
            else:
                # throw AirflowException
                print(f"Batch processing failed or aborted with state: {batch_info['state']}")
                raise Exception(f"Batch processing failed or aborted with state: {batch_info['state']}")
            # Wait for a while before checking again
            time.sleep(60)
        
    except Exception as e:
        logger.error(f"Error while waiting for batch completion: {str(e)}")

        error_payload = {
            "workflowId": workflow_id,
            "taskName": "run_alignment",
            "errorMessage": str(e)
        }

        task_instance.xcom_push(key="run_alignment", value=error_payload)
        raise e

    finally:
        client.disconnect()
        logger.info("Disconnected from Metashape Network Client.")