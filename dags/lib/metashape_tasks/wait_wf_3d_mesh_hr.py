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
        'mesh_hr_batch_id',
        "project_path",
        "project_name"
    ],
    method="GET"
)
def wait_wf_3d_mesh_hr(**context):
    task_instance = context.get("task_instance") or context.get("ti")
    mesh_hr_batch_id = context["mesh_hr_batch_id"]
    metashape_server_ip = get_variable("METASHAPE_SERVER_IP")

    project_path = context.get("project_path")
    project_name = context.get("project_name")

    logger.info(f"mesh_hr_batch_id: {mesh_hr_batch_id}")
    try:
        client = Metashape.NetworkClient()
        client.connect(metashape_server_ip)
        while True:
            batch_info = client.batchInfo(mesh_hr_batch_id)
            
            print(f"Batch Info: {batch_info}")
            # IF completed, break the loop
            if batch_info['state'] == 'completed':
                logger.info("Batch processing completed.")
                try:
                    workflow_id = context["dag_run"].conf.get("workflowId")
                    notify_task_completion(
                        workflow_id=workflow_id,
                        task_name="build_3d_mesh_high_resolution",
                        payload={"build_3d_mesh_high_resolution": "completed"}
                    )
                except Exception as notify_error:
                    logger.error(f"[ERROR] Failed to notify task completion: {str(notify_error)}")            
                break
            # 10 saniyede bir burada log basılıyordu performans açısından in progress statüsünde loglama kaldırıldı
            # elif batch_info['state'] in ['pending', 'queued', 'inprogress']:
            #     print(f"Batch state is {batch_info['state']}. Waiting for completion...")
            elif batch_info['state'] in ['failed', 'aborted']:
                # throw AirflowException
                print(f"Batch processing failed or aborted with state: {batch_info['state']}")
                raise Exception(f"Batch processing failed or aborted with state: {batch_info['state']}")
            # Wait for a while before checking again
            time.sleep(60)
        
    except Exception as e:
        logger.error(f"Error while waiting for batch completion: {str(e)}")

        error_payload = {
            "workflowId": workflow_id,
            "taskName": "build_3d_mesh_high_resolution",
            "errorMessage": str(e),
            "projectInfo": {
                "project_path": project_path,
                "project_name": project_name
            }

        }

        task_instance.xcom_push(key="build_3d_mesh_high_resolution", value=error_payload)
        raise e

    finally:
        client.disconnect()
        logger.info("Disconnected from Metashape Network Client.")