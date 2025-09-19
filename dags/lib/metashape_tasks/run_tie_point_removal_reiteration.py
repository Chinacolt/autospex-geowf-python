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
        "reprojection_error_threshold",
        "reconstruction_uncertainty_threshold",
        "projection_accuracy_threshold", "metashape_server_ip", "nas_root_path"
    ],
    method="GET"
)
def run_tie_point_removal_reiteration(**context):
    task_instance = context.get('task_instance') or context.get('ti')
    task_name = task_instance.task_id

    try:
        dag_run = context.get("dag_run")
        workflow_id = dag_run.conf.get("workflowId") if dag_run else "unknown"
        logger.info(f"[{task_name}] Starting run_tie_point_removal_reiteration task with workflowId: {workflow_id}")
        project_path = context["project_path"]
        project_name = context["project_name"]
        chunk_label = context["chunk_label"]
        reprojection_error_threshold = float(context["reprojection_error_threshold"])
        reconstruction_uncertainty_threshold = int(context["reconstruction_uncertainty_threshold"])
        projection_accuracy_threshold = int(context["projection_accuracy_threshold"])

        project_file_path = os.path.join(project_path, f"{project_name}.psx")
        if not os.path.exists(project_file_path):
            raise FileNotFoundError(f"Project file not found at {project_file_path}")

        doc = Metashape.Document()
        doc.open(project_file_path, read_only=False)

        chunk = next((c for c in doc.chunks if c.label == chunk_label), None)
        if not chunk:
            raise ValueError(f"Chunk with label '{chunk_label}' not found in project.")

        # Tie point filtering
        tiepoint_filter = Metashape.TiePoints.Filter()
        logger.info(f"Tie Point Filter initialized.")

        # Step 1: Reprojection error
        tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ReprojectionError)
        tiepoint_filter.selectPoints(reprojection_error_threshold)
        first_reproj = len(tiepoint_filter.values)
        tiepoint_filter.removePoints(reprojection_error_threshold)
        logger.info(f"First reprojection: {first_reproj}")

        # Step 2: Reconstruction uncertainty
        tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ReconstructionUncertainty)
        tiepoint_filter.selectPoints(reconstruction_uncertainty_threshold)
        tiepoint_filter.removePoints(reconstruction_uncertainty_threshold)

        # Step 3: Projection accuracy
        tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ProjectionAccuracy)
        tiepoint_filter.selectPoints(projection_accuracy_threshold)
        tiepoint_filter.removePoints(projection_accuracy_threshold)

        # Step 4: Optimize cameras
        chunk.optimizeCameras(
            fit_f=True, fit_cx=True, fit_cy=True,
            fit_b1=False, fit_b2=False, fit_k1=True,
            fit_k2=True, fit_k3=True, fit_k4=False,
            fit_p1=True, fit_p2=True, fit_corrections=False,
            adaptive_fitting=False, tiepoint_covariance=False
        )

        # Step 5: Reprojection kontrolü sonrası fark ölçümü
        tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ReprojectionError)
        second_reproj = len(tiepoint_filter.values)
        reproj_difference = first_reproj - second_reproj

        logger.info(f"Second reprojection: {second_reproj}")
        logger.info(f"Reprojection Difference: {reproj_difference}")

        # Reiterasyon döngüsü
        while reproj_difference > 100:
            first_reproj = second_reproj
            logger.info(f"(loop) First reprojection: {first_reproj}")

            tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ReprojectionError)
            tiepoint_filter.selectPoints(reprojection_error_threshold)
            tiepoint_filter.removePoints(reprojection_error_threshold)

            chunk.optimizeCameras(
                fit_f=True, fit_cx=True, fit_cy=True,
                fit_b1=False, fit_b2=False, fit_k1=True,
                fit_k2=True, fit_k3=True, fit_k4=False,
                fit_p1=True, fit_p2=True, fit_corrections=False,
                adaptive_fitting=False, tiepoint_covariance=False
            )

            tiepoint_filter.init(chunk, criterion=Metashape.TiePoints.Filter.ReprojectionError)
            second_reproj = len(tiepoint_filter.values)
            reproj_difference = first_reproj - second_reproj

            logger.info(f"(loop) Second reprojection: {second_reproj}")
            logger.info(f"(loop) Reprojection Difference: {reproj_difference}")

        logger.info(f"Final reprojection difference: {reproj_difference}")

        doc.save()

        payload = {
            "first_reproj": first_reproj,
            "second_reproj": second_reproj,
            "reproj_difference": reproj_difference
        }

        logger.info(f"[{task_name}] Task completed successfully. Payload: {json.dumps(payload, indent=2)}")

        try:
            notify_task_completion(
                workflow_id=workflow_id,
                task_name=task_name,
                payload=payload
            )
            logger.info(f"[{task_name}] Task completed successfully. Payload sent.")
        except Exception as notify_error:
            logger.error(f"[{task_name}] Notification failed: {type(notify_error).__name__} - {str(notify_error)}")
            raise notify_error



    except Exception as e:
        logger.exception(f"[{task_name}] Task failed: {str(e)}")

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
