import json
import logging

import requests

from .config import get_keycloak_token, get_variable


def notify_task_completion(workflow_id: str, task_name: str, payload: dict):
    try:
        base_url = get_variable("WORKFLOW_API_URL_TEMPLATE")
        target_url = f"{base_url}/complete/{workflow_id}/task/{task_name}"

        token = get_keycloak_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        logging.info(f"[DEBUG] Sending task completion notification to: {target_url}")
        logging.info(f"[DEBUG] Payload: {json.dumps(payload)}")

        response = requests.put(target_url, headers=headers, json=payload)

        logging.info(f"[DEBUG] Completion response: {response.status_code} - {response.text}")
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"[ERROR] HTTP request failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"[ERROR] Failed to notify task completion: {str(e)}")
        raise


def notify_task_failure(workflow_id: str, task_name: str, error_message: str):
    try:
        base_url = get_variable("WORKFLOW_API_URL_TEMPLATE")
        target_url = f"{base_url}/fail/{workflow_id}/task/{task_name}"

        logging.info(f"[DEBUG] Target URL for task failure notification: {target_url}")

        token = get_keycloak_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        payload = {
            "error": error_message
        }

        logging.info(f"[DEBUG] Sending task failure notification to: {target_url}")
        logging.info(f"[DEBUG] Payload: {json.dumps(payload)}")

        response = requests.put(target_url, headers=headers, json=payload)

        logging.info(f"[notify_task_failure] Response status code: {response.status_code}")

        logging.info(f"[DEBUG] Failure response: {response.status_code} - {response.text}")
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"[ERROR] HTTP request failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"[ERROR] Failed to notify task failure: {str(e)}")
        raise
