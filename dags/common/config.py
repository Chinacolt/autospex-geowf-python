from airflow.exceptions import AirflowException
from airflow.models import Variable
import requests
import logging
from typing import Any, Callable, TypeVar, List
from functools import wraps
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json

from pprint import pprint

T = TypeVar('T')
logger = logging.getLogger("airflow.task")

class APIConstants:
    DEFAULT_TIMEOUT = 30
    CONTENT_TYPE = "application/json"


def create_session() -> requests.Session:
    logger.info("[create_session] Initializing session with retry strategy...")
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    logger.info("[create_session] Session created and adapter mounted.")
    return session


def get_variable(variable_name: str) -> str:
    logger.info(f"[get_variable] Fetching variable '{variable_name}' from Airflow...")
    return Variable.get(variable_name)


def get_keycloak_token() -> str:
    try:
        logger.info("[get_keycloak_token] Using subprocess + curl to fetch token...")

        keycloak_base_url = Variable.get("keycloak_base_url")
        realm = Variable.get("keycloak_realm")
        client_id = Variable.get("keycloak_client_id")
        username = Variable.get("keycloak_username")
        password = Variable.get("keycloak_password")

        token_url = f"{keycloak_base_url}/realms/{realm}/protocol/openid-connect/token"

        payload = {
            "grant_type": "password",
            "client_id": client_id,
            "username": username,
            "password": password
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(token_url, data=payload, headers=headers, verify=False)
        response.raise_for_status()

        response_json = response.json()

        access_token = response_json.get("access_token")
        if not access_token:
            raise AirflowException(f"No access token in response: {response_json}")

        logger.info(f"[get_keycloak_token] Access token successfully obtained via curl. Token: {access_token}")
        return access_token

    except requests.exceptions.RequestException as e:
        logger.error(f"[get_keycloak_token] HTTP request failed: {e}")
        raise AirflowException("Failed to get Keycloak token via requests.")
    except Exception as e:
        logger.exception(f"[get_keycloak_token] Unexpected exception: {str(e)}")
        raise

def inject(workflow_conf_key: str, read_params: List[str], method: str):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(**context) -> Any:
            task_instance = context.get('task_instance') or context.get('ti')
            task_name = task_instance.task_id if task_instance else func.__name__
            logger.info(f"[{task_name}] inject decorator started.")

            try:
                dag_run = context.get('dag_run')
                if not dag_run:
                    raise Exception(f"[{task_name}] Missing dag_run in context. This task must be triggered via TriggerDagRunOperator with conf.")

                dag_run_conf = getattr(dag_run, "conf", {})
                logger.info(f"[{task_name}] dag_run.conf: {dag_run_conf}")

                if workflow_conf_key not in dag_run_conf:
                    raise KeyError(f"[{task_name}] Required parameter '{workflow_conf_key}' not found in dag_run.conf")

                workflow_id_value = dag_run_conf[workflow_conf_key]
                logger.info(f"[{task_name}] Resolved workflowId: {workflow_id_value}")

                token = get_keycloak_token()
                logger.info(f"[{task_name}] Retrieved Keycloak token (first 20 chars): {token[:20]}...")

                url_template = get_variable("WORKFLOW_API_URL_TEMPLATE")
                base_url = f"{url_template}/start/{workflow_id_value}/task/{task_name}"

                http_method = method.upper()
                if http_method not in ['GET', 'POST', 'PUT', 'DELETE']:
                    raise ValueError(f"[{task_name}] Invalid HTTP method: {http_method}")

                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": APIConstants.CONTENT_TYPE
                }

                params = {}
                if http_method == "GET" and read_params:
                    params = { "parameters": read_params }
                    logger.info(f"[{task_name}] GET params: {params}")

                data = {}
                if http_method != "GET":
                    data = context.get("body", {})
                    logger.info(f"[{task_name}] Request body: {json.dumps(data)}")

                logger.info(f"[{task_name}] Sending {http_method} request to: {base_url}")

                response = requests.request(
                    method=http_method,
                    url=base_url,
                    headers=headers,
                    params=params if http_method == "GET" else None,
                    json=data if http_method != "GET" else None
                )

                logger.info(f"[{task_name}] Response code: {response.status_code}")
                response.raise_for_status()

                api_response = response.json()
                logger.info(f"[{task_name}] API response: {api_response}")

                for param in read_params:
                    if param in api_response:
                        context[param] = api_response[param]
                        logger.info(f"[{task_name}] Injected param: {param} = {api_response[param]}")

                logger.info(f"[{task_name}] Successfully injected parameters: {read_params}")
                pprint(f"[{task_name}] Context after injection: {context}")
                return func(**context)

            except requests.exceptions.RequestException as e:
                logger.error(f"[{task_name}] HTTP request failed: {str(e)}")
                raise AirflowException(f"[{task_name}] HTTP request error: {str(e)}")
            except Exception as e:
                logger.exception(f"[{task_name}] Unexpected error in inject: {str(e)}")
                raise AirflowException(f"Unexpected error in {func.__name__}: {str(e)}")

        return wrapper
    return decorator