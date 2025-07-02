import logging
from common.config import get_variable, get_keycloak_token, create_session

def check_pending_workflow():
    try:
        base_url = get_variable("WORKFLOW_API_URL_TEMPLATE")
        api_url = f"{base_url}/pending"

        logging.info(f"{api_url}")
        
        token = get_keycloak_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        session = create_session()
        response = session.get(
            api_url,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            workflow_id = response.json()
            logging.info(f"Pending workflow found: {workflow_id}")
            return workflow_id
        elif response.status_code == 204:
            logging.info("No pending workflow found")
            return None
        else:
            logging.error(f"API request failed with status code: {response.status_code}")
            return None
            
    except Exception as e:
        logging.error(f"Error checking pending workflow: {str(e)}")
        return None
    