import logging

import boto3
import botocore

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def get_ssm_parameter(name: str, region_name: str, with_decryption: bool = False) -> str:
    ssm_client = boto3.client("ssm", region_name=region_name)
    try:
        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=with_decryption
        )
        return response["Parameter"]["Value"]
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error retrieving SSM parameter {name}: {e}")
        raise
