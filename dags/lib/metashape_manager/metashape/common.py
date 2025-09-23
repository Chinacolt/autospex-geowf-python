import logging

from lib.metashape_manager.aws.other import get_current_region
from lib.metashape_manager.aws.ssm import get_ssm_parameter

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def get_parameters_from_ssm():
    logger.info("Fetching parameters from SSM Parameter Store...")

    region_name = get_current_region()
    logger.info(f"Current region: {region_name}")

    ami_id = get_ssm_parameter(name="/geo-automation/metashape/ami/id", region_name=region_name)
    logger.info(f"Fetched AMI ID: {ami_id}")
    efs_id = get_ssm_parameter(name="/geo-automation/metashape/efs/id", region_name=region_name)
    logger.info(f"Fetched EFS ID: {efs_id}")
    security_group_id = get_ssm_parameter(name="/geo-automation/metashape/security-group/id", region_name=region_name)
    logger.info(f"Fetched Security Group ID: {security_group_id}")
    subnet_id = get_ssm_parameter(name="/geo-automation/metashape/subnet/id", region_name=region_name)
    logger.info(f"Fetched Subnet ID: {subnet_id}")
    key_name = get_ssm_parameter(name="/geo-automation/metashape/key-pair/name", region_name=region_name)
    logger.info(f"Fetched Key Pair Name: {key_name}")
    r53_hosted_zone_id = get_ssm_parameter(name="/geo-automation/metashape/route53/hosted-zone/id",
                                           region_name=region_name)
    logger.info(f"Fetched Route 53 Hosted Zone ID: {r53_hosted_zone_id}")

    return region_name, ami_id, efs_id, security_group_id, subnet_id, key_name, r53_hosted_zone_id
