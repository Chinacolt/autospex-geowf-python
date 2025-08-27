import logging

import boto3

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def get_hosted_zone_domain_name(r53_hosted_zone_id: str) -> str:
    logger.info(f"get_hosted_zone_domain_name | Getting Route53 hosted zone domain name for {r53_hosted_zone_id}...")
    r53 = boto3.client("route53")
    response = r53.get_hosted_zone(Id=r53_hosted_zone_id)
    domain_name = response["HostedZone"]["Name"]
    if domain_name.endswith("."):
        domain_name = domain_name[:-1]
    logger.info(f"get_hosted_zone_domain_name | Route53 hosted zone domain name is {domain_name}")
    return domain_name


def upsert_dns_record(hosted_zone_id: str, record_name: str, ip_addr: str, ttl: int = 60):
    logger.info(f"upsert_dns_record | Updating Route53 A record for {record_name} to point to {ip_addr}...")

    r53 = boto3.client("route53")
    change_batch = {
        "Comment": "Managed by niricson_geo_automation_infra.py",
        "Changes": [
            {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": record_name,
                    "Type": "A",
                    "TTL": ttl,
                    "ResourceRecords": [{"Value": ip_addr}],
                }
            }
        ]
    }
    resp = r53.change_resource_record_sets(HostedZoneId=hosted_zone_id, ChangeBatch=change_batch)
    change_id = resp["ChangeInfo"]["Id"]

    logging.info(f"upsert_dns_record | Route53 change submitted: {change_id} for {record_name} -> {ip_addr}")
