import logging
from time import sleep

from metashape_manager.aws.dns import upsert_dns_record
from metashape_manager.aws.instance import is_instance_ready, get_instance_ip, is_instance_terminated, \
    get_instances_by_tags, create_ec2_instance

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

SERVER_INIT_SCRIPT_TEMPLATE = """
#!/bin/bash
sleep 30
systemctl daemon-reload
install -o root -g root -m 0644 /dev/stdin /etc/default/metashape-server <<EOF
HOST={HOST}
PORT={PORT}
EOF
systemctl enable metashape-server.service
systemctl start metashape-server.service
"""


def create(region_name: str, subnet_id: str,
           ami_id: str, instance_type: str,
           security_group_id: str, key_name: str,
           uniq_id: str, r53_hosted_zone_id: str,
           host: str, port: int, server_fqdn: str
           ):
    init_script = SERVER_INIT_SCRIPT_TEMPLATE

    init_script = init_script.replace("{HOST}", host)
    init_script = init_script.replace("{PORT}", str(port))
    init_script = init_script.strip()

    instance_id = create_ec2_instance(
        region_name=region_name,
        subnet_id=subnet_id,
        ami_id=ami_id,
        instance_type=instance_type,
        security_group_ids=[security_group_id],
        key_name=key_name,
        init_script=init_script,
        tags={
            "Name": f"metashape-{uniq_id}-server",
            "UNIQ_ID": uniq_id,
            "SERVICE": "metashape",
            "SERVICE_TYPE": "server",
            "datadog": "no"
        }
    )

    ip = get_instance_ip(instance_id=instance_id, region_name=region_name, use_private_ip=True)
    upsert_dns_record(r53_hosted_zone_id, server_fqdn, ip, ttl=60)


def get_instance_id(region_name: str, uniq_id: str):
    try:
        return get_instances_by_tags(region_name=region_name, tags={
            "UNIQ_ID": uniq_id,
            "SERVICE": "metashape",
            "SERVICE_TYPE": "server"
        })[0]
    except IndexError:
        return None


def wait_for_ready(region_name: str, uniq_id: str):
    instance_id = get_instance_id(region_name=region_name, uniq_id=uniq_id)
    if instance_id is None:
        logger.warning(f"wait_for_metashape_server_is_ready | No server instance found for uniq_id {uniq_id}")
        return

    while not is_instance_ready(instance_id, region_name):
        logger.info(f"wait_for_metashape_server_is_ready | Waiting for instance {instance_id} to be ready...")
        sleep(5)
    logger.info(f"wait_for_metashape_server_is_ready | Instance {instance_id} is ready")


def wait_for_terminated(region_name: str, uniq_id: str):
    instance_id = get_instance_id(region_name=region_name, uniq_id=uniq_id)
    if instance_id is None:
        logger.warning(f"wait_for_metashape_server_is_terminated | No server instance found for uniq_id {uniq_id}")
        return

    while not is_instance_terminated(instance_id, region_name):
        logger.info(f"wait_for_metashape_server_is_terminated | Waiting for instance {instance_id} to be terminated...")
        sleep(5)
    logger.info(f"wait_for_metashape_server_is_terminated | Instance {instance_id} is terminated")
