import logging
from time import sleep

from metashape_manager.aws.instance import is_instance_ready, is_instance_terminated, \
    get_instances_by_tags, create_ec2_instance

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

WORKER_INIT_SCRIPT_TEMPLATE = """
#!/bin/bash
sleep 30
/srv/mount_efs.sh {EFS_ID} {REGION_NAME} {MOUNT_PATH}
mkdir -p {DATA_PATH} || true
systemctl daemon-reload
install -o root -g root -m 0644 /dev/stdin /etc/default/metashape-worker <<EOF
HOST={HOST}
PORT={PORT}
DATA_PATH={DATA_PATH}
EOF
systemctl enable metashape-worker.service
systemctl start metashape-worker.service
"""


def create(region_name: str, subnet_id: str,
           ami_id: str, instance_type: str,
           security_group_id: str, key_name: str,
           uniq_id: str,
           host: str, port: int,
           efs_id: str, mount_path: str, data_path: str
           ):
    init_script = WORKER_INIT_SCRIPT_TEMPLATE

    init_script = init_script.replace("{REGION_NAME}", region_name)
    init_script = init_script.replace("{EFS_ID}", efs_id)
    init_script = init_script.replace("{HOST}", host)
    init_script = init_script.replace("{PORT}", str(port))
    init_script = init_script.replace("{MOUNT_PATH}", mount_path)
    init_script = init_script.replace("{DATA_PATH}", data_path)
    init_script = init_script.strip()

    create_ec2_instance(
        region_name=region_name,
        subnet_id=subnet_id,
        ami_id=ami_id,
        instance_type=instance_type,
        security_group_ids=[security_group_id],
        key_name=key_name,
        init_script=init_script,
        tags={
            "Name": f"metashape-{uniq_id}-worker",
            "UNIQ_ID": uniq_id,
            "SERVICE": "metashape",
            "SERVICE_TYPE": "worker",
            "datadog": "no"
        }
    )


def get_instance_ids(region_name: str, uniq_id: str):
    return get_instances_by_tags(region_name=region_name, tags={
        "UNIQ_ID": uniq_id,
        "SERVICE": "metashape",
        "SERVICE_TYPE": "worker"
    })


def wait_for_ready(region_name: str, uniq_id: str):
    instance_ids = get_instance_ids(region_name=region_name, uniq_id=uniq_id)
    if len(instance_ids) == 0:
        logger.warning(f"wait_for_metashape_workers_are_ready | No worker instances found for uniq_id {uniq_id}")
        return

    for instance_id in instance_ids:
        while not is_instance_ready(instance_id, region_name):
            logger.info(f"wait_for_metashape_workers_are_ready | Waiting for instance {instance_id} to be ready...")
            sleep(5)
        logger.info(f"wait_for_metashape_workers_are_ready | Instance {instance_id} is ready")


def wait_for_terminated(region_name: str, uniq_id: str):
    instance_ids = get_instance_ids(region_name=region_name, uniq_id=uniq_id)
    if len(instance_ids) == 0:
        logger.warning(f"wait_for_metashape_workers_are_terminated | No worker instances found for uniq_id {uniq_id}")
        return

    for instance_id in instance_ids:
        while not is_instance_terminated(instance_id, region_name):
            logger.info(
                f"wait_for_metashape_workers_are_terminated | Waiting for instance {instance_id} to be terminated...")
            sleep(5)
        logger.info(f"wait_for_metashape_workers_are_terminated | Instance {instance_id} is terminated")
