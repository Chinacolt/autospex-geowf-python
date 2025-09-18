import logging

from metashape_manager.aws.dns import get_hosted_zone_domain_name
from metashape_manager.aws.instance import destroy_ec2_instance
from metashape_manager.aws.other import get_current_region
from metashape_manager.metashape import server, worker
from metashape_manager.metashape.common import get_parameters_from_ssm

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 80
DEFAULT_MOUNT_PATH = "/data"
DEFAULT_DATA_PATH_TEMPLATE = "/data/geowf/metashape/{uniq_id}"
DEFAULT_SERVER_FQDN_TEMPLATE = "metashape-{uniq_id}-server.{r53_hostname}"


def create_cluster(uniq_id: str, server_instance_type: str, worker_instance_type: str, worker_count: int):
    region_name, ami_id, efs_id, security_group_id, subnet_id, key_name, r53_hosted_zone_id = get_parameters_from_ssm()
    r53_hostname = get_hosted_zone_domain_name(r53_hosted_zone_id=r53_hosted_zone_id)
    server_fqdn = DEFAULT_SERVER_FQDN_TEMPLATE.format(uniq_id=uniq_id, r53_hostname=r53_hostname)

    if server.get_instance_id(region_name=region_name, uniq_id=uniq_id) is None:
        server.create(
            region_name=region_name,
            subnet_id=subnet_id,
            ami_id=ami_id,
            instance_type=server_instance_type,
            security_group_id=security_group_id,
            key_name=key_name,
            uniq_id=uniq_id,
            r53_hosted_zone_id=r53_hosted_zone_id,
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            server_fqdn=server_fqdn
        )

    current_worker_count = len(worker.get_instance_ids(region_name=region_name, uniq_id=uniq_id))
    required_worker_count = worker_count - current_worker_count
    worker_data_path = DEFAULT_DATA_PATH_TEMPLATE.format(uniq_id=uniq_id)

    if required_worker_count < 0:
        required_worker_count = 0

    for i in range(required_worker_count):
        worker.create(
            region_name=region_name,
            subnet_id=subnet_id,
            ami_id=ami_id,
            instance_type=worker_instance_type,
            security_group_id=security_group_id,
            key_name=key_name,
            uniq_id=uniq_id,
            host=server_fqdn,
            port=DEFAULT_PORT,
            efs_id=efs_id,
            mount_path=DEFAULT_MOUNT_PATH,
            data_path=worker_data_path
        )

    server.wait_for_ready(region_name=region_name, uniq_id=uniq_id)
    worker.wait_for_ready(region_name=region_name, uniq_id=uniq_id)

    return server_fqdn, worker_data_path


def destroy_cluster(uniq_id: str):
    region_name = get_current_region()

    server_instance_id = server.get_instance_id(region_name=region_name, uniq_id=uniq_id)
    worker_instance_ids = worker.get_instance_ids(region_name=region_name, uniq_id=uniq_id)

    if server_instance_id is not None:
        destroy_ec2_instance(instance_id=server_instance_id, region_name=region_name)

    for instance_id in worker_instance_ids:
        destroy_ec2_instance(instance_id=instance_id, region_name=region_name)

    server.wait_for_terminated(region_name=region_name, uniq_id=uniq_id)
    worker.wait_for_terminated(region_name=region_name, uniq_id=uniq_id)
