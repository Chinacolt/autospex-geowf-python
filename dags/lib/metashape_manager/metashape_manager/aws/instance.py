import logging

import boto3

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def create_ec2_instance(region_name: str, subnet_id: str,
                        ami_id: str, instance_type: str,
                        security_group_ids: list[str], key_name: str,
                        init_script: str,
                        tags: dict[str, str]
                        ):
    logger.info("create_ec2_instance | Creating EC2 instance with the following parameters:")
    logger.info(f"create_ec2_instance | Instance Type: {instance_type}")
    logger.info(f"create_ec2_instance | AMI ID: {ami_id}")
    logger.info(f"create_ec2_instance | Key Name: {key_name}")
    logger.info(f"create_ec2_instance | Security Group IDs: {security_group_ids}")
    logger.info(f"create_ec2_instance | Subnet ID: {subnet_id}")
    logger.info(f"create_ec2_instance | Region Name: {region_name}")

    ec2 = boto3.resource("ec2", region_name=region_name)
    instances = ec2.create_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=security_group_ids,
        SubnetId=subnet_id,
        UserData=init_script,
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {
                        "Key": k,
                        "Value": v
                    } for k, v in tags.items()
                ]
            },
        ]
    )
    instance_id = instances[0].id
    logger.info(f"create_ec2_instance | Created instance {instance_id}")
    return instance_id


def destroy_ec2_instance(instance_id: str, region_name: str):
    logger.info(f"destroy_ec2_instance | Terminating instance {instance_id}...")
    ec2 = boto3.client("ec2", region_name=region_name)
    ec2.terminate_instances(InstanceIds=[instance_id])
    logger.info(f"destroy_ec2_instance | Termination command sent for instance {instance_id}.")


def get_instance_ip(instance_id: str, region_name: str, use_private_ip: bool = False) -> str:
    logger.info(f"get_instance_ip | Fetching IP for instance {instance_id}...")
    ec2 = boto3.client("ec2", region_name=region_name)
    reservations = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"]
    if not reservations or not reservations[0]["Instances"]:
        raise RuntimeError(f"Could not find instance {instance_id} in {region_name}")
    inst = reservations[0]["Instances"][0]
    if use_private_ip:
        ip = inst.get("PrivateIpAddress")
    else:
        ip = inst.get("PublicIpAddress") or inst.get("PrivateIpAddress")
    if not ip:
        raise RuntimeError(f"Instance {instance_id} has no IP address yet (public/private).")

    logger.info(f"get_instance_ip | Instance {instance_id} has IP: {ip}")

    return ip


def is_instance_ready(instance_id: str, region_name: str):
    status = get_ec2_instance_status(instance_id, region_name)
    logger.info(f"is_instance_ready | Instance {instance_id} status: {status}")
    return status == "ok"


def is_instance_terminated(instance_id: str, region_name: str):
    status = get_ec2_instance_status(instance_id, region_name)
    logger.info(f"is_instance_terminated | Instance {instance_id} status: {status}")
    return status in ["terminated", "not-found"]


def get_ec2_instance_status(instance_id: str, region_name: str):
    ec2 = boto3.client("ec2", region_name=region_name)
    try:
        response = ec2.describe_instance_status(InstanceIds=[instance_id])
    except Exception as e:
        logger.error(f"get_ec2_instance_status | Error describing instance status: {e}")
        return "not-found"

    instance_statuses = response["InstanceStatuses"]

    if len(instance_statuses) == 0:
        reservations = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"]
        return reservations[0]["Instances"][0]["State"]["Name"]

    system_status = response["InstanceStatuses"][0]["SystemStatus"]["Status"]

    return system_status


def get_instances_by_tags(region_name: str, tags: dict[str, str]) -> list[str]:
    ec2 = boto3.client("ec2", region_name=region_name)
    filters = [
        {
            "Name": f"tag:{k}",
            "Values": [v]
        } for k, v in tags.items()
    ]

    filters.append({
        "Name": "instance-state-name",
        "Values": ["pending", "running", "stopping", "stopped"]
    })

    response = ec2.describe_instances(Filters=filters)

    instances = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            instances.append(instance["InstanceId"])

    return instances
