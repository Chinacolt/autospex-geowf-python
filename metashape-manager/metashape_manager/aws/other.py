import boto3


def get_current_region():
    boto_session = boto3.session.Session()
    return boto_session.region_name
