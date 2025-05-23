# Requirements
#
# Raw source data folders in AWS S3 must be copied to the Geo data center based on the parameters below.
#
# The root folder must be sepearated in NAS (or Server Hard drive) for staging to avoid risk of overwriting an existing data
#
# /autospex-geo/
#
# Regions must be the same as S3
#
# projects-canada = /autospex-geo/can/
#
# projects-usa = /autospex-geo/usa/
#
# projects-australia = /autospex-geo/aus/
#
# projects-europe = /autospex-geo/eu/
#
# projects-uk = /autospex-geo/uk/
#
# It must only copy the unique data files from new Ingest missions every time a Workflow starts to avoid duplication.
#
# Parameters
#
# Source data path = /projects-{region}/{client-code}/{site-code}/{structure-code}/{survey-code}/raw-ingest/
#
# e.g. /projects-canada/wsa/lafl/splw/2409/raw-ingest/{data_type}/*/
#
# Data types = highresrgb, highlevelrgb, surveycontrol
#
# Destination data path
#
# /autospex-geo/{region}/{client-code}/{site-code}/{structure-code}/{survey-code}/data/{data_type}/
#
# e.g. /autospex-geo/can/wsa/lafl/splw/2409/data/highresrgb/*/
#
# Data Scope
#
# High Level RGB
#
# High Resolution RGB
#
# Survey Control
#
# Triggers
#
# Update workflow status
#
# Update/append workflow params dictionary
#
# Continue with next step

def copy2nas(source_path: str, destination_path: str):
    """
    Copy files from source to destination.
    :param source_path: The source path to copy files from.
    :type source_path: str
    :param destination_path: The destination path to copy files to.
    :type destination_path: str
    """
    import os
    import shutil

    # Check if the source path exists
    if not os.path.exists(source_path):
        print(f"Source path {source_path} does not exist.")
        return

    # Create the destination directory if it doesn't exist
    os.makedirs(destination_path, exist_ok=True)

    # Copy files from source to destination
    for item in os.listdir(source_path):
        s = os.path.join(source_path, item)
        d = os.path.join(destination_path, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, False, None)
        else:
            shutil.copy2(s, d)
