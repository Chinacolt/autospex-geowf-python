ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5
FROM ${AIRFLOW_IMAGE_NAME}

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgomp1 \
    libgl1 \
    libglu1-mesa \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/sdk
COPY Metashape-2.2.1-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl /opt/sdk/

USER airflow
RUN pip install --no-cache-dir /opt/sdk/Metashape-2.2.1-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl
