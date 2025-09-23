FROM apache/airflow:2.10.4

USER root
RUN apt update \
    && apt -y install gettext-base libssl-dev wget \
    && apt -y install python3 python3-pip python3-dev python3-venv \
    && apt -y install libcurl4 libgl1 libglu1 \
    && apt clean \
    && apt install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN wget https://download.agisoft.com/Metashape-2.1.3-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl \
    && python3 -m pip install ./Metashape-2.1.3-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl \
    && rm ./Metashape-2.1.3-cp37.cp38.cp39.cp310.cp311-abi3-linux_x86_64.whl

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
