FROM autospex-airflow-common

USER root

RUN mkdir -p /var/tmp/agisoft/licensing/licenses/
COPY license.lic /var/tmp/agisoft/licensing/licenses/license.lic
RUN chmod -R 777 /var/tmp/agisoft/licensing/licenses/
