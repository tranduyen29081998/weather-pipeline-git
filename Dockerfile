FROM apache/airflow:2.6.0
USER root
RUN mkdir -p /opt/airflow/data /opt/airflow/config /opt/airflow/dags
RUN chmod -R 777 /opt/airflow/data /opt/airflow/config /opt/airflow/dags || true
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY config/service_account.json /opt/airflow/config/service_account.json