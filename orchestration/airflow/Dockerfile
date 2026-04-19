FROM apache/airflow:2.10.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY orchestration/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt