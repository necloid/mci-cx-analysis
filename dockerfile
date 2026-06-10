FROM apache/airflow:2.9.1-python3.11

USER root

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt