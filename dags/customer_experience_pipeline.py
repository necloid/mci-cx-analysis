from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'customer_experience_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    description='End-to-End Customer Experience Ingestion & Analysis Pipeline'
) as dag:

    download_raw_archive = BashOperator(
        task_id='download_raw_archive',
        bash_command='python /opt/airflow/dags/scripts/download_archive.py'
    )

    download_raw_archive
