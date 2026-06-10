from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'customer_experience_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    description='Customer Experience Ingestion & Analysis Pipeline'
) as dag:

    ingest_download_archive = BashOperator(
        task_id='01_ingest_download_archive',
        bash_command='python /opt/airflow/dags/scripts/01_ingest_download_archive.py'
    )

    ingest_extract_lake = BashOperator(
        task_id='02_ingest_extract_lake',
        bash_command='python /opt/airflow/dags/scripts/02_ingest_extract_lake.py'
    )

    load_to_staging = BashOperator(
        task_id='03_load_to_staging',
        bash_command='python /opt/airflow/dags/scripts/03_load_to_staging.py'
    )
    
    transform_to_core = BashOperator(
        task_id='04_transform_to_core',
        bash_command='python /opt/airflow/dags/scripts/04_transform_to_core.py'
    )

    aggregate_to_analytics = BashOperator(
        task_id='05_aggregate_to_analytics',
        bash_command='python /opt/airflow/dags/scripts/05_aggregate_to_analytics.py'
    )

    ingest_download_archive >> ingest_extract_lake >> load_to_staging >> transform_to_core >> aggregate_to_analytics