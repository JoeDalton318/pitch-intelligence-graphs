from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "membre_a",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="01_world_cup_ingestion",
    default_args=default_args,
    description="Ingestion asynchrone des matchs StatsBomb (Couches Bronze)",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "statsbomb"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingestion_bronze_task",
        bash_command="python /opt/airflow/src/ingestion/fetch_public_data.py",
    )
