from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "membre_c",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="02_world_cup_processing",
    default_args=default_args,
    description="Traitement automatique déclenché par un Sensor MinIO (Silver -> Gold -> Neo4j)",
    schedule_interval="*/10 * * * *",  # Vérifie toutes les 10 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["processing", "silver", "gold", "neo4j", "sensor"],
) as dag:

    # 1. Le Sensor écoute l'apparition du fichier _SUCCESS dans MinIO
    # Note : Nécessite la connexion 'aws_default' ou la configuration Boto3
    # Mais ici on utilise l'endpoint MinIO
    sensor_bronze = S3KeySensor(
        task_id="wait_for_bronze_success",
        bucket_name="bronze",
        bucket_key="statsbomb/events/_SUCCESS",
        wildcard_match=False,
        aws_conn_id="aws_default",  # A configurer dans Airflow avec les credentials MinIO
        poke_interval=60,
        timeout=60 * 60 * 24,  # Attend jusqu'à 24h
    )

    # 2. Ingestion Métadonnées MongoDB (Membre B)
    load_mongo = BashOperator(
        task_id="chargement_mongo_task",
        env={"MONGO_URI": "mongodb://app:app12345@mongo:27017/?authSource=admin"},
        bash_command="python /opt/airflow/src/storage/mongo_writer.py",
    )

    # 3. Traitement Silver (Membre C)
    process_silver = BashOperator(
        task_id="traitement_silver_task",
        env={"MINIO_ENDPOINT": "minio:9000"},
        bash_command="python /opt/airflow/src/processing/clean_passes.py",
    )

    # 4. Chargement Graphe Neo4j (Membre B)
    load_neo4j = BashOperator(
        task_id="chargement_neo4j_task",
        env={
            "NEO4J_URI": "bolt://neo4j:7687",
            "NEO4J_USER": "neo4j",
            "NEO4J_PASSWORD": "neo4j12345",
        },
        bash_command="python /opt/airflow/src/storage/neo4j_loader.py",
    )

    # 5. Chargement Gold (Membre C)
    load_gold = BashOperator(
        task_id="chargement_gold_task",
        env={"MINIO_ENDPOINT": "minio:9000", "PG_HOST": "postgres"},
        bash_command="python /opt/airflow/src/storage/postgres_writer.py",
    )

    # 6. Suppression du fichier _SUCCESS pour réarmer le Sensor pour la prochaine fois
    delete_success_marker = BashOperator(
        task_id="delete_success_marker",
        bash_command="python -c \"from dags.utils.s3_client import get_minio_client; client = get_minio_client(); client.remove_object('bronze', 'statsbomb/events/_SUCCESS')\"",
    )

    # Ordre d'exécution
    sensor_bronze >> [load_mongo, process_silver]
    process_silver >> [load_neo4j, load_gold]
    [load_mongo, load_neo4j, load_gold] >> delete_success_marker
