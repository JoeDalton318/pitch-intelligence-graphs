from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Définition des paramètres par défaut pour garantir la robustesse
default_args = {
    'owner': 'membre_a',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Création du DAG d'orchestration
with DAG(
    dag_id='world_cup_pipeline',
    default_args=default_args,
    description='Orchestration globale du pipeline (Bronze -> Silver -> Gold)',
    schedule_interval=None,  # Déclenchement manuel uniquement
    start_date=datetime(2023, 1, 1),
    catchup=False,           # Pas de rattrapage des dates passées
    tags=['orchestration', 'pipeline_complet'],
) as dag:

    # Task 1 : Ingestion Bronze (Membre A)
    # Collecte les données de l'API StatsBomb vers le datalake MinIO
    ingest_bronze = BashOperator(
        task_id='ingestion_bronze_task',
        bash_command='python /opt/airflow/src/ingestion/fetch_public_data.py',
    )

    # Task 2 : Traitement Silver (Membre C)
    # Nettoyage et agrégation des passes (DuckDB/Polars)
    process_silver = BashOperator(
        task_id='traitement_silver_task',
        bash_command='python /opt/airflow/src/processing/clean_passes.py',
    )

    # Task 3 : Chargement Gold (Membre C)
    # Intégration des données analytiques vers PostgreSQL pour la BI
    load_gold = BashOperator(
        task_id='chargement_gold_task',
        bash_command='python /opt/airflow/src/storage/postgres_writer.py',
    )

    # Définition stricte de l'ordre d'exécution (dépendances)
    ingest_bronze >> process_silver >> load_gold
