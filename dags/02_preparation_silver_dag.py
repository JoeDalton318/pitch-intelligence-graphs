"""
DAG 02 — Préparation Silver

Orchestration : lit les events JSON depuis s3://bronze/, filtre les passes
réussies, agrège par paire (passeur_nom -> receveur_nom), écrit le CSV
dans s3://silver/passes_agg.csv (format aligné sur les scripts Cypher du membre B).

C'est le pendant Airflow du script src/processing/run_silver.py. La logique métier
reste dans src/processing/ ; le DAG ne fait qu'orchestrer et logger.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import dag, task

# Imports depuis le code du projet (monté dans /opt/airflow/src_project/src)
from processing.clean_passes import extract_clean_passes
from processing.aggregate_networks import (
    aggregate_pass_network,
    write_silver_csv,
)
from utils.s3_client import get_minio_client, duckdb_s3_connection


BRONZE_BUCKET = "bronze"
BRONZE_KEY = "statsbomb/events/match_id=3857286.json"
MATCH_ID = 3857286
SILVER_CSV_DEST = "s3://silver/passes_agg.csv"

log = logging.getLogger(__name__)


@dag(
    dag_id="02_preparation_silver",
    description="Bronze (events JSON) -> Silver (passes agrégées CSV) via DuckDB + MinIO",
    schedule=None,                       # déclenchement manuel depuis l'UI Airflow
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["projet6", "silver", "membre-c"],
)
def preparation_silver():

    @task
    def read_bronze() -> list:
        """Télécharge le JSON d'events depuis MinIO/bronze."""
        s3 = get_minio_client()
        response = s3.get_object(BRONZE_BUCKET, BRONZE_KEY)
        try:
            events = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        log.info("Bronze lu : %d events depuis s3://%s/%s",
                 len(events), BRONZE_BUCKET, BRONZE_KEY)
        return events

    @task
    def clean(events: list) -> list:
        """Filtre les passes réussies (StatsBomb : pas de pass.outcome)."""
        clean_rows = extract_clean_passes(events, MATCH_ID)
        log.info("Nettoyage : %d passes réussies extraites", len(clean_rows))
        return clean_rows

    @task
    def aggregate(clean_rows: list) -> list:
        """Agrège par paire (passeur_nom, receveur_nom) et écrit le CSV silver."""
        agg = aggregate_pass_network(clean_rows)
        total = int(agg["nombre_passes"].sum())
        log.info("Agrégation : %d arêtes uniques (total reconstitué : %d)",
                 len(agg), total)

        con = duckdb_s3_connection()
        try:
            write_silver_csv(con, agg, SILVER_CSV_DEST)
        finally:
            con.close()
        log.info("Silver CSV écrit dans %s", SILVER_CSV_DEST)
        # On retourne un petit résumé pour la traçabilité dans les logs Airflow
        return {"aretes": len(agg), "passes_total": total, "dest": SILVER_CSV_DEST}

    # Enchaînement des tâches
    events = read_bronze()
    clean_rows = clean(events)
    aggregate(clean_rows)


preparation_silver()