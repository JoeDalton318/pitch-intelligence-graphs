"""
DAG 05 — Analytics Gold

Orchestration : lit les scores GDS depuis Neo4j (pagerank_score,
betweenness_score sur les noeuds Joueur), remplit les 5 tables du
schéma étoile Gold dans PostgreSQL (dim_joueur, dim_equipe, dim_match,
fact_centralite, fact_style).

C'est le pendant Airflow du script src/storage/postgres_writer.py.
La logique métier reste dans src/storage/ ; le DAG ne fait qu'orchestrer.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

# Import depuis le code du projet (monté dans /opt/airflow/src)
from storage.postgres_writer import main as run_postgres_writer


log = logging.getLogger(__name__)


@dag(
    dag_id="05_analytics_gold",
    description="Neo4j (scores GDS) -> PostgreSQL Gold (schéma étoile Kimball)",
    schedule=None,                       # déclenchement manuel
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["projet6", "gold", "membre-c"],
)
def analytics_gold():

    @task
    def load_scores_to_gold() -> str:
        """
        Exécute le postgres_writer :
        1. Se connecte à Neo4j.
        2. Lit les scores PageRank + Betweenness sur les noeuds Joueur.
        3. Alimente les 5 tables Gold via UPSERT (idempotent).
        """
        log.info("Démarrage du chargement Neo4j -> Gold")
        run_postgres_writer()
        log.info("Chargement terminé avec succès")
        return "OK"

    load_scores_to_gold()


analytics_gold()