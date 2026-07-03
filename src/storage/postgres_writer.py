"""
Postgres Writer — Neo4j -> Postgres Gold.

Rôle : lit les scores de centralité (PageRank + Betweenness) écrits par
GDS sur les noeuds Joueur dans Neo4j, et alimente les 5 tables du schéma
étoile Gold (dim_joueur, dim_equipe, dim_match, fact_centralite, fact_style).

Usage :
    python src/storage/postgres_writer.py

Variables d'environnement (avec valeurs par défaut pour dev local) :
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    MATCH_ID, MATCH_COMPETITION, MATCH_SAISON, MATCH_DATE, MATCH_EQUIPE_DOM, MATCH_EQUIPE_EXT
"""
from __future__ import annotations

import logging
import os
import statistics
from typing import Any

import psycopg2
from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j12345")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "gold")
PG_USER = os.getenv("POSTGRES_USER", "app")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "app12345")

# Métadonnées du match : à passer en env quand on aura plusieurs matchs.
# Pour maintenant on code en dur le match Qatar-Équateur (ouverture CDM 2022).
MATCH_ID = int(os.getenv("MATCH_ID", "3857286"))
MATCH_COMPETITION = os.getenv("MATCH_COMPETITION", "FIFA World Cup")
MATCH_SAISON = os.getenv("MATCH_SAISON", "2022")
MATCH_DATE = os.getenv("MATCH_DATE", "2022-11-20")
MATCH_EQUIPE_DOM = os.getenv("MATCH_EQUIPE_DOM", "Qatar")
MATCH_EQUIPE_EXT = os.getenv("MATCH_EQUIPE_EXT", "Ecuador")


# ---------------------------------------------------------------------------
# Étape 1 — Lire les scores depuis Neo4j
# ---------------------------------------------------------------------------
def fetch_scores_from_neo4j() -> list[dict[str, Any]]:
    """
    Récupère les scores GDS écrits sur les noeuds Joueur.
    Retourne une liste de dicts : {joueur, equipe, pagerank, betweenness}.
    """
    query = """
    MATCH (j:Joueur)
    WHERE j.pagerank_score IS NOT NULL
    RETURN
        j.nom               AS joueur,
        j.equipe            AS equipe,
        j.pagerank_score    AS pagerank,
        j.betweenness_score AS betweenness
    ORDER BY j.pagerank_score DESC
    """
    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        with driver.session() as session:
            result = session.run(query)
            rows = [dict(record) for record in result]
    log.info("Neo4j : %d joueurs avec scores GDS récupérés", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Étape 2 — Alimenter les dimensions et faits Postgres
# ---------------------------------------------------------------------------
def upsert_dim_match(cur) -> None:
    """Insère/met à jour le match courant dans dim_match."""
    cur.execute(
        """
        INSERT INTO gold.dim_match
            (match_id, competition, saison, date_match, equipe_dom, equipe_ext)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE SET
            competition = EXCLUDED.competition,
            saison      = EXCLUDED.saison,
            date_match  = EXCLUDED.date_match,
            equipe_dom  = EXCLUDED.equipe_dom,
            equipe_ext  = EXCLUDED.equipe_ext
        """,
        (MATCH_ID, MATCH_COMPETITION, MATCH_SAISON, MATCH_DATE,
         MATCH_EQUIPE_DOM, MATCH_EQUIPE_EXT),
    )


def upsert_dim_equipe(cur, nom: str) -> int:
    """Insère l'équipe si nouvelle, retourne son equipe_id."""
    cur.execute(
        """
        INSERT INTO gold.dim_equipe (nom)
        VALUES (%s)
        ON CONFLICT (nom) DO UPDATE SET nom = EXCLUDED.nom
        RETURNING equipe_id
        """,
        (nom,),
    )
    return cur.fetchone()[0]


def upsert_dim_joueur(cur, nom: str) -> int:
    """Insère le joueur s'il est nouveau, retourne son joueur_id."""
    cur.execute(
        """
        INSERT INTO gold.dim_joueur (nom)
        VALUES (%s)
        ON CONFLICT (nom) DO UPDATE SET nom = EXCLUDED.nom
        RETURNING joueur_id
        """,
        (nom,),
    )
    return cur.fetchone()[0]


def upsert_fact_centralite(cur, match_id: int, joueur_id: int,
                           equipe_id: int, pagerank: float,
                           betweenness: float) -> None:
    """Insère un score de centralité pour un joueur d'un match."""
    cur.execute(
        """
        INSERT INTO gold.fact_centralite
            (match_id, joueur_id, equipe_id, pagerank, betweenness)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (match_id, joueur_id) DO UPDATE SET
            equipe_id   = EXCLUDED.equipe_id,
            pagerank    = EXCLUDED.pagerank,
            betweenness = EXCLUDED.betweenness,
            computed_at = CURRENT_TIMESTAMP
        """,
        (match_id, joueur_id, equipe_id, pagerank, betweenness),
    )


def compute_and_upsert_fact_style(cur, match_id: int,
                                  equipes_scores: dict[int, list[float]]) -> None:
    """
    Calcule et écrit l'indice de style par équipe :
    - indice_centralisation = écart-type des PageRank (élevé = jeu centralisé)
    - pagerank_max          = score du joueur pivot
    - pagerank_moyen        = score moyen de l'équipe
    - nb_joueurs            = nombre de joueurs impliqués
    """
    for equipe_id, prs in equipes_scores.items():
        if len(prs) < 2:
            # stdev nécessite au moins 2 échantillons
            indice = 0.0
        else:
            indice = statistics.stdev(prs)
        cur.execute(
            """
            INSERT INTO gold.fact_style
                (match_id, equipe_id, indice_centralisation,
                 pagerank_max, pagerank_moyen, nb_joueurs)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id, equipe_id) DO UPDATE SET
                indice_centralisation = EXCLUDED.indice_centralisation,
                pagerank_max          = EXCLUDED.pagerank_max,
                pagerank_moyen        = EXCLUDED.pagerank_moyen,
                nb_joueurs            = EXCLUDED.nb_joueurs,
                computed_at           = CURRENT_TIMESTAMP
            """,
            (match_id, equipe_id, indice, max(prs),
             sum(prs) / len(prs), len(prs)),
        )


# ---------------------------------------------------------------------------
# Étape 3 — Orchestration
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Postgres Writer : Neo4j -> Gold ===")

    # 1. Lire les scores depuis Neo4j
    scores = fetch_scores_from_neo4j()
    if not scores:
        log.warning("Aucun score GDS trouvé dans Neo4j. Abandon.")
        return

    # 2. Se connecter à Postgres et remplir en transaction
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            # 2a. dim_match
            upsert_dim_match(cur)
            log.info("dim_match : match_id=%s inséré/mis à jour", MATCH_ID)

            # 2b. dim_equipe + dim_joueur + fact_centralite
            equipes_scores: dict[int, list[float]] = {}  # equipe_id -> [pageranks]
            for row in scores:
                equipe_id = upsert_dim_equipe(cur, row["equipe"])
                joueur_id = upsert_dim_joueur(cur, row["joueur"])
                upsert_fact_centralite(
                    cur, MATCH_ID, joueur_id, equipe_id,
                    float(row["pagerank"]), float(row["betweenness"]),
                )
                equipes_scores.setdefault(equipe_id, []).append(float(row["pagerank"]))
            log.info("dim_equipe + dim_joueur + fact_centralite : %d joueurs traités",
                     len(scores))

            # 2c. fact_style (calcul agrégé par équipe)
            compute_and_upsert_fact_style(cur, MATCH_ID, equipes_scores)
            log.info("fact_style : %d équipes traitées", len(equipes_scores))

        conn.commit()
        log.info("=== Commit OK ===")
    except Exception as e:
        conn.rollback()
        log.error("Erreur, rollback effectué : %s", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
