"""Agrégation : passes unitaires -> arêtes pondérées du réseau (nb passes par paire)."""
from typing import List, Dict
import pandas as pd


def aggregate_pass_network(clean_rows: List[Dict]) -> pd.DataFrame:
    """
    Transforme une liste de passes unitaires en réseau agrégé.
    Chaque ligne = une arête pondérée du graphe : passeur -> receveur (nb passes).

    Format de sortie = CONTRAT D'INTERFACE avec le membre B (Neo4j).
    Colonnes finales attendues par ses scripts Cypher :
      passeur_nom, receveur_nom, equipe, match_id, nombre_passes
    """
    df = pd.DataFrame(clean_rows)
    agg = (
        df.groupby(["match_id", "equipe", "passeur", "receveur"])
          .size()
          .reset_index(name="nb")
          .sort_values("nb", ascending=False)
    )
    # Renommage pour matcher exactement les scripts Cypher de Gills
    agg = agg.rename(columns={
        "passeur": "passeur_nom",
        "receveur": "receveur_nom",
        "nb": "nombre_passes",
    })
    return agg[["passeur_nom", "receveur_nom", "equipe", "match_id", "nombre_passes"]]


def write_silver_csv(con, agg_df: pd.DataFrame, dest: str) -> None:
    """
    Écrit le DataFrame en CSV avec header (usage Neo4j LOAD CSV).
    dest peut être local ('data/silver/passes_agg.csv')
    ou S3 ('s3://silver/passes_agg.csv').
    """
    con.register("agg_csv", agg_df)
    con.execute(f"COPY agg_csv TO '{dest}' (FORMAT CSV, HEADER)")