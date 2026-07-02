
from typing import List, Dict
import pandas as pd


def aggregate_pass_network(clean_rows: List[Dict]) -> pd.DataFrame:
    """
    Transforme une liste de passes unitaires en réseau agrégé.
    Chaque ligne du résultat = une arête pondérée du graphe : passeur -> receveur (nb passes).

    Format de sortie = CONTRAT D'INTERFACE avec le membre B (Neo4j).
    Ordre des colonnes : passeur, receveur, equipe, match_id, nb
    """
    df = pd.DataFrame(clean_rows)
    agg = (
        df.groupby(["match_id", "equipe", "passeur", "receveur"])
          .size()
          .reset_index(name="nb")
          .sort_values("nb", ascending=False)
    )
    return agg[["passeur", "receveur", "equipe", "match_id", "nb"]]


def write_silver(con, agg_df: pd.DataFrame, dest: str) -> None:
    """
    Écrit le DataFrame en Parquet.
    dest peut être local ('data/silver/passes_agg.parquet')
    ou S3 ('s3://silver/passes_agg.parquet').
    """
    con.register("agg", agg_df)
    con.execute(f"COPY agg TO '{dest}' (FORMAT PARQUET)")