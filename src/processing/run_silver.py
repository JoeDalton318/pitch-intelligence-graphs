"""
Lit le JSON d'events depuis le bucket 'bronze', filtre les passes réussies,
agrège par paire (passeur -> receveur), écrit le Parquet dans 'silver'.
"""
import json
import sys
from pathlib import Path

# Rendre les modules du projet importables
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))           # pour "from src.processing..."
sys.path.insert(0, str(PROJECT_ROOT / "dags"))  # pour "from utils.s3_client..."

from utils.s3_client import get_minio_client, duckdb_s3_connection
from src.processing.clean_passes import extract_clean_passes
from src.processing.aggregate_networks import aggregate_pass_network, write_silver


BRONZE_BUCKET = "bronze"
BRONZE_KEY = "events/3869685.json"
MATCH_ID = 3869685
SILVER_DEST = "s3://silver/passes_agg.parquet"


def main() -> None:
    print(f"[1/4] Téléchargement de s3://{BRONZE_BUCKET}/{BRONZE_KEY} depuis MinIO...")
    s3 = get_minio_client()
    response = s3.get_object(BRONZE_BUCKET, BRONZE_KEY)
    events = json.loads(response.read())
    response.close()
    response.release_conn()
    print(f"      -> {len(events)} events chargés")

    print("[2/4] Filtrage des passes réussies...")
    clean = extract_clean_passes(events, MATCH_ID)
    print(f"      -> {len(clean)} passes réussies")

    print("[3/4] Agrégation par paire (passeur -> receveur)...")
    agg = aggregate_pass_network(clean)
    print(f"      -> {len(agg)} arêtes uniques (total passes reconstitué : {agg['nb'].sum()})")

    print(f"[4/4] Écriture du Parquet vers {SILVER_DEST}...")
    con = duckdb_s3_connection()
    write_silver(con, agg, SILVER_DEST)
    con.close()
    print("      -> OK")

    print("\nSilver produit. Vérifie dans l'UI MinIO : bucket 'silver' -> passes_agg.parquet")


if __name__ == "__main__":
    main()