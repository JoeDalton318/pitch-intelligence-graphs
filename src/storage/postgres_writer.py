import duckdb
import psycopg2
from psycopg2.extras import execute_values
import os


def load_gold():
    print("1. Lecture du fichier Parquet (Couche Silver) via DuckDB...")

    # On utilise des variables d'environnement pour s'adapter (Local ou Docker)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    pg_host = os.getenv("PG_HOST", "127.0.0.1")

    # DuckDB va nous servir uniquement de lecteur S3 ultra-rapide
    con = duckdb.connect(database=":memory:")
    con.execute("SET home_directory='/tmp';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE S3, KEY_ID 'minio', SECRET 'minio12345',
            ENDPOINT '{minio_endpoint}', URL_STYLE 'path', USE_SSL false
        );
    """)

    # fetchall() transforme le résultat directement en liste de tuples natifs Python
    records = con.execute(
        "SELECT * FROM read_parquet('s3://silver/passes_agg.parquet')"
    ).fetchall()
    print(f"-> {len(records)} lignes lues depuis MinIO.")

    print(
        f"2. Connexion à la base de données PostgreSQL (Couche Gold) sur {pg_host}..."
    )
    # Connexion à l'instance Postgres qui tourne dans Docker
    pg_conn = psycopg2.connect(
        host=pg_host, port=5432, user="app", password="app12345", dbname="gold"
    )
    cursor = pg_conn.cursor()

    print("3. Création de la table 'fact_passes' (si inexistante)...")
    # La clé primaire composite garantit l'idempotence (pas de doublons si on relance)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_passes (
            match_id VARCHAR,
            passeur_nom VARCHAR,
            receveur_nom VARCHAR,
            equipe VARCHAR,
            nombre_passes INTEGER,
            PRIMARY KEY (match_id, passeur_nom, receveur_nom)
        );
    """)

    print("4. Upsert (INSERT ON CONFLICT) des données...")
    upsert_query = """
        INSERT INTO fact_passes (match_id, passeur_nom, receveur_nom, equipe, nombre_passes)
        VALUES %s
        ON CONFLICT (match_id, passeur_nom, receveur_nom) 
        DO UPDATE SET 
            equipe = EXCLUDED.equipe,
            nombre_passes = EXCLUDED.nombre_passes;
    """

    # execute_values est une fonction optimisée de psycopg2 pour faire des insertions en masse
    execute_values(cursor, upsert_query, records)
    pg_conn.commit()

    cursor.close()
    pg_conn.close()

    print(
        "Etape C2 (Couche Gold) terminee avec succes ! Les donnees sont dans PostgreSQL, pretes pour Superset."
    )


if __name__ == "__main__":
    load_gold()
