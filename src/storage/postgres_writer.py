import duckdb
import psycopg2
from psycopg2.extras import execute_values

def load_gold():
    print("1. Lecture du fichier Parquet (Couche Silver) via DuckDB...")
    # DuckDB va nous servir uniquement de lecteur S3 ultra-rapide
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE SECRET minio_secret (
            TYPE S3, KEY_ID 'minio', SECRET 'minio12345',
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false
        );
    """)

    # fetchall() transforme le résultat directement en liste de tuples natifs Python
    records = con.execute("SELECT * FROM read_parquet('s3://silver/passes_agg.parquet')").fetchall()
    print(f"-> {len(records)} lignes lues depuis MinIO.")
    
    print("2. Connexion à la base de données PostgreSQL (Couche Gold)...")
    # Connexion à l'instance Postgres qui tourne dans Docker
    pg_conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="app",
        password="app12345",
        dbname="gold"
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
    
    print("✅ Étape C2 (Couche Gold) terminée avec succès ! Les données sont dans PostgreSQL, prêtes pour Superset.")

if __name__ == "__main__":
    load_gold()
