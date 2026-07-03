import duckdb


def process_silver():
    print("Connexion à DuckDB et configuration de MinIO...")
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute("""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID 'minio',
            SECRET 'minio12345',
            ENDPOINT 'localhost:9000',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)

    print("Lecture et agrégation des données brutes depuis MinIO (Bucket Bronze)...")
    query = """
    CREATE OR REPLACE TABLE passes_silver AS
    SELECT 
        replace(list_extract(string_split(filename, '/'), -1), '.json', '') AS match_id,
        player.name AS passeur_nom,
        pass.recipient.name AS receveur_nom,
        team.name AS equipe,
        COUNT(*) AS nombre_passes
    FROM read_json_auto('s3://bronze/statsbomb/events/*.json', filename=true, ignore_errors=true)
    WHERE type.name = 'Pass' 
      AND pass.outcome IS NULL
    GROUP BY 
        match_id, passeur_nom, receveur_nom, equipe
    """

    try:
        con.execute(query)
    except Exception as e:
        print(f"Erreur avec JSON, tentative de lecture au format Parquet : {e}")
        query_parquet = query.replace(
            "read_json_auto('s3://bronze/statsbomb/events/*.json', filename=true, ignore_errors=true)",
            "read_parquet('s3://bronze/statsbomb/events/*.parquet', filename=true)",
        ).replace("'.json'", "'.parquet'")
        con.execute(query_parquet)

    print("Sauvegarde au format Parquet dans la couche Silver (MinIO)...")
    con.execute(
        "COPY passes_silver TO 's3://silver/passes_agg.parquet' (FORMAT PARQUET);"
    )

    print("Sauvegarde au format CSV à la racine du projet (pour Neo4j)...")
    con.execute("COPY passes_silver TO 'passes_agg.csv' (HEADER, DELIMITER ',');")

    print("✅ Étape C1 (Couche Silver) terminée avec succès !")


if __name__ == "__main__":
    process_silver()
