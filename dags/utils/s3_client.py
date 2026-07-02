"""Connexions MinIO / DuckDB-S3 réutilisables par les DAGs et les scripts."""
import os
import duckdb
from minio import Minio

# minio:9000 quand on tourne DANS le conteneur Airflow.
# localhost:9000 quand on tourne depuis ta machine (dev local, notebooks).
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")


def get_minio_client() -> Minio:
    """Client MinIO pour uploader/lister des objets."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def duckdb_s3_connection() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB prête à lire/écrire sur MinIO via l'API S3."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con