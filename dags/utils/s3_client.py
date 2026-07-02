"""Connexions MinIO / DuckDB-S3 réutilisables par les DAGs et les scripts."""
from __future__ import annotations

import os
from io import BytesIO
from urllib.parse import urlparse

import duckdb
from minio import Minio


# ---------------------------------------------------------------------------
# Helpers d'endpoint
# ---------------------------------------------------------------------------
def _endpoint_without_scheme(endpoint_url: str) -> tuple[str, bool]:
    """Sépare 'http://minio:9000' en ('minio:9000', False)."""
    parsed = urlparse(endpoint_url)
    if not parsed.scheme:
        return endpoint_url, False
    return parsed.netloc, parsed.scheme == "https"


# ---------------------------------------------------------------------------
# Client Minio (utilisé par le DAG d'ingestion Bronze + le DAG Silver)
# ---------------------------------------------------------------------------
def get_minio_client() -> Minio:
    """Client MinIO pour uploader / lister / lire des objets."""
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    endpoint, secure = _endpoint_without_scheme(endpoint_url)

    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio12345"),
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    """Crée le bucket s'il n'existe pas (idempotent)."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def put_bytes(
    client: Minio,
    bucket_name: str,
    object_name: str,
    payload: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    """Upload d'un blob binaire (JSON, CSV, Parquet...) vers un bucket."""
    client.put_object(
        bucket_name,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type=content_type,
    )


# ---------------------------------------------------------------------------
# Connexion DuckDB-S3 (utilisée par le DAG Silver pour lire/écrire MinIO)
# ---------------------------------------------------------------------------
def duckdb_s3_connection() -> duckdb.DuckDBPyConnection:
    """
    Connexion DuckDB prête à lire/écrire sur MinIO via l'API S3.

    DuckDB attend l'endpoint SANS le schéma (juste 'minio:9000'),
    donc on réutilise le helper _endpoint_without_scheme.
    """
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    endpoint, secure = _endpoint_without_scheme(endpoint_url)

    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio12345")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl={'true' if secure else 'false'};
        SET s3_url_style='path';
    """)
    return con