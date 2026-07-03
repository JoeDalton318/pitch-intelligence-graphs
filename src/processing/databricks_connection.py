"""
databricks_connection.py
------------------------
Utilitaires de connexion MinIO pour Databricks Free Edition (Serverless).

Databricks Free Edition est un environnement 100% serverless :
- Pas de clusters Spark classiques → pas de fs.s3a.*
- Les notebooks Python tournent sur du compute serverless
- boto3 + pandas sont disponibles nativement

Ce module fournit :
  - get_minio_client()   → client boto3 pour lire/écrire dans MinIO
  - read_json_from_minio()  → lit un fichier JSON depuis un bucket MinIO
  - list_bucket()           → liste les objets d'un bucket
"""

import os
import io
import json
import boto3
from botocore.client import Config
from typing import Optional


def get_minio_client(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
) -> boto3.client:
    """
    Crée et retourne un client boto3 configuré pour MinIO.

    Les paramètres sont lus dans cet ordre de priorité :
      1. Arguments explicites passés à la fonction
      2. Variables d'environnement (MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
      3. Valeurs par défaut (localhost:9000 pour le développement local)

    Args:
        endpoint   : URL publique de MinIO exposée via ngrok
                     Exemple : "https://abc123.ngrok-free.app"
        access_key : Identifiant MinIO
        secret_key : Mot de passe MinIO

    Returns:
        Un client boto3 prêt à l'emploi.
    """
    endpoint   = endpoint   or os.getenv("MINIO_ENDPOINT_URL",  "http://localhost:9000")
    access_key = access_key or os.getenv("MINIO_ACCESS_KEY",    "minio")
    secret_key = secret_key or os.getenv("MINIO_SECRET_KEY",    "minio12345")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO ignore la région, mais boto3 en requiert une
    )

    print(f"[MinIO] Client boto3 configuré → {endpoint}")
    return client


def list_bucket(client: boto3.client, bucket: str, prefix: str = "") -> list:
    """
    Liste les objets d'un bucket MinIO.

    Args:
        client : Client boto3 (retourné par get_minio_client)
        bucket : Nom du bucket (ex. "bronze")
        prefix : Préfixe facultatif pour filtrer les objets (ex. "statsbomb/")

    Returns:
        Liste des clés (chemins) des objets trouvés.
    """
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    keys = [obj["Key"] for obj in objects]
    print(f"[MinIO] {len(keys)} objet(s) trouvé(s) dans s3://{bucket}/{prefix}")
    return keys


def read_json_from_minio(
    client: boto3.client,
    bucket: str,
    key: str,
) -> dict | list:
    """
    Lit un fichier JSON stocké dans MinIO et le retourne comme objet Python.

    Args:
        client : Client boto3 (retourné par get_minio_client)
        bucket : Nom du bucket (ex. "bronze")
        key    : Chemin du fichier dans le bucket (ex. "statsbomb/competitions.json")

    Returns:
        Données JSON sous forme de dict ou de list Python.
    """
    response = client.get_object(Bucket=bucket, Key=key)
    content  = response["Body"].read()
    data     = json.loads(content.decode("utf-8"))
    print(f"[MinIO] Fichier lu → s3://{bucket}/{key}")
    return data


def read_jsonl_from_minio(
    client: boto3.client,
    bucket: str,
    key: str,
) -> list[dict]:
    """
    Lit un fichier JSONL (une ligne JSON par enregistrement) depuis MinIO.

    Args:
        client : Client boto3 (retourné par get_minio_client)
        bucket : Nom du bucket (ex. "bronze")
        key    : Chemin du fichier dans le bucket (ex. "statsbomb/events/123.json")

    Returns:
        Liste de dicts Python (un dict par ligne du fichier).
    """
    response = client.get_object(Bucket=bucket, Key=key)
    lines    = response["Body"].read().decode("utf-8").strip().splitlines()
    records  = [json.loads(line) for line in lines if line.strip()]
    print(f"[MinIO] {len(records)} enregistrement(s) lus → s3://{bucket}/{key}")
    return records
