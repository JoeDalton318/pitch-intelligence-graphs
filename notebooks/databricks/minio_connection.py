# Databricks notebook source
# MAGIC %md
# MAGIC # 🔗 Connexion MinIO via ngrok — PitchIntelligentGraph
# MAGIC Notebook orchestré par Databricks Jobs. L'URL ngrok est injectée comme paramètre.

# COMMAND ----------

# Paramètre injecté par le job Databricks (ou saisi manuellement en test)
dbutils.widgets.text("ngrok_url", "https://4563-2001-861-3541-5040-280e-ec70-c526-f201.ngrok-free.app", "URL ngrok MinIO")
NGROK_URL = dbutils.widgets.get("ngrok_url")
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio12345"
BUCKET_BRONZE    = "bronze"

print(f"✅ Paramètres chargés")
print(f"   → NGROK_URL : {NGROK_URL}")
print(f"   → Bucket    : {BUCKET_BRONZE}")

# COMMAND ----------

# MAGIC %md ## 1️⃣ Test de connectivité réseau vers MinIO

# COMMAND ----------

import urllib.request

try:
    req = urllib.request.Request(
        f"{NGROK_URL}/minio/health/live",
        headers={"ngrok-skip-browser-warning": "1"}
    )
    response = urllib.request.urlopen(req, timeout=10)
    print(f"✅ MinIO joignable — HTTP {response.status}")
except Exception as e:
    raise RuntimeError(f"❌ Connexion réseau échouée : {e}")

# COMMAND ----------

# MAGIC %md ## 2️⃣ Connexion boto3 au bucket bronze

# COMMAND ----------

import boto3
import json
from botocore.client import Config

client = boto3.client(
    "s3",
    endpoint_url=NGROK_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

print("✅ Client boto3 MinIO configuré")

# COMMAND ----------

# MAGIC %md ## 3️⃣ Listing du bucket bronze

# COMMAND ----------

response = client.list_objects_v2(Bucket=BUCKET_BRONZE)
fichiers = [obj["Key"] for obj in response.get("Contents", [])]

print(f"✅ {len(fichiers)} fichier(s) dans s3://{BUCKET_BRONZE}/")
for f in fichiers:
    print(f"   → {f}")

# COMMAND ----------

# MAGIC %md ## 4️⃣ Lecture de competitions.json

# COMMAND ----------

response = client.get_object(Bucket=BUCKET_BRONZE, Key="statsbomb/competitions.json")
competitions = json.loads(response["Body"].read())

print(f"✅ {len(competitions)} compétitions lues depuis MinIO")
print(json.dumps(competitions[0], indent=2))
