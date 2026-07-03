"""
02_preparation_silver_dag.py
------------------------------
DAG Airflow – Étape 2 : Transformation Bronze → Silver via Databricks Serverless.

Ce DAG :
  1. Récupère l'URL ngrok active exposant MinIO localement.
  2. Déclenche le job Databricks (ID : 329083345032947) en lui passant l'URL ngrok.
  3. Attend la fin du job et vérifie son succès avant de marquer la tâche terminée.

Déclenchement : manuel (schedule=None) ou en aval du DAG 01.
Tags : silver, databricks, minio, statsbomb, membre-a
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


# ─────────────────────────────────────────────
# Constantes du job Databricks
# ─────────────────────────────────────────────
DATABRICKS_JOB_ID = 329083345032947
DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST", "https://dbc-6090890b-e77d.cloud.databricks.com")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN", "")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _get_ngrok_url() -> str:
    """Interroge l'API locale ngrok (port 4040) pour obtenir l'URL HTTPS publique."""
    try:
        resp = urllib.request.urlopen("http://localhost:4040/api/tunnels", timeout=5)
        tunnels = json.loads(resp.read())["tunnels"]
        https = [t["public_url"] for t in tunnels if t["proto"] == "https"]
        if not https:
            raise AirflowException("Aucun tunnel HTTPS ngrok actif. Lancez : ngrok http 9000")
        return https[0]
    except Exception as exc:
        raise AirflowException(f"Impossible d'atteindre l'API ngrok locale : {exc}") from exc


def _databricks_request(method: str, path: str, payload: dict | None = None) -> dict:
    """Effectue un appel REST vers l'API Databricks."""
    url  = f"{DATABRICKS_HOST}/api/2.1{path}"
    data = json.dumps(payload).encode() if payload else None
    req  = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type":  "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────

@dag(
    dag_id="02_preparation_silver_databricks",
    description="Déclenche le job Databricks Serverless pour transformer Bronze → Silver via ngrok.",
    start_date=datetime(2026, 1, 1),
    schedule=None,           # déclenché manuellement ou par le DAG 01
    catchup=False,
    tags=["silver", "databricks", "minio", "statsbomb", "membre-a"],
)
def preparation_silver_databricks() -> None:

    @task()
    def get_ngrok_url() -> str:
        """Récupère et valide l'URL ngrok en cours."""
        url = _get_ngrok_url()
        print(f"[ngrok] URL active : {url}")
        return url

    @task()
    def trigger_databricks_job(ngrok_url: str) -> int:
        """Lance le job Databricks avec l'URL ngrok comme paramètre."""
        if not DATABRICKS_TOKEN:
            raise AirflowException(
                "Variable DATABRICKS_TOKEN manquante. "
                "Ajoutez-la dans config/airflow.env."
            )
        payload = {
            "job_id": DATABRICKS_JOB_ID,
            "notebook_params": {"ngrok_url": ngrok_url},
        }
        result = _databricks_request("POST", "/jobs/run-now", payload)
        run_id = result["run_id"]
        print(f"[Databricks] Job déclenché — run_id : {run_id}")
        return run_id

    @task()
    def wait_for_job(run_id: int) -> None:
        """Interroge l'API Databricks toutes les 10 s jusqu'à la fin du run."""
        import time

        max_wait  = 600   # 10 minutes max
        interval  = 10    # secondes entre chaque vérification
        elapsed   = 0

        while elapsed < max_wait:
            result = _databricks_request("GET", f"/jobs/runs/get?run_id={run_id}")
            state  = result.get("state", {})
            lc     = state.get("life_cycle_state", "")
            res    = state.get("result_state", "")

            print(f"[Databricks] run_id={run_id} | life_cycle={lc} | result={res}")

            if lc == "TERMINATED":
                if res == "SUCCESS":
                    print("✅ Job Databricks terminé avec succès.")
                    return
                else:
                    msg = state.get("state_message", "")
                    raise AirflowException(
                        f"❌ Job Databricks échoué : {res} — {msg}"
                    )

            time.sleep(interval)
            elapsed += interval

        raise AirflowException(
            f"⏰ Timeout : le job {run_id} n'a pas terminé en {max_wait}s."
        )

    # ─── Orchestration des tâches ───
    url    = get_ngrok_url()
    run_id = trigger_databricks_job(url)
    wait_for_job(run_id)


preparation_silver_databricks()
