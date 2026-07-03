import os
import sys
import json
import concurrent.futures
from datetime import datetime, timezone
from typing import Any
import requests

DEFAULT_BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


def fetch_json(url: str, timeout: int = 60) -> tuple[bytes, Any]:
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "pitch-intelligent-graph/bronze-ingestion"},
    )
    response.raise_for_status()

    payload = response.content
    parsed = json.loads(payload.decode("utf-8"))
    return payload, parsed


def statsbomb_url(base_url: str, relative_path: str) -> str:
    return f"{base_url.rstrip('/')}/{relative_path.lstrip('/')}"


if __name__ == "__main__":
    # Import du client S3 depuis les utilitaires des dags
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
    from dags.utils.s3_client import get_minio_client, ensure_bucket, put_bytes

    print("Debut de l'ingestion Bronze (StatsBomb vers MinIO)...")
    base_url = os.getenv("STATSBOMB_BASE_URL", DEFAULT_BASE_URL)

    # World Cup : ID 43. Saisons : 2018 (3) et 2022 (106)
    competitions = [
        {"competition_id": 43, "season_id": 3, "name": "World Cup 2018"},
        {"competition_id": 43, "season_id": 106, "name": "World Cup 2022"},
    ]
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")

    client = get_minio_client()
    ensure_bucket(client, bronze_bucket)

    def upload_json(relative_source_path: str, object_name: str) -> Any:
        url = statsbomb_url(base_url, relative_source_path)
        payload, parsed = fetch_json(url)
        put_bytes(client, bronze_bucket, object_name, payload, "application/json")
        return parsed

    print("1. Telechargement des fichiers de competitions...")
    upload_json("competitions.json", "statsbomb/competitions.json")

    all_match_ids = []

    for comp in competitions:
        comp_id = comp["competition_id"]
        season_id = comp["season_id"]
        comp_name = comp["name"]

        print(f"2. Telechargement des matchs pour {comp_name}...")
        matches_source = f"matches/{comp_id}/{season_id}.json"
        matches_object = f"statsbomb/matches/competition_id={comp_id}/season_id={season_id}/matches.json"
        matches = upload_json(matches_source, matches_object)

        match_ids = [int(match["match_id"]) for match in matches if "match_id" in match]
        all_match_ids.extend(match_ids)
        print(f"   -> {len(match_ids)} matchs trouves pour {comp_name}.")

    if not all_match_ids:
        print("Erreur : Aucun match trouve.")
        sys.exit(1)

    print(
        f"3. Telechargement asynchrone des evenements et compositions pour {len(all_match_ids)} matchs..."
    )

    # Fonction wrapper pour l'exécution parallèle
    def download_match_data(match_id):
        upload_json(f"events/{match_id}.json", f"statsbomb/events/{match_id}.json")
        upload_json(f"lineups/{match_id}.json", f"statsbomb/lineups/{match_id}.json")
        return match_id

    # Exécution en parallèle (10 threads)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(download_match_data, m_id): m_id for m_id in all_match_ids
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                future.result()
                if i % 10 == 0 or i == len(all_match_ids):
                    print(
                        f"   Progression : {i}/{len(all_match_ids)} matchs telecharges."
                    )
            except Exception as exc:
                m_id = futures[future]
                print(
                    f"Le telechargement du match {m_id} a genere une exception: {exc}"
                )

    manifest = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "statsbomb/open-data",
        "total_matches": len(all_match_ids),
        "layer": "bronze",
    }
    put_bytes(
        client,
        bronze_bucket,
        "statsbomb/manifests/latest_ingestion.json",
        json.dumps(manifest).encode("utf-8"),
        "application/json",
    )

    # Création du fichier de succès pour que le Sensor Airflow le détecte
    print("4. Creation du fichier _SUCCESS...")
    put_bytes(client, bronze_bucket, "statsbomb/events/_SUCCESS", b"", "text/plain")

    print("Ingestion Bronze terminee avec succes.")
