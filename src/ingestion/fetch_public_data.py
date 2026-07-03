from __future__ import annotations

import json
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


def select_match_ids(matches: list[dict[str, Any]], limit: int) -> list[int]:
    if limit < 1:
        raise ValueError("match limit must be greater than zero")

    selected = sorted(matches, key=lambda item: item.get("match_date", ""))[:limit]
    return [int(match["match_id"]) for match in selected if "match_id" in match]


if __name__ == "__main__":
    import os
    import sys
    from datetime import datetime, timezone

    # Import du client S3 depuis les utilitaires des dags
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
    from dags.utils.s3_client import get_minio_client, ensure_bucket, put_bytes

    print("Debut de l'ingestion Bronze (StatsBomb vers MinIO)...")
    base_url = os.getenv("STATSBOMB_BASE_URL", DEFAULT_BASE_URL)
    competition_id = int(os.getenv("STATSBOMB_COMPETITION_ID", 43))
    season_id = int(os.getenv("STATSBOMB_SEASON_ID", 106))
    match_limit = int(os.getenv("STATSBOMB_MATCH_LIMIT", 1))
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

    print(
        f"2. Telechargement des matchs (competition {competition_id}, saison {season_id})..."
    )
    matches_source = f"matches/{competition_id}/{season_id}.json"
    matches_object = f"statsbomb/matches/competition_id={competition_id}/season_id={season_id}/matches.json"
    matches = upload_json(matches_source, matches_object)

    match_ids = select_match_ids(matches, match_limit)
    if not match_ids:
        print("Erreur : Aucun match trouve.")
        sys.exit(1)

    print(
        f"3. Telechargement des evenements et compositions pour les matchs : {match_ids}..."
    )
    for match_id in match_ids:
        upload_json(
            f"events/{match_id}.json", f"statsbomb/events/match_id={match_id}.json"
        )
        upload_json(
            f"lineups/{match_id}.json", f"statsbomb/lineups/match_id={match_id}.json"
        )

    manifest = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "statsbomb/open-data",
        "match_ids": match_ids,
        "layer": "bronze",
    }
    put_bytes(
        client,
        bronze_bucket,
        "statsbomb/manifests/latest_ingestion.json",
        json.dumps(manifest).encode("utf-8"),
        "application/json",
    )

    print("Ingestion Bronze terminee avec succes.")
