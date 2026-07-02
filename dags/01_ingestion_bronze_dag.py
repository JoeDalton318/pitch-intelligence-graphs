from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task

from ingestion.fetch_public_data import (
    DEFAULT_BASE_URL,
    fetch_json,
    select_match_ids,
    statsbomb_url,
)
from utils.s3_client import ensure_bucket, get_minio_client, put_bytes


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _configured_match_ids() -> list[int]:
    raw_ids = os.getenv("STATSBOMB_MATCH_IDS", "").strip()
    if not raw_ids:
        return []
    return [int(raw_id.strip()) for raw_id in raw_ids.split(",") if raw_id.strip()]


@dag(
    dag_id="01_ingestion_bronze_statsbomb",
    description="Download raw StatsBomb open-data JSON files and store them in MinIO bronze.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "minio", "statsbomb", "membre-a"],
)
def ingestion_bronze_statsbomb() -> None:
    @task(retries=2)
    def download_raw_files_to_bronze() -> dict[str, Any]:
        base_url = os.getenv("STATSBOMB_BASE_URL", DEFAULT_BASE_URL)
        competition_id = _int_env("STATSBOMB_COMPETITION_ID", 43)
        season_id = _int_env("STATSBOMB_SEASON_ID", 106)
        match_limit = _int_env("STATSBOMB_MATCH_LIMIT", 1)
        bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")

        client = get_minio_client()
        ensure_bucket(client, bronze_bucket)

        uploaded_objects: list[str] = []

        def upload_json(relative_source_path: str, object_name: str) -> Any:
            url = statsbomb_url(base_url, relative_source_path)
            payload, parsed = fetch_json(url)
            put_bytes(client, bronze_bucket, object_name, payload, "application/json")
            uploaded_objects.append(object_name)
            return parsed

        upload_json("competitions.json", "statsbomb/competitions.json")

        matches_source = f"matches/{competition_id}/{season_id}.json"
        matches_object = (
            "statsbomb/"
            f"matches/competition_id={competition_id}/season_id={season_id}/matches.json"
        )
        matches = upload_json(matches_source, matches_object)

        match_ids = _configured_match_ids() or select_match_ids(matches, match_limit)
        if not match_ids:
            raise ValueError("No match_id found for bronze ingestion")

        for match_id in match_ids:
            upload_json(
                f"events/{match_id}.json",
                f"statsbomb/events/match_id={match_id}.json",
            )
            upload_json(
                f"lineups/{match_id}.json",
                f"statsbomb/lineups/match_id={match_id}.json",
            )

        manifest = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "statsbomb/open-data",
            "base_url": base_url,
            "competition_id": competition_id,
            "season_id": season_id,
            "match_ids": match_ids,
            "objects": uploaded_objects,
            "layer": "bronze",
        }
        manifest_payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        manifest_object = "statsbomb/manifests/latest_ingestion.json"
        put_bytes(
            client,
            bronze_bucket,
            manifest_object,
            manifest_payload,
            "application/json",
        )
        uploaded_objects.append(manifest_object)

        return {
            "bucket": bronze_bucket,
            "match_count": len(match_ids),
            "uploaded_count": len(uploaded_objects),
            "manifest": manifest_object,
        }

    download_raw_files_to_bronze()


ingestion_bronze_statsbomb()
