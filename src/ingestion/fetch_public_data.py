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
