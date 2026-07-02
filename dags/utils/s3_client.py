from __future__ import annotations

import os
from io import BytesIO
from urllib.parse import urlparse

from minio import Minio


def _endpoint_without_scheme(endpoint_url: str) -> tuple[str, bool]:
    parsed = urlparse(endpoint_url)
    if not parsed.scheme:
        return endpoint_url, False
    return parsed.netloc, parsed.scheme == "https"


def get_minio_client() -> Minio:
    endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
    endpoint, secure = _endpoint_without_scheme(endpoint_url)

    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio12345"),
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def put_bytes(
    client: Minio,
    bucket_name: str,
    object_name: str,
    payload: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    client.put_object(
        bucket_name,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type=content_type,
    )
