from __future__ import annotations
from typing import Any
import boto3
from collector.core.settings import settings

def object_storage_configured() -> bool:
    return bool(settings.object_storage_bucket.strip())

def required_object_storage_bucket() -> str:
    bucket = settings.object_storage_bucket.strip()
    if not bucket:
        raise RuntimeError("OBJECT_STORAGE_BUCKET is required when object storage is enabled")
    return bucket

def s3_client() -> Any:
    client_kwargs: dict[str, str] = {}
    endpoint = settings.object_storage_endpoint.strip()
    access_key = settings.object_storage_access_key.strip()
    secret_key = settings.object_storage_secret_key.strip()

    if endpoint:
        client_kwargs["endpoint_url"] = endpoint

    if access_key or secret_key:
        if not access_key or not secret_key:
            raise RuntimeError(
                "OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY must be configured together"
            )
        client_kwargs["aws_access_key_id"] = access_key
        client_kwargs["aws_secret_access_key"] = secret_key

    return boto3.client("s3", **client_kwargs)
