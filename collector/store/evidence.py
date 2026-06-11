from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import orjson
from collector.store.object_storage import (
    object_storage_configured,
    required_object_storage_bucket,
    s3_client,
)

EVIDENCE_SCHEMA_VERSION = "2026-06-11"
SENSITIVE_KEYS = {
    "access_key_id",
    "secret_access_key",
    "session_token",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "role_arn",
    "external_id",
}


async def write_evidence_artifact(
    *,
    tenant_id: str,
    scan_id: str,
    collection: dict[str, Any],
) -> dict[str, Any] | None:
    if not object_storage_configured():
        return None

    bucket = required_object_storage_bucket()
    key = evidence_key(tenant_id=tenant_id, scan_id=scan_id)
    body = _compressed_evidence(collection)

    await asyncio.to_thread(_upload_bytes, bucket=bucket, key=key, body=body)
    return {
        "kind": "evidence_collection",
        "uri": f"s3://{bucket}/{key}",
        "content_type": "application/zstd+json",
        "schema_version": EVIDENCE_SCHEMA_VERSION,
    }


def evidence_key(*, tenant_id: str, scan_id: str) -> str:
    return f"evidence/{tenant_id}/{scan_id}/collection.json.zst"


def _compressed_evidence(collection: dict[str, Any]) -> bytes:
    try:
        import zstandard as zstd
    except ImportError as exc:
        raise RuntimeError("zstandard is required to write object storage artifacts") from exc

    payload = _redact_sensitive(deepcopy(collection))
    payload.setdefault("schema_version", EVIDENCE_SCHEMA_VERSION)
    payload.setdefault("provider", "aws")
    payload.setdefault("collected_at", datetime.now(UTC).isoformat())

    raw = orjson.dumps(payload)
    return zstd.ZstdCompressor(level=6).compress(raw)


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if str(key).lower() in SENSITIVE_KEYS:
                redacted[key] = "***redacted***"
            else:
                redacted[key] = _redact_sensitive(item)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _upload_bytes(*, bucket: str, key: str, body: bytes) -> None:
    client = s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/zstd+json",
        Metadata={"schema-version": EVIDENCE_SCHEMA_VERSION},
    )


def write_local_evidence(collection: dict[str, Any], path: str) -> None:
    """Legacy/CLI helper"""
    import zstandard as zstd

    payload = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "collected_at": datetime.now(UTC).isoformat(),
        "provider": "aws",
        "collection": collection,
    }
    raw = orjson.dumps(payload)
    compressed = zstd.ZstdCompressor(level=6).compress(raw)
    with open(path, "wb") as f:
        f.write(compressed)
