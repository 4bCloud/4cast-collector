from __future__ import annotations
import orjson
from redis.asyncio import Redis

async def publish_scan_progress(
    redis: Redis,
    *,
    tenant_id: str,
    scan_id: str,
    job_id: str,
    stage: str,
    status: str,
    message: str | None = None,
) -> None:
    payload = {
        "tenant_id": tenant_id,
        "scan_id": scan_id,
        "job_id": job_id,
        "stage": stage,
        "status": status,
        "message": message,
    }
    key = f"scan:{scan_id}:progress"
    await redis.hset(key, mapping=payload)
    await redis.publish(f"{key}:events", orjson.dumps(payload))
