from __future__ import annotations

import logging
from typing import Any

import httpx
from collector.core.settings import settings

log = logging.getLogger(__name__)

class ApiClient:
    def __init__(self, base_url: str | None = None, *, worker_id: str) -> None:
        self.base_url = (base_url or settings.api_base_url).rstrip("/")
        self.worker_id = worker_id
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "x-worker-api-key": settings.worker_api_key,
                "x-worker-id": self.worker_id,
            },
            timeout=30.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_job_auth(self, job_id: str) -> dict[str, Any]:
        response = await self._client.get(f"/internal/scan-jobs/{job_id}/credentials")
        response.raise_for_status()
        return response.json()

    async def update_status(self, job_id: str, status: str, message: str | None = None) -> None:
        response = await self._client.patch(
            f"/api/v1/scan-jobs/{job_id}",
            json={"status": status, "message": message},
            timeout=30.0,
        )
        response.raise_for_status()

    async def submit_worker_result(self, job_id: str, result: dict[str, Any]) -> None:
        response = await self._client.put(
            f"/api/v1/scan-jobs/{job_id}/worker-result",
            json=result,
            timeout=60.0,
        )
        response.raise_for_status()

def build_failed_result(job: dict[str, Any], exc: Exception) -> dict[str, Any]:
    from datetime import datetime, UTC

    return {
        "schema_version": "2026-06-11",
        "agent": "collector",
        "tenant_id": job.get("tenant_id"),
        "scan_id": job.get("scan_id"),
        "status": "failed",
        "finished_at": datetime.now(UTC).isoformat(),
        "collection_coverage": {},
        "artifacts": [],
        "errors": [
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "job_id": job.get("job_id"),
                "scan_id": job.get("scan_id"),
                "scope": "collect",
            }
        ],
    }
