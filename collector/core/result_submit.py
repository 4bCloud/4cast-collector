from __future__ import annotations

import logging
from typing import Any

import httpx
import orjson
from redis.asyncio import Redis
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from collector.core.api_client import ApiClient

log = logging.getLogger(__name__)

RESULT_DEADLETTER_TTL_SECONDS = 48 * 60 * 60


def should_retry_result_submit(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
        ),
    )


class ResultSubmitter:
    def __init__(self, api: ApiClient, redis: Redis) -> None:
        self.api = api
        self.redis = redis

    async def submit(
        self,
        job_id: str,
        result: dict[str, Any],
        *,
        persist_deadletter: bool = True,
    ) -> None:
        deadletter_key = f"deadletter:result:{job_id}"
        normalized = normalize_worker_result(result)
        if persist_deadletter:
            await self.redis.setex(
                deadletter_key,
                RESULT_DEADLETTER_TTL_SECONDS,
                orjson.dumps(normalized),
            )

        async for attempt in AsyncRetrying(
            retry=retry_if_exception(should_retry_result_submit),
            wait=wait_exponential_jitter(initial=1, max=60),
            stop=stop_after_attempt(6),
            reraise=True,
        ):
            with attempt:
                await self.api.submit_worker_result(job_id, normalized)

        await self.redis.delete(deadletter_key)

    async def replay_deadletters(self) -> tuple[int, int]:
        replayed = 0
        failed = 0
        async for raw_key in self.redis.scan_iter(match="deadletter:result:*"):
            key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            job_id = key.rsplit(":", 1)[-1]
            payload = await self.redis.get(key)
            if payload is None:
                continue
            try:
                result = orjson.loads(payload)
                await self.submit(job_id, result, persist_deadletter=False)
                replayed += 1
            except httpx.HTTPStatusError as exc:
                failed += 1
                body = exc.response.text[:500]
                log.warning(
                    "Dead-letter replay failed for job %s: %s %s",
                    job_id,
                    exc,
                    body,
                )
                if exc.response.status_code in {404, 409}:
                    await self.redis.delete(key)
            except Exception as exc:
                failed += 1
                log.warning("Dead-letter replay failed for job %s: %s", job_id, exc)
        if replayed or failed:
            log.info("Dead-letter replay finished: replayed=%s failed=%s", replayed, failed)
        return replayed, failed


def normalize_worker_result(result: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(result)
    errors = normalized.get("errors")
    if not isinstance(errors, list):
        return normalized
    fixed: list[dict[str, Any]] = []
    for item in errors:
        if isinstance(item, str):
            fixed.append({"message": item, "scope": "worker"})
        elif isinstance(item, dict):
            fixed.append(item)
    normalized["errors"] = fixed
    return normalized
