from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ClaimedPostgresJob:
    db_job_id: str
    payload: dict[str, Any]


class PostgresJobQueue:
    def __init__(self, database_url: str, *, stages: list[str], claimant: str) -> None:
        if not database_url:
            raise ValueError("Postgres job backend requires POSTGRES_JOBS_DATABASE_URL or DATABASE_URL")
        if not stages:
            raise ValueError("Postgres job backend requires at least one worker stage")
        self.database_url = _normalize_asyncpg_url(database_url)
        self.stages = stages
        self.claimant = claimant
        self._pool = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required when WORKER_QUEUE_BACKEND=postgres"
            ) from exc
        self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=2)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> None:
        await self.connect()
        async with self._pool.acquire() as connection:
            await connection.execute("SELECT 1")

    async def claim_next(self) -> ClaimedPostgresJob | None:
        await self.connect()
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT id, payload
                    FROM jobs
                    WHERE stage = ANY($1::text[])
                      AND status = 'queued'
                      AND run_after <= now()
                    ORDER BY priority, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    self.stages,
                )
                if row is None:
                    return None
                updated = await connection.fetchrow(
                    """
                    UPDATE jobs
                    SET status = 'claimed',
                        claimed_by = $1,
                        claimed_at = now(),
                        heartbeat_at = now(),
                        updated_at = now()
                    WHERE id = $2
                    RETURNING id, payload
                    """,
                    self.claimant,
                    row["id"],
                )
                payload = _coerce_payload(updated["payload"])
                payload.setdefault("db_job_id", str(updated["id"]))
                return ClaimedPostgresJob(db_job_id=str(updated["id"]), payload=payload)

    async def mark_running(self, db_job_id: str) -> None:
        await self._update_status(db_job_id, "running")

    async def heartbeat(self, db_job_id: str) -> None:
        await self.connect()
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE jobs
                SET heartbeat_at = now(), updated_at = now()
                WHERE id = $1
                  AND claimed_by = $2
                  AND status IN ('claimed', 'running')
                """,
                db_job_id,
                self.claimant,
            )

    async def mark_succeeded(self, db_job_id: str) -> None:
        await self.connect()
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE jobs
                SET status = 'succeeded',
                    heartbeat_at = now(),
                    finished_at = now(),
                    updated_at = now()
                WHERE id = $1
                  AND claimed_by = $2
                  AND status IN ('claimed', 'running')
                """,
                db_job_id,
                self.claimant,
            )

    async def mark_failed(self, db_job_id: str, error: str, *, backoff_seconds: int = 60) -> None:
        await self.connect()
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE jobs
                SET attempts = attempts + 1,
                    last_error = $3,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    heartbeat_at = NULL,
                    status = CASE
                        WHEN attempts + 1 >= max_attempts THEN 'dead'
                        ELSE 'queued'
                    END,
                    run_after = CASE
                        WHEN attempts + 1 >= max_attempts THEN run_after
                        ELSE now() + ($4::text || ' seconds')::interval
                    END,
                    finished_at = CASE
                        WHEN attempts + 1 >= max_attempts THEN now()
                        ELSE finished_at
                    END,
                    updated_at = now()
                WHERE id = $1
                  AND claimed_by = $2
                  AND status IN ('claimed', 'running')
                """,
                db_job_id,
                self.claimant,
                error[:4000],
                str(backoff_seconds),
            )

    async def _update_status(self, db_job_id: str, status: str) -> None:
        await self.connect()
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE jobs
                SET status = $3, heartbeat_at = now(), updated_at = now()
                WHERE id = $1
                  AND claimed_by = $2
                  AND status IN ('claimed', 'running')
                """,
                db_job_id,
                self.claimant,
                status,
            )


def _coerce_payload(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("job payload must be a JSON object")
        return parsed
    raise TypeError(f"unsupported job payload type: {type(raw).__name__}")


def _normalize_asyncpg_url(database_url: str) -> str:
    if database_url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + database_url.removeprefix("postgresql+asyncpg://")
    return database_url
