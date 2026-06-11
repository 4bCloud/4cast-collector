"""
Base collector — all AWS collectors inherit from this.
IMPORTANT: All collectors are read-only. No write operations allowed.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import aioboto3
from tenacity import retry, stop_after_attempt, wait_exponential

from collector.core.settings import settings


class BaseCollector(ABC):
    """
    Abstract base for all AWS data collectors.

    All subclasses MUST be read-only — no create/update/delete calls.
    Every collector returns a dict that gets merged into the account's collection payload.
    """

    #: Override in subclass — used for progress display and cache keys
    name: str = "base"

    def __init__(self, account_id: str, account_name: str, session: Any) -> None:
        self.account_id = account_id
        self.account_name = account_name
        self.session = session  # aioboto3 session with assumed role credentials

    @abstractmethod
    async def collect(self) -> dict:
        """
        Collect data from AWS and return a structured dict.
        Must never perform write operations.
        """
        ...

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=False,
    )
    async def _safe_call(self, coro: Any) -> Any:
        """
        Wrap an AWS API call with retry logic and error handling.
        Returns None on final failure instead of crashing the whole collection.
        """
        try:
            return await coro
        except Exception as exc:
            # Surface the error but don't crash — partial data is better than no data
            return {"_error": str(exc), "_collector": self.name}

    async def _paginate(self, client: Any, method: str, key: str, **kwargs: Any) -> list:
        """
        Generic async paginator for AWS API calls.
        Handles NextToken / NextPageToken automatically.
        """
        results = []
        paginator = client.get_paginator(method)
        async for page in paginator.paginate(**kwargs):
            results.extend(page.get(key, []))
        return results
