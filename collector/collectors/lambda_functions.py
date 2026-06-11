"""
Lambda Collector — READ-ONLY.

FinOps signals:
- Functions with no invocations in the last 14 days.
- Provisioned concurrency configured on functions/aliases.
- Memory/timeout review context for functions with measurable traffic.

IAM required:
- lambda:ListFunctions
- lambda:GetFunctionConcurrency
- lambda:ListProvisionedConcurrencyConfigs
- cloudwatch:GetMetricStatistics
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class LambdaCollector(BaseCollector):
    name = "lambda"

    async def collect(self) -> dict:
        functions: list[dict] = []
        idle_functions: list[dict] = []
        provisioned_concurrency: list[dict] = []
        memory_reviews: list[dict] = []

        raw_functions: list[dict] = []
        async with self.session.client("lambda") as lamb:
            paginator = lamb.get_paginator("list_functions")
            async for page in paginator.paginate():
                raw_functions.extend(page.get("Functions", []))

            async with self.session.client("cloudwatch") as cw:
                semaphore = asyncio.Semaphore(10)

                async def enrich(fn: dict) -> dict:
                    async with semaphore:
                        return await self._enrich_function(fn, lamb, cw)

                for i in range(0, len(raw_functions), 500):
                    batch = raw_functions[i : i + 500]
                    functions.extend(await asyncio.gather(*[enrich(fn) for fn in batch]))

        for enriched in functions:
            if enriched.get("is_idle"):
                idle_functions.append(enriched)
            if enriched.get("provisioned_concurrency_configs"):
                provisioned_concurrency.append(enriched)
            if enriched.get("requires_memory_review"):
                memory_reviews.append(enriched)

        return {
            "functions": functions,
            "total_functions": len(functions),
            "idle_functions": idle_functions,
            "provisioned_concurrency_functions": provisioned_concurrency,
            "memory_review_functions": memory_reviews,
            "price_source": "See aws_pricing.lambda and cost_attribution.lambda.*",
            "cost_note": (
                "Lambda cost is driven by requests, duration, memory size, "
                "provisioned concurrency and data transfer. Idle functions without "
                "provisioned concurrency usually have negligible direct cost."
            ),
        }

    async def _enrich_function(self, fn: dict[str, Any], lamb, cw) -> dict:
        name = str(fn.get("FunctionName") or "")
        arn = str(fn.get("FunctionArn") or name)
        memory_mb = int(fn.get("MemorySize") or 0)
        timeout = int(fn.get("Timeout") or 0)
        runtime = fn.get("Runtime")
        package_type = fn.get("PackageType")
        code_size = int(fn.get("CodeSize") or 0)
        last_modified = str(fn.get("LastModified") or "")

        invocations_14d = await self._metric_sum_14d(cw, name, "Invocations")
        errors_14d = await self._metric_sum_14d(cw, name, "Errors")
        duration_avg_ms = await self._metric_stat_14d(cw, name, "Duration", "Average")
        duration_p95_ms = await self._metric_extended_stat_14d(cw, name, "Duration", "p95")

        pc_configs = await self._provisioned_concurrency_configs(lamb, name)
        total_pc = sum(int(c.get("AllocatedProvisionedConcurrentExecutions") or 0) for c in pc_configs)

        age_days = self._age_days(last_modified)
        metric_available = invocations_14d is not None
        is_idle = bool(metric_available and invocations_14d == 0 and (age_days is None or age_days >= 14))
        requires_memory_review = bool(
            (invocations_14d or 0) > 0
            and memory_mb >= 1024
            and duration_avg_ms is not None
        )

        return {
            "function_name": name,
            "function_arn": arn,
            "runtime": runtime,
            "package_type": package_type,
            "memory_mb": memory_mb,
            "timeout_seconds": timeout,
            "code_size_mb": round(code_size / 1024 / 1024, 2),
            "last_modified": last_modified,
            "age_days": age_days,
            "invocations_14d": invocations_14d,
            "errors_14d": errors_14d,
            "duration_avg_ms": duration_avg_ms,
            "duration_p95_ms": duration_p95_ms,
            "activity_metric_available": metric_available,
            "provisioned_concurrency_total": total_pc,
            "provisioned_concurrency_configs": pc_configs,
            "is_idle": is_idle,
            "requires_memory_review": requires_memory_review,
            "price_source": "See aws_pricing.lambda and cost_attribution.lambda.*",
        }

    async def _provisioned_concurrency_configs(self, lamb, function_name: str) -> list[dict]:
        resp = await self._safe_call(
            lamb.list_provisioned_concurrency_configs(FunctionName=function_name)
        )
        if not resp or resp.get("_error"):
            return []
        configs = []
        for item in resp.get("ProvisionedConcurrencyConfigs") or []:
            configs.append(
                {
                    "qualifier": item.get("Qualifier"),
                    "allocated": int(item.get("AllocatedProvisionedConcurrentExecutions") or 0),
                    "available": int(item.get("AvailableProvisionedConcurrentExecutions") or 0),
                    "status": item.get("Status"),
                    "last_modified": item.get("LastModified"),
                }
            )
        return configs

    async def _metric_sum_14d(self, cw, function_name: str, metric_name: str) -> float | None:
        return await self._metric_stat_14d(cw, function_name, metric_name, "Sum")

    async def _metric_stat_14d(
        self,
        cw,
        function_name: str,
        metric_name: str,
        stat: str,
    ) -> float | None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric_name,
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=[stat],
            )
        )
        if not resp or resp.get("_error"):
            return None
        datapoints = resp.get("Datapoints") or []
        if not datapoints:
            return 0.0
        values = [float(point.get(stat) or 0.0) for point in datapoints]
        if stat == "Sum":
            return round(sum(values), 2)
        return round(sum(values) / len(values), 2)

    async def _metric_extended_stat_14d(
        self,
        cw,
        function_name: str,
        metric_name: str,
        extended_stat: str,
    ) -> float | None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric_name,
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start,
                EndTime=end,
                Period=86400,
                ExtendedStatistics=[extended_stat],
            )
        )
        if not resp or resp.get("_error"):
            return None
        datapoints = resp.get("Datapoints") or []
        values = [
            float((point.get("ExtendedStatistics") or {}).get(extended_stat) or 0.0)
            for point in datapoints
        ]
        values = [value for value in values if value > 0]
        if not values:
            return 0.0
        return round(sum(values) / len(values), 2)

    def _age_days(self, last_modified: str) -> int | None:
        if not last_modified:
            return None
        try:
            value = last_modified.replace("+0000", "+00:00")
            modified = datetime.fromisoformat(value)
            if modified.tzinfo is None:
                modified = modified.replace(tzinfo=timezone.utc)
            return max((datetime.now(timezone.utc) - modified).days, 0)
        except ValueError:
            return None
