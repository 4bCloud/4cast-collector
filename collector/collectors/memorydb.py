"""
Amazon MemoryDB Collector — READ-ONLY.

MemoryDB is Redis-compatible but durable, so it must not be treated as a
simple cache. Collection is intentionally conservative and resilient to
AccessDenied/unsupported-region responses.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class MemoryDBCollector(BaseCollector):
    name = "memorydb"

    async def collect(self) -> dict:
        try:
            async with self.session.client("memorydb") as mem:
                async with self.session.client("cloudwatch") as cw:
                    clusters = await self._collect_clusters(mem, cw)
                    snapshots = await self._collect_snapshots(mem)
        except Exception as exc:
            return {"_error": str(exc), "clusters": [], "snapshots": []}

        old_snapshots = [s for s in snapshots if (s.get("age_days") or 0) >= 90]
        return {
            "clusters": clusters,
            "total_clusters": len(clusters),
            "snapshots": snapshots,
            "old_snapshots_90d": old_snapshots,
            "price_source": "See aws_pricing.memorydb",
            "cost_note": "MemoryDB charges include node-hours, data written, backup/snapshot storage and data transfer. Treat optimization as review unless pricing and utilization are explicit.",
        }

    async def _collect_clusters(self, mem: Any, cw: Any) -> list[dict]:
        clusters: list[dict] = []
        paginator = mem.get_paginator("describe_clusters")
        async for page in paginator.paginate():
            for cluster in page.get("Clusters", []):
                clusters.append(await self._enrich_cluster(cluster, cw))
        return clusters

    async def _collect_snapshots(self, mem: Any) -> list[dict]:
        snapshots: list[dict] = []
        try:
            paginator = mem.get_paginator("describe_snapshots")
            async for page in paginator.paginate():
                for snap in page.get("Snapshots", []):
                    created = snap.get("CreateTime")
                    age_days = None
                    if created:
                        age_days = (datetime.now(timezone.utc) - created).days
                    snapshots.append({
                        "snapshot_name": snap.get("Name"),
                        "cluster_name": snap.get("ClusterName"),
                        "status": snap.get("Status"),
                        "source": snap.get("Source"),
                        "age_days": age_days,
                        "created_at": created.isoformat() if hasattr(created, "isoformat") else str(created) if created else None,
                        "estimated_monthly_cost": None,
                        "cost_estimate_status": "unavailable",
                    })
        except Exception:
            pass
        return snapshots

    async def _metric_avg(self, cw: Any, metric: str, cluster: str, stat: str = "Average") -> float | None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        resp = await self._safe_call(cw.get_metric_statistics(
            Namespace="AWS/MemoryDB",
            MetricName=metric,
            Dimensions=[{"Name": "ClusterName", "Value": cluster}],
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=[stat],
        ))
        if not resp or resp.get("_error"):
            return None
        points = resp.get("Datapoints", [])
        vals = [p.get(stat) for p in points if p.get(stat) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    async def _enrich_cluster(self, cluster: dict, cw: Any) -> dict:
        name = cluster.get("Name")
        node_type = cluster.get("NodeType")
        shards = cluster.get("NumberOfShards") or len(cluster.get("Shards", []))
        replicas = cluster.get("NumberOfReplicasPerShard")
        metrics = {}
        if name:
            for metric in ("CPUUtilization", "DatabaseMemoryUsagePercentage", "CurrConnections", "NetworkBytesIn", "NetworkBytesOut"):
                metrics[metric] = await self._metric_avg(cw, metric, name)
        return {
            "cluster_name": name,
            "arn": cluster.get("ARN"),
            "status": cluster.get("Status"),
            "engine_version": cluster.get("EngineVersion"),
            "node_type": node_type,
            "number_of_shards": shards,
            "replicas_per_shard": replicas,
            "tls_enabled": cluster.get("TLSEnabled"),
            "auto_minor_version_upgrade": cluster.get("AutoMinorVersionUpgrade"),
            "snapshot_retention_limit": cluster.get("SnapshotRetentionLimit"),
            "maintenance_window": cluster.get("MaintenanceWindow"),
            "metrics_30d": {
                "cpu_avg": metrics.get("CPUUtilization"),
                "memory_used_pct_avg": metrics.get("DatabaseMemoryUsagePercentage"),
                "connections_avg": metrics.get("CurrConnections"),
                "network_in_avg": metrics.get("NetworkBytesIn"),
                "network_out_avg": metrics.get("NetworkBytesOut"),
            },
            "price_source": "See aws_pricing.memorydb",
        }
