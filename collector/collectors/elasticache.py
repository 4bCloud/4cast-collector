"""
ElastiCache Collector — READ-ONLY.

Collects Redis and Memcached clusters with full cost analysis:
- Cluster configuration (engine, node type, num nodes)
- Idle detection via CloudWatch (CacheHits, CurrConnections, NetworkBytesIn)
- Multi-AZ / replication group awareness
- Automatic failover status
- gp2 → gp3 EBS migration (ElastiCache Redis uses EBS on some node types)
- Snapshot analysis (Redis only)

IAM required: elasticache:DescribeCacheClusters,
              elasticache:DescribeReplicationGroups,
              elasticache:DescribeSnapshots,
              cloudwatch:GetMetricStatistics (already in policy)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

# Idle thresholds over the 30-day CloudWatch window.
IDLE_CONNECTIONS_THRESHOLD = 5
IDLE_HITS_THRESHOLD = 100
IDLE_BYTES_THRESHOLD = 1024
LOOKBACK_DAYS = 30

GRAVITON_NODE_TARGETS = {
    "cache.t3": "cache.t4g",
    "cache.m5": "cache.m7g",
    "cache.m6i": "cache.m7g",
    "cache.r5": "cache.r7g",
    "cache.r6i": "cache.r7g",
}


class ElastiCacheCollector(BaseCollector):
    name = "elasticache"

    async def collect(self) -> dict:
        clusters = []
        replication_groups = []
        snapshots = []

        async with self.session.client("elasticache") as ec:
            async with self.session.client("cloudwatch") as cw:
                # ── Clusters (Memcached + standalone Redis) ────────────────
                paginator = ec.get_paginator("describe_cache_clusters")
                async for page in paginator.paginate(ShowCacheNodeInfo=True):
                    for cluster in page.get("CacheClusters", []):
                        enriched = await self._enrich_cluster(cluster, cw)
                        clusters.append(enriched)

                # ── Replication Groups (Redis with replication) ────────────
                rg_paginator = ec.get_paginator("describe_replication_groups")
                async for page in rg_paginator.paginate():
                    for rg in page.get("ReplicationGroups", []):
                        replication_groups.append(self._parse_replication_group(rg))

                # ── Redis Snapshots ────────────────────────────────────────
                try:
                    snap_paginator = ec.get_paginator("describe_snapshots")
                    async for page in snap_paginator.paginate(SnapshotSource="manual"):
                        for snap in page.get("Snapshots", []):
                            created = snap.get("NodeSnapshots", [{}])[0].get("SnapshotCreationTime")
                            age_days = None
                            if created:
                                age_days = (datetime.now(timezone.utc) - created).days
                            snapshots.append(
                                {
                                    "snapshot_name": snap.get("SnapshotName"),
                                    "cluster_id": snap.get("CacheClusterId")
                                    or snap.get("ReplicationGroupId"),
                                    "engine": snap.get("Engine"),
                                    "node_type": snap.get("CacheNodeType"),
                                    "age_days": age_days,
                                    "status": snap.get("SnapshotStatus"),
                                }
                            )
                except Exception:
                    pass  # snapshots are optional

        idle_clusters = [c for c in clusters if c.get("is_idle")]
        old_snapshots = [s for s in snapshots if (s.get("age_days") or 0) > 90]

        return {
            "clusters": clusters,
            "replication_groups": replication_groups,
            "total_clusters": len(clusters),
            "idle_clusters": idle_clusters,
            "total_idle": len(idle_clusters),
            "manual_snapshots": snapshots,
            "old_snapshots_90d": old_snapshots,
        }

    async def _enrich_cluster(self, cluster: dict, cw) -> dict:
        cluster_id = cluster.get("CacheClusterId", "")
        engine = cluster.get("Engine", "")
        node_type = cluster.get("CacheNodeType", "")
        num_nodes = cluster.get("NumCacheNodes", 1)
        status = cluster.get("CacheClusterStatus", "")
        multi_az = cluster.get("PreferredAvailabilityZone", "") == "Multiple"
        repl_group = cluster.get("ReplicationGroupId")

        nodes = await self._collect_node_metrics(cluster, cw)
        metrics = self._aggregate_cluster_metrics(nodes)

        # ── Idle detection ─────────────────────────────────────────────────
        cache_hits = metrics.get("cache_hits_avg", 0)
        connections = metrics.get("connections_avg", 0)
        network_in = metrics.get("network_bytes_in_avg", 0)

        is_idle = (
            cache_hits < IDLE_HITS_THRESHOLD
            and connections < IDLE_CONNECTIONS_THRESHOLD
            and network_in < IDLE_BYTES_THRESHOLD
        )

        # ── Safety notes ───────────────────────────────────────────────────
        safety_notes = []
        if repl_group:
            safety_notes.append(
                f"Part of replication group '{repl_group}' — "
                "delete via replication group, not individual cluster."
            )
        if multi_az:
            safety_notes.append("Multi-AZ enabled — has automatic failover.")

        return {
            "cluster_id": cluster_id,
            "engine": engine,
            "engine_version": cluster.get("EngineVersion", ""),
            "node_type": node_type,
            "num_nodes": num_nodes,
            "status": status,
            "availability_zone": cluster.get("PreferredAvailabilityZone"),
            "multi_az": multi_az,
            "replication_group": repl_group,
            "cache_nodes": nodes,
            "graviton_candidate": self._graviton_target_node_type(node_type) is not None,
            "graviton_target_node_type": self._graviton_target_node_type(node_type),
            "price_source": f"See aws_pricing.elasticache for {node_type} hourly rate x {num_nodes} nodes x 730 hours",
            "cost_note": f"Calculate: aws_pricing['{node_type}'] x {num_nodes} nodes x 730h/month",
            "metrics_30d": metrics,
            "is_idle": is_idle,
            "idle_reason": (
                f"avg {connections:.0f} connections, {cache_hits:.0f} cache hits/day over 30 days"
                if is_idle
                else None
            ),
            "price_source": "See aws_pricing.elasticache",
            "safety_note": " | ".join(safety_notes) if safety_notes else None,
        }

    async def _collect_node_metrics(self, cluster: dict, cw) -> list[dict]:
        cluster_id = cluster.get("CacheClusterId", "")
        nodes = cluster.get("CacheNodes") or []
        if not nodes:
            nodes = [{"CacheNodeId": "0001"}]

        enriched = []
        for node in nodes:
            node_id = node.get("CacheNodeId") or "0001"
            dimensions = [
                {"Name": "CacheClusterId", "Value": cluster_id},
                {"Name": "CacheNodeId", "Value": node_id},
            ]
            metrics = {
                "cpu": await self._metric_stats(cw, "CPUUtilization", dimensions, "Average", extended=True),
                "engine_cpu": await self._metric_stats(cw, "EngineCPUUtilization", dimensions, "Average", extended=True),
                "memory_used_pct": await self._metric_stats(cw, "DatabaseMemoryUsagePercentage", dimensions, "Average", extended=True),
                "capacity_used_pct": await self._metric_stats(cw, "DatabaseCapacityUsagePercentage", dimensions, "Average", extended=True),
                "bytes_used_for_cache": await self._metric_stats(cw, "BytesUsedForCache", dimensions, "Average"),
                "freeable_memory": await self._metric_stats(cw, "FreeableMemory", dimensions, "Average"),
                "swap_usage": await self._metric_stats(cw, "SwapUsage", dimensions, "Maximum"),
                "evictions": await self._metric_stats(cw, "Evictions", dimensions, "Sum"),
                "connections": await self._metric_stats(cw, "CurrConnections", dimensions, "Average", extended=True),
                "new_connections": await self._metric_stats(cw, "NewConnections", dimensions, "Sum"),
                "cache_hits": await self._metric_stats(cw, "CacheHits", dimensions, "Sum"),
                "cache_misses": await self._metric_stats(cw, "CacheMisses", dimensions, "Sum"),
                "cache_hit_rate": await self._metric_stats(cw, "CacheHitRate", dimensions, "Average"),
                "network_in": await self._metric_stats(cw, "NetworkBytesIn", dimensions, "Sum"),
                "network_out": await self._metric_stats(cw, "NetworkBytesOut", dimensions, "Sum"),
                "replication_lag": await self._metric_stats(cw, "ReplicationLag", dimensions, "Maximum"),
                "traffic_management_active": await self._metric_stats(cw, "TrafficManagementActive", dimensions, "Maximum"),
                "read_latency_us": await self._metric_stats(cw, "SuccessfulReadRequestLatency", dimensions, "Average", extended=True),
                "write_latency_us": await self._metric_stats(cw, "SuccessfulWriteRequestLatency", dimensions, "Average", extended=True),
                "curr_items": await self._metric_stats(cw, "CurrItems", dimensions, "Average"),
            }
            enriched.append(
                {
                    "node_id": node_id,
                    "endpoint": node.get("Endpoint"),
                    "created_at": str(node.get("CacheNodeCreateTime")) if node.get("CacheNodeCreateTime") else None,
                    "metrics_30d": metrics,
                }
            )
        return enriched

    async def _metric_stats(
        self,
        cw,
        metric_name: str,
        dimensions: list[dict],
        statistic: str,
        *,
        extended: bool = False,
    ) -> dict:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=LOOKBACK_DAYS)
        response = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/ElastiCache",
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=[statistic],
            )
        )
        if not response or response.get("_error"):
            return {"datapoints": 0}
        values = [
            float(dp[statistic])
            for dp in response.get("Datapoints", [])
            if statistic in dp
        ]
        if not values:
            return {"datapoints": 0}
        p95_values = await self._metric_p95(
            cw,
            metric_name=metric_name,
            dimensions=dimensions,
            start=start,
            end=end,
        ) if extended else []
        return {
            "datapoints": len(values),
            "avg": round(sum(values) / len(values), 2),
            "max": round(max(values), 2),
            "sum": round(sum(values), 2),
            "p95": round(max(p95_values), 2) if p95_values else None,
        }

    async def _metric_p95(
        self,
        cw,
        *,
        metric_name: str,
        dimensions: list[dict],
        start: datetime,
        end: datetime,
    ) -> list[float]:
        response = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/ElastiCache",
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=86400,
                ExtendedStatistics=["p95"],
            )
        )
        if not response or response.get("_error"):
            return []
        values = []
        for datapoint in response.get("Datapoints", []):
            extended = datapoint.get("ExtendedStatistics") or {}
            if "p95" in extended:
                values.append(float(extended["p95"]))
        return values

    def _aggregate_cluster_metrics(self, nodes: list[dict]) -> dict:
        def rows(metric_name: str) -> list[dict]:
            out = []
            for node in nodes:
                metric = ((node.get("metrics_30d") or {}).get(metric_name) or {})
                if metric.get("datapoints", 0) > 0:
                    out.append(metric)
            return out

        def avg_of(metric_name: str, field: str = "avg") -> float | None:
            metric_rows = rows(metric_name)
            if not metric_rows:
                return None
            return round(sum(float(row.get(field) or 0) for row in metric_rows) / len(metric_rows), 2)

        def max_of(metric_name: str, field: str = "max") -> float | None:
            metric_rows = rows(metric_name)
            if not metric_rows:
                return None
            return round(max(float(row.get(field) or 0) for row in metric_rows), 2)

        def sum_of(metric_name: str, field: str = "sum") -> float | None:
            metric_rows = rows(metric_name)
            if not metric_rows:
                return None
            return round(sum(float(row.get(field) or 0) for row in metric_rows), 2)

        hit_sum = sum_of("cache_hits") or 0
        miss_sum = sum_of("cache_misses") or 0
        computed_hit_rate = round((hit_sum / (hit_sum + miss_sum)) * 100, 2) if hit_sum + miss_sum > 0 else None

        return {
            "lookback_days": LOOKBACK_DAYS,
            "nodes_sampled": len(nodes),
            "cpu_avg": avg_of("cpu"),
            "cpu_p95": max_of("cpu", "p95"),
            "engine_cpu_avg": avg_of("engine_cpu"),
            "engine_cpu_p95": max_of("engine_cpu", "p95"),
            "memory_used_pct_avg": avg_of("memory_used_pct"),
            "memory_used_pct_p95": max_of("memory_used_pct", "p95"),
            "capacity_used_pct_avg": avg_of("capacity_used_pct"),
            "capacity_used_pct_p95": max_of("capacity_used_pct", "p95"),
            "bytes_used_for_cache_avg": avg_of("bytes_used_for_cache"),
            "freeable_memory_avg_gb": round(avg_of("freeable_memory") / (1024 ** 3), 2) if avg_of("freeable_memory") is not None else None,
            "swap_usage_max_mb": round((max_of("swap_usage") or 0) / (1024 ** 2), 2) if max_of("swap_usage") is not None else None,
            "evictions_sum": sum_of("evictions"),
            "connections_avg": avg_of("connections"),
            "connections_p95": max_of("connections", "p95"),
            "new_connections_sum": sum_of("new_connections"),
            "cache_hits_avg": round(hit_sum / max(LOOKBACK_DAYS, 1), 2),
            "cache_misses_sum": miss_sum,
            "cache_hit_ratio_avg": avg_of("cache_hit_rate") or computed_hit_rate,
            "network_bytes_in_avg": round((sum_of("network_in") or 0) / max(LOOKBACK_DAYS, 1), 2),
            "network_bytes_out_avg": round((sum_of("network_out") or 0) / max(LOOKBACK_DAYS, 1), 2),
            "replication_lag_max": max_of("replication_lag"),
            "traffic_management_active_max": max_of("traffic_management_active"),
            "read_latency_p95_ms": round((max_of("read_latency_us", "p95") or 0) / 1000, 2) if max_of("read_latency_us", "p95") is not None else None,
            "write_latency_p95_ms": round((max_of("write_latency_us", "p95") or 0) / 1000, 2) if max_of("write_latency_us", "p95") is not None else None,
            "curr_items_avg": avg_of("curr_items"),
        }

    def _graviton_target_node_type(self, node_type: str) -> str | None:
        if not node_type or "." not in node_type:
            return None
        family, size = node_type.rsplit(".", 1)
        target_family = GRAVITON_NODE_TARGETS.get(family)
        if not target_family:
            return None
        return f"{target_family}.{size}"

    def _parse_replication_group(self, rg: dict) -> dict:
        return {
            "replication_group_id": rg.get("ReplicationGroupId"),
            "description": rg.get("Description"),
            "status": rg.get("Status"),
            "multi_az": rg.get("MultiAZ") == "enabled",
            "automatic_failover": rg.get("AutomaticFailover") == "enabled",
            "cluster_enabled": rg.get("ClusterEnabled", False),
            "num_node_groups": len(rg.get("NodeGroups", [])),
            "member_clusters": rg.get("MemberClusters", []),
            "at_rest_encryption": rg.get("AtRestEncryptionEnabled", False),
            "transit_encryption": rg.get("TransitEncryptionEnabled", False),
        }
