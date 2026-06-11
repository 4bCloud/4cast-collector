"""
DocumentDB Collector — READ-ONLY.

Collects Amazon DocumentDB clusters with enough operational and billing evidence
for FinOps analysis:
- DB clusters and cluster members
- Instance-level CloudWatch metrics (CPU, connections, memory, IOPS, latency)
- Cluster-level CloudWatch storage/backup metrics
- Manual cluster snapshots and old snapshot hygiene

Important pricing note:
DocumentDB snapshot APIs may return AllocatedStorage=0 or omit a reliable
per-snapshot size. For backup/snapshot cost, prefer CloudWatch cluster metrics:
SnapshotStorageUsed, BackupRetentionPeriodStorageUsed, TotalBackupStorageBilled.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector

DOCDB_BACKUP_PRICE_PER_GB = 0.095  # fallback only; pricing engine enriches aws_pricing.documentdb
BYTES_IN_GB = 1024**3
DOCDB_GRAVITON_CLASS_TARGETS = {
    "db.t3": "db.t4g",
    "db.r5": "db.r6g",
    "db.r6i": "db.r6g",
}


class DocumentDBCollector(BaseCollector):
    """Collect DocumentDB clusters, instances, snapshots and CloudWatch metrics."""

    name = "documentdb"

    async def collect(self) -> dict:
        clusters: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []

        async with self.session.client("docdb") as docdb:
            instances_by_id = await self._list_instances_by_id(docdb)

            paginator = docdb.get_paginator("describe_db_clusters")
            async for page in paginator.paginate(Filters=[{"Name": "engine", "Values": ["docdb"]}]):
                for raw_cluster in page.get("DBClusters", []):
                    clusters.append(self._compact_cluster(raw_cluster, instances_by_id))

            snapshots = await self._collect_cluster_snapshots(docdb)

        async with self.session.client("cloudwatch") as cw:
            for cluster in clusters:
                cluster_metrics = await self._fetch_cluster_metrics(
                    cluster.get("cluster_id", ""), cw
                )
                cluster.update(cluster_metrics)

                for member in cluster.get("members", []):
                    member_metrics = await self._fetch_instance_metrics(
                        member.get("db_identifier", ""), cw
                    )
                    member.update(member_metrics)

        old_snapshots = [s for s in snapshots if (s.get("age_days") or 0) > 90]
        self._attach_snapshot_cost_estimates(clusters, snapshots)
        old_snapshot_cost = sum(
            float(s.get("estimated_monthly_cost") or 0)
            for s in old_snapshots
            if s.get("cost_estimate_status") in {"estimated", "estimated_cluster_level"}
        )

        return {
            "clusters": clusters,
            "total_clusters": len(clusters),
            "stopped_clusters": [c for c in clusters if c.get("status") == "stopped"],
            "single_instance_clusters": [
                c for c in clusters if int(c.get("instance_count") or 0) == 1
            ],
            "manual_snapshots": snapshots,
            "old_snapshots_90d": old_snapshots,
            "old_snapshots_monthly_cost": round(old_snapshot_cost, 2),
            "total_manual_snapshot_cost": round(
                sum(
                    float(s.get("estimated_monthly_cost") or 0)
                    for s in snapshots
                    if s.get("cost_estimate_status") in {"estimated", "estimated_cluster_level"}
                ),
                2,
            ),
            "old_snapshots_cost_note": (
                "DocumentDB manual snapshot cost is estimated at cluster level when "
                "SnapshotStorageUsed or TotalBackupStorageBilled CloudWatch metrics are available. "
                "Do not treat AllocatedStorage=0 from snapshot API as confirmed zero cost."
            ),
        }

    async def _list_instances_by_id(self, docdb) -> dict[str, dict[str, Any]]:
        instances: dict[str, dict[str, Any]] = {}
        paginator = docdb.get_paginator("describe_db_instances")
        async for page in paginator.paginate():
            for inst in page.get("DBInstances", []):
                engine = str(inst.get("Engine") or "").lower()
                if engine != "docdb":
                    continue
                identifier = inst.get("DBInstanceIdentifier")
                if identifier:
                    instances[identifier] = inst
        return instances

    async def _collect_cluster_snapshots(self, docdb) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        try:
            paginator = docdb.get_paginator("describe_db_cluster_snapshots")
            async for page in paginator.paginate(
                SnapshotType="manual",
                Filters=[{"Name": "engine", "Values": ["docdb"]}],
            ):
                for snap in page.get("DBClusterSnapshots", []):
                    created = snap.get("SnapshotCreateTime")
                    age_days = None
                    if created:
                        age_days = (datetime.now(timezone.utc) - created).days

                    allocated_gb = snap.get("AllocatedStorage")
                    reliable_size = allocated_gb not in (None, 0)

                    snapshots.append(
                        {
                            "snapshot_id": snap.get("DBClusterSnapshotIdentifier"),
                            "cluster_id": snap.get("DBClusterIdentifier"),
                            "status": snap.get("Status"),
                            "engine": snap.get("Engine"),
                            "engine_version": snap.get("EngineVersion"),
                            "allocated_storage_gb": allocated_gb,
                            "allocated_storage_reliable": bool(reliable_size),
                            "age_days": age_days,
                            "created_at": created.isoformat() if created else None,
                            "estimated_monthly_cost": None,
                            "cost_estimate_status": "unavailable",
                            "cost_estimate_reason": (
                                "Per-snapshot allocated storage was not reliable. "
                                "Use cluster-level SnapshotStorageUsed/TotalBackupStorageBilled metrics."
                            ),
                            "price_source": "See aws_pricing.documentdb.backup_storage_gb_month",
                        }
                    )
        except Exception as exc:
            snapshots.append(
                {
                    "_error": str(exc),
                    "cost_estimate_status": "unavailable",
                    "cost_estimate_reason": "Failed to collect DocumentDB manual snapshots.",
                }
            )
        return snapshots

    def _compact_cluster(
        self,
        cluster: dict[str, Any],
        instances_by_id: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        tags = {t.get("Key"): t.get("Value") for t in cluster.get("TagList", []) if t.get("Key")}
        env = self._tag(tags, "Environment", "environment", "Env", "env")
        members = cluster.get("DBClusterMembers", [])

        compact_members: list[dict[str, Any]] = []
        for member in members:
            instance_id = member.get("DBInstanceIdentifier")
            inst = instances_by_id.get(instance_id or "", {})
            instance_class = inst.get("DBInstanceClass")
            inst_tags = {
                t.get("Key"): t.get("Value") for t in inst.get("TagList", []) if t.get("Key")
            }
            compact_members.append(
                {
                    "db_identifier": instance_id,
                    "role": "writer" if member.get("IsClusterWriter") else "reader",
                    "is_writer": bool(member.get("IsClusterWriter")),
                    "promotion_tier": member.get("PromotionTier"),
                    "instance_class": instance_class,
                    "graviton_candidate": self._graviton_target_class(instance_class) is not None,
                    "graviton_target_class": self._graviton_target_class(instance_class),
                    "status": inst.get("DBInstanceStatus"),
                    "az": inst.get("AvailabilityZone"),
                    "engine": inst.get("Engine"),
                    "engine_version": inst.get("EngineVersion"),
                    "publicly_accessible": inst.get("PubliclyAccessible"),
                    "performance_insights_enabled": inst.get("PerformanceInsightsEnabled"),
                    "monitoring_interval": inst.get("MonitoringInterval"),
                    "tags": inst_tags,
                }
            )

        return {
            "cluster_id": cluster.get("DBClusterIdentifier"),
            "cluster_arn": cluster.get("DBClusterArn"),
            "engine": cluster.get("Engine"),
            "engine_version": cluster.get("EngineVersion"),
            "status": cluster.get("Status"),
            "endpoint": cluster.get("Endpoint"),
            "reader_endpoint": cluster.get("ReaderEndpoint"),
            "availability_zones": cluster.get("AvailabilityZones", []),
            "multi_az": len(cluster.get("AvailabilityZones", [])) > 1,
            "backup_retention": cluster.get("BackupRetentionPeriod", 0),
            "storage_encrypted": cluster.get("StorageEncrypted", False),
            "deletion_protection": cluster.get("DeletionProtection", False),
            "storage_type": cluster.get("StorageType") or "standard",
            "members": compact_members,
            "instances": [
                m.get("db_identifier") for m in compact_members if m.get("db_identifier")
            ],
            "instance_count": len(compact_members),
            "reader_count": sum(1 for m in compact_members if not m.get("is_writer")),
            "writer_count": sum(1 for m in compact_members if m.get("is_writer")),
            "tags": tags,
            "environment_tag": env,
            "is_production": str(env or "").lower() in {"prod", "production", "prd"},
            "metric_capabilities": {
                "cloudwatch_docdb": {
                    "cluster_metrics": False,
                    "member_metrics": False,
                    "backup_storage_metrics": False,
                }
            },
            "cost_note": (
                "DocumentDB billing has four dimensions: instance-hours, database I/O, "
                "database storage and backup storage. I/O-Optimized removes separate I/O charges."
            ),
            "price_source": "See aws_pricing.documentdb",
        }

    async def _fetch_cluster_metrics(self, cluster_id: str, cw) -> dict[str, Any]:
        if not cluster_id:
            return {}
        dims = [{"Name": "DBClusterIdentifier", "Value": cluster_id}]
        metrics = {
            "volume_used": await self._metric_stats(cw, "VolumeBytesUsed", dims, bytes_to_gb=True),
            "snapshot_storage": await self._metric_stats(
                cw, "SnapshotStorageUsed", dims, bytes_to_gb=True
            ),
            "backup_retention_storage": await self._metric_stats(
                cw, "BackupRetentionPeriodStorageUsed", dims, bytes_to_gb=True
            ),
            "total_backup_billed": await self._metric_stats(
                cw, "TotalBackupStorageBilled", dims, bytes_to_gb=True
            ),
            # Cluster-volume billed I/O. AWS documents these as cluster-level
            # billed read/write operations reported from the storage layer.
            "volume_read_iops": await self._metric_stats(cw, "VolumeReadIOPs", dims),
            "volume_write_iops": await self._metric_stats(cw, "VolumeWriteIOPs", dims),
            "change_stream_log_size": await self._metric_stats(
                cw, "ChangeStreamLogSize", dims, bytes_to_gb=True
            ),
        }
        available = any(m.get("datapoints") for m in metrics.values())
        backup_available = any(
            metrics[k].get("datapoints")
            for k in ("snapshot_storage", "backup_retention_storage", "total_backup_billed")
        )
        return {
            "volume_used_avg_gb": metrics["volume_used"].get("avg"),
            "volume_used_max_gb": metrics["volume_used"].get("max"),
            "snapshot_storage_used_gb": metrics["snapshot_storage"].get("avg"),
            "snapshot_storage_used_max_gb": metrics["snapshot_storage"].get("max"),
            "backup_retention_storage_used_gb": metrics["backup_retention_storage"].get("avg"),
            "total_backup_storage_billed_gb": metrics["total_backup_billed"].get("avg"),
            "backup_metric_used": self._best_backup_metric(metrics),
            "backup_monthly_cost_estimate": self._estimate_backup_cost(metrics),
            "volume_read_iops_avg_30d": metrics["volume_read_iops"].get("avg"),
            "volume_read_iops_p95_30d": metrics["volume_read_iops"].get("p95"),
            "volume_write_iops_avg_30d": metrics["volume_write_iops"].get("avg"),
            "volume_write_iops_p95_30d": metrics["volume_write_iops"].get("p95"),
            "change_stream_log_size_avg_gb": metrics["change_stream_log_size"].get("avg"),
            "metrics_lookback_days": 30,
            "metric_capabilities": {
                "cloudwatch_docdb": {
                    "cluster_metrics": available,
                    "member_metrics": False,
                    "backup_storage_metrics": backup_available,
                    "billed_io_metrics": any(
                        metrics[k].get("datapoints")
                        for k in ("volume_read_iops", "volume_write_iops")
                    ),
                }
            },
        }

    async def _fetch_instance_metrics(self, instance_id: str, cw) -> dict[str, Any]:
        if not instance_id:
            return {}
        dims = [{"Name": "DBInstanceIdentifier", "Value": instance_id}]
        cpu = await self._metric_stats(cw, "CPUUtilization", dims)
        conns = await self._metric_stats(cw, "DatabaseConnections", dims)
        free_mem = await self._metric_stats(cw, "FreeableMemory", dims, bytes_to_gb=True)
        read_iops = await self._metric_stats(cw, "ReadIOPS", dims)
        write_iops = await self._metric_stats(cw, "WriteIOPS", dims)
        read_latency = await self._metric_stats(cw, "ReadLatency", dims, seconds_to_ms=True)
        write_latency = await self._metric_stats(cw, "WriteLatency", dims, seconds_to_ms=True)
        queue = await self._metric_stats(cw, "DiskQueueDepth", dims)
        cache = await self._metric_stats(cw, "BufferCacheHitRatio", dims)
        cpu_credit_balance = await self._metric_stats(cw, "CPUCreditBalance", dims)
        cpu_surplus_charged = await self._metric_stats(cw, "CPUSurplusCreditsCharged", dims)
        low_mem_throttle = await self._metric_stats(cw, "LowMemNumOperationsThrottled", dims)

        has_member_metrics = any(
            m.get("datapoints")
            for m in (cpu, conns, free_mem, read_iops, write_iops, read_latency, write_latency)
        )

        return {
            "cpu_avg_30d": cpu.get("avg"),
            "cpu_max_30d": cpu.get("max"),
            "cpu_p95_30d": cpu.get("p95"),
            "connections_avg_30d": conns.get("avg"),
            "connections_max_30d": conns.get("max"),
            "connections_p95_30d": conns.get("p95"),
            "freeable_memory_avg_gb": free_mem.get("avg"),
            "freeable_memory_min_gb": free_mem.get("min"),
            "read_iops_avg_30d": read_iops.get("avg"),
            "read_iops_p95_30d": read_iops.get("p95"),
            "write_iops_avg_30d": write_iops.get("avg"),
            "write_iops_p95_30d": write_iops.get("p95"),
            "read_latency_p95_30d": read_latency.get("p95"),
            "write_latency_p95_30d": write_latency.get("p95"),
            "disk_queue_depth_p95_30d": queue.get("p95"),
            "buffer_cache_hit_ratio_avg_30d": cache.get("avg"),
            "cpu_credit_balance_min_30d": cpu_credit_balance.get("min"),
            "cpu_surplus_credits_charged_avg_30d": cpu_surplus_charged.get("avg"),
            "low_mem_operations_throttled_avg_30d": low_mem_throttle.get("avg"),
            "metric_capabilities": {
                "cloudwatch_docdb_member": {"member_metrics": has_member_metrics}
            },
        }

    async def _metric_stats(
        self,
        cw,
        metric_name: str,
        dimensions: list[dict[str, str]],
        *,
        bytes_to_gb: bool = False,
        seconds_to_ms: bool = False,
    ) -> dict[str, Any]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        try:
            resp = await cw.get_metric_statistics(
                Namespace="AWS/DocDB",
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=["Average", "Maximum", "Minimum"],
            )
        except Exception:
            return {"avg": None, "max": None, "min": None, "p95": None, "datapoints": 0}

        values = []
        for dp in resp.get("Datapoints", []):
            value = dp.get("Average")
            if value is None:
                continue
            value = float(value)
            if bytes_to_gb:
                value = value / BYTES_IN_GB
            if seconds_to_ms:
                value = value * 1000
            values.append(value)

        if not values:
            return {"avg": None, "max": None, "min": None, "p95": None, "datapoints": 0}

        values.sort()
        idx = min(len(values) - 1, int(len(values) * 0.95))
        return {
            "avg": round(sum(values) / len(values), 3),
            "max": round(max(values), 3),
            "min": round(min(values), 3),
            "p95": round(values[idx], 3),
            "datapoints": len(values),
        }

    def _best_backup_metric(self, metrics: dict[str, dict]) -> str | None:
        if metrics.get("total_backup_billed", {}).get("avg") is not None:
            return "TotalBackupStorageBilled"
        if metrics.get("snapshot_storage", {}).get("avg") is not None:
            return "SnapshotStorageUsed"
        if metrics.get("backup_retention_storage", {}).get("avg") is not None:
            return "BackupRetentionPeriodStorageUsed"
        return None

    def _estimate_backup_cost(self, metrics: dict[str, dict]) -> float | None:
        metric_key = None
        if metrics.get("total_backup_billed", {}).get("avg") is not None:
            metric_key = "total_backup_billed"
        elif metrics.get("snapshot_storage", {}).get("avg") is not None:
            metric_key = "snapshot_storage"
        elif metrics.get("backup_retention_storage", {}).get("avg") is not None:
            metric_key = "backup_retention_storage"
        if not metric_key:
            return None
        gb = metrics[metric_key].get("avg")
        if gb is None:
            return None
        return round(float(gb) * DOCDB_BACKUP_PRICE_PER_GB, 2)

    def _attach_snapshot_cost_estimates(
        self,
        clusters: list[dict[str, Any]],
        snapshots: list[dict[str, Any]],
    ) -> None:
        clusters_by_id = {c.get("cluster_id"): c for c in clusters}
        snapshots_by_cluster: dict[str, list[dict[str, Any]]] = {}
        for snap in snapshots:
            cid = snap.get("cluster_id")
            if cid:
                snapshots_by_cluster.setdefault(cid, []).append(snap)

        for cid, items in snapshots_by_cluster.items():
            cluster = clusters_by_id.get(cid, {})
            metric_used = cluster.get("backup_metric_used")
            snapshot_gb = cluster.get("snapshot_storage_used_gb")
            total_backup_gb = cluster.get("total_backup_storage_billed_gb")
            cluster_cost = cluster.get("backup_monthly_cost_estimate")
            old_items = [s for s in items if (s.get("age_days") or 0) > 90]

            for snap in items:
                snap.update(
                    {
                        "cluster_snapshot_storage_used_gb": snapshot_gb,
                        "cluster_total_backup_storage_billed_gb": total_backup_gb,
                        "cluster_backup_metric_used": metric_used,
                        "cluster_backup_monthly_cost_estimate": cluster_cost,
                    }
                )

            if not old_items:
                continue
            if cluster_cost is None or metric_used is None:
                for snap in old_items:
                    snap["cost_estimate_status"] = "unavailable"
                    snap["cost_estimate_reason"] = (
                        "DocumentDB backup storage metrics were not available for this cluster."
                    )
                continue

            # Cluster-level backup metrics are not per-snapshot. Allocate evenly only
            # to avoid false precision and expose the reason to the report/LLM.
            per_snapshot = round(float(cluster_cost) / len(old_items), 2) if old_items else 0.0
            for snap in old_items:
                snap["estimated_monthly_cost"] = per_snapshot
                snap["cost_estimate_status"] = "estimated_cluster_level"
                snap["cost_estimate_reason"] = (
                    f"Estimated from cluster-level {metric_used}; allocated evenly across old manual snapshots."
                )

    @staticmethod
    def _tag(tags: dict[str, Any], *names: str) -> str:
        lowered = {str(k).lower(): v for k, v in tags.items()}
        for name in names:
            if name.lower() in lowered:
                return str(lowered[name.lower()] or "")
        return ""

    def _graviton_target_class(self, instance_class: str | None) -> str | None:
        if not instance_class or "." not in instance_class:
            return None
        family, size = instance_class.rsplit(".", 1)
        target = DOCDB_GRAVITON_CLASS_TARGETS.get(family)
        if not target:
            return None
        return f"{target}.{size}"
