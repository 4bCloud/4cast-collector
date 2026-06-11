"""
Aurora Collector — READ-ONLY.

Collects Amazon Aurora clusters separately from traditional RDS DB instances.

Why separate from rds.py?
Aurora is billed under Amazon RDS, but its operating model is cluster-based:
- DBCluster owns topology, backup retention, storage and Serverless v2 scaling.
- DBInstance members are writer/readers attached to the cluster.
- Aurora Serverless v2 exposes ACU metrics and min/max capacity configuration.
- Cluster snapshots are different from DB instance snapshots.

This collector never modifies AWS resources.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector

AURORA_BACKUP_PRICE_PER_GB = 0.095
BYTES_IN_GB = 1024**3
SNAPSHOT_COST_UNAVAILABLE = "unavailable"
SNAPSHOT_COST_ESTIMATED = "estimated_from_cluster_metric"
SNAPSHOT_COST_ZERO_CONFIRMED = "zero_confirmed_by_cluster_metric"
AURORA_GRAVITON_CLASS_TARGETS = {
    "db.t3": "db.t4g",
    "db.m5": "db.r7g",
    "db.r5": "db.r7g",
    "db.r6i": "db.r7g",
    "db.r6g": "db.r7g",
}


class AuroraCollector(BaseCollector):
    """Collect Aurora clusters, members, snapshots and CloudWatch metrics."""

    name = "aurora"

    async def collect(self) -> dict:
        clusters: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []

        async with self.session.client("rds") as rds:
            instances_by_id = await self._list_instances_by_id(rds)

            paginator = rds.get_paginator("describe_db_clusters")
            async for page in paginator.paginate():
                for raw_cluster in page.get("DBClusters", []):
                    engine = str(raw_cluster.get("Engine") or "").lower()
                    if not engine.startswith("aurora"):
                        continue
                    clusters.append(self._compact_cluster(raw_cluster, instances_by_id))

            snapshots = await self._collect_cluster_snapshots(rds)

        async with self.session.client("cloudwatch") as cw:
            for cluster in clusters:
                cluster_metrics = await self._fetch_cluster_metrics(
                    cluster.get("cluster_identifier", ""), cw
                )
                cluster.update(cluster_metrics)

                for member in cluster.get("members", []):
                    member_metrics = await self._fetch_instance_metrics(
                        member.get("db_identifier", ""), cw
                    )
                    member.update(member_metrics)

        snapshots = self._enrich_snapshot_costs_from_cluster_metrics(clusters, snapshots)

        old_snapshots = [s for s in snapshots if (s.get("age_days") or 0) > 90]
        old_snapshot_cost = sum(float(s.get("estimated_monthly_cost") or 0) for s in old_snapshots)

        return {
            "clusters": clusters,
            "total_clusters": len(clusters),
            "serverless_v2_clusters": [c for c in clusters if c.get("serverless_v2")],
            "provisioned_clusters": [c for c in clusters if not c.get("serverless_v2")],
            "manual_cluster_snapshots": snapshots,
            "old_cluster_snapshots_90d": old_snapshots,
            "old_cluster_snapshots_monthly_cost": round(old_snapshot_cost, 2),
            "total_manual_cluster_snapshot_cost": round(
                sum(float(s.get("estimated_monthly_cost") or 0) for s in snapshots), 2
            ),
        }

    async def _list_instances_by_id(self, rds) -> dict[str, dict[str, Any]]:
        """Return DB instances keyed by identifier, including Aurora members."""
        instances: dict[str, dict[str, Any]] = {}
        paginator = rds.get_paginator("describe_db_instances")
        async for page in paginator.paginate():
            for db in page.get("DBInstances", []):
                identifier = db.get("DBInstanceIdentifier")
                if identifier:
                    instances[identifier] = db
        return instances

    async def _collect_cluster_snapshots(self, rds) -> list[dict[str, Any]]:
        snapshots: list[dict[str, Any]] = []
        paginator = rds.get_paginator("describe_db_cluster_snapshots")
        async for page in paginator.paginate(SnapshotType="manual"):
            for snap in page.get("DBClusterSnapshots", []):
                engine = str(snap.get("Engine") or "").lower()
                if not engine.startswith("aurora"):
                    continue

                created = snap.get("SnapshotCreateTime")
                age_days = None
                if created:
                    age_days = (datetime.now(timezone.utc) - created).days

                # For Aurora cluster snapshots, AllocatedStorage is often missing or 0 even
                # when backup/snapshot storage is billable at the cluster level. Do not turn
                # that into a false $0.00 saving. Cluster-level CloudWatch metrics are used
                # later to estimate cost when available.
                allocated_gb = snap.get("AllocatedStorage")
                allocated_gb = float(allocated_gb) if allocated_gb not in (None, "") else None

                snapshots.append(
                    {
                        "snapshot_id": snap.get("DBClusterSnapshotIdentifier"),
                        "cluster_identifier": snap.get("DBClusterIdentifier"),
                        "engine": snap.get("Engine"),
                        "engine_version": snap.get("EngineVersion"),
                        "status": snap.get("Status"),
                        "allocated_storage_gb": allocated_gb,
                        "age_days": age_days,
                        "estimated_monthly_cost": None,
                        "cost_estimate_status": SNAPSHOT_COST_UNAVAILABLE,
                        "cost_estimate_reason": (
                            "Per-snapshot Aurora storage size is not reliable from "
                            "describe_db_cluster_snapshots; waiting for cluster-level "
                            "SnapshotStorageUsed or TotalBackupStorageBilled metrics."
                        ),
                        "created_at": created.isoformat() if created else None,
                    }
                )
        return snapshots

    def _enrich_snapshot_costs_from_cluster_metrics(
        self, clusters: list[dict[str, Any]], snapshots: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Attach cluster-level Aurora backup/snapshot cost estimates to snapshots.

        Aurora manual cluster snapshots are billed at the cluster backup/snapshot
        storage layer. The RDS snapshot API may return AllocatedStorage=0, so the
        safest estimate is cluster-level SnapshotStorageUsed or
        TotalBackupStorageBilled from CloudWatch. If those metrics are missing,
        keep the estimate unavailable instead of reporting a false $0.00.
        """
        clusters_by_id = {c.get("cluster_identifier"): c for c in clusters}
        snapshots_by_cluster: dict[str, list[dict[str, Any]]] = {}
        for snap in snapshots:
            cid = snap.get("cluster_identifier")
            if cid:
                snapshots_by_cluster.setdefault(cid, []).append(snap)

        for cluster_id, cluster_snaps in snapshots_by_cluster.items():
            cluster = clusters_by_id.get(cluster_id, {})
            old_snaps = [s for s in cluster_snaps if (s.get("age_days") or 0) > 90]

            snapshot_storage_gb = cluster.get("snapshot_storage_used_avg_gb")
            total_backup_billed_gb = cluster.get("total_backup_storage_billed_avg_gb")
            backup_retention_gb = cluster.get("backup_retention_storage_avg_gb")

            # Prefer SnapshotStorageUsed because it best maps to manual snapshot cleanup.
            # Fall back to TotalBackupStorageBilled only as a cluster-level estimate.
            billable_gb = snapshot_storage_gb
            metric_name = "SnapshotStorageUsed"
            if billable_gb is None:
                billable_gb = total_backup_billed_gb
                metric_name = "TotalBackupStorageBilled"

            if billable_gb is None:
                for snap in cluster_snaps:
                    snap["cluster_snapshot_storage_used_gb"] = snapshot_storage_gb
                    snap["cluster_total_backup_storage_billed_gb"] = total_backup_billed_gb
                    snap["cluster_backup_retention_storage_gb"] = backup_retention_gb
                continue

            billable_gb = float(billable_gb or 0)
            estimated_cluster_cost = round(billable_gb * AURORA_BACKUP_PRICE_PER_GB, 2)

            if billable_gb <= 0:
                status = SNAPSHOT_COST_ZERO_CONFIRMED
                per_old_snapshot_cost = 0.0
                reason = f"{metric_name}=0 GB for cluster {cluster_id}."
            else:
                status = SNAPSHOT_COST_ESTIMATED
                # Conservative allocation: distribute cluster-level snapshot/backup cost
                # across old manual snapshots so the finding has a non-zero signal.
                # The report must state this is a cluster-level estimate.
                per_old_snapshot_cost = (
                    round(estimated_cluster_cost / len(old_snaps), 2) if old_snaps else 0.0
                )
                reason = (
                    f"Estimated from cluster-level CloudWatch {metric_name} "
                    f"({billable_gb:.2f} GB) at ${AURORA_BACKUP_PRICE_PER_GB:.3f}/GB-month. "
                    "Per-snapshot allocation is approximate."
                )

            for snap in cluster_snaps:
                snap["cluster_snapshot_storage_used_gb"] = snapshot_storage_gb
                snap["cluster_total_backup_storage_billed_gb"] = total_backup_billed_gb
                snap["cluster_backup_retention_storage_gb"] = backup_retention_gb
                snap["cluster_backup_metric_used"] = metric_name
                snap["cluster_backup_monthly_cost_estimate"] = estimated_cluster_cost
                snap["aurora_backup_price_per_gb_month"] = AURORA_BACKUP_PRICE_PER_GB
                snap["cost_estimate_status"] = status
                snap["cost_estimate_reason"] = reason
                if (snap.get("age_days") or 0) > 90:
                    snap["estimated_monthly_cost"] = per_old_snapshot_cost
                else:
                    snap["estimated_monthly_cost"] = 0.0

        return snapshots

    def _compact_cluster(
        self,
        cluster: dict[str, Any],
        instances_by_id: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        scaling = cluster.get("ServerlessV2ScalingConfiguration") or {}
        members: list[dict[str, Any]] = []

        for member in cluster.get("DBClusterMembers", []):
            db_id = member.get("DBInstanceIdentifier")
            db = instances_by_id.get(db_id, {})
            instance_class = db.get("DBInstanceClass")
            members.append(
                {
                    "db_identifier": db_id,
                    "role": "writer" if member.get("IsClusterWriter") else "reader",
                    "is_writer": bool(member.get("IsClusterWriter")),
                    "promotion_tier": member.get("PromotionTier"),
                    "instance_class": instance_class,
                    "graviton_candidate": self._graviton_target_class(instance_class) is not None,
                    "graviton_target_class": self._graviton_target_class(instance_class),
                    "status": db.get("DBInstanceStatus"),
                    "az": db.get("AvailabilityZone"),
                    "engine": db.get("Engine"),
                    "engine_version": db.get("EngineVersion"),
                    "publicly_accessible": db.get("PubliclyAccessible"),
                    "performance_insights_enabled": db.get("PerformanceInsightsEnabled"),
                    "monitoring_interval": db.get("MonitoringInterval"),
                    "tags": self._tags_to_dict(db.get("TagList", [])),
                }
            )

        return {
            "cluster_identifier": cluster.get("DBClusterIdentifier"),
            "cluster_arn": cluster.get("DBClusterArn"),
            "engine": cluster.get("Engine"),
            "engine_version": cluster.get("EngineVersion"),
            "status": cluster.get("Status"),
            "engine_mode": cluster.get("EngineMode"),
            "database_name": cluster.get("DatabaseName"),
            "endpoint": cluster.get("Endpoint"),
            "reader_endpoint": cluster.get("ReaderEndpoint"),
            "multi_az": len(cluster.get("AvailabilityZones", [])) > 1,
            "availability_zones": cluster.get("AvailabilityZones", []),
            "backup_retention_days": cluster.get("BackupRetentionPeriod"),
            "storage_encrypted": cluster.get("StorageEncrypted"),
            "deletion_protection": cluster.get("DeletionProtection"),
            "copy_tags_to_snapshot": cluster.get("CopyTagsToSnapshot"),
            "serverless_v2": bool(scaling),
            "serverless_v2_min_acu": scaling.get("MinCapacity"),
            "serverless_v2_max_acu": scaling.get("MaxCapacity"),
            "members": members,
            "writer_count": sum(1 for m in members if m.get("is_writer")),
            "reader_count": sum(1 for m in members if not m.get("is_writer")),
            "tags": self._tags_to_dict(cluster.get("TagList", [])),
            "metric_capabilities": {
                "cloudwatch_aurora": {
                    "cluster_metrics": False,
                    "member_metrics": False,
                    "serverless_v2_metrics": False,
                }
            },
        }

    async def _fetch_cluster_metrics(self, cluster_id: str, cw) -> dict[str, Any]:
        if not cluster_id:
            return {}

        specs = {
            "VolumeBytesUsed": {
                "prefix": "volume_used",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "SnapshotStorageUsed": {
                "prefix": "snapshot_storage_used",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "BackupRetentionPeriodStorageUsed": {
                "prefix": "backup_retention_storage",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "TotalBackupStorageBilled": {
                "prefix": "total_backup_storage_billed",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "ServerlessDatabaseCapacity": {"prefix": "acu", "convert": None, "extended": True},
            "ACUUtilization": {"prefix": "acu_utilization", "convert": None, "extended": True},
            "DatabaseConnections": {
                "prefix": "cluster_connections",
                "convert": None,
                "extended": True,
            },
        }
        metrics = await self._fetch_metrics(
            cw,
            namespace="AWS/RDS",
            dimension_name="DBClusterIdentifier",
            dimension_value=cluster_id,
            specs=specs,
        )

        has_cluster_metrics = any(
            metrics.get(k) is not None
            for k in (
                "volume_used_avg_gb",
                "snapshot_storage_used_avg_gb",
                "backup_retention_storage_avg_gb",
                "total_backup_storage_billed_avg_gb",
                "acu_avg_30d",
                "acu_utilization_avg_30d",
                "cluster_connections_avg_30d",
            )
        )
        has_serverless_metrics = any(
            metrics.get(k) is not None for k in ("acu_avg_30d", "acu_utilization_avg_30d")
        )
        metrics["metric_capabilities"] = {
            "cloudwatch_aurora": {
                "cluster_metrics": has_cluster_metrics,
                "member_metrics": False,
                "serverless_v2_metrics": has_serverless_metrics,
            }
        }
        metrics["metrics_lookback_days"] = 30
        return metrics

    async def _fetch_instance_metrics(self, db_id: str, cw) -> dict[str, Any]:
        if not db_id:
            return {}

        specs = {
            "CPUUtilization": {"prefix": "cpu", "convert": None, "extended": True},
            "DatabaseConnections": {"prefix": "connections", "convert": None, "extended": True},
            "FreeableMemory": {
                "prefix": "freeable_memory",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "ReadIOPS": {"prefix": "read_iops", "convert": None, "extended": True},
            "WriteIOPS": {"prefix": "write_iops", "convert": None, "extended": True},
            "ReadLatency": {"prefix": "read_latency", "convert": "seconds_to_ms", "extended": True},
            "WriteLatency": {
                "prefix": "write_latency",
                "convert": "seconds_to_ms",
                "extended": True,
            },
            "AuroraReplicaLag": {
                "prefix": "replica_lag",
                "convert": "ms_identity",
                "extended": True,
            },
        }
        metrics = await self._fetch_metrics(
            cw,
            namespace="AWS/RDS",
            dimension_name="DBInstanceIdentifier",
            dimension_value=db_id,
            specs=specs,
        )
        metrics["metric_capabilities"] = {
            "cloudwatch_aurora_member": {
                "member_metrics": any(
                    metrics.get(k) is not None
                    for k in ("cpu_avg_30d", "connections_avg_30d", "freeable_memory_avg_gb")
                )
            }
        }
        return metrics

    async def _fetch_metrics(
        self,
        cw,
        *,
        namespace: str,
        dimension_name: str,
        dimension_value: str,
        specs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        period = 86400
        out: dict[str, Any] = {}

        for metric_name, spec in specs.items():
            stats = ["Average", "Maximum"]

            try:
                resp = await cw.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[{"Name": dimension_name, "Value": dimension_value}],
                    StartTime=start,
                    EndTime=end,
                    Period=period,
                    Statistics=stats,
                )
            except Exception:
                continue

            points = resp.get("Datapoints", [])
            values_avg = [float(p["Average"]) for p in points if "Average" in p]
            values_max = [float(p["Maximum"]) for p in points if "Maximum" in p]
            values_p95 = await self._fetch_p95(
                cw,
                namespace=namespace,
                dimension_name=dimension_name,
                dimension_value=dimension_value,
                metric_name=metric_name,
                start=start,
                end=end,
                period=period,
            ) if spec.get("extended") else []

            prefix = spec["prefix"]
            conv = spec.get("convert")
            out[f"{prefix}_avg_30d"] = self._convert(self._avg(values_avg), conv)
            out[f"{prefix}_max_30d"] = self._convert(max(values_max) if values_max else None, conv)
            if spec.get("extended"):
                out[f"{prefix}_p95_30d"] = self._convert(self._avg(values_p95), conv)
            out[f"{prefix}_datapoints_30d"] = len(points)

        return out

    async def _fetch_p95(
        self,
        cw,
        *,
        namespace: str,
        dimension_name: str,
        dimension_value: str,
        metric_name: str,
        start: datetime,
        end: datetime,
        period: int,
    ) -> list[float]:
        try:
            resp = await cw.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=[{"Name": dimension_name, "Value": dimension_value}],
                StartTime=start,
                EndTime=end,
                Period=period,
                ExtendedStatistics=["p95"],
            )
        except Exception:
            return []
        return [
            float(p["ExtendedStatistics"]["p95"])
            for p in resp.get("Datapoints", [])
            if p.get("ExtendedStatistics", {}).get("p95") is not None
        ]

    def _convert(self, value: float | None, mode: str | None) -> float | None:
        if value is None:
            return None
        if mode == "bytes_to_gb":
            return round(value / BYTES_IN_GB, 2)
        if mode == "seconds_to_ms":
            return round(value * 1000, 2)
        if mode == "ms_identity":
            return round(value, 2)
        return round(value, 2)

    def _avg(self, values: list[float]) -> float | None:
        if not values:
            return None
        return sum(values) / len(values)

    def _tags_to_dict(self, tags: list[dict[str, str]]) -> dict[str, str]:
        return {t.get("Key", ""): t.get("Value", "") for t in tags if t.get("Key")}

    def _graviton_target_class(self, instance_class: str | None) -> str | None:
        if not instance_class or "." not in instance_class:
            return None
        family, size = instance_class.rsplit(".", 1)
        target = AURORA_GRAVITON_CLASS_TARGETS.get(family)
        if not target:
            return None
        return f"{target}.{size}"
