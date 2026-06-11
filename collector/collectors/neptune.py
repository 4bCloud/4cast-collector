"""Amazon Neptune Collector — READ-ONLY."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class NeptuneCollector(BaseCollector):
    name = "neptune"

    @staticmethod
    def _is_neptune_engine(value: Any) -> bool:
        return str(value or "").lower().startswith("neptune")

    async def collect(self) -> dict:
        try:
            async with self.session.client("neptune") as nep:
                async with self.session.client("cloudwatch") as cw:
                    clusters = await self._collect_clusters(nep, cw)
                    instances = await self._collect_instances(nep, cw)
                    snapshots = await self._collect_snapshots(nep)
        except Exception as exc:
            return {"_error": str(exc), "clusters": [], "instances": [], "snapshots": []}
        old = [s for s in snapshots if (s.get("age_days") or 0) >= 90]
        return {"clusters": clusters, "instances": instances, "manual_snapshots": snapshots, "old_snapshots_90d": old, "price_source": "See aws_pricing.neptune"}

    async def _collect_clusters(self, nep: Any, cw: Any) -> list[dict]:
        out=[]
        paginator = nep.get_paginator("describe_db_clusters")
        async for page in paginator.paginate():
            for c in page.get("DBClusters", []):
                if not self._is_neptune_engine(c.get("Engine")):
                    continue
                cid=c.get("DBClusterIdentifier")
                out.append({
                    "cluster_identifier": cid,
                    "arn": c.get("DBClusterArn"),
                    "engine": c.get("Engine"),
                    "engine_version": c.get("EngineVersion"),
                    "status": c.get("Status"),
                    "multi_az": len(c.get("AvailabilityZones", [])) > 1,
                    "backup_retention_days": c.get("BackupRetentionPeriod"),
                    "storage_encrypted": c.get("StorageEncrypted"),
                    "deletion_protection": c.get("DeletionProtection"),
                    "members": c.get("DBClusterMembers", []),
                    "metrics_30d": {
                        "cpu_avg": await self._metric_avg(cw, "CPUUtilization", "DBClusterIdentifier", cid),
                        "connections_avg": await self._metric_avg(cw, "DatabaseConnections", "DBClusterIdentifier", cid),
                    },
                    "price_source": "See aws_pricing.neptune",
                })
        return out

    async def _collect_instances(self, nep: Any, cw: Any) -> list[dict]:
        out=[]
        paginator = nep.get_paginator("describe_db_instances")
        async for page in paginator.paginate():
            for i in page.get("DBInstances", []):
                if not self._is_neptune_engine(i.get("Engine")):
                    continue
                iid=i.get("DBInstanceIdentifier")
                out.append({
                    "db_identifier": iid,
                    "cluster_identifier": i.get("DBClusterIdentifier"),
                    "instance_class": i.get("DBInstanceClass"),
                    "engine": i.get("Engine"),
                    "engine_version": i.get("EngineVersion"),
                    "status": i.get("DBInstanceStatus"),
                    "multi_az": i.get("MultiAZ"),
                    "metrics_30d": {
                        "cpu_avg": await self._metric_avg(cw, "CPUUtilization", "DBInstanceIdentifier", iid),
                        "connections_avg": await self._metric_avg(cw, "DatabaseConnections", "DBInstanceIdentifier", iid),
                        "freeable_memory_avg_gb": await self._metric_avg(cw, "FreeableMemory", "DBInstanceIdentifier", iid, divisor=1024**3),
                    },
                    "price_source": "See aws_pricing.neptune",
                })
        return out

    async def _collect_snapshots(self, nep: Any) -> list[dict]:
        out=[]
        try:
            paginator=nep.get_paginator("describe_db_cluster_snapshots")
            async for page in paginator.paginate(SnapshotType="manual"):
                for s in page.get("DBClusterSnapshots", []):
                    if not self._is_neptune_engine(s.get("Engine")):
                        continue
                    created=s.get("SnapshotCreateTime")
                    age=(datetime.now(timezone.utc)-created).days if created else None
                    out.append({"snapshot_id": s.get("DBClusterSnapshotIdentifier"), "cluster_identifier": s.get("DBClusterIdentifier"), "engine": s.get("Engine"), "age_days": age, "created_at": created.isoformat() if hasattr(created,"isoformat") else None, "estimated_monthly_cost": None, "cost_estimate_status": "unavailable"})
        except Exception:
            pass
        return out

    async def _metric_avg(self, cw: Any, metric: str, dim_name: str, dim_val: str | None, divisor: float = 1.0) -> float | None:
        if not dim_val:
            return None
        end=datetime.now(timezone.utc); start=end-timedelta(days=30)
        resp=await self._safe_call(cw.get_metric_statistics(Namespace="AWS/Neptune", MetricName=metric, Dimensions=[{"Name": dim_name, "Value": dim_val}], StartTime=start, EndTime=end, Period=86400, Statistics=["Average"]))
        if not resp or resp.get("_error"): return None
        vals=[p.get("Average") for p in resp.get("Datapoints",[]) if p.get("Average") is not None]
        return round((sum(vals)/len(vals))/divisor,2) if vals else None
