"""Amazon Redshift Collector — READ-ONLY."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class RedshiftCollector(BaseCollector):
    name = "redshift"

    async def collect(self) -> dict:
        clusters=[]; workgroups=[]; namespaces=[]
        try:
            async with self.session.client("redshift") as rs:
                async with self.session.client("cloudwatch") as cw:
                    clusters=await self._collect_clusters(rs,cw)
        except Exception:
            pass
        try:
            async with self.session.client("redshift-serverless") as rss:
                workgroups=await self._collect_workgroups(rss)
                namespaces=await self._collect_namespaces(rss)
        except Exception:
            pass
        return {"clusters": clusters, "serverless_workgroups": workgroups, "serverless_namespaces": namespaces, "price_source": "See aws_pricing.redshift"}

    async def _collect_clusters(self, rs: Any, cw: Any) -> list[dict]:
        out=[]
        paginator=rs.get_paginator("describe_clusters")
        async for page in paginator.paginate():
            for c in page.get("Clusters", []):
                cid=c.get("ClusterIdentifier")
                out.append({"cluster_identifier": cid, "node_type": c.get("NodeType"), "number_of_nodes": c.get("NumberOfNodes"), "status": c.get("ClusterStatus"), "publicly_accessible": c.get("PubliclyAccessible"), "encrypted": c.get("Encrypted"), "availability_zone": c.get("AvailabilityZone"), "automated_snapshot_retention_days": c.get("AutomatedSnapshotRetentionPeriod"), "metrics_30d": {"cpu_avg": await self._metric_avg(cw,"CPUUtilization",cid), "database_connections_avg": await self._metric_avg(cw,"DatabaseConnections",cid), "read_iops_avg": await self._metric_avg(cw,"ReadIOPS",cid), "write_iops_avg": await self._metric_avg(cw,"WriteIOPS",cid)}, "price_source": "See aws_pricing.redshift"})
        return out

    async def _collect_workgroups(self, rss: Any) -> list[dict]:
        out=[]
        paginator=rss.get_paginator("list_workgroups")
        async for page in paginator.paginate():
            for w in page.get("workgroups", []):
                out.append({"workgroup_name": w.get("workgroupName"), "status": w.get("status"), "base_capacity": w.get("baseCapacity"), "enhanced_vpc_routing": w.get("enhancedVpcRouting"), "publicly_accessible": w.get("publiclyAccessible"), "namespace_name": w.get("namespaceName"), "price_source": "See aws_pricing.redshift"})
        return out

    async def _collect_namespaces(self, rss: Any) -> list[dict]:
        out=[]
        paginator=rss.get_paginator("list_namespaces")
        async for page in paginator.paginate():
            for n in page.get("namespaces", []):
                out.append({"namespace_name": n.get("namespaceName"), "status": n.get("status"), "db_name": n.get("dbName"), "kms_key_id": n.get("kmsKeyId"), "price_source": "See aws_pricing.redshift"})
        return out

    async def _metric_avg(self,cw:Any,metric:str,cid:str|None)->float|None:
        if not cid: return None
        end=datetime.now(timezone.utc); start=end-timedelta(days=30)
        resp=await self._safe_call(cw.get_metric_statistics(Namespace="AWS/Redshift", MetricName=metric, Dimensions=[{"Name":"ClusterIdentifier","Value":cid}], StartTime=start, EndTime=end, Period=86400, Statistics=["Average"]))
        if not resp or resp.get("_error"): return None
        vals=[p.get("Average") for p in resp.get("Datapoints",[]) if p.get("Average") is not None]
        return round(sum(vals)/len(vals),2) if vals else None
