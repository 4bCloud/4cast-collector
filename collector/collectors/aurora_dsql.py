"""Amazon Aurora DSQL Collector — READ-ONLY.

The public AWS APIs for Aurora DSQL may not be available in every boto3 version
or region. This collector is intentionally fail-open and records unsupported
regions instead of crashing the whole scan.
"""
from __future__ import annotations

from collector.collectors.base import BaseCollector


class AuroraDSQLCollector(BaseCollector):
    name = "aurora_dsql"

    async def collect(self) -> dict:
        clusters=[]
        try:
            async with self.session.client("dsql") as dsql:
                # Best effort: different SDK versions may expose list_clusters.
                resp = await self._safe_call(dsql.list_clusters())
                if resp and not resp.get("_error"):
                    for c in resp.get("clusters", []) or resp.get("Clusters", []):
                        arn = c.get("arn") or c.get("clusterArn") or c.get("ClusterArn")
                        detail = c
                        if arn:
                            d = await self._safe_call(dsql.get_cluster(identifier=arn))
                            if d and not d.get("_error"):
                                detail = d.get("cluster", d)
                        clusters.append({"cluster_identifier": detail.get("identifier") or detail.get("clusterIdentifier") or detail.get("name"), "arn": detail.get("arn") or detail.get("clusterArn"), "status": detail.get("status"), "multi_region": bool(detail.get("multiRegionProperties") or detail.get("linkedRegions")), "price_source": "See aws_pricing.aurora_dsql"})
        except Exception as exc:
            return {"_warning": f"Aurora DSQL API unavailable or unsupported in this SDK/region: {exc}", "clusters": [], "total_clusters": 0, "price_source": "See aws_pricing.aurora_dsql"}
        return {"clusters": clusters, "total_clusters": len(clusters), "price_source": "See aws_pricing.aurora_dsql"}
