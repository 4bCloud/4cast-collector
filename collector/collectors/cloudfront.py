"""
CloudFront Collector — READ-ONLY.

CloudFront cost analysis:
- Distributions with zero traffic (idle — still costs for SSL cert requests)
- Low cache hit rates (origin requests dominate = defeating the purpose)
- Price class optimization (serving from all edges vs restricted)
- WAF association cost

IAM required: cloudfront:ListDistributions,
              cloudfront:GetDistributionConfig,
              cloudwatch:GetMetricStatistics (already in policy)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

IDLE_REQUESTS_THRESHOLD = 1000   # requests in 30 days
LOW_CACHE_HIT_THRESHOLD = 0.5   # cache hit rate below 50%


class CloudFrontCollector(BaseCollector):
    name = "cloudfront"

    async def collect(self) -> dict:
        distributions = []

        # CloudFront is a global service — must use us-east-1
        async with self.session.client("cloudfront", region_name="us-east-1") as cf:
            async with self.session.client("cloudwatch", region_name="us-east-1") as cw:
                paginator = cf.get_paginator("list_distributions")
                async for page in paginator.paginate():
                    dist_list = page.get("DistributionList", {})
                    for dist in dist_list.get("Items", []):
                        enriched = await self._enrich_distribution(dist, cw)
                        distributions.append(enriched)

        idle_dists    = [d for d in distributions if d.get("is_idle")]
        low_cache_hit = [d for d in distributions if d.get("low_cache_hit_rate")]

        return {
            "distributions":      distributions,
            "total":              len(distributions),
            "idle_distributions": idle_dists,
            "low_cache_hit_rate": low_cache_hit,
            "price_source":       "See aws_pricing.cloudfront",
            "cost_note": (
                "CloudFront: charged per GB transferred + per HTTPS request. "
                "Price class determines which edge locations serve traffic — "
                "restricting to cheaper regions can reduce costs. "
                "Low cache hit rate means most requests hit origin, "
                "negating CloudFront's cost benefit."
            ),
        }

    async def _enrich_distribution(self, dist: dict, cw) -> dict:
        dist_id     = dist.get("Id", "")
        domain_name = dist.get("DomainName", "")
        status      = dist.get("Status", "")
        price_class = dist.get("PriceClass", "")
        enabled     = dist.get("Enabled", False)

        origins = [
            o.get("DomainName", "")
            for o in dist.get("Origins", {}).get("Items", [])
        ]

        # CloudWatch — requests and cache hit rate
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=30)

        total_requests  = 0
        cache_hit_rate  = None

        for metric, stat in [("Requests", "Sum"), ("BytesDownloaded", "Sum")]:
            resp = await self._safe_call(
                cw.get_metric_statistics(
                    Namespace="AWS/CloudFront",
                    MetricName=metric,
                    Dimensions=[{"Name": "DistributionId", "Value": dist_id}],
                    StartTime=start,
                    EndTime=end,
                    Period=2592000,
                    Statistics=[stat],
                )
            )
            if resp and not resp.get("_error"):
                datapoints = resp.get("Datapoints", [])
                if metric == "Requests":
                    total_requests = sum(d["Sum"] for d in datapoints)

        # Cache hit rate via CacheHitRate metric
        chr_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/CloudFront",
                MetricName="CacheHitRate",
                Dimensions=[{"Name": "DistributionId", "Value": dist_id}],
                StartTime=start,
                EndTime=end,
                Period=2592000,
                Statistics=["Average"],
            )
        )
        if chr_resp and not chr_resp.get("_error"):
            datapoints = chr_resp.get("Datapoints", [])
            if datapoints:
                cache_hit_rate = datapoints[0]["Average"]

        is_idle          = total_requests < IDLE_REQUESTS_THRESHOLD
        low_cache_hit    = cache_hit_rate is not None and cache_hit_rate < LOW_CACHE_HIT_THRESHOLD

        return {
            "distribution_id":  dist_id,
            "domain_name":      domain_name,
            "status":           status,
            "enabled":          enabled,
            "price_class":      price_class,
            "origins":          origins,
            "total_requests_30d": int(total_requests),
            "cache_hit_rate":   round(cache_hit_rate, 2) if cache_hit_rate else None,
            "is_idle":          is_idle,
            "low_cache_hit_rate": low_cache_hit,
            "price_source":     "See aws_pricing.cloudfront",
            "cache_hit_note": (
                f"Cache hit rate is {cache_hit_rate:.0f}% — most requests are going to origin. "
                "Review cache behaviors and TTL settings to improve efficiency."
                if low_cache_hit else None
            ),
        }
