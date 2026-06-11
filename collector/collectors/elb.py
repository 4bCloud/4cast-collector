"""ELB Collector — READ-ONLY. ALB/NLB/Classic ELB cost signals."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from agent.analyzer.database_rules_common import profile
from collector.collectors.base import BaseCollector


IDLE_REQUESTS_30D_THRESHOLD = 0
LOW_REQUESTS_30D_THRESHOLD = 100
LOW_PROCESSED_GB_30D_THRESHOLD = 0.1


class ELBCollector(BaseCollector):
    name = "elb"

    async def collect(self) -> dict:
        v2_load_balancers = await self._collect_v2_load_balancers()
        classic_load_balancers = await self._collect_classic_load_balancers()
        load_balancers = v2_load_balancers + classic_load_balancers

        idle = [lb for lb in load_balancers if lb.get("finops_profile", {}).get("classification") == "ELB_IDLE_REVIEW"]
        low_traffic = [
            lb
            for lb in load_balancers
            if lb.get("finops_profile", {}).get("classification") == "ELB_LOW_TRAFFIC_REVIEW"
        ]

        return {
            "load_balancers": load_balancers,
            "total_load_balancers": len(load_balancers),
            "idle_load_balancers": idle,
            "low_traffic_load_balancers": low_traffic,
            "v2_load_balancers": len(v2_load_balancers),
            "classic_load_balancers": len(classic_load_balancers),
            "price_source": "See aws_pricing.elb",
            "cost_note": (
                "Elastic Load Balancing pricing depends on LB hours plus usage "
                "dimensions such as LCU/NLCU, processed bytes, rules and request volume."
            ),
        }

    async def _collect_v2_load_balancers(self) -> list[dict]:
        load_balancers: list[dict] = []
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)

        async with self.session.client("elbv2") as elbv2:
            async with self.session.client("cloudwatch") as cw:
                paginator = elbv2.get_paginator("describe_load_balancers")
                async for page in paginator.paginate():
                    for lb in page.get("LoadBalancers", []):
                        lb_arn = lb.get("LoadBalancerArn")
                        if not lb_arn:
                            continue
                        lb_type = str(lb.get("Type") or "unknown")
                        lb_name = str(lb.get("LoadBalancerName") or lb_arn)
                        lb_dimension = _v2_lb_dimension(lb_arn)
                        target_groups = await self._target_groups(elbv2, lb_arn)
                        target_health = await self._target_health(elbv2, target_groups)
                        healthy_targets = sum(1 for item in target_health if item.get("state") == "healthy")
                        total_targets = len(target_health)

                        namespace = "AWS/NetworkELB" if lb_type == "network" else "AWS/ApplicationELB"
                        requests_30d = None
                        if lb_type == "application":
                            requests_30d = await self._sum_metric(
                                cw,
                                namespace=namespace,
                                metric_name="RequestCount",
                                dimensions=[{"Name": "LoadBalancer", "Value": lb_dimension}],
                                start=start,
                                end=end,
                            )
                        processed_bytes_30d = await self._sum_metric(
                            cw,
                            namespace=namespace,
                            metric_name="ProcessedBytes",
                            dimensions=[{"Name": "LoadBalancer", "Value": lb_dimension}],
                            start=start,
                            end=end,
                        )
                        lb_item = {
                            "load_balancer_arn": lb_arn,
                            "load_balancer_name": lb_name,
                            "dns_name": lb.get("DNSName"),
                            "canonical_hosted_zone_id": lb.get("CanonicalHostedZoneId"),
                            "type": lb_type,
                            "generation": "v2",
                            "scheme": lb.get("Scheme"),
                            "state": (lb.get("State") or {}).get("Code"),
                            "vpc_id": lb.get("VpcId"),
                            "availability_zones": [
                                az.get("ZoneName") for az in lb.get("AvailabilityZones", []) if az.get("ZoneName")
                            ],
                            "target_group_count": len(target_groups),
                            "target_count": total_targets,
                            "healthy_target_count": healthy_targets,
                            "requests_30d": requests_30d,
                            "processed_bytes_30d": processed_bytes_30d,
                            "processed_gb_30d": _bytes_to_gb(processed_bytes_30d),
                            "metric_window_days": 30,
                        }
                        lb_item["finops_profile"] = self._profile_load_balancer(lb_item)
                        load_balancers.append(lb_item)
        return load_balancers

    async def _collect_classic_load_balancers(self) -> list[dict]:
        load_balancers: list[dict] = []
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)

        async with self.session.client("elb") as elb:
            async with self.session.client("cloudwatch") as cw:
                paginator = elb.get_paginator("describe_load_balancers")
                async for page in paginator.paginate():
                    for lb in page.get("LoadBalancerDescriptions", []):
                        lb_name = str(lb.get("LoadBalancerName") or "unknown-classic-elb")
                        health = await self._safe_call(elb.describe_instance_health(LoadBalancerName=lb_name))
                        states = health.get("InstanceStates", []) if health and not health.get("_error") else []
                        healthy_targets = sum(1 for item in states if item.get("State") == "InService")
                        requests_30d = await self._sum_metric(
                            cw,
                            namespace="AWS/ELB",
                            metric_name="RequestCount",
                            dimensions=[{"Name": "LoadBalancerName", "Value": lb_name}],
                            start=start,
                            end=end,
                        )
                        lb_item = {
                            "load_balancer_arn": lb_name,
                            "load_balancer_name": lb_name,
                            "dns_name": lb.get("DNSName"),
                            "canonical_hosted_zone_id": lb.get("CanonicalHostedZoneNameID"),
                            "type": "classic",
                            "generation": "classic",
                            "scheme": lb.get("Scheme"),
                            "state": "active",
                            "vpc_id": lb.get("VPCId"),
                            "availability_zones": lb.get("AvailabilityZones", []),
                            "target_group_count": 0,
                            "target_count": len(states),
                            "healthy_target_count": healthy_targets,
                            "requests_30d": requests_30d,
                            "processed_bytes_30d": None,
                            "processed_gb_30d": None,
                            "metric_window_days": 30,
                        }
                        lb_item["finops_profile"] = self._profile_load_balancer(lb_item)
                        load_balancers.append(lb_item)
        return load_balancers

    async def _target_groups(self, elbv2, lb_arn: str) -> list[dict]:
        result = await self._safe_call(elbv2.describe_target_groups(LoadBalancerArn=lb_arn))
        if not result or result.get("_error"):
            return []
        return result.get("TargetGroups", [])

    async def _target_health(self, elbv2, target_groups: list[dict]) -> list[dict]:
        health_items: list[dict] = []
        for tg in target_groups:
            tg_arn = tg.get("TargetGroupArn")
            if not tg_arn:
                continue
            result = await self._safe_call(elbv2.describe_target_health(TargetGroupArn=tg_arn))
            if not result or result.get("_error"):
                continue
            for item in result.get("TargetHealthDescriptions", []):
                health_items.append(
                    {
                        "target_group_arn": tg_arn,
                        "target_id": (item.get("Target") or {}).get("Id"),
                        "state": (item.get("TargetHealth") or {}).get("State"),
                    }
                )
        return health_items

    async def _sum_metric(
        self,
        cw,
        *,
        namespace: str,
        metric_name: str,
        dimensions: list[dict[str, str]],
        start: datetime,
        end: datetime,
    ) -> float | None:
        result = await self._safe_call(
            cw.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=["Sum"],
            )
        )
        if not result or result.get("_error"):
            return None
        datapoints = result.get("Datapoints", [])
        if not datapoints:
            return 0.0
        return round(sum(float(point.get("Sum") or 0.0) for point in datapoints), 2)

    def _profile_load_balancer(self, lb: dict[str, Any]) -> dict:
        name = str(lb.get("load_balancer_name") or lb.get("load_balancer_arn") or "unknown")
        lb_type = str(lb.get("type") or "unknown")
        healthy = int(lb.get("healthy_target_count") or 0)
        targets = int(lb.get("target_count") or 0)
        requests = lb.get("requests_30d")
        processed_gb = lb.get("processed_gb_30d")
        evidence = [
            f"Load balancer: {name}",
            f"Type: {lb_type}; scheme: {lb.get('scheme')}",
            f"Targets healthy/total: {healthy}/{targets}",
            f"30-day requests: {_format_metric(requests)}",
            f"30-day processed data: {_format_metric(processed_gb)} GB",
        ]

        no_traffic = (
            requests is not None
            and requests <= IDLE_REQUESTS_30D_THRESHOLD
            and (processed_gb is None or processed_gb <= LOW_PROCESSED_GB_30D_THRESHOLD)
        )
        no_targets = targets == 0 or healthy == 0
        if no_targets or no_traffic:
            return profile(
                service="ELB",
                classification="ELB_IDLE_REVIEW",
                confidence="MEDIUM" if no_targets else "HIGH",
                risk="MEDIUM",
                evidence=evidence,
                warnings=[
                    "Load balancers may be retained for DNS cutover, failover or blue/green deployments."
                ],
                safe_actions=[
                    "Confirm owner, DNS records and deployment path before deleting the load balancer.",
                    "Estimate LB-hour and LCU/NLCU savings before counting monthly savings.",
                ],
                unsafe_actions=["delete_load_balancer_without_dns_and_owner_validation"],
                savings_treatment="cost_review",
                cost_estimate_status="elb_lcu_pricing_required",
                llm_guidance="Present as ELB cost review. Do not claim savings until LB-hour and LCU/NLCU pricing is attributed.",
            )

        low_requests = requests is not None and requests <= LOW_REQUESTS_30D_THRESHOLD
        low_bytes = processed_gb is not None and processed_gb <= LOW_PROCESSED_GB_30D_THRESHOLD
        if low_requests or low_bytes:
            return profile(
                service="ELB",
                classification="ELB_LOW_TRAFFIC_REVIEW",
                confidence="LOW",
                risk="MEDIUM",
                evidence=evidence,
                warnings=["Low traffic can be normal for admin, webhook, failover or batch endpoints."],
                safe_actions=[
                    "Validate whether this low-traffic load balancer can be consolidated, removed or moved behind an existing ingress.",
                    "Estimate LB-hour and LCU/NLCU savings before counting monthly savings.",
                ],
                unsafe_actions=["consolidate_load_balancer_without_routing_validation"],
                savings_treatment="cost_review",
                cost_estimate_status="elb_lcu_pricing_required",
                llm_guidance="Present as low-traffic ELB cost review, not confirmed savings.",
            )

        return profile(
            service="ELB",
            classification="NO_ACTION",
            confidence="HIGH",
            risk="LOW",
            evidence=evidence,
            llm_guidance="No ELB optimization candidate from current metrics.",
        )


def _v2_lb_dimension(lb_arn: str) -> str:
    marker = ":loadbalancer/"
    if marker not in lb_arn:
        return lb_arn
    return lb_arn.split(marker, 1)[1]


def _bytes_to_gb(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / (1024 ** 3), 4)


def _format_metric(value: Any) -> str:
    if value is None:
        return "unavailable"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.4f}".rstrip("0").rstrip(".")
