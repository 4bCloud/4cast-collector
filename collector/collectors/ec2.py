"""
EC2 Collector — READ-ONLY.

Collects EC2 instances with:
- CPU utilization via CloudWatch AWS/EC2
- Network utilization via CloudWatch AWS/EC2
- CPU credits for burstable instances
- Optional CloudWatch Agent memory/disk metrics when available
- ALL attached EBS volumes per instance (type, size, IOPS)
- ASG membership detection
- Stopped instance duration
- Real cost breakdown: EC2 + EBS(s)

This gives the LLM the TRUE cost/risk context of each instance, not just
the compute cost or a single CPU average.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector

# EBS pricing per GB-month (On-Demand, approximate — enriched by pricing engine)
EBS_PRICE_PER_GB = {
    "gp2": 0.10,
    "gp3": 0.08,
    "io1": 0.125,
    "io2": 0.125,
    "st1": 0.045,
    "sc1": 0.025,
    "standard": 0.05,
}

# io1/io2 also charge per IOPS-month
EBS_IOPS_PRICE = {
    "io1": 0.065,
    "io2": 0.065,
}


# Graviton migration mapping (x86 -> ARM64 equivalent)
GRAVITON_FAMILY_MAP = {
    "t2": "t4g",
    "t3": "t4g",
    "t3a": "t4g",
    "m5": "m8g",
    "m5a": "m8g",
    "m6i": "m8g",
    "m6a": "m8g",
    "m7i": "m8g",
    "c5": "c8g",
    "c5a": "c8g",
    "c6i": "c8g",
    "c6a": "c8g",
    "c7i": "c8g",
    "r5": "r8g",
    "r5a": "r8g",
    "r6i": "r8g",
    "r6a": "r8g",
    "r7i": "r8g",
}


class EC2Collector(BaseCollector):
    name = "ec2"

    LOOKBACK_DAYS = 14
    METRIC_PERIOD_SECONDS = 3600

    async def collect(self) -> dict:
        instances = []

        async with self.session.client("ec2") as ec2:
            async with self.session.client("cloudwatch") as cw:
                paginator = ec2.get_paginator("describe_instances")
                async for page in paginator.paginate(
                    Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]
                ):
                    for reservation in page.get("Reservations", []):
                        for inst in reservation.get("Instances", []):
                            instance_data = await self._enrich_instance(inst, ec2, cw)
                            instances.append(instance_data)

        return {
            "instances": instances,
            "total_running": sum(1 for i in instances if i["state"] == "running"),
            "total_stopped": sum(1 for i in instances if i["state"] == "stopped"),
            "stopped_over_7_days": [
                i for i in instances if i["state"] == "stopped" and i.get("stopped_days", 0) > 7
            ],
            "low_cpu_instances": [
                i
                for i in instances
                if i["state"] == "running"
                and i.get("cpu_avg_14d") is not None
                and i["cpu_avg_14d"] < 5
            ],
            "gp2_upgrade_candidates": [
                {
                    "instance_id": i["instance_id"],
                    "instance_name": i["name"],
                    "volumes": [
                        v for v in i.get("ebs_volumes", []) if v.get("volume_type") == "gp2"
                    ],
                }
                for i in instances
                if any(v.get("volume_type") == "gp2" for v in i.get("ebs_volumes", []))
            ],
        }

    async def _enrich_instance(self, inst: dict, ec2, cw) -> dict:
        instance_id = inst["InstanceId"]
        instance_type = inst.get("InstanceType", "")
        architecture = inst.get("Architecture", "x86_64")
        state = inst.get("State", {}).get("Name", "unknown")

        # ── Tags ──────────────────────────────────────────────────────────
        tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
        name = tags.get("Name", instance_id)
        env = self._get_environment_tag(tags)
        is_prod = env.lower() in ("prod", "production", "prd")

        # ── ASG detection ─────────────────────────────────────────────────
        asg_name = tags.get("aws:autoscaling:groupName")
        in_asg = bool(asg_name)

        # ── Graviton migration candidate detection ────────────────────────
        inst_family = instance_type.split(".")[0] if "." in instance_type else instance_type
        graviton_target = GRAVITON_FAMILY_MAP.get(inst_family)
        platform = inst.get("Platform", "").lower()
        platform_details = inst.get("PlatformDetails", "")

        graviton_candidate = (
            architecture == "x86_64"
            and graviton_target is not None
            and platform not in ("windows",)
        )

        # ── CloudWatch metrics ────────────────────────────────────────────
        ec2_metrics = {}
        cwagent_metrics = {}

        if state == "running":
            ec2_metrics = await self._collect_cloudwatch_ec2_metrics(cw, instance_id, instance_type)
            cwagent_metrics = await self._collect_cloudwatch_agent_metrics(cw, instance_id)

        # ── Stopped duration ──────────────────────────────────────────────
        stopped_days = None
        if state == "stopped":
            stopped_days = self._parse_stopped_days(inst)

        # ── EBS volumes attached to this instance ─────────────────────────
        ebs_volumes = await self._collect_attached_volumes(inst, ec2)
        ebs_monthly_cost = sum(v.get("estimated_monthly_cost", 0) for v in ebs_volumes)

        # ── Region from AZ ────────────────────────────────────────────────
        az = inst.get("Placement", {}).get("AvailabilityZone", "")
        region = az[:-1] if az else ""

        metric_capabilities = self._build_metric_capabilities(
            ec2_metrics=ec2_metrics,
            cwagent_metrics=cwagent_metrics,
        )

        return {
            "instance_id": instance_id,
            "name": name,
            "type": instance_type,
            "state": state,
            "region": region,
            "az": az,
            "is_production": is_prod,
            "environment_tag": env,
            "in_asg": in_asg,
            "asg_name": asg_name,
            "launch_time": (
                inst.get("LaunchTime", "").isoformat()
                if hasattr(inst.get("LaunchTime", ""), "isoformat")
                else str(inst.get("LaunchTime", ""))
            ),
            "private_ip": inst.get("PrivateIpAddress", ""),
            "private_dns": inst.get("PrivateDnsName", ""),
            "public_ip": inst.get("PublicIpAddress", ""),
            "public_dns": inst.get("PublicDnsName", ""),
            "architecture": architecture,
            "is_arm64": architecture == "arm64",
            "platform": platform,
            "platform_details": platform_details,
            "graviton_candidate": graviton_candidate,
            "graviton_target": graviton_target,
            "graviton_note": (
                "Architecture migration candidate only. Requires ARM64 compatibility validation, "
                "dependency review, image rebuild, testing and rollout plan."
                if graviton_candidate
                else None
            ),
            "tags": tags,
            # CloudWatch AWS/EC2 CPU metrics
            "cpu_avg_14d": ec2_metrics.get("cpu", {}).get("avg"),
            "cpu_p95_14d": ec2_metrics.get("cpu", {}).get("p95"),
            "cpu_max_14d": ec2_metrics.get("cpu", {}).get("max"),
            "cpu_datapoints_14d": ec2_metrics.get("cpu", {}).get("datapoints", 0),
            # CloudWatch AWS/EC2 Network metrics
            "network_in_avg_mbps_14d": ec2_metrics.get("network_in_mbps", {}).get("avg"),
            "network_in_p95_mbps_14d": ec2_metrics.get("network_in_mbps", {}).get("p95"),
            "network_in_max_mbps_14d": ec2_metrics.get("network_in_mbps", {}).get("max"),
            "network_out_avg_mbps_14d": ec2_metrics.get("network_out_mbps", {}).get("avg"),
            "network_out_p95_mbps_14d": ec2_metrics.get("network_out_mbps", {}).get("p95"),
            "network_out_max_mbps_14d": ec2_metrics.get("network_out_mbps", {}).get("max"),
            # CPU credits for burstable families
            "cpu_credit_balance_min_14d": ec2_metrics.get("cpu_credit_balance", {}).get("min"),
            "cpu_credit_balance_avg_14d": ec2_metrics.get("cpu_credit_balance", {}).get("avg"),
            "cpu_credit_usage_avg_14d": ec2_metrics.get("cpu_credit_usage", {}).get("avg"),
            "cpu_surplus_credits_charged_14d": ec2_metrics.get(
                "cpu_surplus_credits_charged", {}
            ).get("sum"),
            # CloudWatch Agent metrics
            "cwagent_memory_used_avg_pct": cwagent_metrics.get("memory_used", {}).get("avg"),
            "cwagent_memory_used_p95_pct": cwagent_metrics.get("memory_used", {}).get("p95"),
            "cwagent_memory_used_max_pct": cwagent_metrics.get("memory_used", {}).get("max"),
            "cwagent_memory_available_avg_pct": cwagent_metrics.get("memory_available", {}).get(
                "avg"
            ),
            "cwagent_disk_used_avg_pct": cwagent_metrics.get("disk_used", {}).get("avg"),
            "cwagent_disk_used_p95_pct": cwagent_metrics.get("disk_used", {}).get("p95"),
            "cwagent_disk_used_max_pct": cwagent_metrics.get("disk_used", {}).get("max"),
            "metric_capabilities": metric_capabilities,
            "stopped_days": stopped_days,
            # EBS breakdown
            "ebs_volumes": ebs_volumes,
            "ebs_total_gb": sum(v.get("size_gb", 0) for v in ebs_volumes),
            "ebs_monthly_cost_est": round(ebs_monthly_cost, 2),
            "ebs_has_gp2": any(v.get("volume_type") == "gp2" for v in ebs_volumes),
            "ebs_gp2_savings_potential": round(
                sum(
                    v.get("size_gb", 0) * (EBS_PRICE_PER_GB["gp2"] - EBS_PRICE_PER_GB["gp3"])
                    for v in ebs_volumes
                    if v.get("volume_type") == "gp2"
                ),
                2,
            ),
        }

    def _get_environment_tag(self, tags: dict[str, str]) -> str:
        """Return environment tag without relying on account names."""
        for key in (
            "Environment",
            "environment",
            "env",
            "ENV",
            "Stage",
            "stage",
            "STAGE",
            "Workspace",
            "workspace",
        ):
            if key in tags:
                return tags[key]
        return ""

    def _parse_stopped_days(self, inst: dict) -> int | None:
        """Parse stopped duration from EC2 StateTransitionReason."""
        state_reason = inst.get("StateTransitionReason", "")
        try:
            match = re.search(r"\((\d{4}-\d{2}-\d{2})", state_reason)
            if match:
                stopped_date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                return (datetime.now(timezone.utc) - stopped_date).days
        except Exception:
            return None
        return None

    async def _collect_cloudwatch_ec2_metrics(
        self,
        cw,
        instance_id: str,
        instance_type: str,
    ) -> dict:
        """Collect default AWS/EC2 CloudWatch metrics for a running instance."""
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=self.LOOKBACK_DAYS)

        metrics: dict[str, Any] = {}

        cpu_points = await self._get_metric_values(
            cw=cw,
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            start=start,
            end=end,
            period=self.METRIC_PERIOD_SECONDS,
            statistics=["Average", "Maximum"],
            value_key="Average",
        )
        metrics["cpu"] = self._stats(cpu_points)

        network_in_points = await self._get_metric_values(
            cw=cw,
            namespace="AWS/EC2",
            metric_name="NetworkIn",
            dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            start=start,
            end=end,
            period=self.METRIC_PERIOD_SECONDS,
            statistics=["Sum"],
            value_key="Sum",
            transform=lambda value: self._bytes_per_period_to_mbps(
                value,
                self.METRIC_PERIOD_SECONDS,
            ),
        )
        metrics["network_in_mbps"] = self._stats(network_in_points)

        network_out_points = await self._get_metric_values(
            cw=cw,
            namespace="AWS/EC2",
            metric_name="NetworkOut",
            dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            start=start,
            end=end,
            period=self.METRIC_PERIOD_SECONDS,
            statistics=["Sum"],
            value_key="Sum",
            transform=lambda value: self._bytes_per_period_to_mbps(
                value,
                self.METRIC_PERIOD_SECONDS,
            ),
        )
        metrics["network_out_mbps"] = self._stats(network_out_points)

        if self._is_burstable_instance(instance_type):
            credit_balance_points = await self._get_metric_values(
                cw=cw,
                namespace="AWS/EC2",
                metric_name="CPUCreditBalance",
                dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Average", "Minimum"],
                value_key="Average",
            )
            metrics["cpu_credit_balance"] = self._stats(credit_balance_points)

            cpu_credit_usage_points = await self._get_metric_values(
                cw=cw,
                namespace="AWS/EC2",
                metric_name="CPUCreditUsage",
                dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Average"],
                value_key="Average",
            )
            metrics["cpu_credit_usage"] = self._stats(cpu_credit_usage_points)

            surplus_charged_points = await self._get_metric_values(
                cw=cw,
                namespace="AWS/EC2",
                metric_name="CPUSurplusCreditsCharged",
                dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Sum"],
                value_key="Sum",
            )
            metrics["cpu_surplus_credits_charged"] = self._stats(surplus_charged_points)

        return metrics

    async def _collect_cloudwatch_agent_metrics(self, cw, instance_id: str) -> dict:
        """
        Collect CloudWatch Agent metrics if the agent is publishing data.

        CWAgent metric dimensions depend on the agent config. We first discover
        the exact dimension set with list_metrics, then query using those dimensions.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=self.LOOKBACK_DAYS)

        metrics: dict[str, Any] = {}

        mem_used_dimensions = await self._find_cwagent_metric_dimensions(
            cw,
            metric_name="mem_used_percent",
            instance_id=instance_id,
        )
        if mem_used_dimensions:
            points = await self._get_metric_values(
                cw=cw,
                namespace="CWAgent",
                metric_name="mem_used_percent",
                dimensions=mem_used_dimensions,
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Average", "Maximum"],
                value_key="Average",
            )
            metrics["memory_used"] = self._stats(points)

        mem_available_dimensions = await self._find_cwagent_metric_dimensions(
            cw,
            metric_name="mem_available_percent",
            instance_id=instance_id,
        )
        if mem_available_dimensions:
            points = await self._get_metric_values(
                cw=cw,
                namespace="CWAgent",
                metric_name="mem_available_percent",
                dimensions=mem_available_dimensions,
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Average"],
                value_key="Average",
            )
            metrics["memory_available"] = self._stats(points)

        disk_dimensions = await self._find_cwagent_metric_dimensions(
            cw,
            metric_name="disk_used_percent",
            instance_id=instance_id,
        )
        if disk_dimensions:
            points = await self._get_metric_values(
                cw=cw,
                namespace="CWAgent",
                metric_name="disk_used_percent",
                dimensions=disk_dimensions,
                start=start,
                end=end,
                period=self.METRIC_PERIOD_SECONDS,
                statistics=["Average", "Maximum"],
                value_key="Average",
            )
            metrics["disk_used"] = self._stats(points)

        return metrics

    async def _find_cwagent_metric_dimensions(
        self,
        cw,
        metric_name: str,
        instance_id: str,
    ) -> list[dict] | None:
        """
        Discover CWAgent metric dimensions for a specific instance.

        CloudWatch Agent metrics often include dimensions such as:
          InstanceId, InstanceType, ImageId, device, fstype, path

        get_metric_statistics must use the same dimension set as the metric.
        """
        response = await self._safe_call(
            cw.list_metrics(
                Namespace="CWAgent",
                MetricName=metric_name,
                Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            )
        )

        if not response or response.get("_error"):
            return None

        metrics = response.get("Metrics", [])
        if not metrics:
            return None

        # Prefer a filesystem/root disk metric when disk has multiple devices.
        if metric_name == "disk_used_percent":
            root_candidates = []
            for metric in metrics:
                dims = metric.get("Dimensions", [])
                dim_map = {d.get("Name"): d.get("Value") for d in dims}
                path = dim_map.get("path") or dim_map.get("Path")
                device = dim_map.get("device") or dim_map.get("Device")

                if path == "/" or device in ("/dev/xvda1", "/dev/nvme0n1p1"):
                    root_candidates.append(dims)

            if root_candidates:
                return root_candidates[0]

        return metrics[0].get("Dimensions", [])

    async def _get_metric_values(
        self,
        *,
        cw,
        namespace: str,
        metric_name: str,
        dimensions: list[dict],
        start: datetime,
        end: datetime,
        period: int,
        statistics: list[str],
        value_key: str,
        transform=None,
    ) -> list[float]:
        """Return metric datapoint values from CloudWatch get_metric_statistics."""
        response = await self._safe_call(
            cw.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=period,
                Statistics=statistics,
            )
        )

        if not response or response.get("_error"):
            return []

        values: list[float] = []
        for point in response.get("Datapoints", []):
            if value_key not in point:
                continue

            raw_value = point.get(value_key)
            if raw_value is None:
                continue

            try:
                value = float(raw_value)
                if transform:
                    value = float(transform(value))
                if math.isfinite(value):
                    values.append(value)
            except (TypeError, ValueError):
                continue

        return values

    def _stats(self, values: list[float]) -> dict:
        """Return avg/p95/max/min/sum/datapoints for numeric metric values."""
        clean = [v for v in values if math.isfinite(v)]
        if not clean:
            return {
                "avg": None,
                "p95": None,
                "max": None,
                "min": None,
                "sum": None,
                "datapoints": 0,
            }

        clean.sort()
        p95_index = min(len(clean) - 1, max(0, math.ceil(len(clean) * 0.95) - 1))

        return {
            "avg": round(sum(clean) / len(clean), 2),
            "p95": round(clean[p95_index], 2),
            "max": round(max(clean), 2),
            "min": round(min(clean), 2),
            "sum": round(sum(clean), 2),
            "datapoints": len(clean),
        }

    def _bytes_per_period_to_mbps(self, byte_count: float, period_seconds: int) -> float:
        """Convert CloudWatch network Sum bytes per period into Mbps."""
        if period_seconds <= 0:
            return 0.0
        return (byte_count * 8) / period_seconds / 1_000_000

    def _is_burstable_instance(self, instance_type: str) -> bool:
        """Return True for T-family burstable instances."""
        family = instance_type.split(".")[0] if "." in instance_type else instance_type
        return family.startswith("t")

    def _build_metric_capabilities(
        self,
        *,
        ec2_metrics: dict,
        cwagent_metrics: dict,
    ) -> dict:
        """Build metric capability metadata for this EC2 instance."""
        return {
            "cloudwatch_ec2": {
                "available": bool(ec2_metrics),
                "cpu": bool(ec2_metrics.get("cpu", {}).get("datapoints", 0)),
                "network": bool(
                    ec2_metrics.get("network_in_mbps", {}).get("datapoints", 0)
                    or ec2_metrics.get("network_out_mbps", {}).get("datapoints", 0)
                ),
                "cpu_credits": bool(
                    ec2_metrics.get("cpu_credit_balance", {}).get("datapoints", 0)
                    or ec2_metrics.get("cpu_credit_usage", {}).get("datapoints", 0)
                ),
            },
            "cloudwatch_agent": {
                "available": bool(cwagent_metrics),
                "memory": bool(
                    cwagent_metrics.get("memory_used", {}).get("datapoints", 0)
                    or cwagent_metrics.get("memory_available", {}).get("datapoints", 0)
                ),
                "disk": bool(cwagent_metrics.get("disk_used", {}).get("datapoints", 0)),
            },
            # Filled later by runner.py when Grafana enrichment is enabled.
            "external_observability": {
                "grafana": {
                    "available": False,
                    "matched": False,
                    "memory": False,
                    "cpu": False,
                    "disk": False,
                    "matched_label_name": None,
                    "matched_label": None,
                    "match_method": None,
                }
            },
        }

    async def _collect_attached_volumes(self, inst: dict, ec2) -> list[dict]:
        """
        Collect all EBS volumes attached to this instance.

        For stopped instances this is critical — the instance isn't running
        but EBS volumes keep accruing charges.
        """
        volumes = []

        for bdm in inst.get("BlockDeviceMappings", []):
            device = bdm.get("DeviceName", "")
            ebs_info = bdm.get("Ebs", {})
            vol_id = ebs_info.get("VolumeId")

            if not vol_id:
                continue

            vol_data = await self._safe_call(ec2.describe_volumes(VolumeIds=[vol_id]))

            if not vol_data or "_error" in vol_data:
                volumes.append(
                    {
                        "volume_id": vol_id,
                        "device": device,
                        "volume_type": "unknown",
                        "size_gb": 0,
                        "iops": None,
                        "throughput_mbps": None,
                        "delete_on_termination": ebs_info.get("DeleteOnTermination", True),
                        "estimated_monthly_cost": 0,
                        "gp2_to_gp3_saving": 0,
                    }
                )
                continue

            vol = vol_data.get("Volumes", [{}])[0]
            vol_type = vol.get("VolumeType", "gp2")
            size_gb = vol.get("Size", 0)
            iops = vol.get("Iops")
            throughput = vol.get("Throughput")

            monthly_cost = size_gb * EBS_PRICE_PER_GB.get(vol_type, 0.10)
            if vol_type in EBS_IOPS_PRICE and iops:
                billable_iops = max(0, iops - 3000)
                monthly_cost += billable_iops * EBS_IOPS_PRICE[vol_type]

            gp2_saving = 0.0
            if vol_type == "gp2":
                gp2_saving = round(size_gb * (EBS_PRICE_PER_GB["gp2"] - EBS_PRICE_PER_GB["gp3"]), 2)

            volumes.append(
                {
                    "volume_id": vol_id,
                    "device": device,
                    "volume_type": vol_type,
                    "size_gb": size_gb,
                    "iops": iops,
                    "throughput_mbps": throughput,
                    "state": vol.get("State"),
                    "encrypted": vol.get("Encrypted", False),
                    "delete_on_termination": ebs_info.get("DeleteOnTermination", True),
                    "estimated_monthly_cost": round(monthly_cost, 2),
                    "gp2_to_gp3_saving": gp2_saving,
                }
            )

        return volumes
