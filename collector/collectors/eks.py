"""
EKS Collector — READ-ONLY.

EKS has critical hidden costs:
- Control plane: $0.10/hour ($73/month) per cluster — always charged
- Extended Support: $0.60/hour ($438/month) when K8s version is out of support
- Worker nodes: EC2 or Fargate (covered by EC2 collector)
- EKS Auto Mode (2024+): additional management fee

IAM required: eks:ListClusters, eks:DescribeCluster,
              eks:ListNodegroups, eks:DescribeNodegroup,
              autoscaling:DescribeAutoScalingGroups,
              ec2:DescribeInstances,
              cloudwatch:GetMetricStatistics
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

# Kubernetes EOL dates — versions past standard support trigger Extended Support ($0.60/hr)
# Updated: May 2026
K8S_STANDARD_SUPPORT_EOL = {
    "1.24": "2024-01-31",
    "1.25": "2024-05-01",
    "1.26": "2024-06-11",
    "1.27": "2024-07-26",
    "1.28": "2024-11-26",
    "1.29": "2025-03-23",
    "1.30": "2025-07-23",
    "1.31": "2025-11-26",
    "1.32": "2026-03-23",
    "1.33": "2026-07-23",  # upcoming
}

EXTENDED_SUPPORT_MULTIPLIER = 6  # 6x the standard control plane price
LOOKBACK_DAYS = 30

GRAVITON_FAMILY_TARGETS = {
    "t3": "t4g",
    "t3a": "t4g",
    "m5": "m7g",
    "m5a": "m7g",
    "m5n": "m7g",
    "m6i": "m7g",
    "m6a": "m7g",
    "c5": "c7g",
    "c5a": "c7g",
    "c5n": "c7g",
    "c6i": "c7g",
    "c6a": "c7g",
    "r5": "r7g",
    "r5a": "r7g",
    "r5n": "r7g",
    "r6i": "r7g",
    "r6a": "r7g",
}

NON_GRAVITON_AMI_MARKERS = ("X86_64", "AL2_X86_64", "AL2023_X86_64")


class EKSCollector(BaseCollector):
    name = "eks"

    async def collect(self) -> dict:
        clusters = []

        async with self.session.client("eks") as eks:
            async with self.session.client("autoscaling") as autoscaling:
                async with self.session.client("ec2") as ec2:
                    async with self.session.client("cloudwatch") as cw:
                        paginator = eks.get_paginator("list_clusters")
                        async for page in paginator.paginate():
                            for cluster_name in page.get("clusters", []):
                                enriched = await self._enrich_cluster(
                                    cluster_name,
                                    eks,
                                    autoscaling,
                                    ec2,
                                    cw,
                                )
                                if enriched:
                                    clusters.append(enriched)

        extended_support = [c for c in clusters if c.get("in_extended_support")]
        near_eol         = [c for c in clusters if c.get("days_until_eol") is not None
                            and 0 < c["days_until_eol"] <= 90]
        node_groups = [ng for c in clusters for ng in c.get("node_groups", [])]
        graviton_reviews = [
            ng for ng in node_groups
            if any(p.get("classification") == "EKS_GRAVITON_REVIEW" for p in ng.get("finops_profiles", []))
        ]
        spot_reviews = [
            ng for ng in node_groups
            if any(p.get("classification") == "EKS_SPOT_REVIEW" for p in ng.get("finops_profiles", []))
        ]
        rightsizing_reviews = [
            ng for ng in node_groups
            if any(p.get("classification") == "EKS_NODEGROUP_RIGHTSIZE_REVIEW" for p in ng.get("finops_profiles", []))
        ]

        return {
            "clusters":                  clusters,
            "total_clusters":            len(clusters),
            "total_node_groups":         len(node_groups),
            "in_extended_support":       extended_support,
            "near_eol_90d":              near_eol,
            "graviton_nodegroup_reviews": graviton_reviews,
            "spot_nodegroup_reviews":     spot_reviews,
            "rightsizing_nodegroup_reviews": rightsizing_reviews,
            "price_source":              "See aws_pricing.eks",
            "critical_note": (
                "EKS control plane is $0.10/hour ($73/month) per cluster regardless of usage. "
                "When Kubernetes version leaves standard support, price jumps to "
                "$0.60/hour ($438/month) — a 6x increase. "
                "Always upgrade before EOL to avoid Extended Support charges."
            ),
        }

    async def _enrich_cluster(self, cluster_name: str, eks, autoscaling, ec2, cw) -> dict | None:
        resp = await self._safe_call(
            eks.describe_cluster(name=cluster_name)
        )
        if not resp or resp.get("_error"):
            return None

        cluster = resp.get("cluster", {})
        version = cluster.get("version", "")
        status  = cluster.get("status", "")
        tags    = cluster.get("tags", {})

        # ── Extended Support detection ─────────────────────────────────────
        in_extended_support = False
        days_until_eol      = None
        eol_date_str        = K8S_STANDARD_SUPPORT_EOL.get(version)

        if eol_date_str:
            eol_date = datetime.strptime(eol_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            now      = datetime.now(timezone.utc)
            delta    = (eol_date - now).days
            days_until_eol = delta
            in_extended_support = delta < 0  # past EOL = in extended support

        # ── Node groups ────────────────────────────────────────────────────
        node_groups = []
        try:
            ng_paginator = eks.get_paginator("list_nodegroups")
            async for page in ng_paginator.paginate(clusterName=cluster_name):
                for ng_name in page.get("nodegroups", []):
                    ng_resp = await self._safe_call(
                        eks.describe_nodegroup(
                            clusterName=cluster_name,
                            nodegroupName=ng_name,
                        )
                    )
                    if ng_resp and not ng_resp.get("_error"):
                        ng = ng_resp.get("nodegroup", {})
                        scaling = ng.get("scalingConfig", {})
                        node_groups.append(
                            await self._enrich_nodegroup(
                                cluster_name=cluster_name,
                                ng=ng,
                                autoscaling=autoscaling,
                                ec2=ec2,
                                cw=cw,
                            )
                        )
        except Exception:
            pass

        cluster_profiles = self._cluster_profiles(
            cluster_name=cluster_name,
            version=version,
            in_extended_support=in_extended_support,
            days_until_eol=days_until_eol,
            eol_date_str=eol_date_str,
        )

        return {
            "cluster_name":        cluster_name,
            "version":             version,
            "status":              status,
            "endpoint":            cluster.get("endpoint", ""),
            "region":              cluster.get("arn", "").split(":")[3],
            "tags":                tags,
            "in_extended_support": in_extended_support,
            "days_until_eol":      days_until_eol,
            "eol_date":            eol_date_str,
            "node_groups":         node_groups,
            "total_node_groups":   len(node_groups),
            "finops_profiles":     cluster_profiles,
            "extended_support_warning": (
                f"CRITICAL: Cluster is on K8s {version} which is past standard support EOL "
                f"({eol_date_str}). You are being charged $0.60/hour ($438/month) "
                "instead of $0.10/hour ($73/month). Upgrade immediately."
                if in_extended_support else None
            ),
            "eol_warning": (
                f"Cluster K8s {version} reaches end of standard support on {eol_date_str} "
                f"({days_until_eol} days). Plan upgrade to avoid 6x price increase."
                if days_until_eol is not None and 0 < days_until_eol <= 90 else None
            ),
            "price_source": "See aws_pricing.eks",
        }

    async def _enrich_nodegroup(self, cluster_name: str, ng: dict, autoscaling, ec2, cw) -> dict:
        ng_name = ng.get("nodegroupName") or ng.get("name") or "unknown-nodegroup"
        scaling = ng.get("scalingConfig", {})
        resources = ng.get("resources") or {}
        asg_names = [
            asg.get("name")
            for asg in resources.get("autoScalingGroups", [])
            if asg.get("name")
        ]
        asgs = await self._describe_asgs(autoscaling, asg_names)
        instance_ids = [
            inst.get("InstanceId")
            for asg in asgs
            for inst in asg.get("Instances", [])
            if inst.get("InstanceId")
            and inst.get("LifecycleState") in {"InService", "Pending", "Standby"}
        ]
        ec2_instances = await self._describe_instances(ec2, instance_ids)
        metrics = await self._nodegroup_metrics(cluster_name, ec2_instances, cw)
        instance_types = sorted(
            {
                str(instance.get("instance_type"))
                for instance in ec2_instances
                if instance.get("instance_type")
            }
            or set(ng.get("instanceTypes") or [])
        )
        target_types = self._graviton_targets(instance_types)
        finops_profiles = self._nodegroup_profiles(
            cluster_name=cluster_name,
            ng_name=ng_name,
            nodegroup=ng,
            instance_types=instance_types,
            target_types=target_types,
            instance_count=len(ec2_instances),
            metrics=metrics,
        )

        return {
            "name": ng_name,
            "arn": ng.get("nodegroupArn"),
            "status": ng.get("status"),
            "version": ng.get("version"),
            "release_version": ng.get("releaseVersion"),
            "instance_types": instance_types,
            "graviton_target_types": target_types,
            "ami_type": ng.get("amiType"),
            "capacity_type": ng.get("capacityType"),
            "disk_size_gb": ng.get("diskSize"),
            "node_role": ng.get("nodeRole"),
            "subnets": ng.get("subnets", []),
            "labels": ng.get("labels", {}),
            "taints": ng.get("taints", []),
            "launch_template": ng.get("launchTemplate"),
            "remote_access": ng.get("remoteAccess"),
            "health_issues": (ng.get("health") or {}).get("issues", []),
            "auto_scaling_groups": [
                {
                    "name": asg.get("AutoScalingGroupName"),
                    "min_size": asg.get("MinSize"),
                    "max_size": asg.get("MaxSize"),
                    "desired_capacity": asg.get("DesiredCapacity"),
                    "enabled_metrics": [m.get("Metric") for m in asg.get("EnabledMetrics", [])],
                }
                for asg in asgs
            ],
            "instances": ec2_instances,
            "instance_count": len(ec2_instances),
            "min_size": scaling.get("minSize", 0),
            "max_size": scaling.get("maxSize", 0),
            "desired_size": scaling.get("desiredSize", 0),
            "metrics_30d": metrics,
            "finops_profiles": finops_profiles,
        }

    async def _describe_asgs(self, autoscaling, asg_names: list[str]) -> list[dict]:
        if not asg_names:
            return []
        response = await self._safe_call(
            autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=asg_names)
        )
        if not response or response.get("_error"):
            return []
        return response.get("AutoScalingGroups", [])

    async def _describe_instances(self, ec2, instance_ids: list[str]) -> list[dict]:
        if not instance_ids:
            return []
        response = await self._safe_call(ec2.describe_instances(InstanceIds=instance_ids))
        if not response or response.get("_error"):
            return []
        instances = []
        for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                instances.append(
                    {
                        "instance_id": instance.get("InstanceId"),
                        "instance_type": instance.get("InstanceType"),
                        "state": (instance.get("State") or {}).get("Name"),
                        "availability_zone": (instance.get("Placement") or {}).get("AvailabilityZone"),
                        "lifecycle": instance.get("InstanceLifecycle") or "on-demand",
                        "launch_time": str(instance.get("LaunchTime")),
                        "image_id": instance.get("ImageId"),
                        "private_ip": instance.get("PrivateIpAddress"),
                        "tags": {
                            tag.get("Key"): tag.get("Value")
                            for tag in instance.get("Tags", [])
                            if tag.get("Key")
                        },
                    }
                )
        return instances

    async def _nodegroup_metrics(self, cluster_name: str, instances: list[dict], cw) -> dict:
        instance_metrics = []
        for instance in instances[:50]:
            instance_id = instance.get("instance_id")
            if not instance_id:
                continue
            metrics = {
                "instance_id": instance_id,
                "ec2": {
                    "cpu_utilization": await self._metric_stats(
                        cw,
                        namespace="AWS/EC2",
                        metric_name="CPUUtilization",
                        dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                        statistic="Average",
                        extended=True,
                    ),
                    "network_in_bytes": await self._metric_stats(
                        cw,
                        namespace="AWS/EC2",
                        metric_name="NetworkIn",
                        dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                        statistic="Sum",
                    ),
                    "network_out_bytes": await self._metric_stats(
                        cw,
                        namespace="AWS/EC2",
                        metric_name="NetworkOut",
                        dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                        statistic="Sum",
                    ),
                },
                "container_insights": {
                    "node_cpu_utilization": await self._metric_stats(
                        cw,
                        namespace="ContainerInsights",
                        metric_name="node_cpu_utilization",
                        dimensions=[
                            {"Name": "ClusterName", "Value": cluster_name},
                            {"Name": "InstanceId", "Value": instance_id},
                        ],
                        statistic="Average",
                        extended=True,
                    ),
                    "node_memory_utilization": await self._metric_stats(
                        cw,
                        namespace="ContainerInsights",
                        metric_name="node_memory_utilization",
                        dimensions=[
                            {"Name": "ClusterName", "Value": cluster_name},
                            {"Name": "InstanceId", "Value": instance_id},
                        ],
                        statistic="Average",
                        extended=True,
                    ),
                    "node_filesystem_utilization": await self._metric_stats(
                        cw,
                        namespace="ContainerInsights",
                        metric_name="node_filesystem_utilization",
                        dimensions=[
                            {"Name": "ClusterName", "Value": cluster_name},
                            {"Name": "InstanceId", "Value": instance_id},
                        ],
                        statistic="Average",
                        extended=True,
                    ),
                    "node_network_total_bytes": await self._metric_stats(
                        cw,
                        namespace="ContainerInsights",
                        metric_name="node_network_total_bytes",
                        dimensions=[
                            {"Name": "ClusterName", "Value": cluster_name},
                            {"Name": "InstanceId", "Value": instance_id},
                        ],
                        statistic="Average",
                    ),
                },
            }
            instance_metrics.append(metrics)

        return {
            "lookback_days": LOOKBACK_DAYS,
            "instances_sampled": len(instance_metrics),
            "ec2_cpu": self._aggregate_metric(instance_metrics, "ec2", "cpu_utilization"),
            "ec2_network_in_bytes": self._aggregate_metric(instance_metrics, "ec2", "network_in_bytes"),
            "ec2_network_out_bytes": self._aggregate_metric(instance_metrics, "ec2", "network_out_bytes"),
            "node_cpu_utilization": self._aggregate_metric(instance_metrics, "container_insights", "node_cpu_utilization"),
            "node_memory_utilization": self._aggregate_metric(instance_metrics, "container_insights", "node_memory_utilization"),
            "node_filesystem_utilization": self._aggregate_metric(instance_metrics, "container_insights", "node_filesystem_utilization"),
            "node_network_total_bytes": self._aggregate_metric(instance_metrics, "container_insights", "node_network_total_bytes"),
            "container_insights_available": any(
                (m.get("container_insights") or {}).get("node_memory_utilization", {}).get("datapoints", 0) > 0
                for m in instance_metrics
            ),
            "per_instance": instance_metrics,
        }

    async def _metric_stats(
        self,
        cw,
        *,
        namespace: str,
        metric_name: str,
        dimensions: list[dict],
        statistic: str,
        extended: bool = False,
    ) -> dict:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=LOOKBACK_DAYS)
        kwargs = {
            "Namespace": namespace,
            "MetricName": metric_name,
            "Dimensions": dimensions,
            "StartTime": start,
            "EndTime": end,
            "Period": 86400,
            "Statistics": [statistic],
        }
        response = await self._safe_call(cw.get_metric_statistics(**kwargs))
        if not response or response.get("_error"):
            return {"datapoints": 0}
        values = []
        for datapoint in response.get("Datapoints", []):
            if statistic in datapoint:
                values.append(float(datapoint[statistic]))
        if not values:
            return {"datapoints": 0}
        p95_values = await self._metric_p95(
            cw,
            namespace=namespace,
            metric_name=metric_name,
            dimensions=dimensions,
            start=start,
            end=end,
        ) if extended else []
        return {
            "datapoints": len(values),
            "avg": round(sum(values) / len(values), 2),
            "max": round(max(values), 2),
            "p95": round(max(p95_values), 2) if p95_values else None,
        }

    async def _metric_p95(
        self,
        cw,
        *,
        namespace: str,
        metric_name: str,
        dimensions: list[dict],
        start: datetime,
        end: datetime,
    ) -> list[float]:
        response = await self._safe_call(
            cw.get_metric_statistics(
                Namespace=namespace,
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

    def _aggregate_metric(self, instance_metrics: list[dict], group: str, metric_name: str) -> dict:
        metric_rows = [
            (item.get(group) or {}).get(metric_name) or {}
            for item in instance_metrics
        ]
        with_data = [row for row in metric_rows if row.get("datapoints", 0) > 0]
        if not with_data:
            return {"datapoints": 0}
        return {
            "instances_with_data": len(with_data),
            "avg": round(sum(float(row.get("avg") or 0) for row in with_data) / len(with_data), 2),
            "max": round(max(float(row.get("max") or 0) for row in with_data), 2),
            "p95": round(max(float(row.get("p95") or 0) for row in with_data if row.get("p95") is not None), 2)
            if any(row.get("p95") is not None for row in with_data)
            else None,
        }

    def _cluster_profiles(
        self,
        *,
        cluster_name: str,
        version: str,
        in_extended_support: bool,
        days_until_eol: int | None,
        eol_date_str: str | None,
    ) -> list[dict]:
        if in_extended_support:
            return [{
                "classification": "EKS_EXTENDED_SUPPORT_UPGRADE",
                "confidence": "HIGH",
                "risk": "LOW",
                "evidence": [
                    f"Cluster {cluster_name} is running Kubernetes {version}",
                    f"Standard support ended on {eol_date_str}",
                    "EKS extended support control plane pricing is materially higher than standard support.",
                ],
                "safe_actions": ["Plan and execute an EKS version upgrade before continuing compute optimization."],
                "unsafe_actions": ["force_upgrade_without_addon_and_workload_compatibility_validation"],
                "candidate_monthly_savings": 365.0,
                "savings_treatment": "cost_avoidance",
                "llm_guidance": "EKS extended support cost avoidance. Confirm current AWS EKS support pricing before quoting final savings.",
            }]
        if days_until_eol is not None and 0 < days_until_eol <= 90:
            return [{
                "classification": "EKS_VERSION_UPGRADE_PLANNING",
                "confidence": "HIGH",
                "risk": "LOW",
                "evidence": [
                    f"Cluster {cluster_name} is running Kubernetes {version}",
                    f"Standard support EOL is {eol_date_str} ({days_until_eol} days).",
                ],
                "warnings": ["This is cost avoidance, not immediate savings."],
                "safe_actions": ["Schedule EKS version upgrade and validate managed add-ons before EOL."],
                "unsafe_actions": ["ignore_eol_until_extended_support_charges_start"],
                "candidate_monthly_savings": 0.0,
                "savings_treatment": "cost_avoidance",
                "llm_guidance": "EKS version lifecycle planning. Explain the avoided extended support charge risk.",
            }]
        return []

    def _nodegroup_profiles(
        self,
        *,
        cluster_name: str,
        ng_name: str,
        nodegroup: dict,
        instance_types: list[str],
        target_types: list[str],
        instance_count: int,
        metrics: dict,
    ) -> list[dict]:
        profiles = []
        capacity_type = str(nodegroup.get("capacityType") or "ON_DEMAND")
        scaling = nodegroup.get("scalingConfig", {})
        desired = int(scaling.get("desiredSize") or 0)
        minimum = int(scaling.get("minSize") or 0)
        maximum = int(scaling.get("maxSize") or 0)
        cpu_p95 = float((metrics.get("node_cpu_utilization") or {}).get("p95") or (metrics.get("ec2_cpu") or {}).get("p95") or 0)
        memory_p95 = float((metrics.get("node_memory_utilization") or {}).get("p95") or 0)
        ci_available = bool(metrics.get("container_insights_available"))

        if target_types and self._is_x86_ami(nodegroup.get("amiType")):
            profiles.append({
                "classification": "EKS_GRAVITON_REVIEW",
                "confidence": "MEDIUM",
                "risk": "MEDIUM",
                "evidence": [
                    f"Nodegroup {cluster_name}/{ng_name}",
                    f"Current instance types: {', '.join(instance_types) or 'unknown'}",
                    f"Potential Graviton target types: {', '.join(target_types)}",
                    f"Capacity type: {capacity_type}; desired nodes: {desired}",
                ],
                "warnings": [
                    "Requires container image architecture compatibility and add-on validation.",
                    "Validate daemonsets, CNI/add-ons and workload node selectors before migration.",
                ],
                "safe_actions": ["Create a parallel ARM64 nodegroup, cordon/drain gradually, and validate workload compatibility."],
                "unsafe_actions": ["replace_nodegroup_in_place_without_multi_arch_image_validation"],
                "candidate_monthly_savings": 0.0,
                "savings_treatment": "migration_upside_not_core_potential",
                "cost_estimate_status": "requires_ec2_node_pricing_match",
                "llm_guidance": "EKS Graviton review. Do not claim savings unless pricing is available for current and target node instance types.",
            })

        if capacity_type == "ON_DEMAND" and desired >= 2:
            profiles.append({
                "classification": "EKS_SPOT_REVIEW",
                "confidence": "MEDIUM",
                "risk": "MEDIUM",
                "evidence": [
                    f"Nodegroup {cluster_name}/{ng_name} runs {desired} desired On-Demand node(s).",
                    f"Scaling range min={minimum}, max={maximum}; instance count observed={instance_count}.",
                    f"Instance types: {', '.join(instance_types) or 'unknown'}",
                ],
                "warnings": ["Only apply to interruption-tolerant workloads with PodDisruptionBudgets and mixed capacity strategy."],
                "safe_actions": ["Introduce a separate Spot nodegroup for stateless/tolerant workloads and migrate selectively."],
                "unsafe_actions": ["move_stateful_or_single_replica_workloads_to_spot_without_pdb"],
                "candidate_monthly_savings": 0.0,
                "savings_treatment": "cost_review",
                "llm_guidance": "EKS Spot review. Recommend only for stateless/tolerant workloads; ask for workload criticality if unknown.",
            })

        if ci_available and desired > minimum and cpu_p95 > 0 and memory_p95 > 0 and cpu_p95 < 35 and memory_p95 < 55:
            profiles.append({
                "classification": "EKS_NODEGROUP_RIGHTSIZE_REVIEW",
                "confidence": "MEDIUM",
                "risk": "MEDIUM",
                "evidence": [
                    f"Nodegroup {cluster_name}/{ng_name} has desired={desired}, min={minimum}, max={maximum}.",
                    f"30d p95 node CPU {cpu_p95:.1f}%, p95 memory {memory_p95:.1f}%.",
                    "Container Insights node metrics were available.",
                ],
                "warnings": ["Validate pod requests/limits, HPA behavior, scheduled jobs and peak windows before scaling down."],
                "safe_actions": ["Review reducing desired/min capacity or moving workloads to smaller node types in a canary nodegroup."],
                "unsafe_actions": ["reduce_capacity_below_pod_requests_or_without_peak_validation"],
                "candidate_monthly_savings": 0.0,
                "savings_treatment": "cost_review",
                "llm_guidance": "EKS rightsizing review with Container Insights. Use workload peaks and pod requests before recommending an exact downsize.",
            })
        elif not ci_available:
            profiles.append({
                "classification": "EKS_INSUFFICIENT_NODE_METRICS",
                "confidence": "LOW",
                "risk": "MEDIUM",
                "evidence": [
                    f"Nodegroup {cluster_name}/{ng_name} has EC2 mapping but no Container Insights node memory/filesystem metrics.",
                    f"EC2 CPU p95 observed: {cpu_p95:.1f}%" if cpu_p95 else "EC2 CPU p95 unavailable.",
                ],
                "warnings": ["Memory and pod request data are required for safe Kubernetes rightsizing."],
                "safe_actions": ["Enable CloudWatch Container Insights or export Kubernetes metrics before nodegroup downsizing."],
                "unsafe_actions": ["rightsize_eks_nodes_from_cpu_only"],
                "candidate_monthly_savings": 0.0,
                "savings_treatment": "observability_gap",
                "llm_guidance": "Missing EKS node memory/pod metrics. Ask for Container Insights before concrete rightsizing.",
            })

        return profiles

    def _graviton_targets(self, instance_types: list[str]) -> list[str]:
        targets = []
        for instance_type in instance_types:
            if "." not in instance_type or instance_type.endswith("g"):
                continue
            family, size = instance_type.split(".", 1)
            target_family = GRAVITON_FAMILY_TARGETS.get(family)
            if target_family:
                targets.append(f"{target_family}.{size}")
        return sorted(set(targets))

    def _is_x86_ami(self, ami_type: str | None) -> bool:
        if not ami_type:
            return True
        normalized = ami_type.upper()
        if "ARM_64" in normalized or "ARM64" in normalized:
            return False
        return any(marker in normalized for marker in NON_GRAVITON_AMI_MARKERS) or "X86" in normalized
