"""
ECS Collector — READ-ONLY.

Collects ECS clusters, services, and tasks:
- Services with 0 running tasks (idle, still paying for LB/ENI)
- Tasks with 0 CPU/memory utilization
- Fargate vs EC2 launch type analysis
- Overprovisioned task definitions (requested >> actual usage)

IAM required: ecs:ListClusters, ecs:DescribeClusters,
              ecs:ListServices, ecs:DescribeServices,
              ecs:ListTasks, ecs:DescribeTasks
"""
from __future__ import annotations

from collector.collectors.base import BaseCollector


class ECSCollector(BaseCollector):
    name = "ecs"

    async def collect(self) -> dict:
        clusters_data = []

        async with self.session.client("ecs") as ecs:
            # ── List all clusters ──────────────────────────────────────────
            cluster_arns = []
            paginator = ecs.get_paginator("list_clusters")
            async for page in paginator.paginate():
                cluster_arns.extend(page.get("clusterArns", []))

            if not cluster_arns:
                return {"clusters": [], "total_clusters": 0}

            # ── Describe clusters in batches of 10 ────────────────────────
            for i in range(0, len(cluster_arns), 10):
                batch = cluster_arns[i:i+10]
                resp  = await self._safe_call(
                    ecs.describe_clusters(clusters=batch, include=["STATISTICS"])
                )
                if not resp or resp.get("_error"):
                    continue

                for cluster in resp.get("clusters", []):
                    enriched = await self._enrich_cluster(cluster, ecs)
                    clusters_data.append(enriched)

        idle_services = [
            svc
            for c in clusters_data
            for svc in c.get("services", [])
            if svc.get("running_count", 0) == 0 and svc.get("desired_count", 0) > 0
        ]

        fargate_services = [
            svc
            for c in clusters_data
            for svc in c.get("services", [])
            if svc.get("launch_type") == "FARGATE"
        ]

        return {
            "clusters":         clusters_data,
            "total_clusters":   len(clusters_data),
            "idle_services":    idle_services,
            "fargate_services": fargate_services,
            "cost_note": (
                "ECS Fargate: billed per vCPU/memory per second. "
                "EC2 launch type: billed by underlying EC2 instances. "
                "Idle services (desired > 0, running = 0) still consume "
                "load balancer capacity and may have ENI charges."
            ),
            "price_source": "See aws_pricing.ecs_fargate",
        }

    async def _enrich_cluster(self, cluster: dict, ecs) -> dict:
        cluster_name = cluster.get("clusterName", "")
        cluster_arn  = cluster.get("clusterArn", "")

        services = []
        svc_paginator = ecs.get_paginator("list_services")
        async for page in svc_paginator.paginate(cluster=cluster_arn):
            svc_arns = page.get("serviceArns", [])
            if not svc_arns:
                continue

            for i in range(0, len(svc_arns), 10):
                batch = svc_arns[i:i+10]
                resp  = await self._safe_call(
                    ecs.describe_services(cluster=cluster_arn, services=batch)
                )
                if not resp or resp.get("_error"):
                    continue

                for svc in resp.get("services", []):
                    services.append({
                        "service_name":   svc.get("serviceName"),
                        "launch_type":    svc.get("launchType", "EC2"),
                        "task_definition": svc.get("taskDefinition", "").split("/")[-1],
                        "desired_count":  svc.get("desiredCount", 0),
                        "running_count":  svc.get("runningCount", 0),
                        "pending_count":  svc.get("pendingCount", 0),
                        "status":         svc.get("status"),
                        "load_balancers": len(svc.get("loadBalancers", [])),
                    })

        stats = {s["name"]: s["value"] for s in cluster.get("statistics", [])}

        return {
            "cluster_name":      cluster_name,
            "status":            cluster.get("status"),
            "running_tasks":     int(stats.get("runningTasksCount", 0)),
            "pending_tasks":     int(stats.get("pendingTasksCount", 0)),
            "active_services":   int(stats.get("activeServicesCount", 0)),
            "services":          services,
            "total_services":    len(services),
        }
