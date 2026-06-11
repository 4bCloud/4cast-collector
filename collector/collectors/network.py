"""Network Collector — READ-ONLY. NAT, EIP, VPC endpoint and Lambda signals."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from collector.collectors.base import BaseCollector


class NetworkCollector(BaseCollector):
    name = "network"

    async def collect(self) -> dict:
        nat_gateways = await self._collect_nat_gateways()
        elastic_ips = await self._collect_elastic_ips()
        vpc_endpoints = await self._collect_vpc_endpoints()
        route_tables = await self._collect_route_tables()
        subnets = await self._collect_subnets()
        idle_lambdas = await self._collect_idle_lambdas()

        return {
            "nat_gateways": nat_gateways,
            "idle_nat_gateways": [
                n for n in nat_gateways if n.get("idle_candidate")
            ],
            "elastic_ips": elastic_ips,
            "unassociated_elastic_ips": [
                e for e in elastic_ips if not e.get("association_id")
            ],
            "vpc_endpoints": vpc_endpoints,
            "route_tables": route_tables,
            "subnets": subnets,
            "idle_load_balancers": [],
            "idle_lambdas": idle_lambdas,
        }

    async def _collect_nat_gateways(self) -> list[dict]:
        nat_gateways = []
        async with self.session.client("ec2") as ec2:
            async with self.session.client("cloudwatch") as cw:
                paginator = ec2.get_paginator("describe_nat_gateways")
                async for page in paginator.paginate(
                    Filters=[{"Name": "state", "Values": ["available"]}]
                ):
                    for ngw in page.get("NatGateways", []):
                        ngw_id = ngw["NatGatewayId"]
                        end = datetime.now(timezone.utc)
                        start = end - timedelta(days=30)
                        bytes_out = await self._sum_metric(
                            cw,
                            namespace="AWS/NATGateway",
                            metric_name="BytesOutToDestination",
                            dimensions=[{"Name": "NatGatewayId", "Value": ngw_id}],
                            start=start,
                            end=end,
                        )
                        bytes_in = await self._sum_metric(
                            cw,
                            namespace="AWS/NATGateway",
                            metric_name="BytesInFromSource",
                            dimensions=[{"Name": "NatGatewayId", "Value": ngw_id}],
                            start=start,
                            end=end,
                        )
                        tags = {t["Key"]: t["Value"] for t in ngw.get("Tags", [])}
                        total_bytes = bytes_out + bytes_in
                        nat_gateways.append({
                            "nat_gateway_id": ngw_id,
                            "vpc_id": ngw.get("VpcId"),
                            "subnet_id": ngw.get("SubnetId"),
                            "state": ngw.get("State"),
                            "connectivity_type": ngw.get("ConnectivityType"),
                            "bytes_out_to_destination_30d": bytes_out,
                            "bytes_in_from_source_30d": bytes_in,
                            "bytes_processed_30d": total_bytes,
                            "gb_processed_30d": round(total_bytes / (1024 ** 3), 4),
                            "idle_candidate": total_bytes < 1_000_000,
                            "name": tags.get("Name"),
                            "tags": tags,
                        })
        return nat_gateways

    async def _collect_elastic_ips(self) -> list[dict]:
        async with self.session.client("ec2") as ec2:
            result = await self._safe_call(ec2.describe_addresses())
            if not result or "_error" in result:
                return []
            return [
                {
                    "allocation_id": addr.get("AllocationId"),
                    "association_id": addr.get("AssociationId"),
                    "instance_id": addr.get("InstanceId"),
                    "network_interface_id": addr.get("NetworkInterfaceId"),
                    "public_ip": addr.get("PublicIp"),
                    "domain": addr.get("Domain"),
                }
                for addr in result.get("Addresses", [])
            ]

    async def _collect_vpc_endpoints(self) -> list[dict]:
        async with self.session.client("ec2") as ec2:
            paginator = ec2.get_paginator("describe_vpc_endpoints")
            endpoints = []
            async for page in paginator.paginate():
                for ep in page.get("VpcEndpoints", []):
                    endpoints.append({
                        "vpc_endpoint_id": ep.get("VpcEndpointId"),
                        "vpc_id": ep.get("VpcId"),
                        "service_name": ep.get("ServiceName"),
                        "vpc_endpoint_type": ep.get("VpcEndpointType"),
                        "state": ep.get("State"),
                        "route_table_ids": ep.get("RouteTableIds", []),
                        "subnet_ids": ep.get("SubnetIds", []),
                        "private_dns_enabled": ep.get("PrivateDnsEnabled"),
                    })
            return endpoints

    async def _collect_route_tables(self) -> list[dict]:
        async with self.session.client("ec2") as ec2:
            paginator = ec2.get_paginator("describe_route_tables")
            route_tables = []
            async for page in paginator.paginate():
                for rt in page.get("RouteTables", []):
                    tags = {t["Key"]: t["Value"] for t in rt.get("Tags", [])}
                    route_tables.append({
                        "route_table_id": rt.get("RouteTableId"),
                        "vpc_id": rt.get("VpcId"),
                        "name": tags.get("Name"),
                        "associations": [
                            {
                                "subnet_id": a.get("SubnetId"),
                                "main": a.get("Main", False),
                            }
                            for a in rt.get("Associations", [])
                        ],
                        "nat_gateway_routes": [
                            {
                                "destination_cidr_block": r.get("DestinationCidrBlock"),
                                "destination_ipv6_cidr_block": r.get("DestinationIpv6CidrBlock"),
                                "nat_gateway_id": r.get("NatGatewayId"),
                                "state": r.get("State"),
                            }
                            for r in rt.get("Routes", [])
                            if r.get("NatGatewayId")
                        ],
                        "gateway_endpoint_ids": [
                            r.get("GatewayId")
                            for r in rt.get("Routes", [])
                            if str(r.get("GatewayId") or "").startswith("vpce-")
                        ],
                    })
            return route_tables

    async def _collect_subnets(self) -> list[dict]:
        async with self.session.client("ec2") as ec2:
            paginator = ec2.get_paginator("describe_subnets")
            subnets = []
            async for page in paginator.paginate():
                for subnet in page.get("Subnets", []):
                    tags = {t["Key"]: t["Value"] for t in subnet.get("Tags", [])}
                    subnets.append({
                        "subnet_id": subnet.get("SubnetId"),
                        "vpc_id": subnet.get("VpcId"),
                        "availability_zone": subnet.get("AvailabilityZone"),
                        "available_ip_address_count": subnet.get("AvailableIpAddressCount"),
                        "default_for_az": subnet.get("DefaultForAz"),
                        "map_public_ip_on_launch": subnet.get("MapPublicIpOnLaunch"),
                        "name": tags.get("Name"),
                    })
            return subnets

    async def _collect_idle_elbs(self) -> list[dict]:
        idle = []
        async with self.session.client("elbv2") as elbv2:
            paginator = elbv2.get_paginator("describe_load_balancers")
            async for page in paginator.paginate():
                for lb in page.get("LoadBalancers", []):
                    lb_arn = lb["LoadBalancerArn"]
                    tg_result = await self._safe_call(
                        elbv2.describe_target_groups(LoadBalancerArn=lb_arn)
                    )
                    target_groups = tg_result.get("TargetGroups", []) if tg_result and not tg_result.get("_error") else []

                    has_healthy_targets = False
                    for tg in target_groups:
                        health = await self._safe_call(
                            elbv2.describe_target_health(TargetGroupArn=tg["TargetGroupArn"])
                        )
                        if health and not health.get("_error"):
                            if any(
                                t.get("TargetHealth", {}).get("State") == "healthy"
                                for t in health.get("TargetHealthDescriptions", [])
                            ):
                                has_healthy_targets = True
                                break

                    if not has_healthy_targets:
                        idle.append({
                            "load_balancer_arn": lb_arn,
                            "name": lb.get("LoadBalancerName"),
                            "type": lb.get("Type"),
                            "scheme": lb.get("Scheme"),
                            "dns_name": lb.get("DNSName"),
                        })
        return idle

    async def _collect_idle_lambdas(self) -> list[dict]:
        """Find Lambda functions with no invocations in the last 30 days."""
        idle = []
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)

        async with self.session.client("lambda") as lmb:
            async with self.session.client("cloudwatch") as cw:
                paginator = lmb.get_paginator("list_functions")
                async for page in paginator.paginate():
                    for fn in page.get("Functions", []):
                        fn_name = fn["FunctionName"]
                        metrics = await self._safe_call(
                            cw.get_metric_statistics(
                                Namespace="AWS/Lambda",
                                MetricName="Invocations",
                                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                                StartTime=start,
                                EndTime=end,
                                Period=2592000,
                                Statistics=["Sum"],
                            )
                        )
                        total_invocations = 0
                        if metrics and not metrics.get("_error"):
                            for dp in metrics.get("Datapoints", []):
                                total_invocations += dp.get("Sum", 0)

                        if total_invocations == 0:
                            idle.append({
                                "function_name": fn_name,
                                "runtime": fn.get("Runtime"),
                                "memory_mb": fn.get("MemorySize"),
                                "last_modified": fn.get("LastModified"),
                                "invocations_30d": 0,
                            })
        return idle

    async def _sum_metric(
        self,
        cw,
        *,
        namespace: str,
        metric_name: str,
        dimensions: list[dict],
        start: datetime,
        end: datetime,
    ) -> float:
        metrics = await self._safe_call(
            cw.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start,
                EndTime=end,
                Period=2592000,
                Statistics=["Sum"],
            )
        )
        if not metrics or metrics.get("_error"):
            return 0.0
        return float(sum(dp.get("Sum", 0) for dp in metrics.get("Datapoints", [])))
