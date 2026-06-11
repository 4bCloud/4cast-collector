"""
DynamoDB Collector — READ-ONLY.

DynamoDB cost analysis:
- Provisioned capacity vs actual consumption (overprovisioned = waste)
- On-Demand vs Provisioned mode recommendation
- Tables without TTL (data grows forever)
- Global tables (replicated = N× the cost)
- Unused indexes consuming capacity

IAM required: dynamodb:ListTables, dynamodb:DescribeTable,
              dynamodb:ListGlobalTables,
              cloudwatch:GetMetricStatistics (already in policy)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

# Utilization below this threshold = overprovisioned
UTILIZATION_THRESHOLD = 30  # % consumed vs provisioned


class DynamoDBCollector(BaseCollector):
    name = "dynamodb"

    async def collect(self) -> dict:
        tables = []

        async with self.session.client("dynamodb") as ddb:
            async with self.session.client("cloudwatch") as cw:
                paginator = ddb.get_paginator("list_tables")
                async for page in paginator.paginate():
                    for table_name in page.get("TableNames", []):
                        enriched = await self._enrich_table(table_name, ddb, cw)
                        if enriched:
                            tables.append(enriched)

        provisioned_tables    = [t for t in tables if t.get("billing_mode") == "PROVISIONED"]
        overprovisioned       = [t for t in provisioned_tables if t.get("is_overprovisioned")]
        on_demand_candidates  = [t for t in provisioned_tables if t.get("suggest_on_demand")]
        no_ttl_tables         = [t for t in tables if not t.get("has_ttl")]

        return {
            "tables":                tables,
            "total_tables":          len(tables),
            "provisioned_tables":    provisioned_tables,
            "overprovisioned":       overprovisioned,
            "on_demand_candidates":  on_demand_candidates,
            "tables_without_ttl":   no_ttl_tables,
            "price_source":          "See aws_pricing.dynamodb",
            "cost_note": (
                "DynamoDB: Provisioned = fixed RCU/WCU charge. "
                "On-Demand = pay per request (more expensive at high scale, "
                "cheaper at low/variable scale). "
                "Global Tables replicate to N regions = N× the cost. "
                "TTL automatically deletes expired items at no charge."
            ),
        }

    async def _enrich_table(self, table_name: str, ddb, cw) -> dict | None:
        resp = await self._safe_call(ddb.describe_table(TableName=table_name))
        if not resp or resp.get("_error"):
            return None

        table  = resp.get("Table", {})
        billing = table.get("BillingModeSummary", {})
        billing_mode = billing.get("BillingMode", "PROVISIONED")

        # Provisioned capacity
        provisioned_rcu = 0
        provisioned_wcu = 0
        if billing_mode == "PROVISIONED":
            throughput      = table.get("ProvisionedThroughput", {})
            provisioned_rcu = throughput.get("ReadCapacityUnits", 0)
            provisioned_wcu = throughput.get("WriteCapacityUnits", 0)

        # TTL configuration
        ttl_resp = await self._safe_call(ddb.describe_time_to_live(TableName=table_name))
        has_ttl  = False
        if ttl_resp and not ttl_resp.get("_error"):
            ttl_status = ttl_resp.get("TimeToLiveDescription", {}).get("TimeToLiveStatus", "")
            has_ttl    = ttl_status == "ENABLED"

        # CloudWatch — actual consumption vs provisioned
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        consumed_rcu_avg = 0
        consumed_wcu_avg = 0

        for metric, attr in [
            ("ConsumedReadCapacityUnits", "consumed_rcu_avg"),
            ("ConsumedWriteCapacityUnits", "consumed_wcu_avg"),
        ]:
            cw_resp = await self._safe_call(
                cw.get_metric_statistics(
                    Namespace="AWS/DynamoDB",
                    MetricName=metric,
                    Dimensions=[{"Name": "TableName", "Value": table_name}],
                    StartTime=start,
                    EndTime=end,
                    Period=86400,
                    Statistics=["Average"],
                )
            )
            if cw_resp and not cw_resp.get("_error"):
                datapoints = cw_resp.get("Datapoints", [])
                if datapoints:
                    avg = sum(d["Average"] for d in datapoints) / len(datapoints)
                    if attr == "consumed_rcu_avg":
                        consumed_rcu_avg = avg
                    else:
                        consumed_wcu_avg = avg

        # Overprovisioning analysis
        is_overprovisioned = False
        suggest_on_demand  = False
        rcu_utilization    = None
        wcu_utilization    = None

        if billing_mode == "PROVISIONED" and provisioned_rcu > 0:
            rcu_utilization    = (consumed_rcu_avg / provisioned_rcu * 100) if provisioned_rcu else 0
            wcu_utilization    = (consumed_wcu_avg / provisioned_wcu * 100) if provisioned_wcu else 0
            is_overprovisioned = (
                rcu_utilization < UTILIZATION_THRESHOLD and
                wcu_utilization < UTILIZATION_THRESHOLD
            )
            # If usage is very low and irregular, On-Demand may be cheaper
            suggest_on_demand  = (
                rcu_utilization < 10 and wcu_utilization < 10
            )

        tags = {t["Key"]: t["Value"] for t in table.get("Tags", []) if "Key" in t}

        return {
            "table_name":          table_name,
            "table_status":        table.get("TableStatus"),
            "billing_mode":        billing_mode,
            "item_count":          table.get("ItemCount", 0),
            "size_bytes":          table.get("TableSizeBytes", 0),
            "size_gb":             round(table.get("TableSizeBytes", 0) / 1024**3, 4),
            "global_table":        bool(table.get("GlobalTableVersion")),
            "replicas":            len(table.get("Replicas", [])),
            "gsi_count":           len(table.get("GlobalSecondaryIndexes", [])),
            "provisioned_rcu":     provisioned_rcu,
            "provisioned_wcu":     provisioned_wcu,
            "consumed_rcu_avg":    round(consumed_rcu_avg, 2),
            "consumed_wcu_avg":    round(consumed_wcu_avg, 2),
            "rcu_utilization_pct": round(rcu_utilization, 1) if rcu_utilization is not None else None,
            "wcu_utilization_pct": round(wcu_utilization, 1) if wcu_utilization is not None else None,
            "is_overprovisioned":  is_overprovisioned,
            "suggest_on_demand":   suggest_on_demand,
            "has_ttl":             has_ttl,
            "tags":                tags,
            "price_source":        "See aws_pricing.dynamodb",
        }
