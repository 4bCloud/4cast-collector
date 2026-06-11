"""Amazon Keyspaces Collector — READ-ONLY."""

from __future__ import annotations

from collector.collectors.base import BaseCollector


class KeyspacesCollector(BaseCollector):
    name = "keyspaces"

    async def collect(self) -> dict:
        keyspaces = []
        try:
            async with self.session.client("keyspaces") as ks:
                paginator = ks.get_paginator("list_keyspaces")
                async for page in paginator.paginate():
                    for k in page.get("keyspaces", []):
                        name = k.get("keyspaceName")
                        # Skip internal Cassandra/Keyspaces system keyspaces —
                        # they are not customer resources and inflate the payload.
                        if name in {
                            "system_schema",
                            "system",
                            "system_auth",
                            "system_distributed",
                            "system_multiregion_info",
                        }:
                            continue
                        tables = []
                        try:
                            tp = ks.get_paginator("list_tables")
                            async for tpage in tp.paginate(keyspaceName=name):
                                for t in tpage.get("tables", []):
                                    tables.append(
                                        await self._enrich_table(ks, name, t.get("tableName"), t)
                                    )
                        except Exception:
                            pass
                        keyspaces.append(
                            {
                                "keyspace_name": name,
                                "resource_arn": k.get("resourceArn"),
                                "tables": tables,
                            }
                        )
        except Exception as exc:
            return {"_error": str(exc), "keyspaces": [], "tables": []}
        tables = [t for k in keyspaces for t in k.get("tables", [])]
        return {
            "keyspaces": keyspaces,
            "tables": tables,
            "total_tables": len(tables),
            "price_source": "See aws_pricing.keyspaces",
        }

    async def _enrich_table(self, ks, keyspace: str, table: str | None, raw: dict) -> dict:
        if not table:
            return {}
        resp = await self._safe_call(ks.get_table(keyspaceName=keyspace, tableName=table))
        obj = resp if resp and not resp.get("_error") else raw
        throughput = (
            obj.get("capacitySpecificationSummary") or obj.get("capacitySpecification") or {}
        )
        pitr = obj.get("pointInTimeRecovery") or obj.get("pointInTimeRecoverySummary") or {}
        return {
            "keyspace_name": keyspace,
            "table_name": table,
            "resource_arn": obj.get("resourceArn"),
            "status": obj.get("status"),
            "capacity_mode": throughput.get("throughputMode"),
            "read_capacity_units": throughput.get("readCapacityUnits"),
            "write_capacity_units": throughput.get("writeCapacityUnits"),
            "pitr_status": pitr.get("status"),
            "ttl_status": (obj.get("ttl") or {}).get("status"),
            "price_source": "See aws_pricing.keyspaces",
        }
