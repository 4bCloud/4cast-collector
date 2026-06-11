"""Amazon Timestream Collector — READ-ONLY."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class TimestreamCollector(BaseCollector):
    name = "timestream"

    async def collect(self) -> dict:
        try:
            async with self.session.client("timestream-write") as ts:
                async with self.session.client("cloudwatch") as cw:
                    databases = await self._collect_databases(ts, cw)
        except Exception as exc:
            return {"_error": str(exc), "databases": [], "tables": []}
        tables=[t for d in databases for t in d.get("tables", [])]
        return {"databases": databases, "tables": tables, "total_tables": len(tables), "price_source": "See aws_pricing.timestream"}

    async def _collect_databases(self, ts: Any, cw: Any) -> list[dict]:
        dbs=[]
        paginator=ts.get_paginator("list_databases")
        async for page in paginator.paginate():
            for db in page.get("Databases", []):
                name=db.get("DatabaseName")
                tables=[]
                try:
                    tp=ts.get_paginator("list_tables")
                    async for tpage in tp.paginate(DatabaseName=name):
                        for t in tpage.get("Tables", []):
                            tables.append(await self._enrich_table(ts,cw,name,t.get("TableName"),t))
                except Exception:
                    pass
                dbs.append({"database_name": name, "arn": db.get("Arn"), "kms_key_id": db.get("KmsKeyId"), "tables": tables})
        return dbs

    async def _enrich_table(self, ts: Any, cw: Any, db: str, table: str | None, raw: dict) -> dict:
        if not table: return {}
        desc=await self._safe_call(ts.describe_table(DatabaseName=db, TableName=table))
        table_obj=(desc or {}).get("Table", {}) if not (desc or {}).get("_error") else raw
        retention=table_obj.get("RetentionProperties", {})
        writes=await self._metric_avg(cw,"SuccessfulRequestLatency",db,table)
        return {"database_name": db, "table_name": table, "arn": table_obj.get("Arn"), "status": table_obj.get("TableStatus"), "memory_store_retention_hours": retention.get("MemoryStoreRetentionPeriodInHours"), "magnetic_store_retention_days": retention.get("MagneticStoreRetentionPeriodInDays"), "metrics_30d": {"request_latency_avg": writes}, "price_source": "See aws_pricing.timestream"}

    async def _metric_avg(self,cw:Any,metric:str,db:str,table:str)->float|None:
        end=datetime.now(timezone.utc); start=end-timedelta(days=30)
        resp=await self._safe_call(cw.get_metric_statistics(Namespace="AWS/Timestream", MetricName=metric, Dimensions=[{"Name":"DatabaseName","Value":db},{"Name":"TableName","Value":table}], StartTime=start, EndTime=end, Period=86400, Statistics=["Average"]))
        if not resp or resp.get("_error"): return None
        vals=[p.get("Average") for p in resp.get("Datapoints",[]) if p.get("Average") is not None]
        return round(sum(vals)/len(vals),2) if vals else None
