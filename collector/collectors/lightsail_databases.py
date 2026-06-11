"""Amazon Lightsail managed databases Collector — READ-ONLY."""
from __future__ import annotations

from datetime import datetime, timezone

from collector.collectors.base import BaseCollector


class LightsailDatabasesCollector(BaseCollector):
    name = "lightsail_databases"

    async def collect(self) -> dict:
        databases=[]; snapshots=[]
        try:
            async with self.session.client("lightsail") as ls:
                resp=await self._safe_call(ls.get_relational_databases())
                if resp and not resp.get("_error"):
                    for db in resp.get("relationalDatabases", []):
                        databases.append({"name": db.get("name"), "arn": db.get("arn"), "engine": db.get("engine"), "engine_version": db.get("engineVersion"), "state": db.get("state"), "bundle_id": db.get("relationalDatabaseBundleId"), "master_database_name": db.get("masterDatabaseName"), "backup_retention_enabled": db.get("backupRetentionEnabled"), "publicly_accessible": db.get("publiclyAccessible"), "price_source": "See aws_pricing.lightsail_databases"})
                sresp=await self._safe_call(ls.get_relational_database_snapshots())
                if sresp and not sresp.get("_error"):
                    for s in sresp.get("relationalDatabaseSnapshots", []):
                        created=s.get("createdAt")
                        age=(datetime.now(timezone.utc)-created).days if created else None
                        snapshots.append({"snapshot_name": s.get("name"), "database_name": s.get("fromRelationalDatabaseName"), "engine": s.get("engine"), "state": s.get("state"), "age_days": age, "created_at": created.isoformat() if hasattr(created,"isoformat") else None, "estimated_monthly_cost": None, "cost_estimate_status": "unavailable"})
        except Exception as exc:
            return {"_error": str(exc), "databases": [], "snapshots": []}
        old=[s for s in snapshots if (s.get("age_days") or 0)>=90]
        return {"databases": databases, "total_databases": len(databases), "snapshots": snapshots, "old_snapshots_90d": old, "price_source": "See aws_pricing.lightsail_databases"}
