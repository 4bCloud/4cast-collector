"""
RDS Collector — READ-ONLY.

Collects RDS instances with full cost breakdown:
- Instance compute cost
- Storage (gp2/gp3/io1) — including gp2→gp3 migration opportunity
- Provisioned IOPS cost
- Automated backup storage (free up to DB size, charged above)
- Manual snapshots (always charged)
- Multi-AZ standby (doubles storage cost)

Real cost = instance + storage + IOPS + snapshots
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

# RDS storage pricing per GB-month (approximate — enriched by pricing engine)
RDS_STORAGE_PRICE = {
    "gp2": 0.115,  # General Purpose SSD
    "gp3": 0.115,  # General Purpose SSD (newer, same price but better perf)
    "io1": 0.125,  # Provisioned IOPS SSD
    "io2": 0.125,  # Provisioned IOPS SSD
    "standard": 0.10,  # Magnetic (legacy)
}

# io1/io2 IOPS pricing per IOPS-month
RDS_IOPS_PRICE = {
    "io1": 0.10,
    "io2": 0.10,
}

# Backup storage pricing per GB-month (above free tier)
RDS_BACKUP_PRICE_PER_GB = 0.095
RDS_GRAVITON_CLASS_TARGETS = {
    "db.t3": "db.t4g",
    "db.m5": "db.m7g",
    "db.m6i": "db.m7g",
    "db.m6a": "db.m7g",
    "db.r5": "db.r7g",
    "db.r6i": "db.r7g",
    "db.r6a": "db.r7g",
}

# gp3 is same price as gp2 for RDS but better performance
# HOWEVER: gp3 gives 3000 IOPS free vs gp2's baseline (3 IOPS/GB)
# So for volumes <1000GB, gp3 gives MORE free IOPS at same price


class RDSCollector(BaseCollector):
    name = "rds"

    async def collect(self) -> dict:
        instances = []
        snapshots_summary = []

        async with self.session.client("rds") as rds:
            # ── Collect standalone instances (MySQL, PostgreSQL, etc.) ─────
            paginator = rds.get_paginator("describe_db_instances")
            async for page in paginator.paginate():
                for db in page.get("DBInstances", []):
                    # Skip Aurora cluster members — they are collected via describe_db_clusters
                    if db.get("DBClusterIdentifier"):
                        continue
                    enriched = await self._enrich_instance(db, rds)
                    instances.append(enriched)

            # ── Collect Aurora clusters ────────────────────────────────────
            # Aurora uses a cluster model: billing is at cluster level,
            # but each reader/writer instance has its own cost.
            # describe_db_instances alone misses Aurora instances.
            try:
                cluster_paginator = rds.get_paginator("describe_db_clusters")
                async for page in cluster_paginator.paginate():
                    for cluster in page.get("DBClusters", []):
                        engine = cluster.get("Engine", "")
                        if not engine.startswith("aurora"):
                            continue
                        # Collect each Aurora cluster member instance
                        for member in cluster.get("DBClusterMembers", []):
                            member_id = member.get("DBInstanceIdentifier")
                            if not member_id:
                                continue
                            try:
                                resp = await rds.describe_db_instances(
                                    DBInstanceIdentifier=member_id
                                )
                                db_list = resp.get("DBInstances", [])
                                if db_list:
                                    enriched = await self._enrich_instance(db_list[0], rds)
                                    enriched["aurora_cluster_id"] = cluster.get(
                                        "DBClusterIdentifier"
                                    )
                                    enriched["aurora_role"] = (
                                        "writer" if member.get("IsClusterWriter") else "reader"
                                    )
                                    instances.append(enriched)
                            except Exception:
                                pass  # Instance may not be accessible
            except Exception:
                pass  # describe_db_clusters may not be available in all regions

            # ── Collect manual snapshots ───────────────────────────────────
            snap_paginator = rds.get_paginator("describe_db_snapshots")
            async for page in snap_paginator.paginate(SnapshotType="manual"):
                for snap in page.get("DBSnapshots", []):
                    created = snap.get("SnapshotCreateTime")
                    age_days = None
                    if created:
                        age_days = (datetime.now(timezone.utc) - created).days

                    allocated_storage = snap.get("AllocatedStorage", 0)
                    est_cost = allocated_storage * RDS_BACKUP_PRICE_PER_GB

                    snapshots_summary.append(
                        {
                            "snapshot_id": snap.get("DBSnapshotIdentifier"),
                            "db_identifier": snap.get("DBInstanceIdentifier"),
                            "engine": snap.get("Engine"),
                            "status": snap.get("Status"),
                            "allocated_storage_gb": allocated_storage,
                            "age_days": age_days,
                            "estimated_monthly_cost": round(est_cost, 2),
                            "created_at": str(created),
                        }
                    )

        # ── Enrich instances with CloudWatch metrics (30-day window) ────────
        # 30 days captures monthly patterns (batch jobs, reporting cycles)
        # and gives statistically reliable averages for rightsizing decisions.
        async with self.session.client("cloudwatch") as cw:
            for inst in instances:
                metrics = await self._fetch_rds_metrics(inst["db_identifier"], cw)
                inst.update(metrics)

        stopped = [i for i in instances if i["status"] == "stopped"]

        # gp2 upgrade candidates — with storage details for saving calculation
        gp2_candidates = []
        for inst in instances:
            if inst.get("storage_type") == "gp2":
                allocated_gb = inst.get("storage_allocated_gb", 0)
                current_iops = inst.get("storage_iops", 0) or 0
                # gp3 gives 3000 free IOPS baseline — if current IOPS <= 3000, no extra charge
                # saving_note: LLM should use aws_pricing for gp2 vs gp3 per GB rates
                gp2_candidates.append(
                    {
                        "db_identifier": inst.get("db_identifier"),
                        "storage_allocated_gb": allocated_gb,
                        "current_iops": current_iops,
                        "needs_provisioned_iops": current_iops > 3000,
                        "migration_risk": "VERY LOW — zero downtime via AWS Console",
                        "saving_note": (
                            f"Migrate {allocated_gb}GB from gp2 to gp3. "
                            f"Use aws_pricing.rds.{'{region}'}.gp2 vs gp3 per-GB rates to calculate saving. "
                            f"gp3 baseline: 3000 IOPS free (vs gp2's {max(100, allocated_gb * 3)} IOPS). "
                            f"gp3 is always equal or better than gp2 for volumes under 1TB."
                        ),
                    }
                )

        # Old snapshots (>90 days) — likely forgotten
        old_snapshots = [s for s in snapshots_summary if (s.get("age_days") or 0) > 90]
        old_snapshots_cost = sum(s.get("estimated_monthly_cost", 0) for s in old_snapshots)

        return {
            "instances": instances,
            "stopped_instances": stopped,
            "total": len(instances),
            "total_stopped": len(stopped),
            "gp2_upgrade_candidates": gp2_candidates,
            "manual_snapshots": snapshots_summary,
            "old_snapshots_90d": old_snapshots,
            "old_snapshots_monthly_cost": round(old_snapshots_cost, 2),
            "total_manual_snapshot_cost": round(
                sum(s.get("estimated_monthly_cost", 0) for s in snapshots_summary), 2
            ),
        }

    async def _fetch_rds_metrics(self, db_id: str, cw) -> dict:
        """
        Fetch 30-day CloudWatch metrics for RDS safety analysis.

        RDS rightsizing should not be based on CPU only. The rule layer needs
        connections, memory headroom, IOPS and latency to decide whether a DB is
        a candidate, an investigation, or explicitly blocked.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        period = 86400  # daily rollups keep payload compact
        metrics: dict = {}

        metric_specs = {
            "CPUUtilization": {
                "unit": "Percent",
                "prefix": "cpu",
                "convert": None,
                "extended": True,
            },
            "DatabaseConnections": {
                "unit": "Count",
                "prefix": "connections",
                "convert": None,
                "extended": True,
            },
            "FreeableMemory": {
                "unit": "Bytes",
                "prefix": "freeable_memory",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "FreeStorageSpace": {
                "unit": "Bytes",
                "prefix": "free_storage",
                "convert": "bytes_to_gb",
                "extended": False,
            },
            "ReadIOPS": {
                "unit": "Count/Second",
                "prefix": "read_iops",
                "convert": None,
                "extended": True,
            },
            "WriteIOPS": {
                "unit": "Count/Second",
                "prefix": "write_iops",
                "convert": None,
                "extended": True,
            },
            "ReadLatency": {
                "unit": "Seconds",
                "prefix": "read_latency",
                "convert": "seconds_to_ms",
                "extended": True,
            },
            "WriteLatency": {
                "unit": "Seconds",
                "prefix": "write_latency",
                "convert": "seconds_to_ms",
                "extended": True,
            },
            "DiskQueueDepth": {
                "unit": "Count",
                "prefix": "disk_queue_depth",
                "convert": None,
                "extended": True,
            },
            "ReplicaLag": {
                "unit": "Seconds",
                "prefix": "replica_lag",
                "convert": "seconds_to_ms",
                "extended": True,
            },
            "NetworkReceiveThroughput": {
                "unit": "Bytes/Second",
                "prefix": "network_receive",
                "convert": None,
                "extended": True,
            },
            "NetworkTransmitThroughput": {
                "unit": "Bytes/Second",
                "prefix": "network_transmit",
                "convert": None,
                "extended": True,
            },
            "CPUCreditBalance": {
                "unit": "Count",
                "prefix": "cpu_credit_balance",
                "convert": None,
                "extended": False,
            },
            "CPUSurplusCreditsCharged": {
                "unit": "Count",
                "prefix": "cpu_surplus_credits_charged",
                "convert": None,
                "extended": False,
            },
        }

        async def fetch_metric(metric_name: str, spec: dict) -> None:
            kwargs = {
                "Namespace": "AWS/RDS",
                "MetricName": metric_name,
                "Dimensions": [{"Name": "DBInstanceIdentifier", "Value": db_id}],
                "StartTime": start,
                "EndTime": end,
                "Period": period,
                "Statistics": ["Average", "Maximum", "Minimum"],
            }
            resp = await self._safe_call(cw.get_metric_statistics(**kwargs))
            if not resp or resp.get("_error"):
                return

            datapoints = resp.get("Datapoints", [])
            if not datapoints:
                return

            prefix = spec["prefix"]
            avg_values = [d["Average"] for d in datapoints if "Average" in d]
            max_values = [d["Maximum"] for d in datapoints if "Maximum" in d]
            min_values = [d["Minimum"] for d in datapoints if "Minimum" in d]
            p95_values = await self._fetch_p95(
                cw,
                metric_name=metric_name,
                db_id=db_id,
                start=start,
                end=end,
                period=period,
            ) if spec.get("extended") else []

            def convert(value: float) -> float:
                if spec.get("convert") == "bytes_to_gb":
                    return value / 1024**3
                if spec.get("convert") == "seconds_to_ms":
                    return value * 1000
                return value

            if avg_values:
                metrics[f"{prefix}_avg_30d"] = round(convert(sum(avg_values) / len(avg_values)), 2)
            if max_values:
                metrics[f"{prefix}_max_30d"] = round(convert(max(max_values)), 2)
            if min_values:
                metrics[f"{prefix}_min_30d"] = round(convert(min(min_values)), 2)
            if p95_values:
                # p95 of daily p95s: conservative enough for a compact report.
                metrics[f"{prefix}_p95_30d"] = round(convert(max(p95_values)), 2)

        for metric_name, spec in metric_specs.items():
            await fetch_metric(metric_name, spec)

        # Backward-compatible aliases consumed by existing context/report code.
        if "cpu_avg_30d" not in metrics and "cpu_avg_30d" in metrics:
            pass
        if "connections_avg_30d" not in metrics and "connections_avg_30d" in metrics:
            pass
        if "freeable_memory_avg_30d" in metrics:
            metrics["freeable_memory_avg_gb"] = metrics["freeable_memory_avg_30d"]
        if "read_iops_avg_30d" in metrics:
            metrics["read_iops_avg"] = metrics["read_iops_avg_30d"]
        if "write_iops_avg_30d" in metrics:
            metrics["write_iops_avg"] = metrics["write_iops_avg_30d"]

        metrics["metrics_lookback_days"] = 30
        metrics["metric_capabilities"] = {
            "cloudwatch": {
                "available": bool(metrics),
                "cpu": "cpu_avg_30d" in metrics,
                "connections": "connections_avg_30d" in metrics,
                "memory": "freeable_memory_avg_gb" in metrics,
                "storage": "free_storage_avg_30d" in metrics,
                "iops": "read_iops_avg" in metrics or "write_iops_avg" in metrics,
                "latency": "read_latency_p95_30d" in metrics or "write_latency_p95_30d" in metrics,
            }
        }
        return metrics

    async def _fetch_p95(self, cw, *, metric_name: str, db_id: str, start, end, period: int) -> list[float]:
        resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName=metric_name,
                Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_id}],
                StartTime=start,
                EndTime=end,
                Period=period,
                ExtendedStatistics=["p95"],
            )
        )
        if not resp or resp.get("_error"):
            return []
        return [
            float(d["ExtendedStatistics"]["p95"])
            for d in resp.get("Datapoints", [])
            if d.get("ExtendedStatistics", {}).get("p95") is not None
        ]

    async def _enrich_instance(self, db: dict, rds) -> dict:
        """Build full cost breakdown for a single RDS instance."""
        tags = {t["Key"]: t["Value"] for t in db.get("TagList", [])}
        env = tags.get("env", tags.get("environment", tags.get("Environment", "")))
        is_prod = env.lower() in ("prod", "production", "prd")

        db_id = db.get("DBInstanceIdentifier", "")
        instance_class = db.get("DBInstanceClass", "")
        storage_type = db.get("StorageType", "gp2")
        allocated_gb = db.get("AllocatedStorage", 0)
        iops = db.get("Iops") or 0
        multi_az = db.get("MultiAZ", False)
        status = db.get("DBInstanceStatus", "")
        engine = db.get("Engine", "")
        engine_version = db.get("EngineVersion", "")

        # ── Storage cost ──────────────────────────────────────────────────
        storage_price = RDS_STORAGE_PRICE.get(storage_type, 0.115)
        storage_cost = allocated_gb * storage_price

        # Multi-AZ doubles storage cost (standby replica)
        if multi_az:
            storage_cost *= 2

        # ── Provisioned IOPS cost ─────────────────────────────────────────
        iops_cost = 0.0
        if storage_type in RDS_IOPS_PRICE and iops > 0:
            iops_cost = iops * RDS_IOPS_PRICE[storage_type]

        # ── Automated backup storage ──────────────────────────────────────
        # AWS gives free backup storage equal to DB size.
        # We estimate automated backup as 1× DB size above free tier
        # (conservative — actual depends on retention period and change rate)
        backup_storage_est = allocated_gb  # rough estimate of charged backup
        backup_cost_est = backup_storage_est * RDS_BACKUP_PRICE_PER_GB * 0.5  # 50% heuristic

        # ── Total storage cost ────────────────────────────────────────────
        total_storage_cost = storage_cost + iops_cost

        # ── gp2 → gp3 analysis ────────────────────────────────────────────
        # For RDS: gp3 gives 3000 IOPS + 125 MB/s free vs gp2's 3 IOPS/GB
        # Price is the same per GB but gp3 often eliminates need for io1
        gp2_analysis = None
        if storage_type == "gp2":
            gp2_baseline_iops = allocated_gb * 3  # gp2: 3 IOPS per GB
            gp3_free_iops = 3000
            gp3_better_iops = gp3_free_iops > gp2_baseline_iops

            gp2_analysis = {
                "can_migrate_to_gp3": True,
                "current_baseline_iops": gp2_baseline_iops,
                "gp3_free_iops": gp3_free_iops,
                "gp3_better_performance": gp3_better_iops,
                "migration_risk": "LOW",
                "migration_note": (
                    f"gp3 provides {gp3_free_iops} free IOPS vs gp2's "
                    f"{gp2_baseline_iops} baseline IOPS at same price. "
                    "Zero downtime migration via AWS console."
                    if gp3_better_iops
                    else f"gp3 gives {gp3_free_iops} free IOPS. Your gp2 has "
                    f"{gp2_baseline_iops} baseline IOPS — gp3 would be slower. "
                    "Consider io1/io2 instead if high IOPS needed."
                ),
            }

        # ── Safety notes ──────────────────────────────────────────────────
        safety_notes = []
        if multi_az:
            safety_notes.append(
                "Multi-AZ enabled — instance class changes cause a brief failover (~30-60s). "
                "Schedule during maintenance window."
            )
        if is_prod:
            safety_notes.append(
                "Production database — any changes require thorough testing and maintenance window."
            )
        if storage_type in ("io1", "io2") and iops > 0:
            safety_notes.append(
                f"Provisioned IOPS ({iops} IOPS) — verify application IOPS requirements "
                "before changing storage type."
            )

        return {
            "db_identifier": db_id,
            "engine": f"{engine} {engine_version}".strip(),
            "instance_class": instance_class,
            "status": status,
            "multi_az": multi_az,
            "is_production": is_prod,
            "environment_tag": env,
            "tags": tags,
            "read_replica_source": db.get("ReadReplicaSourceDBInstanceIdentifier"),
            "is_read_replica": bool(db.get("ReadReplicaSourceDBInstanceIdentifier")),
            # Storage breakdown
            "storage": {
                "type": storage_type,
                "allocated_gb": allocated_gb,
                "iops_provisioned": iops or None,
                "storage_cost_monthly": round(storage_cost, 2),
                "iops_cost_monthly": round(iops_cost, 2),
                "backup_cost_est_monthly": round(backup_cost_est, 2),
                "total_storage_cost_monthly": round(total_storage_cost, 2),
                "multi_az_doubles_storage": multi_az,
            },
            # gp2 → gp3 opportunity
            "gp2_to_gp3": gp2_analysis,
            # Storage type — needed for gp2 detection and metrics
            "storage_type": storage_type,
            "storage_allocated_gb": allocated_gb,
            "storage_iops": iops or None,
            "graviton_candidate": self._graviton_target_class(instance_class) is not None,
            "graviton_target_class": self._graviton_target_class(instance_class),
            # Safety
            "safety_note": " | ".join(safety_notes) if safety_notes else None,
        }

    def _graviton_target_class(self, instance_class: str) -> str | None:
        if not instance_class or "." not in instance_class:
            return None
        family, size = instance_class.rsplit(".", 1)
        target_family = RDS_GRAVITON_CLASS_TARGETS.get(family)
        if not target_family:
            return None
        return f"{target_family}.{size}"
