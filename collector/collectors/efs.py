"""
EFS Collector — READ-ONLY.

Amazon EFS (Elastic File System) analysis:
- File systems with no throughput (idle)
- Provisioned throughput vs actual usage (overprovisioned = waste)
- Standard vs Intelligent-Tiering storage class
- Lifecycle policy missing (data never migrates to IA tier)
- One Zone vs Multi-AZ (cost vs availability trade-off)

IAM required: elasticfilesystem:DescribeFileSystems,
              elasticfilesystem:DescribeLifecycleConfiguration,
              cloudwatch:GetMetricStatistics (already in policy)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

IDLE_BYTES_THRESHOLD = 1024 * 1024  # 1MB total I/O in 7 days = idle


class EFSCollector(BaseCollector):
    name = "efs"

    async def collect(self) -> dict:
        file_systems = []

        async with self.session.client("efs") as efs:
            async with self.session.client("cloudwatch") as cw:
                paginator = efs.get_paginator("describe_file_systems")
                async for page in paginator.paginate():
                    for fs in page.get("FileSystems", []):
                        enriched = await self._enrich_fs(fs, efs, cw)
                        file_systems.append(enriched)

        idle_fs = [f for f in file_systems if f.get("is_idle")]
        no_lifecycle = [f for f in file_systems if not f.get("has_lifecycle_policy")]
        provisioned_throughput = [
            f for f in file_systems if f.get("throughput_mode") == "provisioned"
        ]

        return {
            "file_systems": file_systems,
            "total": len(file_systems),
            "idle_file_systems": idle_fs,
            "without_lifecycle_policy": no_lifecycle,
            "provisioned_throughput": provisioned_throughput,
            "price_source": "See aws_pricing.efs",
            "cost_note": (
                "EFS billed per GB-month of storage + throughput. "
                "Standard storage is more expensive than IA. "
                "Lifecycle policies automatically move infrequent data to IA (cheaper). "
                "Provisioned throughput has a fixed monthly charge regardless of usage."
            ),
        }

    async def _enrich_fs(self, fs: dict, efs, cw) -> dict:
        fs_id = fs.get("FileSystemId", "")
        tags = {t["Key"]: t["Value"] for t in fs.get("Tags", [])}
        name = tags.get("Name", fs_id)

        # Throughput mode
        throughput_mode = fs.get("ThroughputMode", "bursting")
        provisioned_throughput = fs.get("ProvisionedThroughputInMibps")

        # Storage breakdown
        storage = fs.get("SizeInBytes", {})
        total_bytes = storage.get("Value", 0)
        ia_bytes = storage.get("ValueInIA", 0)
        std_bytes = storage.get("ValueInStandard", 0)

        # Lifecycle policy
        has_lifecycle = False
        try:
            lc_resp = await self._safe_call(
                efs.describe_lifecycle_configuration(FileSystemId=fs_id)
            )
            if lc_resp and lc_resp.get("LifecyclePolicies"):
                has_lifecycle = True
        except Exception:
            pass

        # CloudWatch — data I/O to detect idle
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        is_idle = False

        io_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/EFS",
                MetricName="DataReadIOBytes",
                Dimensions=[{"Name": "FileSystemId", "Value": fs_id}],
                StartTime=start,
                EndTime=end,
                Period=604800,  # 7 days
                Statistics=["Sum"],
            )
        )
        if io_resp and not io_resp.get("_error"):
            datapoints = io_resp.get("Datapoints", [])
            total_io = sum(d["Sum"] for d in datapoints)
            is_idle = total_io < IDLE_BYTES_THRESHOLD

        return {
            "filesystem_id": fs_id,
            "name": name,
            "lifecycle_state": fs.get("LifeCycleState"),
            "throughput_mode": throughput_mode,
            "provisioned_throughput_mibps": provisioned_throughput,
            "storage_class": fs.get("PerformanceMode"),
            "one_zone": fs.get("AvailabilityZoneName") is not None,
            "encrypted": fs.get("Encrypted", False),
            "total_size_gb": round(total_bytes / 1024**3, 3),
            "standard_size_gb": round(std_bytes / 1024**3, 3),
            "ia_size_gb": round(ia_bytes / 1024**3, 3),
            "has_lifecycle_policy": has_lifecycle,
            "is_idle": is_idle,
            "tags": tags,
            "price_source": "See aws_pricing.efs",
            "provisioned_note": (
                f"Provisioned throughput of {provisioned_throughput} MiB/s has a fixed "
                "monthly charge. Verify if actual throughput justifies provisioned mode "
                "vs switching to Elastic throughput (pay-per-use)."
                if throughput_mode == "provisioned"
                else None
            ),
        }
