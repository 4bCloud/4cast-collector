"""EBS Collector — READ-ONLY."""
from __future__ import annotations
from datetime import datetime, timezone
from collector.collectors.base import BaseCollector


class EBSCollector(BaseCollector):
    name = "ebs"

    async def collect(self) -> dict:
        unattached = []
        old_snapshots = []
        now = datetime.now(timezone.utc)

        async with self.session.client("ec2") as ec2:
            # Unattached volumes
            vol_paginator = ec2.get_paginator("describe_volumes")
            async for page in vol_paginator.paginate(
                Filters=[{"Name": "status", "Values": ["available"]}]
            ):
                for vol in page.get("Volumes", []):
                    tags = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}
                    create_time = vol.get("CreateTime")
                    age_days = None
                    if create_time and hasattr(create_time, "astimezone"):
                        age_days = max(0, (now - create_time.astimezone(timezone.utc)).days)
                    unattached.append({
                        "volume_id": vol.get("VolumeId"),
                        "size_gb": vol.get("Size"),
                        "type": vol.get("VolumeType"),
                        "iops": vol.get("Iops"),
                        "throughput_mbps": vol.get("Throughput"),
                        "state": vol.get("State"),
                        "create_time": str(create_time or ""),
                        "age_days": age_days,
                        "name": tags.get("Name"),
                        "tags": tags,
                    })

            # Snapshots older than 90 days owned by this account
            snap_paginator = ec2.get_paginator("describe_snapshots")
            cutoff = now.timestamp() - (90 * 86400)
            async for page in snap_paginator.paginate(OwnerIds=["self"]):
                for snap in page.get("Snapshots", []):
                    start = snap.get("StartTime")
                    if start and hasattr(start, "timestamp") and start.timestamp() < cutoff:
                        age_days = max(0, (now - start.astimezone(timezone.utc)).days)
                        old_snapshots.append({
                            "snapshot_id": snap.get("SnapshotId"),
                            "volume_size_gb": snap.get("VolumeSize"),
                            "start_time": str(start),
                            "age_days": age_days,
                            "description": snap.get("Description", "")[:100],
                        })

        return {
            "unattached_volumes": unattached,
            "unattached_count": len(unattached),
            "unattached_total_gb": sum(v["size_gb"] for v in unattached),
            "old_snapshots_90d": old_snapshots,
            "old_snapshots_count": len(old_snapshots),
        }
