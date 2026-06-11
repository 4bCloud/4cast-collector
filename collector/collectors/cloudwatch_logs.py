"""
CloudWatch Logs Collector — READ-ONLY.

One of the most common hidden costs in AWS:
- Log groups with NO retention policy → logs accumulate forever
- Log groups with zero recent activity → ghost groups
- High-volume log groups that should use S3 export instead

IAM required: logs:DescribeLogGroups, logs:DescribeLogStreams
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

# Thresholds
NO_RETENTION_RISK_MB = 100    # log groups with no retention over 100MB are flagged
INACTIVE_DAYS        = 30     # no events in 30 days = inactive


class CloudWatchLogsCollector(BaseCollector):
    name = "cloudwatch_logs"

    async def collect(self) -> dict:
        log_groups        = []
        no_retention      = []
        inactive_groups   = []
        total_stored_bytes = 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=INACTIVE_DAYS)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        async with self.session.client("logs") as logs:
            paginator = logs.get_paginator("describe_log_groups")
            async for page in paginator.paginate():
                for lg in page.get("logGroups", []):
                    name          = lg.get("logGroupName", "")
                    stored_bytes  = lg.get("storedBytes", 0)
                    retention     = lg.get("retentionInDays")  # None = never expires
                    creation_ms   = lg.get("creationTime", 0)
                    last_event_ms = await self._last_event_timestamp(logs, name)

                    total_stored_bytes += stored_bytes

                    entry = {
                        "name":              name,
                        "stored_bytes":      stored_bytes,
                        "stored_mb":         round(stored_bytes / 1024 / 1024, 2),
                        "stored_gb":         round(stored_bytes / 1024 / 1024 / 1024, 3),
                        "retention_days":    retention,
                        "has_retention":     retention is not None,
                        "creation_time_ms":  creation_ms,
                        "last_event_ms":     last_event_ms,
                        "last_event_age_days": self._age_days(last_event_ms),
                    }

                    log_groups.append(entry)

                    # No retention policy
                    if retention is None:
                        no_retention.append(entry)

                    # Inactive (no recent events)
                    if last_event_ms and last_event_ms < cutoff_ms and stored_bytes > 0:
                        inactive_groups.append(entry)

        # Sort by size descending
        no_retention.sort(key=lambda x: x["stored_bytes"], reverse=True)
        inactive_groups.sort(key=lambda x: x["stored_bytes"], reverse=True)

        return {
            "total_log_groups":      len(log_groups),
            "total_stored_gb":       round(total_stored_bytes / 1024 / 1024 / 1024, 3),
            "no_retention_count":    len(no_retention),
            "no_retention_groups":   no_retention,
            "inactive_groups":       inactive_groups,
            "price_source":          "See aws_pricing.cloudwatch_logs",
            "recommendation":        (
                "Set retention policy on ALL log groups. "
                "7-30 days for application logs, 90 days for audit/compliance. "
                "Logs older than retention are deleted automatically at no charge."
            ),
        }

    async def _last_event_timestamp(self, logs, log_group_name: str) -> int | None:
        """Return the newest stream event timestamp for a log group, if visible."""
        try:
            response = await self._safe_call(
                logs.describe_log_streams(
                    logGroupName=log_group_name,
                    orderBy="LastEventTime",
                    descending=True,
                    limit=1,
                )
            )
        except Exception:
            return None
        if not response or "_error" in response:
            return None
        streams = response.get("logStreams") or []
        if not streams:
            return None
        return streams[0].get("lastEventTimestamp")

    def _age_days(self, timestamp_ms: int | None) -> int | None:
        if not timestamp_ms:
            return None
        event_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return max((datetime.now(timezone.utc) - event_time).days, 0)
