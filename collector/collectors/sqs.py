"""
SQS Collector — READ-ONLY.

SQS cost analysis:
- Queues with no messages and no traffic (idle)
- DLQ (Dead Letter Queues) with accumulated messages = processing failures
- FIFO vs Standard pricing difference
- Large message payloads (>64KB billed as multiple requests)

IAM required: sqs:ListQueues, sqs:GetQueueAttributes
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector


class SQSCollector(BaseCollector):
    name = "sqs"

    async def collect(self) -> dict:
        queues      = []
        dlq_alarms  = []

        async with self.session.client("sqs") as sqs:
            async with self.session.client("cloudwatch") as cw:
                paginator = sqs.get_paginator("list_queues")
                async for page in paginator.paginate():
                    for url in page.get("QueueUrls", []):
                        enriched = await self._enrich_queue(url, sqs, cw)
                        if enriched:
                            queues.append(enriched)
                            if enriched.get("is_dlq_with_messages"):
                                dlq_alarms.append(enriched)

        idle_queues = [q for q in queues if q.get("is_idle")]

        return {
            "queues":             queues,
            "total_queues":       len(queues),
            "idle_queues":        idle_queues,
            "dlq_with_messages":  dlq_alarms,
            "price_source":       "See aws_pricing.sqs",
            "cost_note": (
                "SQS pricing: per million requests. "
                "First 1M requests/month free. "
                "FIFO queues cost more than Standard. "
                "Messages >64KB billed as multiple 64KB chunks."
            ),
        }

    async def _enrich_queue(self, queue_url: str, sqs, cw) -> dict | None:
        attrs_resp = await self._safe_call(
            sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["All"],
            )
        )
        if not attrs_resp or attrs_resp.get("_error"):
            return None

        attrs = attrs_resp.get("Attributes", {})
        name  = queue_url.split("/")[-1]

        msgs_available     = int(attrs.get("ApproximateNumberOfMessages", 0))
        msgs_in_flight     = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        msgs_delayed       = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        is_fifo            = name.endswith(".fifo")
        redrive_policy     = attrs.get("RedrivePolicy", "")
        is_dlq             = "sourceQueues" in attrs or "deadLetterTargetArn" in redrive_policy

        messages_sent_14d = await self._sum_metric_14d(cw, name, "NumberOfMessagesSent")
        metric_available = messages_sent_14d is not None

        is_idle = (
            metric_available and
            msgs_available == 0 and
            msgs_in_flight == 0 and
            msgs_delayed   == 0 and
            messages_sent_14d == 0
        )

        return {
            "queue_name":            name,
            "queue_url":             queue_url,
            "is_fifo":               is_fifo,
            "is_dlq":                is_dlq,
            "messages_available":    msgs_available,
            "messages_in_flight":    msgs_in_flight,
            "messages_delayed":      msgs_delayed,
            "messages_sent_14d":     messages_sent_14d,
            "activity_metric_available": metric_available,
            "is_idle":               is_idle,
            "is_dlq_with_messages":  is_dlq and msgs_available > 0,
            "visibility_timeout":    int(attrs.get("VisibilityTimeout", 30)),
            "retention_seconds":     int(attrs.get("MessageRetentionPeriod", 345600)),
            "max_message_size":      int(attrs.get("MaximumMessageSize", 262144)),
            "dlq_alert": (
                f"DLQ has {msgs_available} unprocessed messages — "
                "indicates upstream processing failures."
                if is_dlq and msgs_available > 0 else None
            ),
            "price_source": "See aws_pricing.sqs",
        }

    async def _sum_metric_14d(self, cw, queue_name: str, metric_name: str) -> float | None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/SQS",
                MetricName=metric_name,
                Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=["Sum"],
            )
        )
        if not resp or resp.get("_error"):
            return None
        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            return 0.0
        return round(sum(float(point.get("Sum") or 0.0) for point in datapoints), 2)
