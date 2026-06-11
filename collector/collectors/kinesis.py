"""
Kinesis Collector — READ-ONLY.

Kinesis Data Streams + Kinesis Data Firehose analysis:
- Streams with no data flowing (idle shards still billed)
- Firehose delivery streams with no records
- Shard-hour billing (each shard = $0.015/hour regardless of usage)

IAM required: kinesis:ListStreams, kinesis:DescribeStreamSummary,
              firehose:ListDeliveryStreams, firehose:DescribeDeliveryStream,
              cloudwatch:GetMetricStatistics (already in policy)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

IDLE_RECORDS_THRESHOLD = 100  # avg records/min over 7 days


class KinesisCollector(BaseCollector):
    name = "kinesis"

    async def collect(self) -> dict:
        streams = []
        firehoses = []

        async with self.session.client("kinesis") as kin:
            async with self.session.client("firehose") as fh:
                async with self.session.client("cloudwatch") as cw:
                    # ── Kinesis Data Streams ───────────────────────────────
                    paginator = kin.get_paginator("list_streams")
                    async for page in paginator.paginate():
                        for stream_name in page.get("StreamNames", []):
                            enriched = await self._enrich_stream(stream_name, kin, cw)
                            if enriched:
                                streams.append(enriched)

                    # ── Kinesis Data Firehose ──────────────────────────────
                    fh_paginator = fh.get_paginator("list_delivery_streams")
                    async for page in fh_paginator.paginate():
                        for stream_name in page.get("DeliveryStreamNames", []):
                            enriched = await self._enrich_firehose(stream_name, fh, cw)
                            if enriched:
                                firehoses.append(enriched)

        idle_streams = [s for s in streams if s.get("is_idle")]
        idle_firehoses = [f for f in firehoses if f.get("is_idle")]

        return {
            "streams": streams,
            "firehoses": firehoses,
            "idle_streams": idle_streams,
            "idle_firehoses": idle_firehoses,
            "price_source": "See aws_pricing.kinesis",
            "cost_note": (
                "Kinesis Data Streams: charged per shard-hour + per GB payload. "
                "Each shard costs ~$10.80/month regardless of traffic. "
                "Idle streams with multiple shards are pure waste. "
                "Firehose: charged per GB ingested + format conversion + delivery."
            ),
        }

    async def _enrich_stream(self, stream_name: str, kin, cw) -> dict | None:
        resp = await self._safe_call(kin.describe_stream_summary(StreamName=stream_name))
        if not resp or resp.get("_error"):
            return None

        summary = resp.get("StreamDescriptionSummary", {})
        shards = summary.get("OpenShardCount", 0)
        status = summary.get("StreamStatus", "")

        # CloudWatch — GetRecords metric
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        is_idle = True

        cw_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/Kinesis",
                MetricName="GetRecords.Records",
                Dimensions=[{"Name": "StreamName", "Value": stream_name}],
                StartTime=start,
                EndTime=end,
                Period=86400,
                Statistics=["Sum"],
            )
        )
        if cw_resp and not cw_resp.get("_error"):
            datapoints = cw_resp.get("Datapoints", [])
            total = sum(d["Sum"] for d in datapoints)
            is_idle = total < IDLE_RECORDS_THRESHOLD

        return {
            "stream_name": stream_name,
            "status": status,
            "shard_count": shards,
            "retention_hours": summary.get("RetentionPeriodHours", 24),
            "is_idle": is_idle,
            "price_source": "See aws_pricing.kinesis",
        }

    async def _enrich_firehose(self, stream_name: str, fh, cw) -> dict | None:
        resp = await self._safe_call(fh.describe_delivery_stream(DeliveryStreamName=stream_name))
        if not resp or resp.get("_error"):
            return None

        desc = resp.get("DeliveryStreamDescription", {})
        status = desc.get("DeliveryStreamStatus", "")

        # CloudWatch — IncomingBytes
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=14)
        is_idle = True

        cw_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/Firehose",
                MetricName="IncomingBytes",
                Dimensions=[{"Name": "DeliveryStreamName", "Value": stream_name}],
                StartTime=start,
                EndTime=end,
                Period=604800,
                Statistics=["Sum"],
            )
        )
        if cw_resp and not cw_resp.get("_error"):
            datapoints = cw_resp.get("Datapoints", [])
            total = sum(d["Sum"] for d in datapoints)
            is_idle = total < 1024 * 1024  # less than 1MB in 7 days

        # Destination
        destinations = desc.get("Destinations", [{}])
        dest = destinations[0] if destinations else {}
        dest_type = (
            "S3"
            if dest.get("S3DestinationDescription")
            else "Redshift"
            if dest.get("RedshiftDestinationDescription")
            else "ElasticSearch"
            if dest.get("ElasticsearchDestinationDescription")
            else "Splunk"
            if dest.get("SplunkDestinationDescription")
            else "HTTP"
            if dest.get("HttpEndpointDestinationDescription")
            else "Unknown"
        )

        return {
            "stream_name": stream_name,
            "status": status,
            "destination": dest_type,
            "is_idle": is_idle,
            "price_source": "See aws_pricing.firehose",
        }
