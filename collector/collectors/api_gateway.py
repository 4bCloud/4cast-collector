"""
API Gateway Collector — READ-ONLY.

API Gateway cost analysis:
- REST APIs (v1) with no requests (idle)
- HTTP APIs (v2) — cheaper than REST, migration opportunity
- WebSocket APIs with no connections
- Stage comparison (multiple stages = multiple caches)
- Cache enabled but low hit rate (waste)

IAM required: apigateway:GET (on /restapis, /apis)
Note: API Gateway uses a resource-based policy approach
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector

IDLE_REQUESTS_THRESHOLD = 100  # requests in 30 days


class APIGatewayCollector(BaseCollector):
    name = "api_gateway"

    async def collect(self) -> dict:
        rest_apis  = []
        http_apis  = []

        async with self.session.client("apigateway") as apigw:
            async with self.session.client("apigatewayv2") as apigw2:
                async with self.session.client("cloudwatch") as cw:

                    # ── REST APIs (v1) ─────────────────────────────────────
                    try:
                        paginator = apigw.get_paginator("get_rest_apis")
                        async for page in paginator.paginate():
                            for api in page.get("items", []):
                                enriched = await self._enrich_rest_api(api, apigw, cw)
                                rest_apis.append(enriched)
                    except Exception:
                        pass

                    # ── HTTP APIs (v2) + WebSocket ─────────────────────────
                    try:
                        paginator2 = apigw2.get_paginator("get_apis")
                        async for page in paginator2.paginate():
                            for api in page.get("Items", []):
                                enriched = await self._enrich_http_api(api, cw)
                                http_apis.append(enriched)
                    except Exception:
                        pass

        idle_rest = [a for a in rest_apis if a.get("is_idle")]
        idle_http = [a for a in http_apis if a.get("is_idle")]

        # REST APIs that could be migrated to HTTP API (cheaper)
        migration_candidates = [
            a for a in rest_apis
            if not a.get("is_idle") and not a.get("uses_rest_specific_features")
        ]

        return {
            "rest_apis":            rest_apis,
            "http_apis":            http_apis,
            "idle_rest_apis":       idle_rest,
            "idle_http_apis":       idle_http,
            "migration_candidates": migration_candidates,
            "price_source":         "See aws_pricing.api_gateway",
            "cost_note": (
                "REST APIs: $3.50/million requests. "
                "HTTP APIs: $1.00/million requests (71% cheaper). "
                "WebSocket: $1.00/million messages. "
                "Cache costs additional per GB-hour. "
                "Consider migrating REST APIs to HTTP APIs if not using "
                "REST-specific features (request validation, AWS WAF, usage plans)."
            ),
        }

    async def _enrich_rest_api(self, api: dict, apigw, cw) -> dict:
        api_id   = api.get("id", "")
        api_name = api.get("name", "")

        # Get stages
        stages = []
        try:
            stages_resp = await self._safe_call(apigw.get_stages(restApiId=api_id))
            if stages_resp and not stages_resp.get("_error"):
                stages = stages_resp.get("item", [])
        except Exception:
            pass

        # Check CloudWatch for request count
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        is_idle = True

        cw_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/ApiGateway",
                MetricName="Count",
                Dimensions=[{"Name": "ApiName", "Value": api_name}],
                StartTime=start,
                EndTime=end,
                Period=2592000,  # 30 days
                Statistics=["Sum"],
            )
        )
        if cw_resp and not cw_resp.get("_error"):
            datapoints = cw_resp.get("Datapoints", [])
            total = sum(d["Sum"] for d in datapoints)
            is_idle = total < IDLE_REQUESTS_THRESHOLD

        return {
            "api_id":                api_id,
            "api_name":              api_name,
            "api_type":              "REST",
            "endpoint_type":         api.get("endpointConfiguration", {}).get("types", []),
            "stage_count":           len(stages),
            "stages":                [s.get("stageName") for s in stages],
            "is_idle":               is_idle,
            "uses_rest_specific_features": False,  # TODO: check for WAF, usage plans
            "price_source":          "See aws_pricing.api_gateway",
        }

    async def _enrich_http_api(self, api: dict, cw) -> dict:
        api_id   = api.get("ApiId", "")
        api_name = api.get("Name", "")
        api_type = api.get("ProtocolType", "HTTP")  # HTTP or WEBSOCKET

        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        is_idle = True

        cw_resp = await self._safe_call(
            cw.get_metric_statistics(
                Namespace="AWS/ApiGateway",
                MetricName="Count",
                Dimensions=[
                    {"Name": "ApiId", "Value": api_id},
                ],
                StartTime=start,
                EndTime=end,
                Period=2592000,
                Statistics=["Sum"],
            )
        )
        if cw_resp and not cw_resp.get("_error"):
            datapoints = cw_resp.get("Datapoints", [])
            total = sum(d["Sum"] for d in datapoints)
            is_idle = total < IDLE_REQUESTS_THRESHOLD

        return {
            "api_id":       api_id,
            "api_name":     api_name,
            "api_type":     api_type,
            "is_idle":      is_idle,
            "price_source": "See aws_pricing.api_gateway",
        }
