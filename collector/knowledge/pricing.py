import asyncio
import json
import logging
from typing import Any, Optional
import boto3
from collector.core.settings import settings

log = logging.getLogger(__name__)

CE_SERVICE_TO_PRICING = {
    "Amazon Elastic Compute Cloud - Compute": "AmazonEC2",
    "Amazon EC2": "AmazonEC2",
    "Amazon Relational Database Service": "AmazonRDS",
    "Amazon Simple Storage Service": "AmazonS3",
    "Amazon ElastiCache": "AmazonElastiCache",
    "AmazonCloudWatch": "AmazonCloudWatch",
    "Amazon Elastic Load Balancing": "AWSELB",
    "Amazon Virtual Private Cloud": "AmazonVPC",
    "Amazon EC2 Container Registry (ECR)": "AmazonECR",
    "Amazon Simple Queue Service": "AWSQueueService",
    "CodeBuild": "AWSCodeBuild",
    "AWS Secrets Manager": "AWSSecretsManager",
    "Amazon Redshift": "AmazonRedshift",
    "AWS Lambda": "AWSLambda",
    "AWS Key Management Service": "awskms",
    "AWS CodePipeline": "AWSCodePipeline",
    "Amazon Elastic Block Store": "AmazonEC2",
    "Amazon DocumentDB": "AmazonDocDB",
    "Amazon DocumentDB (with MongoDB compatibility)": "AmazonDocDB",
    "Amazon DynamoDB": "AmazonDynamoDB",
    "Amazon MemoryDB": "AmazonMemoryDB",
    "Amazon Neptune": "AmazonNeptune",
    "Amazon Timestream": "AmazonTimestream",
    "Amazon Keyspaces": "AmazonKeyspaces",
    "Amazon Lightsail": "AmazonLightsail",
    "Amazon Aurora DSQL": "AmazonAuroraDSQL",
}

REGION_TO_LOCATION = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "eu-west-1": "Europe (Ireland)",
    "eu-west-2": "Europe (London)",
    "eu-central-1": "Europe (Frankfurt)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "sa-east-1": "South America (Sao Paulo)",
}

class AWSPricingEngine:
    def __init__(self, session=None):
        self._client = session.client("pricing", region_name="us-east-1") if session else boto3.client("pricing", region_name="us-east-1")

    async def fetch_for_collection(self, collection: dict) -> dict:
        active = self._extract_active(collection)
        if not active["services"]: return {}
        
        instance_types = self._extract_instance_types(collection)
        pricing = {"_meta": {"services_fetched": list(active["services"]), "regions_fetched": list(active["regions"])}}
        
        tasks = []
        if "AmazonEC2" in active["services"]:
            for r in active["regions"]: tasks.append(self._fetch_ec2(r, instance_types))
        if "AmazonRDS" in active["services"]:
            for r in active["regions"]: tasks.append(self._fetch_rds(r))
        
        # ... other services simplified for now ...
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, dict) and "_service" in res:
                s, r, d = res["_service"], res["_region"], res["data"]
                pricing.setdefault(s, {})[r] = d
                
        return pricing

    def _extract_active(self, collection: dict) -> dict:
        services, regions = set(), set()
        for account in collection.get("accounts", {}).values():
            ce = account.get("cost_explorer", {})
            for item in ce.get("by_service_30d", []):
                if item.get("amount", 0) >= 1:
                    code = CE_SERVICE_TO_PRICING.get(item["key"])
                    if code: services.add(code)
            for item in ce.get("by_region_30d", []):
                if item.get("amount", 0) >= 1 and item["key"] in REGION_TO_LOCATION:
                    regions.add(item["key"])
        if not regions: regions.add("us-east-1")
        return {"services": services, "regions": regions}

    def _extract_instance_types(self, collection: dict) -> set[str]:
        types = set()
        for account in collection.get("accounts", {}).values():
            for region_data in account.get("ec2", {}).values():
                for inst in region_data.get("instances", []):
                    t = inst.get("type")
                    if t and "." in t: types.add(t)
        return types

    async def _fetch_ec2(self, region, itypes):
        loc = REGION_TO_LOCATION.get(region)
        if not loc: return None
        try:
            data = await asyncio.to_thread(self._get_ec2, region, loc, itypes)
            return {"_service": "ec2", "_region": region, "data": data}
        except Exception as exc:
            log.warning("EC2 pricing fetch failed for %s: %s", region, exc)
            return None

    def _get_ec2(self, region, location, itypes):
        filters = [{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}, {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"}, {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"}]
        data = {}
        for page in self._client.get_paginator("get_products").paginate(ServiceCode="AmazonEC2", Filters=filters):
            for pstr in page.get("PriceList", []):
                item = json.loads(pstr)
                attrs = item.get("product", {}).get("attributes", {})
                itype = attrs.get("instanceType")
                if itypes and itype not in itypes: continue
                price = self._parse_price(item)
                if price: data[itype] = {"price_per_hour": price, "vcpu": attrs.get("vcpu"), "memory_gb": attrs.get("memory")}
        return data

    async def _fetch_rds(self, region):
        loc = REGION_TO_LOCATION.get(region)
        if not loc: return None
        try:
            data = await asyncio.to_thread(self._get_rds, region)
            return {"_service": "rds", "_region": region, "data": data}
        except Exception as exc:
            log.warning("RDS pricing fetch failed for %s: %s", region, exc)
            return None

    def _get_rds(self, region):
        filters = [{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}, {"Type": "TERM_MATCH", "Field": "databaseEngine", "Value": "MySQL"}]
        data = {}
        for page in self._client.get_paginator("get_products").paginate(ServiceCode="AmazonRDS", Filters=filters):
            for pstr in page.get("PriceList", []):
                item = json.loads(pstr)
                itype = item.get("product", {}).get("attributes", {}).get("instanceType")
                price = self._parse_price(item)
                if price: data[itype] = {"price_per_hour": price}
        return data

    def _parse_price(self, item):
        try:
            for term in item.get("terms", {}).get("OnDemand", {}).values():
                for dim in term.get("priceDimensions", {}).values():
                    p = dim.get("pricePerUnit", {}).get("USD")
                    if p: return float(p)
        except Exception: pass
        return None
