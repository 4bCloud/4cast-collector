from __future__ import annotations
from collections import defaultdict
from typing import Any

def build_cost_attribution(collection: dict[str, Any]) -> dict[str, Any]:
    accounts: dict[str, Any] = collection.get("accounts") or {}
    account_results: dict[str, Any] = {}
    org_buckets: dict[str, dict[str, Any]] = defaultdict(_bucket)
    org_services: dict[str, float] = defaultdict(float)

    for account_id, account_data in accounts.items():
        if not isinstance(account_data, dict) or account_data.get("_error"):
            continue
        result = _attribute_account(str(account_id), account_data)
        account_results[str(account_id)] = result
        account_data["cost_attribution"] = result
        for service, amount in (result.get("services") or {}).items():
            org_services[service] += float(amount or 0)
        for bucket_key, bucket in (result.get("buckets") or {}).items():
            target = org_buckets[bucket_key]
            target["amount"] += float(bucket.get("amount") or 0)
            target["usage_quantity"] += float(bucket.get("usage_quantity") or 0)
            target["services"].update(bucket.get("services") or [])
            target["usage_types"].update(bucket.get("usage_types") or [])
            target["operations"].update(bucket.get("operations") or [])

    buckets = {
        key: _finalize_bucket(key, bucket)
        for key, bucket in sorted(org_buckets.items(), key=lambda item: item[0])
    }
    return {
        "schema_version": "2026-05-22",
        "source": "cost_explorer.by_usage_type_last_closed_month",
        "accounts": account_results,
        "services": {key: round(value, 2) for key, value in sorted(org_services.items())},
        "buckets": buckets,
        "top_buckets": sorted(
            buckets.values(),
            key=lambda item: float(item.get("amount") or 0),
            reverse=True,
        )[:15],
    }

def _attribute_account(account_id: str, account_data: dict[str, Any]) -> dict[str, Any]:
    ce = account_data.get("cost_explorer") or {}
    by_service = ce.get("by_usage_type_last_closed_month") or {}
    buckets: dict[str, dict[str, Any]] = defaultdict(_bucket)
    services: dict[str, float] = defaultdict(float)

    for service_name, rows in by_service.items():
        if not isinstance(rows, list): continue
        for row in rows:
            if not isinstance(row, dict): continue
            amount = _f(row.get("amount"))
            if amount <= 0: continue
            usage_type = str(row.get("usage_type") or "")
            operation = str(row.get("operation") or "")
            bucket_key = _bucket_for_usage(service_name, usage_type, operation)
            bucket = buckets[bucket_key]
            bucket["amount"] += amount
            bucket["usage_quantity"] += _f(row.get("usage_quantity"))
            bucket["services"].add(str(service_name))
            bucket["usage_types"].add(usage_type)
            bucket["operations"].add(operation)
            services[str(service_name)] += amount

    finalized = {key: _finalize_bucket(key, bucket) for key, bucket in sorted(buckets.items())}
    return {
        "account_id": account_id,
        "account_name": account_data.get("account_name", account_id),
        "services": {key: round(value, 2) for key, value in sorted(services.items())},
        "buckets": finalized,
        "top_buckets": sorted(finalized.values(), key=lambda item: float(item.get("amount") or 0), reverse=True)[:10],
    }

def _bucket(): return {"amount": 0.0, "usage_quantity": 0.0, "services": set(), "usage_types": set(), "operations": set()}

def _finalize_bucket(key: str, bucket: dict[str, Any]) -> dict[str, Any]:
    return {
        "bucket": key,
        "label": _bucket_label(key),
        "amount": round(float(bucket.get("amount") or 0), 2),
        "usage_quantity": round(float(bucket.get("usage_quantity") or 0), 4),
        "services": sorted(bucket.get("services") or []),
        "usage_types": sorted(v for v in (bucket.get("usage_types") or []) if v),
        "operations": sorted(v for v in (bucket.get("operations") or []) if v),
    }

def _bucket_for_usage(service_name: str, usage_type: str, operation: str) -> str:
    # Simplified version for now
    if service_name == "EC2 - Other":
        if "natgateway-hours" in usage_type: return "ec2_other.nat_gateway_hours"
        if "ebs:volumeusage" in usage_type: return "ec2_other.ebs_volume_storage"
    return f"{service_name.lower()}.other"

def _bucket_label(key: str) -> str:
    return key.replace("_", " ").replace(".", " / ")

def _f(v):
    try: return float(v or 0)
    except: return 0.0
