from __future__ import annotations
from collections import defaultdict
from typing import Any

MIN_SPEND_USD = 1.0

def build_pricing_coverage_audit(collection: dict[str, Any]) -> dict[str, Any]:
    pricing = collection.get("aws_pricing") or {}
    attribution = collection.get("cost_attribution") or {}
    available_pricing_keys = {str(key) for key, value in pricing.items() if key != "_meta"}

    spend_by_service: dict[str, float] = defaultdict(float)
    for account_data in (collection.get("accounts") or {}).values():
        if not isinstance(account_data, dict): continue
        ce = account_data.get("cost_explorer") or {}
        for item in ce.get("by_service_30d") or []:
            service = str(item.get("key") or "")
            if service: spend_by_service[service] += float(item.get("amount") or 0)

    services = []
    for service, amount in sorted(spend_by_service.items(), key=lambda item: item[1], reverse=True):
        if amount < MIN_SPEND_USD: continue
        services.append({
            "cost_explorer_service": service,
            "monthly_spend": round(amount, 2),
            "coverage_status": "priced" if service in available_pricing_keys else "not_priced"
        })

    return {
        "schema_version": "2026-05-21",
        "summary": {
            "cost_explorer_spend_audited": round(sum(s["monthly_spend"] for s in services), 2),
            "services_audited": len(services),
        },
        "services": services,
    }
