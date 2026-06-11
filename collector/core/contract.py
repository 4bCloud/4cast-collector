from __future__ import annotations
from datetime import UTC, datetime
from typing import Any

def build_worker_scan_result(
    *,
    scan_id: str,
    collection: dict[str, Any],
    started_at: float,
    finished_at: float,
    tenant_id: str = "local",
    status: str = "succeeded",
) -> dict[str, Any]:
    return {
        "schema_version": "2026-06-11",
        "agent": "collector",
        "tenant_id": tenant_id,
        "scan_id": scan_id,
        "status": status,
        "started_at": datetime.fromtimestamp(started_at, UTC).isoformat(),
        "finished_at": datetime.fromtimestamp(finished_at, UTC).isoformat(),
        "duration_seconds": round(max(finished_at - started_at, 0), 2),
        "accounts": _accounts_summary(collection),
        "collection_coverage": collection.get("collection_coverage") or {},
        "artifacts": [],
        "errors": _error_output(collection),
    }

def _accounts_summary(collection: dict[str, Any]) -> list[dict[str, Any]]:
    accounts: list[dict[str, Any]] = []
    for account_id, account_data in (collection.get("accounts") or {}).items():
        if not isinstance(account_data, dict):
            continue
        services = []
        regions = set()
        for key, value in account_data.items():
            if key.startswith("_") or key in {"account_id", "account_name"}:
                continue
            if isinstance(value, dict) and value:
                services.append(key)
                regions.update(str(region) for region in value if not str(region).startswith("_"))
            elif isinstance(value, list) and value:
                services.append(key)
        accounts.append(
            {
                "account_id": str(account_id),
                "account_name": str(account_data.get("account_name") or account_id),
                "status": "error" if account_data.get("_error") else "collected",
                "regions": sorted(regions),
                "services_collected": sorted(set(services)),
            }
        )
    return accounts

def _error_output(collection: dict[str, Any]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for account_id, account_data in (collection.get("accounts") or {}).items():
        if not isinstance(account_data, dict):
            continue
        if account_data.get("_error"):
            errors.append(
                {
                    "account_id": str(account_id),
                    "scope": "account",
                    "message": str(account_data.get("_error")),
                }
            )
        for service, service_data in account_data.items():
            if not isinstance(service_data, dict) or service.startswith("_"):
                continue
            for region, region_data in service_data.items():
                if isinstance(region_data, dict) and region_data.get("_error"):
                    errors.append(
                        {
                            "account_id": str(account_id),
                            "scope": "service_region",
                            "service": service,
                            "region": str(region),
                            "message": str(region_data.get("_error")),
                        }
                    )
    return errors
