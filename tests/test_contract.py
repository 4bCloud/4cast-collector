import pytest
from collector.core.contract import build_worker_scan_result

def test_build_worker_scan_result_minimal():
    started_at = 1718000000.0
    finished_at = 1718000010.0
    scan_id = "scan-123"
    tenant_id = "tenant-456"
    collection = {
        "accounts": {
            "123456789012": {
                "account_name": "test-account",
                "ec2": {"us-east-1": {}}
            }
        },
        "collection_coverage": {"summary": {"total": 1, "ok": 1, "failed": 0, "timeout": 0}}
    }
    
    result = build_worker_scan_result(
        scan_id=scan_id,
        collection=collection,
        started_at=started_at,
        finished_at=finished_at,
        tenant_id=tenant_id
    )
    
    assert result["scan_id"] == scan_id
    assert result["tenant_id"] == tenant_id
    assert result["status"] == "succeeded"
    assert result["duration_seconds"] == 10.0
    assert len(result["accounts"]) == 1
    assert result["accounts"][0]["account_id"] == "123456789012"
    assert "ec2" in result["accounts"][0]["services_collected"]
    assert "us-east-1" in result["accounts"][0]["regions"]
    assert result["collection_coverage"]["summary"]["ok"] == 1
