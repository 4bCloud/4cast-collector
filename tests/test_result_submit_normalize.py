from collector.core.api_client import build_failed_result
from collector.core.result_submit import normalize_worker_result


def test_normalize_worker_result_coerces_string_errors():
    normalized = normalize_worker_result({"errors": ["RuntimeError: boom"]})
    assert normalized["errors"] == [{"message": "RuntimeError: boom", "scope": "worker"}]


def test_build_failed_result_uses_dict_errors():
    result = build_failed_result(
        {"job_id": "j1", "scan_id": "s1", "tenant_id": "t1"},
        RuntimeError("boom"),
    )
    assert isinstance(result["errors"][0], dict)
    assert result["errors"][0]["message"] == "boom"
