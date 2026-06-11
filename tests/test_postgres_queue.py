import json

import pytest

from collector.core.postgres_queue import _coerce_payload


def test_coerce_payload_from_dict():
    assert _coerce_payload({"scan_id": "s1", "tenant_id": "t1"}) == {
        "scan_id": "s1",
        "tenant_id": "t1",
    }


def test_coerce_payload_from_json_string():
    raw = json.dumps({"scan_id": "s1", "stage": "collect"})
    assert _coerce_payload(raw) == {"scan_id": "s1", "stage": "collect"}


def test_coerce_payload_none():
    assert _coerce_payload(None) == {}


def test_coerce_payload_rejects_non_object_json():
    with pytest.raises(ValueError, match="JSON object"):
        _coerce_payload(json.dumps(["not", "an", "object"]))


def test_coerce_payload_rejects_invalid_type():
    with pytest.raises(TypeError, match="unsupported job payload type"):
        _coerce_payload(42)
