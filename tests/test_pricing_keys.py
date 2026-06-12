"""Pricing dict keys must be strings for evidence serialization (orjson)."""

from __future__ import annotations

import orjson
import pytest

from collector.knowledge.pricing import AWSPricingEngine


class _FakePaginator:
    def paginate(self, **_kwargs):
        yield {
            "PriceList": [
                '{"product":{"attributes":{}},"terms":{"OnDemand":{"t1":{"priceDimensions":{"d1":{"pricePerUnit":{"USD":"0.05"}}}}}}}}',
                '{"product":{"attributes":{"instanceType":"t3.micro"}},"terms":{"OnDemand":{"t1":{"priceDimensions":{"d1":{"pricePerUnit":{"USD":"0.10"}}}}}}}}',
            ]
        }


class _FakePricingClient:
    def get_paginator(self, _name: str) -> _FakePaginator:
        return _FakePaginator()


def test_get_ec2_skips_missing_instance_type_keys() -> None:
    engine = AWSPricingEngine()
    engine._client = _FakePricingClient()
    data = engine._get_ec2("us-east-1", "US East (N. Virginia)", set())
    assert list(data.keys()) == ["t3.micro"]
    orjson.dumps({"ec2": {"us-east-1": data}})
