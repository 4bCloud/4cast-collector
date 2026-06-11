import httpx
import pytest

from collector.core.result_submit import should_retry_result_submit


def test_should_retry_on_5xx():
    response = httpx.Response(503, request=httpx.Request("PUT", "http://test"))
    assert should_retry_result_submit(httpx.HTTPStatusError("fail", request=response.request, response=response))


def test_should_not_retry_on_4xx():
    response = httpx.Response(400, request=httpx.Request("PUT", "http://test"))
    assert not should_retry_result_submit(
        httpx.HTTPStatusError("fail", request=response.request, response=response)
    )
