"""Exercise real adapters with offline HTTP/SDK fixtures, never live searches."""
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.run_discussion import classify_search_source_availability
from sovereign_hall.services.spider_service import SpiderSwarm


@pytest.mark.asyncio
@pytest.mark.parametrize("provider,body,status", [
    ("bing", '<html class="captcha">Verify</html>', 200),
    ("sogou", '<html>antispider</html>', 200),
    ("baidu", '<html>captcha</html>', 200),
    ("bing", 'unavailable', 503),
    ("sogou", 'unavailable', 503),
])
async def test_blocked_adapter_opens_circuit_without_false_success(provider, body, status):
    spider = SpiderSwarm(timeout=1, retry_times=1)
    await spider.client.aclose()
    requests = []

    async def handler(request):
        requests.append(request)
        return httpx.Response(status, text=body)

    spider.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    spider.provider_failure_threshold = 2
    try:
        for _ in range(3):
            assert await spider._do_search("offline fixture", 1, [provider]) == []
        report = spider.get_provider_health_report()
        assert len(requests) == 2
        assert report["success_counts"] == {}
        assert report["failure_counts"] == {provider: 2}
        assert report["skipped_open_circuit_counts"] == {provider: 1}
        assert report["circuit_open_sources"] == [provider]
        assert report["states"][provider]["last_failure_code"]
        # The exact same JSON is persisted in SearchQueriesValidated.
        audit = {"network_enabled": True, "submitted_query_count": 3,
                 "provider_accepted_count": 3, "result_document_count": 0,
                 "provider_health": json.loads(json.dumps(report))}
        assert classify_search_source_availability(audit) == "external_search_unavailable"
    finally:
        await spider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["bing", "sogou"])
async def test_adapter_timeout_is_not_swallowed_as_empty_success(provider):
    spider = SpiderSwarm(timeout=1, retry_times=1)
    spider.provider_failure_threshold = 1
    spider.client.get = AsyncMock(side_effect=httpx.ReadTimeout("offline timeout"))
    try:
        assert await spider._do_search("offline fixture", 1, [provider]) == []
        assert spider.get_provider_health_report()["failure_counts"] == {provider: 1}
        assert spider.get_provider_health_report()["success_counts"] == {}
    finally:
        await spider.close()


@pytest.mark.asyncio
async def test_empty_results_are_successful_and_half_open_probe_recovers():
    spider = SpiderSwarm(timeout=1, retry_times=1)
    await spider.client.aclose()
    blocked = True

    async def handler(request):
        return httpx.Response(200, text='captcha' if blocked else '<html>No results</html>')

    spider.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    spider.provider_failure_threshold = 1
    try:
        await spider._do_search("offline fixture", 1, ["bing"])
        state = spider._provider_state("bing")
        assert state["consecutive_failures"] == 1
        state["circuit_open_until"] = 0
        await spider._do_search("offline fixture", 1, ["bing"])
        assert state["consecutive_failures"] == 2
        assert state["half_open_in_flight"] is False
        blocked = False
        state["circuit_open_until"] = 0
        baseline = spider._provider_metric_snapshot()
        for _ in range(3):
            assert await spider._do_search("offline zero hits", 1, ["bing"]) == []
        report = spider.get_provider_health_report(baseline)
        assert report["success_counts"] == {"bing": 3}
        assert report["empty_success_counts"] == {"bing": 3}
        assert report["circuit_open_sources"] == []
        assert state["consecutive_failures"] == 0
        assert classify_search_source_availability({
            "submitted_query_count": 3, "provider_accepted_count": 3,
            "provider_health": report,
        }) == "provider_no_results"
    finally:
        await spider.close()


@pytest.mark.asyncio
async def test_ddg_sdk_failure_reaches_circuit_and_empty_success_does_not(monkeypatch):
    spider = SpiderSwarm(timeout=1, retry_times=1)
    spider.provider_failure_threshold = 1

    def unavailable(*args, **kwargs):
        raise RuntimeError("offline SDK provider failure")

    sdk = SimpleNamespace(text=unavailable)
    # DDGS is a lazy class proxy; patch the import boundary so no real SDK
    # instance (and hence no network request) can be constructed by this test.
    monkeypatch.setitem(sys.modules, "ddgs", SimpleNamespace(DDGS=lambda **kwargs: sdk))
    try:
        assert await spider._do_search("offline fixture", 1, ["ddg"]) == []
        report = spider.get_provider_health_report()
        assert report["failure_counts"] == {"ddg": 1}
        assert report["states"]["ddg"]["last_failure_code"] == "RuntimeError"
        spider._provider_state("ddg")["circuit_open_until"] = 0
        sdk.text = lambda *args, **kwargs: []
        assert await spider._do_search("offline fixture", 1, ["ddg"]) == []
        assert spider.get_provider_health_report()["empty_success_counts"] == {"ddg": 1}
        assert spider.get_provider_health_report()["circuit_open_sources"] == []
    finally:
        await spider.close()
