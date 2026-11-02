"""Offline recovery regressions; no external HTTP/SDK calls or production DB."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.spider_service import SpiderSwarm


@pytest.fixture
async def spider():
    swarm = SpiderSwarm(timeout=1, retry_times=1)
    swarm.search_interval = 0
    swarm.provider_failure_threshold = 1
    yield swarm
    await swarm.close()


@pytest.mark.asyncio
async def test_same_query_probes_after_circuit_cooldown(spider):
    spider._bing_search = AsyncMock(return_value=[])
    await spider._record_provider_outcome('bing', success=False, failure_code='provider_timeout')
    await spider.aggressive_search(['云计算 财报'], 1, ['bing'])
    assert spider._bing_search.await_count == 0
    assert '云计算 财报' not in spider._search_cache
    spider._provider_state('bing')['circuit_open_until'] = 0
    await spider.aggressive_search(['云计算 财报'], 1, ['bing'])
    assert spider._bing_search.await_count == 1
    assert spider.get_provider_health_report()['states']['bing']['circuit_state'] == 'closed'
    assert spider.last_provider_health_report['empty_success_counts'] == {'bing': 1}


@pytest.mark.asyncio
@pytest.mark.parametrize('half_open', [False, True])
async def test_cancelled_admitted_call_releases_probe_and_preserves_failure_evidence(spider, half_open):
    if half_open:
        await spider._record_provider_outcome('bing', success=False, failure_code='provider_timeout')
        spider._provider_state('bing')['circuit_open_until'] = 0
    started = asyncio.Event()

    async def blocked(*args, **kwargs):
        started.set()
        await asyncio.Future()

    spider._bing_search = blocked
    before = dict(spider._provider_state('bing'))
    task = asyncio.create_task(spider._do_search('云计算 财报', 1, ['bing']))
    await started.wait()
    if half_open:
        assert spider.get_provider_health_report()['states']['bing']['circuit_state'] == 'half_open_probe_in_flight'
        assert not await spider._provider_call_allowed('bing')
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    state = spider._provider_state('bing')
    assert state == before
    report = spider.get_provider_health_report()
    assert report['cancelled_counts'] == {'bing': 1}
    assert report['success_counts'] == {}
    assert report['failure_counts'] == ({'bing': 1} if half_open else {})
    spider._bing_search = AsyncMock(return_value=[])
    await spider._do_search('云计算 财报', 1, ['bing'])
    assert spider._bing_search.await_count == 1
    assert not state['half_open_in_flight']


@pytest.mark.asyncio
async def test_outer_deadline_cancellation_allows_next_half_open_probe(spider):
    await spider._record_provider_outcome('bing', success=False, failure_code='provider_timeout')
    spider._provider_state('bing')['circuit_open_until'] = 0
    async def blocked(*args, **kwargs):
        await asyncio.Future()
    spider._bing_search = blocked
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(spider._do_search('云计算 财报', 1, ['bing']), .01)
    assert not spider._provider_state('bing')['half_open_in_flight']
    assert spider.get_provider_health_report()['cancelled_counts'] == {'bing': 1}
    assert await spider._provider_call_allowed('bing')


@pytest.mark.asyncio
async def test_cancellation_while_queued_does_not_release_another_probe(spider):
    await spider._record_provider_outcome('bing', success=False, failure_code='provider_timeout')
    spider._provider_state('bing')['circuit_open_until'] = 0
    spider._provider_semaphores['bing'] = asyncio.Semaphore(1)
    started = asyncio.Event()
    async def blocked(*args, **kwargs):
        started.set()
        await asyncio.Future()
    spider._bing_search = blocked
    first = asyncio.create_task(spider._do_search('云计算 财报', 1, ['bing']))
    await started.wait()
    second = asyncio.create_task(spider._do_search('云计算 订单', 1, ['bing']))
    await asyncio.sleep(0)
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    assert spider._provider_state('bing')['half_open_in_flight']
    assert spider.get_provider_health_report()['cancelled_counts'] == {}
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not spider._provider_state('bing')['half_open_in_flight']


@pytest.mark.asyncio
async def test_cache_keeps_each_query_results_before_cross_query_dedup(spider):
    doc = Document(id='fixture', title='行业季度报告',
                   content='行业季度报告披露经营活动现金流及最新订单变化，研究资料保留可追溯链接。',
                   url='https://example.invalid/research', source='fixture')
    spider._search_single_query = AsyncMock(return_value=[doc])
    result = await spider.aggressive_search(['云计算 财报', '云计算 订单'], 1, ['bing'])
    assert len(result) == 1
    assert spider._search_cache['云计算 财报'][0] == [doc]
    assert spider._search_cache['云计算 订单'][0] == [doc]
    result = await spider.aggressive_search(['云计算 订单'], 1, ['bing'])
    assert result == [doc]
    assert spider._search_single_query.await_count == 2


@pytest.mark.asyncio
async def test_normal_zero_hits_remain_success_without_negative_cache(spider):
    spider._bing_search = AsyncMock(return_value=[])
    for _ in range(2):
        assert await spider.aggressive_search(['云计算 财报'], 1, ['bing']) == []
    assert spider._bing_search.await_count == 2
    report = spider.get_provider_health_report()
    assert report['empty_success_counts'] == {'bing': 2}
    assert report['failure_counts'] == {}
    assert report['circuit_open_sources'] == []


@pytest.mark.asyncio
async def test_cancellation_audit_round_trip_shared_read_model_and_check_db(tmp_path, capsys):
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.application.get_system_status import get_system_status, format_system_status
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.check_db import show_canonical_pipeline_status

    path = tmp_path / 'recovery.db'
    db = DatabaseService(str(path))
    await db._init_db()
    try:
        coordinator = ResearchRoundCoordinator(db)
        state = await coordinator.start(base_topic='offline fixture', research_objective='recovery audit')
        await coordinator.record_event(state.id, 'SearchQueriesValidated', {
            'network_enabled': True, 'submitted_query_count': 1, 'provider_accepted_count': 1,
            'availability_status': 'external_search_unavailable',
            'provider_health': {'cancelled_counts': {'bing': 1}, 'success_counts': {},
                                'recovery_contract': 'uncached_empty_cancel_safe_probe_v1',
                                'states': {'bing': {'circuit_state': 'half_open_probe_ready'}}},
        })
        status = await get_system_status(db)
        assert 'cancelled=1' in format_system_status(status)
    finally:
        await db.close()
    await asyncio.to_thread(show_canonical_pipeline_status, path)
    output = capsys.readouterr().out
    assert '搜索调用取消: 1' in output
    assert '已释放探针名额' in output
    legacy = {'latest_round': {'id': 'legacy'}, 'search_query_gate': {'provider_health': {'states': {}}}}
    assert 'cancelled=' not in format_system_status(legacy)
