"""Quote availability recovery and fail-closed source-time boundaries."""
import asyncio
from collections import Counter
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.domain.portfolio.quote_freshness import MARKET_ZONE, is_fresh_quote
from sovereign_hall import check_db


def sina_body(ticker='159366', stamp=None):
    now = stamp or datetime.now(MARKET_ZONE)
    fields = [''] * 32
    fields[0], fields[3] = '医疗港股', '1.492'
    fields[30], fields[31] = now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S')
    return ('var hq_str_sz' + ticker + '="' + ','.join(fields) + '";\n').encode('gbk')


async def client(handler):
    md = MarketDataService()
    await md._client.aclose()
    md._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return md


@pytest.mark.asyncio
async def test_healthy_source_beats_stalled_source_and_cleans_up_request():
    cancelled = asyncio.Event()
    async def handle(request):
        if request.url.host in {'web.sqt.gtimg.cn','qt.gtimg.cn'}:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        if request.url.host == 'push2.eastmoney.com':
            raise httpx.ReadError('connection dropped')
        return httpx.Response(200, content=sina_body())
    md = await client(handle)
    try:
        q = await asyncio.wait_for(md.get_current_quote('159366'), timeout=0.5)
        assert q['source'] == 'sina_realtime_quote' and is_fresh_quote(q)
        assert cancelled.is_set()
        assert all(a.get('finished_at') for a in md.last_quote_attempts['159366'])
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_transient_outage_gets_one_bounded_recovery_pass():
    counts = Counter()
    def handle(request):
        counts[request.url.host] += 1
        if request.url.host == 'hq.sinajs.cn' and counts[request.url.host] == 2:
            return httpx.Response(200, content=sina_body())
        raise httpx.ConnectError('temporary network outage')
    md = await client(handle)
    try:
        q = await md.get_current_quote('159366')
        assert is_fresh_quote(q)
        assert max(counts.values()) == 2 and sum(counts.values()) <= 8
        assert any(a.get('retryable') for a in md.last_quote_attempts['159366'])
    finally:
        await md.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [
    sina_body('159999'),
    sina_body(stamp=datetime.now(MARKET_ZONE) - timedelta(days=1)),
    b'var hq_str_sz159366="";',
])
async def test_wrong_symbol_or_old_close_cannot_become_realtime_price(body):
    count = 0
    def handle(request):
        nonlocal count
        count += 1
        if request.url.host == 'hq.sinajs.cn':
            return httpx.Response(200, content=body)
        return httpx.Response(200, text='')
    md = await client(handle)
    md.is_market_open = AsyncMock(return_value=False)
    try:
        assert await md.get_current_quote('159366') is None
        assert not md._quote_cache
        assert count == 4  # Two Tencent hosts; invalid data gets no network retry.
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_cancelling_quote_fetch_drains_every_provider():
    started, stopped = set(), set()
    ready = asyncio.Event()
    async def handle(request):
        host = request.url.host
        started.add(host)
        if len(started) == 3:
            ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.add(host)
    md = await client(handle)
    try:
        task = asyncio.create_task(md.get_current_quote('159366'))
        await asyncio.wait_for(ready.wait(), 0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped == started and not md._quote_cache
    finally:
        await md.close()


def test_simulation_and_view_reject_old_source_even_with_closed_market_setting():
    now = datetime.now(MARKET_ZONE)
    q = {'price': 1.492, 'source': 'sina_realtime_quote',
         'quoted_at': (now - timedelta(days=1)).isoformat(), 'fetched_at': now.isoformat()}
    sim = InvestmentSimulation()
    sim.config = {**sim.config, 'closed_market_quote_age_seconds': 7 * 86400}
    assert not sim._quote_is_fresh(q)
    assert not check_db.is_fresh_realtime_quote(q)


def test_synchronous_quote_refresh_owns_and_closes_each_loop_client(monkeypatch):
    instances = []
    class ScopedMarket:
        def __init__(self):
            self.closed = False
            instances.append(self)
        async def get_current_quotes(self, tickers):
            assert tickers == ['159366'] and not self.closed
            now = datetime.now(MARKET_ZONE).isoformat()
            return {'159366': {'price': 1.492, 'source': 'sina_realtime_quote',
                              'quoted_at': now, 'fetched_at': now}}
        async def close(self):
            self.closed = True
    monkeypatch.setattr('sovereign_hall.services.market_data.MarketDataService', ScopedMarket)
    for _ in range(2):
        assert '159366' in check_db.get_realtime_prices(['159366.SZ', '159366'])
        assert instances[-1].closed
    assert len(instances) == 2


@pytest.mark.asyncio
async def test_transport_failure_during_cancellation_is_drained():
    started,stopped=set(),set()
    requests=[]
    ready=asyncio.Event()
    async def handle(request):
        host=request.url.host
        requests.append(asyncio.current_task());started.add(host)
        if len(started)==3:
            ready.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if host=='push2.eastmoney.com':
                raise httpx.RemoteProtocolError('connection failed during cancellation')
            raise
        finally:
            stopped.add(host)
    md=await client(handle)
    loop=asyncio.get_running_loop();previous=loop.get_exception_handler();unhandled=[]
    loop.set_exception_handler(lambda loop,context:unhandled.append(context))
    try:
        task=asyncio.create_task(md.get_current_quote('159366'))
        await asyncio.wait_for(ready.wait(),.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped==started and all(request.done() for request in requests)
        requests.clear()
        import gc
        gc.collect();await asyncio.sleep(0)
        assert not unhandled and not md._quote_cache
    finally:
        loop.set_exception_handler(previous)
        await md.close()


def tencent_body(ticker='159366',stamp=None):
    fields=['']*40
    fields[1],fields[2],fields[3]='ETF',ticker,'1.492'
    fields[30]=(stamp or datetime.now(MARKET_ZONE)).strftime('%Y%m%d%H%M%S')
    return ('v_sz'+ticker+'="'+'~'.join(fields)+'";').encode('gbk')


@pytest.mark.asyncio
async def test_tencent_web_route_returns_fresh_event_without_waiting_for_legacy_host():
    calls=[]
    async def handle(request):
        calls.append(request.url.host)
        if request.url.host=='web.sqt.gtimg.cn':
            return httpx.Response(200,content=tencent_body())
        if request.url.host=='qt.gtimg.cn':
            await asyncio.Event().wait()
        return httpx.Response(200,text='')
    md=await client(handle)
    try:
        result=await asyncio.wait_for(md.get_current_quote('159366'),.5)
        assert result['source']=='tencent_realtime_quote' and is_fresh_quote(result)
        assert result['price']==1.492 and 'qt.gtimg.cn' not in calls
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_stalled_tencent_web_route_leaves_time_for_legacy_fallback():
    stopped=asyncio.Event()
    async def handle(request):
        if request.url.host=='web.sqt.gtimg.cn':
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        if request.url.host=='qt.gtimg.cn':
            return httpx.Response(200,content=tencent_body())
        return httpx.Response(200,text='')
    md=await client(handle);md._quote_request_timeout=.1
    try:
        result=await asyncio.wait_for(md.get_current_quote('159366'),.5)
        assert result['source']=='tencent_realtime_quote' and is_fresh_quote(result)
        assert stopped.is_set()
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_stale_source_is_logged_with_real_timestamp_and_age(caplog):
    caplog.set_level('INFO',logger='sovereign_hall.services.market_data')
    stamp=datetime.now(MARKET_ZONE)-timedelta(minutes=30)
    async def handle(request):
        if request.url.host in {'web.sqt.gtimg.cn','qt.gtimg.cn'}:
            return httpx.Response(200,content=tencent_body(stamp=stamp))
        return httpx.Response(200,text='')
    md=await client(handle)
    try:
        assert await md.get_current_quote('159366') is None
        attempt=next(a for a in md.last_quote_attempts['159366'] if a['source']=='tencent_realtime_quote')
        assert attempt['status']=='stale_quote'
        assert attempt['quoted_at']==stamp.replace(microsecond=0).isoformat()
        assert attempt['source_age_seconds']>=1800 and attempt['freshness_limit_seconds']==120
        assert 'source_age_seconds' in caplog.text and attempt['quoted_at'] in caplog.text
    finally:
        await md.close()
