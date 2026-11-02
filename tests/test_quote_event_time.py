"""Source-time regressions: real stale response shape, boundary and atomic path."""
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
import math

import httpx
import pytest

from sovereign_hall.domain.portfolio.quote_freshness import MARKET_ZONE, is_fresh_quote
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations


def quote(**changes):
    now = datetime.now(MARKET_ZONE).isoformat()
    return dict(price=1.311, source='tencent_realtime_quote', fetched_at=now,
                quoted_at=now) | changes


@pytest.mark.parametrize('changes', [
    {'quoted_at': None}, {'quoted_at': ''}, {'quoted_at': 'garbage'},
    {'quoted_at': '2026-09-30T16:14:24+08:00'},
    lambda: {'quoted_at': (datetime.now(MARKET_ZONE) + timedelta(minutes=2)).isoformat()},
    {'fetched_at': (datetime.now(MARKET_ZONE) - timedelta(minutes=3)).isoformat()},
    {'price': math.nan}, {'price': math.inf}, {'price': -1}, {'source': ''},
])
def test_both_source_and_retrieval_time_must_be_fresh(changes):
    if callable(changes):
        changes = changes()
    assert not is_fresh_quote(quote(**changes))


def test_timestamp_timezone_and_expiry():
    tick = '2026-10-07T14:00:00+08:00'
    q = quote(quoted_at=tick, fetched_at='2026-10-07T06:00:30Z')
    assert is_fresh_quote(q, now=datetime(2026, 10, 7, 14, 2, tzinfo=MARKET_ZONE))
    assert not is_fresh_quote(q, now=datetime(2026, 10, 7, 14, 2, 1, tzinfo=MARKET_ZONE))
    assert is_fresh_quote(q | {'fetched_at': '2026-10-07T14:00:30'},
                          now=datetime(2026, 10, 7, 14, 1, tzinfo=MARKET_ZONE))


def tencent_body(ticker, stamp, price='1.311'):
    fields = [''] * 40
    fields[1], fields[2], fields[3], fields[30] = 'ETF', ticker, price, stamp
    return ('v_sz' + ticker + '="' + '~'.join(fields) + '";').encode('gbk')


@pytest.mark.asyncio
@pytest.mark.parametrize('stamp', ['20260930161424', '', 'unknown', '20990101093000'])
async def test_stale_or_missing_tencent_time_uses_only_fresh_fallback(stamp):
    md = MarketDataService()
    await md._client.aclose()
    calls = []
    def handle(req):
        calls.append(req.url.host)
        if req.url.host in {'web.sqt.gtimg.cn','qt.gtimg.cn'}:
            return httpx.Response(200, content=tencent_body('159273', stamp))
        if req.url.host == 'hq.sinajs.cn':
            return httpx.Response(200, text='')
        assert 'f86' in req.url.params['fields']
        return httpx.Response(200, json={'rc': 0, 'data': {
            'f43': 1311, 'f59': 3, 'f57': '159273', 'f58': 'ETF',
            'f86': int(datetime.now(MARKET_ZONE).timestamp())}})
    md._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    try:
        q = await md.get_current_quote('159273')
        assert q['price'] == 1.311 and q['source'] == 'eastmoney_realtime_quote'
        assert is_fresh_quote(q)
        assert 'push2.eastmoney.com' in calls and any(host in calls for host in ('web.sqt.gtimg.cn','qt.gtimg.cn'))
        initial_calls = len(calls)
        await md.get_current_quote('159273')
        assert len(calls) == initial_calls  # Fresh cache preserves provider event time.
        md._quote_cache['159273']['quoted_at'] = '2026-09-30T16:14:24+08:00'
        await md.get_current_quote('159273')
        assert len(calls) > initial_calls
    finally:
        await md.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('data', [
    {'f86': 1790753664}, {'f86': None}, {'f86': True}, {'f57': '159996'},
])
async def test_invalid_fallback_cannot_turn_old_tick_into_valid_quote(data):
    md = MarketDataService()
    await md._client.aclose()
    def handle(req):
        if req.url.host in {'web.sqt.gtimg.cn','qt.gtimg.cn'}:
            return httpx.Response(200, content=tencent_body('159273', '20260930161424'))
        if req.url.host == 'hq.sinajs.cn':
            return httpx.Response(200, text='')
        return httpx.Response(200, json={'rc': 0, 'data': {
            'f43': 1311, 'f59': 3, 'f57': '159273',
            'f86': int(datetime.now(MARKET_ZONE).timestamp())} | data})
    md._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    try:
        assert await md.get_current_quote('159273') is None
        assert not md._quote_cache
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_unknown_calendar_cannot_authorize_execution():
    md = MarketDataService()
    md._load_trade_days = AsyncMock(return_value=None)
    try:
        assert not await md.is_market_open(datetime(2026, 10, 8, 10))
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_real_execution_path_rejects_stale_source_and_preserves_fresh_atomic_path(tmp_path):
    db = DatabaseService(str(tmp_path / 'quote.db'))
    await db._init_db()
    md = MarketDataService()
    # An open-session double isolates the event-time gate, not investment quality.
    md.is_market_open = AsyncMock(return_value=True)
    md.is_trading_day = AsyncMock(return_value=True)
    md.get_execution_quote = AsyncMock(side_effect=lambda ticker: quote(quoted_at='2026-09-30T16:14:24+08:00'))
    sim = InvestmentSimulation(db, quote_provider=md)
    await sim.initialize()
    intent = await sim.create_execution_intent(ticker='159273', direction='long',
        target_position=0.1, reason='event time regression', idempotency_key='stale')
    try:
        r = await sim.execute_intent(intent)
        assert r['action'] == 'hold'
        for table in ('simulation_trades', 'quote_snapshots', 'simulation_ledger_entries'):
            assert (await (await db._connection.execute('SELECT COUNT(*) FROM ' + table)).fetchone())[0] == 0
        assert sim.cash == 10000 and not sim.positions
        # A separate price-free intent with a fresh source tick can fill once.
        observed = []
        def fresh_quote(ticker):
            actual = quote()
            observed.append(actual)
            return actual
        md.get_execution_quote = AsyncMock(side_effect=fresh_quote)
        fresh = await sim.create_execution_intent(ticker='159273', direction='long',
            target_position=0.1, reason='fresh transport regression', idempotency_key='fresh')
        result = await sim.execute_intent(fresh)
        assert result['action'] == 'buy'
        row = await (await db._connection.execute('SELECT quoted_at,fetched_at FROM quote_snapshots')).fetchone()
        assert row[0] == observed[0]['quoted_at']
        assert row[1] == observed[0]['fetched_at']
        from sovereign_hall.domain.portfolio.quote_freshness import retrieved_after
        created = (await (await db._connection.execute(
            'SELECT created_at FROM execution_intents WHERE id=?', (fresh,))).fetchone())[0]
        assert retrieved_after({'fetched_at': row[1]}, created)
        assert (await sim.execute_intent(fresh))['action'] == 'already_executed'
        assert (await (await db._connection.execute('SELECT COUNT(*) FROM simulation_trades')).fetchone())[0] == 1
        await apply_architecture_migrations(db._connection)
        assert (await (await db._connection.execute('SELECT quoted_at FROM quote_snapshots')).fetchone())[0] == row[0]
    finally:
        await md.close()
        await db.close()


@pytest.mark.asyncio
async def test_bare_price_adapter_is_not_a_timestamped_quote():
    sim = InvestmentSimulation(quote_provider=type('Legacy', (), {'get_current_price': AsyncMock(return_value=1.0)})())
    assert await sim.get_current_quote('159273') is None


@pytest.mark.asyncio
async def test_valuation_only_quote_never_reaches_atomic_writes_even_when_fresh(tmp_path):
    db = DatabaseService(str(tmp_path / 'valuation_only.db'))
    await db._init_db()
    md = MarketDataService()
    md.is_market_open = AsyncMock(return_value=True)
    md.is_trading_day = AsyncMock(return_value=True)
    # A broken provider sends a reference through the execution interface.
    md.get_execution_quote = AsyncMock(side_effect=lambda ticker: quote(valuation_only=True,
        valuation_kind='session_close'))
    sim = InvestmentSimulation(db, quote_provider=md)
    try:
        await sim.initialize()
        intent = await sim.create_execution_intent(ticker='159273', direction='long',
            target_position=0.1, reason='reference must never fill',
            idempotency_key='valuation_only')
        result = await sim.execute_intent(intent)
        assert result['action'] == 'hold'
        assert sim.cash == 10000 and not sim.positions
        for table in ('simulation_trades', 'quote_snapshots', 'simulation_ledger_entries'):
            assert (await (await db._connection.execute('SELECT COUNT(*) FROM ' + table)).fetchone())[0] == 0
    finally:
        await md.close()
        await db.close()


@pytest.mark.asyncio
async def test_forward_migration_never_reconstructs_legacy_source_time(tmp_path):
    import aiosqlite
    path = tmp_path / 'legacy.db'
    async with aiosqlite.connect(path) as conn:
        await apply_architecture_migrations(conn)
        await conn.execute("INSERT INTO quote_snapshots(id,ticker,price,provider,purpose,fetched_at,received_at) VALUES ('old','159273',1.311,'tencent_realtime_quote','execution','2026-10-07T09:51:48','2026-10-07T09:51:48')")
        await conn.commit()
        await apply_architecture_migrations(conn)
        row = await (await conn.execute("SELECT fetched_at,quoted_at FROM quote_snapshots WHERE id='old'")).fetchone()
        assert row == ('2026-10-07T09:51:48', None)


@pytest.mark.asyncio
@pytest.mark.parametrize('rejection', [False, True])
async def test_quote_expiring_while_waiting_for_transaction_leaves_no_writes(tmp_path, monkeypatch, rejection):
    from sovereign_hall.application import execute_simulation_cycle as module
    db = DatabaseService(str(tmp_path / 'expiry.db'))
    await db._init_db()
    await InvestmentSimulation(db).initialize()
    executor = module.AtomicSimulationExecutor(db)
    now = datetime.now(MARKET_ZONE).isoformat()
    common = dict(ticker='159273', price=1.311, quote_provider='tencent_realtime_quote',
                  quote_fetched_at=now, quote_quoted_at=now, idempotency_key='expires', reason='test')
    if rejection:
        req = module.ExecutionRejectionCommitRequest(**common, code='lot_cap', intent_id='test')
        action = executor.commit_rejection
    else:
        req = module.TradeCommitRequest(**common, direction='buy', shares=100,
            gross_amount=131.1, commission=5, stamp_duty=0, slippage_cost=0.1,
            total_fee=5.1, new_cash=9863.8, new_positions={})
        action = executor.commit
    ages = iter([0, 121])
    monkeypatch.setattr(module, 'quote_age_seconds', lambda *a, **kw: next(ages))
    try:
        with pytest.raises(module.StaleRealtimeQuote, match='awaiting transaction'):
            await action(req)
        for table in ('quote_snapshots', 'simulation_trades', 'simulation_ledger_entries'):
            assert (await (await db._connection.execute('SELECT COUNT(*) FROM ' + table)).fetchone())[0] == 0
    finally:
        await db.close()
