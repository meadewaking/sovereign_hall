"""A fresh research cache is not a new post-intent execution observation."""
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.application.execute_simulation_cycle import (
    AtomicSimulationExecutor, ExecutionRejectionCommitRequest,
    InvalidAccountTransition, StaleRealtimeQuote, TradeCommitRequest,
)
from sovereign_hall.domain.portfolio.quote_freshness import MARKET_ZONE, retrieved_after
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.heuristic_policy import HeuristicRiskContext
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator


def quote(**changes):
    stamp = datetime.now(MARKET_ZONE).isoformat()
    return dict(ticker='510300', price=1.0, source='sina_realtime_quote',
                fetched_at=stamp, quoted_at=stamp) | changes


class Market:
    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return True

    async def get_current_quote(self, ticker):
        return quote(ticker=ticker)


async def account(tmp_path, market=None):
    db = DatabaseService(str(tmp_path / 'account.db'))
    await db._init_db()
    sim = InvestmentSimulation(db, market or Market())
    await sim.initialize()
    return db, sim


async def rows(db, table):
    return [dict(r) for r in await (await db._connection.execute(
        'SELECT * FROM ' + table + ' ORDER BY rowid')).fetchall()]


@pytest.mark.parametrize('fetched', [None, '', 'invalid',
    '2026-10-23T13:59:59.999999+08:00', '2026-10-23T14:00:01.000001+08:00'])
def test_retrieval_window_has_no_future_or_cache_tolerance(fetched):
    assert not retrieved_after({'fetched_at': fetched}, '2026-10-23T14:00:00',
        now=datetime(2026, 10, 23, 14, 0, 1, tzinfo=MARKET_ZONE))


def test_retrieval_window_accepts_shared_post_intent_batch_and_utc():
    assert retrieved_after({'fetched_at': '2026-10-23T06:00:00.5Z'},
        '2026-10-23T14:00:00', now=datetime(2026, 10, 23, 14, 0, 1, tzinfo=MARKET_ZONE))


@pytest.mark.asyncio
async def test_execution_bypasses_fresh_research_cache_without_retimestamping():
    md = MarketDataService()
    await md._client.aclose()
    old = quote(fetched_at=(datetime.now(MARKET_ZONE) - timedelta(seconds=1)).isoformat())
    md._quote_cache['510300'] = old
    calls = []
    def handle(request):
        calls.append(request.url.host)
        if request.url.host != 'hq.sinajs.cn':
            return httpx.Response(200, text='')
        now = datetime.now(MARKET_ZONE)
        values = [''] * 32
        values[0], values[3] = 'ETF', '1.0'
        values[30], values[31] = now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S')
        return httpx.Response(200, content=('var hq_str_sh510300="' + ','.join(values) + '";').encode('gbk'))
    md._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    try:
        assert await md.get_current_quote('510300') == old and not calls
        requested = datetime.now(MARKET_ZONE)
        actual = await md.get_execution_quote('510300')
        assert actual is not None and calls
        assert retrieved_after(actual, requested.isoformat())
        assert old['fetched_at'] != actual['fetched_at']
        assert (await md.get_current_quote('510300')) == actual
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_failed_execution_refresh_cannot_fall_back_to_fresh_cache():
    md = MarketDataService()
    await md._client.aclose()
    md._quote_cache['510300'] = quote()
    md._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, text='')))
    try:
        assert await md.get_execution_quote('510300') is None
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_public_durable_entry_persists_intent_before_quote_and_replays_once(tmp_path):
    market = Market()
    db, sim = await account(tmp_path, market)
    async def after_intent(ticker):
        intents = await rows(db, 'execution_intents')
        assert len(intents) == 1 and intents[0]['status'] == 'pending'
        # A separate connection witnesses committed durability, not just a
        # write visible to its own transaction.
        import sqlite3
        with sqlite3.connect(db.db_path) as other:
            assert other.execute('SELECT count(*) FROM execution_intents').fetchone()[0] == 1
        return quote(ticker=ticker)
    market.get_current_quote = after_intent
    try:
        result = await sim.execute_trade('510300', 'long', .1, confidence=.8,
            idempotency_key='public-entry')
        assert result['action'] == 'buy'
        assert (await sim.execute_trade('510300', 'long', .1, confidence=.8,
            idempotency_key='public-entry'))['action'] == 'already_executed'
        intent = (await rows(db, 'execution_intents'))[0]
        snapshots = await rows(db, 'quote_snapshots')
        assert intent['status'] == 'executed' and len(snapshots) == 1
        assert snapshots[0]['intent_id'] == intent['id']
        assert retrieved_after(snapshots[0], intent['created_at'])
        assert len(await rows(db, 'simulation_trades')) == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_pre_intent_custom_cache_never_becomes_even_a_rejection_quote(tmp_path):
    market = Market()
    cached = quote()
    market.get_current_quote = AsyncMock(return_value=cached)
    db, sim = await account(tmp_path, market)
    try:
        intent = await sim.create_execution_intent(ticker='510300', direction='long',
            target_position=.1, reason='cached', idempotency_key='cached')
        result = await sim.execute_intent(intent)
        assert result['blocker_code'] == 'realtime_quote_unavailable'
        assert sim.cash == 10000 and not sim.positions
        for table in ('simulation_trades', 'quote_snapshots', 'simulation_ledger_entries'):
            assert await rows(db, table) == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_shared_batch_fetched_after_intent_remains_usable(tmp_path):
    market = Market()
    db, sim = await account(tmp_path, market)
    try:
        intent = await sim.create_execution_intent(ticker='510300', direction='long',
            target_position=.1, reason='shared actual batch boundary', idempotency_key='shared')
        batch = quote()
        market.get_current_quote = AsyncMock(return_value=batch)
        assert (await sim.execute_intent(intent))['action'] == 'buy'
        assert (await rows(db, 'quote_snapshots'))[0]['fetched_at'] == batch['fetched_at']
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_future_retrieval_cannot_make_an_execution_snapshot(tmp_path):
    market = Market()
    db, sim = await account(tmp_path, market)
    try:
        intent = await sim.create_execution_intent(ticker='510300', direction='long',
            target_position=.1, reason='future retrieval', idempotency_key='future')
        market.get_current_quote = AsyncMock(return_value=quote(
            fetched_at=(datetime.now(MARKET_ZONE) + timedelta(seconds=1)).isoformat()))
        result = await sim.execute_intent(intent)
        assert result['blocker_code'] == 'realtime_quote_unavailable'
        assert not await rows(db, 'quote_snapshots') and not await rows(db, 'simulation_trades')
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('blocker', ['market_closed', 'non_trading_day', 'daily_trade_limit'])
async def test_public_entry_keeps_hard_gates_before_execution_fetch(tmp_path, blocker):
    market = Market()
    market.get_execution_quote = AsyncMock(side_effect=AssertionError('must not request execution quote'))
    db, sim = await account(tmp_path, market)
    if blocker == 'market_closed':
        market.is_market_open = AsyncMock(return_value=False)
    elif blocker == 'non_trading_day':
        market.is_trading_day = AsyncMock(return_value=False)
    else:
        sim.count_trades_on_date = AsyncMock(return_value=5)
    try:
        result = await sim.execute_trade('510300', 'long', .1, confidence=.8)
        assert result['action'] == 'pending' and result['blocker_code'] == blocker
        assert len(await rows(db, 'execution_intents')) == 1
        assert not await rows(db, 'quote_snapshots') and not await rows(db, 'simulation_trades')
        market.get_execution_quote.assert_not_called()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_public_intent_persistence_failure_requests_no_execution_quote(tmp_path):
    market = Market()
    market.get_execution_quote = AsyncMock()
    db, sim = await account(tmp_path, market)
    sim.create_execution_intent = AsyncMock(return_value=None)
    try:
        assert (await sim.execute_trade('510300', 'long', .1))['action'] == 'error'
        market.get_execution_quote.assert_not_called()
        assert not await rows(db, 'simulation_trades') and not await rows(db, 'quote_snapshots')
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('direction', ['neutral', '观望'])
async def test_legacy_hold_alias_keeps_price_free_canonical_path(tmp_path, direction):
    db, sim = await account(tmp_path)
    try:
        assert (await sim.execute_trade('510300', direction, 0))['action'] == 'hold'
        assert (await rows(db, 'execution_intents'))[0]['direction'] == 'hold'
        assert not await rows(db, 'quote_snapshots')
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('canonical', [False, True])
@pytest.mark.parametrize('bypass_flag', [False, True])
async def test_a_persisted_intent_cannot_manufacture_research_evidence(tmp_path, canonical, bypass_flag):
    db, sim = await account(tmp_path)
    sim.risk_context = HeuristicRiskContext(None, 'stale_evidence_fixture', None, .2,
        False, '', [], min_confidence=.55, tape_update={'validation_status': 'stale_tape'})
    try:
        if canonical:
            intent = await sim.create_execution_intent(ticker='510300', direction='long',
                target_position=.1, confidence=.8, reason='no linked evidence')
            result = await sim.execute_intent(intent, signal_count=1,
                risk_cap_already_applied=bypass_flag)
        else:
            result = await sim.execute_trade('510300', 'long', .1, confidence=.8,
                signal_count=1, risk_cap_already_applied=bypass_flag)
        assert result['action'] == 'hold'
        assert not await rows(db, 'simulation_trades')
        assert sim.cash == 10000 and not sim.positions
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('quorum', [False, True])
async def test_only_linked_directional_quorum_can_supply_committee_evidence(tmp_path, quorum):
    db, sim = await account(tmp_path)
    sim.risk_context = HeuristicRiskContext(None, 'stale_evidence_fixture', None, .2,
        False, '', [], min_confidence=.55, tape_update={'validation_status': 'stale_tape'})
    try:
        state = await ResearchRoundCoordinator(db).start(base_topic='fixture', research_objective='evidence boundary')
        await sim.record_committee_outcomes([dict(ticker='510300', direction='long', confidence=.8,
            target_position=.1, decision_id='decision', vote_quorum_met=quorum)], round_id=state.id)
        intent = await sim.create_execution_intent(ticker='510300', direction='long', target_position=.1,
            confidence=.8, reason='linked fixture approval', round_id=state.id, decision_id='decision')
        result = await sim.execute_intent(intent)
        assert result['action'] == ('buy' if quorum else 'hold')
        assert len(await rows(db, 'simulation_trades')) == int(quorum)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_cached_exit_stays_durable_then_same_intent_recovers_once(tmp_path):
    market = Market()
    db, sim = await account(tmp_path, market)
    try:
        assert (await sim.execute_trade('510300', 'long', .1, confidence=.8))['action'] == 'buy'
        cached = quote()
        market.get_current_quote = AsyncMock(return_value=cached)
        intent = await sim.create_execution_intent(ticker='510300', direction='sell',
            target_position=0, reason='protective exit', idempotency_key='exit')
        assert (await sim.execute_intent(intent))['action'] == 'pending'
        assert len(await rows(db, 'simulation_trades')) == len(await rows(db, 'quote_snapshots')) == 1
        market.get_current_quote = Market().get_current_quote
        assert (await sim.execute_intent(intent))['action'] == 'sell'
        assert (await sim.execute_intent(intent))['action'] == 'already_executed'
        assert not sim.positions
        assert len(await rows(db, 'simulation_trades')) == len(await rows(db, 'quote_snapshots')) == 2
        assert all(p['status'] == 'executed' for p in await rows(db, 'simulation_pending_decisions'))
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('side', ['buy', 'sell', 'rejection'])
async def test_atomic_boundary_rolls_back_fresh_pre_intent_quote_for_every_outcome(tmp_path, side):
    db, sim = await account(tmp_path)
    try:
        if side == 'sell':
            assert (await sim.execute_trade('510300', 'long', .1, confidence=.8))['action'] == 'buy'
        cached = quote()
        intent = await sim.create_execution_intent(ticker='510300', direction='sell' if side == 'sell' else 'long',
            target_position=0 if side == 'sell' else .1, reason='atomic stale cache', idempotency_key='atomic')
        request = TradeCommitRequest(ticker='510300', direction='sell' if side == 'sell' else 'buy',
            shares=100, price=1, gross_amount=100, commission=0, stamp_duty=0, slippage_cost=0,
            total_fee=0, reason='fixture', quote_provider=cached['source'], quote_fetched_at=cached['fetched_at'],
            quote_quoted_at=cached['quoted_at'], new_cash=sim.cash + (100 if side == 'sell' else -100),
            new_positions={'510300': {'shares': sim.positions['510300']['shares'] - 100}} if side == 'sell' else {'510300': {'shares': 100}},
            idempotency_key='atomic', intent_id=intent)
        tables=('execution_intents','quote_snapshots','simulation_trades','simulation_ledger_entries','simulation_positions','system_stats')
        before={t:await rows(db,t) for t in tables}
        with pytest.raises(StaleRealtimeQuote, match='durable intent'):
            if side == 'rejection':
                await AtomicSimulationExecutor(db).commit_rejection(ExecutionRejectionCommitRequest(
                    ticker='510300',price=1,quote_provider=cached['source'],quote_fetched_at=cached['fetched_at'],
                    quote_quoted_at=cached['quoted_at'],code='lot',reason='fixture',intent_id=intent,idempotency_key='atomic'))
            else:
                await AtomicSimulationExecutor(db).commit(request)
        assert before == {t:await rows(db,t) for t in tables}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_atomic_fill_without_durable_intent_is_rejected(tmp_path):
    db, sim = await account(tmp_path)
    q = quote()
    try:
        request = TradeCommitRequest(ticker='510300',direction='buy',shares=100,price=1,gross_amount=100,
            commission=0,stamp_duty=0,slippage_cost=0,total_fee=0,reason='fixture',quote_provider=q['source'],
            quote_fetched_at=q['fetched_at'],quote_quoted_at=q['quoted_at'],new_cash=9900,
            new_positions={'510300':{'shares':100}},idempotency_key='no-intent')
        with pytest.raises(InvalidAccountTransition, match='price-free intent'):
            await AtomicSimulationExecutor(db).commit(request)
        assert await rows(db,'simulation_trades') == []
        assert await rows(db,'quote_snapshots') == []
    finally:
        await db.close()
