"""Synthetic engineering cases; none of these quotes/approvals prove returns."""
from datetime import datetime, timedelta
import json
import sqlite3

import pytest

from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.core.config import get_config
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.forward_runtime import FrozenForwardEvaluation
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.paired_allocation import ENGINE, PairedAllocationEvaluation, instant


class Market:
    opened = True
    mode = "fresh"

    def __init__(self):
        self.calls = []
        self.cached = None

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.opened

    async def get_current_quote(self, ticker):
        stamp = datetime.now().astimezone().isoformat()
        quote = dict(ticker=ticker, price=1., source="engineering_fixture_realtime", quoted_at=stamp, fetched_at=stamp)
        self.cached = quote
        return quote

    async def get_execution_quote(self, ticker):
        self.calls.append(ticker)
        if self.mode == "missing":
            return None
        if self.mode == "cache":
            return dict(self.cached, ticker=ticker)
        quote = await self.get_current_quote(ticker)
        if self.mode == "stale":
            quote["quoted_at"] = (datetime.now().astimezone() - timedelta(minutes=10)).isoformat()
        return quote

    async def get_unadjusted_ohlc(self, *args):
        return []


async def scalar(db, query):
    async with db._connection.execute(query) as cursor:
        return (await cursor.fetchone())[0]


async def producer(db, decisions):
    round_state = await ResearchRoundCoordinator(db).start(base_topic="fixture", research_objective="engineering only")
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    decisions = [dict(ticker="588170", direction="long", confidence=.8, target_position=.1,
                      vote_quorum_met=True, decision_id=f"decision_{round_state.id}_{i}") | d
                 for i, d in enumerate(decisions)]
    await sim.record_committee_outcomes(decisions, round_id=round_state.id, source="engineering_fixture")
    async with db._connection.execute("SELECT * FROM research_rounds WHERE id=?", (round_state.id,)) as c:
        record = dict(await c.fetchone())
    async with db._connection.execute("SELECT * FROM simulation_committee_outcomes WHERE round_id=? ORDER BY id", (round_state.id,)) as c:
        approvals = [dict(r) for r in await c.fetchall()]
    return round_state.id, decisions, dict(round=record, policies=[], committee=approvals,
                                          captured_at=datetime.now().astimezone().isoformat())


@pytest.fixture
async def engine(tmp_path):
    db = DatabaseService(str(tmp_path / "source.db"))
    await db._init_db()
    market = Market()
    service = PairedAllocationEvaluation(db, market, root=tmp_path / "forward",
                                         policy=dict(get_config().get("simulation", {})))
    try:
        await service.initialize()
        yield db, market, service
    finally:
        await service.close()
        await db.close()


async def assert_temporal_closure(service):
    for sim in service.books.values():
        async with sim.db_service._connection.execute("SELECT i.created_at,q.fetched_at,q.quoted_at FROM simulation_trades t "
            "JOIN execution_intents i ON i.id=t.intent_id JOIN quote_snapshots q ON q.id=t.quote_snapshot_id") as c:
            for r in await c.fetchall():
                assert instant(r[0]) <= instant(r[1])
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") <= 5
        async with sim.db_service._connection.execute("PRAGMA foreign_key_check") as c:
            assert not await c.fetchall()


async def test_risk_contract_books_execute_and_restart_without_reset(tmp_path):
    from sovereign_hall.services.risk_budget_forward import RiskBudgetForwardEvaluation
    db = DatabaseService(str(tmp_path/'metadata.db'))
    await db._init_db()
    market = Market()
    async def history(*args):
        end = datetime.now().date()-timedelta(days=1)
        return [dict(date=(end-timedelta(days=20-i)).isoformat(),
                     close=1+(.005 if i % 2 else -.005), adjustment='raw') for i in range(21)]
    market.get_unadjusted_ohlc = history
    service = RiskBudgetForwardEvaluation(db,market,root=tmp_path/'risk',
                                         policy=dict(get_config().get('simulation',{})))
    try:
        await service.initialize()
        identity,decisions,records = await producer(db,[dict(target_position=.5,expected_days=90)])
        await service.capture_round(identity,decisions,producer_records=records)
        assert service.books['candidate'].positions['588170']['shares'] <= 1100
        assert service.books['candidate'].positions['588170']['holding_period_days'] == 90
        assert service.books['candidate'].max_holding_days == 180
        assert service.books['control'].max_holding_days == 30
        cash = service.books['candidate'].cash
        experiment_id = service.experiment_id
        await assert_temporal_closure(service)
        await service.close()
        service = RiskBudgetForwardEvaluation(db,market,root=tmp_path/'risk',
                                             policy=dict(get_config().get('simulation',{})))
        await service.initialize()
        assert service.experiment_id == experiment_id
        assert service.books['candidate'].cash == cash
        assert service.books['candidate'].positions['588170']['holding_period_days'] == 90
    finally:
        await service.close()
        await db.close()


async def test_same_approved_entry_then_only_candidate_topup_with_real_cooldown_preserved(engine):
    db, market, service = engine
    identity, decisions, records = await producer(db, [{}])
    await service.capture_round(identity, decisions, producer_records=records)
    assert service.books["control"].positions["588170"]["shares"] == 1000
    assert service.books["candidate"].positions["588170"]["shares"] == 1000
    # A new approval cannot bypass the unchanged three-day cooldown.
    identity, decisions, records = await producer(db, [dict(target_position=.3)])
    await service.capture_round(identity, decisions, producer_records=records)
    assert service.books["candidate"].positions["588170"]["shares"] == 1000
    # Advance only the fixture's cooldown state; this is not a market sample.
    for sim in service.books.values():
        sim.last_trade_records["588170"] = (datetime.now() - timedelta(days=4)).isoformat()
    identity, decisions, records = await producer(db, [dict(target_position=.3)])
    await service.capture_round(identity, decisions, producer_records=records)
    assert service.books["control"].positions["588170"]["shares"] == 1000
    assert service.books["candidate"].positions["588170"]["shares"] > 1000
    assert service.books["candidate"].positions["588170"]["shares"] <= 3000
    before = await scalar(service.books["candidate"].db_service, "SELECT COUNT(*) FROM simulation_trades")
    await service.capture_round(identity, decisions, producer_records=records)
    assert await scalar(service.books["candidate"].db_service, "SELECT COUNT(*) FROM simulation_trades") == before
    await assert_temporal_closure(service)


@pytest.mark.parametrize("mode", ["cache", "missing", "stale"])
async def test_unusable_execution_batch_preserves_same_ruling_then_recovers_once(engine, mode):
    db, market, service = engine
    market.mode = mode
    identity, decisions, records = await producer(db, [{}])
    await service.capture_round(identity, decisions, producer_records=records)
    for name in ("control", "candidate", "stress"):
        sim = service.books[name]
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") == 0
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM quote_snapshots WHERE purpose='execution'") == 0
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM execution_intents") == 1
        assert await sim.pending_decision_count() == 1
    market.mode = "fresh"
    await service.tick()
    for name in ("control", "candidate", "stress"):
        sim = service.books[name]
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") == 1
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM execution_intents") == 1
        assert await sim.pending_decision_count() == 0
    await assert_temporal_closure(service)


async def test_all_lifecycle_and_benchmark_intents_precede_common_execution_request(engine):
    _, _, service = engine
    for name in ("control", "candidate", "stress"):
        sim = service.books[name]
        sim.cash = 9900
        sim.positions = {"588170": dict(shares=100, avg_cost=1., peak_price=1.,
            opened_at=(datetime.now()-timedelta(days=31)).isoformat())}
        await sim.save_state()
    await service.tick()
    packet = json.loads((service.directory / "last_tick.json").read_text())
    requested = instant(packet["execution_market"]["requested_at"])
    for name, sim in service.books.items():
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") == 1
        async with sim.db_service._connection.execute("SELECT created_at FROM execution_intents") as c:
            assert all(instant(r[0]) <= requested for r in await c.fetchall())
    await assert_temporal_closure(service)


async def test_prepare_only_never_executes_before_shared_phase_and_requires_durable_account(engine):
    _, market, service = engine
    sim = service.books["control"]
    sim.positions = {"588170": dict(shares=100, avg_cost=1., peak_price=1.,
        opened_at=(datetime.now()-timedelta(days=31)).isoformat())}
    sim.cash = 9900
    sim.quote_provider = market
    await sim.save_state()
    reviews = await sim.review_open_positions(prepare_only=True)
    assert reviews[0]["execution"]["action"] == "prepared"
    assert await scalar(sim.db_service, "SELECT COUNT(*) FROM execution_intents") == 1
    assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") == 0
    with pytest.raises(RuntimeError, match="durable account"):
        await InvestmentSimulation().review_open_positions(prepare_only=True)


@pytest.mark.parametrize("change", ["quorum", "budget", "old"])
async def test_forged_or_pre_window_approvals_cannot_create_intents(engine, change):
    db, _, service = engine
    identity, decisions, records = await producer(db, [{}])
    if change == "quorum":
        decisions[0]["vote_quorum_met"] = False
    elif change == "budget":
        decisions[0]["target_position"] = .9
    else:
        records["committee"][0]["created_at"] = "2020-01-01T10:00:00"
    with pytest.raises(RuntimeError, match="observed forward committee"):
        await service.capture_round(identity, decisions, producer_records=records)
    assert all(not sim.positions for sim in service.books.values())
    for sim in service.books.values():
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM execution_intents") == 0


async def test_closed_session_holds_and_daily_capacity_still_caps_five_fills(engine):
    db, market, service = engine
    market.opened = False
    identity, decisions, records = await producer(db, [dict(ticker=t) for t in ("588170", "510300", "512800", "159915", "510500", "159919")])
    await service.capture_round(identity, decisions, producer_records=records)
    for name in ("control", "candidate", "stress"):
        assert await scalar(service.books[name].db_service, "SELECT COUNT(*) FROM simulation_trades") == 0
    market.opened = True
    await service.tick()
    for name in ("control", "candidate", "stress"):
        sim = service.books[name]
        assert await scalar(sim.db_service, "SELECT COUNT(*) FROM simulation_trades") == 5
        assert await sim.pending_decision_count() == 1
    await assert_temporal_closure(service)


async def test_filtered_queue_still_expires_entries_when_no_quote_is_available(engine):
    db, market, service = engine
    market.mode = "missing"
    identity, decisions, records = await producer(db, [{}])
    await service.capture_round(identity, decisions, producer_records=records)
    sim = service.books["candidate"]
    async with sim.db_service.transaction() as conn:
        await conn.execute("UPDATE simulation_pending_decisions SET expires_at='2020-01-01T10:00:00'")
    result = await sim.replay_pending_decisions(eligible_tickers=set())
    assert result["expired"] == 1 and result["attempted"] == 0
    assert await sim.pending_decision_count() == 0


async def test_atomic_failure_rolls_back_book_and_blocks_partial_comparison(engine):
    db, _, service = engine
    identity, decisions, records = await producer(db, [{}])
    sim = service.books["control"]
    async with sim.db_service.transaction() as conn:
        await conn.execute("CREATE TRIGGER fail_ledger BEFORE INSERT ON simulation_ledger_entries "
                           "BEGIN SELECT RAISE(ABORT,'fixture atomic ledger failure'); END")
    with pytest.raises(RuntimeError, match="paired execution failed"):
        await service.capture_round(identity, decisions, producer_records=records)
    for sim in service.books.values():
        assert sim.cash == 10000 and not sim.positions
        for table in ("simulation_trades", "quote_snapshots", "simulation_ledger_entries"):
            assert await scalar(sim.db_service, "SELECT COUNT(*) FROM " + table) == 0
    assert (service.directory / "active_input.json").exists()
    assert json.loads((service.directory / "blocked.json").read_text())["reason"] == "incomplete_paired_execution"
    await service.tick()
    assert all(not sim.positions for sim in service.books.values())


async def test_frozen_process_new_selection_does_not_replace_old_pointer_and_cache_is_rejected(tmp_path):
    db = DatabaseService(str(tmp_path / "source.db"))
    await db._init_db()
    market = Market()
    root = tmp_path / "forward"
    root.mkdir()
    old = b'{"experiment_id":"keep_old_blocked"}\n'
    (root / "active_runtime.json").write_bytes(old)
    service = FrozenForwardEvaluation(db, market, root=root, policy=dict(get_config().get("simulation", {})),
        bootstrap_engine=ENGINE, selection_name="prepared_runtime.json", new_window_reason="fixture explicit new window; old invalid prefix excluded")
    try:
        await service.initialize()
        assert service.engine_version == ENGINE
        assert (root / "active_runtime.json").read_bytes() == old
        market.mode = "cache"
        identity, decisions, _ = await producer(db, [{}])
        await service.capture_round(identity, decisions)
        for name in ("control", "candidate", "stress"):
            with sqlite3.connect(service.directory / (name + ".db")) as conn:
                assert conn.execute("SELECT COUNT(*) FROM simulation_trades").fetchone()[0] == 0
        market.mode = "fresh"
        await service.tick()
        await service._collect_ready()
        await service.close()
        initial_start = service.started_at
        replacement = FrozenForwardEvaluation(db, market, root=root, policy=service.policy,
            selection_name="prepared_runtime.json")
        try:
            await replacement.initialize()
            assert replacement.started_at == initial_start
            assert replacement.experiment_id == service.experiment_id
            for name in ("control", "candidate", "stress"):
                with sqlite3.connect(service.directory / (name + ".db")) as conn:
                    assert conn.execute("SELECT COUNT(*) FROM simulation_trades").fetchone()[0] == 1
                    assert conn.execute("SELECT COUNT(*) FROM execution_intents").fetchone()[0] == 1
        finally:
            await replacement.close()
        (root / "prepared_runtime.json").unlink()
        another = FrozenForwardEvaluation(db, market, root=root, policy=service.policy,
            bootstrap_engine=ENGINE, selection_name="prepared_runtime.json", new_window_reason="fixture cannot reseed books")
        try:
            with pytest.raises(RuntimeError, match="metadata cannot be replaced"):
                await another.initialize()
        finally:
            await another.close()
    finally:
        await service.close()
        await db.close()
