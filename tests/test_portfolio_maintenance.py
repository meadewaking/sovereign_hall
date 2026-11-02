"""Account duty through research pauses; fixture prices test engineering only."""
import asyncio
from datetime import datetime, timedelta
import json
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.portfolio_maintenance import PortfolioMaintenance


class Session:
    def __init__(self):
        self.open = True
        self.available = True
        self.stale = False

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.open

    async def get_current_quote(self, ticker):
        if not self.available:
            return None
        now = datetime.now().isoformat()
        quoted = (datetime.now() - timedelta(minutes=10)).isoformat() if self.stale else now
        return dict(ticker=ticker, price=1.0, source="maintenance_fixture_quote",
                    fetched_at=now, quoted_at=quoted)


@pytest.fixture
async def account(tmp_path):
    db = DatabaseService(str(tmp_path / "account.db"))
    await db._init_db()
    market = Session()
    simulation = InvestmentSimulation(db, quote_provider=market)
    await simulation.initialize()
    simulation.cash = 9900.0
    simulation.positions = {"588170": dict(shares=100, avg_cost=1.0, peak_price=1.0,
        opened_at=(datetime.now() - timedelta(days=31)).isoformat())}
    await simulation.save_state()
    duty = PortfolioMaintenance(simulation, status_path=tmp_path / "account_status.json",
                                policy_snapshot_id="engineering_fixture")
    try:
        yield db, simulation, market, duty
    finally:
        await db.close()


async def scalar(db, sql):
    async with db._connection.execute(sql) as cursor:
        return (await cursor.fetchone())[0]


async def test_snapshot_survives_invalidation_during_status_write(account, monkeypatch):
    import sovereign_hall.services.storage_governor as module
    db, _, _, _ = account
    governor = module.StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    worker = module.storage_worker
    async def invalidate_during_write(fn, *args, **kwargs):
        if fn is module._atomic_write_json:
            governor.invalidate_snapshot()
        return await worker(fn, *args, **kwargs)
    monkeypatch.setattr(module, 'storage_worker', invalidate_during_write)
    snapshot = await governor.snapshot(refresh=True)
    assert snapshot is not None
    assert isinstance(snapshot.research_round_allowed, bool)


async def test_account_duty_continues_during_archive_generation(account):
    from sovereign_hall.run_discussion import await_storage_capacity
    from sovereign_hall.services.storage_governor import StoreWatermark, StorageSnapshot
    db, simulation, market, duty = account
    release = asyncio.Event()

    class Governor:
        async def before_round(self):
            await release.wait()
            mark = StoreWatermark(50, 100, .6, .8, .9)
            return StorageSnapshot(mark, mark, 'now')

    task = asyncio.create_task(await_storage_capacity(Governor(), portfolio_maintenance=duty))
    try:
        async with asyncio.timeout(5):
            while duty.last_result is None:
                await asyncio.sleep(.01)
        assert not task.done()
        assert duty.last_result['status'] == 'ready'
        assert await scalar(db, 'SELECT count(*) FROM simulation_trades') == 1
        release.set()
        assert (await asyncio.wait_for(task, 5)).research_round_allowed
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_capacity_gate_stays_closed_while_lifecycle_exit_and_score_run(account, monkeypatch):
    from sovereign_hall.run_discussion import await_storage_capacity
    from sovereign_hall.services.storage_governor import StorageGovernor
    from sovereign_hall.core.config import get_config
    db, simulation, market, duty = account
    cfg = get_config().get("database", {})
    monkeypatch.setitem(cfg, "max_bundle_size_bytes", 1)
    governor = StorageGovernor(db, db.db_path.parent / "knowledge")
    await governor.initialize()
    task = asyncio.create_task(await_storage_capacity(governor, .01, portfolio_maintenance=duty))
    try:
        async with asyncio.timeout(5):
            while duty.last_result is None:
                await asyncio.sleep(.01)
        report = duty.last_result
        assert not task.done()
        assert not (await governor.snapshot()).research_round_allowed
        assert report["context"] == "storage_capacity_wait"
        assert report["status"] == "ready"
        assert report["reviews"][0]["execution"]["action"] == "sell"
        assert report["performance"]["score"] == pytest.approx(simulation.cash / 10000 - 1)
        assert report["performance"]["recorded_cost_ratio"] > 0
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
        assert await scalar(db, "SELECT count(*) FROM execution_intents WHERE status='executed'") == 1
        assert await scalar(db, "SELECT count(*) FROM quote_snapshots WHERE purpose='execution'") == 1
        assert json.loads(duty.status_path.read_text())["policy_snapshot_id"] == "engineering_fixture"
        monkeypatch.setitem(cfg, "max_bundle_size_bytes", 2 * 1024**3)
        assert (await asyncio.wait_for(task, 5)).research_round_allowed
        duty._next_due = 0
        await duty.run_if_due(context="repeat")
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def test_closed_session_defers_then_open_session_requotes_and_resolves_once(account):
    db, simulation, market, duty = account
    market.open = False
    first = await duty.run_if_due(context="topic_cooldown_wait")
    assert first["reviews"][0]["action"] == "exit_pending_execution"
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
    assert await scalar(db, "SELECT count(*) FROM quote_snapshots") == 0
    market.open = True
    duty._next_due = 0
    await duty.run_if_due(context="topic_cooldown_wait")
    duty._next_due = 0
    await duty.run_if_due(context="topic_cooldown_wait")
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    assert await scalar(db, "SELECT count(*) FROM execution_intents") == 1
    assert await scalar(db, "SELECT count(*) FROM simulation_pending_decisions WHERE status='executed'") == 1


@pytest.mark.parametrize("stale", [False, True])
async def test_missing_or_stale_quote_stays_na_without_fill_or_reusing_old_status(account, stale):
    db, simulation, market, duty = account
    # First get a successful, fresh valuation without triggering an exit.
    simulation.positions["588170"]["opened_at"] = datetime.now().isoformat()
    await simulation.save_state()
    good = await duty.run_if_due(context="before_pause")
    assert good["performance"]["score"] == pytest.approx(0)
    market.stale = stale
    market.available = stale
    duty._next_due = 0
    report = await duty.run_if_due(context="daily_token_budget_wait")
    assert report["status"] == "valuation_unavailable"
    assert report["performance"]["score"] is None
    assert report["performance"]["total_assets"] is None
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
    assert await scalar(db, "SELECT count(*) FROM quote_snapshots") == 0


async def test_wait_duty_keeps_daily_five_fill_hard_gate(account):
    db, simulation, market, duty = account
    position = simulation.positions["588170"]
    simulation.positions = {ticker: dict(position) for ticker in
                            ["510300", "510500", "159915", "588170", "159366", "512100"]}
    simulation.cash = 9400.0
    await simulation.save_state()
    await duty.run_if_due(context="empty_round_backoff")
    duty._next_due = 0
    await duty.run_if_due(context="empty_round_backoff")
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 5
    assert len(simulation.positions) == 1
    assert await scalar(db, "SELECT count(*) FROM execution_intents WHERE status='deferred'") == 1
    assert await scalar(db, "SELECT count(*) FROM quote_snapshots WHERE purpose='execution'") == 5


async def test_fill_failure_rolls_back_and_remains_retryable_during_wait(account):
    db, simulation, market, duty = account
    await db._connection.execute("""CREATE TRIGGER fail_duty_ledger
        BEFORE INSERT ON simulation_ledger_entries BEGIN
        SELECT RAISE(ABORT, 'duty ledger failure'); END""")
    await db._connection.commit()
    report = await duty.run_if_due(context="storage_capacity_wait")
    assert report["status"] == "degraded"
    assert simulation.cash == 9900
    assert "588170" in simulation.positions
    for table in ["simulation_trades", "quote_snapshots", "simulation_ledger_entries"]:
        assert await scalar(db, f"SELECT count(*) FROM {table}") == 0
    assert await scalar(db, "SELECT count(*) FROM execution_intents WHERE status='pending'") == 1
    await db._connection.execute("DROP TRIGGER fail_duty_ledger")
    await db._connection.commit()
    duty._next_due = 0
    assert (await duty.run_if_due(context="storage_capacity_wait"))["status"] == "ready"
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    assert await scalar(db, "SELECT count(*) FROM execution_intents") == 1


async def test_linked_pending_resolution_failure_rolls_back_entire_lifecycle_fill(account):
    db, simulation, market, duty = account
    market.open = False
    await duty.run_if_due(context="storage_capacity_wait")
    await db._connection.execute("""CREATE TRIGGER fail_duty_pending
        BEFORE UPDATE OF status ON simulation_pending_decisions
        WHEN NEW.status='executed' BEGIN
        SELECT RAISE(ABORT, 'pending resolution failure'); END""")
    await db._connection.commit()
    market.open = True
    duty._next_due = 0
    assert (await duty.run_if_due(context="storage_capacity_wait"))["status"] == "degraded"
    assert simulation.cash == 9900
    assert simulation.positions["588170"]["shares"] == 100
    for table in ["simulation_trades", "quote_snapshots", "simulation_ledger_entries"]:
        assert await scalar(db, f"SELECT count(*) FROM {table}") == 0
    assert await scalar(db, "SELECT count(*) FROM execution_intents WHERE status='deferred'") == 1
    assert await scalar(db, "SELECT count(*) FROM simulation_pending_decisions WHERE status='pending_next_trading_session'") == 1
    await db._connection.execute("DROP TRIGGER fail_duty_pending")
    await db._connection.commit()
    duty._next_due = 0
    await duty.run_if_due(context="storage_capacity_wait")
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    assert await scalar(db, "SELECT count(*) FROM simulation_pending_decisions WHERE status='executed'") == 1


async def test_long_budget_or_cooldown_wait_preserves_delay_and_paces_duty(tmp_path, monkeypatch):
    import sovereign_hall.services.portfolio_maintenance as module
    clock = [100.0]
    original_sleep = asyncio.sleep
    async def sleep(seconds):
        clock[0] += seconds
        await original_sleep(0)
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    simulation = type("Simulation", (), dict(
        review_open_positions=AsyncMock(return_value=[]),
        replay_pending_decisions=AsyncMock(return_value={"executed": 0}),
        get_performance_metrics=AsyncMock(return_value={"valuation_complete": True, "score": -.1}),
    ))()
    duty = PortfolioMaintenance(simulation, status_path=tmp_path / "status.json")
    await duty.sleep(185, context="daily_token_budget_wait")
    assert clock[0] == 285
    assert simulation.review_open_positions.await_count == 4
    assert simulation.replay_pending_decisions.await_count == 4
    assert simulation.get_performance_metrics.await_count == 4
    await duty.run_if_due(context="not_due")
    assert simulation.get_performance_metrics.await_count == 4


async def test_cancelled_wait_leaves_no_background_account_writer(account):
    db, simulation, market, duty = account
    market.open = False
    task = asyncio.create_task(duty.sleep(3600, context="daily_token_budget_wait"))
    async with asyncio.timeout(3):
        while duty.last_result is None:
            await asyncio.sleep(.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not db._connection.in_transaction
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
    assert await scalar(db, "SELECT count(*) FROM execution_intents") == 1
