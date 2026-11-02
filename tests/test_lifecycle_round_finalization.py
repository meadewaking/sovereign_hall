"""Workflow regressions; fixture quotes make no investment-quality claim."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.domain.portfolio.quote_freshness import MARKET_ZONE
from sovereign_hall.domain.research import ResearchRoundStatus
from sovereign_hall.run_discussion import run_committee_approved_simulation
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.investment_simulation import InvestmentSimulation


class SessionQuotes:
    def __init__(self, *, available=True):
        self.available = available
        self.open = True

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.open

    async def get_current_quote(self, ticker):
        if not self.available:
            return None
        now = datetime.now(MARKET_ZONE).isoformat()
        return dict(ticker=ticker, price=1.0, source="workflow_fixture_realtime_quote",
                    fetched_at=now, quoted_at=now)


async def prepare_round(db, market):
    await db._init_db()
    simulation = InvestmentSimulation(db, quote_provider=market)
    await simulation.initialize()
    simulation.cash = 9_900.0
    simulation.positions = {
        "588170": dict(shares=100, avg_cost=1.0, peak_price=1.0,
                       opened_at=(datetime.now() - timedelta(days=31)).isoformat())
    }
    await simulation.save_state()
    simulation.daily_reflection = AsyncMock(return_value="")
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="lifecycle and committee HOLD",
        research_objective="preserve existing fill across later research stages",
    )
    reviews = await simulation.review_open_positions(round_id=round_state.id)
    for stage in (
        ResearchRoundStatus.MEMORY_LOADED, ResearchRoundStatus.SOURCES_PERSISTED,
        ResearchRoundStatus.PROPOSALS_EXTRACTED, ResearchRoundStatus.COMMITTEE_DECIDED,
        ResearchRoundStatus.PREDICTIONS_RECORDED,
    ):
        await coordinator.advance(round_state.id, stage)
    return simulation, coordinator, round_state.id, reviews


async def account_rows(db):
    conn = await db._get_connection()
    result = {}
    for table in ("simulation_trades", "simulation_ledger_entries", "quote_snapshots",
                  "execution_intents", "simulation_positions"):
        result[table] = [tuple(row) for row in await conn.execute_fetchall(
            f"SELECT * FROM {table} ORDER BY rowid")]
    result["cash"] = [tuple(row) for row in await conn.execute_fetchall(
        "SELECT value FROM system_stats WHERE key='simulation_cash'")]
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [True, False])
@pytest.mark.parametrize("still_open", [True, False])
async def test_pre_research_exit_then_committee_hold_completes_without_new_intent(
    tmp_path, available, still_open,
):
    db = DatabaseService(str(tmp_path / "lifecycle.db"))
    market = SessionQuotes(available=available)
    try:
        simulation, coordinator, round_id, reviews = await prepare_round(db, market)
        before = await account_rows(db)
        market.open = still_open
        result = await run_committee_approved_simulation(
            simulation, market, None,
            [dict(ticker="561980", direction="hold", confidence=0.58,
                  target_position=0, vote_quorum_met=True)],
            round_id=round_id, round_coordinator=coordinator,
            pre_reviewed_positions=reviews,
        )
        assert (await coordinator.get(round_id)).status == ResearchRoundStatus.PORTFOLIO_REVIEWED
        assert result["terminal"] == ("filled" if available else "committee_hold")
        assert result["fills"] == result["cycle_fills"] == int(available)
        assert result["replay_fills"] == 0
        after_pipeline = await account_rows(db)
        for table in ("simulation_trades", "simulation_ledger_entries", "quote_snapshots", "cash"):
            assert after_pipeline[table] == before[table]
        target = ResearchRoundStatus(result["terminal"])
        await coordinator.advance(round_id, target, event_type="SimulationPipelineTerminal",
                                  payload=result, terminal_code=result["terminal"])
        await coordinator.advance(round_id, ResearchRoundStatus.REFLECTED)
        completed = await coordinator.advance(round_id, ResearchRoundStatus.COMPLETED)
        assert completed.terminal_code == result["terminal"]
        assert await account_rows(db) == after_pipeline
        conn = await db._get_connection()
        events = await conn.execute_fetchall(
            "SELECT event_type FROM round_events WHERE round_id=? ORDER BY sequence", (round_id,))
        assert "FirstExecutionIntentPersisted" not in {row[0] for row in events}
        if available:
            intent_id = (await conn.execute_fetchall(
                "SELECT intent_id FROM simulation_trades WHERE round_id=?", (round_id,)))[0][0]
            # The original intent can be retried without another fee or fill.
            replay = await simulation.execute_intent(intent_id)
            assert replay["action"] == "already_executed"
            assert await account_rows(db) == after_pipeline
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_terminal_event_failure_rolls_back_workflow_without_undoing_prior_fill(
    tmp_path, monkeypatch,
):
    db = DatabaseService(str(tmp_path / "rollback.db"))
    try:
        simulation, coordinator, round_id, _ = await prepare_round(db, SessionQuotes())
        await coordinator.advance(round_id, ResearchRoundStatus.PORTFOLIO_REVIEWED)
        before_round = await coordinator.get(round_id)
        before_account = await account_rows(db)
        append_event = coordinator._append_event

        async def fail_terminal(conn, rid, event_type, payload):
            if event_type == "SimulationPipelineTerminal":
                raise RuntimeError("event write failure")
            return await append_event(conn, rid, event_type, payload)

        monkeypatch.setattr(coordinator, "_append_event", fail_terminal)
        with pytest.raises(RuntimeError, match="event write failure"):
            await coordinator.advance(round_id, ResearchRoundStatus.FILLED,
                                      event_type="SimulationPipelineTerminal", terminal_code="filled")
        assert await coordinator.get(round_id) == before_round
        assert await account_rows(db) == before_account
        monkeypatch.setattr(coordinator, "_append_event", append_event)
        await coordinator.advance(round_id, ResearchRoundStatus.FILLED,
                                  event_type="SimulationPipelineTerminal", terminal_code="filled")
        events = await (await db._get_connection()).execute_fetchall(
            "SELECT event_type FROM round_events WHERE round_id=? AND event_type='SimulationPipelineTerminal'",
            (round_id,),
        )
        assert len(events) == 1
        assert await account_rows(db) == before_account
    finally:
        await db.close()
