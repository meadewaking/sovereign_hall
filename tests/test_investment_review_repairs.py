"""Regression cases for the investment audit; all money/prices are fixtures."""
import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.core.config import get_config
from sovereign_hall.application.execute_simulation_cycle import InvalidAccountTransition, TradeCommitRequest, AtomicSimulationExecutor, validate_balance_checkpoint
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.heuristic_policy import HeuristicRiskContext
from sovereign_hall.services.portfolio_maintenance import PortfolioMaintenance
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.experiment_store import ExperimentStore, TrialReturnSeries
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.continuous_evaluation import ContinuousEvaluation
from sovereign_hall.services.replayable_policy import MarketSnapshot, InstrumentRuleSet, PriceFreeIntent, plan_execution
from sovereign_hall.domain.portfolio.execution_policy import ExecutionPolicyLimits
from sovereign_hall.run_discussion import run_committee_approved_simulation


class Market:
    def __init__(self, opened=True):
        self.opened = opened
        self.price = 1.0
        self.missing_history = False
        self.history_requests = []

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.opened

    async def get_current_quote(self, ticker):
        now = datetime.now().isoformat()
        return dict(ticker=ticker, price=self.price, source="audit_fixture_realtime", fetched_at=now, quoted_at=now)

    async def get_unadjusted_ohlc(self, ticker, start, end):
        self.history_requests.append((ticker, start, end))
        return [] if self.missing_history else [dict(date=start, open=1.2, high=1.2, low=1.2,
            close=1.2, volume=100, adjustment="raw", provider="fixture", fetched_at=datetime.now().isoformat())]


async def make_account(tmp_path, name="account", market=None):
    db = DatabaseService(str(tmp_path / (name + ".db")))
    await db._init_db()
    sim = InvestmentSimulation(db, market or Market())
    await sim.initialize()
    return db, sim


async def scalar(db, sql):
    return (await (await db._connection.execute(sql)).fetchone())[0]


@pytest.mark.asyncio
async def test_open_and_deferred_ruling_keep_same_approved_budget(tmp_path, monkeypatch):
    import sovereign_hall.run_discussion as runner
    ctx = HeuristicRiskContext(None, "fixture", None, 1, False, "", [], min_confidence=.55)
    monkeypatch.setattr(runner, "load_latest_heuristic_context", lambda: ctx)
    monkeypatch.setattr(runner, "recent_prediction_observation_count", lambda ticker: 1)
    targets = []
    for opened in (True, False):
        market = Market(opened)
        db, sim = await make_account(tmp_path, str(opened), market)
        try:
            sim.cash = 4000
            sim.positions = {"510300": dict(shares=6000, avg_cost=1, opened_at=datetime.now().isoformat())}
            await sim.save_state()
            await run_committee_approved_simulation(sim, market, None,
                [dict(ticker="588170", direction="long", confidence=.58, target_position=.20)])
            targets.append(await scalar(db, "SELECT target_position FROM execution_intents WHERE ticker='588170'"))
        finally:
            await db.close()
    assert targets == [.10, .10]


@pytest.mark.asyncio
async def test_approved_entry_budget_audit_preserves_the_deferred_ruling(tmp_path, monkeypatch):
    import sovereign_hall.run_discussion as runner

    ctx = HeuristicRiskContext(None, "fixture", None, 1, False, "", [], min_confidence=.55)
    monkeypatch.setattr(runner, "load_latest_heuristic_context", lambda: ctx)
    monkeypatch.setattr(runner, "recent_prediction_observation_count", lambda ticker: 1)
    market = Market(opened=False)
    db, sim = await make_account(tmp_path, market=market)
    try:
        coordinator = ResearchRoundCoordinator(db)
        record = await coordinator.start(base_topic="entry audit", research_objective="reference budget")
        ticker = "002382"
        decision = dict(ticker=ticker, direction="long", confidence=.58, target_position=.08,
                        decision_id="entry-audit-fixture", expected_days=60, vote_quorum_met=True)
        stamp = datetime.now().isoformat()
        result = await run_committee_approved_simulation(
            sim, market, None, [decision], round_id=record.id, round_coordinator=coordinator,
            proposal_reference_quotes={ticker: dict(price=4.97, source="reference_fixture",
                                                    quoted_at=stamp, valuation_kind="session_close")},
        )
        payload = json.loads(await scalar(db, "SELECT payload_json FROM round_events "
                                        "WHERE event_type='ApprovedEntryBudgetObserved'"))
        entry, = payload["entries"]
        assert entry["decision_id"] == decision["decision_id"]
        assert entry["status"] == "zero_lots_at_reference"
        assert entry["target_after_confidence"] == .04
        assert entry["max_quote_before_tighter_caps"] == 4.0
        assert entry["reference_price"] == 4.97
        assert entry["time_exit_at_most_days"] == 30
        assert result["terminal"] == "market_closed_pending"
        assert await scalar(db, "SELECT target_position FROM execution_intents") == .04
        assert await scalar(db, "SELECT count(*) FROM quote_snapshots") == 0
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("bypass_flag", [False, True])
async def test_current_risk_limit_cannot_be_skipped(tmp_path, monkeypatch, bypass_flag):
    db, sim = await make_account(tmp_path)
    monkeypatch.setitem(get_config().get("simulation", {}), "max_single_position", .10)
    try:
        result = await sim.execute_trade("588170", "long", .80, confidence=.8,
                                         risk_cap_already_applied=bypass_flag, signal_count=1)
        assert result["action"] == "buy"
        assert result["shares"] == 1000
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_old_inflated_intent_is_restricted_by_original_committee_approval(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        round_state = await ResearchRoundCoordinator(db).start(base_topic="fixture", research_objective="budget")
        await sim.record_committee_outcomes([dict(ticker="588170", direction="long", confidence=.58,
            target_position=.20, decision_id="decision", round_id=round_state.id)], round_id=round_state.id)
        intent = await sim.create_execution_intent(ticker="588170", direction="long", target_position=.8,
            confidence=.58, reason="legacy inflated fixture", round_id=round_state.id, decision_id="decision")
        result = await sim.execute_intent(intent)
        assert result["shares"] == 1000
        await sim.execute_intent(intent)
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_investment_contract_is_persisted_and_normalized(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        intent = await sim.create_execution_intent(
            ticker="510300", direction="long", target_position=.1,
            confidence=.8, reason="contract fixture",
            holding_period_days=30, strategy_stop_loss_pct=-.08,
            strategy_take_profit_pct=.15,
            failure_conditions="财报不及预期",
        )
        row = await (await db._connection.execute(
            "SELECT holding_period_days,strategy_stop_loss_pct,"
            "strategy_take_profit_pct,failure_conditions FROM execution_intents WHERE id=?",
            (intent,),
        )).fetchone()
        assert tuple(row) == (30, -.08, .15, "财报不及预期")
        result = await sim.execute_intent(intent)
        assert result["action"] == "buy"
        await sim.save_state()
        restarted = InvestmentSimulation(db, Market())
        await restarted.initialize()
        assert restarted.positions["510300"]["holding_period_days"] == 30
        assert restarted.positions["510300"]["strategy_stop_loss_pct"] == -.08
        assert restarted.positions["510300"]["failure_conditions"] == "财报不及预期"
        assert (await restarted.execute_intent(intent))["action"] == "already_executed"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_event_level_attribution_keeps_unmatched_legacy_sale_visible(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        await db._connection.execute(
            "INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
            "VALUES ('510300','buy',100,1,5,?),('510300','sell',100,1.2,5,?)",
            ("2026-01-01T09:30:00", "2026-01-02T09:30:00"),
        )
        await db._connection.execute(
            "INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
            "VALUES ('588170','sell',100,1,5,?)", ("2026-01-03T09:30:00",)
        )
        await db._connection.commit()
        result = await sim.get_event_level_attribution()
        assert result["trade_count"] == 3
        assert result["by_ticker"]["510300"] == 10.0
        assert result["unmatched_sales"][0]["ticker"] == "588170"
        assert result["realized_pnl"] is None
        assert result["matched_realized_pnl"] == 10.0
    finally:
        await db.close()


def test_contract_percentage_point_units():
    from sovereign_hall.run_discussion import investment_contract_terms
    terms = investment_contract_terms(dict(direction="long", stop_loss=1, take_profit=15,
                                          expected_days=14, failure_conditions="订单取消"))
    assert terms["strategy_stop_loss_pct"] == -.01
    assert terms["strategy_take_profit_pct"] == .15
    assert terms["failure_conditions"] == "订单取消"


@pytest.mark.asyncio
async def test_partial_sales_allocate_entry_fees_once(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        for side, shares, price, fee, day in [
            ("buy", 200, 1, 5, 1), ("sell", 100, 1.2, 5, 2),
            ("sell", 100, 1.3, 5, 3),
        ]:
            await db._connection.execute(
                "INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
                "VALUES ('510300',?,?,?,?,?)",
                (side, shares, price, fee, f"2026-01-0{day}T09:30:00"),
            )
        await db._connection.commit()
        result = await sim.get_event_level_attribution(limit=1)
        assert result["trade_count"] == 3  # Never truncate inventory history.
        assert result["realized_pnl"] == 35
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_screen_rejects_explicit_security_name_mismatch(tmp_path):
    market = Market()
    original = market.get_current_quote
    async def quote(ticker):
        return {**await original(ticker), "name": "证券甲"}
    market.get_current_quote = quote
    db, sim = await make_account(tmp_path, market=market)
    try:
        accepted, rejected = await sim.screen_proposal_lot_feasibility([
            dict(ticker="002285", security_name="证券乙", direction="long"),
            dict(ticker="002186", security_name="证券甲", direction="long"),
        ])
        assert [p["ticker"] for p in accepted] == ["002186"]
        assert rejected[0]["code"] == "proposal_identity_mismatch"
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
        screening = accepted[0]["screening_quote"]
        assert screening["price"] > 0
        assert screening["name"] == "证券甲"
        assert screening["max_executable_quote"] >= screening["price"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stage3_market_reference_uses_durable_fund_identity(tmp_path, monkeypatch):
    import hashlib

    from sovereign_hall.run_discussion import format_committee_market_reference
    from sovereign_hall.services import security_identity

    ticker = "159581"
    fund_name = "万家中证红利交易型开放式指数证券投资基金"
    index_name = "中证红利指数"
    body = (
        '<meta charset="utf-8"><table class="info">'
        f"<tr><th>基金代码</th><td>{ticker}</td><th>基金全称</th><td>{fund_name}</td></tr>"
        f"<tr><th>基金简称</th><td>红利ETF万家</td><th>跟踪标的</th><td>{index_name}</td></tr>"
        "</table>"
    ).encode()

    async def fetch(code):
        now = datetime.now().astimezone()
        receipt = dict(url=f"https://fundf10.eastmoney.com/jbgk_{code}.html", status=200,
                       requested_at=now.isoformat(), captured_at=now.isoformat(),
                       sha256=hashlib.sha256(body).hexdigest())
        return security_identity.parse_fund_identity(body, receipt, ticker=code, now=now)

    market = Market()
    original = market.get_current_quote

    async def quote(code):
        return {**await original(code), "name": "红利ETF万家"}

    market.get_current_quote = quote
    monkeypatch.setattr(security_identity, "fetch_fund_identity", fetch)
    db, sim = await make_account(tmp_path, market=market)
    try:
        round_ = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="test")
        proposals = [dict(ticker=ticker, security_name=fund_name, direction="long"),
                     dict(ticker=ticker, security_name="红利低波ETF", direction="long")]
        accepted, rejected = await sim.screen_proposal_lot_feasibility(proposals, round_id=round_.id)
        assert len(accepted) == 1
        assert rejected[0]["code"] == "proposal_identity_mismatch"
        card = accepted[0]["screening_quote"]["fund_identity"]
        assert card["tracked_index"] == index_name
        assert card["publication_time"] is None
        assert card["purpose"] == "security_identity_only"
        assert "raw_body_base64" not in card
        assert index_name in format_committee_market_reference(accepted[0])
        cursor = await db._connection.execute(
            "SELECT payload_json FROM round_events WHERE event_type='ProposalIdentityChecked' ORDER BY id")
        event = json.loads((await cursor.fetchone())[0])
        assert card["source_sha256"] == event["profile"]["receipt"]["sha256"]
        assert await scalar(db, "SELECT count(*) FROM execution_intents") == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_lot_infeasible_memory_lists_only_tickers_above_current_ceiling(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        await sim._record_candidate_rejections([
            {"ticker": "600519", "code": "proposal_lot_infeasible",
             "reason": "当前筛选参考价1485.7500元高于整手可执行上限87.1303元"},
            {"ticker": "300274", "code": "proposal_lot_infeasible",
             "reason": "当前筛选参考价75.8100元高于整手可执行上限70.6900元"},
            {"ticker": "002186", "code": "committee_hold", "reason": "参考价200.0000元"},
        ], source="test")
        text = await sim.format_lot_infeasible_memory(80.0)
        assert "600519(实测1485.75元" in text
        assert "300274" not in text
        assert "002186" not in text
        assert await sim.format_lot_infeasible_memory(2000.0) == ""
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stale_account_writer_cannot_overwrite_another_fill(tmp_path):
    db, first = await make_account(tmp_path)
    second = InvestmentSimulation(db, Market())
    await second.initialize()
    try:
        assert (await first.execute_trade("510300", "long", .10, confidence=.8))["action"] == "buy"
        cash = first.cash
        stale = await second.execute_trade("588170", "long", .10, confidence=.8)
        assert stale["action"] == "error"
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
        assert float(await scalar(db, "SELECT value FROM system_stats WHERE key='simulation_cash'")) == cash
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_checkpoint_detects_unrecorded_cash_change_before_next_fill(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        assert (await sim.execute_trade("510300", "long", .10, confidence=.8))["action"] == "buy"
        await db._connection.execute("UPDATE system_stats SET value=CAST(value AS REAL)+100 WHERE key='simulation_cash'")
        await db._connection.commit()
        assert (await sim.get_performance_metrics())["reconciliation"]["status"] == "mismatch"
        result = await sim.execute_trade("588170", "long", .10, confidence=.8)
        assert result["action"] == "error"
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_observed_checkpoint_retains_legacy_orphan_sale_and_cash(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        await db._connection.execute("INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
            "VALUES ('510300','sell',100,3,5,?)", (datetime.now().isoformat(),))
        await db._connection.commit()
        async with db.transaction() as conn:
            boundary = await validate_balance_checkpoint(conn, sim.cash, {})
        assert boundary["historical_unmatched_sale_ids"] == [1]
        assert boundary["historical_cash_difference"] == -295
        assert sim.cash == 10000
        assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_transaction_enforces_limit_even_if_caller_skips_sizing(tmp_path, monkeypatch):
    db, sim = await make_account(tmp_path)
    monkeypatch.setitem(get_config().get("simulation", {}), "max_single_position", .10)
    now = datetime.now().isoformat()
    cost = sim._trade_cost("588170", "buy", price=1, shares=4000)
    request = TradeCommitRequest(ticker="588170", direction="buy", shares=4000, price=1,
        gross_amount=4000, commission=cost.commission, stamp_duty=0, slippage_cost=cost.slippage,
        total_fee=cost.total, reason="untrusted sizing fixture", quote_provider="fixture_realtime",
        quote_fetched_at=now, quote_quoted_at=now, new_cash=10000-4000-cost.total,
        new_positions={"588170": dict(shares=4000, avg_cost=1)}, idempotency_key="bad_budget")
    try:
        with pytest.raises(InvalidAccountTransition, match="budget"):
            await AtomicSimulationExecutor(db).commit(request)
        for table in ("simulation_trades", "quote_snapshots", "simulation_ledger_entries"):
            assert await scalar(db, "SELECT count(*) FROM " + table) == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stop_is_serviced_while_research_is_still_waiting_by_same_owner(tmp_path):
    market = Market()
    db, sim = await make_account(tmp_path, market=market)
    try:
        sim.cash = 9900
        sim.positions = {"588170": dict(shares=100, avg_cost=1, opened_at=datetime.now().isoformat())}
        await sim.save_state()
        duty = PortfolioMaintenance(sim, status_path=tmp_path / "status.json")
        duty.interval_seconds = .02
        owner = asyncio.current_task()
        owners = []
        review = sim.review_open_positions

        async def tracked():
            owners.append(asyncio.current_task())
            return await review()

        sim.review_open_positions = tracked
        finished = asyncio.Event()

        async def research():
            await asyncio.sleep(.04)
            market.price = .85
            async with asyncio.timeout(3):
                while sim.positions:
                    await asyncio.sleep(.01)
            assert not finished.is_set()
            return "research completes after the risk exit"

        assert await duty.during(research(), context="long_model_wait")
        finished.set()
        assert owners and set(owners) == {owner}
        assert await scalar(db, "SELECT count(*) FROM simulation_trades WHERE direction='sell'") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_cancelling_owner_joins_research_task(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        started, cancelled = asyncio.Event(), asyncio.Event()
        async def research():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        duty = PortfolioMaintenance(sim, status_path=tmp_path / "status.json")
        task = asyncio.create_task(duty.during(research(), context="cancel_fixture"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_pipeline_rechecks_positions_after_old_hold_review(tmp_path):
    market = Market()
    db, sim = await make_account(tmp_path, market=market)
    try:
        sim.cash = 9900
        sim.positions = {"588170": dict(shares=100, avg_cost=1, opened_at=datetime.now().isoformat())}
        await sim.save_state()
        earlier = await sim.review_open_positions()
        assert earlier[0]["action"] == "hold"
        market.price = .85
        result = await run_committee_approved_simulation(sim, market, None, [], pre_reviewed_positions=earlier)
        assert result["exit_fills"] == 1
        assert result["terminal"] == "filled"
        assert not sim.positions
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_nested_transaction_fails_instead_of_deadlocking(tmp_path):
    db, _ = await make_account(tmp_path)
    try:
        async with db.transaction():
            with pytest.raises(RuntimeError, match="nested"):
                async with db.transaction():
                    pass
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_recent_reflection_uses_window_and_skips_unchanged_input(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        for i in range(60):
            await db._connection.execute("INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
                "VALUES ('510300','buy',?,1,5,?)", (i+1, (datetime.now()-timedelta(days=60-i)).isoformat()))
        await db._connection.commit()
        llm = type("FixtureLLM", (), {"chat": AsyncMock(return_value="fixture reflection")})()
        history = await sim.get_trade_history(days=7)
        assert 6 <= len(history) <= 7
        await sim.daily_reflection(llm)
        prompt = llm.chat.await_args.kwargs["user"]
        assert "60股" in prompt and "11股" not in prompt
        assert "费用5.0000" in prompt
        assert await sim.daily_reflection(llm) == ""
        assert llm.chat.await_count == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_raw_fallback_keeps_actual_adjustment(tmp_path, monkeypatch):
    md = MarketDataService()
    calls = []
    async def chunk(*args, qfq=True, **kwargs):
        calls.append(qfq)
        return ([], True) if qfq else ([dict(date="2026-09-01", open=1, high=1, low=1, close=1, volume=100)], False)
    monkeypatch.setattr(md, "_eastmoney_ohlc_in_cooldown", lambda: True)
    monkeypatch.setattr(md, "_tencent_fetch_chunk", chunk)
    try:
        bars = await md.get_ohlc("510300", "20260901", "20260901")
        assert calls == [True, False]
        assert bars[0]["adjustment"] == "raw"
        assert bars[0]["provider"] == "tencent_ohlc_raw"
    finally:
        await md.close()


@pytest.mark.asyncio
async def test_current_trial_is_aligned_before_statistics(tmp_path):
    db, _ = await make_account(tmp_path)
    store = ExperimentStore(db.db_path)
    try:
        with sqlite3.connect(db.db_path) as c:
            for name, dates in [("a", ["2026-09-01", "2026-09-02"]), ("b", ["2026-09-02", "2026-09-03"])]:
                c.execute("""INSERT INTO evaluation_runs(id,sleeve,strategy_version,strategy_hash,engine_version,
                    data_cutoff,data_hash,universe_hash,universe_json,cost_model_version,cost_schedule_json,
                    initial_capital,trial_count,status,config_json,metrics_json,result_json,created_at)
                    VALUES (?,?,?,?,'fixture','2026-09-03','d','u','[]','f','{}',10000,1,'not_proven','{}','{}',?,?)""",
                    (name,"llm_research",name,name,json.dumps({"curve":[dict(date=d,net_return=.01) for d in dates]}),datetime.now().isoformat()))
        family = await store.unique_trial_returns_dated("llm_research",
            current=TrialReturnSeries("current", "hash", {"2026-09-01": .01, "2026-09-02": .02}))
        assert family.family_status == "misaligned_dates"
        assert family.quality_warnings
        assert not family.aligned_returns()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_prediction_backoff_does_not_starve_newly_matured_rows(tmp_path):
    db, _ = await make_account(tmp_path)
    recorder = DecisionRecorder(str(db.db_path))
    try:
        await recorder._ensure_tables()
        for i, name in enumerate(("bad1", "bad2", "good")):
            await db._connection.execute("INSERT INTO price_predictions(id,ticker,status,predicted_at,entry_date,expected_days) "
                "VALUES (?,'510300','pending',?,?,14)", (name, (datetime.now()-timedelta(days=40-i)).isoformat(),
                                                         (datetime.now()-timedelta(days=40-i)).isoformat()))
        await db._connection.commit()
        calls = []
        async def validate(key):
            calls.append(key)
            if key.startswith("bad"):
                return {"error": "prediction_window_data_unavailable"}
            await db._connection.execute("UPDATE price_predictions SET status='validated',result='correct' WHERE id=?", (key,))
            await db._connection.commit()
            return {"result": "correct"}
        recorder.validate_single = validate
        await recorder.validate_pending(max_count=2)
        await recorder.validate_pending(max_count=2)
        assert calls == ["bad1", "bad2", "good"]
        assert await scalar(db, "SELECT count(*) FROM prediction_validation_attempts") == 3
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_invalid_prediction_is_retained_as_unjudgeable_without_retry_loop(tmp_path):
    db, _ = await make_account(tmp_path)
    recorder = DecisionRecorder(str(db.db_path))
    try:
        await recorder._ensure_tables()
        when = (datetime.now()-timedelta(days=40)).isoformat()
        await db._connection.execute("INSERT INTO price_predictions(id,ticker,status,predicted_at,entry_date,expected_days) "
            "VALUES ('invalid','510300','pending',?,?,14)", (when, when))
        await db._connection.commit()
        recorder.validate_single = AsyncMock(return_value={"error": "invalid_prediction_anchor"})
        await recorder.validate_pending()
        assert await scalar(db, "SELECT status FROM price_predictions WHERE id='invalid'") == "unjudgeable"
        assert await scalar(db, "SELECT result FROM price_predictions WHERE id='invalid'") == "unknown"
        await recorder.validate_pending()
        assert recorder.validate_single.await_count == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_empty_account_is_awaiting_candidates_not_an_execution_failure(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        performance = await sim.get_performance_metrics()
        assert performance["execution_status"] == "no_approved_intent"
        assert performance["capital_status"] == "awaiting_candidates"
        assert performance["evaluation_status"] == "insufficient_samples"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_replay_uses_current_equity_and_identical_fee_sizing(tmp_path):
    db, sim = await make_account(tmp_path)
    try:
        sim.cash = 8000
        await sim.save_state()
        quote = await sim.get_current_quote("510300")
        plan = plan_execution(price_free_intents=[PriceFreeIntent("510300", "long", 1)],
            market_snapshot=MarketSnapshot({"510300": quote}), instrument_rules=InstrumentRuleSet(),
            limits=ExecutionPolicyLimits(1), cash_available=8000, cost_schedule=sim.cost_schedule)
        result = await sim.execute_trade("510300", "long", 1, confidence=.8, signal_count=1)
        assert result["shares"] == plan.steps[0].trade_shares
        assert result["remaining_cash"] == pytest.approx(8000 + plan.steps[0].cash_delta)
    finally:
        await db.close()


@pytest.fixture
async def evaluation(tmp_path):
    db, production = await make_account(tmp_path, "production")
    market = Market(False)
    service = ContinuousEvaluation(db, market, root=tmp_path / "forward", policy=dict(get_config().get("simulation", {})))
    await service.initialize()
    try:
        yield db, production, service, market
    finally:
        await service.close()
        await db.close()


@pytest.mark.asyncio
async def test_forward_comparison_uses_private_books_and_durable_common_inputs(evaluation):
    db, production, service, market = evaluation
    decision = dict(ticker="588170", direction="long", confidence=.58, target_position=.20, decision_id="d1")
    await service.capture_round("fixture_round", [decision])
    packet = json.loads((service.directory / "inputs/fixture_round.json").read_text())
    assert packet["intents"]["candidate"]
    assert packet["quotes"]["588170"]["fetched_at"] >= packet["research_quotes"]["588170"]["fetched_at"]
    market.opened = True
    await service.tick()
    assert service.books["control"].positions["588170"]["shares"] == 5000
    assert service.books["candidate"].positions["588170"]["shares"] == 1000
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0
    assert production.cash == 10000 and not production.positions
    assert await scalar(db, "SELECT count(*) FROM experiment_registry") == 1
    await service.capture_round("fixture_round", [decision])
    assert await scalar(service.books["candidate"].db_service, "SELECT count(*) FROM simulation_trades") == 1


@pytest.mark.asyncio
async def test_forward_daily_valuation_is_asof_and_missing_data_stays_blocked(evaluation):
    db, _, service, market = evaluation
    service.started_at = "2026-10-14T16:00:00+08:00"
    for sim in service.books.values():
        for ticker, when, price in [("510300", "2026-10-15T10:00:00", 1), ("588170", "2026-10-19T10:00:00", 2)]:
            await sim.db_service._connection.execute("INSERT INTO simulation_trades(ticker,direction,shares,price,fee,traded_at) "
                "VALUES (?,'buy',100,?,5.05,?)", (ticker, price, when))
        await sim.db_service._connection.commit()
    assert await service._value_day("2026-10-15")
    result = json.loads((service.directory / "sessions/2026-10-15.json").read_text())
    assert result["accounts"]["candidate"]["positions"] == {"510300": 100}
    assert result["accounts"]["candidate"]["total_assets"] == pytest.approx(10014.95)
    assert {r[0] for r in market.history_requests} == {"510300"}
    assert await scalar(db, "SELECT count(*) FROM evaluation_runs") == 3
    await service._value_day("2026-10-15")
    assert await scalar(db, "SELECT count(*) FROM evaluation_runs") == 3
    market.missing_history = True
    assert not await service._value_day("2026-10-16")
    assert service.last_status["evaluation_status"] == "data_blocked"
    assert service.last_status["last_usable_session"] == "2026-10-15"
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0


@pytest.mark.asyncio
async def test_interrupted_forward_input_cannot_be_overwritten_or_replayed(evaluation):
    db, _, service, _ = evaluation
    active = service.directory / "active_input.json"
    service._write(active, {"round_id": "interrupted", "decisions": []})
    await service.capture_round("later", [dict(ticker="588170", direction="long", confidence=.8, target_position=.2)])
    assert service.last_status["evaluation_status"] == "data_blocked"
    assert json.loads(active.read_text())["round_id"] == "interrupted"
    assert not (service.directory / "inputs/later.json").exists()
    assert await scalar(db, "SELECT count(*) FROM simulation_trades") == 0


@pytest.mark.asyncio
async def test_forward_restart_retains_original_registration_and_private_books(evaluation):
    db, _, service, market = evaluation
    started_at = service.started_at
    await service.capture_round("saved", [dict(ticker="588170", direction="long", confidence=.8,
                                               target_position=.2, decision_id="d")])
    await service.close()
    replacement = ContinuousEvaluation(db, market, root=service.root, policy=service.policy)
    try:
        await replacement.initialize()
        assert replacement.started_at == started_at
        assert replacement.experiment_id == service.experiment_id
        assert await scalar(db, "SELECT count(*) FROM experiment_registry") == 1
        await replacement.capture_round("saved", [])
        assert await scalar(replacement.books["candidate"].db_service, "SELECT count(*) FROM execution_intents") == 1
        replacement._write(replacement.directory / "active_tick.json", {"interrupted": True})
        replacement._next_due = 0
        await replacement.tick()
        assert replacement.last_status["evaluation_status"] == "data_blocked"
    finally:
        await replacement.close()
