import sqlite3
import json
import inspect
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.core import AgentRole, Document, PlaybookEntry
from sovereign_hall.core.config import get_config
from sovereign_hall.agents import get_persona
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.heuristic_policy import (
    HeuristicRiskContext,
    apply_heuristic_risk_cap,
    derive_simulation_risk_memory,
    failure_ticker_constraints,
    format_heuristic_prompt_context,
    format_heuristic_status,
    format_policy_checklist,
)
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SpiderSwarm
from sovereign_hall.services.learning_engine import LearningEngine
from sovereign_hall.services.research_discussion import ResearchDiscussionSystem
from sovereign_hall.services.prediction_tracker import PredictionTracker
from sovereign_hall.services.backtest_engine import get_backtest_engine
from sovereign_hall.services.prediction_store import ensure_prediction_tables
from sovereign_hall.run_discussion import (
    TOPIC_POOL,
    aggregate_committee_decision,
    choose_review_depth,
    build_proposal_thesis,
    build_lessons_with_heuristic_context,
    parse_committee_vote,
    proposal_priority_score,
    select_next_topic,
    stage2_deep_research,
    stage3_ic_discussion,
)
from sovereign_hall.services.persistence import PersistenceManager
import sovereign_hall.services.persistence as persistence_module


def test_entry_imports():
    import sovereign_hall.check_db  # noqa: F401
    import sovereign_hall.research_interactive  # noqa: F401
    import sovereign_hall.run_discussion  # noqa: F401


def test_check_db_safe_input_handles_closed_stdin(monkeypatch):
    import sovereign_hall.check_db as check_db

    def raise_eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)

    assert check_db.safe_input("choice: ") is None


def test_check_db_realtime_quotes_are_opt_in(monkeypatch):
    import sovereign_hall.check_db as check_db

    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)
    assert check_db.realtime_quotes_enabled() is False

    monkeypatch.setenv("SOVEREIGN_HALL_REALTIME_QUOTES", "1")
    assert check_db.realtime_quotes_enabled() is True


def test_market_data_ticker_mapping():
    svc = MarketDataService()
    assert svc.infer_market("600519") == "sh"
    assert svc.infer_market("159995") == "sz"
    assert svc.eastmoney_secid("512880") == "1.512880"


def test_llm_client_uses_configured_defaults():
    client = LLMClient(provider="local")
    llm_config = get_config().get_llm_config()

    assert client.timeout == llm_config.get("timeout")
    assert client.max_retries == llm_config.get("max_retries", 3)
    assert client.retry_delay == llm_config.get("retry_delay", 2.0)
    assert client.max_concurrent == llm_config.get("max_concurrent")


@pytest.mark.asyncio
async def test_spider_uses_nested_rate_limit_config():
    config = get_config()
    original = dict(config.get_spider_config())
    config.set("spider.max_concurrent", 4)
    config.set("spider.rate_limit", {"requests_per_minute": 6, "burst": 2})

    spider = None
    try:
        spider = SpiderSwarm()
        assert spider.max_concurrent == 4
        assert spider.rate_limiter.rate == pytest.approx(0.1)
        assert spider.rate_limiter.burst == 2
    finally:
        if spider:
            await spider.close()
        config._config["spider"] = original


@pytest.mark.asyncio
async def test_market_data_cools_down_eastmoney_after_repeated_ohlc_failures(monkeypatch):
    class FailingClient:
        is_closed = False

        def __init__(self):
            self.calls = 0

        async def get(self, url, params=None):
            self.calls += 1
            request = httpx.Request("GET", url, params=params)
            response = httpx.Response(502, request=request)
            raise httpx.HTTPStatusError("bad gateway", request=request, response=response)

        async def aclose(self):
            pass

    svc = MarketDataService()
    await svc._client.aclose()
    failing_client = FailingClient()
    svc._client = failing_client

    async def fake_tencent(*_args, **_kwargs):
        return []

    async def fake_akshare(*_args, **_kwargs):
        return [{"date": "2026-06-10", "open": 1.0, "close": 1.0, "high": 1.0, "low": 1.0, "volume": 0}]

    monkeypatch.setattr(svc, "_fetch_tencent_ohlc", fake_tencent)
    monkeypatch.setattr(svc, "_fetch_akshare_ohlc", fake_akshare)

    for _ in range(4):
        bars = await svc.get_ohlc("600519", "2026-06-01", "2026-06-10")
        assert bars

    assert failing_client.calls == 3
    await svc.close()


@pytest.mark.asyncio
async def test_document_can_be_stored(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    doc = Document(
        title="测试标题",
        content="这是一段有效的测试文档内容，足够长，可以被写入数据库。",
        url="https://example.com/a",
        source="unit",
        sector="测试",
        keywords=["测试"],
    )

    await db.add_document(doc)

    assert await db.count_documents() == 1
    stored = await db.get_document(doc.id)
    assert stored["title"] == "测试标题"
    await db.close()


@pytest.mark.asyncio
async def test_dict_proposal_can_be_stored(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()

    long_thesis = "测试提案" + "完整论证" * 2000

    await db.add_proposal({
        "ticker": "512880",
        "direction": "long",
        "target_position": 0.1,
        "entry_price": 1.0,
        "stop_loss": 0.95,
        "take_profit": 1.15,
        "holding_period": 30,
        "confidence": 0.6,
        "thesis": long_thesis,
        "sector": "半导体",
    })

    proposals = await db.get_proposals(limit=5)
    assert len(proposals) == 1
    assert proposals[0]["ticker"] == "512880"
    assert proposals[0]["thesis"] == long_thesis
    await db.close()


@pytest.mark.asyncio
async def test_decision_records_absolute_prices(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    decision_id = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        entry_price=10.0,
        expected_days=30,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT current_price, target_price, stop_loss FROM price_predictions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row == (10.0, 11.5, 9.5)


@pytest.mark.asyncio
async def test_validate_pending_waits_for_expected_window(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=30,
    )

    result = await recorder.validate_pending(max_count=10)
    assert result["validated"] == 0


@pytest.mark.asyncio
async def test_simulation_refuses_trade_without_real_price(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.get_current_price = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=None,
    )

    assert result["success"] is False
    assert "真实价格" in result["reason"]


@pytest.mark.asyncio
async def test_simulation_blocks_on_non_trading_day(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=False)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.get_current_price = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=None,
    )

    assert result["success"] is False
    assert "非交易日" in result["reason"]


@pytest.mark.asyncio
async def test_simulation_does_not_buy_for_short_without_position(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="short",
        target_position=0.1,
        current_price=10.0,
    )

    assert result["success"] is False
    assert result["action"] == "hold"
    assert sim.positions == {}


def test_heuristic_risk_cap_uses_latest_policy_as_constraint(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[],
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.25, 0.7, context=context)

    assert capped == 0.10
    assert "限制" in reason
    assert "样本外风险" in reason


def test_heuristic_risk_cap_tightens_recent_failure_ticker(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "market_state": {"ticker": "000977"},
                "signals": {},
                "positions": {},
            }
        ],
    )

    capped, reason = apply_heuristic_risk_cap("000977.SZ", 0.10, 0.8, context=context)

    assert capped == 0.05
    assert "failure case" in reason


def test_heuristic_risk_cap_warns_on_thin_cost_stress(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.031,
        max_position=0.08,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.157,
        cost_stress_score=0.014,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.06, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.06
    assert "成本扰动余量很薄" in reason
    assert "3x滑点 0.014000" in status
    assert "样本外score=0.157000" in prompt


def test_heuristic_risk_cap_tightens_failed_etf_sleeve(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.027,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        sleeve_diagnostics={
            "allocator_status": "not_promoted",
            "sleeves": {
                "etf": {
                    "score": -0.06,
                    "cost_stress_score": -0.09,
                    "promotable": False,
                    "reason": "主样本score未转正；3x滑点余量低于0.02",
                },
                "single_stock": {
                    "score": 0.027,
                    "cost_stress_score": 0.011,
                    "promotable": False,
                    "reason": "3x滑点余量低于0.02",
                },
            },
        },
    )

    capped, reason = apply_heuristic_risk_cap("512880", 0.08, 0.8, context=context)
    single_capped, single_reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.04
    assert "ETF sleeve" in reason
    assert single_capped == 0.08
    assert single_reason is None
    assert "sleeve allocator: not_promoted" in status
    assert "etf cap/warning score=-0.060000" in prompt


def test_heuristic_risk_cap_uses_reduced_single_stock_cap(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap6",
        score=0.061,
        max_position=0.06,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        out_of_sample_score=0.073,
        cost_stress_score=0.053,
        sleeve_diagnostics={
            "allocator_status": "not_promoted",
            "sleeves": {
                "etf": {
                    "score": -0.09,
                    "cost_stress_score": -0.11,
                    "promotable": False,
                    "reason": "主样本score未转正；3x滑点余量低于0.02",
                },
                "single_stock": {
                    "score": 0.061,
                    "cost_stress_score": 0.053,
                    "promotable": True,
                    "reason": "通过主样本、样本外和3x滑点检查",
                },
            },
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)

    assert capped == pytest.approx(0.06)
    assert "限制到6.0%" in reason
    assert "single_stock pass score=0.061000" in status


def test_heuristic_context_warns_when_price_source_is_unvalidated(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap6",
        score=0.056,
        max_position=0.06,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.095,
        cost_stress_score=0.049,
        price_source="prediction current_price fallback; daily_prices table unavailable or empty",
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.06, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.03)
    assert "限制到3.0%" in reason
    assert "daily_prices缺失" in reason
    assert "禁止放大仓位" in reason
    assert "数据质量风险" in status
    assert "弱价格覆盖模拟买入上限: 3.0%" in status
    assert "弱价格覆盖仓位<=3.0%" in prompt
    assert "current_price fallback" in prompt


def test_heuristic_context_surfaces_min_signal_count(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.056,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.068,
        cost_stress_score=0.052,
        min_signal_count=2,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.05
    assert "至少2条本地同日预测观察" in reason
    assert "本地信号观察门槛: >=2" in status
    assert "本地信号观察门槛=2条" in prompt


def test_heuristic_context_surfaces_price_coverage(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_source="prediction current_price fallback; daily_prices table unavailable or empty",
        price_coverage={
            "status": "unvalidated_prediction_current_price_fallback",
            "independent_price_row_ratio": 0.0,
            "missing_position_price_slot_ratio": 0.3778,
            "missing_price_day_ratio": 0.2791,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.025)
    assert "持仓缺价槽位37.8%" in reason
    assert "弱覆盖模拟买入上限2.5%" in reason
    assert "价格覆盖" in status
    assert "弱价格覆盖模拟买入上限: 2.5%" in status
    assert "daily_prices覆盖0.0%" in prompt
    assert "弱覆盖模拟买入上限=2.5%" in prompt


def test_simulation_trade_losses_derive_risk_memory():
    failures = derive_simulation_risk_memory([
        {
            "id": 1,
            "ticker": "512880",
            "direction": "buy",
            "shares": 1000,
            "price": 1.0,
            "fee": 0.3,
            "traded_at": "2026-06-01T09:30:00",
        },
        {
            "id": 2,
            "ticker": "512880",
            "direction": "sell",
            "shares": 1000,
            "price": 0.95,
            "fee": 1.235,
            "traded_at": "2026-06-02T09:30:00",
        },
    ])

    assert len(failures) == 1
    assert failures[0]["ticker"] == "512880"
    assert failures[0]["last_loss_pct"] < -0.03
    assert failures[0]["expires_at"].startswith("2026-06-10")


@pytest.mark.asyncio
async def test_simulation_refreshes_closed_loss_risk_memory(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    conn = db._connection
    await conn.executemany(
        """
        INSERT INTO simulation_trades (ticker, direction, shares, price, fee, reason, traded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("512880", "buy", 1000, 1.0, 0.3, "entry", "2026-06-01T09:30:00"),
            ("512880", "sell", 1000, 0.95, 1.235, "exit", "2026-06-02T09:30:00"),
        ],
    )
    await conn.commit()

    failures = await sim.refresh_simulation_risk_memory()
    async with conn.execute("SELECT ticker, last_loss_pct FROM simulation_risk_memory") as cursor:
        rows = await cursor.fetchall()
    await db.close()

    assert failures[0]["ticker"] == "512880"
    assert rows[0][0] == "512880"
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.03,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        simulation_failures=failures,
    )
    capped, reason = apply_heuristic_risk_cap("512880", 0.08, 0.8, context=context)

    assert capped == 0.04
    assert "模拟账户近期已实现亏损风险记忆" in reason


def test_format_heuristic_status_includes_failure_cases(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-10..2026-05-15",
                "suspected_reason": "entry reversed quickly",
            }
        ],
    )

    status = format_heuristic_status(context)

    assert "cost_robust_hold4" in status
    assert "worst_trade" in status


def test_format_heuristic_prompt_context_marks_failure_tickers(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.067,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-27..2026-05-30",
                "market_state": {"ticker": "688256"},
                "suspected_reason": "entry reversed quickly",
            }
        ],
    )

    prompt = format_heuristic_prompt_context(context)

    assert "本地Heuristic风控约束" in prompt
    assert "688256" in prompt
    assert "不得编造成外部市场事实" in prompt
    assert "限制到4.0%或观望" in prompt


def test_heuristic_policy_checklist_surfaces_promoted_gates(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.05,
        max_position=0.05,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        min_signal_count=2,
        min_confidence=0.66,
        min_risk_reward=0.9,
        min_holding_days=6,
        max_gross=0.2,
        universe="single_stock",
    )

    checklist = format_policy_checklist(context)
    prompt = format_heuristic_prompt_context(context)

    assert "置信度>=66%" in checklist
    assert "风险收益比>=0.90" in checklist
    assert "最短持有>=6天" in checklist
    assert "组合总模拟仓位<=20%" in checklist
    assert "Heuristic入场校验" in prompt


def test_failure_ticker_constraints_explain_exact_cap(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="no_lookahead_failure_half_size",
        score=0.08,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-27..2026-05-30",
                "market_state": {"ticker": "688256"},
                "suspected_reason": "entry reversed quickly",
            }
        ],
        failure_ticker_scale=0.5,
    )

    constraints = failure_ticker_constraints(context)

    assert constraints == [
        {
            "ticker": "688256",
            "max_simulated_position": 0.04,
            "action": "cap_to_failure_scale_and_require_new_evidence",
            "reason": "worst_trade",
        }
    ]


def test_run_discussion_appends_heuristic_context(monkeypatch):
    monkeypatch.setattr(
        "sovereign_hall.run_discussion.format_heuristic_prompt_context",
        lambda: "【本地Heuristic风控约束】failure tickers: 688256",
    )

    prompt = build_lessons_with_heuristic_context("【历史教训】控制换手")

    assert "【历史教训】控制换手" in prompt
    assert "failure tickers: 688256" in prompt


def test_interactive_research_extracts_general_investment_keywords():
    system = ResearchDiscussionSystem.__new__(ResearchDiscussionSystem)

    keywords = system._generate_search_keywords("选择一只三个月左右适合持有的矿业股票", AgentRole.CYCLE_ANALYST)

    assert "选择一只三个月左右适合持有的矿业股票" in keywords
    assert "持有期三个月" in keywords
    assert "股票" in keywords
    assert "周期" in keywords


@pytest.mark.asyncio
async def test_simulation_applies_heuristic_position_cap(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.setattr(
        "sovereign_hall.services.investment_simulation.apply_heuristic_risk_cap",
        lambda ticker, target_position, confidence: (0.10, "heuristic cap"),
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.25,
        current_price=9.0,
        reason="committee",
    )

    assert result["action"] == "buy"
    assert sim.positions["600519"]["shares"] == 100


@pytest.mark.asyncio
async def test_prediction_tracker_waits_for_window(tmp_path):
    db_path = tmp_path / "test.db"
    tracker = PredictionTracker(str(db_path))
    await tracker.create_prediction(
        conclusion_id="",
        ticker="600519",
        current_price=10.0,
        target_price=11.0,
        stop_loss=9.5,
        direction="long",
        confidence=0.7,
        expected_days=30,
    )

    assert await tracker.validate_predictions() == 0


def test_backtest_singleton_returns_instance():
    assert get_backtest_engine() is not None


def test_expected_days_are_normalized():
    assert DecisionRecorder.normalize_expected_days(1) == 3
    assert DecisionRecorder.normalize_expected_days(365) == 180
    assert DecisionRecorder.normalize_expected_days(None, "短线事件驱动") == 14
    assert DecisionRecorder.normalize_expected_days(None, "半年产业趋势") == 120


@pytest.mark.asyncio
async def test_decision_records_dynamic_expected_days(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    decision_id = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT expected_days FROM price_predictions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row == (7,)


@pytest.mark.asyncio
async def test_recent_duplicate_decision_reuses_existing_id(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    first = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )
    second = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.72,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM price_predictions").fetchone()[0]
    conn.close()

    assert second == first
    assert count == 1


@pytest.mark.asyncio
async def test_prediction_schema_migrates_existing_table(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE price_predictions (id TEXT PRIMARY KEY, ticker TEXT NOT NULL)")
    conn.commit()
    conn.close()

    await ensure_prediction_tables(str(db_path))

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(price_predictions)")}
    daily_prices_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_prices'"
    ).fetchone()
    conn.close()

    assert {"entry_date", "discussion_context", "expected_days"}.issubset(columns)
    assert daily_prices_exists is not None


@pytest.mark.asyncio
async def test_learning_engine_generates_error_profiles(tmp_path):
    db_path = tmp_path / "test.db"
    await ensure_prediction_tables(str(db_path))
    conn = sqlite3.connect(db_path)
    rows = [
        ("p1", "600519", "long", 0.82, 30, "wrong", 0.0, "事实: 估值修复 审议深度: full; vote_margin=0.10"),
        ("p2", "000858", "long", 0.78, 30, "partial", 0.3, "事实: 消费修复 审议深度: focused; vote_margin=0.05"),
        ("p3", "512880", "short", 0.45, 7, "correct", 1.0, "事实: 交易拥挤"),
    ]
    for row in rows:
        conn.execute(
            """
            INSERT INTO price_predictions (
                id, ticker, direction, confidence, expected_days, status, result,
                accuracy_score, discussion_context, predicted_at, validated_at
            ) VALUES (?, ?, ?, ?, ?, 'validated', ?, ?, ?, datetime('now'), datetime('now'))
            """,
            row,
        )
    conn.commit()
    conn.close()

    engine = LearningEngine(str(db_path))
    profiles = await engine.analyze_error_profiles()
    prompt = await engine.generate_lessons_prompt()

    assert profiles
    assert profiles[0]["direction"] == "long"
    assert "错误画像" in prompt
    assert "600519" in prompt


def test_committee_votes_can_defer_to_hold():
    decision = aggregate_committee_decision(
        {"confidence": 0.8, "target_position": 0.2},
        ["【投票】观望 | 置信度: 70% | 仓位: 0%"] * 7,
    )

    assert decision["direction"] == "hold"
    assert decision["target_position"] == 0.0


def test_committee_vote_accepts_structured_json():
    vote = parse_committee_vote(
        '{"direction":"long","confidence":0.62,"position":0.08,'
        '"risk_flags":["估值偏高"],"invalid_if":"跌破支撑"}'
    )

    assert vote["direction"] == "long"
    assert vote["confidence"] == pytest.approx(0.62)
    assert vote["position"] == pytest.approx(0.08)
    assert vote["risk_flags"] == ["估值偏高"]


def test_committee_aggregation_uses_custom_vote_weights():
    decision = aggregate_committee_decision(
        {"confidence": 0.5, "target_position": 0.1},
        [
            '{"direction":"hold","confidence":0.7,"position":0}',
            '{"direction":"short","confidence":0.6,"position":0.05}',
            '{"direction":"short","confidence":0.6,"position":0.05}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert decision["direction"] == "short"
    assert decision["vote_summary"]["hold"] == pytest.approx(2.0)
    assert decision["vote_margin"] > 0


def test_proposal_review_depth_tracks_priority():
    weak = {"ticker": "159995", "confidence": 0.42, "target_position": 0.03, "thesis": "推断: 主题轮动"}
    strong = {
        "ticker": "600519",
        "confidence": 0.76,
        "target_position": 0.18,
        "thesis": "事实: 业绩改善；证据: 财报；否决条件: 需求回落",
    }

    assert proposal_priority_score(strong) > proposal_priority_score(weak)
    assert choose_review_depth(weak) == "light"
    assert choose_review_depth(strong) == "full"


def test_topic_pool_resets_after_full_cycle_and_skips_recent(monkeypatch):
    monkeypatch.setattr("sovereign_hall.run_discussion.save_completed_topics", lambda topics: None)
    completed = set(TOPIC_POOL)
    topic = select_next_topic(completed, recent_topics={TOPIC_POOL[0]})

    assert completed == set()
    assert topic == TOPIC_POOL[1]


def test_topic_selection_falls_back_to_oldest_recent_when_pool_saturated(monkeypatch):
    monkeypatch.setattr("sovereign_hall.run_discussion.save_completed_topics", lambda topics: None)
    recent_topics = {
        topic: f"2026-05-27T{hour:02d}:00:00"
        for hour, topic in enumerate(TOPIC_POOL)
    }
    completed = set(TOPIC_POOL[1:])

    topic = select_next_topic(completed, recent_topics=recent_topics)

    assert topic == TOPIC_POOL[0]
    assert completed == set()


def test_persistence_preserves_token_breakdown(tmp_path, monkeypatch):
    stats_file = tmp_path / "session_stats.json"
    history_dir = tmp_path / "history"
    stats_file.write_text(json.dumps({
        "start_time": "2026-01-01T00:00:00",
        "total_rounds": 2,
        "total_time_seconds": 12.5,
        "topics_discussed": [],
        "proposals_generated": 0,
        "winning_proposals": 0,
        "token_stats": {
            "total_tokens": 100,
            "total_cost_usd": 0.2,
            "total_requests": 3,
            "prompt_tokens": 40,
            "completion_tokens": 50,
            "unattributed_tokens": 10,
        },
        "last_updated": "2026-01-01T00:00:00",
    }), encoding="utf-8")
    monkeypatch.setattr(persistence_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(persistence_module, "STATS_FILE", stats_file)
    monkeypatch.setattr(persistence_module, "HISTORY_DIR", history_dir)

    manager = PersistenceManager()
    loaded = manager.load_previous_stats()
    manager.add_time(7.5)

    saved = json.loads(stats_file.read_text(encoding="utf-8"))
    assert loaded["prompt_tokens"] == 40
    assert loaded["completion_tokens"] == 50
    assert loaded["unattributed_tokens"] == 10
    assert saved["total_time_seconds"] == 20.0


def test_agent_system_prompt_discourages_repetition_and_requires_evidence():
    prompt = get_persona(AgentRole.CIO).get_system_prompt()

    assert "不复述题目" in prompt
    assert "已验证事实" in prompt
    assert "证据不足" in prompt
    assert "不要为了节省token删减" in prompt


def test_core_discussion_prompts_are_evidence_rich_and_machine_readable():
    stage2_source = inspect.getsource(stage2_deep_research)
    stage3_source = inspect.getsource(stage3_ic_discussion)

    assert "只输出合法JSON" in stage2_source
    assert "证据不足时输出空数组" in stage2_source
    assert "max_tokens=8000" in stage2_source
    assert "build_structured_vote_prompt" in stage3_source
    assert "review_depth" in stage3_source
    assert "max_tokens=3000" in stage3_source


def test_proposal_thesis_preserves_evidence_and_reject_conditions():
    thesis = build_proposal_thesis({
        "thesis": "事实: 订单增长；推断: 盈利弹性提升",
        "evidence": ["公告披露新订单", "行业价格回暖"],
        "reject_if": "订单取消或毛利率继续下滑",
    })

    assert "事实: 订单增长" in thesis
    assert "证据: 公告披露新订单；行业价格回暖" in thesis
    assert "否决条件: 订单取消或毛利率继续下滑" in thesis


@pytest.mark.asyncio
async def test_database_migrates_legacy_blacklist_schema(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE blacklist (ticker TEXT PRIMARY KEY, reason TEXT, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO blacklist (ticker, reason, created_at) VALUES (?, ?, ?)",
        ("600519", "legacy", "2026-01-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.add_to_blacklist("600519", "again")
    await db.close()

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(blacklist)")}
    row = conn.execute(
        "SELECT failure_count, added_at FROM blacklist WHERE ticker = ?",
        ("600519",),
    ).fetchone()
    conn.close()

    assert {"failure_count", "added_at", "expires_at"}.issubset(columns)
    assert row[0] == 2
    assert row[1] is not None


@pytest.mark.asyncio
async def test_playbook_insert_supports_legacy_schema(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE playbook (
            entry_id TEXT PRIMARY KEY,
            category TEXT,
            situation TEXT,
            action_taken TEXT,
            outcome TEXT,
            lesson TEXT,
            confidence_delta REAL,
            ticker TEXT,
            refs TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.add_playbook_entry(PlaybookEntry(
        ticker="600519",
        situation="高估值回撤",
        lesson="等待确认信号",
        outcome="avoided_loss",
        confidence_delta=0.2,
        pattern="risk",
        action="hold",
        examples=["case-1"],
    ))
    await db.close()

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT category, action_taken, lesson, confidence_delta, ticker, refs FROM playbook"
    ).fetchone()
    conn.close()

    assert row[0] == "risk"
    assert row[1] == "hold"
    assert row[2] == "等待确认信号"
    assert row[3] == 0.2
    assert row[4] == "600519"
    assert json.loads(row[5]) == ["case-1"]


@pytest.mark.asyncio
async def test_report_conclusion_ids_are_backfilled_and_new_rows_get_id(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE report_conclusions (
            id INT,
            question TEXT,
            conclusion TEXT,
            ticker TEXT,
            position REAL,
            stop_loss REAL,
            take_profit REAL,
            holding_period TEXT,
            confidence REAL,
            key_points TEXT,
            risks TEXT,
            created_at TEXT,
            learned_at TEXT
        )
    """)
    conn.execute("INSERT INTO report_conclusions (question, conclusion) VALUES (?, ?)", ("q1", "c1"))
    conn.execute("INSERT INTO report_conclusions (id, question, conclusion) VALUES (?, ?, ?)", (10, "q2", "c2"))
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db.init_report_tables()
    await db.save_report_conclusion("q3", "c3", ticker="600519")
    await db.close()

    conn = sqlite3.connect(db_path)
    null_ids, total, max_id = conn.execute(
        "SELECT SUM(id IS NULL), COUNT(*), MAX(id) FROM report_conclusions"
    ).fetchone()
    ids = [row[0] for row in conn.execute("SELECT id FROM report_conclusions ORDER BY id")]
    conn.close()

    assert null_ids == 0
    assert total == 3
    assert max_id == 12
    assert ids == [10, 11, 12]
