import sqlite3
import json
import inspect
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.core import AgentRole, Document, PlaybookEntry
from sovereign_hall.agents import get_persona
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.heuristic_policy import (
    HeuristicRiskContext,
    apply_heuristic_risk_cap,
)
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.prediction_tracker import PredictionTracker
from sovereign_hall.services.backtest_engine import get_backtest_engine
from sovereign_hall.services.prediction_store import ensure_prediction_tables
from sovereign_hall.run_discussion import (
    TOPIC_POOL,
    aggregate_committee_decision,
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


def test_market_data_ticker_mapping():
    svc = MarketDataService()
    assert svc.infer_market("600519") == "sh"
    assert svc.infer_market("159995") == "sz"
    assert svc.eastmoney_secid("512880") == "1.512880"


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

    await db.add_proposal({
        "ticker": "512880",
        "direction": "long",
        "target_position": 0.1,
        "entry_price": 1.0,
        "stop_loss": 0.95,
        "take_profit": 1.15,
        "holding_period": 30,
        "confidence": 0.6,
        "thesis": "测试提案",
        "sector": "半导体",
    })

    proposals = await db.get_proposals(limit=5)
    assert len(proposals) == 1
    assert proposals[0]["ticker"] == "512880"
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


def test_committee_votes_can_defer_to_hold():
    decision = aggregate_committee_decision(
        {"confidence": 0.8, "target_position": 0.2},
        ["【投票】观望 | 置信度: 70% | 仓位: 0%"] * 7,
    )

    assert decision["direction"] == "hold"
    assert decision["target_position"] == 0.0


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
    assert "第一行必须是：【投票】" in stage3_source
    assert "max_tokens=3000" in stage3_source


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
