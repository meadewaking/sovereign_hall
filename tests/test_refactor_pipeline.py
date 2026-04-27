import sqlite3
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.prediction_tracker import PredictionTracker
from sovereign_hall.services.backtest_engine import get_backtest_engine


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
async def test_simulation_refuses_trade_without_real_price():
    sim = InvestmentSimulation()
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
