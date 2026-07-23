import csv
import sqlite3
import json
import inspect
import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.core import AgentRole, Document, PlaybookEntry
from sovereign_hall.core.config import get_config
from sovereign_hall.agents import get_persona
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.portfolio_policy import (
    deployment_position_floor,
    deployment_status,
    review_position,
)
from sovereign_hall.services.reward_policy import (
    MAX_DAILY_TRADES,
    capital_reward_breakdown,
    idle_cash_exposure_penalty,
    limit_rebalance_actions,
)
from sovereign_hall.services.heuristic_policy import (
    HeuristicRiskContext,
    apply_heuristic_risk_cap,
    build_price_readiness_stall_report,
    derive_simulation_risk_memory,
    failure_ticker_constraints,
    format_heuristic_prompt_context,
    format_heuristic_status,
    format_price_readiness_backfill_plan,
    format_price_readiness_backfill_queue,
    format_price_readiness_stall_note,
    format_policy_checklist,
    prepare_candidate_rejection_feedback,
    recent_prediction_observation_count,
    sanitize_candidate_rejection_reason,
)
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SpiderSwarm
from sovereign_hall.services.learning_engine import LearningEngine
from sovereign_hall.services.research_discussion import ResearchDiscussionSystem
from sovereign_hall.services.prediction_tracker import PredictionTracker
from sovereign_hall.services.backtest_engine import get_backtest_engine
from sovereign_hall.services.prediction_store import ensure_prediction_tables
from sovereign_hall.utils import format_cost_breakdown, format_token, format_token_breakdown
from sovereign_hall.run_discussion import (
    TOPIC_POOL,
    aggregate_committee_decision,
    choose_review_depth,
    build_proposal_thesis,
    build_lessons_with_heuristic_context,
    cli_args_can_run_without_instance_lock,
    filter_repeated_rejection_proposals,
    parse_committee_vote,
    preflight_committee_decisions,
    proposal_priority_score,
    select_next_topic,
    stage2_deep_research,
    stage3_ic_discussion,
    run_committee_approved_simulation,
)
from sovereign_hall.services.persistence import PersistenceManager
import sovereign_hall.services.persistence as persistence_module


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_script_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_entry_imports():
    import sovereign_hall.check_db  # noqa: F401
    import sovereign_hall.research_interactive  # noqa: F401
    import sovereign_hall.run_discussion  # noqa: F401


def test_run_discussion_help_does_not_need_instance_lock():
    assert cli_args_can_run_without_instance_lock(["--help"]) is True
    assert cli_args_can_run_without_instance_lock(["--once"]) is False


def test_capital_reward_prioritizes_net_return_and_penalizes_long_idle_cash():
    invested = {
        "total_return": 0.08,
        "max_drawdown": -0.05,
        "cost_paid": 0.01,
        "idle_cash_penalty": idle_cash_exposure_penalty([0.03] * 20),
    }
    lower_return = {**invested, "total_return": 0.05}
    idle = {
        **invested,
        "idle_cash_penalty": idle_cash_exposure_penalty([0.80] * 20),
    }

    assert capital_reward_breakdown(invested)["score"] > capital_reward_breakdown(lower_return)["score"]
    assert capital_reward_breakdown(invested)["score"] > capital_reward_breakdown(idle)["score"]
    assert idle_cash_exposure_penalty([0.80] * 20) > idle_cash_exposure_penalty([0.80, 0.03] * 10)


def test_rebalance_daily_limit_prioritizes_exits_before_buys():
    current = {f"old{i}": 0.10 for i in range(6)}
    target = {f"new{i}": 0.10 for i in range(6)}

    limited, deferred = limit_rebalance_actions(current, target, MAX_DAILY_TRADES)

    assert len(set(current) - set(limited)) == MAX_DAILY_TRADES
    assert not (set(limited) & set(target))
    assert deferred == 7


def test_backtest_never_exceeds_five_transactions_per_day():
    module = load_script_module(
        "run_heuristic_cycle_daily_limit_module",
        "scripts/run_heuristic_cycle.py",
    )
    rows = []
    for day_index, day in enumerate(("2026-01-01", "2026-01-02", "2026-01-03")):
        for ticker_index in range(10):
            rows.append({
                "date": day,
                "ticker": f"{ticker_index:06d}",
                "price": 10.0 + day_index * 0.1,
                "confidence": 0.9,
                "risk_reward": 2.0,
                "close_observations": 3,
                "stop_gap": 0.05,
                "return_1d": 0.01,
                "signal_strength": 1.0 - ticker_index * 0.01,
                "price_source": "daily_prices",
            })
    result = module.run_backtest(
        module.pd.DataFrame(rows),
        module.PolicyConfig(
            name="daily_limit",
            max_names=10,
            max_position=0.10,
            max_gross=1.0,
            min_confidence=0.65,
            min_risk_reward=0.8,
        ),
        module.CostConfig(),
    )

    assert int(result["curve"]["trade_count"].max()) <= MAX_DAILY_TRADES
    assert result["metrics"]["max_daily_trade_count"] <= MAX_DAILY_TRADES
    assert result["metrics"]["trade_count"] == int(result["curve"]["trade_count"].sum())


def test_backtest_marks_held_ticker_from_price_history_without_new_signal():
    module = load_script_module(
        "run_heuristic_cycle_independent_marks_module",
        "scripts/run_heuristic_cycle.py",
    )
    daily = module.pd.DataFrame(
        [
            {"date": "2026-01-01", "ticker": "600519", "price": 10.0},
            {"date": "2026-01-02", "ticker": "510300", "price": 4.0},
        ]
    )
    history = module.pd.DataFrame(
        [
            {"date": "2026-01-01", "ticker": "600519", "close": 10.0},
            {"date": "2026-01-02", "ticker": "600519", "close": 10.2},
        ]
    )

    marks = module.build_mark_prices_by_date(daily, history)

    assert marks["2026-01-02"]["600519"] == pytest.approx(10.2)


def test_price_readiness_accepts_near_complete_history_when_latest_date_is_covered():
    module = load_script_module(
        "heuristic_cycle_near_complete_readiness_module",
        "scripts/run_heuristic_cycle.py",
    )
    rows = []
    for index in range(20):
        rows.append(
            {
                "ticker": f"{index:06d}",
                "date": "2026-01-02",
                "price_source": "daily_prices",
                "close_observations": 1,
            }
        )
    rows.append(
        {
            "ticker": "999999",
            "date": "2026-01-01",
            "price_source": "prediction_current_price",
            "close_observations": 1,
        }
    )

    readiness = module.build_price_readiness_report(module.pd.DataFrame(rows), module.pd.DataFrame())

    assert readiness["status"] == "ready_with_historical_provider_gaps"
    assert readiness["latest_missing_tickers"] == []


def test_committee_preflight_records_every_non_executable_decision():
    executable, rejected = preflight_committee_decisions(
        [
            {"ticker": "600519.SH", "direction": "hold", "risk_flags": ["证据不足"]},
            {"ticker": "", "direction": "long", "target_position": 0.1},
            {"ticker": "600050", "direction": "short"},
            {"ticker": "推荐标的代码", "direction": "hold"},
            {"ticker": "510300", "direction": "long", "target_position": 0.1},
        ],
        current_tickers=set(),
        normalize_ticker=lambda ticker: ticker.replace(".SH", ""),
    )

    assert [row["ticker"] for row in executable] == ["510300"]
    assert {row["code"] for row in rejected} == {
        "committee_hold",
        "missing_ticker",
        "invalid_ticker",
        "short_without_position",
    }
    assert "证据不足" in rejected[0]["reason"]


def test_rejection_feedback_keeps_audit_but_removes_obsolete_price_claims():
    raw_reason = (
        "投委会证据未形成多头/退出裁决；risk_flags="
        "标的与逻辑错配未纠正,连续9轮partial daily_prices覆盖无进展,"
        "止损物理性失效：98%缺价交易日导致退出无法执行"
    )

    active, superseded = sanitize_candidate_rejection_reason(raw_reason)
    prepared = prepare_candidate_rejection_feedback([{"last_reason": raw_reason}])[0]

    assert "标的与逻辑错配" in active
    assert "daily_prices" not in active
    assert "98%" not in active
    assert len(superseded) == 2
    assert prepared["last_reason"] == raw_reason
    assert prepared["feedback_reason"] == active
    assert prepared["feedback_usable"] is True


def test_redeployment_state_preserves_raw_blocker_but_returns_sanitized_view(tmp_path):
    async def run():
        db = DatabaseService(str(tmp_path / "test.db"))
        await db._init_db()
        sim = InvestmentSimulation(db)
        await sim.init_tables()
        await sim._write_redeployment_state(
            status="blocked_no_approved_candidates",
            deployment_gap=1000.0,
            blocker_code="missing_approved_candidates",
            blocker_reason=(
                "投委会未批准；risk_flags=止损物理性失效：98%缺价交易日导致平仓指令无法执行,"
                "标的与逻辑错配"
            ),
            next_action="重新研究",
            source="test",
        )
        state = await sim.get_redeployment_state()
        raw = (
            await db._connection.execute_fetchall(
                "SELECT blocker_reason FROM simulation_redeployment_state WHERE id=1"
            )
        )[0][0]
        await db.close()
        return state, raw

    state, raw = __import__("asyncio").run(run())
    assert "98%" in raw
    assert "98%" in state["blocker_reason_audit"]
    assert "98%" not in state["blocker_reason"]
    assert "标的与逻辑错配" in state["blocker_reason"]


def test_repeated_candidate_requires_traceable_evidence_delta_during_cooldown():
    now = datetime(2026, 7, 22, 14, 0, 0)
    memory = [{
        "ticker": "159995",
        "code": "committee_hold",
        "rejection_count": 48,
        "last_seen_at": "2026-07-22T10:00:00",
        "feedback_usable": True,
        "feedback_reason": "标的与逻辑错配未纠正",
    }]
    unchanged = {"ticker": "159995", "direction": "long", "confidence": 0.8}
    traceable = unchanged | {
        "resolved_rejection": "标的与逻辑错配未纠正",
        "evidence_delta": "本地文档doc-42确认该ETF成分与提案主题一致",
        "evidence": ["doc-42 成分核验"],
    }

    eligible, rejected = filter_repeated_rejection_proposals(
        [unchanged], memory, now=now
    )
    traceable_eligible, traceable_rejected = filter_repeated_rejection_proposals(
        [traceable], memory, now=now
    )

    assert eligible == []
    assert rejected[0]["code"] == "repeated_candidate_cooldown"
    assert traceable_eligible == [traceable]
    assert traceable_rejected == []


def test_cycle_comparison_uses_retained_best_not_diagnostic_max(tmp_path):
    module = load_script_module(
        "run_heuristic_cycle_retained_best_module",
        "scripts/run_heuristic_cycle.py",
    )
    run_dir = tmp_path / "20260715_000000"
    run_dir.mkdir()
    (run_dir / "best_metrics.json").write_text(
        json.dumps({"score": -0.284204, "reward_version": "capital_return_v2"}),
        encoding="utf-8",
    )
    (run_dir / "summary.csv").write_text(
        "trial_name,score\nretained,-0.284204\ndiagnostic_only,0.022448\n",
        encoding="utf-8",
    )

    assert module.completed_run_best_score(run_dir) == pytest.approx(-0.284204)
    score, path = module.previous_best_score(tmp_path)
    assert score == pytest.approx(-0.284204)
    assert path == run_dir / "best_metrics.json"


def test_token_format_uses_short_units():
    assert format_token(999) == "999"
    assert format_token(1_234) == "1.23k"
    assert format_token(1_234_567) == "1.23m"
    assert format_token(1_234_567_890) == "1.23g"


def test_token_and_cost_breakdowns_include_input_output():
    stats = {
        "total_tokens": 1_234_567,
        "prompt_tokens": 1_000_000,
        "completion_tokens": 234_567,
        "total_cost": 0.123456,
        "input_cost_usd": 0.023456,
        "output_cost_usd": 0.1,
    }

    assert format_token_breakdown(stats) == "1.23m (输入 1.00m / 输出 234.6k)"
    assert format_cost_breakdown(stats) == "$0.1235 (输入 $0.0235 / 输出 $0.1000)"


def test_check_db_safe_input_handles_closed_stdin(monkeypatch):
    import sovereign_hall.check_db as check_db

    def raise_eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)

    assert check_db.safe_input("choice: ") is None


def test_check_db_blank_choice_exits_safely(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "sovereign_hall.db").write_bytes(b"")
    monkeypatch.setattr(check_db, "project_root", tmp_path)
    monkeypatch.setattr(check_db, "show_stats", lambda _db_path: [])
    monkeypatch.setattr(check_db, "safe_input", lambda _prompt: "")

    check_db.main()
    output = capsys.readouterr().out

    assert "空输入，安全退出" in output
    assert "无效选择" not in output


def test_research_interactive_safe_input_handles_closed_stdin(monkeypatch):
    import sovereign_hall.research_interactive as research_interactive

    def raise_eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)

    assert research_interactive.safe_input("question: ") is None


def test_research_interactive_help_is_cli_only():
    import sovereign_hall.research_interactive as research_interactive

    with pytest.raises(SystemExit) as exc:
        research_interactive.parse_args(["--help"])

    assert exc.value.code == 0


def test_check_db_realtime_quotes_are_on_by_default(monkeypatch):
    import sovereign_hall.check_db as check_db

    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)
    assert check_db.realtime_quotes_enabled() is True

    monkeypatch.setenv("SOVEREIGN_HALL_REALTIME_QUOTES", "0")
    assert check_db.realtime_quotes_enabled() is False


def test_check_db_filters_placeholder_candidate_rejections():
    import sovereign_hall.check_db as check_db

    filtered = check_db.filter_supported_candidate_rejections([
        {"ticker": "600519", "code": "committee_hold"},
        {"ticker": "推荐标的代码", "code": "committee_hold"},
        {"ticker": "06862", "code": "committee_hold"},
    ])

    assert filtered == [{"ticker": "600519", "code": "committee_hold"}]


def test_check_db_requires_realtime_quote_without_local_fallback(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats (key, value) VALUES ('simulation_cash', '9000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute("INSERT INTO simulation_positions VALUES ('600519', 100, 10.0)")
    conn.execute(
        "CREATE TABLE simulation_trades (ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE price_predictions (ticker TEXT, current_price REAL, predicted_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO price_predictions VALUES (?, ?, ?)",
        [
            ("600519", 10.5, "2026-06-12T10:00:00"),
            ("600519.SH", 12.3, "2026-06-15T10:00:00"),
        ],
    )
    conn.commit()
    conn.close()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)
    monkeypatch.setattr(check_db, "get_realtime_prices", lambda tickers: {})

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "当前资产: N/A" in output
    assert "实时现价不可用" in output
    assert "不使用本地估值/预测价/成本价兜底" in output
    assert "本地最近预测价" not in output


def test_check_db_values_positions_from_realtime_quote(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '9000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute("INSERT INTO simulation_positions VALUES ('600519', 100, 10.0)")
    conn.execute(
        "CREATE TABLE simulation_trades (ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        check_db,
        "get_realtime_prices",
        lambda tickers: {
            "600519": {
                "price": 12.3,
                "source": "test_realtime_quote",
                "fetched_at": "2026-07-13T15:00:00",
            }
        },
    )

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "当前资产: 10230.00 元（实时现价）" in output
    assert "实时现价12.300" in output
    assert "test_realtime_quote" in output


def test_check_db_reports_pending_decision_terminal_counts(tmp_path, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "pending_status.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '10000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute(
        "CREATE TABLE simulation_trades "
        "(ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.execute(
        """
        CREATE TABLE simulation_pending_decisions (
            id INTEGER PRIMARY KEY, ticker TEXT, direction TEXT,
            target_position REAL, defer_code TEXT, status TEXT,
            created_at TEXT, updated_at TEXT, resolved_at TEXT,
            resolution TEXT, replay_count INTEGER
        )
        """
    )
    rows = [
        (1, "600519", "long", 0.1, "market_closed", "executed", "2026-07-18T10:00:00", "2026-07-19T10:01:00", "2026-07-19T10:01:00", "buy:filled", 1),
        (2, "000001", "long", 0.1, "market_closed", "rejected", "2026-07-18T10:02:00", "2026-07-19T10:03:00", "2026-07-19T10:03:00", "hold:heuristic veto", 1),
        (3, "159915", "sell", 0.0, "market_closed", "expired", "2026-07-10T10:00:00", "2026-07-19T10:04:00", "2026-07-19T10:04:00", "expired_without_open-session_replay", 0),
        (4, "512880", "long", 0.1, "daily_trade_limit", "pending_next_trading_session", "2026-07-19T10:05:00", "2026-07-19T10:05:00", None, None, 0),
    ]
    conn.executemany(
        "INSERT INTO simulation_pending_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "待执行裁决: 1 条" in output
    assert "executed=1, rejected=1, expired=1, pending=1" in output
    assert "最近裁决结果: expired | 159915 sell" in output
    assert "expired_without_open-session_replay" in output


def test_check_db_reports_live_daily_price_backfill_progress(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
    conn.execute("INSERT INTO daily_prices VALUES ('600519', '2026-06-18', 10.5)")
    conn.commit()
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,45,2026-05-01,2026-06-20,1585,2026-06-20,True,1,"
        "backfill this ticker's latest local daily_prices row first\n"
        "2,512880,44,2026-05-02,2026-06-10,1197,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-19,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n"
        "2026-06-10,512880,prediction_current_price\n",
        encoding="utf-8",
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "latest_missing_tickers": ["600519", "688256"],
            "unblock_tickers": ["600519", "688256"],
            "minimum_next_rows": 2,
            "missing_tickers_top10": [
                {"ticker": "600519", "signal_days": 45, "last_signal_date": "2026-06-20"},
                {"ticker": "512880", "signal_days": 44, "last_signal_date": "2026-06-10"},
            ],
            "backfill_plan_path": str(plan_path),
            "backfill_plan": {
                "total_missing_tickers": 2,
                "minimum_next_rows": 1,
                "top_priority_tickers": ["600519", "512880"],
            },
        },
    )

    text = check_db.format_daily_price_backfill_progress(conn, context=context)
    conn.close()

    assert "优先队列任意本地价格(非解锁口径): 1/2 tickers" in text
    assert "计划日期覆盖: 2/3 signal dates；缺口=1，补齐后重跑验证" in text
    assert "600519(missing 2026-05-01..2026-06-20, 45d, 1585obs, plan_covered=2/2" in text
    assert "512880(missing 2026-05-02..2026-06-10, 44d, 1197obs, plan_covered=0/1)" in text
    assert "下一步本地补齐: 512880 2026-05-02..2026-06-10 (44 signal days)" in text
    assert "最小解锁批次: 600519, 688256 (2 signal rows)" in text
    assert f"机器可读补齐计划: {tmp_path / 'daily_price_backfill_plan.csv'}" in text
    assert "计划优先级Top: 600519, 512880" in text
    assert "本地DB覆盖检查: python scripts/backfill_daily_prices.py --status --limit 5 --plan" in text
    assert "不联网计划查看: python scripts/backfill_daily_prices.py --dry-run --limit 5 --plan" in text
    assert "本地CSV精确日期校验: python scripts/backfill_daily_prices.py --import-csv data/local_daily_prices.csv" in text
    assert "本地CSV模板生成: python scripts/backfill_daily_prices.py --status --limit 5 --export-template" in text
    assert "local_daily_prices_template.csv" in text
    assert "MarketDataService fetch 默认关闭" in text
    assert f"--plan {plan_path}" in text
    assert "旧历史artifact复用仓位上限 <= 0.5%" in text
    assert "旧历史artifact复用仓位上限" in text
    assert "不受该历史缺口停机帽约束" in text


def test_check_db_exports_stable_local_daily_price_template(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
    conn.execute("INSERT INTO daily_prices VALUES ('600519', '2026-06-18', 10.5)")
    conn.commit()
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,45,2026-05-01,2026-06-20,1585,2026-06-20,True,1,"
        "backfill this ticker's latest local daily_prices row first\n"
        "2,512880,44,2026-05-02,2026-06-10,1197,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-19,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n"
        "2026-06-10,512880,prediction_current_price\n",
        encoding="utf-8",
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "missing_tickers_top10": [
                {"ticker": "600519", "signal_days": 45, "last_signal_date": "2026-06-20"},
                {"ticker": "512880", "signal_days": 44, "last_signal_date": "2026-06-10"},
            ],
            "backfill_plan_path": str(plan_path),
            "backfill_plan": {
                "total_missing_tickers": 2,
                "minimum_next_rows": 1,
                "top_priority_tickers": ["600519", "512880"],
            },
        },
    )

    progress = check_db.daily_price_backfill_progress(conn, context=context)
    output_path = tmp_path / "data" / "local_daily_prices_template.csv"
    written = check_db.export_daily_price_template_from_progress(progress, output_path)
    progress["template_written_rows"] = written
    progress["stable_template_path"] = str(output_path)
    text = check_db.format_daily_price_backfill_progress(conn, progress=progress)
    conn.close()

    rows = list(csv.DictReader(output_path.open("r", encoding="utf-8")))
    assert written == 1
    assert rows[0]["ticker"] == "512880"
    assert rows[0]["date"] == "2026-06-10"
    assert rows[0]["close"] == ""
    assert "入口已生成待填写模板" in text
    assert f"{output_path} (1 rows)" in text
    assert "模板填完后校验" in text
    assert "模板填完后严格校验" in text
    assert "严格校验通过后导入" in text
    assert "--require-plan-coverage" in text
    assert "--coverage-limit 5" in text

    progress["template_csv_status"] = check_db.inspect_local_daily_price_csv(output_path)
    text_with_status = check_db.format_daily_price_backfill_progress(conn, progress=progress)
    assert "模板当前状态: rows=1, valid_ohlc=0, blank=1, invalid=0" in text_with_status
    assert "模板尚未填入独立OHLC" in text_with_status


def test_backfill_daily_prices_imports_local_csv_without_network(tmp_path):
    module = load_script_module("backfill_daily_prices_test_module", "scripts/backfill_daily_prices.py")
    csv_path = tmp_path / "daily_prices.csv"
    csv_path.write_text(
        "ticker,date,open,high,low,close,volume\n"
        "600519,2026-06-20,10,11,9,10.5,1000\n"
        "512880,2026-06-20,,,,1.234,\n"
        "BAD,2026-06-20,1,1,1,0,0\n",
        encoding="utf-8",
    )

    rows, invalid = module.rows_from_csv(csv_path)

    assert len(rows) == 2
    assert rows[0][:2] == ("600519", "2026-06-20")
    assert rows[1][2:6] == (1.234, 1.234, 1.234, 1.234)
    assert invalid and invalid[0]["ticker"] == "BAD"

    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE daily_prices (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                source TEXT,
                PRIMARY KEY (ticker, date)
            )
            """
        )
    written = module.upsert_csv_rows(db_path, rows, "unit_csv")

    with sqlite3.connect(db_path) as conn:
        stored = conn.execute(
            "SELECT ticker, date, close, source FROM daily_prices ORDER BY ticker"
        ).fetchall()

    assert written == 2
    assert stored == [
        ("512880", "2026-06-20", 1.234, "unit_csv"),
        ("600519", "2026-06-20", 10.5, "unit_csv"),
    ]


def test_backfill_daily_prices_validates_exact_plan_dates(tmp_path):
    module = load_script_module("backfill_daily_prices_exact_plan_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    requests = module.requests_from_plan(plan_path, module.parse_date("2026-06-20"))
    rows = [("600519", "2026-06-20", 10.0, 10.0, 10.0, 10.0, 100.0)]

    summary = module.summarize_plan_coverage(rows, requests, plan_path, max_age_days=7)

    assert "csv_exact_ticker_coverage=0/1" in summary
    assert "signal_dates=1/2" in summary
    assert "missing_top=600519" in summary


def test_backfill_daily_prices_import_csv_defaults_to_latest_plan(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_latest_plan_module", "scripts/backfill_daily_prices.py")
    runs_root = tmp_path / "runs" / "heuristic_cycle"
    run_dir = runs_root / "20260703_000000"
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (run_dir / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    csv_path = tmp_path / "daily_prices.csv"
    csv_path.write_text(
        "ticker,date,close\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--runs-root",
            str(runs_root),
            "--import-csv",
            str(csv_path),
            "--source",
            "local_csv",
            "--dry-run",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 0
    assert f"Plan: {plan_path.resolve()}" in output
    assert "Plan coverage: plan_requests=1" in output
    assert "signal_dates=1/2" in output


def test_backfill_daily_prices_strict_plan_coverage_gate(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_strict_plan_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    partial_csv = tmp_path / "partial_daily_prices.csv"
    partial_csv.write_text(
        "ticker,date,close\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--plan",
            str(plan_path),
            "--import-csv",
            str(partial_csv),
            "--source",
            "local_csv",
            "--dry-run",
            "--coverage-limit",
            "1",
            "--require-plan-coverage",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 4
    assert "STRICT plan coverage failed" in output
    assert "signal_dates=1/2" in output

    full_csv = tmp_path / "full_daily_prices.csv"
    full_csv.write_text(
        "ticker,date,close\n"
        "600519,2026-06-05,10.1\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--plan",
            str(plan_path),
            "--import-csv",
            str(full_csv),
            "--source",
            "local_csv",
            "--dry-run",
            "--coverage-limit",
            "1",
            "--require-plan-coverage",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 0
    assert "STRICT plan coverage passed" in output
    assert "signal_dates=2/2" in output


def test_backfill_daily_prices_blocks_market_fetch_by_default(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_local_guard_module", "scripts/backfill_daily_prices.py")
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--ticker",
            "600519",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-02",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 3
    assert "MarketDataService fetch disabled by default" in output
    assert "--import-csv data/local_daily_prices.csv" in output


def test_backfill_plan_uses_missing_date_range_and_csv_plan_coverage(tmp_path):
    module = load_script_module("backfill_daily_prices_plan_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,159990,43,2026-04-29,2026-06-10,445,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n"
        "2,600690,7,2026-05-28,2026-06-09,8,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )

    requests = module.requests_from_plan(plan_path, datetime(2026, 6, 27).date())

    assert requests[0].ticker == "159990"
    assert requests[0].start.isoformat() == "2026-04-29"
    assert requests[0].end.isoformat() == "2026-06-10"
    assert requests[1].end.isoformat() == "2026-06-09"
    coverage = module.summarize_plan_coverage(
        [("159990", "2026-06-10", 1.0, 1.0, 1.0, 1.0, 0.0)],
        requests,
    )
    assert "csv_exact_ticker_coverage=1/2" in coverage
    assert "signal_dates=1/2" in coverage
    assert "missing_top=600690" in coverage


def test_backfill_market_request_extends_before_weekend_signal(tmp_path):
    module = load_script_module("backfill_daily_prices_weekend_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600141,1,2026-05-30,2026-05-30,1,2026-05-30,True,1,backfill\n",
        encoding="utf-8",
    )

    exact = module.requests_from_plan(plan_path, datetime(2026, 6, 1).date())
    fetch = module.requests_from_plan(
        plan_path,
        datetime(2026, 6, 1).date(),
        lookback_days=7,
    )

    assert exact[0].start.isoformat() == "2026-05-30"
    assert fetch[0].start.isoformat() == "2026-05-23"
    assert fetch[0].end.isoformat() == "2026-05-30"


def test_backfill_plan_status_uses_exact_signal_tape_dates(tmp_path):
    module = load_script_module("backfill_daily_prices_status_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,688256,3,2026-05-09,2026-06-26,6,2026-06-26,True,1,"
        "backfill this ticker's latest local daily_prices row first\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price,close_observations,price_source\n"
        "2026-05-09,688256,1182.53,2,prediction_current_price\n"
        "2026-05-10,688256,1182.53,3,prediction_current_price\n"
        "2026-06-26,688256,1455.69,1,prediction_current_price\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE daily_prices (
                ticker TEXT,
                date TEXT,
                close REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_prices VALUES (?, ?, ?)",
            [
                ("688256", "2026-05-08", 1000.0),
                ("688256", "2026-06-18", 1200.0),
            ],
        )

    summary, details = module.summarize_db_plan_coverage(db_path, plan_path, limit=5, max_age_days=7)
    assert summary["status"] == "needs_local_daily_prices"
    assert summary["checked_signal_dates"] == 3
    assert summary["covered_signal_dates"] == 2
    assert details[0]["missing_signal_dates"] == ["2026-06-26"]
    rendered = module.format_db_plan_coverage(summary, details)
    assert "signal_dates=2/3" in rendered
    assert "missing_dates=2026-06-26" in rendered


def test_backfill_daily_prices_exports_missing_template_only(tmp_path):
    module = load_script_module("backfill_daily_prices_template_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,688256,3,2026-05-09,2026-06-26,6,2026-06-26,True,1,"
        "backfill this ticker's latest local daily_prices row first\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price,close_observations,price_source\n"
        "2026-05-09,688256,1182.53,2,prediction_current_price\n"
        "2026-05-10,688256,1182.53,3,prediction_current_price\n"
        "2026-06-26,688256,1455.69,1,prediction_current_price\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
        conn.executemany(
            "INSERT INTO daily_prices VALUES (?, ?, ?)",
            [
                ("688256", "2026-05-08", 1000.0),
                ("688256", "2026-06-18", 1200.0),
            ],
        )

    summary, details = module.summarize_db_plan_coverage(db_path, plan_path, limit=5, max_age_days=7)
    template_path = tmp_path / "local_daily_prices_template.csv"
    written = module.export_missing_price_template(template_path, summary, details)

    assert written == 1
    rows = list(csv.DictReader(template_path.open("r", encoding="utf-8")))
    assert rows == [
        {
            "ticker": "688256",
            "date": "2026-06-26",
            "open": "",
            "high": "",
            "low": "",
            "close": "",
            "volume": "",
            "source_note": "fill_from_independent_local_ohlc_before_import",
        }
    ]


def test_price_readiness_uses_signal_date_price_source_not_ticker_level_history(tmp_path):
    module = load_script_module("heuristic_cycle_stdlib_readiness_test_module", "scripts/run_heuristic_cycle_stdlib.py")
    daily = [
        {
            "ticker": "688256",
            "date": "2026-06-20",
            "price_source": "daily_prices",
            "close_observations": 3,
        },
        {
            "ticker": "688256",
            "date": "2026-06-26",
            "price_source": "prediction_current_price",
            "close_observations": 1,
        },
        {
            "ticker": "159928",
            "date": "2026-06-26",
            "price_source": "daily_prices",
            "close_observations": 2,
        },
        {
            "ticker": "159990",
            "date": "2026-05-01",
            "price_source": "prediction_current_price",
            "close_observations": 100,
        },
        {
            "ticker": "159990",
            "date": "2026-05-02",
            "price_source": "prediction_current_price",
            "close_observations": 100,
        },
    ]
    price_history = {("2026-06-20", "688256"): 10.0, ("2026-06-26", "159928"): 1.0}

    readiness = module.build_price_readiness_report(daily, price_history)

    assert readiness["status"] == "partial_daily_price_backfill_needed"
    assert readiness["total_signal_ticker_count"] == 3
    assert readiness["priced_signal_ticker_count"] == 1
    assert readiness["missing_signal_ticker_count"] == 2
    assert readiness["latest_missing_tickers"] == ["688256"]
    assert readiness["minimum_next_rows"] == 1
    assert readiness["missing_tickers_top10"][0]["ticker"] == "688256"
    assert readiness["missing_tickers_top10"][0]["last_signal_date"] == "2026-06-26"
    plan_rows, plan_summary = module.build_daily_price_backfill_plan(daily, price_history, tmp_path)
    assert plan_summary["top_priority_tickers"][0] == "688256"
    assert plan_rows[0]["ticker"] == "688256"


def test_market_data_ticker_mapping():
    svc = MarketDataService()
    assert svc.is_supported_ticker("600519.SH") is True
    assert svc.is_supported_ticker("推荐标的代码") is False
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
    assert proposals[0]["created_at"]
    await db.close()


@pytest.mark.asyncio
async def test_proposal_timestamp_repair_preserves_legacy_null_and_orders_new_first(tmp_path):
    db_path = tmp_path / "legacy_proposals.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await db._connection.execute(
        """
        INSERT INTO proposals (proposal_id, ticker, direction, created_at)
        VALUES ('legacy-null', '600000', 'long', NULL)
        """
    )
    await db._connection.commit()

    await db.add_proposal({
        "proposal_id": "new-timestamped",
        "ticker": "510300",
        "direction": "long",
        "created_at": "2026-07-23T15:00:00",
    })

    proposals = await db.get_proposals(limit=5)
    assert [row["proposal_id"] for row in proposals] == [
        "new-timestamped",
        "legacy-null",
    ]
    assert proposals[1]["created_at"] is None
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
    sim.get_current_quote = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=999.0,
    )

    assert result["success"] is False
    assert "实时现价" in result["reason"]


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


def test_portfolio_policy_targets_full_deployment_without_strategic_cash(tmp_path):
    status = deployment_status(cash=7200.0, total_assets=10000.0, target_invested_ratio=1.0)

    assert status["target_invested_ratio"] == 1.0
    assert status["invested_ratio"] == pytest.approx(0.28)
    assert status["deployment_gap"] == pytest.approx(7200.0)
    assert deployment_position_floor(7200.0, 10000.0, 4) == pytest.approx(0.18)

    evaluator = load_script_module("run_heuristic_cycle_full_deployment_module", "scripts/run_heuristic_cycle.py")
    weights = evaluator.capped_proportional_allocation(
        {"A": 9.0, "B": 1.0, "C": 1.0, "D": 1.0},
        total_weight=1.0,
        max_weight=0.25,
    )
    assert sum(weights.values()) == pytest.approx(1.0)
    assert max(weights.values()) <= 0.25 + 1e-12

    db_path = tmp_path / "simulation.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '7200')")
        conn.execute(
            """
            CREATE TABLE simulation_positions (
                ticker TEXT, shares REAL, avg_cost REAL, opened_at TEXT,
                last_mark_price REAL, last_mark_at TEXT, last_mark_source TEXT,
                last_reviewed_at TEXT, review_status TEXT, review_reason TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO simulation_positions VALUES
            ('600050', 100, 4.5, '2026-05-01', 4.5, '2026-05-02',
             'stale local price', '2026-07-13', 'blocked_stale_price', 'stale')
            """
        )
        conn.execute(
            """
            CREATE TABLE simulation_candidate_rejections (
                ticker TEXT, code TEXT, rejection_count INTEGER, last_reason TEXT,
                source TEXT, first_seen_at TEXT, last_seen_at TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO simulation_candidate_rejections VALUES (?, 'committee_hold', 1, 'x', 'test', '2026-07-21', '2026-07-21')",
            [("600519",), ("推荐标的代码",), ("06862",)],
        )
    report = evaluator.build_portfolio_lifecycle_report(db_path)
    assert report["cash"] == pytest.approx(7200.0)
    assert report["status"] == "realtime_valuation_required"
    assert report["invested_ratio"] is None
    assert report["deployment_gap"] is None
    assert [row["ticker"] for row in report["candidate_rejection_memory"]] == ["600519"]


def test_position_review_blocks_non_realtime_price_instead_of_fabricating_exit():
    review = review_position(
        ticker="512660",
        avg_cost=1.483,
        opened_at="2026-05-12T06:02:08",
        price=1.278,
        price_at="2026-06-05",
        price_source="stale local prediction current_price",
        now=datetime.fromisoformat("2026-07-13T09:00:00"),
        max_price_age_days=3,
    )

    assert review.action == "blocked_non_realtime_price"
    assert review.holding_days == 62
    assert review.price_age_days == 38


def test_position_review_exits_fresh_stop_or_max_holding_breach():
    stopped = review_position(
        ticker="512660",
        avg_cost=1.483,
        opened_at="2026-07-01T09:00:00",
        price=1.30,
        price_at="2026-07-13T09:00:00",
        price_source="test realtime quote",
        now=datetime.fromisoformat("2026-07-13T10:00:00"),
        max_price_age_days=3,
        stop_loss_pct=-0.08,
    )
    expired = review_position(
        ticker="600050",
        avg_cost=4.52,
        opened_at="2026-05-11T09:00:00",
        price=4.60,
        price_at="2026-07-13T09:00:00",
        price_source="test realtime quote",
        now=datetime.fromisoformat("2026-07-13T10:00:00"),
        max_holding_days=30,
    )

    assert stopped.action == "exit"
    assert "止损" in stopped.reason
    assert expired.action == "exit"
    assert "最大持有期" in expired.reason


@pytest.mark.asyncio
async def test_simulation_reviews_every_position_and_only_executes_fresh_exit():
    sim = InvestmentSimulation()
    sim.positions = {
        "512660": {"shares": 300, "avg_cost": 1.483, "opened_at": "2026-05-12T06:02:08"},
        "600050": {"shares": 100, "avg_cost": 4.52, "opened_at": "2026-05-11T02:22:01"},
    }
    sim.resolve_trade_price_detail = AsyncMock(side_effect=[
        {
            "price": None,
            "source": "realtime_quote_unavailable",
            "price_at": "",
        },
        {
            "price": 4.10,
            "source": "test realtime quote",
            "price_at": datetime.now().isoformat(),
        },
    ])
    sim.execute_trade = AsyncMock(return_value={"success": True, "action": "sell"})

    reviews = await sim.review_open_positions()

    assert [row["action"] for row in reviews] == ["blocked_realtime_price", "exit"]
    sim.execute_trade.assert_awaited_once()
    assert sim.execute_trade.await_args.kwargs["ticker"] == "600050"


@pytest.mark.asyncio
async def test_committee_redeployment_awaits_complete_realtime_asset_estimate(monkeypatch):
    """A lifecycle exit must be able to flow into same-cycle candidate sizing."""
    import sovereign_hall.run_discussion as discussion_module

    context = HeuristicRiskContext(None, "", None, 0.10, True, "test", [])
    monkeypatch.setattr(discussion_module, "load_latest_heuristic_context", lambda: context)
    monkeypatch.setattr(discussion_module, "recent_prediction_observation_count", lambda ticker: 1)
    monkeypatch.setattr(
        discussion_module,
        "apply_heuristic_risk_cap",
        lambda ticker, target, confidence, **kwargs: (target, ""),
    )

    simulation = type("FakeSimulation", (), {})()
    # Same-day fills must not bypass the mandatory lifecycle review.
    simulation.last_trade_date = datetime.now()
    simulation.last_trade_records = {}
    simulation.positions = {}
    simulation.calculate_assets = AsyncMock(return_value={
        "valuation_complete": True,
        "total_assets": 10_000.0,
        "known_total_assets": 10_000.0,
        "cash": 10_000.0,
        "positions_value": 0.0,
        "positions": {},
        "position_values": {},
        "invested_ratio": 0.0,
        "deployment_gap": 10_000.0,
        "target_invested_ratio": 1.0,
        "missing_price_tickers": [],
    })
    simulation.get_recent_reflection = AsyncMock(return_value="")
    simulation.review_open_positions = AsyncMock(return_value=[])
    simulation.daily_reflection = AsyncMock(return_value="")
    simulation.save_snapshot = AsyncMock()
    simulation.is_in_cooldown = lambda ticker: False
    simulation._normalize_ticker = lambda ticker: ticker
    simulation.resolve_trade_price = AsyncMock(return_value=(10.0, "test_realtime_quote"))
    simulation._estimate_trade_assets = AsyncMock(return_value=({}, 10_000.0, []))
    simulation.execute_trade = AsyncMock(return_value={"success": True, "action": "hold"})
    simulation.count_trades_on_date = AsyncMock(return_value=1)
    simulation.record_redeployment_attempt = AsyncMock(return_value={
        "status": "blocked_candidate_execution",
        "deployment_gap": 10_000.0,
        "blocker_code": "candidate_execution_blocked",
    })
    market_data = type(
        "FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)}
    )()

    await run_committee_approved_simulation(
        simulation,
        market_data,
        None,
        [{
            "ticker": "600519",
            "direction": "long",
            "confidence": 0.8,
            "target_position": 0.1,
        }],
    )

    simulation._estimate_trade_assets.assert_awaited_once_with("600519", 10.0)
    simulation.execute_trade.assert_awaited_once()
    simulation.review_open_positions.assert_awaited_once()
    simulation.record_redeployment_attempt.assert_awaited_once()


@pytest.mark.asyncio
async def test_redeployment_queue_recovers_and_persists_attempts(tmp_path, capsys):
    db = DatabaseService(str(tmp_path / "test.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.cash = 9_727.22
    sim.positions = {}
    await sim.save_state()

    await sim._bootstrap_redeployment_state()
    recovered = await sim.get_redeployment_state()
    assert recovered["status"] == "pending_approved_candidates"
    assert recovered["deployment_gap"] == pytest.approx(9_727.22)
    assert recovered["source"] == "account_state_recovery"

    await sim.record_redeployment_attempt(
        {
            "valuation_complete": True,
            "deployment_gap": 9_727.22,
        },
        candidate_count=0,
        trade_count=0,
        blockers=["投委会无批准的可执行多头候选"],
        rejections=[
            {"code": "committee_hold", "ticker": "600519", "reason": "证据不足"},
            {"code": "committee_hold", "ticker": "510300", "reason": "证据不足"},
        ],
    )
    attempted = await sim.get_redeployment_state()
    assert attempted["status"] == "blocked_no_approved_candidates"
    assert attempted["attempt_count"] == 1
    assert attempted["last_candidate_count"] == 0
    assert "投委会" in attempted["blocker_reason"]
    assert attempted["last_rejection_counts"] == {"committee_hold": 2}
    assert attempted["rejection_counts_total"] == {"committee_hold": 2}
    assert "committee_hold=2" in attempted["next_action"]
    rejection_memory = await sim.get_candidate_rejection_memory()
    assert {(row["ticker"], row["code"], row["rejection_count"]) for row in rejection_memory} == {
        ("600519", "committee_hold", 1),
        ("510300", "committee_hold", 1),
    }
    feedback = await sim.format_redeployment_learning_context()
    assert "模拟再配置逐标的拒绝记忆" in feedback
    assert "600519 / committee_hold" in feedback
    assert "不得原样重提" in feedback
    await sim._record_candidate_rejections(
        [{"code": "market_closed", "ticker": "600519", "reason": "等待开市"}],
        source="test",
    )
    evidence_feedback = await sim.format_redeployment_learning_context()
    assert "market_closed" not in evidence_feedback
    assert "等待开市" not in evidence_feedback
    combined_prompt = build_lessons_with_heuristic_context(
        "历史教训",
        redeployment_context=evidence_feedback,
    )
    assert "历史教训" in combined_prompt
    assert "新增的本地可追溯证据" in combined_prompt

    await sim._record_candidate_rejections(
        [{"code": "committee_hold", "ticker": "推荐标的代码", "reason": "示例占位符"}],
        source="test",
    )
    async with db._connection.execute(
        "SELECT ticker FROM simulation_candidate_rejections WHERE ticker = ?",
        ("推荐标的代码",),
    ) as cursor:
        invalid_rows = await cursor.fetchall()
    assert invalid_rows == []

    await sim.record_redeployment_attempt(
        {"valuation_complete": True, "deployment_gap": 9_727.22},
        candidate_count=0,
        trade_count=0,
        blockers=["ticker缺失"],
        rejections=[{"code": "missing_ticker", "ticker": "", "reason": "ticker缺失"}],
    )
    attempted = await sim.get_redeployment_state()
    assert attempted["attempt_count"] == 2
    assert attempted["last_rejection_counts"] == {"missing_ticker": 1}
    assert attempted["rejection_counts_total"] == {
        "committee_hold": 2,
        "missing_ticker": 1,
    }

    restarted = InvestmentSimulation(db)
    await restarted.initialize()
    persisted = await restarted.get_redeployment_state()
    assert persisted["status"] == "blocked_no_approved_candidates"
    assert persisted["attempt_count"] == 2
    assert persisted["rejection_counts_total"]["committee_hold"] == 2
    await db.close()

    import sovereign_hall.check_db as check_db

    check_db.show_investment_status(tmp_path / "test.db")
    output = capsys.readouterr().out
    assert "逐标的重复拒绝记忆" in output
    assert "600519 / committee_hold x1" in output
    assert "重提要求: 必须给出新增本地可追溯证据" in output


@pytest.mark.asyncio
async def test_simulation_position_schema_migrates_lifecycle_columns(tmp_path):
    db = DatabaseService(str(tmp_path / "test.db"))
    await db._init_db()
    conn = db._connection
    await conn.execute("DROP TABLE IF EXISTS simulation_positions")
    await conn.execute(
        "CREATE TABLE simulation_positions (ticker TEXT PRIMARY KEY, shares INTEGER, avg_cost REAL, updated_at TEXT)"
    )
    await conn.execute(
        "INSERT INTO simulation_positions VALUES ('600050', 100, 4.52, '2026-05-11T02:22:01')"
    )
    await conn.commit()

    sim = InvestmentSimulation(db)
    await sim.init_tables()
    async with conn.execute("PRAGMA table_info(simulation_positions)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    await db.close()

    assert {
        "opened_at", "peak_price", "last_mark_price", "last_mark_at",
        "last_mark_source", "last_reviewed_at", "review_status", "review_reason",
    } <= columns


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


def test_heuristic_risk_cap_uses_full_investment_target_instead_of_cash_reserve(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.067,
        max_position=0.05,
        max_gross=0.15,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
    )

    capped, reason = apply_heuristic_risk_cap(
        "600519",
        0.05,
        0.8,
        current_position=0.0,
        current_gross_exposure=0.13,
        context=context,
    )
    checklist = format_policy_checklist(context)

    assert capped == pytest.approx(0.05)
    assert reason is None
    assert "组合目标投资比例=100%" in checklist


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

    assert capped == pytest.approx(0.015)
    assert "限制到1.5%" in reason
    assert "daily_prices缺失" in reason
    assert "禁止放大仓位" in reason
    assert "数据质量风险" in status
    assert "弱价格覆盖历史证据复用上限: 1.5%" in status
    assert "弱价格覆盖历史证据仓位<=1.5%" in prompt
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


def test_heuristic_context_surfaces_evaluation_engine(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        evaluation_engine="stdlib_fallback",
        evaluation_warning="numpy/pandas import did not complete during preflight",
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert "评估引擎: stdlib_fallback" in status
    assert "评估提示: numpy/pandas import did not complete during preflight" in status
    assert "评估引擎: stdlib_fallback" in prompt
    assert "numpy/pandas import did not complete during preflight" in prompt


def test_heuristic_context_surfaces_evaluator_health(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.063373,
        max_position=0.05,
        overfit_risk=False,
        warning="主评估器已本地复核fallback结果",
        failure_cases=[],
        evaluation_engine="stdlib_fallback",
        evaluator_health={
            "validation_status": "matched",
            "baseline_engine": "stdlib_fallback",
            "validation_engine": "pandas_primary",
            "baseline_score": 0.06337303806043082,
            "validation_score": 0.06337303806043082,
            "score_abs_diff": 0.0,
            "score_tolerance": 1e-9,
        },
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert "评估器复核: matched: stdlib_fallback vs pandas_primary" in status
    assert "score差=0" in status
    assert "baseline=0.063373, validation=0.063373" in prompt


def test_heuristic_risk_cap_tightens_insufficient_signal_count(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.056,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        min_signal_count=2,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, signal_count=1, context=context)

    assert capped == pytest.approx(0.015)
    assert "本地同日预测观察1/2不足" in reason
    assert "孤证仓位上限1.5%" in reason


def test_recent_prediction_observation_count_uses_latest_fresh_day(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE price_predictions (ticker TEXT, predicted_at TEXT)")
    conn.executemany(
        "INSERT INTO price_predictions (ticker, predicted_at) VALUES (?, ?)",
        [
            ("600519", "2026-06-09T10:00:00"),
            ("600519.SH", "2026-06-11T10:00:00"),
            ("600519", "2026-06-11T14:30:00"),
            ("000858", "2026-06-11T14:30:00"),
        ],
    )
    conn.commit()
    conn.close()

    count = recent_prediction_observation_count(
        "600519.SH",
        db_path=db_path,
        now=datetime.fromisoformat("2026-06-12T09:00:00"),
    )
    stale_count = recent_prediction_observation_count(
        "600519",
        db_path=db_path,
        max_age_days=0,
        now=datetime.fromisoformat("2026-06-12T09:00:00"),
    )

    assert count == 2
    assert stale_count == 0


def test_tape_freshness_is_recomputed_and_vetoes_new_simulated_long(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_freshness

    tape = refresh_tape_update_freshness(
        {
            "validation_status": "thin_tape_update",
            "current_latest_prediction_date": "2026-07-06",
            "latest_prediction_age_days": 3,
            "max_latest_prediction_age_days": 3,
            "new_prediction_rows_since_previous": 0,
        },
        now=datetime.fromisoformat("2026-07-11T09:00:00"),
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="stale_tape_guard",
        score=0.0,
        max_position=0.04,
        overfit_risk=False,
        warning="local only",
        failure_cases=[],
        tape_update=tape,
    )

    new_cap, new_reason = apply_heuristic_risk_cap(
        "600519", 0.04, 0.8, current_position=0.0, current_gross_exposure=0.0, context=context
    )
    held_cap, held_reason = apply_heuristic_risk_cap(
        "600519", 0.04, 0.8, current_position=0.02, current_gross_exposure=0.02, context=context
    )
    status = format_heuristic_status(context)

    assert tape["latest_prediction_age_days"] == 5
    assert tape["validation_status"] == "stale_tape"
    assert new_cap == 0.0
    assert held_cap == pytest.approx(0.02)
    assert "拒绝新增或扩大模拟多头仓位" in new_reason
    assert "拒绝新增或扩大模拟多头仓位" in held_reason
    assert "陈旧tape复用否决" in status


def test_current_committee_evidence_is_not_frozen_by_historical_price_gaps(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="fresh_committee_deployment",
        score=-0.01,
        max_position=0.10,
        min_confidence=0.65,
        min_signal_count=1,
        overfit_risk=True,
        warning="historical coverage incomplete",
        failure_cases=[],
        price_source="prediction current_price fallback; daily_prices partial",
        price_coverage={"status": "partial_daily_prices_low_signal_coverage", "independent_price_row_ratio": 0.0},
        tape_update={"validation_status": "stale_tape"},
        price_readiness_stall={"status": "stalled_partial_daily_prices"},
    )

    capped, reason = apply_heuristic_risk_cap(
        "600519",
        0.10,
        0.80,
        signal_count=1,
        current_position=0.0,
        current_gross_exposure=0.0,
        fresh_local_evidence=True,
        context=context,
    )
    stale_cap, _ = apply_heuristic_risk_cap(
        "600519",
        0.10,
        0.80,
        signal_count=1,
        current_position=0.0,
        current_gross_exposure=0.0,
        context=context,
    )

    assert capped == pytest.approx(0.10)
    assert "不再强制空仓" in reason
    assert stale_cap == 0.0


def test_tape_entry_veto_clears_only_with_fresh_broad_update(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_freshness

    base = {
        "validation_status": "stale_tape",
        "current_latest_prediction_date": "2026-07-09",
        "latest_prediction_age_days": 9,
        "max_latest_prediction_age_days": 3,
        "min_new_rows_for_validation": 20,
        "min_latest_date_rows_for_validation": 5,
        "entry_veto_reason": "old artifact value",
    }
    boundary_now = datetime.fromisoformat("2026-07-12T09:00:00")
    broad = refresh_tape_update_freshness(
        {**base, "new_prediction_rows_since_previous": 20, "latest_date_prediction_rows": 5},
        now=boundary_now,
    )
    thin = refresh_tape_update_freshness(
        {**base, "new_prediction_rows_since_previous": 20, "latest_date_prediction_rows": 4},
        now=boundary_now,
    )
    expired = refresh_tape_update_freshness(
        {
            **base,
            "current_latest_prediction_date": "2026-07-08",
            "new_prediction_rows_since_previous": 20,
            "latest_date_prediction_rows": 5,
        },
        now=boundary_now,
    )

    assert broad["latest_prediction_age_days"] == 3
    assert broad["validation_status"] == "fresh_tape_update"
    assert broad["enough_for_policy_widening"] is True
    assert "entry_veto_reason" not in broad
    assert thin["validation_status"] == "thin_tape_update"
    assert thin["enough_for_policy_widening"] is False
    assert thin["freshness_recovery_pending"] is True
    assert "entry_veto_reason" not in thin
    assert expired["latest_prediction_age_days"] == 4
    assert expired["validation_status"] == "stale_tape"
    assert expired["enough_for_policy_widening"] is False


def test_entry_tape_refresh_overlays_new_local_db_rows(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_from_local_db

    db_path = tmp_path / "local.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE price_predictions (ticker TEXT, predicted_at TEXT)")
        conn.executemany(
            "INSERT INTO price_predictions VALUES (?, ?)",
            [
                ("600030", "2026-07-06T10:00:00"),
                ("159995", "2026-07-06T11:00:00"),
                ("159985", "2026-07-13T01:31:37"),
            ],
        )

    refreshed = refresh_tape_update_from_local_db(
        {
            "validation_status": "stale_tape",
            "current_prediction_rows": 2,
            "new_prediction_rows_since_previous": 0,
            "current_latest_prediction_date": "2026-07-06",
            "latest_date_prediction_rows": 2,
            "min_new_rows_for_validation": 20,
            "min_latest_date_rows_for_validation": 5,
            "max_latest_prediction_age_days": 3,
        },
        db_path=db_path,
        now=datetime.fromisoformat("2026-07-13T09:00:00"),
    )

    assert refreshed["current_prediction_rows"] == 3
    assert refreshed["new_prediction_rows_since_previous"] == 1
    assert refreshed["current_latest_prediction_date"] == "2026-07-13"
    assert refreshed["latest_date_prediction_rows"] == 1
    assert refreshed["latest_prediction_age_days"] == 0
    assert refreshed["validation_status"] == "thin_tape_update"
    assert refreshed["enough_for_policy_widening"] is False
    assert refreshed["freshness_recovery_pending"] is True
    assert refreshed["live_db_appended_rows_since_run"] == 1

    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="recovery_guard",
        score=0.0,
        max_position=0.04,
        overfit_risk=True,
        warning="local only",
        failure_cases=[],
        tape_update=refreshed,
    )
    assert context.stale_tape_entry_veto is True


def test_cycle_propagates_pending_stale_recovery_across_thin_runs(tmp_path):
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_recovery_module", "scripts/run_heuristic_cycle.py")
    stale_run = tmp_path / "20260712_120000"
    thin_run = tmp_path / "20260713_120000"
    stale_run.mkdir()
    thin_run.mkdir()
    (stale_run / "tape_update.json").write_text(
        json.dumps({"validation_status": "stale_tape", "current_prediction_rows": 2}),
        encoding="utf-8",
    )
    (thin_run / "tape_update.json").write_text(
        json.dumps(
            {
                "validation_status": "thin_tape_update",
                "current_prediction_rows": 3,
                "previous_run": str(stale_run),
            }
        ),
        encoding="utf-8",
    )
    now = datetime.now()
    predictions = pd.DataFrame(
        {"predicted_at": pd.to_datetime([now, now, now])}
    )

    report = module.build_tape_update_report(predictions, thin_run)

    assert report["validation_status"] == "thin_tape_update"
    assert report["freshness_recovery_pending"] is True


def test_cycle_tape_baseline_skips_same_day_reruns(tmp_path):
    module = load_script_module("run_heuristic_cycle_baseline_module", "scripts/run_heuristic_cycle.py")
    old_run = tmp_path / "20260712_120000"
    rerun_one = tmp_path / "20260713_120000"
    rerun_two = tmp_path / "20260713_121000"
    for run in (old_run, rerun_one, rerun_two):
        run.mkdir()
    (old_run / "tape_update.json").write_text(
        json.dumps({"validation_status": "stale_tape", "current_prediction_rows": 2}), encoding="utf-8"
    )
    (rerun_one / "tape_update.json").write_text(
        json.dumps({"validation_status": "thin_tape_update", "previous_run": str(old_run)}), encoding="utf-8"
    )
    (rerun_two / "tape_update.json").write_text(
        json.dumps({"validation_status": "thin_tape_update", "previous_run": str(rerun_one)}), encoding="utf-8"
    )

    baseline = module.distinct_date_tape_baseline(rerun_two, "20260713")

    assert baseline == old_run


def test_sparse_split_checks_are_inconclusive_not_robust():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_sparse_split_module", "scripts/run_heuristic_cycle.py")
    daily = pd.DataFrame(
        [
            {
                "date": f"2026-06-{day:02d}",
                "ticker": "600519",
                "price": 10.0,
                "price_source": "daily_prices",
                "confidence": 0.1,
                "risk_reward": 0.0,
                "close_observations": 1,
                "stop_gap": 0.05,
                "signal_strength": 0.1,
                "return_1d": 0.0,
                "momentum_2d": 0.0,
                "momentum_3d": 0.0,
                "momentum_5d": 0.0,
                "vol_2d": 0.0,
                "vol_3d": 0.0,
                "vol_5d": 0.0,
            }
            for day in range(1, 11)
        ]
    )
    checks = module.split_checks(daily, module.PolicyConfig(name="no_trade"), module.CostConfig())

    assert checks["insufficient_trade_evidence"] is True
    assert checks["overfit_risk"] is True
    assert "risk avoidance" in checks["inconclusive_reason"]


def test_zero_trade_failure_analysis_does_not_invent_drawdown_or_overtrading():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_zero_trade_failure_module", "scripts/run_heuristic_cycle.py")
    curve = pd.DataFrame(
        [
            {
                "date": "2026-07-01",
                "signal_date": "2026-06-30",
                "equity": 1.0,
                "net_return": 0.0,
                "turnover": 0.0,
                "gross_exposure": 0.0,
                "cost": 0.0,
                "positions": "{}",
            },
            {
                "date": "2026-07-02",
                "signal_date": "2026-07-01",
                "equity": 1.0,
                "net_return": 0.0,
                "turnover": 0.0,
                "gross_exposure": 0.0,
                "cost": 0.0,
                "positions": "{}",
            },
        ]
    )
    daily = pd.DataFrame(
        columns=["date", "ticker", "price", "confidence", "risk_reward", "close_observations"]
    )
    failures = module.analyze_failures(
        {"curve": curve, "trades": []}, daily, module.PolicyConfig(name="no_trade")
    )

    assert [row["case_type"] for row in failures] == ["insufficient_trade_evidence"]
    assert failures[0]["market_state"]["gross_exposure"] == 0.0
    assert "no positions opened" in failures[0]["result"]


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

    assert capped == pytest.approx(0.0125)
    assert "持仓缺价槽位37.8%" in reason
    assert "弱覆盖模拟买入上限1.2%" in reason
    assert "价格覆盖" in status
    assert "弱价格覆盖历史证据复用上限: 1.2%" in status
    assert "daily_prices覆盖0.0%" in prompt
    assert "弱覆盖模拟买入上限=1.2%" in prompt


def test_heuristic_price_coverage_cap_scales_with_partial_coverage(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_source="daily_prices table with fallback to prediction current_price",
        price_coverage={
            "status": "partial_daily_prices_with_missing_hold_prices",
            "independent_price_row_ratio": 0.60,
            "missing_position_price_slot_ratio": 0.18,
            "missing_price_day_ratio": 0.10,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)

    assert capped == pytest.approx(0.0175)
    assert "弱覆盖模拟买入上限1.7%" in reason


def test_pandas_daily_tape_uses_bounded_asof_daily_prices():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_test_module", "scripts/run_heuristic_cycle.py")

    predictions = pd.DataFrame(
        [
            {
                "date": "2026-06-20",
                "ticker": "159995",
                "current_price": 2.7,
                "target_price": 3.0,
                "stop_loss": 2.5,
                "direction": "long",
                "confidence": 0.8,
                "expected_days": 30,
            },
            {
                "date": "2026-06-30",
                "ticker": "159995",
                "current_price": 2.8,
                "target_price": 3.1,
                "stop_loss": 2.6,
                "direction": "long",
                "confidence": 0.8,
                "expected_days": 30,
            },
        ]
    )
    price_history = pd.DataFrame(
        [{"date": "2026-06-18", "ticker": "159995", "close": 2.55}]
    )

    daily = module.build_daily_tape(predictions, price_history)
    by_date = daily.set_index("date")

    assert by_date.loc["2026-06-20", "price"] == pytest.approx(2.55)
    assert by_date.loc["2026-06-20", "price_source"] == "daily_prices"
    assert str(by_date.loc["2026-06-20", "daily_price_date"])[:10] == "2026-06-18"
    assert by_date.loc["2026-06-30", "price"] == pytest.approx(2.8)
    assert by_date.loc["2026-06-30", "price_source"] == "prediction_current_price"


def test_stdlib_daily_tape_uses_bounded_asof_daily_prices():
    module = load_script_module(
        "run_heuristic_cycle_stdlib_test_module",
        "scripts/run_heuristic_cycle_stdlib.py",
    )

    predictions = [
        {
            "date": "2026-06-20",
            "ticker": "159995",
            "current_price": 2.7,
            "target_price": 3.0,
            "stop_loss": 2.5,
            "direction": "long",
            "confidence": 0.8,
            "expected_days": 30,
        },
        {
            "date": "2026-06-30",
            "ticker": "159995",
            "current_price": 2.8,
            "target_price": 3.1,
            "stop_loss": 2.6,
            "direction": "long",
            "confidence": 0.8,
            "expected_days": 30,
        },
    ]

    daily = module.build_daily_tape(predictions, {("2026-06-18", "159995"): 2.55})
    by_date = {row["date"]: row for row in daily}

    assert by_date["2026-06-20"]["price"] == pytest.approx(2.55)
    assert by_date["2026-06-20"]["price_source"] == "daily_prices"
    assert by_date["2026-06-30"]["price"] == pytest.approx(2.8)
    assert by_date["2026-06-30"]["price_source"] == "prediction_current_price"


def test_heuristic_context_surfaces_price_readiness(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "total_signal_ticker_count": 12,
            "priced_signal_ticker_count": 0,
            "missing_signal_ticker_count": 12,
            "latest_signal_date": "2026-06-20",
            "latest_missing_tickers": ["600519", "688256"],
            "minimum_next_rows": 2,
            "missing_tickers_top10": [
                {
                    "ticker": "600519",
                    "signal_days": 45,
                    "first_signal_date": "2026-05-01",
                    "last_signal_date": "2026-06-20",
                    "total_signal_observations": 1585,
                },
                {
                    "ticker": "512880",
                    "signal_days": 44,
                    "first_signal_date": "2026-05-02",
                    "last_signal_date": "2026-06-10",
                    "total_signal_observations": 1197,
                },
            ],
            "backfill_plan_path": str(tmp_path / "daily_price_backfill_plan.csv"),
            "backfill_plan": {
                "total_missing_tickers": 12,
                "minimum_next_rows": 2,
                "top_priority_tickers": ["600519", "512880"],
            },
            "next_action": "Backfill latest local daily_prices first.",
        },
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)
    queue = format_price_readiness_backfill_queue(context)
    plan = format_price_readiness_backfill_plan(context)

    assert "daily_prices补齐: blocked_no_daily_prices" in status
    assert "daily_prices阻塞历史证据复用上限: 0.5%" in status
    assert "daily_prices优先补齐队列: 600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20)" in status
    assert "daily_prices补齐计划: plan=" in status
    assert "top=600519, 512880" in status
    assert "缺少12/12个signal ticker" in status
    assert "最新缺价ticker=600519, 688256" in prompt
    assert "daily_prices优先补齐队列: 600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20)" in prompt
    assert "daily_prices补齐计划: plan=" in prompt
    assert "daily_prices阻塞模拟买入上限=0.5%" in prompt
    assert "本地数据质量任务" in prompt
    assert queue.startswith("600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20), 512880")
    assert "missing_tickers=12" in plan
    assert "latest_rows_to_unblock=2" in plan


def test_heuristic_risk_cap_tightens_blocked_price_readiness(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "total_signal_ticker_count": 12,
            "priced_signal_ticker_count": 0,
            "missing_signal_ticker_count": 12,
            "latest_missing_tickers": ["159995"],
            "minimum_next_rows": 1,
        },
    )

    capped, reason = apply_heuristic_risk_cap(
        "159995",
        0.10,
        confidence=0.8,
        context=context,
    )

    assert capped == pytest.approx(0.005)
    assert "daily_prices补齐blocked_no_daily_prices" in reason
    assert "补齐前模拟买入上限0.5%" in reason


def test_price_readiness_stall_report_counts_consecutive_blocked_runs(tmp_path):
    readiness_payload = {
        "status": "blocked_no_daily_prices",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 0,
        "missing_signal_ticker_count": 307,
        "latest_missing_tickers": ["159995"],
        "missing_tickers_top10": [{"ticker": "159995", "signal_days": 45}],
    }
    for run_id in ("20260622_123523", "20260623_123529", "20260624_123500"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)

    assert report["status"] == "stalled_no_daily_prices"
    assert report["consecutive_blocked_runs"] == 3
    assert report["first_blocked_run"] == "20260622_123523"
    assert report["latest_blocked_run"] == "20260624_123500"
    assert report["next_ticker"] == "159995"
    assert report["same_next_ticker_runs"] == 3


def test_price_readiness_stall_report_counts_partial_no_progress(tmp_path):
    readiness_payload = {
        "status": "partial_daily_price_backfill_needed",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 30,
        "missing_signal_ticker_count": 277,
        "latest_missing_tickers": ["002221", "600030"],
        "minimum_next_rows": 2,
        "missing_tickers_top10": [
            {"ticker": "002221", "signal_days": 2},
            {"ticker": "159990", "signal_days": 43},
        ],
    }
    for run_id in ("20260628_123812", "20260629_123708", "20260630_123538"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap4_min2obs",
        score=0.0,
        max_position=0.04,
        overfit_risk=False,
        warning="partial daily_prices stalled",
        failure_cases=[],
        price_readiness_stall=report,
    )

    capped, reason = apply_heuristic_risk_cap("002221", 0.04, 0.8, context=context)
    note = format_price_readiness_stall_note(context)

    assert report["status"] == "stalled_partial_daily_prices"
    assert report["stall_kind"] == "partial_daily_price_backfill_needed"
    assert report["consecutive_blocked_runs"] == 3
    assert report["priced_signal_ticker_count"] == 30
    assert report["missing_signal_ticker_count"] == 277
    assert report["next_ticker"] == "002221"
    assert report["unblock_tickers"] == ["002221", "600030"]
    assert report["same_unblock_batch_runs"] == 3
    assert capped == pytest.approx(0.002)
    assert "partial daily_prices覆盖无进展" in note
    assert "最小解锁批次=002221, 600030(2行)，同一解锁批次连续3轮" in note
    assert "数据补齐未推进仓位上限0.20%" in reason


def test_price_readiness_stall_report_dedupes_same_day_reruns(tmp_path):
    readiness_payload = {
        "status": "partial_daily_price_backfill_needed",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 28,
        "missing_signal_ticker_count": 279,
        "latest_missing_tickers": ["159995", "600030"],
        "minimum_next_rows": 2,
        "missing_tickers_top10": [{"ticker": "159995", "signal_days": 1}],
    }
    for run_id in ("20260707_123540", "20260708_150542", "20260708_150718"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)

    assert report["status"] == "partial_daily_price_backfill_needed"
    assert report["consecutive_blocked_runs"] == 2
    assert report["blocked_run_ids"] == ["20260707_123540", "20260708_150718"]
    assert report["raw_lookback_runs"] == 3
    assert report["deduped_by_run_date"] is True
    assert report["unblock_tickers"] == ["159995", "600030"]


def test_heuristic_context_surfaces_price_readiness_stall(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness_stall={
            "status": "stalled_no_daily_prices",
            "consecutive_blocked_runs": 3,
            "minimum_blocked_runs": 3,
            "blocked_run_ids": ["20260622_123523", "20260623_123529", "20260624_123500"],
            "first_blocked_run": "20260622_123523",
            "latest_blocked_run": "20260624_123500",
            "next_ticker": "159995",
            "same_next_ticker_runs": 3,
            "next_action": "Backfill independently validated local daily_prices for 159995, then rerun the cycle.",
        },
    )

    capped, reason = apply_heuristic_risk_cap("159995", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)
    checklist = format_policy_checklist(context)
    note = format_price_readiness_stall_note(context)

    assert capped == pytest.approx(0.0025)
    assert "连续3/3轮daily_prices为0" in reason
    assert "数据补齐未推进仓位上限0.25%" in reason
    assert "daily_prices连续阻塞" in status
    assert "连续阻塞模拟买入上限=0.25%" in prompt
    assert "不得新增leaderboard分支" in prompt
    assert "daily_prices连续阻塞历史证据仓位<=0.25%" in checklist
    assert "下一步ticker=159995" in note


def test_heuristic_risk_cap_tightens_thin_tape_update(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        tape_update={
            "validation_status": "thin_tape_update",
            "new_prediction_rows_since_previous": 1,
            "current_latest_prediction_date": "2026-06-14",
            "latest_date_prediction_rows": 1,
            "latest_prediction_age_days": 1,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.01)
    assert "薄样本验证模拟买入上限1.0%" in reason
    assert "较上轮新增1行" in status
    assert "薄tape历史证据仓位<=1.0%" in prompt


def test_heuristic_risk_cap_tightens_zero_new_tape_update(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        tape_update={
            "validation_status": "thin_tape_update",
            "new_prediction_rows_since_previous": 0,
            "current_latest_prediction_date": "2026-06-14",
            "latest_date_prediction_rows": 1,
            "latest_prediction_age_days": 2,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.005)
    assert "零新增样本" in reason
    assert "薄样本验证模拟买入上限0.5%" in reason
    assert "薄样本历史证据复用上限: 0.5%" in status
    assert "薄tape历史证据仓位<=0.5%" in prompt


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
    assert "组合目标投资比例=100%" in checklist
    assert "禁止战略现金" in prompt
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
    sim.get_current_quote = AsyncMock(return_value={
        "price": 9.0,
        "source": "test_realtime_quote",
        "fetched_at": datetime.now().isoformat(),
    })
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.setattr(
        "sovereign_hall.services.investment_simulation.apply_heuristic_risk_cap",
        lambda ticker, target_position, confidence, **kwargs: (0.10, "heuristic cap"),
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.25,
        current_price=9.0,
        reason="committee",
        confidence=0.7,
        signal_count=1,
    )

    assert result["action"] == "buy"
    assert result["price"] == pytest.approx(9.0)
    assert sim.positions["600519"]["shares"] == 100


@pytest.mark.asyncio
async def test_simulation_daily_trade_limit_cannot_be_bypassed_by_direct_call(monkeypatch):
    sim = InvestmentSimulation()
    sim.max_daily_trades = MAX_DAILY_TRADES
    sim.count_trades_on_date = AsyncMock(return_value=MAX_DAILY_TRADES)
    sim.resolve_trade_price = AsyncMock(return_value=(10.0, "test_realtime_quote"))
    fake_market = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=True),
        },
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.10,
        current_price=999.0,
    )

    assert result["success"] is False
    assert result["action"] == "pending"
    assert "硬上限 5 笔" in result["reason"]
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_limit_persists_price_free_pending_decision(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.max_daily_trades = MAX_DAILY_TRADES
    sim.count_trades_on_date = AsyncMock(return_value=MAX_DAILY_TRADES)
    sim.resolve_trade_price = AsyncMock(return_value=(10.0, "should_not_be_used"))
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=999.0,
        reason="committee ruling",
        confidence=0.7,
    )

    row = await (await db._connection.execute(
        "SELECT ticker, direction, target_position, defer_code, status FROM simulation_pending_decisions"
    )).fetchone()
    columns = {item[1] for item in await (await db._connection.execute(
        "PRAGMA table_info(simulation_pending_decisions)"
    )).fetchall()}
    await db.close()

    assert result["action"] == "pending"
    assert tuple(row) == ("600519", "long", 0.1, "daily_trade_limit", "pending_next_trading_session")
    assert "price" not in columns
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_market_closed_persists_exit_without_filling(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "closed.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.resolve_trade_price = AsyncMock(return_value=(12.0, "should_not_be_used"))
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=False)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="sell",
        target_position=0.0,
        current_price=999.0,
        reason="stop loss",
    )
    pending = await sim.pending_decision_count()
    trade_count = (await (await db._connection.execute("SELECT COUNT(*) FROM simulation_trades")).fetchone())[0]
    await db.close()

    assert result["action"] == "pending"
    assert pending == 1
    assert trade_count == 0
    assert sim.positions["600519"]["shares"] == 100
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_replay_waits_for_open_market_without_fetching_quote(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_closed.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    await sim.record_pending_decision(
        ticker="600519",
        direction="sell",
        target_position=0.0,
        reason="closed-market stop",
        defer_code="market_closed",
    )
    sim.execute_trade = AsyncMock()
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=False)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.replay_pending_decisions()
    row = await (await db._connection.execute(
        "SELECT status, replay_count FROM simulation_pending_decisions"
    )).fetchone()
    await db.close()

    assert result["status"] == "waiting_market_open"
    assert result["remaining"] == 1
    assert tuple(row) == ("pending_next_trading_session", 0)
    sim.execute_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_replay_is_exit_first_and_resolves_each_row_once(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_replay.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    buy_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="deferred buy", defer_code="daily_trade_limit",
    )
    sell_id = await sim.record_pending_decision(
        ticker="000001", direction="sell", target_position=0.0,
        reason="deferred exit", defer_code="market_closed",
    )
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.count_trades_on_date = AsyncMock(return_value=0)
    sim.execute_trade = AsyncMock(side_effect=[
        {"success": True, "action": "sell", "ticker": "000001"},
        {"success": False, "action": "hold", "ticker": "600519", "reason": "heuristic veto"},
    ])

    result = await sim.replay_pending_decisions()
    second = await sim.replay_pending_decisions()
    rows = await (await db._connection.execute(
        "SELECT id, status, replay_count FROM simulation_pending_decisions ORDER BY id"
    )).fetchall()
    await db.close()

    assert result["executed"] == 1
    assert result["rejected"] == 1
    assert result["remaining"] == 0
    assert second["status"] == "empty"
    assert [call.kwargs["ticker"] for call in sim.execute_trade.await_args_list] == ["000001", "600519"]
    assert [call.kwargs["pending_decision_id"] for call in sim.execute_trade.await_args_list] == [sell_id, buy_id]
    assert all(call.kwargs["current_price"] == 0.0 for call in sim.execute_trade.await_args_list)
    assert [tuple(row) for row in rows] == [(buy_id, "rejected", 1), (sell_id, "executed", 1)]


@pytest.mark.asyncio
async def test_pending_replay_daily_limit_reuses_row_without_duplicate(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_no_duplicate.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    pending_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="deferred buy", defer_code="market_closed",
    )
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    # Capacity exists when the queue is selected, then another caller consumes it.
    sim.count_trades_on_date = AsyncMock(side_effect=[0, MAX_DAILY_TRADES])

    result = await sim.replay_pending_decisions()
    rows = await (await db._connection.execute(
        "SELECT id, status, defer_code, replay_count FROM simulation_pending_decisions"
    )).fetchall()
    await db.close()

    assert result["attempted"] == 1
    assert result["remaining"] == 1
    assert [tuple(row) for row in rows] == [
        (pending_id, "pending_next_trading_session", "daily_trade_limit", 1)
    ]


@pytest.mark.asyncio
async def test_pending_replay_expires_stale_ruling_without_trade(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_expired.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    pending_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="stale ruling", defer_code="market_closed",
    )
    await db._connection.execute(
        "UPDATE simulation_pending_decisions SET expires_at = ? WHERE id = ?",
        ((datetime.now() - timedelta(days=1)).isoformat(), pending_id),
    )
    await db._connection.commit()
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.execute_trade = AsyncMock()

    result = await sim.replay_pending_decisions()
    row = await (await db._connection.execute(
        "SELECT status, resolution FROM simulation_pending_decisions WHERE id = ?", (pending_id,)
    )).fetchone()
    await db.close()

    assert result["expired"] == 1
    assert tuple(row) == ("expired", "expired_without_open-session_replay")
    sim.execute_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_market_data_realtime_execution_window():
    market = MarketDataService()
    market.is_trading_day = AsyncMock(return_value=True)
    try:
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T10:00:00")) is True
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T12:30:00")) is False
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T17:00:00")) is False
    finally:
        await market.close()


@pytest.mark.asyncio
async def test_simulation_passes_portfolio_gross_to_heuristic_cap(monkeypatch):
    sim = InvestmentSimulation()
    sim.cash = 8000.0
    sim.positions = {"000001": {"shares": 100, "avg_cost": 20.0}}
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    seen = {}
    quotes = {
        "600519": 4.0,
        "000001": 20.0,
    }

    async def realtime_quote(ticker):
        return {
            "price": quotes[ticker],
            "source": "test_realtime_quote",
            "fetched_at": datetime.now().isoformat(),
        }

    sim.get_current_quote = AsyncMock(side_effect=realtime_quote)

    def fake_cap(ticker, target_position, confidence, **kwargs):
        seen.update(kwargs)
        return 0.05, "gross cap"

    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.setattr(
        "sovereign_hall.services.investment_simulation.apply_heuristic_risk_cap",
        fake_cap,
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.25,
        current_price=4.0,
        reason="committee",
        confidence=0.7,
        signal_count=2,
    )

    assert result["action"] == "buy"
    assert seen["current_position"] == pytest.approx(0.0)
    assert seen["current_gross_exposure"] == pytest.approx(0.20)


@pytest.mark.asyncio
async def test_simulation_assets_are_na_when_realtime_quote_missing(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            ("p1", "600519.SH", 12.3, datetime.now().isoformat(), "long", 0.7),
    )
    await conn.commit()

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value=None)

    assets = await sim.calculate_assets(prices={"600519": 999.0})
    await db.close()

    assert assets["valuation_complete"] is False
    assert assets["total_assets"] is None
    assert assets["positions_value"] is None
    assert assets["missing_price_tickers"] == ["600519"]


@pytest.mark.asyncio
async def test_simulation_assets_use_realtime_quote_not_local_or_prediction(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 15.0, "2026-06-15T10:00:00", "long", 0.7),
    )
    await conn.execute(
        """
        INSERT INTO daily_prices (ticker, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("600519", "2026-07-06", 11.8, 12.4, 11.6, 12.0, 1000, "local_test"),
    )
    await conn.commit()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value={
        "price": 99.0,
        "source": "test_realtime_quote",
        "fetched_at": datetime.now().isoformat(),
    })

    assets = await sim.calculate_assets()
    await db.close()

    assert assets["valuation_complete"] is True
    assert assets["total_assets"] == pytest.approx(18900.0)
    assert assets["positions_value"] == pytest.approx(9900.0)
    sim.get_current_quote.assert_awaited_once_with("600519")


@pytest.mark.asyncio
async def test_simulation_assets_do_not_fallback_to_stale_local_price(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 15.0, "2026-07-06T10:00:00", "long", 0.7),
    )
    await conn.execute(
        """
        INSERT INTO daily_prices (ticker, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("600519", "2026-06-18", 11.8, 12.4, 11.6, 12.0, 1000, "stale_local_test"),
    )
    await conn.commit()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value=None)

    assets = await sim.calculate_assets()
    await db.close()

    assert assets["valuation_complete"] is False
    assert assets["total_assets"] is None
    assert assets["positions_value"] is None


@pytest.mark.asyncio
async def test_simulation_trade_refuses_local_prediction_without_realtime_quote(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 12.3, datetime.now().isoformat(), "long", 0.7),
    )
    await conn.commit()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.get_current_quote = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.2,
        current_price=None,
        reason="prediction-only simulation",
        confidence=0.8,
        signal_count=2,
        risk_cap_already_applied=True,
    )
    await db.close()

    assert result["success"] is False
    assert result["action"] == "hold"
    assert "实时现价" in result["reason"]
    assert sim.positions == {}


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
    assert "二次修正与反事实复盘" in stage3_source
    assert "vote_max_tokens" in stage3_source


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
