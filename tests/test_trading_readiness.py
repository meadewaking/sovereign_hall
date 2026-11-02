from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import sqlite3

import aiosqlite
import pytest

from sovereign_hall.application.prepare_shadow_execution import prepare_execution_intents
from sovereign_hall.domain.portfolio.costs import CostSchedule
from sovereign_hall.domain.portfolio.instruments import InstrumentProfile, InstrumentType
from sovereign_hall.domain.portfolio.shadow import (
    RiskDecision,
    RiskStatus,
    SleeveId,
    TargetPortfolio,
)
from sovereign_hall.infrastructure.paper_broker import PaperBroker
from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.ports.broker import BrokerOrder, BrokerQuote
from sovereign_hall.services.evaluation_engine import (
    CanonicalEvaluationEngine,
    DailyBar,
    EvaluationSpec,
    SystematicETFMomentumV1,
)
from sovereign_hall.services.experiment_store import ExperimentStore
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.shadow_runner import (
    MarketTruthRepository,
    ShadowEvaluationService,
    production_simulation_fingerprint,
)
from sovereign_hall.services.statistical_validation import (
    probability_of_backtest_overfitting,
    validate_experiment_family,
)
from sovereign_hall.scripts.backfill_daily_prices import (
    persist_instrument_profiles,
    requests_from_predictions,
)


def _bar(
    ticker: str,
    day: str,
    price: float,
    *,
    trade_status: str = "normal",
) -> DailyBar:
    return DailyBar(
        ticker=ticker,
        date=day,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1000,
        source="test",
        provider="test_provider",
        fetched_at=f"{day}T18:00:00",
        adjustment="qfq",
        quality_status="validated",
        trade_status=trade_status,
    )


def _spec(sleeve: SleeveId = SleeveId.LLM_RESEARCH) -> EvaluationSpec:
    return EvaluationSpec(
        sleeve=sleeve,
        strategy_version="test_v1",
        qualification_sessions=756 if sleeve is SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1 else 252,
        minimum_closed_trades=0,
    )


def test_595_trade_uses_minimum_commission_and_asset_specific_stamp_duty():
    schedule = CostSchedule()
    stock = schedule.calculate("600000", "sell", notional=595)
    etf = schedule.calculate("510300", "sell", notional=595)

    assert stock.commission == pytest.approx(5.0)
    assert stock.stamp_duty == pytest.approx(595 * 0.0005)
    assert etf.commission == pytest.approx(5.0)
    assert etf.stamp_duty == 0.0
    assert etf.slippage == pytest.approx(595 * 0.0005)


def test_cost_pressure_is_only_for_realistic_10000_account():
    report = CostSchedule().small_account_pressure(10000)
    assert report["minimum_commission_notional_threshold"] == pytest.approx(16666.6666667)
    assert report["full_account_below_minimum_commission_threshold"] is True
    assert report["etf_full_turnover_round_trip_cost"] == pytest.approx(20.0)
    with pytest.raises(ValueError):
        CostSchedule().small_account_pressure(100000)


def test_one_lot_quote_ceiling_includes_minimum_commission_and_slippage():
    schedule = CostSchedule()

    ceiling = schedule.one_lot_execution_quote_ceiling(
        "510300",
        cash=57.14928,
        total_assets=9829.15,
        max_single_position=0.10,
        lot_size=100,
    )

    expected = (57.14928 - schedule.minimum_commission) / (
        100 * (1 + schedule.slippage_rate)
    )
    assert ceiling == pytest.approx(expected)
    assert ceiling == pytest.approx(0.521232184, rel=1e-7)
    affordable = schedule.calculate("510300", "buy", price=ceiling, shares=100)
    assert affordable.notional + affordable.total == pytest.approx(57.14928)
    overstated = schedule.calculate("510300", "buy", price=0.5710, shares=100)
    assert overstated.notional + overstated.total > 57.14928


def test_simulation_and_canonical_engine_use_identical_cost_model():
    simulation = InvestmentSimulation()
    simulation_cost = simulation._trade_cost("510300", "buy", price=5.95, shares=100)
    sessions = ["2026-01-05", "2026-01-06"]
    profile = InstrumentProfile.from_ticker("510300")
    result = CanonicalEvaluationEngine(simulation.cost_schedule).run(
        [_bar("510300", day, 5.95) for day in sessions],
        {sessions[0]: {"510300": 0.10}},
        _spec(),
        benchmark_ticker="510300",
        trading_sessions=sessions,
        instruments={"510300": profile},
    )

    assert len(result.fills) == 1
    assert result.fills[0].commission == simulation_cost.commission == pytest.approx(5.0)
    assert result.fills[0].total_cost == simulation_cost.total
    assert result.fills[0].cost_model_version == simulation.cost_model_version


def test_close_signal_executes_only_at_next_session_open_including_weekends():
    sessions = ["2026-01-05", "2026-01-06"]
    result = CanonicalEvaluationEngine().run(
        [_bar("510300", day, 5.0) for day in sessions],
        {"2026-01-03": {"510300": 0.10}},
        _spec(),
        trading_sessions=sessions,
        instruments={"510300": InstrumentProfile.from_ticker("510300")},
    )
    assert result.fills[0].signal_date == "2026-01-03"
    assert result.fills[0].fill_date == "2026-01-05"
    assert result.curve[0]["fill_count"] == 1


def test_missing_candidate_open_rejects_entry_and_marks_evaluation_unavailable():
    sessions = ["2026-01-05", "2026-01-06"]
    result = CanonicalEvaluationEngine().run(
        [_bar("510300", "2026-01-05", 5.0)],
        {"2026-01-05": {"510300": 0.10}},
        _spec(),
        trading_sessions=sessions,
        instruments={"510300": InstrumentProfile.from_ticker("510300")},
    )
    assert result.fills == ()
    assert result.metrics["missing_open_rejections"] == 1
    assert result.status == "data_unavailable"
    assert "candidate_or_exit_open_missing" in result.metrics["data_quality"]["reasons"]


def test_missing_held_mark_carries_last_price_and_records_stale_days():
    sessions = ["2026-01-05", "2026-01-06", "2026-01-07"]
    bars = [_bar("510300", "2026-01-05", 5.0), _bar("510300", "2026-01-06", 5.1)]
    result = CanonicalEvaluationEngine().run(
        bars,
        {"2026-01-05": {"510300": 0.10}},
        _spec(),
        trading_sessions=sessions,
        instruments={"510300": InstrumentProfile.from_ticker("510300")},
    )
    assert result.curve[-1]["stale_days"] == {"510300": 1}
    assert result.curve[-1]["market_value"] == result.curve[-2]["market_value"]
    assert result.metrics["stale_mark_sessions"] == 1
    assert result.status == "data_unavailable"


def test_suspended_bar_cannot_fill_but_is_not_fabricated_as_missing_data():
    sessions = ["2026-01-05", "2026-01-06"]
    result = CanonicalEvaluationEngine().run(
        [
            _bar("510300", "2026-01-05", 5.0),
            _bar("510300", "2026-01-06", 5.0, trade_status="suspended"),
        ],
        {"2026-01-05": {"510300": 0.10}},
        _spec(),
        trading_sessions=sessions,
        instruments={"510300": InstrumentProfile.from_ticker("510300")},
    )
    assert result.fills == ()
    assert result.metrics["suspended_or_inactive_rejections"] == 1
    assert result.metrics["data_quality"]["usable"] is True


def test_missing_listing_metadata_is_an_explicit_qualification_blocker():
    sessions = ["2026-01-05", "2026-01-06"]
    spec = replace(_spec(), require_instrument_lifecycle_metadata=True)
    result = CanonicalEvaluationEngine().run(
        [_bar("510300", day, 5.0) for day in sessions],
        {"2026-01-05": {"510300": 0.10}},
        spec,
        trading_sessions=sessions,
        instruments={"510300": InstrumentProfile.from_ticker("510300")},
    )
    assert result.status == "data_unavailable"
    assert result.metrics["incomplete_lifecycle_profiles"] == ["510300"]
    assert (
        "instrument_lifecycle_metadata_incomplete"
        in result.metrics["data_quality"]["reasons"]
    )


def test_cagr_is_hidden_below_252_sessions_and_available_at_gate():
    short_sessions = [f"2026-01-{day:02d}" for day in range(1, 11)]
    short = CanonicalEvaluationEngine().run(
        [_bar("510300", day, 5.0) for day in short_sessions],
        {},
        _spec(),
        trading_sessions=short_sessions,
        instruments={},
    )
    assert short.metrics["net_cagr"] is None
    assert short.metrics["annualization_status"] == "insufficient_history"

    start = date(2025, 1, 1)
    sessions: list[str] = []
    cursor = start
    while len(sessions) < 252:
        if cursor.weekday() < 5:
            sessions.append(cursor.isoformat())
        cursor += timedelta(days=1)
    full = CanonicalEvaluationEngine().run(
        [_bar("510300", day, 5.0) for day in sessions],
        {},
        _spec(),
        trading_sessions=sessions,
        instruments={},
    )
    assert full.metrics["net_cagr"] == pytest.approx(0.0)
    assert full.metrics["annualization_status"] == "available"


def test_identical_inputs_produce_identical_orders_curve_and_metrics():
    sessions = ["2026-01-05", "2026-01-06", "2026-01-07"]
    args = (
        [_bar("510300", day, 5.0 + index * 0.1) for index, day in enumerate(sessions)],
        {"2026-01-05": {"510300": 0.10}, "2026-01-06": {}},
        _spec(),
    )
    kwargs = {
        "trading_sessions": sessions,
        "instruments": {"510300": InstrumentProfile.from_ticker("510300")},
    }
    first = CanonicalEvaluationEngine().run(*args, **kwargs)
    second = CanonicalEvaluationEngine().run(*args, **kwargs)
    assert first.as_dict() == second.as_dict()


def test_reporting_gates_do_not_inflate_the_strategy_trial_hash():
    base = _spec()
    stricter_reporting = replace(
        base,
        qualification_sessions=999,
        minimum_closed_trades=100,
        require_instrument_lifecycle_metadata=True,
        minimum_dsr_probability=0.99,
    )
    changed_trading_rule = replace(base, rebalance_band=0.01)
    assert stricter_reporting.strategy_hash == base.strategy_hash
    assert changed_trading_rule.strategy_hash != base.strategy_hash


def test_drawdown_risk_reduces_only_incremental_budget_and_blocks_at_15pct():
    spec = _spec()
    position = type("Position", (), {"shares": 100})()
    bars = {"510300": _bar("510300", "2026-01-06", 10.0)}
    reduced = CanonicalEvaluationEngine.apply_drawdown_risk(
        {"510300": 0.20, "510500": 0.10},
        {"510300": position},
        10000,
        bars,
        {},
        -0.10,
        spec,
    )
    assert reduced.status is RiskStatus.REDUCED
    assert reduced.approved_targets["510300"] == pytest.approx(0.15)
    assert reduced.approved_targets["510500"] == pytest.approx(0.05)
    blocked = CanonicalEvaluationEngine.apply_drawdown_risk(
        {"510300": 0.20, "510500": 0.10},
        {"510300": position},
        10000,
        bars,
        {},
        -0.15,
        spec,
    )
    assert blocked.status is RiskStatus.BLOCKED_NEW_RISK
    assert blocked.approved_targets == {"510300": pytest.approx(0.10)}


def test_frozen_systematic_etf_baseline_uses_12_1_and_ten_percent_weights():
    start = date(2024, 1, 1)
    bars: list[DailyBar] = []
    cursor = start
    count = 0
    while count < 270:
        if cursor.weekday() < 5:
            bars.append(_bar("510300", cursor.isoformat(), 4.0 + count * 0.01))
            count += 1
        cursor += timedelta(days=1)
    targets, signals = SystematicETFMomentumV1().build(bars)
    assert SystematicETFMomentumV1.version == "systematic_etf_momentum_v1"
    assert signals
    assert all(weight == pytest.approx(0.10) for target in targets.values() for weight in target.values())
    assert all(signal.sleeve is SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1 for signal in signals)


def test_systematic_baseline_never_ranks_a_stale_month_end_price():
    start = date(2024, 1, 1)
    bars: list[DailyBar] = []
    cursor = start
    count = 0
    while count < 253:
        if cursor.weekday() < 5:
            bars.append(_bar("510300", cursor.isoformat(), 4.0 + count * 0.01))
            count += 1
        cursor += timedelta(days=1)
    for offset in range(45):
        day = cursor + timedelta(days=offset)
        if day.weekday() < 5:
            bars.append(_bar("510500", day.isoformat(), 5.0))
    targets, _signals = SystematicETFMomentumV1().build(bars)
    last_510300_date = max(bar.date for bar in bars if bar.ticker == "510300")
    assert all(
        "510300" not in target
        for signal_date, target in targets.items()
        if signal_date > last_510300_date
    )


def test_research_to_execution_boundary_never_contains_share_quantity():
    portfolio = TargetPortfolio(
        sleeve=SleeveId.LLM_RESEARCH,
        as_of="2026-01-05",
        strategy_version="v1",
        targets={"510300": 0.10},
    )
    risk = RiskDecision(RiskStatus.APPROVED, portfolio.targets, (), 0.0)
    intents = prepare_execution_intents(portfolio, risk, current_weights={})
    assert intents[0].target_position == pytest.approx(0.10)
    assert not hasattr(intents[0], "shares")


@pytest.mark.asyncio
async def test_migration_is_idempotent_and_evaluation_results_are_append_only(tmp_path):
    db_path = tmp_path / "readiness.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        await apply_architecture_migrations(conn)
        tables = {
            row[0]
            for row in await (
                await conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            ).fetchall()
        }
        assert {"instrument_master", "trading_calendar", "evaluation_runs", "paper_accounts"} <= tables
        values = (
            "eval_x", "llm_research", "v1", "hash", "engine", "2026-01-01",
            "data", "universe", "[]", "cost", "{}", 10000, 1,
            "not_proven", "{}", "{}", "{}", "2026-01-01T00:00:00",
        )
        await conn.execute(
            """
            INSERT INTO evaluation_runs (
                id,sleeve,strategy_version,strategy_hash,engine_version,data_cutoff,
                data_hash,universe_hash,universe_json,cost_model_version,
                cost_schedule_json,initial_capital,trial_count,status,config_json,
                metrics_json,result_json,created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            values,
        )
        await conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            await conn.execute("UPDATE evaluation_runs SET status = 'qualified' WHERE id = 'eval_x'")


@pytest.mark.asyncio
async def test_paper_broker_enforces_lots_t_plus_one_costs_and_reconciliation(tmp_path):
    db_path = tmp_path / "paper.db"
    broker = PaperBroker(db_path)
    await broker.initialize()
    async with aiosqlite.connect(db_path) as conn:
        await conn.executemany(
            """
            INSERT INTO trading_calendar (
                trade_date, market, is_open, source, fetched_at, quality_status
            ) VALUES (?, 'CN', 1, 'test', 'now', 'validated')
            """,
            [("2026-01-05",), ("2026-01-06",), ("2026-01-07",)],
        )
        await conn.commit()
    await broker.submit_order(
        BrokerOrder(
            "510300", "buy", 100, "2026-01-05T15:00:00", "2026-01-06",
            "buy", submitted_at="2026-01-05T15:01:00",
        )
    )
    await broker.submit_order(
        BrokerOrder(
            "510300", "sell", 100, "2026-01-05T15:00:00", "2026-01-06",
            "same_day_sell", submitted_at="2026-01-05T15:02:00",
        )
    )
    quote = BrokerQuote("510300", 5.95, "test", "2026-01-06T09:30:00")
    fills = await broker.execute_pending("2026-01-06", {"510300": quote})
    assert [fill.side for fill in fills] == ["buy"]
    assert fills[0].total_cost == pytest.approx(5.0 + 595 * 0.0005)
    await broker.submit_order(
        BrokerOrder(
            "510300", "sell", 100, "2026-01-06T15:00:00", "2026-01-07", "sell"
        )
    )
    exit_fills = await broker.execute_pending(
        "2026-01-07",
        {"510300": BrokerQuote("510300", 6.0, "test", "2026-01-07T09:30:00")},
    )
    assert exit_fills[0].side == "sell"
    async with aiosqlite.connect(db_path) as conn:
        async with conn.execute(
            "SELECT stamp_duty FROM paper_fills WHERE side = 'sell'"
        ) as cursor:
            assert float((await cursor.fetchone())[0]) == 0.0
    reconciliation = await broker.reconcile("2026-01-07T15:00:00")
    assert reconciliation.status == "balanced"


def test_paper_broker_hard_rejects_live_mode(tmp_path):
    with pytest.raises(RuntimeError, match="live execution"):
        PaperBroker(tmp_path / "paper.db", execution_mode="live")


@pytest.mark.asyncio
async def test_shadow_sleeves_never_mutate_production_simulation_account(tmp_path):
    db_path = tmp_path / "shadow.db"
    await ExperimentStore(db_path).initialize()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE system_stats(key TEXT PRIMARY KEY, value TEXT);
            INSERT INTO system_stats VALUES('simulation_cash', '9000');
            CREATE TABLE simulation_positions(ticker TEXT, shares INTEGER, avg_cost REAL);
            INSERT INTO simulation_positions VALUES('600000', 100, 10);
            CREATE TABLE simulation_trades(
                id INTEGER PRIMARY KEY, ticker TEXT, direction TEXT, shares INTEGER,
                price REAL, fee REAL, reason TEXT, traded_at TEXT
            );
            INSERT INTO simulation_trades VALUES(1, '600000', 'buy', 100, 10, 5, 'x', '2026-01-01');
            CREATE TABLE price_predictions(
                id TEXT, ticker TEXT, direction TEXT, confidence REAL,
                predicted_at TEXT, round_id TEXT, decision_id TEXT
            );
            INSERT INTO price_predictions VALUES(
                'p1','510300','long',0.8,'2026-04-01T15:00:00','r1','d1'
            );
            INSERT INTO instrument_master(
                ticker,instrument_type,lot_size,source,updated_at
            ) VALUES('510300','etf',100,'test','now');
            """
        )
        for day, price in (
            ("2025-01-02", 4.0),
            ("2026-04-01", 5.0),
            ("2026-04-02", 5.1),
        ):
            conn.execute(
                """
                INSERT INTO trading_calendar (
                    trade_date, market, is_open, source, fetched_at, quality_status
                ) VALUES (?, 'CN', 1, 'test', 'now', 'validated')
                """,
                (day,),
            )
            conn.execute(
                """
                INSERT INTO daily_prices(
                    ticker,date,open,high,low,close,volume,source,adjustment,
                    provider,fetched_at,quality_status,trade_status
                ) VALUES('510300',?,?,?,?,?,1000,'test','qfq','test','now','validated','normal')
                """,
                (day, price, price, price, price),
            )
        conn.commit()
    before = production_simulation_fingerprint(db_path)
    outcome = await ShadowEvaluationService(db_path).run(SleeveId.LLM_RESEARCH, persist=True)
    after = production_simulation_fingerprint(db_path)
    assert after == before
    assert outcome.result.metrics["sessions"] == 2
    assert outcome.result.metrics["net_cagr"] is None
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM simulation_trades").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM evaluation_runs").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_llm_sleeve_is_forward_only_from_april_2026(tmp_path):
    db_path = tmp_path / "llm.db"
    await ExperimentStore(db_path).initialize()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE price_predictions(
                id TEXT,ticker TEXT,direction TEXT,confidence REAL,
                predicted_at TEXT,round_id TEXT,decision_id TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO price_predictions VALUES(?,?,?,?,?,?,?)",
            [
                ("old", "510300", "long", 0.9, "2026-03-31T15:00:00", "r0", "d0"),
                ("new", "510500", "long", 0.8, "2026-04-01T15:00:00", "r1", "d1"),
            ],
        )
        conn.commit()
    targets, signals = MarketTruthRepository(db_path).llm_forward_targets()
    assert [signal.ticker for signal in signals] == ["510500"]
    assert targets == {"2026-04-01": {"510500": 0.10}}


def test_llm_lifecycle_backfill_uses_only_forward_signals_even_without_embedded_price(tmp_path):
    db_path = tmp_path / "signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE price_predictions(ticker TEXT, predicted_at TEXT, current_price REAL)"
        )
        conn.executemany(
            "INSERT INTO price_predictions VALUES(?,?,?)",
            [
                ("510050", "2026-03-31T15:00:00", 5.0),
                ("600000", "2026-04-02T15:00:00", None),
                ("510300", "2026-08-15T15:00:00", 4.0),
            ],
        )
        conn.commit()
    requests = requests_from_predictions(
        db_path,
        date(2026, 4, 1),
        date(2026, 8, 14),
    )
    assert [request.ticker for request in requests] == ["600000"]
    assert requests[0].start == date(2026, 4, 1)
    assert requests[0].end == date(2026, 8, 14)


def test_signal_universe_master_persists_type_without_inventing_listing_dates(tmp_path):
    db_path = tmp_path / "master.db"
    written = persist_instrument_profiles(
        db_path,
        ["510300", "600000"],
        source="prediction_signal_universe",
    )
    assert written == 2
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT ticker, instrument_type, lot_size, listed_at, delisted_at, source
            FROM instrument_master ORDER BY ticker
            """
        ).fetchall()
    assert rows == [
        ("510300", "etf", 100, None, None, "prediction_signal_universe"),
        ("600000", "a_share", 100, None, None, "prediction_signal_universe"),
    ]


def test_dsr_pbo_gates_remain_unavailable_for_single_frozen_trial():
    returns = [0.001, -0.0005] * 30
    validation = validate_experiment_family(returns, {"only": returns})
    assert validation.dsr_probability is None
    assert validation.pbo_probability is None
    assert validation.status == "insufficient_evidence"
    assert probability_of_backtest_overfitting({"only": returns}) is None
