"""Synthetic regression cases; never investment-quality or return evidence."""
from copy import deepcopy
from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from sovereign_hall.services.exchange_calendar import load_exchange_calendar
from sovereign_hall.services.portfolio_regime_policy import build_review
from sovereign_hall.services.market_state_policy import UNIVERSE
from sovereign_hall.core import PROJECT_ROOT

ZONE = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def observation():
    cutoff = "2026-10-21T14:00:00+08:00"
    days = sorted(day for day, opened in load_exchange_calendar(2026).sessions.items()
                  if opened and day < cutoff[:10])[-70:]
    tapes = {ticker: dict(ticker=ticker, source_sha256="synthetic_" + ticker,
        bars=[dict(date=day, close=300-i) for i, day in enumerate(days)]) for ticker in UNIVERSE}
    state = dict(cutoff=cutoff, latest_completed_session=days[-1], cash=8200.0,
        positions=[dict(ticker="512800", shares=1800, avg_cost=1.0, opened_at="2026-10-20")],
        quotes={"512800": dict(ticker="512800", price=1.0, source="sina_realtime_quote",
            quoted_at="2026-10-21T13:59:59+08:00", fetched_at="2026-10-21T13:59:59+08:00")},
        policy=dict(execution_mode="simulation", stop_loss_pct=-.08, take_profit_pct=.15,
            max_holding_days=30, trading_fee=.0003, minimum_commission=5,
            etf_stamp_duty=0, slippage_rate=.0005))
    return tapes, state


def test_inherited_exposure_changes_action_with_real_costs_and_no_fake_vote(observation):
    result = build_review(*observation)
    assert result["regime"] == "defensive" and result["changes_action"]
    assert result["arms"]["control"]["reviews"][0]["action"] == "hold"
    candidate = result["arms"]["candidate"]
    assert candidate["intent_drafts"] == [dict(ticker="512800", direction="sell",
        target_position=0.0, reason="declared_equity_regime_abstention", confidence=None)]
    assert candidate["estimated_exit_fees"] == pytest.approx(5.9)
    assert result["model_calls"] == result["account_writes"] == 0
    assert result["investment_quality_improvement"] is result["return_improvement"] is None


def test_cash_is_a_genuine_abstention_branch_without_mandated_trades(observation):
    tapes, state = observation
    state.update(cash=10000, positions=[])
    result = build_review(tapes, state)
    assert not result["changes_action"]
    assert all(not arm["intent_drafts"] for arm in result["arms"].values())


def test_one_eligible_signal_restores_ordinary_review_without_forcing_an_entry(observation):
    tapes, state = observation
    for i, bar in enumerate(tapes["510300"]["bars"]):
        bar["close"] = 100+i
    result = build_review(tapes, state)
    assert result["regime"] == "ordinary" and not result["changes_action"]
    assert all(not arm["intent_drafts"] for arm in result["arms"].values())


def test_existing_protective_exit_is_common_to_both_arms(observation):
    tapes, state = observation
    state["quotes"]["512800"]["price"] = .90
    result = build_review(tapes, state)
    assert not result["changes_action"]
    assert all("止损" in arm["intent_drafts"][0]["reason"] for arm in result["arms"].values())


def test_unknown_etf_exposure_is_not_guessed_from_its_prefix(observation):
    tapes, state = observation
    state["positions"][0]["ticker"] = "518880"
    quote = state["quotes"].pop("512800")
    quote["ticker"] = "518880"
    state["quotes"]["518880"] = quote
    result = build_review(tapes, state)
    assert not result["changes_action"]
    assert all(not arm["intent_drafts"] for arm in result["arms"].values())


@pytest.mark.parametrize("kind", ["missing", "stale_source", "future_fetch", "uncontrolled", "valuation_only"])
def test_untrusted_quote_cannot_price_exit_but_price_free_reduction_is_possible(observation, kind):
    tapes, state = observation
    quote = state["quotes"]["512800"]
    if kind == "missing":
        state["quotes"] = {}
    elif kind == "stale_source":
        quote["quoted_at"] = "2026-10-21T13:50:00+08:00"
    elif kind == "future_fetch":
        quote["fetched_at"] = "2026-10-21T14:00:01+08:00"
    elif kind == "uncontrolled":
        quote["source"] = "made_up_realtime_quote"
    else:
        quote["valuation_only"] = True
    result = build_review(tapes, state)
    arm = result["arms"]["candidate"]
    assert len(arm["intent_drafts"]) == 1 and arm["estimated_exit_fees"] == 0
    assert arm["unpriced_exits"] == ["512800"]
    assert all("price" not in key for key in arm["intent_drafts"][0])


def test_replay_is_deterministic_and_does_not_modify_inputs(observation):
    before = deepcopy(observation)
    assert build_review(*observation) == build_review(*observation)
    assert observation == before


@pytest.mark.parametrize("kind", ["missing_universe", "future_session", "duplicate", "fractional", "nan_cash"])
def test_ambiguous_state_is_rejected_before_planning(observation, kind):
    tapes, state = observation
    if kind == "missing_universe":
        tapes.pop("510500")
    elif kind == "future_session":
        state["latest_completed_session"] = "2026-10-21"
    elif kind == "duplicate":
        state["positions"] *= 2
    elif kind == "fractional":
        state["positions"][0]["shares"] = 1800.0
    else:
        state["cash"] = float("nan")
    with pytest.raises(ValueError):
        build_review(tapes, state)


def cli():
    spec = importlib.util.spec_from_file_location("portfolio_regime_cli",
        PROJECT_ROOT / "scripts/run_portfolio_regime_experiment.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_rejects_changed_source_before_database_or_network(tmp_path):
    module = cli()
    (tmp_path / "worker.py").write_text("tampered")
    (tmp_path / "protocol.json").write_text(json.dumps(dict(worker_sha256="expected", artifacts={})))
    with pytest.raises(RuntimeError, match="worker changed"):
        module.verify(tmp_path)
    assert len(list(tmp_path.iterdir())) == 2


@pytest.mark.parametrize("elapsed,allowed", [(0, True), (120, True), (121, False), (-1, False)])
def test_old_common_account_cannot_be_reaged_into_a_new_transition(elapsed, allowed):
    module = cli()
    cutoff = datetime(2026, 10, 21, 14, tzinfo=ZONE)
    data = dict(cutoff=cutoff.isoformat())
    if allowed:
        module.check_execution_window(data, now=cutoff + timedelta(seconds=elapsed))
    else:
        with pytest.raises(RuntimeError, match="transition window"):
            module.check_execution_window(data, now=cutoff + timedelta(seconds=elapsed))
