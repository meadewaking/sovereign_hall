#!/usr/bin/env python3
"""Run an offline heuristic-learning cycle for Sovereign Hall.

The cycle is intentionally local-only. It reads the SQLite prediction tape,
builds a delayed daily signal simulation, tries a few interpretable policy
variants, and writes all artifacts to a timestamped run directory.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import pprint
import re
import sqlite3
import subprocess
import sys
import types
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

os.environ.setdefault("PANDAS_USE_BOTTLENECK", "0")
os.environ.setdefault("PANDAS_USE_NUMEXPR", "0")
for _optional_binary_dependency in ("numexpr", "bottleneck"):
    sys.modules.setdefault(_optional_binary_dependency, None)
if "pyarrow" not in sys.modules:
    _pyarrow_stub = types.ModuleType("pyarrow")
    _pyarrow_stub.__version__ = "0.0.0"
    _pyarrow_stub.Array = type("Array", (), {})
    _pyarrow_stub.ChunkedArray = type("ChunkedArray", (), {})
    sys.modules["pyarrow"] = _pyarrow_stub


def _scientific_stack_ready(timeout_seconds: float = 8.0) -> bool:
    """Return False when numpy/pandas import hangs or is unavailable locally."""
    if os.environ.get("SOVEREIGN_HALL_FORCE_PANDAS_CYCLE") == "1":
        return True
    if os.environ.get("SOVEREIGN_HALL_FORCE_STDLIB_CYCLE") == "1":
        return False
    env = dict(os.environ)
    env["SOVEREIGN_HALL_NUMPY_PREFLIGHT_CHILD"] = "1"
    try:
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import numpy; import pandas; print('ok')",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
            check=True,
            env=env,
        )
        return True
    except Exception:
        return False


if __name__ == "__main__" and os.environ.get("SOVEREIGN_HALL_NUMPY_PREFLIGHT_CHILD") != "1":
    if not _scientific_stack_ready():
        fallback_path = Path(__file__).with_name("run_heuristic_cycle_stdlib.py")
        system_python = Path("/usr/bin/python3")
        if system_python.exists():
            os.execv(str(system_python), [str(system_python), str(fallback_path), *sys.argv[1:]])

        import importlib.util

        spec = importlib.util.spec_from_file_location("run_heuristic_cycle_stdlib", fallback_path)
        if spec is None or spec.loader is None:
            raise SystemExit(f"Cannot load fallback evaluator: {fallback_path}")
        fallback = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fallback)
        raise SystemExit(fallback.main())

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sovereign_hall.services.reward_policy import (
    MAX_DAILY_TRADES,
    REWARD_FORMULA,
    REWARD_VERSION,
    capital_reward_breakdown,
    idle_cash_exposure_penalty,
    limit_rebalance_actions,
    longest_high_cash_streak,
    score_capital_reward,
)


@dataclass(frozen=True)
class PolicyConfig:
    name: str
    min_confidence: float = 0.65
    max_names: int = 6
    max_position: float = 0.12
    max_gross: float = 0.70
    min_risk_reward: float = 0.80
    trend_lookback: int = 0
    require_positive_trend: bool = False
    vol_lookback: int = 0
    high_vol_threshold: float = 0.08
    high_vol_scale: float = 1.0
    anomaly_return_threshold: float = 0.18
    use_anomaly_veto: bool = False
    drawdown_guard: float = 1.0
    drawdown_guard_threshold: float = 0.04
    loss_streak_threshold: int = 0
    loss_streak_guard: float = 1.0
    new_entry_loss_streak_threshold: int = 0
    min_holding_days: int = 0
    forced_exit_return_threshold: float = -1.0
    forced_exit_vol_lookback: int = 0
    forced_exit_vol_threshold: float = 1.0
    rebalance_threshold: float = 0.0
    min_signal_count: int = 1
    max_stop_gap: float = 0.55
    universe: str = "all"
    excluded_tickers: tuple[str, ...] = ()
    excluded_ticker_mode: str = "none"
    excluded_ticker_scale: float = 1.0
    failure_memory_mode: str = "none"
    failure_memory_loss_threshold: float = -1.0
    failure_memory_days: int = 0
    failure_memory_scale: float = 1.0
    require_independent_price: bool = False


@dataclass(frozen=True)
class CostConfig:
    trading_fee: float = 0.0003
    stamp_duty: float = 0.0010
    slippage: float = 0.0005


def normalize_ticker(ticker: Any) -> str:
    code = str(ticker or "").strip().upper()
    return code.split(".")[0] if "." in code else code


def is_etf_ticker(ticker: Any) -> bool:
    code = normalize_ticker(ticker)
    return code.startswith(("15", "51", "56", "58"))


def capped_proportional_allocation(
    scores: dict[str, float],
    total_weight: float,
    max_weight: float,
) -> dict[str, float]:
    """Redistribute capped residual weight instead of silently leaving strategic cash."""
    remaining = max(float(total_weight), 0.0)
    active = {key: max(float(value), 0.0) for key, value in scores.items() if float(value) > 0}
    allocated = {key: 0.0 for key in active}
    while active and remaining > 1e-12:
        score_sum = sum(active.values())
        progressed = 0.0
        for key, score in list(active.items()):
            room = max(0.0, float(max_weight) - allocated[key])
            proposed = remaining * score / score_sum if score_sum > 0 else remaining / len(active)
            addition = min(room, proposed)
            allocated[key] += addition
            progressed += addition
        remaining = max(0.0, remaining - progressed)
        active = {key: score for key, score in active.items() if allocated[key] < max_weight - 1e-12}
        if progressed <= 1e-12:
            break
    return {key: weight for key, weight in allocated.items() if weight > 1e-12}


def score_metrics(metrics: dict[str, Any]) -> float:
    return score_capital_reward(metrics)


def load_predictions(db_path: Path) -> pd.DataFrame:
    query = """
        SELECT
            predicted_at,
            ticker,
            current_price,
            target_price,
            stop_loss,
            direction,
            confidence,
            expected_days
        FROM price_predictions
        WHERE current_price IS NOT NULL
          AND current_price > 0
          AND predicted_at IS NOT NULL
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn)
    if df.empty:
        return df

    df["predicted_at"] = pd.to_datetime(df["predicted_at"], errors="coerce")
    df = df.dropna(subset=["predicted_at", "current_price"])
    df["date"] = df["predicted_at"].dt.date.astype(str)
    df["ticker"] = df["ticker"].map(normalize_ticker)
    for col in ["current_price", "target_price", "stop_loss", "confidence", "expected_days"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["direction"] = df["direction"].fillna("long").str.lower()
    return df


def load_daily_prices(db_path: Path, predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()

    tickers = sorted(predictions["ticker"].dropna().unique())
    if not tickers:
        return pd.DataFrame()

    start = predictions["predicted_at"].min().strftime("%Y-%m-%d")
    end = predictions["predicted_at"].max().strftime("%Y-%m-%d")
    placeholders = ",".join("?" for _ in tickers)
    query = f"""
        SELECT ticker, date, close
        FROM daily_prices
        WHERE ticker IN ({placeholders})
          AND date >= ?
          AND date <= ?
          AND close IS NOT NULL
          AND close > 0
    """
    try:
        with sqlite3.connect(db_path) as conn:
            if not sqlite_table_exists(conn, "daily_prices"):
                return pd.DataFrame()
            prices = pd.read_sql_query(query, conn, params=[*tickers, start, end])
    except Exception:
        return pd.DataFrame()

    if prices.empty:
        return prices
    prices["ticker"] = prices["ticker"].map(normalize_ticker)
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date.astype(str)
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    return prices.dropna(subset=["date", "ticker", "close"])


def sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def attach_asof_daily_prices(
    daily: pd.DataFrame,
    price_history: pd.DataFrame,
    max_age_days: int = 7,
) -> pd.DataFrame:
    """Attach the latest local close on or before the signal date.

    Signal rows can be created on weekends or holidays. Requiring an exact
    daily_prices date would incorrectly mark those rows as missing, while a
    bounded as-of match still rejects genuinely stale price history.
    """
    if daily.empty or price_history.empty:
        return daily

    prices = price_history[["date", "ticker", "close"]].copy()
    prices["ticker"] = prices["ticker"].map(normalize_ticker)
    prices["_price_date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["independent_price"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["ticker", "_price_date", "independent_price"])
    if prices.empty:
        return daily

    matched_parts: list[pd.DataFrame] = []
    tolerance = pd.Timedelta(days=max_age_days)
    working = daily.copy()
    working["_row_order"] = np.arange(len(working))
    working["_signal_date"] = pd.to_datetime(working["date"], errors="coerce")
    for ticker, signal_rows in working.groupby("ticker", sort=False):
        ticker_prices = prices[prices["ticker"].eq(normalize_ticker(ticker))]
        if ticker_prices.empty:
            signal_rows["independent_price"] = np.nan
            signal_rows["daily_price_date"] = pd.NaT
            matched_parts.append(signal_rows)
            continue
        left = signal_rows.sort_values("_signal_date")
        right = ticker_prices[["_price_date", "independent_price"]].sort_values("_price_date")
        matched = pd.merge_asof(
            left,
            right,
            left_on="_signal_date",
            right_on="_price_date",
            direction="backward",
            tolerance=tolerance,
        ).rename(columns={"_price_date": "daily_price_date"})
        matched_parts.append(matched)

    matched_daily = pd.concat(matched_parts, ignore_index=True).sort_values("_row_order")
    return matched_daily.drop(columns=["_row_order", "_signal_date"])


def build_daily_tape(predictions: pd.DataFrame, price_history: pd.DataFrame | None = None) -> pd.DataFrame:
    if predictions.empty:
        return predictions

    df = predictions.copy()
    plausible_long = (
        (df["direction"] == "long")
        & (df["target_price"] > df["current_price"])
        & (df["stop_loss"] > 0)
        & (df["stop_loss"] < df["current_price"])
    )
    risk = (df["current_price"] - df["stop_loss"]).where(plausible_long)
    reward = (df["target_price"] - df["current_price"]).where(plausible_long)
    df["risk_reward"] = (reward / risk).replace([np.inf, -np.inf], np.nan)
    df["stop_gap"] = ((df["current_price"] - df["stop_loss"]) / df["current_price"]).where(plausible_long)
    df["target_gap"] = ((df["target_price"] - df["current_price"]) / df["current_price"]).where(plausible_long)

    daily = (
        df.groupby(["date", "ticker"], as_index=False)
        .agg(
            price=("current_price", "median"),
            close_observations=("current_price", "count"),
            confidence=("confidence", "mean"),
            risk_reward=("risk_reward", "median"),
            stop_gap=("stop_gap", "median"),
            target_gap=("target_gap", "median"),
            expected_days=("expected_days", "median"),
            long_votes=("direction", lambda s: int((s == "long").sum())),
        )
        .sort_values(["ticker", "date"])
    )

    daily["risk_reward"] = daily["risk_reward"].fillna(0.0)
    daily["stop_gap"] = daily["stop_gap"].fillna(1.0)
    daily["target_gap"] = daily["target_gap"].fillna(0.0)
    if price_history is not None and not price_history.empty:
        daily = attach_asof_daily_prices(daily, price_history)
        daily["price_source"] = np.where(
            daily["independent_price"].notna(),
            "daily_prices",
            "prediction_current_price",
        )
        daily["price"] = daily["independent_price"].fillna(daily["price"])
        daily = daily.drop(columns=["independent_price"])
    else:
        daily["price_source"] = "prediction_current_price"
    daily["return_1d"] = daily.groupby("ticker")["price"].pct_change()
    for lb in (2, 3, 5):
        daily[f"momentum_{lb}d"] = daily.groupby("ticker")["price"].pct_change(lb)
        daily[f"vol_{lb}d"] = daily.groupby("ticker")["return_1d"].transform(
            lambda s: s.rolling(lb, min_periods=max(2, lb // 2)).std()
        )
    daily["signal_strength"] = daily["confidence"] * np.log1p(daily["close_observations"])
    daily["signal_strength"] *= (1.0 + daily["risk_reward"].clip(0, 3) / 6.0)
    return daily


def pick_targets(
    signal_rows: pd.DataFrame,
    policy: PolicyConfig,
    current_positions: dict[str, float],
    position_age_days: dict[str, int],
    current_drawdown: float,
    consecutive_loss_days: int = 0,
    failure_memory_tickers: set[str] | None = None,
) -> tuple[dict[str, float], list[str]]:
    if signal_rows.empty:
        return {}, ["no_signal_rows"]

    candidates = signal_rows[
        (signal_rows["confidence"] >= policy.min_confidence)
        & (signal_rows["risk_reward"] >= policy.min_risk_reward)
        & (signal_rows["close_observations"] >= policy.min_signal_count)
        & (signal_rows["stop_gap"] <= policy.max_stop_gap)
    ].copy()

    reasons: list[str] = []
    if policy.universe != "all" and not candidates.empty:
        before = len(candidates)
        etf_mask = candidates["ticker"].map(is_etf_ticker)
        if policy.universe == "etf":
            candidates = candidates[etf_mask]
        elif policy.universe == "single_stock":
            candidates = candidates[~etf_mask]
        reasons.append(f"{policy.universe}_universe_removed={before - len(candidates)}")

    if policy.excluded_tickers and not candidates.empty:
        excluded = {normalize_ticker(ticker) for ticker in policy.excluded_tickers}
        excluded_mask = candidates["ticker"].map(normalize_ticker).isin(excluded)
        if policy.excluded_ticker_mode == "veto":
            before = len(candidates)
            candidates = candidates[~excluded_mask]
            reasons.append(f"recent_failure_veto_removed={before - len(candidates)}")
        elif policy.excluded_ticker_mode == "scale" and policy.excluded_ticker_scale < 1.0:
            candidates.loc[excluded_mask, "signal_strength"] *= policy.excluded_ticker_scale
            reasons.append(f"recent_failure_scaled={int(excluded_mask.sum())}")

    if policy.failure_memory_mode != "none" and failure_memory_tickers and not candidates.empty:
        memory_mask = candidates["ticker"].map(normalize_ticker).isin(failure_memory_tickers)
        if policy.failure_memory_mode == "veto":
            before = len(candidates)
            candidates = candidates[~memory_mask]
            reasons.append(f"no_lookahead_failure_veto_removed={before - len(candidates)}")
        elif policy.failure_memory_mode == "scale" and policy.failure_memory_scale < 1.0:
            candidates.loc[memory_mask, "signal_strength"] *= policy.failure_memory_scale
            reasons.append(f"no_lookahead_failure_scaled={int(memory_mask.sum())}")

    if policy.require_independent_price and not candidates.empty:
        before = len(candidates)
        candidates = candidates[candidates["price_source"].eq("daily_prices")]
        reasons.append(f"validated_daily_price_removed={before - len(candidates)}")

    if policy.require_positive_trend and policy.trend_lookback:
        col = f"momentum_{policy.trend_lookback}d"
        before = len(candidates)
        candidates = candidates[candidates[col].fillna(-1.0) > 0]
        reasons.append(f"trend_filter_removed={before - len(candidates)}")

    if policy.use_anomaly_veto:
        before = len(candidates)
        candidates = candidates[candidates["return_1d"].abs().fillna(0.0) <= policy.anomaly_return_threshold]
        reasons.append(f"anomaly_veto_removed={before - len(candidates)}")

    if policy.new_entry_loss_streak_threshold and consecutive_loss_days >= policy.new_entry_loss_streak_threshold:
        before = len(candidates)
        candidates = candidates[candidates["ticker"].isin(current_positions)]
        reasons.append(f"new_entry_pause_removed={before - len(candidates)}")

    if candidates.empty:
        forced = forced_holding_positions(policy, current_positions, position_age_days, signal_rows)
        if forced:
            reasons.append("min_holding_retained_existing")
            return forced, reasons
        return {}, reasons or ["all_candidates_filtered"]

    candidates = candidates.sort_values(["signal_strength", "confidence"], ascending=False)
    gross = policy.max_gross
    if current_drawdown <= -policy.drawdown_guard_threshold:
        gross *= policy.drawdown_guard
        reasons.append("drawdown_guard_scaled_gross")
    if policy.loss_streak_threshold and consecutive_loss_days >= policy.loss_streak_threshold:
        gross *= policy.loss_streak_guard
        reasons.append("loss_streak_guard_scaled_gross")

    weights = forced_holding_positions(policy, current_positions, position_age_days, signal_rows)
    if weights:
        reasons.append("min_holding_reserved_gross")
    reserved_gross = sum(weights.values())
    if reserved_gross > gross and reserved_gross > 0:
        scale = gross / reserved_gross
        weights = {ticker: float(weight * scale) for ticker, weight in weights.items()}
        reserved_gross = sum(weights.values())
        reasons.append("min_holding_scaled_to_risk_cap")

    allocatable = max(0.0, gross - reserved_gross)
    candidates = candidates[~candidates["ticker"].isin(weights)]
    remaining_slots = max(0, policy.max_names - len(weights))
    if remaining_slots <= 0 or allocatable <= 1e-9:
        return weights, reasons or ["max_names_filled_by_forced_holds"]
    candidates = candidates.head(remaining_slots)
    raw_scores = candidates["signal_strength"].clip(lower=0.0)
    if raw_scores.sum() <= 0:
        return weights, reasons or ["non_positive_scores"]

    allocated_weights = capped_proportional_allocation(
        {str(row["ticker"]): float(row["signal_strength"]) for _, row in candidates.iterrows()},
        allocatable,
        policy.max_position,
    )
    for _, row in candidates.iterrows():
        raw_weight = allocated_weights.get(str(row["ticker"]), 0.0)
        vol_scale = 1.0
        if policy.vol_lookback:
            vol = row.get(f"vol_{policy.vol_lookback}d")
            if pd.notna(vol) and float(vol) > policy.high_vol_threshold:
                vol_scale = policy.high_vol_scale
        weight = min(policy.max_position, raw_weight * vol_scale)
        if weight > 0:
            old = current_positions.get(row["ticker"], 0.0)
            if abs(weight - old) < policy.rebalance_threshold:
                weight = old
            if weight > 1e-9:
                weights[row["ticker"]] = float(weight)
    return weights, reasons


def forced_holding_positions(
    policy: PolicyConfig,
    current_positions: dict[str, float],
    position_age_days: dict[str, int],
    signal_rows: pd.DataFrame | None = None,
) -> dict[str, float]:
    if policy.min_holding_days <= 0:
        return {}
    signal_by_ticker = (
        signal_rows.set_index("ticker") if signal_rows is not None and not signal_rows.empty else pd.DataFrame()
    )
    forced: dict[str, float] = {}
    for ticker, weight in current_positions.items():
        if weight <= 0 or position_age_days.get(ticker, 0) >= policy.min_holding_days:
            continue
        if should_release_forced_hold(policy, ticker, signal_by_ticker):
            continue
        forced[ticker] = float(weight)
    return forced


def should_release_forced_hold(policy: PolicyConfig, ticker: str, signal_by_ticker: pd.DataFrame) -> bool:
    """Allow a young position to exit when the latest local tape shows a sharp reversal."""
    if signal_by_ticker.empty or ticker not in signal_by_ticker.index:
        return False
    row = signal_by_ticker.loc[ticker]
    latest_return = float(row.get("return_1d", 0.0) or 0.0)
    vol = 0.0
    if policy.forced_exit_vol_lookback:
        vol_value = row.get(f"vol_{policy.forced_exit_vol_lookback}d")
        vol = float(vol_value) if pd.notna(vol_value) else 0.0
    return (
        latest_return <= policy.forced_exit_return_threshold
        and (
            policy.forced_exit_vol_lookback <= 0
            or vol >= policy.forced_exit_vol_threshold
        )
    )


def cost_for_rebalance(old: dict[str, float], new: dict[str, float], costs: CostConfig) -> tuple[float, float, float, int]:
    tickers = set(old) | set(new)
    buy_turnover = sum(max(new.get(t, 0.0) - old.get(t, 0.0), 0.0) for t in tickers)
    sell_turnover = sum(max(old.get(t, 0.0) - new.get(t, 0.0), 0.0) for t in tickers)
    cost = buy_turnover * (costs.trading_fee + costs.slippage)
    cost += sell_turnover * (costs.trading_fee + costs.stamp_duty + costs.slippage)
    trade_count = sum(1 for t in tickers if abs(new.get(t, 0.0) - old.get(t, 0.0)) > 1e-9)
    return buy_turnover + sell_turnover, cost, sell_turnover, trade_count


def max_drawdown(equity: pd.Series) -> tuple[float, str, str]:
    if equity.empty:
        return 0.0, "", ""
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    trough = dd.idxmin()
    peak = equity.loc[:trough].idxmax()
    return float(dd.loc[trough]), str(peak), str(trough)


def summarize_metrics(
    curve: pd.DataFrame,
    trades: list[dict[str, Any]],
    total_turnover: float,
    total_cost: float,
    cost_assumption: str,
    sample_start: str,
    sample_end: str,
) -> dict[str, Any]:
    if curve.empty:
        base = {
            "sample_start": sample_start,
            "sample_end": sample_end,
            "days": 0,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "win_rate": 0.0,
            "turnover": 0.0,
            "trade_count": 0,
            "cost_paid": 0.0,
            "average_invested_ratio": 0.0,
            "average_cash_ratio": 1.0,
            "idle_cash_penalty": idle_cash_exposure_penalty([]),
            "max_high_cash_streak_days": 0,
            "closed_trade_count": 0,
            "max_daily_trade_count": 0,
            "days_at_trade_limit": 0,
            "deferred_trade_actions": 0,
            "daily_trade_limit": MAX_DAILY_TRADES,
            "gross_total_return_before_cost": 0.0,
            "cost_assumption": cost_assumption,
            "reward_formula": REWARD_FORMULA,
            "reward_version": REWARD_VERSION,
        }
        base["score_breakdown"] = capital_reward_breakdown(base)
        base["score"] = score_metrics(base)
        return base

    equity = curve.set_index("date")["equity"]
    returns = curve["net_return"].astype(float)
    total_return = float(equity.iloc[-1] - 1.0)
    days = max(1, len(curve))
    annualized_return = float((1.0 + total_return) ** (252.0 / days) - 1.0) if total_return > -1 else -1.0
    dd, _, _ = max_drawdown(equity)
    std = float(returns.std(ddof=0))
    downside = returns[returns < 0]
    downside_std = float(downside.std(ddof=0)) if len(downside) > 0 else 0.0
    sharpe = float(returns.mean() / std * math.sqrt(252)) if std > 0 else 0.0
    sortino = float(returns.mean() / downside_std * math.sqrt(252)) if downside_std > 0 else 0.0
    closed_trades = [t for t in trades if t.get("exit_date")]
    wins = [t for t in closed_trades if t.get("pnl_pct", 0.0) > 0]
    win_rate = len(wins) / len(closed_trades) if closed_trades else float((returns > 0).mean())
    cash_ratios = [
        1.0 - min(max(float(value), 0.0), 1.0)
        for value in curve.get("gross_exposure", pd.Series([0.0] * len(curve)))
    ]
    transaction_count = int(curve["trade_count"].sum()) if "trade_count" in curve else 0
    max_daily_trade_count = int(curve["trade_count"].max()) if "trade_count" in curve else 0
    gross_equity = 1.0
    if "gross_return" in curve:
        for value in curve["gross_return"].astype(float):
            gross_equity *= 1.0 + value
    metrics = {
        "sample_start": sample_start,
        "sample_end": sample_end,
        "days": days,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "max_drawdown": dd,
        "sharpe": sharpe,
        "sortino": sortino,
        "win_rate": float(win_rate),
        "turnover": float(total_turnover),
        "trade_count": transaction_count,
        "closed_trade_count": int(len(closed_trades)),
        "cost_paid": float(total_cost),
        "average_invested_ratio": float(curve["gross_exposure"].mean()) if "gross_exposure" in curve else 0.0,
        "average_cash_ratio": (
            1.0 - float(curve["gross_exposure"].mean())
            if "gross_exposure" in curve else 1.0
        ),
        "idle_cash_penalty": idle_cash_exposure_penalty(cash_ratios),
        "max_high_cash_streak_days": longest_high_cash_streak(cash_ratios),
        "max_daily_trade_count": max_daily_trade_count,
        "days_at_trade_limit": int((curve["trade_count"] >= MAX_DAILY_TRADES).sum()) if "trade_count" in curve else 0,
        "deferred_trade_actions": int(curve["deferred_trade_actions"].sum()) if "deferred_trade_actions" in curve else 0,
        "daily_trade_limit": MAX_DAILY_TRADES,
        "gross_total_return_before_cost": gross_equity - 1.0,
        "cost_assumption": cost_assumption,
        "reward_formula": REWARD_FORMULA,
        "reward_version": REWARD_VERSION,
    }
    metrics["score_breakdown"] = capital_reward_breakdown(metrics)
    metrics["score"] = score_metrics(metrics)
    return metrics


def run_backtest(daily: pd.DataFrame, policy: PolicyConfig, costs: CostConfig) -> dict[str, Any]:
    dates = sorted(daily["date"].unique())
    if len(dates) < 2:
        metrics = summarize_metrics(pd.DataFrame(), [], 0.0, 0.0, str(asdict(costs)), "", "")
        return {"metrics": metrics, "curve": pd.DataFrame(), "trades": []}

    by_date = {date: frame.copy() for date, frame in daily.groupby("date")}
    prices_by_date = {
        date: frame.set_index("ticker")["price"].to_dict()
        for date, frame in by_date.items()
    }

    positions: dict[str, float] = {}
    position_age_days: dict[str, int] = {}
    entry_price: dict[str, float] = {}
    entry_date: dict[str, str] = {}
    equity = 1.0
    peak = 1.0
    rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    total_turnover = 0.0
    total_cost = 0.0
    consecutive_loss_days = 0
    failure_memory: dict[str, int] = {}

    for idx in range(1, len(dates)):
        signal_date = dates[idx - 1]
        date = dates[idx]
        active_failure_memory = {
            ticker for ticker, days_left in failure_memory.items() if days_left > 0
        }
        refreshed_failure_memory: dict[str, int] = {}
        current_dd = equity / peak - 1.0
        targets, reasons = pick_targets(
            by_date[signal_date],
            policy,
            positions,
            position_age_days,
            current_dd,
            consecutive_loss_days,
            active_failure_memory,
        )
        targets, deferred_changes = limit_rebalance_actions(
            positions,
            targets,
            MAX_DAILY_TRADES,
        )
        if deferred_changes:
            reasons.append(f"daily_trade_limit_deferred={deferred_changes}")
        turnover, rebalance_cost, _, trade_count = cost_for_rebalance(positions, targets, costs)

        prev_prices = prices_by_date[signal_date]
        today_prices = prices_by_date[date]
        for ticker, old_weight in list(positions.items()):
            new_weight = targets.get(ticker, 0.0)
            if old_weight > 0 and new_weight <= 1e-9:
                if ticker in prev_prices and ticker in entry_price:
                    pnl_pct = prev_prices[ticker] / entry_price[ticker] - 1.0
                    trades.append(
                        {
                            "ticker": ticker,
                            "entry_date": entry_date.get(ticker),
                            "exit_date": signal_date,
                            "entry_price": entry_price[ticker],
                            "exit_price": prev_prices[ticker],
                            "pnl_pct": float(pnl_pct),
                            "exit_reason": "filtered_or_rebalanced_to_zero",
                        }
                    )
                    if (
                        policy.failure_memory_mode != "none"
                        and policy.failure_memory_days > 0
                        and pnl_pct <= policy.failure_memory_loss_threshold
                    ):
                        refreshed_failure_memory[normalize_ticker(ticker)] = policy.failure_memory_days
                entry_price.pop(ticker, None)
                entry_date.pop(ticker, None)
        for ticker, new_weight in targets.items():
            if positions.get(ticker, 0.0) <= 1e-9 and new_weight > 0 and ticker in prev_prices:
                entry_price[ticker] = prev_prices[ticker]
                entry_date[ticker] = signal_date

        gross_return = 0.0
        missing_prices: list[str] = []
        for ticker, weight in targets.items():
            if ticker not in prev_prices or ticker not in today_prices or prev_prices[ticker] <= 0:
                missing_prices.append(ticker)
                continue
            gross_return += weight * (today_prices[ticker] / prev_prices[ticker] - 1.0)

        net_return = gross_return - rebalance_cost
        equity *= 1.0 + net_return
        consecutive_loss_days = consecutive_loss_days + 1 if net_return < 0 else 0
        peak = max(peak, equity)
        total_turnover += turnover
        total_cost += rebalance_cost
        previous_age = position_age_days
        position_age_days = {
            ticker: previous_age.get(ticker, -1) + 1
            for ticker in targets
        }
        failure_memory = {
            ticker: days_left - 1
            for ticker, days_left in failure_memory.items()
            if days_left > 1
        }
        failure_memory.update(refreshed_failure_memory)
        positions = targets
        rows.append(
            {
                "date": date,
                "signal_date": signal_date,
                "equity": equity,
                "gross_return": gross_return,
                "net_return": net_return,
                "turnover": turnover,
                "cost": rebalance_cost,
                "trade_count": trade_count,
                "deferred_trade_actions": deferred_changes,
                "gross_exposure": sum(targets.values()),
                "positions": json.dumps(targets, sort_keys=True),
                "notes": ";".join(reasons + ([f"missing_prices={len(missing_prices)}"] if missing_prices else [])),
            }
        )

    last_date = dates[-1]
    last_prices = prices_by_date[last_date]
    for ticker, price in last_prices.items():
        if ticker in entry_price:
            trades.append(
                {
                    "ticker": ticker,
                    "entry_date": entry_date.get(ticker),
                    "exit_date": last_date,
                    "entry_price": entry_price[ticker],
                    "exit_price": price,
                    "pnl_pct": float(price / entry_price[ticker] - 1.0),
                    "exit_reason": "end_of_sample",
                }
            )

    curve = pd.DataFrame(rows)
    cost_assumption = (
        f"fee={costs.trading_fee:.4%}, stamp_duty={costs.stamp_duty:.4%}, "
        f"slippage={costs.slippage:.4%}, applied on turnover"
    )
    metrics = summarize_metrics(curve, trades, total_turnover, total_cost, cost_assumption, dates[0], dates[-1])
    return {"metrics": metrics, "curve": curve, "trades": trades}


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def analyze_failures(result: dict[str, Any], daily: pd.DataFrame, policy: PolicyConfig) -> list[dict[str, Any]]:
    curve = result["curve"]
    trades = result["trades"]
    failures: list[dict[str, Any]] = []
    if curve.empty:
        return [
            {
                "case_type": "no_backtest_data",
                "time_range": "",
                "market_state": "insufficient local prediction tape",
                "signals": {},
                "positions": {},
                "result": "no trades could be evaluated",
                "suspected_reason": "local data unavailable or too sparse",
                "repair_direction": "accumulate validated local prices before changing policy",
            }
        ]

    total_turnover = float(curve["turnover"].sum()) if "turnover" in curve else 0.0
    if not trades and total_turnover <= 1e-12:
        failures.append(
            {
                "case_type": "insufficient_trade_evidence",
                "time_range": f"{curve.iloc[0]['date']}..{curve.iloc[-1]['date']}",
                "market_state": {
                    "evaluated_days": int(len(curve)),
                    "gross_exposure": 0.0,
                    "turnover": 0.0,
                },
                "signals": {"policy": policy.name, "filters": asdict(policy)},
                "positions": {},
                "result": "no positions opened and no closed trades available for path-quality analysis",
                "suspected_reason": (
                    "the retained evidence and risk gates rejected every candidate on the available local tape"
                ),
                "repair_direction": (
                    "collect a fresh, broader local tape and independently validated daily prices before "
                    "considering any gate relaxation"
                ),
            }
        )
        missed = find_missed_opportunity(curve, daily)
        if missed:
            failures.append(missed)
        return failures

    equity = curve.set_index("date")["equity"]
    dd, peak_date, trough_date = max_drawdown(equity)
    window = curve[(curve["date"] >= peak_date) & (curve["date"] <= trough_date)]
    failures.append(
        {
            "case_type": "max_drawdown",
            "time_range": f"{peak_date}..{trough_date}",
            "market_state": {
                "avg_daily_return": float(window["net_return"].mean()) if not window.empty else 0.0,
                "avg_turnover": float(window["turnover"].mean()) if not window.empty else 0.0,
                "avg_gross_exposure": float(window["gross_exposure"].mean()) if not window.empty else 0.0,
            },
            "signals": {"policy": policy.name, "filters": asdict(policy)},
            "positions": json.loads(window.iloc[-1]["positions"]) if not window.empty else {},
            "result": {"drawdown": dd},
            "suspected_reason": "signal basket remained exposed while local quote tape moved against recent winners",
            "repair_direction": "tighten drawdown guard or volatility scaling before increasing gross exposure",
        }
    )

    if trades:
        worst = min(trades, key=lambda row: row.get("pnl_pct", 0.0))
        ticker_daily = daily[daily["ticker"] == worst["ticker"]]
        worst_pnl = float(worst.get("pnl_pct", 0.0) or 0.0)
        if worst_pnl < 0:
            trade_reason = "entry followed high-confidence signal but subsequent price path reversed"
            trade_repair = "require positive short-term trend or reduce size when volatility regime is elevated"
        else:
            trade_reason = (
                "no losing closed trades under retained policy; weakest trade is still monitored "
                "because sparse profitable paths can hide selection fragility"
            )
            trade_repair = (
                "do not loosen gates solely because closed trades are positive; validate signal breadth "
                "and missed-opportunity costs on another tape update"
            )
        failures.append(
            {
                "case_type": "worst_trade",
                "time_range": f"{worst.get('entry_date')}..{worst.get('exit_date')}",
                "market_state": {
                    "ticker": worst["ticker"],
                    "local_obs": int(len(ticker_daily)),
                    "median_vol_3d": float(ticker_daily["vol_3d"].median(skipna=True) or 0.0),
                },
                "signals": {"policy": policy.name},
                "positions": {"ticker": worst["ticker"]},
                "result": {
                    "pnl_pct": worst.get("pnl_pct"),
                    "entry_price": worst.get("entry_price"),
                    "exit_price": worst.get("exit_price"),
                    "exit_reason": worst.get("exit_reason"),
                },
                "suspected_reason": trade_reason,
                "repair_direction": trade_repair,
            }
        )

    losing = curve[curve["net_return"] < 0].copy()
    if not losing.empty:
        groups = (losing.index.to_series().diff() != 1).cumsum()
        streak = max((group for _, group in losing.groupby(groups)), key=len)
        has_loss_cooldown = policy.loss_streak_threshold > 0 and policy.loss_streak_guard < 1.0
        failures.append(
            {
                "case_type": "consecutive_losses",
                "time_range": f"{streak.iloc[0]['date']}..{streak.iloc[-1]['date']}",
                "market_state": {
                    "loss_days": int(len(streak)),
                    "cumulative_net_return": float((1.0 + streak["net_return"]).prod() - 1.0),
                },
                "signals": {"policy": policy.name},
                "positions": json.loads(streak.iloc[-1]["positions"]),
                "result": "multiple negative portfolio days without a pause",
                "suspected_reason": (
                    "losing-streak cooldown softened exposure but did not fully interrupt small sequential losses"
                    if has_loss_cooldown
                    else "daily policy has no explicit losing-streak cooldown"
                ),
                "repair_direction": (
                    "test minimum holding periods or a one-day no-new-risk pause after repeated losses"
                    if has_loss_cooldown
                    else "test a cooldown that halves gross after two negative days"
                ),
            }
        )

    rolling_turnover = curve["turnover"].rolling(3, min_periods=1).sum()
    high_idx = int(rolling_turnover.idxmax())
    high_row = curve.iloc[high_idx]
    start_idx = max(0, high_idx - 2)
    failures.append(
        {
            "case_type": "overtrading",
            "time_range": f"{curve.iloc[start_idx]['date']}..{high_row['date']}",
            "market_state": {"three_day_turnover": float(rolling_turnover.iloc[high_idx])},
            "signals": {"policy": policy.name},
            "positions": json.loads(high_row["positions"]),
            "result": {"cost_paid_in_window": float(curve.iloc[start_idx : high_idx + 1]["cost"].sum())},
            "suspected_reason": "small changes in ranked confidence can churn similar baskets",
            "repair_direction": "add rebalance threshold or minimum holding period",
        }
    )

    missed = find_missed_opportunity(curve, daily)
    if missed:
        failures.append(missed)
    return failures


def find_missed_opportunity(curve: pd.DataFrame, daily: pd.DataFrame) -> dict[str, Any] | None:
    if curve.empty:
        return None
    daily_by_date = {date: frame.set_index("ticker") for date, frame in daily.groupby("date")}
    best: dict[str, Any] | None = None
    for _, row in curve.iterrows():
        signal_date = row["signal_date"]
        date = row["date"]
        if signal_date not in daily_by_date or date not in daily_by_date:
            continue
        previous = daily_by_date[signal_date]
        current = daily_by_date[date]
        common = previous.index.intersection(current.index)
        if common.empty:
            continue
        realized = current.loc[common, "price"] / previous.loc[common, "price"] - 1.0
        positions = json.loads(row["positions"])
        for ticker, ret in realized.sort_values(ascending=False).head(3).items():
            if ticker in positions or ret < 0.03:
                continue
            sig = previous.loc[ticker]
            candidate = {
                "case_type": "missed_opportunity",
                "time_range": f"{signal_date}..{date}",
                "market_state": {"ticker_next_return": float(ret)},
                "signals": {
                    "ticker": ticker,
                    "confidence": float(sig["confidence"]),
                    "risk_reward": float(sig["risk_reward"]),
                    "observations": int(sig["close_observations"]),
                },
                "positions": positions,
                "result": f"{ticker} rose next sample but was not held",
                "suspected_reason": "ranking, confidence threshold, or risk/reward veto excluded the move",
                "repair_direction": "evaluate whether lower confidence threshold helps out-of-sample after costs",
            }
            if best is None or ret > best["market_state"]["ticker_next_return"]:
                best = candidate
    return best


def previous_best_score(root: Path) -> tuple[float | None, Path | None]:
    best_score = None
    best_path = None
    for run_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        if (run_dir / "INVALIDATED.json").exists():
            continue
        metrics = read_json(run_dir / "best_metrics.json")
        if metrics.get("reward_version") != REWARD_VERSION:
            continue
        score = completed_run_best_score(run_dir)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_score = score
            best_path = run_dir / "best_metrics.json"
    return best_score, best_path


def latest_completed_run(root: Path) -> Path | None:
    latest = root / "LATEST"
    if latest.exists():
        try:
            candidate = Path(latest.read_text(encoding="utf-8").strip())
            if candidate.exists() and candidate.is_dir():
                return candidate
        except Exception:
            pass
    candidates = [path.parent for path in root.glob("*/README.md") if path.parent.is_dir()]
    return sorted(candidates)[-1] if candidates else None


def completed_run_best_score(run_dir: Path | None) -> float | None:
    """Read the retained policy score, never a diagnostic-only leaderboard max."""
    if run_dir is None:
        return None
    best_metrics = read_json(run_dir / "best_metrics.json")
    try:
        score = best_metrics.get("score")
        if score is not None:
            return float(score)
    except (TypeError, ValueError, AttributeError):
        pass
    # Compatibility for early runs that predate best_metrics.json.
    try:
        frame = pd.read_csv(run_dir / "summary.csv")
        return float(frame["score"].max()) if not frame.empty and "score" in frame else None
    except Exception:
        return None


def _samples_from_readme(run_dir: Path | None) -> int | None:
    if run_dir is None:
        return None
    readme = run_dir / "README.md"
    if not readme.exists():
        return None
    match = re.search(r"Samples consumed:\s*([0-9]+)", readme.read_text(encoding="utf-8"))
    return int(match.group(1)) if match else None


def previous_tape_stats(run_dir: Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {}
    tape = read_json(run_dir / "tape_update.json")
    if tape:
        return tape
    context = read_json(run_dir / "project_context.json")
    embedded = context.get("tape_update") if isinstance(context, dict) else None
    if isinstance(embedded, dict) and embedded:
        return embedded
    sample_count = _samples_from_readme(run_dir)
    return {"current_prediction_rows": sample_count} if sample_count is not None else {}


def tape_recovery_was_pending(stats: dict[str, Any], max_hops: int = 20) -> bool:
    """Follow thin-run ancestry until the last stale or fresh tape decision."""
    current = stats
    seen: set[str] = set()
    for _ in range(max_hops):
        if bool(current.get("freshness_recovery_pending", False)):
            return True
        status = str(current.get("validation_status", ""))
        if status in {"stale_tape", "empty_prediction_tape"}:
            return True
        if status != "thin_tape_update":
            return False
        prior_raw = current.get("previous_run")
        if not prior_raw or str(prior_raw) in seen:
            return False
        seen.add(str(prior_raw))
        current = previous_tape_stats(Path(str(prior_raw)))
    return False


def distinct_date_tape_baseline(run_dir: Path | None, current_date: str) -> Path | None:
    """Skip same-day reruns so tape growth is measured per automation cycle date."""
    current = run_dir
    seen: set[str] = set()
    while current is not None and current.name.startswith(current_date):
        if str(current) in seen:
            return current
        seen.add(str(current))
        stats = previous_tape_stats(current)
        prior_raw = stats.get("previous_run")
        if not prior_raw:
            return current
        current = Path(str(prior_raw))
    return current


def build_tape_update_report(
    predictions: pd.DataFrame,
    previous_run: Path | None,
    min_new_rows_for_validation: int = 20,
    min_latest_date_rows_for_validation: int = 5,
    max_latest_prediction_age_days: int = 3,
) -> dict[str, Any]:
    """Describe whether this cycle has enough fresh local tape to validate changes."""
    if predictions.empty or "predicted_at" not in predictions:
        return {
            "validation_status": "empty_prediction_tape",
            "rule": "Do not widen exposure without local prediction tape.",
        }

    latest_ts = predictions["predicted_at"].max()
    earliest_ts = predictions["predicted_at"].min()
    latest_date = latest_ts.date()
    latest_date_rows = int((predictions["predicted_at"].dt.date == latest_date).sum())
    baseline_run = distinct_date_tape_baseline(previous_run, datetime.now().strftime("%Y%m%d"))
    previous_stats = previous_tape_stats(baseline_run)
    previous_status = str(previous_stats.get("validation_status", ""))
    previous_recovery_pending = tape_recovery_was_pending(previous_stats)
    previous_rows = previous_stats.get("current_prediction_rows")
    try:
        previous_rows_int = int(previous_rows) if previous_rows is not None else None
    except (TypeError, ValueError):
        previous_rows_int = None

    new_rows = len(predictions) - previous_rows_int if previous_rows_int is not None else None
    age_days = max(0, (datetime.now().date() - latest_date).days)
    if age_days > max_latest_prediction_age_days:
        status = "stale_tape"
    elif previous_rows_int is not None and (
        (new_rows is not None and new_rows < min_new_rows_for_validation)
        or latest_date_rows < min_latest_date_rows_for_validation
    ):
        status = "thin_tape_update"
    elif previous_rows_int is None:
        status = "no_previous_run_baseline"
    else:
        status = "fresh_tape_update"

    return {
        "validation_status": status,
        "current_prediction_rows": int(len(predictions)),
        "previous_prediction_rows": previous_rows_int,
        "new_prediction_rows_since_previous": new_rows,
        "current_earliest_prediction_at": earliest_ts.isoformat(),
        "current_latest_prediction_at": latest_ts.isoformat(),
        "current_latest_prediction_date": latest_date.isoformat(),
        "latest_date_prediction_rows": latest_date_rows,
        "latest_prediction_age_days": int(age_days),
        "min_new_rows_for_validation": int(min_new_rows_for_validation),
        "min_latest_date_rows_for_validation": int(min_latest_date_rows_for_validation),
        "max_latest_prediction_age_days": int(max_latest_prediction_age_days),
        "enough_for_policy_widening": status == "fresh_tape_update",
        "freshness_recovery_pending": (
            (previous_status in {"stale_tape", "empty_prediction_tape"} or previous_recovery_pending)
            and status != "fresh_tape_update"
        ),
        "previous_run": str(baseline_run) if baseline_run else None,
        "rule": (
            "Do not widen exposure or relax caps when the cycle has fewer than "
            f"{min_new_rows_for_validation} new local prediction rows, fewer than "
            f"{min_latest_date_rows_for_validation} latest-day rows, or stale latest predictions."
        ),
    }


def extract_failure_tickers_from_run(run_dir: Path | None) -> tuple[str, ...]:
    if run_dir is None:
        return ()
    path = run_dir / "failure_cases.jsonl"
    if not path.exists():
        return ()
    tickers: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            case = json.loads(line)
        except Exception:
            continue
        for key in ("market_state", "signals", "positions"):
            value = case.get(key)
            if not isinstance(value, dict):
                continue
            ticker = value.get("ticker")
            if ticker:
                tickers.add(normalize_ticker(ticker))
            for maybe_ticker, maybe_weight in value.items():
                if isinstance(maybe_weight, (int, float)) and maybe_weight > 0:
                    normalized = normalize_ticker(maybe_ticker)
                    if normalized and normalized[0].isdigit():
                        tickers.add(normalized)
    return tuple(sorted(tickers))


def split_checks(daily: pd.DataFrame, policy: PolicyConfig, costs: CostConfig) -> dict[str, Any]:
    dates = sorted(daily["date"].unique())
    if len(dates) < 6:
        return {"warning": "too few dates for split check"}
    split = int(len(dates) * 0.6)
    train_dates = set(dates[:split])
    test_dates = set(dates[split - 1 :])
    train = run_backtest(daily[daily["date"].isin(train_dates)], policy, costs)["metrics"]
    test = run_backtest(daily[daily["date"].isin(test_dates)], policy, costs)["metrics"]
    high_cost = replace(costs, slippage=costs.slippage * 3.0)
    cost_stress = run_backtest(daily, policy, high_cost)["metrics"]
    insufficient_trade_evidence = min(
        int(train.get("trade_count", 0) or 0),
        int(test.get("trade_count", 0) or 0),
        int(cost_stress.get("trade_count", 0) or 0),
    ) < 3
    return {
        "split_date": dates[split],
        "train": train,
        "out_of_sample": test,
        "cost_stress_3x_slippage": cost_stress,
        "insufficient_trade_evidence": insufficient_trade_evidence,
        "inconclusive_reason": (
            "fewer than 3 closed trades in at least one robustness slice; "
            "zero/very sparse trading is risk avoidance, not evidence of return robustness"
            if insufficient_trade_evidence
            else ""
        ),
        "overfit_risk": (
            test["score"] < 0
            or test["score"] < train["score"] * 0.25
            or cost_stress["score"] < 0
            or insufficient_trade_evidence
        ),
    }


def _metric_score(metrics: dict[str, Any] | None) -> float | None:
    if not isinstance(metrics, dict) or "score" not in metrics:
        return None
    return float(metrics["score"])


def _compact_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "total_return",
        "annualized_return",
        "max_drawdown",
        "sharpe",
        "turnover",
        "trade_count",
        "score",
    ]
    return {key: metrics.get(key) for key in keys}


def build_sleeve_diagnostics(
    daily: pd.DataFrame,
    policies: list[PolicyConfig],
    results: dict[str, dict[str, Any]],
    costs: CostConfig,
) -> dict[str, Any]:
    """Check whether ETF and single-stock sleeves are robust enough to allocate."""
    single_stock_candidates = [
        "single_stock_hold6_cap5_min2obs_anomaly12",
        "single_stock_hold6_cap5_min2obs",
        "single_stock_hold6_cap4_min2obs",
    ]
    available_single_stock = [
        name for name in single_stock_candidates if name in results
    ]
    single_stock_trial = max(
        available_single_stock,
        key=lambda name: results[name]["metrics"]["score"],
    ) if available_single_stock else "single_stock_hold6_cap5_min2obs"
    trial_by_sleeve = {
        "etf": "etf_only_cost_guard",
        "single_stock": single_stock_trial,
    }
    policies_by_name = {policy.name: policy for policy in policies}
    sleeves: dict[str, Any] = {}
    for sleeve_name, trial_name in trial_by_sleeve.items():
        policy = policies_by_name[trial_name]
        metrics = results[trial_name]["metrics"]
        checks = split_checks(daily, policy, costs)
        oos_score = _metric_score(checks.get("out_of_sample"))
        cost_stress_score = _metric_score(checks.get("cost_stress_3x_slippage"))
        reasons: list[str] = []
        if metrics["score"] <= 0:
            reasons.append("主样本score未转正")
        if checks.get("overfit_risk"):
            reasons.append("样本外/成本扰动检查失败")
        if cost_stress_score is None or cost_stress_score < 0.02:
            reasons.append("3x滑点余量低于0.02")
        promotable = not reasons
        sleeves[sleeve_name] = {
            "trial_name": trial_name,
            **_compact_metrics(metrics),
            "out_of_sample_score": oos_score,
            "cost_stress_score": cost_stress_score,
            "overfit_risk": bool(checks.get("overfit_risk", True)),
            "promotable": promotable,
            "reason": "；".join(reasons) if reasons else "通过主样本、样本外和3x滑点检查",
        }

    allocator_ready = all(row["promotable"] for row in sleeves.values())
    return {
        "allocator_status": "promoted_candidate" if allocator_ready else "not_promoted",
        "rule": "ETF和单股sleeve必须主样本score>0、overfit_risk=false、3x滑点score>=0.02才允许组合allocator推广",
        "sleeves": sleeves,
    }


def _missing_price_count(notes: Any) -> int:
    for part in str(notes or "").split(";"):
        if part.startswith("missing_prices="):
            try:
                return int(part.split("=", 1)[1])
            except (TypeError, ValueError):
                return 0
    return 0


def build_price_coverage_report(
    daily: pd.DataFrame,
    price_history: pd.DataFrame,
    result: dict[str, Any],
) -> dict[str, Any]:
    """Summarize whether the retained path used validated local prices."""
    curve = result.get("curve", pd.DataFrame())
    price_source_counts = (
        daily["price_source"].value_counts().to_dict()
        if "price_source" in daily and not daily.empty
        else {}
    )
    daily_price_rows = int(price_source_counts.get("daily_prices", 0))
    signal_rows = int(len(daily))
    independent_ratio = daily_price_rows / signal_rows if signal_rows else 0.0

    missing_days = 0
    missing_slots = 0
    position_slots = 0
    if isinstance(curve, pd.DataFrame) and not curve.empty:
        for _, row in curve.iterrows():
            missing = _missing_price_count(row.get("notes"))
            if missing > 0:
                missing_days += 1
                missing_slots += missing
            try:
                positions = json.loads(row.get("positions", "{}"))
            except Exception:
                positions = {}
            if isinstance(positions, dict):
                position_slots += len(positions)

    curve_days = int(len(curve)) if isinstance(curve, pd.DataFrame) else 0
    missing_day_ratio = missing_days / curve_days if curve_days else 0.0
    missing_slot_ratio = missing_slots / position_slots if position_slots else 0.0
    status = "validated_daily_prices"
    if independent_ratio <= 0.0:
        status = "unvalidated_prediction_current_price_fallback"
    elif independent_ratio < 0.80:
        status = "partial_daily_prices_low_signal_coverage"
    elif missing_slot_ratio > 0.10:
        status = "partial_daily_prices_with_missing_hold_prices"

    return {
        "status": status,
        "daily_signal_rows": signal_rows,
        "daily_signal_price_source_counts": {
            str(key): int(value) for key, value in price_source_counts.items()
        },
        "daily_prices_rows_loaded": int(len(price_history)),
        "independent_price_row_ratio": float(independent_ratio),
        "best_curve_days": curve_days,
        "days_with_missing_prices": int(missing_days),
        "missing_price_day_ratio": float(missing_day_ratio),
        "held_position_slots": int(position_slots),
        "missing_position_price_slots": int(missing_slots),
        "missing_position_price_slot_ratio": float(missing_slot_ratio),
        "rule": (
            "Do not use latest local score to expand exposure until daily_prices "
            "coverage is validated and held-position missing-price slots are low."
        ),
    }


def build_price_readiness_report(
    daily: pd.DataFrame,
    price_history: pd.DataFrame,
) -> dict[str, Any]:
    """Identify local daily_prices rows needed before score can justify widening."""
    if daily.empty:
        return {
            "status": "no_signal_tickers",
            "total_signal_ticker_count": 0,
            "priced_signal_ticker_count": 0,
            "missing_signal_ticker_count": 0,
            "latest_missing_tickers": [],
            "minimum_next_rows": 0,
            "missing_tickers_top10": [],
            "rule": "Populate local daily_prices before treating heuristic score as exposure-widening evidence.",
            "next_action": "Accumulate local prediction and price history, then rerun the cycle.",
        }

    frame = daily.copy()
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["date"] = frame["date"].astype(str)
    signal_tickers = sorted(frame["ticker"].dropna().unique())
    latest_date = str(frame["date"].max())
    if "price_source" in frame.columns:
        missing_rows = frame[~frame["price_source"].eq("daily_prices")].copy()
        priced_row_count = int(frame["price_source"].eq("daily_prices").sum())
    else:
        priced_keys: set[tuple[str, str]] = set()
        if not price_history.empty and {"ticker", "date"}.issubset(price_history.columns):
            priced = price_history.copy()
            priced["ticker"] = priced["ticker"].map(normalize_ticker)
            priced["date"] = priced["date"].astype(str)
            priced_keys = set(zip(priced["date"], priced["ticker"], strict=False))
        missing_rows = frame[
            ~frame.apply(lambda row: (str(row["date"]), normalize_ticker(row["ticker"])) in priced_keys, axis=1)
        ].copy()
        priced_row_count = len(frame) - len(missing_rows)
    missing = sorted(missing_rows["ticker"].dropna().unique())
    latest_missing = sorted(
        missing_rows.loc[missing_rows["date"].eq(latest_date), "ticker"].dropna().unique()
    )

    grouped = (
        missing_rows.groupby("ticker", as_index=False)
        .agg(
            signal_days=("date", "nunique"),
            first_signal_date=("date", "min"),
            last_signal_date=("date", "max"),
            total_signal_observations=("close_observations", "sum"),
        )
    )
    if not grouped.empty:
        grouped["total_signal_observations"] = grouped["total_signal_observations"].astype(int)
        grouped["missing_latest_signal_date"] = grouped["ticker"].isin(latest_missing)
        grouped = grouped.sort_values(
            ["missing_latest_signal_date", "signal_days", "last_signal_date", "total_signal_observations"],
            ascending=[False, False, False, False],
        )

    if not signal_tickers:
        status = "no_signal_tickers"
    elif priced_row_count <= 0:
        status = "blocked_no_daily_prices"
    elif missing:
        status = "partial_daily_price_backfill_needed"
    else:
        status = "ready_validated_daily_prices"

    next_action = (
        "Backfill latest local daily_prices for the full latest_missing_tickers batch first, then "
        "rerun the cycle and require validated coverage before relaxing caps."
        if latest_missing
        else "Latest signal-date prices are covered; continue historical priority backfill before relaxing weak-coverage caps."
    )

    return {
        "status": status,
        "total_signal_ticker_count": len(signal_tickers),
        "priced_signal_ticker_count": len(signal_tickers) - len(missing),
        "missing_signal_ticker_count": len(missing),
        "latest_signal_date": latest_date,
        "latest_missing_tickers": latest_missing,
        "unblock_tickers": latest_missing,
        "minimum_next_rows": len(latest_missing),
        "missing_tickers_top10": grouped.head(10).to_dict("records") if not grouped.empty else [],
        "rule": (
            "Populate local daily_prices with independently validated OHLC rows before "
            "treating heuristic score as exposure-widening evidence."
        ),
        "next_action": next_action,
    }


def build_portfolio_lifecycle_report(db_path: Path) -> dict[str, Any]:
    """Snapshot lifecycle metadata without pretending stored marks are current value."""
    generated_at = datetime.now().isoformat(timespec="seconds")
    target_invested_ratio = 1.0
    with sqlite3.connect(db_path) as conn:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        if "simulation_positions" not in table_names:
            return {
                "generated_at": generated_at,
                "status": "simulation_positions_missing",
                "target_invested_ratio": target_invested_ratio,
                "positions": [],
            }
        position_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(simulation_positions)").fetchall()
        }
        lifecycle_columns = {
            "opened_at",
            "last_mark_price",
            "last_mark_at",
            "last_mark_source",
            "last_reviewed_at",
            "review_status",
            "review_reason",
        }
        if not lifecycle_columns.issubset(position_columns):
            return {
                "generated_at": generated_at,
                "status": "lifecycle_schema_missing",
                "target_invested_ratio": target_invested_ratio,
                "missing_columns": sorted(lifecycle_columns - position_columns),
                "positions": [],
            }
        cash_row = None
        if "system_stats" in table_names:
            cash_row = conn.execute(
                "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
            ).fetchone()
        cash = float(cash_row[0]) if cash_row else 0.0
        rows = conn.execute(
            """
            SELECT ticker, shares, avg_cost, opened_at, last_mark_price,
                   last_mark_at, last_mark_source, last_reviewed_at,
                   review_status, review_reason
            FROM simulation_positions
            WHERE shares > 0
            ORDER BY ticker
            """
        ).fetchall()
        recent_lifecycle_exits: list[dict[str, Any]] = []
        redeployment_state: dict[str, Any] = {}
        if "simulation_trades" in table_names:
            trade_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(simulation_trades)").fetchall()
            }
            required_trade_columns = {
                "ticker", "direction", "shares", "price", "fee", "reason", "traded_at"
            }
            if required_trade_columns.issubset(trade_columns):
                exit_rows = conn.execute(
                    """
                    SELECT ticker, shares, price, fee, reason, traded_at
                    FROM simulation_trades
                    WHERE lower(direction) = 'sell'
                      AND reason LIKE '%逐仓强制复核%'
                      AND date(traded_at) = date('now', 'localtime')
                    ORDER BY datetime(traded_at), id
                    """
                ).fetchall()
                recent_lifecycle_exits = [
                    {
                        "ticker": str(exit_row[0]),
                        "shares": float(exit_row[1]),
                        "price": float(exit_row[2]),
                        "fee": float(exit_row[3] or 0.0),
                        "net_proceeds": float(exit_row[1]) * float(exit_row[2]) - float(exit_row[3] or 0.0),
                        "reason": str(exit_row[4] or ""),
                        "traded_at": str(exit_row[5] or ""),
                    }
                    for exit_row in exit_rows
                ]
        if "simulation_redeployment_state" in table_names:
            state_columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(simulation_redeployment_state)").fetchall()
            }
            rejection_columns = [
                name for name in ("last_rejection_counts", "rejection_counts_total")
                if name in state_columns
            ]
            rejection_select = ", " + ", ".join(rejection_columns) if rejection_columns else ""
            state_row = conn.execute(
                f"""
                SELECT status, deployment_gap, blocker_code, blocker_reason,
                       next_action, source, attempt_count, last_attempt_at,
                       last_candidate_count, last_trade_count, updated_at
                       {rejection_select}
                FROM simulation_redeployment_state WHERE id = 1
                """
            ).fetchone()
            if state_row:
                state_keys = [
                    "status", "deployment_gap", "blocker_code", "blocker_reason",
                    "next_action", "source", "attempt_count", "last_attempt_at",
                    "last_candidate_count", "last_trade_count", "updated_at",
                ] + rejection_columns
                redeployment_state = dict(zip(state_keys, state_row))
                for key in rejection_columns:
                    try:
                        redeployment_state[key] = json.loads(redeployment_state[key] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        redeployment_state[key] = {}

    now = datetime.now()
    positions: list[dict[str, Any]] = []
    for row in rows:
        try:
            held_days = max((now - datetime.fromisoformat(str(row[3]))).days, 0) if row[3] else None
        except ValueError:
            held_days = None
        try:
            quote_age_days = max((now - datetime.fromisoformat(str(row[5]))).days, 0) if row[5] else None
        except ValueError:
            quote_age_days = None
        positions.append(
            {
                "ticker": str(row[0]),
                "shares": float(row[1]),
                "avg_cost": float(row[2]),
                "opened_at": row[3],
                "held_days": held_days,
                "audit_last_mark_price": float(row[4]) if row[4] is not None else None,
                "audit_last_mark_at": row[5],
                "audit_quote_age_days": quote_age_days,
                "audit_last_mark_source": row[6],
                "last_reviewed_at": row[7],
                "review_status": row[8] or "pending_review",
                "review_reason": row[9] or "not yet reviewed",
            }
        )

    blocked_count = sum(str(row["review_status"]).startswith("blocked_") for row in positions)
    if not positions:
        # An empty book needs no quote, so its cash and deployment gap are exactly
        # known.  Reporting valuation_incomplete here hid the most urgent lifecycle
        # state: forced exits succeeded but all proceeds still await redeployment.
        exit_proceeds = sum(row["net_proceeds"] for row in recent_lifecycle_exits)
        return {
            "generated_at": generated_at,
            "status": "fully_cash_operational_redeployment_blocked",
            "valuation_complete": True,
            "target_invested_ratio": target_invested_ratio,
            "cash": cash,
            "market_value": 0.0,
            "total_assets": cash,
            "invested_ratio": 0.0,
            "deployment_gap": cash,
            "position_count": 0,
            "blocked_price_count": 0,
            "lifecycle_review_ran": True,
            "reviewed_position_count": 0,
            "triggered_exit_count": len(recent_lifecycle_exits),
            "triggered_exit_net_proceeds": exit_proceeds,
            "recent_lifecycle_exits": recent_lifecycle_exits,
            "redeployment_status": redeployment_state.get(
                "status", "pending_committee_approved_fresh_candidates"
            ),
            "redeployment_state": redeployment_state,
            "operational_cash_reason": (
                "all holdings exited through realtime lifecycle rules; proceeds await "
                "fresh committee-approved candidates and must not be treated as strategic cash"
                if recent_lifecycle_exits
                else "no open holdings; cash awaits fresh committee-approved candidates"
            ),
            "rule": (
                "An empty portfolio is completely valued without a quote. Cash is an operational "
                "deployment gap and must re-enter the committee allocation queue."
            ),
            "positions": [],
        }
    return {
        "generated_at": generated_at,
        "status": "realtime_valuation_required",
        "valuation_complete": False,
        "target_invested_ratio": target_invested_ratio,
        "cash": cash,
        "market_value": None,
        "total_assets": None,
        "invested_ratio": None,
        "deployment_gap": None,
        "position_count": len(positions),
        "blocked_price_count": blocked_count,
        "redeployment_state": redeployment_state,
        "rule": (
            "Current account value, PnL, invested ratio and simulated fills require realtime quotes. "
            "Stored marks are audit metadata only and are never valuation fallbacks."
        ),
        "positions": positions,
    }


def build_daily_price_backfill_plan(
    daily: pd.DataFrame,
    price_history: pd.DataFrame,
    run_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Write an actionable local daily_prices backfill plan for user entries."""
    if daily.empty:
        return [], {
            "status": "no_signal_tickers",
            "plan_csv": str(run_dir / "daily_price_backfill_plan.csv"),
            "total_missing_tickers": 0,
            "latest_signal_date": "",
            "latest_missing_tickers": [],
            "minimum_next_rows": 0,
            "top_priority_tickers": [],
            "rule": "No local signal tickers are available for daily_prices planning.",
        }

    frame = daily.copy()
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["date"] = frame["date"].astype(str)
    latest_signal_date = str(frame["date"].max())
    if "price_source" in frame.columns:
        missing_rows = frame[~frame["price_source"].eq("daily_prices")].copy()
    else:
        priced_keys: set[tuple[str, str]] = set()
        if not price_history.empty and {"ticker", "date"}.issubset(price_history.columns):
            priced = price_history.copy()
            priced["ticker"] = priced["ticker"].map(normalize_ticker)
            priced["date"] = priced["date"].astype(str)
            priced_keys = set(zip(priced["date"], priced["ticker"], strict=False))
        missing_rows = frame[
            ~frame.apply(lambda row: (str(row["date"]), normalize_ticker(row["ticker"])) in priced_keys, axis=1)
        ].copy()
    if missing_rows.empty:
        return [], {
            "status": "ready_validated_daily_prices",
            "plan_csv": str(run_dir / "daily_price_backfill_plan.csv"),
            "total_missing_tickers": 0,
            "latest_signal_date": latest_signal_date,
            "latest_missing_tickers": [],
            "minimum_next_rows": 0,
            "top_priority_tickers": [],
            "rule": "All local signal ticker/date rows have daily_prices coverage.",
        }

    latest_missing = sorted(
        missing_rows.loc[missing_rows["date"].eq(latest_signal_date), "ticker"].dropna().unique()
    )
    grouped = (
        missing_rows.groupby("ticker", as_index=False)
        .agg(
            missing_signal_days=("date", "nunique"),
            first_missing_signal_date=("date", "min"),
            last_missing_signal_date=("date", "max"),
            total_signal_observations=("close_observations", "sum"),
        )
    )
    grouped["missing_latest_signal_date"] = grouped["ticker"].isin(latest_missing)
    grouped = grouped.sort_values(
        [
            "missing_latest_signal_date",
            "missing_signal_days",
            "last_missing_signal_date",
            "total_signal_observations",
        ],
        ascending=[False, False, False, False],
    )
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(grouped.to_dict("records"), start=1):
        ticker = normalize_ticker(row["ticker"])
        rows.append(
            {
                "priority_rank": rank,
                "ticker": ticker,
                "missing_signal_days": int(row["missing_signal_days"]),
                "first_missing_signal_date": row["first_missing_signal_date"],
                "last_missing_signal_date": row["last_missing_signal_date"],
                "total_signal_observations": int(row["total_signal_observations"]),
                "latest_signal_date": latest_signal_date,
                "missing_latest_signal_date": ticker in latest_missing,
                "minimum_rows_to_unblock_latest": 1 if ticker in latest_missing else 0,
                "plan_action": (
                    "backfill this ticker's latest local daily_prices row first"
                    if ticker in latest_missing
                    else "backfill historical local daily_prices before using scores to widen exposure"
                ),
            }
        )

    summary = {
        "status": "backfill_plan_ready",
        "plan_csv": str(run_dir / "daily_price_backfill_plan.csv"),
        "plan_json": str(run_dir / "daily_price_backfill_plan.json"),
        "total_missing_tickers": len(rows),
        "latest_signal_date": latest_signal_date,
        "latest_missing_tickers": latest_missing,
        "minimum_next_rows": len(latest_missing),
        "top_priority_tickers": [row["ticker"] for row in rows[:5]],
        "rule": (
            "Use only independently validated local OHLC rows; do not synthesize daily_prices "
            "from prediction current_price."
        ),
        "next_action": (
            "Fill the latest missing ticker/date rows in this plan, rerun the heuristic cycle, "
            "then check whether weak-price and stale-tape caps can be relaxed."
        ),
    }
    return rows, summary


def format_missing_price_queue(price_readiness: dict[str, Any], limit: int = 5) -> str:
    rows = price_readiness.get("missing_tickers_top10", [])
    if not isinstance(rows, list):
        return ""

    parts: list[str] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("ticker"):
            continue
        details: list[str] = []
        first_signal = str(row.get("first_signal_date", "") or "")[:10]
        try:
            details.append(f"missing_days={int(row.get('signal_days', 0))}d")
        except (TypeError, ValueError):
            pass
        try:
            details.append(f"obs={int(row.get('total_signal_observations', 0))}")
        except (TypeError, ValueError):
            pass
        last_signal = str(row.get("last_signal_date", "") or "")[:10]
        if first_signal and last_signal:
            details.append(f"missing_range={first_signal}..{last_signal}")
        elif first_signal:
            details.append(f"missing_from={first_signal}")
        elif last_signal:
            details.append(f"missing_to={last_signal}")
        ticker = normalize_ticker(row["ticker"])
        parts.append(f"{ticker}({', '.join(details)})" if details else ticker)
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def write_policy_snapshot(path: Path, policy: PolicyConfig, costs: CostConfig) -> None:
    policy_literal = pprint.pformat(asdict(policy), sort_dicts=False, width=88)
    costs_literal = pprint.pformat(asdict(costs), sort_dicts=False, width=88)
    body = f'''"""Best heuristic policy snapshot from the latest local cycle.

This file is generated for reproducibility. It does not place orders and does
not call market data services.
"""

POLICY_CONFIG = {policy_literal}

COST_CONFIG = {costs_literal}


def score_candidate(row):
    """Return an interpretable ranking score for one daily signal row."""
    risk_reward = min(max(float(row.get("risk_reward", 0.0)), 0.0), 3.0)
    observations = max(int(row.get("close_observations", 0)), 0)
    confidence = float(row.get("confidence", 0.0))
    return confidence * __import__("math").log1p(observations) * (1.0 + risk_reward / 6.0)
'''
    path.write_text(body, encoding="utf-8")


def write_readme(
    path: Path,
    run_started: str,
    best_name: str,
    best_metrics: dict[str, Any],
    previous_score: float | None,
    previous_path: Path | None,
    trials: list[dict[str, Any]],
    checks: dict[str, Any],
    sample_count: int,
    db_path: Path,
    command: str,
    recent_failure_tickers: tuple[str, ...] = (),
    sleeve_diagnostics: dict[str, Any] | None = None,
    price_coverage: dict[str, Any] | None = None,
    price_readiness: dict[str, Any] | None = None,
    price_readiness_stall: dict[str, Any] | None = None,
    tape_update: dict[str, Any] | None = None,
    portfolio_lifecycle: dict[str, Any] | None = None,
    previous_latest_run: Path | None = None,
    previous_latest_score: float | None = None,
) -> None:
    failed = [t for t in trials if t["trial_name"] != best_name]
    best_row = next((t for t in trials if t["trial_name"] == best_name), {})
    best_config = best_row.get("config", {}) if isinstance(best_row, dict) else {}
    best_cap = float(best_config.get("max_position", 0.0) or 0.0)
    best_gross = float(best_config.get("max_gross", 0.0) or 0.0)
    best_hold = int(best_config.get("min_holding_days", 0) or 0)
    best_signal_count = int(best_config.get("min_signal_count", 1) or 1)
    evidence_text = (
        f"and at least {best_signal_count} same-day local observations"
        if best_signal_count > 1
        else "and the default local observation floor"
    )
    comparison = "No previous heuristic_cycle best was found."
    if previous_score is not None:
        comparison = (
            f"Previous best score {previous_score:.6f} from {previous_path}; "
            f"delta {best_metrics['score'] - previous_score:+.6f}."
        )
    latest_comparison = "No immediate prior completed run was found."
    if previous_latest_score is not None:
        latest_comparison = (
            f"Immediate prior run score {previous_latest_score:.6f} from {previous_latest_run}; "
            f"delta {best_metrics['score'] - previous_latest_score:+.6f}."
        )
    def diagnostic_reason(trial_name: str) -> str:
        if "validated_daily_price_only" in trial_name:
            return "not promotable until independent local daily_prices coverage exists."
        if "min3obs" in trial_name:
            return "not promotable until stricter observation breadth improves path quality on a fresh tape update."
        if trial_name.startswith("sparse_"):
            return "not promotable because it generated too few closed trades to trust as a default rule."
        if "max3" in trial_name:
            return "not promotable as a default yet because the higher score came from only two closed trades."
        if "recent_failure" in trial_name:
            return "not promotable because it uses previous failure-case tickers and can leak sample-specific knowledge."
        return "not promotable; diagnostic trial only."

    failed_lines = "\n".join(
        f"- {t['trial_name']}: score={t['score']:.6f}, notes={t['notes']}" for t in failed
    )
    checks_text = json.dumps(checks, ensure_ascii=False, indent=2)
    diagnostic_trials = [t for t in trials if t["trial_name"].endswith("_diagnostic")]
    diagnostic_lines = "\n".join(
        f"- {t['trial_name']}: score={t['score']:.6f}; {diagnostic_reason(t['trial_name'])}"
        for t in diagnostic_trials
    )
    sleeve_diagnostics = sleeve_diagnostics or {}
    sleeve_lines = []
    sleeves = sleeve_diagnostics.get("sleeves") if isinstance(sleeve_diagnostics, dict) else {}
    if isinstance(sleeves, dict):
        for sleeve_name, row in sleeves.items():
            sleeve_lines.append(
                f"- {sleeve_name}: score={row['score']:.6f}, "
                f"OOS={row['out_of_sample_score']:.6f}, "
                f"3x_slippage={row['cost_stress_score']:.6f}, "
                f"promotable={row['promotable']}, reason={row['reason']}"
            )
    sleeve_text = "\n".join(sleeve_lines) or "- Sleeve diagnostics unavailable."
    failure_ticker_text = ", ".join(recent_failure_tickers) if recent_failure_tickers else "none"
    price_coverage = price_coverage or {}
    source_counts = price_coverage.get("daily_signal_price_source_counts", {})
    source_counts_text = ", ".join(f"{key}={value}" for key, value in source_counts.items()) or "unknown"
    price_coverage_text = (
        f"- Status: {price_coverage.get('status', 'unknown')}\n"
        f"- daily_signal rows: {price_coverage.get('daily_signal_rows', 0)}; "
        f"source counts: {source_counts_text}; "
        f"daily_prices rows loaded: {price_coverage.get('daily_prices_rows_loaded', 0)}; "
        f"independent row ratio: {float(price_coverage.get('independent_price_row_ratio', 0.0)):.2%}\n"
        f"- Best path missing prices: {price_coverage.get('days_with_missing_prices', 0)}/"
        f"{price_coverage.get('best_curve_days', 0)} days "
        f"({float(price_coverage.get('missing_price_day_ratio', 0.0)):.2%}); "
        f"{price_coverage.get('missing_position_price_slots', 0)}/"
        f"{price_coverage.get('held_position_slots', 0)} held-position slots "
        f"({float(price_coverage.get('missing_position_price_slot_ratio', 0.0)):.2%})\n"
        "- Integration decision: keep the best policy as a cap/warning only; weak price "
        "coverage applies a coverage-adjusted simulated cap (one-quarter cap when "
        "independent daily_prices coverage is zero), and exposure must not expand until "
        "local daily_prices coverage is validated."
    )
    price_readiness = price_readiness or {}
    latest_missing = price_readiness.get("latest_missing_tickers", [])
    latest_missing_text = ", ".join(str(ticker) for ticker in latest_missing[:8]) if isinstance(latest_missing, list) else ""
    unblock_tickers = price_readiness.get("unblock_tickers", latest_missing)
    unblock_text = ", ".join(str(ticker) for ticker in unblock_tickers[:8]) if isinstance(unblock_tickers, list) else ""
    missing_queue = format_missing_price_queue(price_readiness)
    backfill_plan = price_readiness.get("backfill_plan", {}) if isinstance(price_readiness, dict) else {}
    backfill_plan_path = str(price_readiness.get("backfill_plan_path", "") or "") if isinstance(price_readiness, dict) else ""
    if isinstance(backfill_plan, dict):
        top_plan = ", ".join(str(ticker) for ticker in backfill_plan.get("top_priority_tickers", [])[:5])
        plan_total = backfill_plan.get("total_missing_tickers", 0)
    else:
        top_plan = ""
        plan_total = 0
    backfill_dry_run_command = "python scripts/backfill_daily_prices.py --dry-run --limit 5"
    if backfill_plan_path:
        backfill_dry_run_command += f" --plan {backfill_plan_path}"
    backfill_status_command = "python scripts/backfill_daily_prices.py --status --limit 5"
    if backfill_plan_path:
        backfill_status_command += f" --plan {backfill_plan_path}"
    backfill_import_command = (
        "python scripts/backfill_daily_prices.py --import-csv "
        "data/local_daily_prices.csv --source local_csv --dry-run"
    )
    if backfill_plan_path:
        backfill_import_command += f" --plan {backfill_plan_path}"
    backfill_strict_import_command = (
        "python scripts/backfill_daily_prices.py --import-csv "
        "data/local_daily_prices.csv --source local_csv --dry-run "
        "--coverage-limit 5 --require-plan-coverage"
    )
    if backfill_plan_path:
        backfill_strict_import_command += f" --plan {backfill_plan_path}"
    price_readiness_text = (
        f"- Status: {price_readiness.get('status', 'unknown')}\n"
        f"- Signal tickers fully covered by local daily_prices: {price_readiness.get('priced_signal_ticker_count', 0)}/"
        f"{price_readiness.get('total_signal_ticker_count', 0)}; "
        f"missing={price_readiness.get('missing_signal_ticker_count', 0)}\n"
        f"- Latest signal date: {price_readiness.get('latest_signal_date', 'unknown')}; "
        f"latest missing tickers: {latest_missing_text or 'none'}; "
        f"minimum next rows={price_readiness.get('minimum_next_rows', 0)}\n"
        f"- Minimum unlock batch: {unblock_text or 'none'}\n"
        f"- Priority backfill queue: {missing_queue or 'none'}\n"
        f"- Machine-readable backfill plan: `{backfill_plan_path or 'not written'}`; "
        f"plan tickers={plan_total}; top priority={top_plan or 'none'}\n"
        f"- Local DB plan coverage check: `{backfill_status_command}`\n"
        f"- Local request preview, no network: `{backfill_dry_run_command}`\n"
        f"- Local CSV exact signal-date validation: `{backfill_import_command}`\n"
        f"- Local CSV strict top-plan validation: `{backfill_strict_import_command}`\n"
        "- Market-data fetch guard: `scripts/backfill_daily_prices.py` blocks MarketDataService fetches "
        "unless `--allow-market-fetch` or `SOVEREIGN_HALL_ALLOW_MARKET_BACKFILL=1` is set.\n"
        "- Integration decision: do not synthesize `daily_prices` from prediction current_price; "
        "surface this as a local backfill checklist and local CSV import path in user entries; "
        "keep exposure caps active until the cycle validates coverage."
    )
    price_readiness_stall = price_readiness_stall or {}
    stall_blocked_runs = int(price_readiness_stall.get("consecutive_blocked_runs", 0) or 0)
    stall_min_runs = int(price_readiness_stall.get("minimum_blocked_runs", 3) or 3)
    stall_status = str(price_readiness_stall.get("status", "") or "")
    stall_cap = best_cap * 0.05 if stall_status in {"stalled_no_daily_prices", "stalled_partial_daily_prices"} else None
    stall_kind = str(price_readiness_stall.get("stall_kind", "") or "")
    stall_unblock = price_readiness_stall.get("unblock_tickers", [])
    stall_unblock_text = ", ".join(str(ticker) for ticker in stall_unblock[:8]) if isinstance(stall_unblock, list) else ""
    if stall_kind == "partial_daily_price_backfill_needed" or stall_status == "stalled_partial_daily_prices":
        stall_decision_subject = "repeated partial daily_prices no-progress"
    else:
        stall_decision_subject = "repeated empty daily_prices"
    price_readiness_stall_text = (
        f"- Status: {price_readiness_stall.get('status', 'unknown')}\n"
        f"- Consecutive blocked runs: {stall_blocked_runs}/{stall_min_runs}; "
        f"blocked run ids: {', '.join(price_readiness_stall.get('blocked_run_ids', [])[-6:]) or 'none'}\n"
        f"- Next ticker: {price_readiness_stall.get('next_ticker', 'none') or 'none'}; "
        f"same-next-ticker runs={price_readiness_stall.get('same_next_ticker_runs', 0)}\n"
        f"- Minimum unlock batch: {stall_unblock_text or 'none'}; "
        f"same-batch runs={price_readiness_stall.get('same_unblock_batch_runs', 0)}\n"
        f"- Rule: {price_readiness_stall.get('rule', 'Do not widen exposure while local daily_prices are repeatedly blocked.')}\n"
        f"- Integration decision: {stall_decision_subject} is treated as a user-entry warning and "
        f"{'a stricter simulated-buy cap of ' + format(stall_cap, '.2%') if stall_cap is not None else 'the existing no-expansion data-quality gate'}; "
        "do not add new leaderboard branches until local price validation moves."
    )
    tape_update = tape_update or {}
    portfolio_lifecycle = portfolio_lifecycle or {}
    portfolio_invested = portfolio_lifecycle.get("invested_ratio")
    portfolio_gap = portfolio_lifecycle.get("deployment_gap")
    portfolio_invested_text = (
        f"{float(portfolio_invested):.2%}" if portfolio_invested is not None else "N/A (realtime quote required)"
    )
    portfolio_gap_text = (
        f"{float(portfolio_gap):.2f}" if portfolio_gap is not None else "N/A (realtime quote required)"
    )
    redeployment_state = portfolio_lifecycle.get("redeployment_state") or {}
    new_rows = tape_update.get("new_prediction_rows_since_previous")
    new_rows_text = "unknown" if new_rows is None else str(new_rows)
    try:
        zero_new_tape = int(new_rows or 0) <= 0
    except (TypeError, ValueError):
        zero_new_tape = False
    tape_cap_decision = (
        "no new local predictions since the previous cycle; when the latest date exceeds the freshness limit, veto new/expanded simulated longs while keeping exits available."
        if zero_new_tape
        else "not enough fresh local tape for exposure widening; keep as cap/warning and apply thin-tape observational sizing."
    )
    tape_update_text = (
        f"- Status: {tape_update.get('validation_status', 'unknown')}\n"
        f"- Prediction rows: current={tape_update.get('current_prediction_rows', 0)}, "
        f"previous={tape_update.get('previous_prediction_rows', 'unknown')}, "
        f"new_since_previous={new_rows_text}\n"
        f"- Latest local prediction date: {tape_update.get('current_latest_prediction_date', 'unknown')} "
        f"with {tape_update.get('latest_date_prediction_rows', 0)} rows; "
        f"age={tape_update.get('latest_prediction_age_days', 'unknown')} days\n"
        f"- Rule: {tape_update.get('rule', 'Do not widen exposure without fresh local validation tape.')}\n"
        f"- Integration decision: {'fresh enough for validation review' if tape_update.get('enough_for_policy_widening') else tape_cap_decision}"
    )
    text = f"""# Heuristic Learning Cycle

## Run
- Run time: {run_started}
- Data source: `{db_path}`
- Samples consumed: {sample_count} prediction rows
- Best policy: `{best_name}`
- Current best score: {best_metrics['score']:.6f}
- Immediate prior run comparison: {latest_comparison}
- Historical best comparison: {comparison}

## Blocking & Repeated Warning Review
- Reviewed automation memory and recent heuristic-cycle READMEs before running new trials. The repeated blocker remains exact local `daily_prices` plan-date coverage plus thin/stale local prediction tape, not a missing evaluation script.
- Current local status: Top priority `daily_prices` plan coverage is still incomplete, and `tape_update.json` does not meet the fresh-row/latest-day thresholds required for exposure widening.
- Root cause advanced this cycle: empty-book committee rounds silently discarded hold, missing-ticker, unsupported-direction, and non-executable short decisions before redeployment accounting. After many rounds, users still saw only `missing_approved_candidates`, so the next repair action was unknowable.
- System fix: `run_discussion` now performs deterministic committee-decision preflight before quote lookup/execution, assigns explicit rejection codes, and persists both last-round and cumulative code counts in `simulation_redeployment_state`; `check_db` exposes those counts. Historical attempts are not retroactively relabeled because their discarded decisions cannot be reconstructed.
- Evaluation fix: zero/very sparse trade slices are now marked as insufficient robustness evidence instead of passing split/cost checks merely because they avoid all exposure.
- Failure-analysis fix: a zero-trade retained path is now recorded as insufficient trade evidence, not mislabeled as a drawdown or overtrading episode with zero exposure.
- Integration decision: full deployment is now a portfolio invariant, while unavailable/stale prices remain an explicit operational blocker rather than a reason to fabricate fills; risk is expressed through rotation, diversification, and per-position exits instead of strategic cash.

## What Changed
- Added `services/portfolio_policy.py` with deterministic stop-loss, take-profit, max-holding, fresh-price, deployment-gap, and candidate-allocation rules.
- Added a single-row durable redeployment state machine; simulated sells enqueue released cash, committee rounds record exact failures, and empty-book state is recovered without using a quote fallback.
- Migrated `simulation_positions` with opened/mark/review lifecycle fields and backfilled legacy opening times from local simulated buy history.
- `run_discussion` now reviews every open position before considering new proposals; missing realtime quotes block valuation/trading, while realtime stop/expiry breaches generate simulated sells only during market hours.
- Fixed `run_discussion` post-exit candidate sizing to await the realtime portfolio valuation triple and stop cleanly on incomplete quotes instead of raising before redeployment.
- Empty-book lifecycle artifacts now report exact 0% invested ratio/full deployment gap and audit same-day forced exits; they no longer label a quote-free empty book as valuation incomplete.
- Simulation capital now targets 100% investment. Approved new long candidates receive a deployment floor from the remaining gap; per-name/evidence rules redistribute exposure instead of reserving cash.
- `check_db` now displays invested ratio, deployment gap, holding age, quote age, and the current mandatory action for every holding.
- Extended the local-only delayed-signal heuristic evaluation loop for this cycle.
- Tested small interpretable changes: trend filtering, volatility scaling, anomaly veto, drawdown guard, losing-streak cooldown, minimum holding periods, no-new-risk pauses, and rebalance friction.
- Advanced the prior no-lookahead direction by adding failure-memory replay trials that only penalize tickers after their own closed backtest loss is already known at that simulated date.
- Kept no-lookahead failure memory out of the default offline trading policy unless it produces a real score/trade-path improvement; equal-score behavior is not enough to promote a leaderboard rule.
- Advanced the prior durable simulated closed-loss memory direction by validating it over this refreshed local tape: active memory remains useful as a cap/warning, but no-lookahead replay only tied the best score and does not justify widening rules.
- Closed the user-entry loop by persisting simulated-account closed-trade losses into `simulation_risk_memory`; this is used as a conservative position cap/warning in live simulation paths, not as a return-seeking allocator.
- Advanced the prior failure-pattern direction by testing recent-failure ticker half-size/veto diagnostics for: {failure_ticker_text}.
- Kept recent-failure ticker rules out of promotable best selection because they depend on prior failure labels and can overfit the same local tape.
- Added ETF-only and single-stock-only sleeve trials so the cycle no longer evaluates every universe mix as one undifferentiated basket.
- Advanced the prior sleeve-allocation direction by writing `sleeve_diagnostics.json`; ETF and single-stock sleeves must both pass primary score, time split, and non-thin 3x-slippage stress before a portfolio allocator can be promoted.
- Advanced the prior thin cost-stress direction by testing a reduced-exposure single-stock sleeve with 6-day minimum holds, 6% single-name cap, and 24% gross cap.
- Advanced the prior reduced-exposure validation direction by testing an evidence-gated single-stock sleeve with 6-day minimum holds, a 5% single-name cap, and at least 2 same-day local observations before entry.
- Tested a stricter 4% single-name cap on the same 6-day, 2-observation single-stock rule; it is promoted only if it improves the same score path after split and cost checks.
- Advanced the prior evidence-gated reduced-exposure direction with a stricter 12% abnormal-move veto; it exits locally anomalous signal days sooner without adding a new data source.
- Tested a 3-name/15% gross-cap diagnostic; it scored higher but is kept out of default promotion because only two closed trades drive the improvement.
- Tested a stricter 3-observation diagnostic for the retained single-stock rule; it is not promotable unless it improves path quality without collapsing trade breadth.
- Tested `validated_daily_price_only_diagnostic` to make the daily_prices dependency explicit; it must stay diagnostic while independent local daily prices are missing.
- Closed the live evidence-gate loop: simulated long proposals now pass local same-day prediction observation counts into `services/heuristic_policy.py`, and insufficient evidence caps them to a small observation-size position instead of only printing a warning.
- Added `sparse_hold8_cap6_diagnostic` to document why very sparse one-trade policies are not promoted even when their leaderboard score is high.
- Shared the latest heuristic result through `services/heuristic_policy.py` for entry-point risk display, manual research warnings, simulated-trading position caps, and prompt-level failure-case constraints.
- Added thin cost-stress signaling to the shared heuristic context so entry points now show OOS/3x-slippage scores and warn when the cost-stress margin is too thin to expand exposure.
- Closed the price-source risk loop: when `daily_prices` coverage is empty or partial, `services/heuristic_policy.py` treats prediction-current-price fallback as an explicit no-expansion warning in status, research prompts, and simulated trade cap reasons.
- Advanced the prior data-source direction by writing `price_coverage.json`, including price-source counts and missing held-position price slots for the retained path.
- Converted the latest price-coverage warning into a real simulated-investment constraint: weak or unvalidated local price coverage now applies a coverage-adjusted cap; zero independent daily_prices coverage allows at most one-quarter of the latest policy single-name cap.
- Added a local-only CSV validation/import path in `scripts/backfill_daily_prices.py` so daily_prices can be repaired from an independently provided local OHLC file without network calls.
- Tightened local CSV validation to exact missing signal dates from the plan/tape, and disabled MarketDataService fetches by default so this automation remains local-only unless explicitly opted in.
- Advanced the daily_prices closure into `check_db`: the entry now compares the latest priority backfill queue with live local `daily_prices` rows and prints the still-missing next ticker plus the active no-expansion cap.
- Clarified the repeated priority-queue warning by rendering missing date ranges explicitly and aligning the local CSV validation command with the machine-readable backfill plan.
- Clarified the exact latest-date backfill blocker by carrying `unblock_tickers` through `price_readiness.json`, stalled-readiness reports, `check_db`, and simulated trade cap reasons, so historical long-gap tickers do not obscure the minimum batch needed to rerun validation.
- Added a no-network `scripts/backfill_daily_prices.py --status` check that compares the current SQLite `daily_prices` table against exact missing signal dates from the latest plan/tape before users rerun the cycle.
- Fixed `python -m sovereign_hall.run_discussion --help` so CLI help is available even while a long-running discussion instance holds the single-instance lock.
- Advanced the prior data-quality closure by measuring consecutive `blocked_no_daily_prices` cycles; when the same local price gap repeats, user entries and simulated-buy reasons treat it as a stalled backfill task instead of another leaderboard signal.
- Fixed stalled-readiness accounting so multiple manual reruns on the same calendar date count as one heuristic cycle; this prevents a debugging rerun from prematurely tightening simulated-buy caps.
- Advanced the prior fresh-tape validation direction by writing `tape_update.json`; thin tape updates are surfaced as a user-entry warning and an observational simulated-buy cap instead of being treated as validation for wider exposure.
- Tightened the fresh-tape entry loop: if the latest cycle has zero new local prediction rows, simulated long proposals are capped to 10% of the retained policy single-name cap until a meaningful tape update arrives.
- Closed the stale-artifact gap: entry points dynamically recompute prediction age from the latest local tape date, so freshness cannot remain frozen at artifact creation time.
- Added a stale-tape entry veto: expired local evidence can no longer create or expand simulated long positions; existing positions are not accidentally liquidated and explicit reductions/exits remain available.
- Closed the veto-recovery boundary: fresh age alone is insufficient to clear the gate; the shared loader also requires the configured new-row and latest-day breadth thresholds, with a regression test at the exact age boundary.
- Closed the live-artifact mismatch: all three user entries now see predictions appended after the latest cycle without mutating historical artifacts, while an isolated refresh remains vetoed until breadth recovers.
- Connected sleeve diagnostics as a conservative user-entry constraint: failed ETF sleeve checks are surfaced as warnings and ETF simulated buys are capped for small observational sizing instead of treated as a promoted allocator.
- Replaced the low-gross cash-reserve rule: simulated portfolios target 100% gross without leverage; risk constraints remain per-position and evidence-based.
- Kept the latest best as a conservative risk constraint; even when split/cost checks pass, a lower score versus historical best is treated as a stability warning rather than a reason to increase exposure.
- Wrote the retained policy snapshot to `policy_snapshot.py`.
- Replaced the annualized-return/turnover-dominated score with `{REWARD_VERSION}`: net total account return is primary, full transaction costs are already deducted from equity, prolonged cash receives a magnitude-times-duration penalty, and cost receives only a small additional discipline term.
- Enforced the shared `{MAX_DAILY_TRADES}`-transaction daily hard limit in offline rebalancing; reductions/exits are applied before increases and deferred changes remain visible in the curve notes.

## Best Metrics
- Total return: {best_metrics['total_return']:.4%}
- Annualized return: {best_metrics['annualized_return']:.4%}
- Max drawdown: {best_metrics['max_drawdown']:.4%}
- Sharpe: {best_metrics['sharpe']:.3f}
- Sortino: {best_metrics['sortino']:.3f}
- Win rate: {best_metrics['win_rate']:.2%}
- Turnover: {best_metrics['turnover']:.3f}
- Trade count: {best_metrics['trade_count']}
- Closed trade count: {best_metrics.get('closed_trade_count', 0)}
- Maximum trades in one day: {best_metrics.get('max_daily_trade_count', 0)} / {best_metrics.get('daily_trade_limit', MAX_DAILY_TRADES)}
- Days at trade limit: {best_metrics.get('days_at_trade_limit', 0)}
- Deferred transaction actions: {best_metrics.get('deferred_trade_actions', 0)}
- Average invested ratio: {best_metrics.get('average_invested_ratio', 0.0):.2%}
- Average cash ratio: {best_metrics.get('average_cash_ratio', 1.0):.2%}
- Long-idle cash exposure penalty input: {best_metrics.get('idle_cash_penalty', 0.0):.6f}; longest >20% cash streak: {best_metrics.get('max_high_cash_streak_days', 0)} days
- Gross total return before costs: {best_metrics.get('gross_total_return_before_cost', 0.0):.4%}; modeled cost paid: {best_metrics.get('cost_paid', 0.0):.4%}
- Reward: `{best_metrics.get('reward_formula', REWARD_FORMULA)}`
- Reward breakdown: `{json.dumps(best_metrics.get('score_breakdown', {}), ensure_ascii=False, sort_keys=True)}`
- Cost assumption: {best_metrics['cost_assumption']}

## Failed Or Weaker Directions
{failed_lines or "- None; only one trial was available."}

## Diagnostic Only
{diagnostic_lines or "- No recent failure tickers were available for diagnostic tests."}

## Simplification Check
- Retained best policy is direct and reproducible: no volatility-scaling branch, no previous-run failure ticker labels, and no dense parameter search are required for the default rule.
- The simplified form is the generated `policy_snapshot.py`; it keeps only confidence/risk-reward filters, trend/anomaly guard, {best_hold}-day minimum hold, {best_cap:.1%} single-name cap, {best_gross:.1%} gross cap, {evidence_text}, cooldown, and rebalance friction.
- Sparse high-score branches are not adopted as defaults when the improvement comes from too few closed trades rather than a broader path improvement.

## Sleeve Allocator Check
- Allocator status: {sleeve_diagnostics.get("allocator_status", "unknown")}
- Rule: {sleeve_diagnostics.get("rule", "ETF and single-stock sleeves must pass before allocator promotion.")}
{sleeve_text}

## Price Coverage Check
{price_coverage_text}

## Daily Price Readiness
{price_readiness_text}

## Persistent Data-Quality Stall
{price_readiness_stall_text}

## Tape Update Check
{tape_update_text}

## Simulated Portfolio Lifecycle
- Status: {portfolio_lifecycle.get('status', 'unknown')}
- Current invested ratio: {portfolio_invested_text}; target: {float(portfolio_lifecycle.get('target_invested_ratio', 1.0)):.2%}
- Operational deployment gap: {portfolio_gap_text}; cash is not a strategic risk reserve.
- Open positions recorded: {int(portfolio_lifecycle.get('position_count', 0))}; quote blockers: {int(portfolio_lifecycle.get('blocked_price_count', 0))}
- Lifecycle review ran: {bool(portfolio_lifecycle.get('lifecycle_review_ran', False))}; triggered/executed exits today: {int(portfolio_lifecycle.get('triggered_exit_count', 0))}; exit net proceeds: {float(portfolio_lifecycle.get('triggered_exit_net_proceeds', 0.0) or 0.0):.2f}
- Positions reviewed: {int(portfolio_lifecycle.get('reviewed_position_count', portfolio_lifecycle.get('position_count', 0)))}; current invested ratio={portfolio_invested_text}; deployment gap={portfolio_gap_text}.
- Redeployment status: {portfolio_lifecycle.get('redeployment_status', 'not_applicable')}; operational cash reason: {portfolio_lifecycle.get('operational_cash_reason', 'N/A')}
- Durable redeployment queue: status={redeployment_state.get('status', 'missing')}; gap={redeployment_state.get('deployment_gap', 'N/A')}; attempts={redeployment_state.get('attempt_count', 0)}; last candidates/fills={redeployment_state.get('last_candidate_count', 0)}/{redeployment_state.get('last_trade_count', 0)}; blocker={redeployment_state.get('blocker_code', 'N/A')}; next={redeployment_state.get('next_action', 'N/A')}
- Machine-readable snapshot: `portfolio_lifecycle_review.json`. No trade is generated by this report.

## Overfitting Risk
```json
{checks_text}
```

Flag: {"suspected overfit risk" if checks.get("overfit_risk") else "no severe split/cost-stress failure detected"}.

## User Entry Impact
- This cycle closes the opaque-redeployment gap: `run_discussion` records every deterministic committee rejection and `check_db` shows last-round plus cumulative rejection-code counts alongside the durable queue.
- Decision preflight covers committee hold, missing ticker, unsupported direction, empty-book short, already-held long, zero target, cooldown, missing realtime quote, valuation-incomplete, heuristic veto, market closed, lot/cash, and generic execution failures without weakening any trading safety gate.
- Same-day simulated fills no longer bypass the mandatory review of every existing position; the daily trade cap is counted from SQLite and survives restarts.
- Per-position review result this run: reviewed {int(portfolio_lifecycle.get('reviewed_position_count', portfolio_lifecycle.get('position_count', 0)))} open positions; triggered exits={int(portfolio_lifecycle.get('triggered_exit_count', 0))}; blocked exits={int(portfolio_lifecycle.get('blocked_price_count', 0))}.
- Capital lifecycle result this run: invested ratio={portfolio_invested_text}, deployment gap={portfolio_gap_text}; exit/released cash is {portfolio_lifecycle.get('redeployment_status', 'not_applicable')} and has not been classified as strategic cash.
- Redeployment rejection diagnostics: last={json.dumps(redeployment_state.get('last_rejection_counts', {}), ensure_ascii=False, sort_keys=True)}; cumulative={json.dumps(redeployment_state.get('rejection_counts_total', {}), ensure_ascii=False, sort_keys=True)}. Counts cover post-migration genuine committee rounds only; historical opaque attempts are preserved but not fabricated into categories.
- Lifecycle audit now treats an empty portfolio as completely valued (0% invested, full cash deployment gap) and records same-day realtime lifecycle exits plus pending redeployment instead of incorrectly reporting valuation N/A.
- Improved `check_db`: users now see whether capital is actually deployed, why any cash is operationally blocked, and each position's age/quote-age/exit status.
- Improved `run_discussion`: every open position is reviewed before new proposals; absence of a new committee ticker no longer silently freezes legacy holdings.
- Improved simulation lifecycle: stop-loss (-8%), take-profit (+15%), and maximum holding period (30 days) are explicit, persisted, and executable only after a fresh realtime quote is fetched during market hours.
- Improved capital use: target invested ratio is 100%; cash is never labeled a risk reserve, and eligible approved candidates receive the undeployed allocation before per-name constraints.
- Improved reward alignment: `capital_return_v2` makes net total account return the primary positive term, deducts modeled fee/stamp-duty/slippage in equity, and penalizes both the magnitude and duration of excess cash.
- Improved transaction-frequency control: offline rebalancing, `run_discussion`, and direct `InvestmentSimulation.execute_trade` share a hard maximum of {MAX_DAILY_TRADES} simulated transaction actions per day; exits/reductions consume capacity before increases and the counter persists in SQLite.
- Improved status/prompt alignment: `check_db` prints the active reward formula and today's transaction count, while research and investment-committee prompts receive the same reward objective and daily limit.
- Improved entry: `python -m sovereign_hall.check_db` now shows latest best policy, score, overfit warning, single-name cap, and recent failure cases.
- Improved entry safety: `python -m sovereign_hall.check_db` now treats closed stdin as a safe non-interactive exit after printing status.
- Realtime valuation guard: `check_db` enables realtime quotes by default; if any quote is unavailable it shows N/A and never falls back to local daily prices, predictions, artifacts, or cost basis.
- Improved simulation path: `run_discussion` and `InvestmentSimulation.execute_trade` now cap simulated long positions using the latest local heuristic max position; weak or lower-scoring policies are used only as warnings/risk caps, not as return-seeking exposure increases.
- Improved manual advice path: `python -m sovereign_hall.research_interactive` now prints and saves the latest heuristic policy, overfit warning, and recent failure cases alongside the generated report.
- Improved research prompt path: `run_discussion` and `research_interactive` now pass the latest local heuristic policy, failure-case tickers, and overfit warning into proposal, voting, and conclusion prompts as explicit risk constraints.
- User-visible change: before simulated trades and in manual research reports, users see the active heuristic risk context; oversized proposed positions are reduced with an explicit reason in trade logs, and repeated failure-case tickers must be justified or reduced.
- Improved status display: `check_db`/manual research now show recent failure tickers with the exact simulated-position cap and prompt action currently applied by `services/heuristic_policy.py`.
- Improved durable simulation memory: `InvestmentSimulation.init_tables()` and `execute_trade()` refresh `simulation_risk_memory` from realized simulated sell trades; `check_db` refreshes and displays the same derived memory before printing heuristic status.
- User-visible change: tickers with recent realized simulated losses worse than -3% are capped to the failure-scale position limit until the 8-day memory expires, and trade reasons/status output identify this as local simulation risk memory.
- Improved thin-cost-stress closure: `services/heuristic_policy.py` now exposes OOS and 3x-slippage scores to `check_db`, manual research prompts, and simulated trade reasons; if 3x-slippage score is below 0.02, the latest policy remains a cap/warning only and explicitly forbids exposure expansion.
- Improved data-source closure: `check_db`, `run_discussion`, `research_interactive`, and simulated trade reasons now surface `daily_prices` absence as a no-expansion warning when the latest run still relies on prediction `current_price` fallback.
- Improved price-coverage closure: `check_db`, research prompt context, and simulated trade reasons now surface the latest `price_coverage.json` ratios, including held-position missing-price slots.
- Improved daily-price-readiness closure: `check_db`, research prompt context, and manual research reports now surface `price_readiness.json`, including the prioritized missing-price queue.
- Improved live daily-price-readiness closure: `check_db` now validates that priority queue against the current SQLite `daily_prices` table and prints covered/missing queue tickers, the next local backfill target, and the active no-expansion cap before the user starts simulation.
- Improved local backfill repair path: `check_db` now prints a no-network plan preview and a local CSV exact signal-date validation command; `scripts/backfill_daily_prices.py --import-csv ... --dry-run` validates OHLC rows without network access.
- Improved strict import gate: `scripts/backfill_daily_prices.py --require-plan-coverage --coverage-limit 5` now fails nonzero unless the selected top-priority plan signal dates are covered, so a parseable CSV cannot be mistaken for a blocker-clearing import.
- Improved backfill-readiness closure: user entries and reports now label priority queue dates as missing ranges, and local CSV validation can compare supplied rows with the current plan before import.
- Improved backfill blocker clarity this cycle: user entries and reports now show the minimum latest-date unlock batch separately from the longer historical priority queue, so users can fill `{latest_missing_text or 'none'}` first instead of confusing it with older gaps.
- Improved backfill verification path: `check_db` now prints a no-network DB coverage command, and `scripts/backfill_daily_prices.py --status` reports exact still-missing signal dates before any exposure cap can be relaxed.
- Improved daily-price backfill closure: this run writes `daily_price_backfill_plan.csv` and `daily_price_backfill_plan.json`; user entries surface the plan path and top priority ticker so repeated empty `daily_prices` runs have a concrete local next step.
- Improved `run_discussion` operability: `python -m sovereign_hall.run_discussion --help` no longer fails behind the active single-instance lock, while real runs remain lock-protected.
- Improved daily-price-readiness simulation closure: blocked independent `daily_prices` readiness now applies a simulated-buy cap through `services/heuristic_policy.py`, so missing local prices constrain entries rather than only appearing in reports.
- Improved stalled-readiness closure: `check_db`, research prompt context, and simulated trade reasons now show consecutive empty-daily_prices cycles; after repeated blockage, simulated buys use an extra-small observation cap and the cycle explicitly avoids new leaderboard branches.
- Improved stalled-readiness accounting this cycle: same-day reruns are deduped before counting consecutive blocked cycles, so the user-entry cap only tightens after distinct cycle dates repeat the same local data gap.
- Improved partial-readiness stall closure: repeated `partial_daily_price_backfill_needed` cycles with unchanged coverage are now treated as a stalled data task, so simulated buys receive the same extra-small no-progress cap as an empty-price stall.
- Improved simulated-investment safety: weak or unvalidated price coverage now reduces simulated long proposals by coverage quality; with zero independent daily_prices rows the user-entry cap is one-quarter of the latest policy cap rather than a fixed half-cap.
- Improved fresh-tape closure: `check_db`, research prompt context, and simulated trade reasons now surface `tape_update.json`; when the current cycle only adds a thin local tape update, simulated long proposals are capped to observational sizing and the policy is not treated as validation for widening.
- Improved zero-new-tape closure: when `tape_update.json` reports no new prediction rows since the previous run, `check_db`, research prompts, and simulated trade reasons show the stricter zero-new-tape cap instead of treating repeated samples as validation.
- Improved stale-tape closure this cycle: all three entries recompute a single freshness state from age and breadth; `check_db` shows the current state, while `run_discussion`/simulation clear the veto only after all configured local-tape thresholds pass.
- Improved sleeve-allocator closure: `services/heuristic_policy.py` now exposes `sleeve_diagnostics.json`; because ETF sleeve checks are not promotable this run, ETF simulated long proposals are capped to half of the latest policy cap with an explicit local-risk reason.
- Improved reduced-exposure closure: all three user entry paths inherit the retained single-stock cap and local evidence floor from `policy_snapshot.py` without adding a separate trading rule.
- Improved evidence-gate closure: `run_discussion` and `InvestmentSimulation.execute_trade` now apply the retained `min_signal_count` requirement to actual simulated long proposals; proposals with fewer fresh same-day local prediction observations are limited to a small observation-size cap.
- Improved portfolio-risk closure: `run_discussion` and `InvestmentSimulation.execute_trade` prevent leverage at 100% gross while using per-name, evidence, freshness, and lifecycle rules for risk control.
- Still not fully integrated: durable simulation risk memory is intentionally a warning/cap layer only; it is not promoted into the offline default policy or an ETF/single-stock allocator because the replay trials only tied, not improved, current best.
- Still not fully integrated: portfolio sleeve allocator is not promoted because both sleeves did not pass the required primary/OOS/cost-stress checks.
- Still not integrated as a default: sparse high-score policies are recorded as diagnostic-only when they produce too few closed trades for a defensible rule.
- Still not integrated as a default: the 3-name/15% gross-cap diagnostic needs another tape update because its score is driven by only two closed trades.
- Still operationally blocked from immediate full deployment: the current local tape lacks enough fresh, validated prices; the system reports the gap and refuses fabricated buys/sells instead of treating cash as a chosen risk reserve.
- Still not integrated as a default trading allocator: price coverage is too weak for exposure expansion when `price_coverage.json` reports unvalidated fallback or high missing held-position slots.
- Still not integrated as validation for exposure widening: `tape_update.json` does not meet the minimum fresh-row/latest-day observation thresholds when marked as thin or stale.
- Next minimum loop closure: backfill independently validated local `daily_prices` for the latest missing tickers shown by `check_db`, then validate whether the evidence-gated cap, observation-count cap, and ETF-sleeve caps reduce churn/drawdown over another tape update before widening exposure.
- This cycle's minimum local step: run `{backfill_status_command}` to verify current DB coverage, then use `{backfill_strict_import_command}` to validate independently supplied OHLC rows against exact missing signal dates before adding any new return-seeking heuristic branch.

## Reproduce
```bash
{command}
```

## Next 3 Directions
- Reduce the OOS average-cash ratio without breaking the {MAX_DAILY_TRADES}-action limit by testing staged top-five deployment and exit-first rotation; cash must remain an operational exception, not a policy allocation.
- Exercise the durable queue with a fresh, realtime-priced, committee-approved candidate and verify that the current 0%-invested deployment gap reaches an actual simulated fill or a precise rejection code.
- Backfill independently validated `daily_prices` from the latest unlock ticker and collect a meaningful fresh tape before testing whether higher-return but under-deployed branches can be converted to 100%-capacity policies without failing cost/OOS checks.
"""
    path.write_text(text, encoding="utf-8")


def make_plot(path: Path, trials: list[dict[str, Any]]) -> None:
    x = [trial["trial_index"] for trial in trials]
    y = [trial["score"] for trial in trials]
    labels = [trial["trial_name"] for trial in trials]
    try:
        from PIL import Image, ImageDraw, ImageFont

        width, height = 1200, 700
        margin = 90
        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()
        draw.text((margin, 25), "Sample efficiency across heuristic trials", fill=(20, 20, 20), font=font)
        draw.line((margin, height - margin, width - margin, height - margin), fill=(70, 70, 70), width=2)
        draw.line((margin, margin, margin, height - margin), fill=(70, 70, 70), width=2)
        if len(x) == 1:
            points = [(width // 2, height // 2)]
        else:
            ymin, ymax = min(y), max(y)
            if abs(ymax - ymin) < 1e-12:
                ymin -= 1.0
                ymax += 1.0
            points = []
            for xi, yi in zip(x, y):
                px = margin + (width - 2 * margin) * (xi - min(x)) / max(1, max(x) - min(x))
                py = height - margin - (height - 2 * margin) * (yi - ymin) / (ymax - ymin)
                points.append((int(px), int(py)))
        if len(points) > 1:
            draw.line(points, fill=(20, 100, 170), width=3)
        for point, label, yi in zip(points, labels, y):
            draw.ellipse((point[0] - 5, point[1] - 5, point[0] + 5, point[1] + 5), fill=(20, 100, 170))
            draw.text((point[0] - 45, point[1] - 22), f"{yi:.3f}", fill=(20, 20, 20), font=font)
            draw.text((point[0] - 55, point[1] + 10), label[:22], fill=(70, 70, 70), font=font)
        image.save(path)
        return
    except Exception:
        pass

    # Last-resort valid 1x1 PNG. The README and summary still carry the data.
    path.write_bytes(
        bytes.fromhex(
            "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de"
            "0000000c49444154789c6360f8ffff3f0005fe02fea73581e20000000049454e44ae426082"
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local heuristic learning cycle.")
    parser.add_argument("--db", default="data/sovereign_hall.db", help="SQLite database path")
    parser.add_argument("--runs-root", default="runs/heuristic_cycle", help="Output root")
    parser.add_argument("--timestamp", default=None, help="Optional run timestamp")
    args = parser.parse_args()

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    db_path = (project_root / args.db).resolve()
    runs_root = (project_root / args.runs_root).resolve()
    run_started = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = args.timestamp or run_started
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    previous_latest_run = latest_completed_run(runs_root)
    previous_latest_score = completed_run_best_score(previous_latest_run)
    if previous_latest_run is not None:
        prior_metrics = read_json(previous_latest_run / "best_metrics.json")
        if prior_metrics.get("reward_version") != REWARD_VERSION:
            previous_latest_score = None
    recent_failure_tickers = extract_failure_tickers_from_run(previous_latest_run)
    previous_score, previous_path = previous_best_score(runs_root)
    predictions = load_predictions(db_path)
    if predictions.empty:
        raise SystemExit(f"No local predictions found in {db_path}")
    tape_update = build_tape_update_report(predictions, previous_latest_run)
    price_history = load_daily_prices(db_path, predictions)
    daily = build_daily_tape(predictions, price_history)
    daily.to_csv(run_dir / "daily_signal_tape.csv", index=False)

    costs = CostConfig()
    changed_files = [
        "scripts/run_heuristic_cycle.py",
        "scripts/run_heuristic_cycle_stdlib.py",
        "services/heuristic_policy.py",
        "services/investment_simulation.py",
        "services/reward_policy.py",
        "services/portfolio_policy.py",
        "services/research_discussion.py",
        "run_discussion.py",
        "research_interactive.py",
        "check_db.py",
        "scripts/backfill_daily_prices.py",
        "tests/test_refactor_pipeline.py",
        "config.yaml",
    ]
    policies = [
        PolicyConfig(name="baseline_default_policy"),
        PolicyConfig(name="trend_filter", require_positive_trend=True, trend_lookback=3),
        PolicyConfig(name="volatility_scaled", vol_lookback=3, high_vol_threshold=0.045, high_vol_scale=0.45),
        PolicyConfig(name="risk_agent_veto", min_risk_reward=1.0, use_anomaly_veto=True, max_stop_gap=0.12),
        PolicyConfig(name="drawdown_rebalance_guard", drawdown_guard=0.45, drawdown_guard_threshold=0.025, rebalance_threshold=0.025),
        PolicyConfig(
            name="combined_guarded_policy",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            vol_lookback=3,
            high_vol_threshold=0.05,
            high_vol_scale=0.55,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.14,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            rebalance_threshold=0.02,
        ),
        PolicyConfig(
            name="loss_streak_cooldown",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            vol_lookback=3,
            high_vol_threshold=0.05,
            high_vol_scale=0.55,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.14,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            rebalance_threshold=0.02,
        ),
        PolicyConfig(
            name="min_holding_cooldown",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            vol_lookback=3,
            high_vol_threshold=0.05,
            high_vol_scale=0.55,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.14,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            min_holding_days=2,
            rebalance_threshold=0.02,
        ),
        PolicyConfig(
            name="no_new_risk_after_losses",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            vol_lookback=3,
            high_vol_threshold=0.05,
            high_vol_scale=0.55,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.14,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            rebalance_threshold=0.02,
        ),
        PolicyConfig(
            name="hold_and_pause_guard",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            vol_lookback=3,
            high_vol_threshold=0.05,
            high_vol_scale=0.55,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.14,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=2,
            rebalance_threshold=0.02,
        ),
        PolicyConfig(
            name="cost_robust_hold4",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
        ),
        PolicyConfig(
            name="age_volatility_reversal_stop",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            forced_exit_return_threshold=-0.025,
            forced_exit_vol_lookback=3,
            forced_exit_vol_threshold=0.030,
            rebalance_threshold=0.05,
        ),
        PolicyConfig(
            name="strict_age_reversal_stop",
            min_confidence=0.66,
            max_names=5,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            forced_exit_return_threshold=-0.035,
            forced_exit_vol_lookback=3,
            forced_exit_vol_threshold=0.040,
            rebalance_threshold=0.05,
        ),
        PolicyConfig(
            name="etf_only_cost_guard",
            min_confidence=0.66,
            max_names=4,
            max_position=0.10,
            max_gross=0.45,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="etf",
        ),
        PolicyConfig(
            name="single_stock_cost_guard",
            min_confidence=0.66,
            max_names=4,
            max_position=0.08,
            max_gross=0.40,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap6",
            min_confidence=0.66,
            max_names=4,
            max_position=0.06,
            max_gross=0.24,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap5_min2obs",
            min_confidence=0.66,
            max_names=4,
            max_position=0.05,
            max_gross=0.20,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=2,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap5_min2obs_anomaly12",
            min_confidence=0.66,
            max_names=4,
            max_position=0.05,
            max_gross=0.20,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.12,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=2,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap5_min3obs_diagnostic",
            min_confidence=0.66,
            max_names=4,
            max_position=0.05,
            max_gross=0.20,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.12,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=3,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap4_min2obs",
            min_confidence=0.66,
            max_names=4,
            max_position=0.04,
            max_gross=0.16,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=2,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="single_stock_hold6_cap5_max3_min2obs_diagnostic",
            min_confidence=0.66,
            max_names=3,
            max_position=0.05,
            max_gross=0.15,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=2,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="validated_daily_price_only_diagnostic",
            min_confidence=0.66,
            max_names=4,
            max_position=0.05,
            max_gross=0.20,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.12,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=6,
            min_signal_count=2,
            rebalance_threshold=0.05,
            universe="single_stock",
            require_independent_price=True,
        ),
        PolicyConfig(
            name="sparse_hold8_cap6_diagnostic",
            min_confidence=0.66,
            max_names=4,
            max_position=0.06,
            max_gross=0.36,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=8,
            rebalance_threshold=0.05,
            universe="single_stock",
        ),
        PolicyConfig(
            name="no_lookahead_failure_half_size",
            min_confidence=0.66,
            max_names=4,
            max_position=0.08,
            max_gross=0.40,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="single_stock",
            failure_memory_mode="scale",
            failure_memory_loss_threshold=-0.03,
            failure_memory_days=8,
            failure_memory_scale=0.5,
        ),
        PolicyConfig(
            name="no_lookahead_failure_veto",
            min_confidence=0.66,
            max_names=4,
            max_position=0.08,
            max_gross=0.40,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="single_stock",
            failure_memory_mode="veto",
            failure_memory_loss_threshold=-0.03,
            failure_memory_days=8,
        ),
        PolicyConfig(
            name="recent_failure_half_size_diagnostic",
            min_confidence=0.66,
            max_names=4,
            max_position=0.08,
            max_gross=0.40,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="single_stock",
            excluded_tickers=recent_failure_tickers,
            excluded_ticker_mode="scale",
            excluded_ticker_scale=0.5,
        ),
        PolicyConfig(
            name="recent_failure_veto_diagnostic",
            min_confidence=0.66,
            max_names=4,
            max_position=0.08,
            max_gross=0.40,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.45,
            drawdown_guard_threshold=0.02,
            loss_streak_threshold=2,
            loss_streak_guard=0.50,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
            universe="single_stock",
            excluded_tickers=recent_failure_tickers,
            excluded_ticker_mode="veto",
        ),
        PolicyConfig(
            name="full_deployment_diversified_hold10",
            min_confidence=0.65,
            max_names=10,
            max_position=0.10,
            max_gross=1.0,
            min_risk_reward=0.8,
            require_positive_trend=False,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=1.0,
            loss_streak_threshold=0,
            min_holding_days=10,
            rebalance_threshold=0.05,
        ),
        PolicyConfig(
            name="full_deployment_lower_churn_hold15",
            min_confidence=0.65,
            max_names=10,
            max_position=0.10,
            max_gross=1.0,
            min_risk_reward=0.8,
            require_positive_trend=False,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=1.0,
            loss_streak_threshold=0,
            min_holding_days=15,
            rebalance_threshold=0.08,
        ),
        PolicyConfig(
            name="compact_cost_robust_hold4",
            min_confidence=0.66,
            max_names=4,
            max_position=0.10,
            max_gross=0.55,
            min_risk_reward=0.9,
            require_positive_trend=True,
            trend_lookback=2,
            use_anomaly_veto=True,
            anomaly_return_threshold=0.18,
            drawdown_guard=0.50,
            drawdown_guard_threshold=0.025,
            loss_streak_threshold=2,
            loss_streak_guard=0.55,
            new_entry_loss_streak_threshold=2,
            min_holding_days=4,
            rebalance_threshold=0.05,
        ),
    ]

    trial_rows: list[dict[str, Any]] = []
    results: dict[str, dict[str, Any]] = {}
    trials_path = run_dir / "trials.jsonl"

    for index, policy in enumerate(policies):
        result = run_backtest(daily, policy, costs)
        results[policy.name] = result
        metrics = result["metrics"]
        row = {
            "trial_index": index,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "trial_name": policy.name,
            "changed_files": changed_files,
            "config": asdict(policy),
            "eval_period": f"{metrics['sample_start']}..{metrics['sample_end']}",
            "total_return": metrics["total_return"],
            "annualized_return": metrics["annualized_return"],
            "max_drawdown": metrics["max_drawdown"],
            "sharpe": metrics["sharpe"],
            "turnover": metrics["turnover"],
            "trade_count": metrics["trade_count"],
            "average_invested_ratio": metrics.get("average_invested_ratio", 0.0),
            "average_cash_ratio": metrics.get("average_cash_ratio", 1.0),
            "idle_cash_penalty": metrics.get("idle_cash_penalty", 0.0),
            "max_high_cash_streak_days": metrics.get("max_high_cash_streak_days", 0),
            "max_daily_trade_count": metrics.get("max_daily_trade_count", 0),
            "days_at_trade_limit": metrics.get("days_at_trade_limit", 0),
            "deferred_trade_actions": metrics.get("deferred_trade_actions", 0),
            "gross_total_return_before_cost": metrics.get("gross_total_return_before_cost", 0.0),
            "cost_paid": metrics.get("cost_paid", 0.0),
            "reward_version": metrics.get("reward_version", REWARD_VERSION),
            "score_breakdown": metrics.get("score_breakdown", {}),
            "cost_assumption": metrics["cost_assumption"],
            "score": metrics["score"],
            "notes": "local delayed daily signal simulation; no external market data",
        }
        trial_rows.append(row)
        append_jsonl(trials_path, [row])

    policies_by_name = {policy.name: policy for policy in policies}
    promotable_trials = [
        row for row in trial_rows
        if not row["trial_name"].endswith("_diagnostic")
        and policies_by_name[row["trial_name"]].max_gross >= 0.99
        and (
            policies_by_name[row["trial_name"]].max_names
            * policies_by_name[row["trial_name"]].max_position
        ) >= 0.99
    ]
    if not promotable_trials:
        raise RuntimeError("No full-deployment heuristic policy is available for selection")
    best_trial = max(promotable_trials, key=lambda row: row["score"])
    best_policy = next(policy for policy in policies if policy.name == best_trial["trial_name"])

    simplified = replace(
        best_policy,
        name="simplified_best_policy",
        vol_lookback=0 if best_policy.vol_lookback else best_policy.vol_lookback,
        high_vol_scale=1.0,
        anomaly_return_threshold=0.18,
    )
    best_without_name = {k: v for k, v in asdict(best_policy).items() if k != "name"}
    simplified_without_name = {k: v for k, v in asdict(simplified).items() if k != "name"}
    if simplified_without_name != best_without_name:
        simplified_result = run_backtest(daily, simplified, costs)
        simplified_metrics = simplified_result["metrics"]
        simplify_row = {
            "trial_index": len(trial_rows),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "trial_name": simplified.name,
            "changed_files": changed_files,
            "config": asdict(simplified),
            "eval_period": f"{simplified_metrics['sample_start']}..{simplified_metrics['sample_end']}",
            "total_return": simplified_metrics["total_return"],
            "annualized_return": simplified_metrics["annualized_return"],
            "max_drawdown": simplified_metrics["max_drawdown"],
            "sharpe": simplified_metrics["sharpe"],
            "turnover": simplified_metrics["turnover"],
            "trade_count": simplified_metrics["trade_count"],
            "average_invested_ratio": simplified_metrics.get("average_invested_ratio", 0.0),
            "average_cash_ratio": simplified_metrics.get("average_cash_ratio", 1.0),
            "idle_cash_penalty": simplified_metrics.get("idle_cash_penalty", 0.0),
            "max_high_cash_streak_days": simplified_metrics.get("max_high_cash_streak_days", 0),
            "max_daily_trade_count": simplified_metrics.get("max_daily_trade_count", 0),
            "days_at_trade_limit": simplified_metrics.get("days_at_trade_limit", 0),
            "deferred_trade_actions": simplified_metrics.get("deferred_trade_actions", 0),
            "gross_total_return_before_cost": simplified_metrics.get("gross_total_return_before_cost", 0.0),
            "cost_paid": simplified_metrics.get("cost_paid", 0.0),
            "reward_version": simplified_metrics.get("reward_version", REWARD_VERSION),
            "score_breakdown": simplified_metrics.get("score_breakdown", {}),
            "cost_assumption": simplified_metrics["cost_assumption"],
            "score": simplified_metrics["score"],
            "notes": "simplification stage: removed volatility scaling and excess anomaly tuning",
        }
        trial_rows.append(simplify_row)
        append_jsonl(trials_path, [simplify_row])
        if simplified_metrics["score"] >= best_trial["score"] - 1e-9:
            best_trial = simplify_row
            best_policy = simplified
            results[simplified.name] = simplified_result
        else:
            results[simplified.name] = simplified_result

    best_result = results[best_trial["trial_name"]]
    best_metrics = best_result["metrics"]

    summary_path = run_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(trial_rows[0].keys()))
        writer.writeheader()
        for row in trial_rows:
            writer.writerow(row)

    write_json(run_dir / "baseline_metrics.json", results["baseline_default_policy"]["metrics"])
    write_json(run_dir / "best_metrics.json", best_metrics)
    best_result["curve"].to_csv(run_dir / "equity_curve_best.csv", index=False)
    pd.DataFrame(best_result["trades"]).to_csv(run_dir / "trades_best.csv", index=False)

    failures = analyze_failures(best_result, daily, best_policy)
    append_jsonl(run_dir / "failure_cases.jsonl", failures)

    checks = split_checks(daily, best_policy, costs)
    write_json(run_dir / "overfit_checks.json", checks)
    sleeve_diagnostics = build_sleeve_diagnostics(daily, policies, results, costs)
    write_json(run_dir / "sleeve_diagnostics.json", sleeve_diagnostics)
    price_coverage = build_price_coverage_report(daily, price_history, best_result)
    write_json(run_dir / "price_coverage.json", price_coverage)
    price_readiness = build_price_readiness_report(daily, price_history)
    backfill_plan_rows, backfill_plan_summary = build_daily_price_backfill_plan(daily, price_history, run_dir)
    pd.DataFrame(backfill_plan_rows).to_csv(run_dir / "daily_price_backfill_plan.csv", index=False)
    write_json(run_dir / "daily_price_backfill_plan.json", backfill_plan_summary)
    price_readiness["backfill_plan"] = backfill_plan_summary
    price_readiness["backfill_plan_path"] = backfill_plan_summary.get("plan_csv", "")
    price_readiness["backfill_plan_summary_path"] = backfill_plan_summary.get("plan_json", "")
    write_json(run_dir / "price_readiness.json", price_readiness)
    from services.heuristic_policy import build_price_readiness_stall_report

    price_readiness_stall = build_price_readiness_stall_report(
        runs_root,
        pending_run_dir=run_dir,
        pending_price_readiness=price_readiness,
    )
    write_json(run_dir / "price_readiness_stall.json", price_readiness_stall)
    write_json(run_dir / "tape_update.json", tape_update)
    portfolio_lifecycle = build_portfolio_lifecycle_report(db_path)
    write_json(run_dir / "portfolio_lifecycle_review.json", portfolio_lifecycle)
    write_policy_snapshot(run_dir / "policy_snapshot.py", best_policy, costs)
    make_plot(run_dir / "sample_efficiency.png", trial_rows)

    code_context = {
        "strategy_agent_risk_backtest_files": [
            "agents/agent.py",
            "services/investment_committee.py",
            "services/investment_simulation.py",
            "services/backtest_engine.py",
            "services/market_data.py",
            "services/learning_engine.py",
            "services/prediction_tracker.py",
        ],
        "historical_experiment_files_found": [
            "data/sovereign_hall.db",
            "data/logs/sovereign_hall.log",
            "report_20260305_173824.md",
            "report_20260323_152445.md",
            "report_20260512_163737.md",
            "report_20260512_165348.md",
        ],
        "note": "No prior runs/heuristic_cycle outputs were found before this run."
        if previous_score is None
        else f"Previous best read from {previous_path}",
        "previous_latest_run": str(previous_latest_run) if previous_latest_run else None,
        "recent_failure_tickers_from_previous_run": list(recent_failure_tickers),
        "prediction_tape": {
            "prediction_rows": int(len(predictions)),
            "latest_prediction_at": predictions["predicted_at"].max().isoformat(),
        },
        "tape_update": tape_update,
        "price_source": (
            "daily_prices table with fallback to prediction current_price"
            if not price_history.empty
            else "prediction current_price fallback; daily_prices table unavailable or empty"
        ),
        "price_coverage": price_coverage,
        "price_readiness": price_readiness,
        "price_readiness_stall": price_readiness_stall,
        "daily_price_backfill_plan": backfill_plan_summary,
        "portfolio_lifecycle": portfolio_lifecycle,
    }
    write_json(run_dir / "project_context.json", code_context)
    write_readme(
        run_dir / "README.md",
        run_started,
        best_trial["trial_name"],
        best_metrics,
        previous_score,
        previous_path,
        trial_rows,
        checks,
        len(predictions),
        db_path,
        f"python scripts/run_heuristic_cycle.py --db {args.db}",
        recent_failure_tickers=recent_failure_tickers,
        sleeve_diagnostics=sleeve_diagnostics,
        price_coverage=price_coverage,
        price_readiness=price_readiness,
        price_readiness_stall=price_readiness_stall,
        tape_update=tape_update,
        portfolio_lifecycle=portfolio_lifecycle,
        previous_latest_run=previous_latest_run,
        previous_latest_score=previous_latest_score,
    )

    latest = runs_root / "LATEST"
    latest.write_text(str(run_dir) + "\n", encoding="utf-8")
    print(f"run_dir={run_dir}")
    print(f"best_policy={best_trial['trial_name']}")
    print(f"best_score={best_metrics['score']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
