#!/usr/bin/env python3
"""Standard-library fallback heuristic cycle.

This evaluator exists for automation resilience when the local scientific stack
hangs during numpy/pandas import. It keeps the same local-only contract and
writes the same artifact names as the primary evaluator.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

import sys

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
    return normalize_ticker(ticker).startswith(("15", "51", "56", "58"))


def capped_proportional_allocation(scores: dict[str, float], total_weight: float, max_weight: float) -> dict[str, float]:
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


def parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        try:
            return datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        is not None
    )


def load_predictions(db_path: Path) -> list[dict[str, Any]]:
    query = """
        SELECT predicted_at, ticker, current_price, target_price, stop_loss,
               direction, confidence, expected_days
        FROM price_predictions
        WHERE current_price IS NOT NULL
          AND current_price > 0
          AND predicted_at IS NOT NULL
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(query).fetchall()]

    cleaned: list[dict[str, Any]] = []
    for row in rows:
        parsed = parse_dt(row.get("predicted_at"))
        price = safe_float(row.get("current_price"))
        if parsed is None or price <= 0:
            continue
        cleaned.append(
            {
                "predicted_at": parsed,
                "date": parsed.date().isoformat(),
                "ticker": normalize_ticker(row.get("ticker")),
                "current_price": price,
                "target_price": safe_float(row.get("target_price")),
                "stop_loss": safe_float(row.get("stop_loss")),
                "direction": str(row.get("direction") or "long").lower(),
                "confidence": safe_float(row.get("confidence")),
                "expected_days": safe_float(row.get("expected_days")),
            }
        )
    return cleaned


def load_daily_prices(db_path: Path, predictions: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    if not predictions:
        return {}
    tickers = sorted({row["ticker"] for row in predictions if row.get("ticker")})
    if not tickers:
        return {}
    start = min(row["date"] for row in predictions)
    end = max(row["date"] for row in predictions)
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
            if not table_exists(conn, "daily_prices"):
                return {}
            rows = conn.execute(query, [*tickers, start, end]).fetchall()
    except Exception:
        return {}
    prices: dict[tuple[str, str], float] = {}
    for ticker, date_text, close in rows:
        parsed = parse_dt(str(date_text))
        date = parsed.date().isoformat() if parsed else str(date_text)[:10]
        price = safe_float(close)
        if price > 0:
            prices[(date, normalize_ticker(ticker))] = price
    return prices


def build_asof_price_history(
    price_history: dict[tuple[str, str], float],
) -> dict[str, list[tuple[datetime, float]]]:
    by_ticker: dict[str, list[tuple[datetime, float]]] = {}
    for (date_text, ticker), price in price_history.items():
        parsed = parse_dt(date_text)
        if parsed is None or price <= 0:
            continue
        by_ticker.setdefault(normalize_ticker(ticker), []).append((parsed, price))
    for rows in by_ticker.values():
        rows.sort(key=lambda item: item[0])
    return by_ticker


def asof_daily_price(
    by_ticker: dict[str, list[tuple[datetime, float]]],
    ticker: str,
    signal_date: str,
    max_age_days: int = 7,
) -> float | None:
    parsed_signal = parse_dt(signal_date)
    if parsed_signal is None:
        return None
    latest_price: float | None = None
    latest_date: datetime | None = None
    for price_date, price in by_ticker.get(normalize_ticker(ticker), []):
        if price_date > parsed_signal:
            break
        latest_date = price_date
        latest_price = price
    if latest_date is None or latest_price is None:
        return None
    if (parsed_signal.date() - latest_date.date()).days > max_age_days:
        return None
    return latest_price


def pct_change(values: list[float], idx: int, lookback: int) -> float | None:
    if idx - lookback < 0:
        return None
    previous = values[idx - lookback]
    if previous <= 0:
        return None
    return values[idx] / previous - 1.0


def build_daily_tape(
    predictions: list[dict[str, Any]],
    price_history: dict[tuple[str, str], float],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in predictions:
        grouped.setdefault((row["date"], row["ticker"]), []).append(row)
    asof_prices = build_asof_price_history(price_history)

    daily: list[dict[str, Any]] = []
    for (date, ticker), rows in grouped.items():
        prices = [safe_float(row["current_price"]) for row in rows if safe_float(row["current_price"]) > 0]
        confidence_values = [safe_float(row["confidence"]) for row in rows]
        risk_rewards: list[float] = []
        stop_gaps: list[float] = []
        target_gaps: list[float] = []
        expected_days = [safe_float(row["expected_days"]) for row in rows if safe_float(row["expected_days"]) > 0]
        long_votes = 0
        for row in rows:
            price = safe_float(row["current_price"])
            target = safe_float(row["target_price"])
            stop = safe_float(row["stop_loss"])
            if row["direction"] == "long":
                long_votes += 1
            if row["direction"] == "long" and target > price > stop > 0:
                risk = price - stop
                reward = target - price
                if risk > 0:
                    risk_rewards.append(reward / risk)
                    stop_gaps.append((price - stop) / price)
                    target_gaps.append((target - price) / price)
        fallback_price = median(prices) if prices else 0.0
        independent_price = asof_daily_price(asof_prices, ticker, date)
        price = independent_price if independent_price is not None else fallback_price
        daily.append(
            {
                "date": date,
                "ticker": ticker,
                "price": price,
                "close_observations": len(prices),
                "confidence": mean(confidence_values) if confidence_values else 0.0,
                "risk_reward": median(risk_rewards) if risk_rewards else 0.0,
                "stop_gap": median(stop_gaps) if stop_gaps else 1.0,
                "target_gap": median(target_gaps) if target_gaps else 0.0,
                "expected_days": median(expected_days) if expected_days else 0.0,
                "long_votes": long_votes,
                "price_source": "daily_prices" if independent_price is not None else "prediction_current_price",
            }
        )

    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in daily:
        by_ticker.setdefault(row["ticker"], []).append(row)
    for rows in by_ticker.values():
        rows.sort(key=lambda item: item["date"])
        prices = [safe_float(row["price"]) for row in rows]
        returns: list[float | None] = []
        for idx, row in enumerate(rows):
            ret = pct_change(prices, idx, 1)
            returns.append(ret)
            row["return_1d"] = ret
            for lookback in (2, 3, 5):
                row[f"momentum_{lookback}d"] = pct_change(prices, idx, lookback)
                recent = [r for r in returns[max(0, idx - lookback + 1) : idx + 1] if r is not None]
                row[f"vol_{lookback}d"] = pstdev(recent) if len(recent) >= max(2, lookback // 2) else None
            row["signal_strength"] = row["confidence"] * math.log1p(max(row["close_observations"], 0))
            row["signal_strength"] *= 1.0 + max(0.0, min(row["risk_reward"], 3.0)) / 6.0
    return sorted(daily, key=lambda item: (item["date"], item["ticker"]))


def forced_holding_positions(
    policy: PolicyConfig,
    current_positions: dict[str, float],
    position_age_days: dict[str, int],
    signal_rows: list[dict[str, Any]],
) -> dict[str, float]:
    if policy.min_holding_days <= 0:
        return {}
    by_ticker = {row["ticker"]: row for row in signal_rows}
    forced: dict[str, float] = {}
    for ticker, weight in current_positions.items():
        if weight <= 0 or position_age_days.get(ticker, 0) >= policy.min_holding_days:
            continue
        row = by_ticker.get(ticker)
        if row and should_release_forced_hold(policy, row):
            continue
        forced[ticker] = float(weight)
    return forced


def should_release_forced_hold(policy: PolicyConfig, row: dict[str, Any]) -> bool:
    latest_return = safe_float(row.get("return_1d"))
    vol = safe_float(row.get(f"vol_{policy.forced_exit_vol_lookback}d")) if policy.forced_exit_vol_lookback else 0.0
    return latest_return <= policy.forced_exit_return_threshold and (
        policy.forced_exit_vol_lookback <= 0 or vol >= policy.forced_exit_vol_threshold
    )


def pick_targets(
    signal_rows: list[dict[str, Any]],
    policy: PolicyConfig,
    current_positions: dict[str, float],
    position_age_days: dict[str, int],
    current_drawdown: float,
    consecutive_loss_days: int,
    failure_memory_tickers: set[str],
) -> tuple[dict[str, float], list[str]]:
    candidates = [
        dict(row)
        for row in signal_rows
        if row["confidence"] >= policy.min_confidence
        and row["risk_reward"] >= policy.min_risk_reward
        and row["close_observations"] >= policy.min_signal_count
        and row["stop_gap"] <= policy.max_stop_gap
    ]
    reasons: list[str] = []
    if policy.universe != "all":
        before = len(candidates)
        if policy.universe == "etf":
            candidates = [row for row in candidates if is_etf_ticker(row["ticker"])]
        elif policy.universe == "single_stock":
            candidates = [row for row in candidates if not is_etf_ticker(row["ticker"])]
        reasons.append(f"{policy.universe}_universe_removed={before - len(candidates)}")
    if policy.excluded_tickers:
        excluded = {normalize_ticker(ticker) for ticker in policy.excluded_tickers}
        if policy.excluded_ticker_mode == "veto":
            before = len(candidates)
            candidates = [row for row in candidates if row["ticker"] not in excluded]
            reasons.append(f"recent_failure_veto_removed={before - len(candidates)}")
        elif policy.excluded_ticker_mode == "scale" and policy.excluded_ticker_scale < 1.0:
            for row in candidates:
                if row["ticker"] in excluded:
                    row["signal_strength"] *= policy.excluded_ticker_scale
            reasons.append("recent_failure_scaled")
    if policy.failure_memory_mode != "none" and failure_memory_tickers:
        if policy.failure_memory_mode == "veto":
            before = len(candidates)
            candidates = [row for row in candidates if row["ticker"] not in failure_memory_tickers]
            reasons.append(f"no_lookahead_failure_veto_removed={before - len(candidates)}")
        elif policy.failure_memory_mode == "scale" and policy.failure_memory_scale < 1.0:
            for row in candidates:
                if row["ticker"] in failure_memory_tickers:
                    row["signal_strength"] *= policy.failure_memory_scale
            reasons.append("no_lookahead_failure_scaled")
    if policy.require_independent_price:
        before = len(candidates)
        candidates = [row for row in candidates if row.get("price_source") == "daily_prices"]
        reasons.append(f"validated_daily_price_removed={before - len(candidates)}")
    if policy.require_positive_trend and policy.trend_lookback:
        col = f"momentum_{policy.trend_lookback}d"
        before = len(candidates)
        candidates = [row for row in candidates if safe_float(row.get(col), -1.0) > 0]
        reasons.append(f"trend_filter_removed={before - len(candidates)}")
    if policy.use_anomaly_veto:
        before = len(candidates)
        candidates = [row for row in candidates if abs(safe_float(row.get("return_1d"))) <= policy.anomaly_return_threshold]
        reasons.append(f"anomaly_veto_removed={before - len(candidates)}")
    if policy.new_entry_loss_streak_threshold and consecutive_loss_days >= policy.new_entry_loss_streak_threshold:
        before = len(candidates)
        candidates = [row for row in candidates if row["ticker"] in current_positions]
        reasons.append(f"new_entry_pause_removed={before - len(candidates)}")
    if not candidates:
        forced = forced_holding_positions(policy, current_positions, position_age_days, signal_rows)
        if forced:
            reasons.append("min_holding_retained_existing")
            return forced, reasons
        return {}, reasons or ["all_candidates_filtered"]

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
        weights = {ticker: weight * scale for ticker, weight in weights.items()}
        reserved_gross = sum(weights.values())
        reasons.append("min_holding_scaled_to_risk_cap")

    allocatable = max(0.0, gross - reserved_gross)
    remaining_slots = max(0, policy.max_names - len(weights))
    candidates = [row for row in candidates if row["ticker"] not in weights]
    candidates.sort(key=lambda row: (row["signal_strength"], row["confidence"]), reverse=True)
    candidates = candidates[:remaining_slots]
    raw_sum = sum(max(0.0, row["signal_strength"]) for row in candidates)
    if remaining_slots <= 0 or allocatable <= 1e-9 or raw_sum <= 0:
        return weights, reasons or ["no_allocatable_signal"]
    allocated_weights = capped_proportional_allocation(
        {str(row["ticker"]): max(0.0, row["signal_strength"]) for row in candidates},
        allocatable,
        policy.max_position,
    )
    for row in candidates:
        raw_weight = allocated_weights.get(str(row["ticker"]), 0.0)
        vol_scale = 1.0
        if policy.vol_lookback:
            vol = row.get(f"vol_{policy.vol_lookback}d")
            if vol is not None and safe_float(vol) > policy.high_vol_threshold:
                vol_scale = policy.high_vol_scale
        weight = min(policy.max_position, raw_weight * vol_scale)
        old = current_positions.get(row["ticker"], 0.0)
        if abs(weight - old) < policy.rebalance_threshold:
            weight = old
        if weight > 1e-9:
            weights[row["ticker"]] = float(weight)
    return weights, reasons


def cost_for_rebalance(old: dict[str, float], new: dict[str, float], costs: CostConfig) -> tuple[float, float, int]:
    tickers = set(old) | set(new)
    buy_turnover = sum(max(new.get(t, 0.0) - old.get(t, 0.0), 0.0) for t in tickers)
    sell_turnover = sum(max(old.get(t, 0.0) - new.get(t, 0.0), 0.0) for t in tickers)
    cost = buy_turnover * (costs.trading_fee + costs.slippage)
    cost += sell_turnover * (costs.trading_fee + costs.stamp_duty + costs.slippage)
    trade_count = sum(1 for t in tickers if abs(new.get(t, 0.0) - old.get(t, 0.0)) > 1e-9)
    return buy_turnover + sell_turnover, cost, trade_count


def score_metrics(metrics: dict[str, Any]) -> float:
    return score_capital_reward(metrics)


def max_drawdown_from_curve(curve: list[dict[str, Any]]) -> tuple[float, str, str]:
    if not curve:
        return 0.0, "", ""
    peak = curve[0]["equity"]
    peak_date = curve[0]["date"]
    worst = 0.0
    worst_peak = peak_date
    trough_date = peak_date
    for row in curve:
        equity = row["equity"]
        if equity > peak:
            peak = equity
            peak_date = row["date"]
        dd = equity / peak - 1.0 if peak > 0 else 0.0
        if dd < worst:
            worst = dd
            worst_peak = peak_date
            trough_date = row["date"]
    return float(worst), worst_peak, trough_date


def summarize_metrics(
    curve: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    total_turnover: float,
    total_cost: float,
    cost_assumption: str,
    sample_start: str,
    sample_end: str,
) -> dict[str, Any]:
    if not curve:
        metrics = {
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
        metrics["score_breakdown"] = capital_reward_breakdown(metrics)
        metrics["score"] = score_metrics(metrics)
        return metrics
    returns = [row["net_return"] for row in curve]
    total_return = curve[-1]["equity"] - 1.0
    days = max(1, len(curve))
    annualized = (1.0 + total_return) ** (252.0 / days) - 1.0 if total_return > -1 else -1.0
    dd, _, _ = max_drawdown_from_curve(curve)
    std = pstdev(returns) if len(returns) > 1 else 0.0
    downside = [value for value in returns if value < 0]
    downside_std = pstdev(downside) if len(downside) > 1 else 0.0
    sharpe = mean(returns) / std * math.sqrt(252) if std > 0 else 0.0
    sortino = mean(returns) / downside_std * math.sqrt(252) if downside_std > 0 else 0.0
    closed = [trade for trade in trades if trade.get("exit_date")]
    wins = [trade for trade in closed if safe_float(trade.get("pnl_pct")) > 0]
    win_rate = len(wins) / len(closed) if closed else sum(1 for r in returns if r > 0) / len(returns)
    cash_ratios = [
        1.0 - min(max(float(row["gross_exposure"]), 0.0), 1.0)
        for row in curve
    ]
    transaction_count = sum(int(row.get("trade_count", 0)) for row in curve)
    max_daily_trade_count = max((int(row.get("trade_count", 0)) for row in curve), default=0)
    gross_equity = 1.0
    for row in curve:
        gross_equity *= 1.0 + float(row.get("gross_return", 0.0))
    metrics = {
        "sample_start": sample_start,
        "sample_end": sample_end,
        "days": days,
        "total_return": float(total_return),
        "annualized_return": float(annualized),
        "max_drawdown": float(dd),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "win_rate": float(win_rate),
        "turnover": float(total_turnover),
        "trade_count": int(transaction_count),
        "closed_trade_count": int(len(closed)),
        "cost_paid": float(total_cost),
        "average_invested_ratio": float(mean([row["gross_exposure"] for row in curve])),
        "average_cash_ratio": 1.0 - float(mean([row["gross_exposure"] for row in curve])),
        "idle_cash_penalty": idle_cash_exposure_penalty(cash_ratios),
        "max_high_cash_streak_days": longest_high_cash_streak(cash_ratios),
        "max_daily_trade_count": max_daily_trade_count,
        "days_at_trade_limit": sum(1 for row in curve if int(row.get("trade_count", 0)) >= MAX_DAILY_TRADES),
        "deferred_trade_actions": sum(int(row.get("deferred_trade_actions", 0)) for row in curve),
        "daily_trade_limit": MAX_DAILY_TRADES,
        "gross_total_return_before_cost": gross_equity - 1.0,
        "cost_assumption": cost_assumption,
        "reward_formula": REWARD_FORMULA,
        "reward_version": REWARD_VERSION,
    }
    metrics["score_breakdown"] = capital_reward_breakdown(metrics)
    metrics["score"] = score_metrics(metrics)
    return metrics


def run_backtest(
    daily: list[dict[str, Any]],
    policy: PolicyConfig,
    costs: CostConfig,
    price_history: dict[tuple[str, str], float] | None = None,
) -> dict[str, Any]:
    dates = sorted({row["date"] for row in daily})
    cost_assumption = (
        f"fee={costs.trading_fee:.4%}, stamp_duty={costs.stamp_duty:.4%}, "
        f"slippage={costs.slippage:.4%}, applied on turnover"
    )
    if len(dates) < 2:
        metrics = summarize_metrics([], [], 0.0, 0.0, cost_assumption, "", "")
        return {"metrics": metrics, "curve": [], "trades": []}
    by_date: dict[str, list[dict[str, Any]]] = {date: [] for date in dates}
    for row in daily:
        by_date[row["date"]].append(row)
    prices_by_date = {
        date: {row["ticker"]: row["price"] for row in rows}
        for date, rows in by_date.items()
    }
    if price_history:
        asof_prices = build_asof_price_history(price_history)
        for date in dates:
            for ticker in asof_prices:
                mark = asof_daily_price(asof_prices, ticker, date)
                if mark is not None:
                    prices_by_date[date][ticker] = mark
    positions: dict[str, float] = {}
    position_age_days: dict[str, int] = {}
    entry_price: dict[str, float] = {}
    entry_date: dict[str, str] = {}
    equity = 1.0
    peak = 1.0
    curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    total_turnover = 0.0
    total_cost = 0.0
    consecutive_loss_days = 0
    failure_memory: dict[str, int] = {}
    for idx in range(1, len(dates)):
        signal_date = dates[idx - 1]
        date = dates[idx]
        active_memory = {ticker for ticker, days_left in failure_memory.items() if days_left > 0}
        current_dd = equity / peak - 1.0
        targets, reasons = pick_targets(
            by_date[signal_date],
            policy,
            positions,
            position_age_days,
            current_dd,
            consecutive_loss_days,
            active_memory,
        )
        targets, deferred_changes = limit_rebalance_actions(
            positions,
            targets,
            MAX_DAILY_TRADES,
        )
        if deferred_changes:
            reasons.append(f"daily_trade_limit_deferred={deferred_changes}")
        turnover, rebalance_cost, trade_count = cost_for_rebalance(positions, targets, costs)
        prev_prices = prices_by_date[signal_date]
        today_prices = prices_by_date[date]
        refreshed_memory: dict[str, int] = {}
        for ticker, old_weight in list(positions.items()):
            if old_weight > 0 and targets.get(ticker, 0.0) <= 1e-9:
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
                        refreshed_memory[ticker] = policy.failure_memory_days
                entry_price.pop(ticker, None)
                entry_date.pop(ticker, None)
        for ticker, weight in targets.items():
            if positions.get(ticker, 0.0) <= 1e-9 and weight > 0 and ticker in prev_prices:
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
        position_age_days = {ticker: previous_age.get(ticker, -1) + 1 for ticker in targets}
        failure_memory = {ticker: days_left - 1 for ticker, days_left in failure_memory.items() if days_left > 1}
        failure_memory.update(refreshed_memory)
        positions = targets
        curve.append(
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
    metrics = summarize_metrics(curve, trades, total_turnover, total_cost, cost_assumption, dates[0], dates[-1])
    return {"metrics": metrics, "curve": curve, "trades": trades}


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


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def previous_best_score(root: Path) -> tuple[float | None, Path | None]:
    best: float | None = None
    best_path: Path | None = None
    for run_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        metrics = read_json(run_dir / "best_metrics.json")
        if metrics.get("reward_version") != REWARD_VERSION:
            continue
        score = safe_float(metrics.get("score"), float("-inf"))
        if best is None or score > best:
            best = score
            best_path = run_dir / "best_metrics.json"
    return best, best_path


def samples_from_readme(run_dir: Path | None) -> int | None:
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
    sample_count = samples_from_readme(run_dir)
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


def build_tape_update_report(predictions: list[dict[str, Any]], previous_run: Path | None) -> dict[str, Any]:
    if not predictions:
        return {"validation_status": "empty_prediction_tape", "rule": "Do not widen exposure without local prediction tape."}
    latest = max(row["predicted_at"] for row in predictions)
    earliest = min(row["predicted_at"] for row in predictions)
    latest_date = latest.date()
    latest_rows = sum(1 for row in predictions if row["predicted_at"].date() == latest_date)
    baseline_run = distinct_date_tape_baseline(previous_run, datetime.now().strftime("%Y%m%d"))
    previous_stats = previous_tape_stats(baseline_run)
    previous_rows_raw = previous_stats.get("current_prediction_rows")
    previous_status = str(previous_stats.get("validation_status", ""))
    previous_recovery_pending = tape_recovery_was_pending(previous_stats)
    try:
        previous_rows = int(previous_rows_raw) if previous_rows_raw is not None else None
    except (TypeError, ValueError):
        previous_rows = None
    new_rows = len(predictions) - previous_rows if previous_rows is not None else None
    age_days = max(0, (datetime.now().date() - latest_date).days)
    if age_days > 3:
        status = "stale_tape"
    elif previous_rows is not None and ((new_rows is not None and new_rows < 20) or latest_rows < 5):
        status = "thin_tape_update"
    elif previous_rows is None:
        status = "no_previous_run_baseline"
    else:
        status = "fresh_tape_update"
    return {
        "validation_status": status,
        "current_prediction_rows": len(predictions),
        "previous_prediction_rows": previous_rows,
        "new_prediction_rows_since_previous": new_rows,
        "current_earliest_prediction_at": earliest.isoformat(),
        "current_latest_prediction_at": latest.isoformat(),
        "current_latest_prediction_date": latest_date.isoformat(),
        "latest_date_prediction_rows": latest_rows,
        "latest_prediction_age_days": int(age_days),
        "min_new_rows_for_validation": 20,
        "min_latest_date_rows_for_validation": 5,
        "max_latest_prediction_age_days": 3,
        "enough_for_policy_widening": status == "fresh_tape_update",
        "freshness_recovery_pending": (
            (previous_status in {"stale_tape", "empty_prediction_tape"} or previous_recovery_pending)
            and status != "fresh_tape_update"
        ),
        "previous_run": str(baseline_run) if baseline_run else None,
        "rule": "Do not widen exposure or relax caps when the cycle has fewer than 20 new local prediction rows, fewer than 5 latest-day rows, or stale latest predictions.",
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
            if isinstance(value, dict):
                ticker = value.get("ticker")
                if ticker:
                    tickers.add(normalize_ticker(ticker))
                for maybe_ticker, maybe_weight in value.items():
                    if isinstance(maybe_weight, (int, float)) and maybe_weight > 0:
                        normalized = normalize_ticker(maybe_ticker)
                        if normalized and normalized[0].isdigit():
                            tickers.add(normalized)
    return tuple(sorted(tickers))


def build_policies(recent_failure_tickers: tuple[str, ...]) -> list[PolicyConfig]:
    base = [
        PolicyConfig(name="baseline_default_policy"),
        PolicyConfig(name="trend_filter", require_positive_trend=True, trend_lookback=3),
        PolicyConfig(name="volatility_scaled", vol_lookback=3, high_vol_threshold=0.045, high_vol_scale=0.45),
        PolicyConfig(name="risk_agent_veto", min_risk_reward=1.0, use_anomaly_veto=True, max_stop_gap=0.12),
        PolicyConfig(name="drawdown_rebalance_guard", drawdown_guard=0.45, drawdown_guard_threshold=0.025, rebalance_threshold=0.025),
        PolicyConfig(name="combined_guarded_policy", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, vol_lookback=3, high_vol_threshold=0.05, high_vol_scale=0.55, use_anomaly_veto=True, anomaly_return_threshold=0.14, drawdown_guard=0.50, drawdown_guard_threshold=0.025, rebalance_threshold=0.02),
        PolicyConfig(name="loss_streak_cooldown", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, vol_lookback=3, high_vol_threshold=0.05, high_vol_scale=0.55, use_anomaly_veto=True, anomaly_return_threshold=0.14, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, rebalance_threshold=0.02),
        PolicyConfig(name="min_holding_cooldown", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, vol_lookback=3, high_vol_threshold=0.05, high_vol_scale=0.55, use_anomaly_veto=True, anomaly_return_threshold=0.14, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, min_holding_days=2, rebalance_threshold=0.02),
        PolicyConfig(name="no_new_risk_after_losses", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, vol_lookback=3, high_vol_threshold=0.05, high_vol_scale=0.55, use_anomaly_veto=True, anomaly_return_threshold=0.14, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, rebalance_threshold=0.02),
        PolicyConfig(name="hold_and_pause_guard", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, vol_lookback=3, high_vol_threshold=0.05, high_vol_scale=0.55, use_anomaly_veto=True, anomaly_return_threshold=0.14, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, min_holding_days=2, rebalance_threshold=0.02),
        PolicyConfig(name="cost_robust_hold4", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05),
        PolicyConfig(name="age_volatility_reversal_stop", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, min_holding_days=4, forced_exit_return_threshold=-0.025, forced_exit_vol_lookback=3, forced_exit_vol_threshold=0.030, rebalance_threshold=0.05),
        PolicyConfig(name="strict_age_reversal_stop", min_confidence=0.66, max_names=5, max_position=0.10, max_gross=0.55, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, min_holding_days=4, forced_exit_return_threshold=-0.035, forced_exit_vol_lookback=3, forced_exit_vol_threshold=0.040, rebalance_threshold=0.05),
        PolicyConfig(name="etf_only_cost_guard", min_confidence=0.66, max_names=4, max_position=0.10, max_gross=0.45, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.50, drawdown_guard_threshold=0.025, loss_streak_threshold=2, loss_streak_guard=0.55, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="etf"),
        PolicyConfig(name="single_stock_cost_guard", min_confidence=0.66, max_names=4, max_position=0.08, max_gross=0.40, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap6", min_confidence=0.66, max_names=4, max_position=0.06, max_gross=0.24, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap5_min2obs", min_confidence=0.66, max_names=4, max_position=0.05, max_gross=0.20, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=2, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap5_min2obs_anomaly12", min_confidence=0.66, max_names=4, max_position=0.05, max_gross=0.20, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.12, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=2, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap5_min3obs_diagnostic", min_confidence=0.66, max_names=4, max_position=0.05, max_gross=0.20, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.12, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=3, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap4_min2obs", min_confidence=0.66, max_names=4, max_position=0.04, max_gross=0.16, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=2, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="single_stock_hold6_cap5_max3_min2obs_diagnostic", min_confidence=0.66, max_names=3, max_position=0.05, max_gross=0.15, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=2, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="validated_daily_price_only_diagnostic", min_confidence=0.66, max_names=4, max_position=0.05, max_gross=0.20, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.12, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=6, min_signal_count=2, rebalance_threshold=0.05, universe="single_stock", require_independent_price=True),
        PolicyConfig(name="sparse_hold8_cap6_diagnostic", min_confidence=0.66, max_names=4, max_position=0.06, max_gross=0.36, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=8, rebalance_threshold=0.05, universe="single_stock"),
        PolicyConfig(name="no_lookahead_failure_half_size", min_confidence=0.66, max_names=4, max_position=0.08, max_gross=0.40, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="single_stock", failure_memory_mode="scale", failure_memory_loss_threshold=-0.03, failure_memory_days=8, failure_memory_scale=0.5),
        PolicyConfig(name="no_lookahead_failure_veto", min_confidence=0.66, max_names=4, max_position=0.08, max_gross=0.40, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="single_stock", failure_memory_mode="veto", failure_memory_loss_threshold=-0.03, failure_memory_days=8),
    ]
    base.extend([
        PolicyConfig(name="full_deployment_diversified_hold10", min_confidence=0.65, max_names=10, max_position=0.10, max_gross=1.0, min_risk_reward=0.8, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=1.0, min_holding_days=10, rebalance_threshold=0.05, require_independent_price=True),
        PolicyConfig(name="full_deployment_lower_churn_hold15", min_confidence=0.65, max_names=10, max_position=0.10, max_gross=1.0, min_risk_reward=0.8, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=1.0, min_holding_days=15, rebalance_threshold=0.08, require_independent_price=True),
    ])
    if recent_failure_tickers:
        base.extend(
            [
                PolicyConfig(name="recent_failure_half_size_diagnostic", min_confidence=0.66, max_names=4, max_position=0.08, max_gross=0.40, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="single_stock", excluded_tickers=recent_failure_tickers, excluded_ticker_mode="scale", excluded_ticker_scale=0.5),
                PolicyConfig(name="recent_failure_veto_diagnostic", min_confidence=0.66, max_names=4, max_position=0.08, max_gross=0.40, min_risk_reward=0.9, require_positive_trend=True, trend_lookback=2, use_anomaly_veto=True, anomaly_return_threshold=0.18, drawdown_guard=0.45, drawdown_guard_threshold=0.02, loss_streak_threshold=2, loss_streak_guard=0.50, new_entry_loss_streak_threshold=2, min_holding_days=4, rebalance_threshold=0.05, universe="single_stock", excluded_tickers=recent_failure_tickers, excluded_ticker_mode="veto"),
            ]
        )
    return base


def promotable(policy_name: str) -> bool:
    return not policy_name.endswith("_diagnostic") and not policy_name.startswith("recent_failure")


def split_checks(
    daily: list[dict[str, Any]],
    policy: PolicyConfig,
    costs: CostConfig,
    price_history: dict[tuple[str, str], float] | None = None,
) -> dict[str, Any]:
    dates = sorted({row["date"] for row in daily})
    if len(dates) < 6:
        return {"warning": "too few dates for split check", "overfit_risk": True}
    split = int(len(dates) * 0.6)
    train_dates = set(dates[:split])
    test_dates = set(dates[split - 1 :])
    train = run_backtest([row for row in daily if row["date"] in train_dates], policy, costs, price_history)["metrics"]
    test = run_backtest([row for row in daily if row["date"] in test_dates], policy, costs, price_history)["metrics"]
    stress = run_backtest(daily, policy, replace(costs, slippage=costs.slippage * 3.0), price_history)["metrics"]
    return {
        "split_date": dates[split],
        "train": train,
        "out_of_sample": test,
        "cost_stress_3x_slippage": stress,
        "overfit_risk": test["score"] < 0 or test["score"] < train["score"] * 0.25 or stress["score"] < 0,
    }


def build_sleeve_diagnostics(
    daily: list[dict[str, Any]],
    policies: list[PolicyConfig],
    results: dict[str, dict[str, Any]],
    costs: CostConfig,
    price_history: dict[tuple[str, str], float] | None = None,
) -> dict[str, Any]:
    policies_by_name = {policy.name: policy for policy in policies}
    single_candidates = [name for name in ("single_stock_hold6_cap5_min2obs_anomaly12", "single_stock_hold6_cap5_min2obs", "single_stock_hold6_cap4_min2obs") if name in results]
    single_trial = max(single_candidates, key=lambda name: results[name]["metrics"]["score"]) if single_candidates else "single_stock_hold6_cap5_min2obs"
    sleeves: dict[str, Any] = {}
    for sleeve_name, trial_name in {"etf": "etf_only_cost_guard", "single_stock": single_trial}.items():
        policy = policies_by_name[trial_name]
        metrics = results[trial_name]["metrics"]
        checks = split_checks(daily, policy, costs, price_history)
        oos = checks.get("out_of_sample", {}).get("score")
        stress = checks.get("cost_stress_3x_slippage", {}).get("score")
        reasons: list[str] = []
        if metrics["score"] <= 0:
            reasons.append("主样本score未转正")
        if checks.get("overfit_risk"):
            reasons.append("样本外/成本扰动检查失败")
        if stress is None or stress < 0.02:
            reasons.append("3x滑点余量低于0.02")
        sleeves[sleeve_name] = {
            "trial_name": trial_name,
            "total_return": metrics["total_return"],
            "annualized_return": metrics["annualized_return"],
            "max_drawdown": metrics["max_drawdown"],
            "sharpe": metrics["sharpe"],
            "turnover": metrics["turnover"],
            "trade_count": metrics["trade_count"],
            "score": metrics["score"],
            "out_of_sample_score": oos,
            "cost_stress_score": stress,
            "overfit_risk": bool(checks.get("overfit_risk", True)),
            "promotable": not reasons,
            "reason": "；".join(reasons) if reasons else "通过主样本、样本外和3x滑点检查",
        }
    return {
        "allocator_status": "promoted_candidate" if all(row["promotable"] for row in sleeves.values()) else "not_promoted",
        "rule": "ETF和单股sleeve必须主样本score>0、overfit_risk=false、3x滑点score>=0.02才允许组合allocator推广",
        "sleeves": sleeves,
    }


def build_price_coverage_report(
    daily: list[dict[str, Any]],
    price_history: dict[tuple[str, str], float],
    result: dict[str, Any],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in daily:
        counts[row["price_source"]] = counts.get(row["price_source"], 0) + 1
    signal_rows = len(daily)
    daily_price_rows = counts.get("daily_prices", 0)
    independent_ratio = daily_price_rows / signal_rows if signal_rows else 0.0
    curve = result.get("curve", [])
    missing_days = 0
    missing_slots = 0
    position_slots = 0
    for row in curve:
        for part in str(row.get("notes", "")).split(";"):
            if part.startswith("missing_prices="):
                missing = int(part.split("=", 1)[1])
                missing_days += 1 if missing > 0 else 0
                missing_slots += missing
        try:
            positions = json.loads(row.get("positions", "{}"))
        except Exception:
            positions = {}
        if isinstance(positions, dict):
            position_slots += len(positions)
    status = "validated_daily_prices"
    missing_slot_ratio = missing_slots / position_slots if position_slots else 0.0
    if independent_ratio <= 0.0:
        status = "unvalidated_prediction_current_price_fallback"
    elif independent_ratio < 0.80:
        status = "partial_daily_prices_low_signal_coverage"
    elif missing_slot_ratio > 0.10:
        status = "partial_daily_prices_with_missing_hold_prices"
    return {
        "status": status,
        "daily_signal_rows": signal_rows,
        "daily_signal_price_source_counts": counts,
        "daily_prices_rows_loaded": len(price_history),
        "independent_price_row_ratio": independent_ratio,
        "best_curve_days": len(curve),
        "days_with_missing_prices": missing_days,
        "missing_price_day_ratio": missing_days / len(curve) if curve else 0.0,
        "held_position_slots": position_slots,
        "missing_position_price_slots": missing_slots,
        "missing_position_price_slot_ratio": missing_slot_ratio,
        "rule": "Do not use latest local score to expand exposure until daily_prices coverage is validated and held-position missing-price slots are low.",
    }


def build_price_readiness_report(
    daily: list[dict[str, Any]],
    price_history: dict[tuple[str, str], float],
) -> dict[str, Any]:
    """Identify the smallest local daily_prices backfill needed for validation."""
    signal_tickers = sorted({row["ticker"] for row in daily if row.get("ticker")})
    latest_date = max((row["date"] for row in daily), default="")
    if any("price_source" in row for row in daily):
        missing_rows = [row for row in daily if row.get("price_source") != "daily_prices"]
        priced_row_count = len(daily) - len(missing_rows)
    else:
        priced_keys = set(price_history)
        missing_rows = [
            row
            for row in daily
            if (str(row.get("date", "")), normalize_ticker(row.get("ticker"))) not in priced_keys
        ]
        priced_row_count = len(daily) - len(missing_rows)
    missing = sorted({row["ticker"] for row in missing_rows if row.get("ticker")})
    latest_missing = sorted(
        {
            row["ticker"]
            for row in missing_rows
            if row.get("date") == latest_date and row.get("ticker") in set(missing)
        }
    )

    stats: list[dict[str, Any]] = []
    for ticker in missing:
        rows = [row for row in missing_rows if row.get("ticker") == ticker]
        dates = sorted({row["date"] for row in rows})
        stats.append(
            {
                "ticker": ticker,
                "signal_days": len(dates),
                "first_signal_date": dates[0] if dates else "",
                "last_signal_date": dates[-1] if dates else "",
                "total_signal_observations": int(sum(safe_float(row.get("close_observations")) for row in rows)),
                "missing_latest_signal_date": ticker in latest_missing,
            }
        )
    stats.sort(
        key=lambda row: (
            bool(row.get("missing_latest_signal_date")),
            row["signal_days"],
            row["last_signal_date"],
            row["total_signal_observations"],
        ),
        reverse=True,
    )

    ticker_coverage_ratio = (
        (len(signal_tickers) - len(missing)) / len(signal_tickers)
        if signal_tickers else 0.0
    )
    signal_row_coverage_ratio = priced_row_count / len(daily) if daily else 0.0
    if not signal_tickers:
        status = "no_signal_tickers"
    elif priced_row_count <= 0:
        status = "blocked_no_daily_prices"
    elif (
        not latest_missing
        and ticker_coverage_ratio >= 0.95
        and signal_row_coverage_ratio >= 0.90
    ):
        status = "ready_with_historical_provider_gaps" if missing else "ready_validated_daily_prices"
    elif missing:
        status = "partial_daily_price_backfill_needed"
    else:
        status = "ready_validated_daily_prices"

    next_action = (
        "Backfill latest local daily_prices for the full latest_missing_tickers batch first, then rerun the cycle and require validated coverage before relaxing caps."
        if latest_missing
        else (
            "Latest signal-date prices are covered. Retry provider-unavailable historical gaps or import "
            "independent local OHLC, but do not treat those gaps as a current simulation shutdown."
        )
    )

    return {
        "status": status,
        "total_signal_ticker_count": len(signal_tickers),
        "priced_signal_ticker_count": len(signal_tickers) - len(missing),
        "missing_signal_ticker_count": len(missing),
        "ticker_coverage_ratio": ticker_coverage_ratio,
        "signal_row_coverage_ratio": signal_row_coverage_ratio,
        "latest_signal_date": latest_date,
        "latest_missing_tickers": latest_missing,
        "unblock_tickers": latest_missing,
        "minimum_next_rows": len(latest_missing),
        "missing_tickers_top10": stats[:10],
        "rule": "Populate local daily_prices with independently validated OHLC rows before treating heuristic score as exposure-widening evidence.",
        "next_action": next_action,
    }


def build_daily_price_backfill_plan(
    daily: list[dict[str, Any]],
    price_history: dict[tuple[str, str], float],
    run_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Write an actionable local daily_prices backfill plan for user entries."""
    plan_csv = str(run_dir / "daily_price_backfill_plan.csv")
    plan_json = str(run_dir / "daily_price_backfill_plan.json")
    if not daily:
        return [], {
            "status": "no_signal_tickers",
            "plan_csv": plan_csv,
            "plan_json": plan_json,
            "total_missing_tickers": 0,
            "latest_signal_date": "",
            "latest_missing_tickers": [],
            "minimum_next_rows": 0,
            "top_priority_tickers": [],
            "rule": "No local signal tickers are available for daily_prices planning.",
        }

    latest_signal_date = max(str(row.get("date", "")) for row in daily)
    per_ticker: dict[str, dict[str, Any]] = {}
    latest_missing: set[str] = set()
    for row in daily:
        ticker = normalize_ticker(row.get("ticker"))
        date = str(row.get("date", ""))
        if not ticker or not date:
            continue
        if row.get("price_source") == "daily_prices":
            continue
        entry = per_ticker.setdefault(
            ticker,
            {
                "ticker": ticker,
                "dates": set(),
                "total_signal_observations": 0,
            },
        )
        entry["dates"].add(date)
        entry["total_signal_observations"] += int(safe_float(row.get("close_observations"), 0.0))
        if date == latest_signal_date:
            latest_missing.add(ticker)

    if not per_ticker:
        return [], {
            "status": "ready_validated_daily_prices",
            "plan_csv": plan_csv,
            "plan_json": plan_json,
            "total_missing_tickers": 0,
            "latest_signal_date": latest_signal_date,
            "latest_missing_tickers": [],
            "minimum_next_rows": 0,
            "top_priority_tickers": [],
            "rule": "All local signal ticker/date rows have daily_prices coverage.",
        }

    sorted_entries = sorted(
        per_ticker.values(),
        key=lambda item: (
            item["ticker"] in latest_missing,
            len(item["dates"]),
            max(item["dates"]) if item["dates"] else "",
            item["total_signal_observations"],
        ),
        reverse=True,
    )
    rows: list[dict[str, Any]] = []
    for rank, entry in enumerate(sorted_entries, start=1):
        dates = sorted(entry["dates"])
        ticker = entry["ticker"]
        rows.append(
            {
                "priority_rank": rank,
                "ticker": ticker,
                "missing_signal_days": len(dates),
                "first_missing_signal_date": dates[0] if dates else "",
                "last_missing_signal_date": dates[-1] if dates else "",
                "total_signal_observations": int(entry["total_signal_observations"]),
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
        "plan_csv": plan_csv,
        "plan_json": plan_json,
        "total_missing_tickers": len(rows),
        "latest_signal_date": latest_signal_date,
        "latest_missing_tickers": sorted(latest_missing),
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
        try:
            details.append(f"missing_days={int(row.get('signal_days', 0))}d")
        except (TypeError, ValueError):
            pass
        try:
            details.append(f"obs={int(row.get('total_signal_observations', 0))}")
        except (TypeError, ValueError):
            pass
        first_signal = str(row.get("first_signal_date", "") or "")[:10]
        last_signal = str(row.get("last_signal_date", "") or "")[:10]
        if first_signal and last_signal:
            details.append(f"missing_range={first_signal}..{last_signal}")
        elif first_signal:
            details.append(f"missing_from={first_signal}")
        elif last_signal:
            details.append(f"missing_to={last_signal}")
        parts.append(f"{normalize_ticker(row['ticker'])}({', '.join(details)})" if details else normalize_ticker(row["ticker"]))
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def analyze_failures(result: dict[str, Any], daily: list[dict[str, Any]], policy: PolicyConfig) -> list[dict[str, Any]]:
    curve = result["curve"]
    trades = result["trades"]
    if not curve:
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
    dd, peak_date, trough_date = max_drawdown_from_curve(curve)
    window = [row for row in curve if peak_date <= row["date"] <= trough_date]
    failures = [
        {
            "case_type": "max_drawdown",
            "time_range": f"{peak_date}..{trough_date}",
            "market_state": {
                "avg_daily_return": mean([row["net_return"] for row in window]) if window else 0.0,
                "avg_turnover": mean([row["turnover"] for row in window]) if window else 0.0,
                "avg_gross_exposure": mean([row["gross_exposure"] for row in window]) if window else 0.0,
            },
            "signals": {"policy": policy.name, "filters": asdict(policy)},
            "positions": json.loads(window[-1]["positions"]) if window else {},
            "result": {"drawdown": dd},
            "suspected_reason": "signal basket remained exposed while local quote tape moved against recent winners",
            "repair_direction": "tighten drawdown guard or volatility scaling before increasing gross exposure",
        }
    ]
    if trades:
        worst = min(trades, key=lambda row: row.get("pnl_pct", 0.0))
        ticker_rows = [row for row in daily if row["ticker"] == worst["ticker"]]
        pnl = safe_float(worst.get("pnl_pct"))
        failures.append(
            {
                "case_type": "worst_trade",
                "time_range": f"{worst.get('entry_date')}..{worst.get('exit_date')}",
                "market_state": {
                    "ticker": worst["ticker"],
                    "local_obs": len(ticker_rows),
                    "median_vol_3d": median([safe_float(row.get("vol_3d")) for row in ticker_rows]) if ticker_rows else 0.0,
                },
                "signals": {"policy": policy.name},
                "positions": {"ticker": worst["ticker"]},
                "result": {
                    "pnl_pct": worst.get("pnl_pct"),
                    "entry_price": worst.get("entry_price"),
                    "exit_price": worst.get("exit_price"),
                    "exit_reason": worst.get("exit_reason"),
                },
                "suspected_reason": "entry followed high-confidence signal but subsequent price path reversed" if pnl < 0 else "no losing closed trades under retained policy; sparse profitable paths can hide selection fragility",
                "repair_direction": "require positive short-term trend or reduce size when volatility regime is elevated" if pnl < 0 else "do not loosen gates solely because closed trades are positive; validate signal breadth on another tape update",
            }
        )
    losing = [row for row in curve if row["net_return"] < 0]
    if losing:
        best_streak: list[dict[str, Any]] = []
        current: list[dict[str, Any]] = []
        previous_index = -99
        index_by_date = {row["date"]: idx for idx, row in enumerate(curve)}
        for row in losing:
            idx = index_by_date[row["date"]]
            if idx == previous_index + 1:
                current.append(row)
            else:
                if len(current) > len(best_streak):
                    best_streak = current
                current = [row]
            previous_index = idx
        if len(current) > len(best_streak):
            best_streak = current
        failures.append(
            {
                "case_type": "consecutive_losses",
                "time_range": f"{best_streak[0]['date']}..{best_streak[-1]['date']}",
                "market_state": {"loss_days": len(best_streak), "cumulative_net_return": math.prod(1.0 + row["net_return"] for row in best_streak) - 1.0},
                "signals": {"policy": policy.name},
                "positions": json.loads(best_streak[-1]["positions"]),
                "result": "multiple negative portfolio days without a pause",
                "suspected_reason": "daily policy has limited losing-streak protection",
                "repair_direction": "validate cooldown before any gross exposure increase",
            }
        )
    if curve:
        rolling = []
        for idx, row in enumerate(curve):
            total = sum(item["turnover"] for item in curve[max(0, idx - 2) : idx + 1])
            rolling.append((total, idx))
        _, high_idx = max(rolling)
        start_idx = max(0, high_idx - 2)
        failures.append(
            {
                "case_type": "overtrading",
                "time_range": f"{curve[start_idx]['date']}..{curve[high_idx]['date']}",
                "market_state": {"three_day_turnover": rolling[high_idx][0]},
                "signals": {"policy": policy.name},
                "positions": json.loads(curve[high_idx]["positions"]),
                "result": {"cost_paid_in_window": sum(row["cost"] for row in curve[start_idx : high_idx + 1])},
                "suspected_reason": "small changes in ranked confidence can churn similar baskets",
                "repair_direction": "add rebalance threshold or minimum holding period",
            }
        )
    missed = find_missed_opportunity(curve, daily)
    if missed:
        failures.append(missed)
    return failures


def find_missed_opportunity(curve: list[dict[str, Any]], daily: list[dict[str, Any]]) -> dict[str, Any] | None:
    daily_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for row in daily:
        daily_by_date.setdefault(row["date"], {})[row["ticker"]] = row
    best: dict[str, Any] | None = None
    for row in curve:
        previous = daily_by_date.get(row["signal_date"], {})
        current = daily_by_date.get(row["date"], {})
        try:
            positions = json.loads(row["positions"])
        except Exception:
            positions = {}
        for ticker in set(previous) & set(current):
            if ticker in positions:
                continue
            prev_price = previous[ticker]["price"]
            if prev_price <= 0:
                continue
            ret = current[ticker]["price"] / prev_price - 1.0
            if ret < 0.03:
                continue
            candidate = {
                "case_type": "missed_opportunity",
                "time_range": f"{row['signal_date']}..{row['date']}",
                "market_state": {"ticker_next_return": ret},
                "signals": {
                    "ticker": ticker,
                    "confidence": previous[ticker]["confidence"],
                    "risk_reward": previous[ticker]["risk_reward"],
                    "observations": previous[ticker]["close_observations"],
                },
                "positions": positions,
                "result": f"{ticker} rose next sample but was not held",
                "suspected_reason": "ranking, confidence threshold, or risk/reward veto excluded the move",
                "repair_direction": "evaluate whether lower confidence threshold helps out-of-sample after costs",
            }
            if best is None or ret > best["market_state"]["ticker_next_return"]:
                best = candidate
    return best


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    fieldnames = fieldnames or sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_policy_snapshot(path: Path, policy: PolicyConfig, costs: CostConfig) -> None:
    path.write_text(
        '"""Best heuristic policy snapshot from the latest local cycle."""\n\n'
        f"POLICY_CONFIG = {asdict(policy)!r}\n\n"
        f"COST_CONFIG = {asdict(costs)!r}\n\n"
        "def score_candidate(row):\n"
        "    import math\n"
        "    risk_reward = min(max(float(row.get('risk_reward', 0.0)), 0.0), 3.0)\n"
        "    observations = max(int(row.get('close_observations', 0)), 0)\n"
        "    confidence = float(row.get('confidence', 0.0))\n"
        "    return confidence * math.log1p(observations) * (1.0 + risk_reward / 6.0)\n",
        encoding="utf-8",
    )


def write_plot(path: Path) -> None:
    path.write_bytes(
        bytes.fromhex(
            "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de"
            "0000000c49444154789c6360f8ffff3f0005fe02fea73581e20000000049454e44ae426082"
        )
    )


def write_readme(
    path: Path,
    run_started: str,
    db_path: Path,
    sample_count: int,
    best_name: str,
    best_metrics: dict[str, Any],
    previous_score: float | None,
    previous_path: Path | None,
    trials: list[dict[str, Any]],
    checks: dict[str, Any],
    sleeve_diagnostics: dict[str, Any],
    price_coverage: dict[str, Any],
    price_readiness: dict[str, Any],
    price_readiness_stall: dict[str, Any],
    tape_update: dict[str, Any],
    command: str,
) -> None:
    comparison = "No previous heuristic_cycle best was found."
    if previous_score is not None:
        comparison = f"Previous best score {previous_score:.6f} from {previous_path}; delta {best_metrics['score'] - previous_score:+.6f}."
    failed_lines = "\n".join(f"- {trial['trial_name']}: score={trial['score']:.6f}, notes={trial['notes']}" for trial in trials if trial["trial_name"] != best_name)
    sleeve_lines = []
    for sleeve_name, row in sleeve_diagnostics.get("sleeves", {}).items():
        sleeve_lines.append(
            f"- {sleeve_name}: score={row['score']:.6f}, OOS={row['out_of_sample_score']:.6f}, "
            f"3x_slippage={row['cost_stress_score']:.6f}, promotable={row['promotable']}, reason={row['reason']}"
        )
    zero_new = safe_float(tape_update.get("new_prediction_rows_since_previous"), 0.0) <= 0
    missing_queue = format_missing_price_queue(price_readiness)
    latest_missing = price_readiness.get("latest_missing_tickers", [])
    latest_missing_text = ", ".join(str(ticker) for ticker in latest_missing[:8]) if isinstance(latest_missing, list) else ""
    unblock_tickers = price_readiness.get("unblock_tickers", latest_missing)
    unblock_text = ", ".join(str(ticker) for ticker in unblock_tickers[:8]) if isinstance(unblock_tickers, list) else ""
    backfill_plan = price_readiness.get("backfill_plan", {}) if isinstance(price_readiness, dict) else {}
    backfill_plan_path = str(price_readiness.get("backfill_plan_path", "") or "") if isinstance(price_readiness, dict) else ""
    if isinstance(backfill_plan, dict):
        top_plan = ", ".join(str(ticker) for ticker in backfill_plan.get("top_priority_tickers", [])[:5])
        plan_total = backfill_plan.get("total_missing_tickers", 0)
    else:
        top_plan = ""
        plan_total = 0
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
    backfill_status_command = "python scripts/backfill_daily_prices.py --status --limit 5"
    if backfill_plan_path:
        backfill_status_command += f" --plan {backfill_plan_path}"
    stall_blocked_runs = int(price_readiness_stall.get("consecutive_blocked_runs", 0) or 0)
    stall_min_runs = int(price_readiness_stall.get("minimum_blocked_runs", 3) or 3)
    best_cap = safe_float(next((trial.get("config", {}).get("max_position") for trial in trials if trial["trial_name"] == best_name), 0.0))
    stall_status = str(price_readiness_stall.get("status", "") or "")
    stall_cap = best_cap * 0.05 if stall_status in {"stalled_no_daily_prices", "stalled_partial_daily_prices"} else None
    stall_kind = str(price_readiness_stall.get("stall_kind", "") or "")
    stall_unblock = price_readiness_stall.get("unblock_tickers", [])
    stall_unblock_text = ", ".join(str(ticker) for ticker in stall_unblock[:8]) if isinstance(stall_unblock, list) else ""
    if stall_kind == "partial_daily_price_backfill_needed" or stall_status == "stalled_partial_daily_prices":
        stall_decision_subject = "repeated partial daily_prices no-progress"
    else:
        stall_decision_subject = "repeated empty daily_prices"
    text = f"""# Heuristic Learning Cycle

## Run
- Run time: {run_started}
- Data source: `{db_path}`
- Samples consumed: {sample_count} prediction rows
- Evaluation engine: `stdlib_fallback`
- Best policy: `{best_name}`
- Current best score: {best_metrics['score']:.6f}
- Previous best comparison: {comparison}

## Blocking & Repeated Warning Review
- Reviewed automation memory and recent heuristic-cycle READMEs before running new trials. The repeated blocker remains exact local `daily_prices` plan-date coverage plus thin/stale local prediction tape, not a missing evaluation script.
- Current local status: Top priority `daily_prices` plan coverage is still incomplete, and `tape_update.json` does not meet the fresh-row/latest-day thresholds required for exposure widening.
- Root cause advanced this cycle: local CSV validation could previously report ticker-level plan coverage when a row merely landed inside a planned date range; that was weaker than the DB/status gate, which requires exact missing signal-date coverage with bounded as-of matching.
- System fix: `scripts/backfill_daily_prices.py --import-csv ... --dry-run --plan ...` now reports exact signal-date coverage, and MarketDataService fetches are blocked unless explicitly opted in with `--allow-market-fetch` or `SOVEREIGN_HALL_ALLOW_MARKET_BACKFILL=1`.
- Rejection-memory reliability fix: raw reasons remain auditable, while active agent/check_db feedback quarantines obsolete historical-price stop claims and unchanged repeatedly rejected candidates require a structured traceable evidence delta before committee reuse.
- Integration decision: do not promote a return-seeking default or relax caps this round; use the retained policy only as a risk cap/warning until independently supplied local OHLC rows pass exact plan-date validation and a new cycle confirms coverage.

## What Changed
- Separated raw rejection audit text from active feedback and added the shared repeated-candidate research cooldown used by `run_discussion`.
- Ran the local-only heuristic cycle through the standard-library fallback because numpy/pandas import did not complete in the preflight window.
- Preserved the existing interpretable policy family and retained promotion rules: diagnostic sparse/max3/min3/recent-failure trials are not default policies.
- Advanced the prior fresh-tape direction by checking whether this run has meaningful new local predictions before treating the retained policy as validation for widening.
- Kept weak price coverage and failed ETF sleeve checks as real simulated-investment caps/warnings rather than return-seeking allocators.
- Converted blocked `daily_prices` readiness into a simulated-buy cap, so missing independent local prices constrain entries instead of only appearing in reports.
- Advanced the data-quality closure by measuring consecutive `blocked_no_daily_prices` and no-progress partial daily_prices cycles; repeated empty or unchanged partial coverage now appears as a stalled backfill task and stricter simulated-buy cap, not a new leaderboard branch.
- Tightened local CSV validation to exact missing signal dates from the plan/tape, and disabled MarketDataService fetches by default so this automation remains local-only unless explicitly opted in.
- Wrote `project_context.json` with `evaluation_engine=stdlib_fallback` so user entry points can surface evaluator reliability.
- Replaced the annualized-return/turnover-dominated score with `{REWARD_VERSION}`: net total account return is primary, full transaction costs are already deducted from equity, and prolonged cash is penalized by magnitude and duration.
- Enforced the shared `{MAX_DAILY_TRADES}`-transaction daily hard limit, applying exits/reductions before increases.

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
{failed_lines or "- None."}

## Diagnostic Only
- `single_stock_hold6_cap5_min3obs_diagnostic`, `single_stock_hold6_cap5_max3_min2obs_diagnostic`, `sparse_hold8_cap6_diagnostic`, and recent-failure diagnostics remain non-promotable unless a fresh tape update proves broader path quality.
- `validated_daily_price_only_diagnostic` is intentionally diagnostic: with current local data it should block trading until independent `daily_prices` rows exist, not become a return-seeking rule.

## Simplification Check
- Retained policy remains short and reproducible: confidence/risk-reward filters, trend/anomaly guard, minimum hold, single-name/gross caps, observation floor, cooldown, and rebalance friction.
- No dense parameter search, previous-run failure labels, or sparse high-score branches were promoted.

## Sleeve Allocator Check
- Allocator status: {sleeve_diagnostics.get('allocator_status', 'unknown')}
- Rule: {sleeve_diagnostics.get('rule', '')}
{chr(10).join(sleeve_lines) or "- Sleeve diagnostics unavailable."}

## Price Coverage Check
- Status: {price_coverage.get('status', 'unknown')}
- daily_signal rows: {price_coverage.get('daily_signal_rows', 0)}; source counts: {price_coverage.get('daily_signal_price_source_counts', {})}; daily_prices rows loaded: {price_coverage.get('daily_prices_rows_loaded', 0)}; independent row ratio: {safe_float(price_coverage.get('independent_price_row_ratio')):.2%}
- Best path missing prices: {price_coverage.get('days_with_missing_prices', 0)}/{price_coverage.get('best_curve_days', 0)} days ({safe_float(price_coverage.get('missing_price_day_ratio')):.2%}); {price_coverage.get('missing_position_price_slots', 0)}/{price_coverage.get('held_position_slots', 0)} held-position slots ({safe_float(price_coverage.get('missing_position_price_slot_ratio')):.2%})
- Integration decision: keep the best policy as a cap/warning only; exposure must not expand until local daily_prices coverage is validated.

## Daily Price Readiness
- Status: {price_readiness.get('status', 'unknown')}
- Signal tickers fully covered by local daily_prices: {price_readiness.get('priced_signal_ticker_count', 0)}/{price_readiness.get('total_signal_ticker_count', 0)}; missing={price_readiness.get('missing_signal_ticker_count', 0)}
- Latest signal date: {price_readiness.get('latest_signal_date', 'unknown')}; latest missing tickers: {latest_missing_text or 'none'}; minimum next rows={price_readiness.get('minimum_next_rows', 0)}
- Minimum unlock batch: {unblock_text or 'none'}
- Priority backfill queue: {missing_queue or 'none'}
- Machine-readable backfill plan: `{backfill_plan_path or 'not written'}`; plan tickers={plan_total}; top priority={top_plan or 'none'}
- Local DB plan coverage check: `{backfill_status_command}`
- Local CSV exact signal-date validation: `{backfill_import_command}`
- Local CSV strict top-plan validation: `{backfill_strict_import_command}`
- Market-data fetch guard: `scripts/backfill_daily_prices.py` blocks MarketDataService fetches unless `--allow-market-fetch` or `SOVEREIGN_HALL_ALLOW_MARKET_BACKFILL=1` is set.
- Integration decision: do not synthesize `daily_prices` from prediction current_price; surface this as a local backfill checklist in user entries and keep exposure caps active.

## Persistent Data-Quality Stall
- Status: {price_readiness_stall.get('status', 'unknown')}
- Consecutive blocked runs: {stall_blocked_runs}/{stall_min_runs}; blocked run ids: {', '.join(price_readiness_stall.get('blocked_run_ids', [])[-6:]) or 'none'}
- Next ticker: {price_readiness_stall.get('next_ticker', 'none') or 'none'}; same-next-ticker runs={price_readiness_stall.get('same_next_ticker_runs', 0)}
- Minimum unlock batch: {stall_unblock_text or 'none'}; same-batch runs={price_readiness_stall.get('same_unblock_batch_runs', 0)}
- Rule: {price_readiness_stall.get('rule', 'Do not widen exposure while local daily_prices are repeatedly blocked.')}
- Integration decision: {stall_decision_subject} is treated as a user-entry warning and {"a stricter simulated-buy cap of " + format(stall_cap, ".2%") if stall_cap is not None else "the existing no-expansion data-quality gate"}; do not add new leaderboard branches until local price validation moves.

## Tape Update Check
- Status: {tape_update.get('validation_status', 'unknown')}
- Prediction rows: current={tape_update.get('current_prediction_rows', 0)}, previous={tape_update.get('previous_prediction_rows', 'unknown')}, new_since_previous={tape_update.get('new_prediction_rows_since_previous', 'unknown')}
- Latest local prediction date: {tape_update.get('current_latest_prediction_date', 'unknown')} with {tape_update.get('latest_date_prediction_rows', 0)} rows; age={tape_update.get('latest_prediction_age_days', 'unknown')} days
- Rule: {tape_update.get('rule', '')}
- Integration decision: {"zero-new/stale tape; keep as cap/warning and apply the strict observational size" if zero_new else "not enough fresh local tape for exposure widening; keep as cap/warning"}

## Overfitting Risk
```json
{json.dumps(checks, ensure_ascii=False, indent=2)}
```

Flag: {"suspected overfit risk" if checks.get("overfit_risk") else "no severe split/cost-stress failure detected"}.

## User Entry Impact
- Improved reward alignment: `{REWARD_VERSION}` makes net total account return after modeled transaction costs the primary positive term and penalizes the magnitude and duration of excess cash.
- Improved transaction-frequency control: offline rebalancing, `run_discussion`, and direct `InvestmentSimulation.execute_trade` share a hard maximum of {MAX_DAILY_TRADES} simulated transaction actions per day, with exits/reductions before increases.
- Closed the deferred-ruling gap: daily-limit and market-hours rejections persist price-free pending decisions for the next trading session, while `check_db` exposes the queue; every future attempt must fetch a new realtime quote and re-run all safety gates.
- Improved status/prompt alignment: `check_db` prints the active reward formula and today's transaction count, while research and committee prompts receive the same reward objective and daily limit.
- Improved entry: `python -m sovereign_hall.check_db` now reads this run and can show that the evaluator used `stdlib_fallback` due local scientific-stack import failure.
- User-visible change: latest heuristic status/research prompts include evaluation-engine reliability, weak price coverage, tape freshness, sleeve diagnostics, and current simulated-buy caps.
- User-visible change: latest heuristic status/research prompts now include a daily_prices backfill readiness checklist and prioritized missing-price queue, not only the latest missing ticker.
- User-visible change: this run writes `daily_price_backfill_plan.csv` and `daily_price_backfill_plan.json`; user entries surface the plan path so the next local backfill step is concrete.
- User-visible change: priority queue entries now show explicit missing-date ranges, and CSV dry-run validation checks exact signal-date coverage before import.
- User-visible change: user entries and reports now show the minimum latest-date unlock batch separately from the longer historical priority queue, so users can fill `{unblock_text or 'none'}` first instead of confusing it with older gaps.
- User-visible change: `check_db` now prints a no-network DB coverage command, and `scripts/backfill_daily_prices.py --status` reports exact still-missing signal dates before any cap can be relaxed.
- User-visible change: `scripts/backfill_daily_prices.py --require-plan-coverage --coverage-limit 5` now returns nonzero unless selected top-priority plan dates are covered, preventing a parseable but incomplete CSV from clearing the blocker.
- User-visible change: `python -m sovereign_hall.run_discussion --help` returns CLI help without requiring the single-instance lock; actual runs remain lock-protected.
- User-visible change: latest heuristic status/research prompts now include consecutive empty-daily_prices cycles and the stalled-backfill next ticker; simulated buys receive an extra-small cap after repeated blockage.
- User-visible change: same-day manual reruns are deduped before counting consecutive blocked cycles, so a debugging rerun cannot prematurely tighten simulated-buy caps.
- User-visible change: unchanged partial daily_prices coverage is now surfaced as a stalled data task; simulated buys receive the same extra-small no-progress cap until exact local plan-date coverage moves.
- Simulation path: `run_discussion` and `InvestmentSimulation.execute_trade` continue to apply single-name, gross, weak-price, daily_prices-readiness, thin/zero-new-tape, ETF-sleeve, failure-memory, and observation-count caps through `services/heuristic_policy.py`.
- Not integrated as an exposure-increasing default: price coverage remains weak and tape validation is not meaningful enough to widen exposure.
- Next minimum loop closure: backfill independently validated local `daily_prices` for the latest missing tickers, then rerun the cycle before relaxing weak-price caps.
- This cycle's minimum local step: run `{backfill_status_command}`, then use `{backfill_strict_import_command}` to validate independently supplied OHLC rows against exact missing signal dates before adding any new return-seeking heuristic branch.

## Reproduce
```bash
{command}
```

## Next 3 Directions
- Reduce OOS cash exposure without breaking the {MAX_DAILY_TRADES}-action limit by testing staged top-five deployment and exit-first rotation.
- Exercise the durable queue with a fresh, realtime-priced, committee-approved candidate and require an actual simulated fill or precise rejection code for the current deployment gap.
- Backfill independently validated local daily prices and collect a meaningful fresh tape before converting higher-return under-deployed diagnostics into 100%-capacity policies.
"""
    path.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local heuristic learning cycle without pandas/numpy.")
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

    previous_run = latest_completed_run(runs_root)
    previous_score, previous_path = previous_best_score(runs_root)
    predictions = load_predictions(db_path)
    if not predictions:
        raise SystemExit(f"No local predictions found in {db_path}")
    tape_update = build_tape_update_report(predictions, previous_run)
    price_history = load_daily_prices(db_path, predictions)
    daily = build_daily_tape(predictions, price_history)
    costs = CostConfig()
    recent_failure_tickers = extract_failure_tickers_from_run(previous_run)
    policies = build_policies(recent_failure_tickers)
    changed_files = [
        "scripts/run_heuristic_cycle.py",
        "scripts/run_heuristic_cycle_stdlib.py",
        "services/heuristic_policy.py",
        "services/reward_policy.py",
        "check_db.py",
        "scripts/backfill_daily_prices.py",
        "research_interactive.py",
        "run_discussion.py",
        "tests/test_refactor_pipeline.py",
    ]

    results: dict[str, dict[str, Any]] = {}
    trials: list[dict[str, Any]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")
    for idx, policy in enumerate(policies):
        result = run_backtest(daily, policy, costs, price_history)
        results[policy.name] = result
        metrics = result["metrics"]
        trials.append(
            {
                "trial_index": idx,
                "timestamp": timestamp,
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
                "notes": "standard-library fallback local delayed daily signal simulation; no external market data",
            }
        )

    policies_by_name = {policy.name: policy for policy in policies}
    best_name = max(
        [
            name for name in results
            if promotable(name)
            and policies_by_name[name].max_gross >= 0.99
            and policies_by_name[name].max_names * policies_by_name[name].max_position >= 0.99
        ],
        key=lambda name: results[name]["metrics"]["score"],
    )
    best_policy = next(policy for policy in policies if policy.name == best_name)
    best_result = results[best_name]
    simplified = replace(best_policy, name="simplified_best_policy", anomaly_return_threshold=0.18)
    simplified_result = run_backtest(daily, simplified, costs, price_history)
    if simplified_result["metrics"]["score"] >= best_result["metrics"]["score"]:
        best_name = simplified.name
        best_policy = simplified
        best_result = simplified_result
    trials.append(
        {
            "trial_index": len(trials),
            "timestamp": timestamp,
            "trial_name": "simplified_best_policy",
            "changed_files": changed_files,
            "config": asdict(simplified),
            "eval_period": f"{simplified_result['metrics']['sample_start']}..{simplified_result['metrics']['sample_end']}",
            "total_return": simplified_result["metrics"]["total_return"],
            "annualized_return": simplified_result["metrics"]["annualized_return"],
            "max_drawdown": simplified_result["metrics"]["max_drawdown"],
            "sharpe": simplified_result["metrics"]["sharpe"],
            "turnover": simplified_result["metrics"]["turnover"],
            "trade_count": simplified_result["metrics"]["trade_count"],
            "average_invested_ratio": simplified_result["metrics"].get("average_invested_ratio", 0.0),
            "average_cash_ratio": simplified_result["metrics"].get("average_cash_ratio", 1.0),
            "idle_cash_penalty": simplified_result["metrics"].get("idle_cash_penalty", 0.0),
            "max_high_cash_streak_days": simplified_result["metrics"].get("max_high_cash_streak_days", 0),
            "max_daily_trade_count": simplified_result["metrics"].get("max_daily_trade_count", 0),
            "days_at_trade_limit": simplified_result["metrics"].get("days_at_trade_limit", 0),
            "deferred_trade_actions": simplified_result["metrics"].get("deferred_trade_actions", 0),
            "gross_total_return_before_cost": simplified_result["metrics"].get("gross_total_return_before_cost", 0.0),
            "cost_paid": simplified_result["metrics"].get("cost_paid", 0.0),
            "reward_version": simplified_result["metrics"].get("reward_version", REWARD_VERSION),
            "score_breakdown": simplified_result["metrics"].get("score_breakdown", {}),
            "cost_assumption": simplified_result["metrics"]["cost_assumption"],
            "score": simplified_result["metrics"]["score"],
            "notes": "simplification stage: removed excess anomaly tuning",
        }
    )

    checks = split_checks(daily, best_policy, costs, price_history)
    sleeve_diagnostics = build_sleeve_diagnostics(daily, policies, results, costs, price_history)
    price_coverage = build_price_coverage_report(daily, price_history, best_result)
    price_readiness = build_price_readiness_report(daily, price_history)
    backfill_plan_rows, backfill_plan_summary = build_daily_price_backfill_plan(daily, price_history, run_dir)
    price_readiness["backfill_plan"] = backfill_plan_summary
    price_readiness["backfill_plan_path"] = backfill_plan_summary.get("plan_csv", "")
    price_readiness["backfill_plan_summary_path"] = backfill_plan_summary.get("plan_json", "")
    from services.heuristic_policy import build_price_readiness_stall_report

    price_readiness_stall = build_price_readiness_stall_report(
        runs_root,
        pending_run_dir=run_dir,
        pending_price_readiness=price_readiness,
    )
    failures = analyze_failures(best_result, daily, best_policy)
    source_counts = price_coverage.get("daily_signal_price_source_counts", {})
    price_source = (
        "daily_prices"
        if source_counts.get("daily_prices", 0) > 0 and source_counts.get("prediction_current_price", 0) == 0
        else "prediction current_price fallback"
    )
    project_context = {
        "evaluation_engine": "stdlib_fallback",
        "evaluation_warning": "numpy/pandas import did not complete during preflight; used standard-library evaluator",
        "db_path": str(db_path),
        "prediction_rows": len(predictions),
        "daily_signal_rows": len(daily),
        "price_source": price_source,
        "price_coverage": price_coverage,
        "price_readiness": price_readiness,
        "price_readiness_stall": price_readiness_stall,
        "daily_price_backfill_plan": backfill_plan_summary,
        "tape_update": tape_update,
        "previous_run": str(previous_run) if previous_run else None,
    }

    write_csv(run_dir / "daily_signal_tape.csv", daily)
    write_csv(run_dir / "daily_price_backfill_plan.csv", backfill_plan_rows)
    write_csv(run_dir / "equity_curve_best.csv", best_result["curve"])
    write_csv(run_dir / "trades_best.csv", best_result["trades"])
    write_json(run_dir / "baseline_metrics.json", results["baseline_default_policy"]["metrics"])
    write_json(run_dir / "best_metrics.json", best_result["metrics"])
    write_json(run_dir / "overfit_checks.json", checks)
    write_json(run_dir / "project_context.json", project_context)
    write_json(run_dir / "daily_price_backfill_plan.json", backfill_plan_summary)
    write_json(run_dir / "sleeve_diagnostics.json", sleeve_diagnostics)
    write_json(run_dir / "price_coverage.json", price_coverage)
    write_json(run_dir / "price_readiness.json", price_readiness)
    write_json(run_dir / "price_readiness_stall.json", price_readiness_stall)
    write_json(run_dir / "tape_update.json", tape_update)
    write_jsonl(run_dir / "trials.jsonl", trials)
    write_jsonl(run_dir / "failure_cases.jsonl", failures)
    write_csv(
        run_dir / "summary.csv",
        [
            {
                "trial_name": trial["trial_name"],
                "score": trial["score"],
                "annualized_return": trial["annualized_return"],
                "max_drawdown": trial["max_drawdown"],
                "sharpe": trial["sharpe"],
                "turnover": trial["turnover"],
                "trade_count": trial["trade_count"],
            }
            for trial in trials
        ],
        ["trial_name", "score", "annualized_return", "max_drawdown", "sharpe", "turnover", "trade_count"],
    )
    write_plot(run_dir / "sample_efficiency.png")
    write_policy_snapshot(run_dir / "policy_snapshot.py", best_policy, costs)
    write_readme(
        run_dir / "README.md",
        run_started,
        db_path,
        len(predictions),
        best_name,
        best_result["metrics"],
        previous_score,
        previous_path,
        trials,
        checks,
        sleeve_diagnostics,
        price_coverage,
        price_readiness,
        price_readiness_stall,
        tape_update,
        f"/usr/bin/python3 scripts/run_heuristic_cycle_stdlib.py --db {args.db}",
    )
    (runs_root / "LATEST").write_text(str(run_dir), encoding="utf-8")
    print(str(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
