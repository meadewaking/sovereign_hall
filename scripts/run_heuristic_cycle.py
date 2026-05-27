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
import sqlite3
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

import numpy as np
import pandas as pd


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
    rebalance_threshold: float = 0.0
    min_signal_count: int = 1
    max_stop_gap: float = 0.55


@dataclass(frozen=True)
class CostConfig:
    trading_fee: float = 0.0003
    stamp_duty: float = 0.0010
    slippage: float = 0.0005


def normalize_ticker(ticker: Any) -> str:
    code = str(ticker or "").strip().upper()
    return code.split(".")[0] if "." in code else code


def score_metrics(metrics: dict[str, Any]) -> float:
    turnover_penalty = max(0.0, float(metrics.get("turnover", 0.0)) - 1.0)
    cost_penalty = float(metrics.get("cost_paid", 0.0))
    return (
        float(metrics.get("annualized_return", 0.0))
        - 0.5 * abs(float(metrics.get("max_drawdown", 0.0)))
        - 0.1 * turnover_penalty
        - cost_penalty
    )


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
        independent = price_history.rename(columns={"close": "independent_price"})
        daily = daily.merge(
            independent[["date", "ticker", "independent_price"]],
            on=["date", "ticker"],
            how="left",
        )
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
        forced = forced_holding_positions(policy, current_positions, position_age_days)
        if forced:
            reasons.append("min_holding_retained_existing")
            return forced, reasons
        return {}, reasons or ["all_candidates_filtered"]

    candidates = candidates.sort_values(["signal_strength", "confidence"], ascending=False).head(policy.max_names)
    gross = policy.max_gross
    if current_drawdown <= -policy.drawdown_guard_threshold:
        gross *= policy.drawdown_guard
        reasons.append("drawdown_guard_scaled_gross")
    if policy.loss_streak_threshold and consecutive_loss_days >= policy.loss_streak_threshold:
        gross *= policy.loss_streak_guard
        reasons.append("loss_streak_guard_scaled_gross")

    weights = forced_holding_positions(policy, current_positions, position_age_days)
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
    raw_scores = candidates["signal_strength"].clip(lower=0.0)
    if raw_scores.sum() <= 0:
        return weights, reasons or ["non_positive_scores"]

    for _, row in candidates.iterrows():
        raw_weight = allocatable * float(row["signal_strength"] / raw_scores.sum())
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
            weights[row["ticker"]] = float(weight)
    return weights, reasons


def forced_holding_positions(
    policy: PolicyConfig,
    current_positions: dict[str, float],
    position_age_days: dict[str, int],
) -> dict[str, float]:
    if policy.min_holding_days <= 0:
        return {}
    return {
        ticker: float(weight)
        for ticker, weight in current_positions.items()
        if weight > 0 and position_age_days.get(ticker, 0) < policy.min_holding_days
    }


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
            "cost_assumption": cost_assumption,
        }
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
        "trade_count": int(sum(1 for t in trades if t.get("exit_date"))),
        "cost_paid": float(total_cost),
        "cost_assumption": cost_assumption,
    }
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

    for idx in range(1, len(dates)):
        signal_date = dates[idx - 1]
        date = dates[idx]
        current_dd = equity / peak - 1.0
        targets, reasons = pick_targets(
            by_date[signal_date],
            policy,
            positions,
            position_age_days,
            current_dd,
            consecutive_loss_days,
        )
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
                "suspected_reason": "entry followed high-confidence signal but subsequent price path reversed",
                "repair_direction": "require positive short-term trend or reduce size when volatility regime is elevated",
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
    for summary in sorted(root.glob("*/summary.csv")):
        try:
            df = pd.read_csv(summary)
        except Exception:
            continue
        if "score" not in df or df.empty:
            continue
        score = float(df["score"].max())
        if best_score is None or score > best_score:
            best_score = score
            best_path = summary
    return best_score, best_path


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
    return {
        "split_date": dates[split],
        "train": train,
        "out_of_sample": test,
        "cost_stress_3x_slippage": cost_stress,
        "overfit_risk": (
            test["score"] < 0
            or test["score"] < train["score"] * 0.25
            or cost_stress["score"] < 0
        ),
    }


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
) -> None:
    failed = [t for t in trials if t["trial_name"] != best_name]
    comparison = "No previous heuristic_cycle best was found."
    if previous_score is not None:
        comparison = (
            f"Previous best score {previous_score:.6f} from {previous_path}; "
            f"delta {best_metrics['score'] - previous_score:+.6f}."
        )
    failed_lines = "\n".join(
        f"- {t['trial_name']}: score={t['score']:.6f}, notes={t['notes']}" for t in failed
    )
    checks_text = json.dumps(checks, ensure_ascii=False, indent=2)
    text = f"""# Heuristic Learning Cycle

## Run
- Run time: {run_started}
- Data source: `{db_path}`
- Samples consumed: {sample_count} prediction rows
- Best policy: `{best_name}`
- Current best score: {best_metrics['score']:.6f}
- Previous best comparison: {comparison}

## What Changed
- Extended the local-only delayed-signal heuristic evaluation loop for this cycle.
- Tested small interpretable changes: trend filtering, volatility scaling, anomaly veto, drawdown guard, losing-streak cooldown, minimum holding periods, no-new-risk pauses, and rebalance friction.
- Made `python -m sovereign_hall.check_db` exit cleanly in non-interactive automation after printing database and simulated portfolio status.
- Wrote the retained policy snapshot to `policy_snapshot.py`.

## Best Metrics
- Total return: {best_metrics['total_return']:.4%}
- Annualized return: {best_metrics['annualized_return']:.4%}
- Max drawdown: {best_metrics['max_drawdown']:.4%}
- Sharpe: {best_metrics['sharpe']:.3f}
- Sortino: {best_metrics['sortino']:.3f}
- Win rate: {best_metrics['win_rate']:.2%}
- Turnover: {best_metrics['turnover']:.3f}
- Trade count: {best_metrics['trade_count']}
- Cost assumption: {best_metrics['cost_assumption']}

## Failed Or Weaker Directions
{failed_lines or "- None; only one trial was available."}

## Overfitting Risk
```json
{checks_text}
```

Flag: {"suspected overfit risk" if checks.get("overfit_risk") else "no severe split/cost-stress failure detected"}.

## Reproduce
```bash
{command}
```

## Next 3 Directions
- Separate ETF and single-stock universes before mixing them in one portfolio.
- Add validated simulation trade outcomes once local prices are available.
- Test a stricter cost-stress gate that rejects policies with negative 3x-slippage scores.
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
    db_path = (project_root / args.db).resolve()
    runs_root = (project_root / args.runs_root).resolve()
    run_started = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = args.timestamp or run_started
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    previous_score, previous_path = previous_best_score(runs_root)
    predictions = load_predictions(db_path)
    if predictions.empty:
        raise SystemExit(f"No local predictions found in {db_path}")
    price_history = load_daily_prices(db_path, predictions)
    daily = build_daily_tape(predictions, price_history)
    daily.to_csv(run_dir / "daily_signal_tape.csv", index=False)

    costs = CostConfig()
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
            "changed_files": ["scripts/run_heuristic_cycle.py"],
            "config": asdict(policy),
            "eval_period": f"{metrics['sample_start']}..{metrics['sample_end']}",
            "total_return": metrics["total_return"],
            "annualized_return": metrics["annualized_return"],
            "max_drawdown": metrics["max_drawdown"],
            "sharpe": metrics["sharpe"],
            "turnover": metrics["turnover"],
            "trade_count": metrics["trade_count"],
            "cost_assumption": metrics["cost_assumption"],
            "score": metrics["score"],
            "notes": "local delayed daily signal simulation; no external market data",
        }
        trial_rows.append(row)
        append_jsonl(trials_path, [row])

    best_trial = max(trial_rows, key=lambda row: row["score"])
    best_policy = next(policy for policy in policies if policy.name == best_trial["trial_name"])

    simplified = replace(
        best_policy,
        name="simplified_best_policy",
        vol_lookback=0 if best_policy.vol_lookback else best_policy.vol_lookback,
        high_vol_scale=1.0,
        anomaly_return_threshold=0.18,
    )
    simplified_result = run_backtest(daily, simplified, costs)
    simplified_metrics = simplified_result["metrics"]
    simplify_row = {
        "trial_index": len(trial_rows),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "trial_name": simplified.name,
        "changed_files": ["scripts/run_heuristic_cycle.py"],
        "config": asdict(simplified),
        "eval_period": f"{simplified_metrics['sample_start']}..{simplified_metrics['sample_end']}",
        "total_return": simplified_metrics["total_return"],
        "annualized_return": simplified_metrics["annualized_return"],
        "max_drawdown": simplified_metrics["max_drawdown"],
        "sharpe": simplified_metrics["sharpe"],
        "turnover": simplified_metrics["turnover"],
        "trade_count": simplified_metrics["trade_count"],
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
        "price_source": (
            "daily_prices table with fallback to prediction current_price"
            if not price_history.empty
            else "prediction current_price fallback; daily_prices table unavailable or empty"
        ),
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
    )

    latest = runs_root / "LATEST"
    latest.write_text(str(run_dir) + "\n", encoding="utf-8")
    print(f"run_dir={run_dir}")
    print(f"best_policy={best_trial['trial_name']}")
    print(f"best_score={best_metrics['score']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
