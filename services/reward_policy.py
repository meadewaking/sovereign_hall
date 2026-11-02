"""Shared capital-return reward and transaction-frequency rules.

The reward is intentionally expressed in account-return units.  Net total
return already contains the full configured fee, stamp-duty and slippage drag.
"""

from __future__ import annotations

from typing import Any, Iterable


MAX_DAILY_TRADES = 5
REWARD_VERSION = "capital_return_v2"
OPERATIONAL_CASH_TOLERANCE = 0.05
HIGH_CASH_THRESHOLD = 0.20
MAX_DRAWDOWN_WEIGHT = 0.35
IDLE_CASH_WEIGHT = 0.15
EXTRA_COST_DISCIPLINE_WEIGHT = 0.10


def idle_cash_exposure_penalty(cash_ratios: Iterable[float]) -> float:
    """Penalize both the size and duration of non-operational cash.

    Every day contributes its excess over the 5% operational allowance.  Cash
    above 20% receives an additional half-weight contribution, so a large cash
    balance held for many days is materially worse than a short rounding gap.
    """
    values = [min(max(float(value), 0.0), 1.0) for value in cash_ratios]
    if not values:
        return 1.0 + 0.5 * (1.0 - HIGH_CASH_THRESHOLD)
    normal_excess = sum(max(value - OPERATIONAL_CASH_TOLERANCE, 0.0) for value in values)
    high_excess = sum(max(value - HIGH_CASH_THRESHOLD, 0.0) for value in values)
    return (normal_excess + 0.5 * high_excess) / len(values)


def longest_high_cash_streak(cash_ratios: Iterable[float]) -> int:
    longest = 0
    current = 0
    for value in cash_ratios:
        if float(value) > HIGH_CASH_THRESHOLD:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def capital_reward_breakdown(metrics: dict[str, Any]) -> dict[str, float]:
    """Return the transparent components of the unified capital reward."""
    total_return = float(metrics.get("total_return", 0.0) or 0.0)
    max_drawdown = abs(float(metrics.get("max_drawdown", 0.0) or 0.0))
    cost_paid = max(float(metrics.get("cost_paid", 0.0) or 0.0), 0.0)
    idle_cash = metrics.get("idle_cash_penalty")
    if idle_cash is None:
        average_cash = float(metrics.get("average_cash_ratio", 1.0) or 0.0)
        idle_cash = idle_cash_exposure_penalty([average_cash])
    idle_cash = max(float(idle_cash), 0.0)
    drawdown_penalty = MAX_DRAWDOWN_WEIGHT * max_drawdown
    cash_penalty = IDLE_CASH_WEIGHT * idle_cash
    # Costs are already fully deducted from total_return.  This small additional
    # term makes otherwise similar paths prefer the one requiring less turnover.
    cost_discipline_penalty = EXTRA_COST_DISCIPLINE_WEIGHT * cost_paid
    return {
        "net_total_return": total_return,
        "drawdown_penalty": drawdown_penalty,
        "idle_cash_penalty": cash_penalty,
        "cost_discipline_penalty": cost_discipline_penalty,
        "score": total_return - drawdown_penalty - cash_penalty - cost_discipline_penalty,
    }


def score_capital_reward(metrics: dict[str, Any]) -> float:
    return capital_reward_breakdown(metrics)["score"]


def limit_rebalance_actions(
    current: dict[str, float],
    target: dict[str, float],
    max_trades: int = MAX_DAILY_TRADES,
) -> tuple[dict[str, float], int]:
    """Apply at most ``max_trades`` ticker-level changes, reductions first."""
    limit = max(int(max_trades), 0)
    tickers = set(current) | set(target)
    changes = [
        ticker
        for ticker in tickers
        if abs(float(target.get(ticker, 0.0)) - float(current.get(ticker, 0.0))) > 1e-9
    ]
    reductions = sorted(
        (ticker for ticker in changes if target.get(ticker, 0.0) < current.get(ticker, 0.0)),
        key=lambda ticker: (
            0 if target.get(ticker, 0.0) <= 1e-9 else 1,
            -abs(target.get(ticker, 0.0) - current.get(ticker, 0.0)),
            ticker,
        ),
    )
    increases = sorted(
        (ticker for ticker in changes if target.get(ticker, 0.0) > current.get(ticker, 0.0)),
        key=lambda ticker: (
            -abs(target.get(ticker, 0.0) - current.get(ticker, 0.0)),
            ticker,
        ),
    )
    selected = set((reductions + increases)[:limit])
    result = {ticker: float(weight) for ticker, weight in current.items() if float(weight) > 1e-9}
    for ticker in selected:
        weight = float(target.get(ticker, 0.0))
        if weight > 1e-9:
            result[ticker] = weight
        else:
            result.pop(ticker, None)
    return result, max(len(changes) - len(selected), 0)


REWARD_FORMULA = (
    "score = net_total_return - 0.35*abs(max_drawdown) "
    "- 0.15*idle_cash_exposure_penalty - 0.10*cost_paid; "
    "net_total_return already includes full fee/stamp-duty/slippage"
)
