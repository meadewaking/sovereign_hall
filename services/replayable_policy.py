"""PR3.2 — Pure, replayable decision functions shared by production and shadow.

The plan calls for three semantic boundaries that both the production
runner and a frozen shadow arm must call with identical inputs:

* ``review_positions`` — apply lifecycle rules (stop loss / take profit /
  max holding period / price-age gate) to every open position.
* ``build_targets`` — turn approved committee decisions into a target
  portfolio subject to single-name and gross-exposure limits.
* ``plan_execution`` — turn price-free intents into a concrete execution
  plan against a shared quote snapshot, instrument rules and limits.

Each function takes only explicit inputs and reads no database, no LLM and
no network.  A frozen shadow arm calls them with the same inputs the
production runner captured, so the two arms make the same decision.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence

from ..domain.portfolio.execution_policy import (
    ExecutionPolicyLimits,
    cap_target_position,
    approved_execution_target,
    affordable_lot_quantity,
)
from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.quote_freshness import is_fresh_quote
from ..domain.portfolio.instruments import (
    InstrumentProfile,
    classify_instrument,
)
from ..domain.portfolio.shadow import TargetPortfolio
from .portfolio_policy import PositionReview, review_position


@dataclass(frozen=True)
class PositionState:
    ticker: str
    shares: int
    avg_cost: float
    opened_at: str | None
    peak_price: float | None = None
    last_reviewed_at: str | None = None
    review_status: str | None = None
    review_reason: str | None = None


@dataclass(frozen=True)
class MarketSnapshot:
    """A frozen quote snapshot shared by both arms of an experiment."""

    quotes: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    fetched_at: str | None = None
    provider: str | None = None

    def quote_for(self, ticker: str) -> Mapping[str, Any] | None:
        cleaned = str(ticker or "").strip().upper().split(".", 1)[0]
        for key, value in self.quotes.items():
            if str(key).strip().upper().split(".", 1)[0] == cleaned:
                return value
        return None


@dataclass(frozen=True)
class PolicyConfig:
    """The subset of policy parameters that drive replayable decisions."""

    max_single_position: float = 1.0
    max_gross: float = 1.0
    stop_loss_pct: float = -0.08
    take_profit_pct: float = 0.15
    max_holding_days: int = 30
    max_price_age_days: int = 3


@dataclass(frozen=True)
class ApprovedDecision:
    ticker: str
    direction: str  # "long" / "sell" / "hold"
    target_weight: float
    confidence: float = 0.0
    reason: str = ""


@dataclass(frozen=True)
class PriceFreeIntent:
    """An intent that has not yet been bound to a quote.

    Both arms persist these intents at the same natural round; the binding
    to a shared quote snapshot happens later, in ``plan_execution``.
    """

    ticker: str
    direction: str
    target_weight: float
    priority: int = 100
    reason: str = ""


@dataclass(frozen=True)
class InstrumentRuleSet:
    """Per-instrument trading rules used by ``plan_execution``."""

    profiles: Mapping[str, InstrumentProfile] = field(default_factory=dict)

    def profile_for(self, ticker: str) -> InstrumentProfile:
        cleaned = str(ticker or "").strip().upper().split(".", 1)[0]
        for key, profile in self.profiles.items():
            if str(key).strip().upper().split(".", 1)[0] == cleaned:
                return profile
        return InstrumentProfile.from_ticker(cleaned)


@dataclass(frozen=True)
class ExecutionStep:
    ticker: str
    direction: str
    target_shares: int
    limit_price: float | None
    reason: str
    rejected: bool = False
    reject_code: str | None = None
    trade_shares: int = 0
    fees: float = 0.0
    cash_delta: float = 0.0


@dataclass(frozen=True)
class ExecutionPlan:
    steps: tuple[ExecutionStep, ...]
    notes: tuple[str, ...] = ()


def review_positions(
    *,
    portfolio_state: Sequence[PositionState],
    market_snapshot: MarketSnapshot,
    policy_config: PolicyConfig,
    now: datetime | None = None,
) -> list[PositionReview]:
    """Apply lifecycle rules to every open position.

    Both arms of an experiment call this with the same ``portfolio_state``
    and ``market_snapshot``; the resulting reviews are byte-identical when
    the inputs match.  No DB, no LLM, no network.
    """
    reviews: list[PositionReview] = []
    for position in portfolio_state:
        quote = market_snapshot.quote_for(position.ticker) or {}
        price = quote.get("price")
        price_at = quote.get("fetched_at") or quote.get("price_at")
        price_source = str(quote.get("source") or "realtime_quote_unavailable")
        reviews.append(
            review_position(
                ticker=position.ticker,
                avg_cost=float(position.avg_cost or 0.0),
                opened_at=position.opened_at,
                price=float(price) if price is not None else None,
                price_at=price_at,
                price_source=price_source,
                now=now,
                max_price_age_days=policy_config.max_price_age_days,
                stop_loss_pct=policy_config.stop_loss_pct,
                take_profit_pct=policy_config.take_profit_pct,
                max_holding_days=policy_config.max_holding_days,
            )
        )
    return reviews


def build_targets(
    *,
    approved_decisions: Sequence[ApprovedDecision],
    portfolio_state: Sequence[PositionState],
    policy_config: PolicyConfig,
    sleeve: str,
    as_of: str,
    strategy_version: str,
    total_assets: float,
    market_snapshot: MarketSnapshot | None = None,
) -> TargetPortfolio:
    """Turn approved decisions into a target portfolio subject to limits.

    The same approved decisions applied to the same starting portfolio must
    produce the same target weights in both arms.  ``cap_target_position``
    enforces single-name and gross-exposure limits deterministically.
    """
    if total_assets <= 0:
        raise ValueError("positive current equity is required")
    targets: dict[str, float] = {}
    for position in portfolio_state:
        quote = market_snapshot.quote_for(position.ticker) if market_snapshot else None
        if not quote or float(quote.get("price") or 0) <= 0:
            raise ValueError("current holdings require explicit market marks; cost basis is not a mark")
        targets[position.ticker] = position.shares * float(quote["price"]) / total_assets
    current_gross = sum(targets.values())
    notes: list[str] = []
    for decision in sorted(approved_decisions, key=lambda d: d.direction not in {"sell", "short"}):
        if str(decision.direction).lower() == "hold":
            continue
        current_position = targets.get(decision.ticker, 0.0)
        if decision.direction in {"sell", "short"}:
            current_gross -= targets.pop(decision.ticker, 0.0)
            continue
        capped, reasons = cap_target_position(
            approved_execution_target(decision.target_weight, decision.confidence),
            current_position=current_position,
            current_gross_exposure=current_gross,
            limits=ExecutionPolicyLimits(
                max_single_position=policy_config.max_single_position,
                max_gross=policy_config.max_gross,
            ),
        )
        if capped > 0:
            targets[decision.ticker] = capped
            current_gross += capped - current_position
        if reasons:
            notes.append(
                f"{decision.ticker}: capped by {','.join(reasons)} "
                f"({decision.target_weight:.4f} -> {capped:.4f})"
            )
    return TargetPortfolio(
        sleeve=sleeve,  # type: ignore[arg-type]
        as_of=as_of,
        strategy_version=strategy_version,
        targets=targets,
    )


def plan_execution(
    *,
    price_free_intents: Sequence[PriceFreeIntent],
    market_snapshot: MarketSnapshot,
    instrument_rules: InstrumentRuleSet,
    limits: ExecutionPolicyLimits,
    cash_available: float,
    current_prices: Mapping[str, float] | None = None,
    current_positions: Mapping[str, int] | None = None,
    cost_schedule: CostSchedule | None = None,
    now: datetime | None = None,
    remaining_trade_slots: int = 5,
) -> ExecutionPlan:
    """Bind price-free intents to a shared quote snapshot.

    Both arms persist their price-free intents at the same natural round;
    this function then binds them to the same shared quote snapshot to
    produce concrete share counts.  An intent whose quote is missing or
    whose lot size cannot be satisfied is rejected with a concrete code;
    it does not fall back to a stale or cost-basis price.
    """
    positions = dict(current_positions or {})
    schedule = cost_schedule or CostSchedule()
    clock = now or (datetime.fromisoformat(market_snapshot.fetched_at) if market_snapshot.fetched_at else datetime.now())
    prices = {ticker: float(quote["price"]) for ticker in positions
              if (quote := market_snapshot.quote_for(ticker)) and is_fresh_quote(dict(quote), now=clock)}
    equity = float(cash_available) + sum(shares * prices.get(ticker, 0) for ticker, shares in positions.items())
    steps: list[ExecutionStep] = []
    notes: list[str] = []
    remaining_cash = float(cash_available)
    for intent in sorted(price_free_intents, key=lambda i: i.priority):
        quote = market_snapshot.quote_for(intent.ticker) or {}
        price = float(quote["price"]) if is_fresh_quote(dict(quote), now=clock) else None
        if price is None or price <= 0:
            steps.append(
                ExecutionStep(
                    ticker=intent.ticker,
                    direction=intent.direction,
                    target_shares=0,
                    limit_price=None,
                    reason="quote unavailable; no fallback to stale or cost price",
                    rejected=True,
                    reject_code="quote_unavailable",
                )
            )
            continue
        profile = instrument_rules.profile_for(intent.ticker)
        lot_size = int(profile.lot_size or 100)
        if lot_size <= 0:
            steps.append(
                ExecutionStep(
                    ticker=intent.ticker,
                    direction=intent.direction,
                    target_shares=0,
                    limit_price=price,
                    reason=f"invalid lot_size {lot_size}",
                    rejected=True,
                    reject_code="invalid_lot_size",
                )
            )
            continue
        if remaining_trade_slots <= 0:
            steps.append(ExecutionStep(intent.ticker, intent.direction, positions.get(intent.ticker, 0),
                price, "daily fill limit", True, "daily_limit"))
            continue
        current = positions.get(intent.ticker, 0)
        gross = sum(shares * prices.get(ticker, 0) for ticker, shares in positions.items())
        weight, _ = cap_target_position(intent.target_weight,
            current_position=current * price / equity if equity else 0,
            current_gross_exposure=gross / equity if equity else 0, limits=limits)
        target_value = weight * equity
        if str(intent.direction).lower() == "sell":
            target_shares = (
                int(target_value // price)
                if target_value > 0
                else 0
            )
            target_shares -= target_shares % lot_size
            quantity = max(0, current - target_shares)
            cost = schedule.calculate(profile, "sell", price=price, shares=quantity) if quantity else None
            steps.append(
                ExecutionStep(
                    ticker=intent.ticker,
                    direction=intent.direction,
                    target_shares=max(0, target_shares),
                    limit_price=price,
                    reason="sell target after lot-size rounding",
                    trade_shares=quantity, fees=cost.total if cost else 0,
                    cash_delta=(cost.notional - cost.total) if cost else 0,
                )
            )
            remaining_trade_slots -= bool(quantity)
            continue
        if len(prices) != len(positions):
            steps.append(ExecutionStep(intent.ticker, intent.direction, current, price,
                "incomplete portfolio valuation", True, "valuation_incomplete"))
            continue
        quantity = affordable_lot_quantity(remaining_cash, price, profile, schedule,
            value_budget=max(0, target_value - current * price))
        target_shares = current + quantity
        if quantity <= 0:
            steps.append(
                ExecutionStep(
                    ticker=intent.ticker,
                    direction=intent.direction,
                    target_shares=0,
                    limit_price=price,
                    reason=(
                        "insufficient cash for minimum lot; "
                        "no leverage, no fallback"
                    ),
                    rejected=True,
                    reject_code="lot_or_cost_blocked",
                )
            )
            continue
        cost = schedule.calculate(profile, "buy", price=price, shares=quantity)
        remaining_cash -= cost.notional + cost.total
        positions[intent.ticker] = target_shares
        prices[intent.ticker] = price
        remaining_trade_slots -= 1
        steps.append(
            ExecutionStep(
                ticker=intent.ticker,
                direction=intent.direction,
                target_shares=target_shares,
                limit_price=price,
                reason="buy target after lot-size and cash rounding",
                trade_shares=quantity, fees=cost.total,
                cash_delta=-cost.notional - cost.total,
            )
        )
    return ExecutionPlan(steps=tuple(steps), notes=tuple(notes))


__all__ = [
    "ApprovedDecision",
    "ExecutionPlan",
    "ExecutionStep",
    "InstrumentRuleSet",
    "MarketSnapshot",
    "PolicyConfig",
    "PositionState",
    "PriceFreeIntent",
    "build_targets",
    "plan_execution",
    "review_positions",
]
