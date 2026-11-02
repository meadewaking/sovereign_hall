"""Pure, production execution constraints.

This module has no dependency on run artifacts, databases, market-data clients,
or backtest code. It can therefore be shared by every simulated-execution path.
"""

from __future__ import annotations

from dataclasses import dataclass
import math


def approved_execution_target(requested: float, confidence: float | None) -> float:
    """Size a committee approval once, independently of the execution session.

    An account deployment objective never authorizes increasing this budget.
    Preserve the existing open-session confidence scaling for deferred rulings.
    """
    target = float(requested)
    certainty = float(confidence) if confidence is not None else 0.0
    if not math.isfinite(target) or not math.isfinite(certainty):
        raise ValueError("committee target and confidence must be finite")
    scale = .3 if certainty < .4 else (.5 if certainty < .6 else 1.0)
    return max(0.0, min(target, 1.0)) * scale


def affordable_lot_quantity(cash, price, profile, cost_schedule, *, value_budget=None):
    """Shared integer sizing; fees use operational cash, not the value budget."""
    if not all(math.isfinite(float(v)) for v in (cash, price)) or price <= 0 or cash <= 0:
        return 0
    budget = min(cash, value_budget) if value_budget is not None else cash
    lots = max(0, int(budget / price / profile.lot_size))
    while lots > 0:
        shares = lots * profile.lot_size
        cost = cost_schedule.calculate(profile, "buy", price=price, shares=shares)
        if cost.notional + cost.total <= cash + 1e-9:
            return shares
        lots -= 1
    return 0


@dataclass(frozen=True)
class ExecutionPolicyLimits:
    max_single_position: float
    max_gross: float = 1.0

    def __post_init__(self) -> None:
        if not 0 < self.max_single_position <= 1:
            raise ValueError("max_single_position must be in (0, 1]")
        if not 0 < self.max_gross <= 1:
            raise ValueError("max_gross must be in (0, 1]")


def cap_target_position(
    requested: float,
    *,
    current_position: float = 0.0,
    current_gross_exposure: float = 0.0,
    limits: ExecutionPolicyLimits,
) -> tuple[float, tuple[str, ...]]:
    """Apply deterministic single-name and gross-exposure limits."""
    requested = max(0.0, min(float(requested or 0.0), 1.0))
    current_position = max(0.0, float(current_position or 0.0))
    current_gross_exposure = max(0.0, float(current_gross_exposure or 0.0))
    reasons: list[str] = []

    capped = min(requested, limits.max_single_position)
    if capped < requested:
        reasons.append("max_single_position")

    gross_room = max(
        0.0,
        limits.max_gross - max(0.0, current_gross_exposure - current_position),
    )
    if capped > gross_room:
        capped = gross_room
        reasons.append("max_gross")
    return max(0.0, capped), tuple(reasons)
