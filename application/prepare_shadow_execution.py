"""Translate risk-approved targets into quantity-free execution intents."""

from __future__ import annotations

from typing import Mapping

from ..domain.portfolio.models import ExecutionIntent
from ..domain.portfolio.shadow import RiskDecision, TargetPortfolio
from ..domain.portfolio.instruments import normalize_ticker


def prepare_execution_intents(
    portfolio: TargetPortfolio,
    risk_decision: RiskDecision,
    *,
    current_weights: Mapping[str, float] | None = None,
) -> tuple[ExecutionIntent, ...]:
    """Create target-weight instructions; order quantities remain broker-side."""
    current = {
        normalize_ticker(ticker): max(0.0, float(weight))
        for ticker, weight in (current_weights or {}).items()
    }
    approved = {
        normalize_ticker(ticker): max(0.0, float(weight))
        for ticker, weight in risk_decision.approved_targets.items()
    }
    intents: list[ExecutionIntent] = []
    for ticker in sorted(set(current) | set(approved)):
        old_weight = current.get(ticker, 0.0)
        target_weight = approved.get(ticker, 0.0)
        if abs(target_weight - old_weight) <= 1e-12:
            continue
        direction = "buy" if target_weight > old_weight else "sell"
        intents.append(
            ExecutionIntent(
                ticker=ticker,
                direction=direction,
                target_position=target_weight,
                reason=(
                    f"shadow_sleeve={portfolio.sleeve.value}; "
                    f"strategy={portfolio.strategy_version}; "
                    f"risk={risk_decision.status.value}; "
                    f"target_weight={target_weight:.8f}"
                ),
                idempotency_key=(
                    f"shadow:{portfolio.sleeve.value}:{portfolio.as_of}:"
                    f"{portfolio.strategy_version}:{ticker}:{direction}"
                ),
            )
        )
    return tuple(intents)

