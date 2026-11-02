"""Typed boundaries between research, portfolio construction and risk."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping

from .instruments import normalize_ticker


class SleeveId(str, Enum):
    LLM_RESEARCH = "llm_research"
    SYSTEMATIC_ETF_MOMENTUM_V1 = "systematic_etf_momentum_v1"


class RiskStatus(str, Enum):
    APPROVED = "approved"
    REDUCED = "reduced"
    BLOCKED_NEW_RISK = "blocked_new_risk"


class ArmKind(str, Enum):
    """Which side of a registered experiment this arm represents.

    A control arm runs the frozen policy version; a candidate arm runs the
    proposed change.  Both start from the same 10000 RMB cash, the same
    frozen universe and the same market input batch.
    """

    CONTROL = "control"
    CANDIDATE = "candidate"


class ExecutionClock(str, Enum):
    """When an arm's intents are filled.

    ``DAILY_NEXT_OPEN`` is the legacy shadow engine: signals dated T are
    filled at T+1's daily open.  ``PRODUCTION_EQUIVALENT_FORWARD`` requires
    both arms to persist price-free intents at the same natural-round
    controlled quote, so the result is directly comparable to production.

    An arm that has not implemented production-equivalent forward mode must
    report ``execution_clock_mismatch`` and may not be promoted.
    """

    DAILY_NEXT_OPEN = "daily_next_open"
    PRODUCTION_EQUIVALENT_FORWARD = "production_equivalent_forward"


class ExperimentStatus(str, Enum):
    """Lifecycle of a registered experiment.

    ``eligible`` means the experiment has met its data and sample
    preconditions; ``promoted`` is recorded separately and never written
    into the main config automatically.
    """

    REGISTERED = "registered"
    COLLECTING = "collecting"
    DATA_BLOCKED = "data_blocked"
    INSUFFICIENT_SAMPLES = "insufficient_samples"
    REJECTED = "rejected"
    ELIGIBLE = "eligible"
    PROMOTED = "promoted"
    RETRACTED = "retracted"


@dataclass(frozen=True)
class CandidateSignal:
    """Research may express conviction, but never shares or order notional."""

    ticker: str
    direction: str
    score: float
    signal_at: str
    available_at: str
    sleeve: SleeveId
    evidence_ids: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        direction = str(self.direction).lower()
        if direction not in {"long", "sell", "hold"}:
            raise ValueError(f"unsupported signal direction: {self.direction}")
        object.__setattr__(self, "ticker", normalize_ticker(self.ticker))
        object.__setattr__(self, "direction", direction)

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["sleeve"] = self.sleeve.value
        value["evidence_ids"] = list(self.evidence_ids)
        value["metadata"] = dict(self.metadata)
        return value


@dataclass(frozen=True)
class TargetPortfolio:
    sleeve: SleeveId
    as_of: str
    strategy_version: str
    targets: Mapping[str, float]
    initial_capital: float = 10000.0
    risk_status: RiskStatus = RiskStatus.APPROVED
    risk_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if float(self.initial_capital) != 10000.0:
            raise ValueError("the current shadow account must start with 10000")
        normalized = {
            normalize_ticker(ticker): float(weight)
            for ticker, weight in self.targets.items()
            if float(weight) > 0
        }
        if any(weight > 1.0 for weight in normalized.values()):
            raise ValueError("target weights must not exceed 100%")
        if sum(normalized.values()) > 1.0 + 1e-9:
            raise ValueError("target portfolio cannot use leverage")
        object.__setattr__(self, "targets", normalized)


@dataclass(frozen=True)
class RiskDecision:
    status: RiskStatus
    approved_targets: Mapping[str, float]
    reasons: tuple[str, ...]
    drawdown: float
