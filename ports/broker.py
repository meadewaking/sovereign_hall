"""Broker boundary shared by safe paper execution and future adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol


SAFE_EXECUTION_MODES = frozenset({"simulation", "paper"})


def assert_safe_execution_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized not in SAFE_EXECUTION_MODES:
        raise RuntimeError(
            "live execution is intentionally unavailable; only simulation/paper are permitted"
        )
    return normalized


@dataclass(frozen=True)
class BrokerOrder:
    ticker: str
    side: str
    shares: int
    signal_at: str
    earliest_fill_date: str
    idempotency_key: str
    order_type: str = "market"
    limit_price: float | None = None
    reason: str = ""
    submitted_at: str = ""


@dataclass(frozen=True)
class BrokerQuote:
    ticker: str
    price: float
    provider: str
    fetched_at: str
    tradable: bool = True


@dataclass(frozen=True)
class BrokerFill:
    fill_id: str
    order_id: str
    ticker: str
    side: str
    shares: int
    price: float
    notional: float
    total_cost: float
    filled_at: str
    cost_model_version: str


@dataclass(frozen=True)
class BrokerAccount:
    account_id: str
    initial_capital: float
    cash: float
    positions: Mapping[str, int]


@dataclass(frozen=True)
class ReconciliationResult:
    account_id: str
    status: str
    cash_difference: float
    share_differences: Mapping[str, int]
    snapshot_id: str


class BrokerPort(Protocol):
    async def submit_order(self, order: BrokerOrder) -> str:
        ...

    async def execute_pending(
        self,
        session: str,
        opening_quotes: Mapping[str, BrokerQuote],
    ) -> tuple[BrokerFill, ...]:
        ...

    async def account(self) -> BrokerAccount:
        ...

    async def reconcile(self, snapshot_at: str) -> ReconciliationResult:
        ...

