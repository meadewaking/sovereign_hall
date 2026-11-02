"""Typed simulation execution contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from ..common.ids import new_id


class ExecutionIntentStatus(str, Enum):
    PENDING = "pending"
    DEFERRED = "deferred"
    EXECUTING = "executing"
    EXECUTED = "executed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class QuoteSnapshot:
    ticker: str
    price: float
    provider: str
    fetched_at: str
    purpose: str
    id: str = field(default_factory=lambda: new_id("quote"))
    round_id: Optional[str] = None
    intent_id: Optional[str] = None
    received_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def __post_init__(self) -> None:
        if self.price <= 0:
            raise ValueError("quote price must be positive")
        if not self.provider.strip():
            raise ValueError("quote provider is required")
        if not self.fetched_at.strip():
            raise ValueError("quote fetched_at is required")


@dataclass(frozen=True)
class ExecutionIntent:
    ticker: str
    direction: str
    target_position: float
    reason: str
    id: str = field(default_factory=lambda: new_id("intent"))
    round_id: Optional[str] = None
    decision_id: Optional[str] = None
    confidence: Optional[float] = None
    priority: int = 100
    status: ExecutionIntentStatus = ExecutionIntentStatus.PENDING
    defer_code: Optional[str] = None
    idempotency_key: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    expires_at: str = field(
        default_factory=lambda: (datetime.now() + timedelta(days=7)).isoformat()
    )

    def __post_init__(self) -> None:
        direction = self.direction.lower()
        if direction not in {"long", "buy", "short", "sell", "hold"}:
            raise ValueError(f"unsupported intent direction: {self.direction}")
        if not 0.0 <= float(self.target_position) <= 1.0:
            raise ValueError("target_position must be between 0 and 1")

    @property
    def stable_idempotency_key(self) -> str:
        if self.idempotency_key:
            return self.idempotency_key
        return ":".join(
            (
                self.round_id or "standalone",
                self.decision_id or self.id,
                self.ticker,
                self.direction.lower(),
            )
        )

