"""Realtime quote provider port."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol


class QuoteProvider(Protocol):
    async def get_current_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        ...

    async def is_trading_day(self) -> bool:
        ...

    async def is_market_open(self) -> bool:
        ...

