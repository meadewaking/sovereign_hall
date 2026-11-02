"""Canonical instrument metadata used by research, evaluation and execution.

The project previously classified ETFs independently in several modules.  A
single classifier is deliberately kept small and deterministic so every cost,
risk and execution path agrees about the security being handled.

PR2.1 introduces ``InstrumentIdentity`` so a six-digit code never has to stand
in for both a stock and an index.  ``000001`` is ambiguous: it is Ping An Bank
on Shenzhen and the Shanghai Composite Index on Shanghai.  Callers that need a
concrete index must pass an ``InstrumentIdentity`` with ``kind=INDEX``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class InstrumentType(str, Enum):
    A_SHARE = "a_share"
    ETF = "etf"


class InstrumentKind(str, Enum):
    """The kind of security a code refers to.

    A six-digit code can refer to an A-share, an ETF or a market index
    depending on context.  The kind is therefore part of the identity, not
    something that can be inferred from the code alone.
    """

    A_SHARE = "a_share"
    ETF = "etf"
    INDEX = "index"


class Market(str, Enum):
    """Trading venue for an instrument.

    ``CN`` is the aggregate market used by the trading calendar; it is not a
    venue for an individual instrument.
    """

    SH = "sh"
    SZ = "sz"
    CN = "CN"


ETF_PREFIXES = (
    "159",
    "510",
    "511",
    "512",
    "513",
    "515",
    "516",
    "517",
    "518",
    "560",
    "561",
    "562",
    "563",
    "588",
)


def normalize_ticker(ticker: Any) -> str:
    """Return the six-digit local code without a market suffix."""
    code = str(ticker or "").strip().upper()
    return code.split(".", 1)[0] if "." in code else code


def classify_instrument(ticker: Any) -> InstrumentType:
    code = normalize_ticker(ticker)
    return InstrumentType.ETF if code.startswith(ETF_PREFIXES) else InstrumentType.A_SHARE


def is_etf_ticker(ticker: Any) -> bool:
    return classify_instrument(ticker) is InstrumentType.ETF


def infer_market_for_code(code: str) -> Market:
    """Best-effort venue inference for a six-digit A-share/ETF code.

    Indices are ambiguous (``000001`` is SH for the composite index and SZ for
    Ping An Bank) and must be resolved through an explicit
    ``InstrumentIdentity`` rather than this helper.
    """
    cleaned = normalize_ticker(code)
    if not cleaned.isdigit() or len(cleaned) != 6:
        raise ValueError(f"unsupported code for market inference: {code!r}")
    if cleaned.startswith(("600", "601", "603", "605", "688")):
        return Market.SH
    if cleaned.startswith(("000", "001", "002", "003", "300", "301")):
        return Market.SZ
    if cleaned.startswith(ETF_PREFIXES):
        # ETFs trade on both venues; the leading digits pick a side.
        if cleaned.startswith(("510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588")):
            return Market.SH
        return Market.SZ
    return Market.SH if cleaned.startswith("6") else Market.SZ


@dataclass(frozen=True)
class InstrumentIdentity:
    """Unambiguous instrument identity: code + market + kind.

    ``000001`` on Shanghai with ``kind=INDEX`` is the Shanghai Composite
    Index; ``000001`` on Shenzhen with ``kind=A_SHARE`` is Ping An Bank.
    Neither can be confused with the other once the identity is constructed.
    """

    code: str
    market: Market
    kind: InstrumentKind

    def __post_init__(self) -> None:
        cleaned = normalize_ticker(self.code)
        if len(cleaned) != 6 or not cleaned.isdigit():
            raise ValueError(f"unsupported instrument code: {self.code!r}")
        object.__setattr__(self, "code", cleaned)
        if not isinstance(self.market, Market):
            object.__setattr__(self, "market", Market(str(self.market)))
        if not isinstance(self.kind, InstrumentKind):
            object.__setattr__(self, "kind", InstrumentKind(str(self.kind)))

    @property
    def symbol(self) -> str:
        return f"{self.market.value.upper()}{self.code}"

    @classmethod
    def parse(cls, symbol: str) -> "InstrumentIdentity":
        """Parse ``SH000001`` / ``sz.000001`` / ``000001.SZ`` into an identity.

        The kind still has to be supplied by the caller because the same
        symbol can refer to an index or a share depending on context.
        """
        text = str(symbol or "").strip().upper()
        if not text:
            raise ValueError("empty instrument symbol")
        market: Market | None = None
        if "." in text:
            left, right = text.split(".", 1)
            if left in {"SH", "SZ"}:
                market = Market(left.lower())
                code = right
            elif right in {"SH", "SZ"}:
                market = Market(right.lower())
                code = left
            else:
                code = text.replace(".", "")
        elif text.startswith("SH"):
            market = Market.SH
            code = text[2:]
        elif text.startswith("SZ"):
            market = Market.SZ
            code = text[2:]
        else:
            code = text
        cleaned = normalize_ticker(code)
        if not cleaned.isdigit() or len(cleaned) != 6:
            raise ValueError(f"unsupported instrument symbol: {symbol!r}")
        if market is None:
            market = infer_market_for_code(cleaned)
        return cls(code=cleaned, market=market, kind=InstrumentKind.A_SHARE)


# Canonical reference identities. Use these instead of bare ``"000001"`` so a
# stock code is never silently used as an index tape.
SHANGHAI_COMPOSITE_INDEX = InstrumentIdentity(
    code="000001", market=Market.SH, kind=InstrumentKind.INDEX
)
SHENZHEN_COMPONENT_INDEX = InstrumentIdentity(
    code="399001", market=Market.SZ, kind=InstrumentKind.INDEX
)
CSI_300_INDEX = InstrumentIdentity(
    code="000300", market=Market.SH, kind=InstrumentKind.INDEX
)


@dataclass(frozen=True)
class InstrumentProfile:
    ticker: str
    instrument_type: InstrumentType
    lot_size: int = 100
    listed_at: str | None = None
    delisted_at: str | None = None
    source: str = "canonical_ticker_classifier_v1"
    # PR2.1 — optional identity fields.  Older callers still construct a
    # profile from a bare ticker; the market is then inferred from the code.
    market: Market | None = None
    kind: InstrumentKind | None = None
    # PR2.2 — trading rules that cannot be expressed by a single lot_size.
    min_first_buy_size: int | None = None
    incremental_step: int | None = None
    sell_odd_lot_rule: str | None = None
    settlement: str | None = None  # "T+1" / "T+0"
    trading_venue: str | None = None

    def __post_init__(self) -> None:
        code = normalize_ticker(self.ticker)
        if len(code) != 6 or not code.isdigit():
            raise ValueError(f"unsupported A-share/ETF ticker: {self.ticker!r}")
        if int(self.lot_size) <= 0:
            raise ValueError("lot_size must be positive")
        object.__setattr__(self, "ticker", code)
        if self.market is None:
            object.__setattr__(self, "market", infer_market_for_code(code))
        if self.kind is None:
            object.__setattr__(
                self,
                "kind",
                InstrumentKind.ETF
                if classify_instrument(code) is InstrumentType.ETF
                else InstrumentKind.A_SHARE,
            )

    def is_active_on(self, value: str | date | datetime) -> bool:
        """Return whether the instrument is listed on the requested session."""
        if isinstance(value, datetime):
            day = value.date().isoformat()
        elif isinstance(value, date):
            day = value.isoformat()
        else:
            day = str(value or "")[:10]
        if not day:
            return False
        if self.listed_at and day < str(self.listed_at)[:10]:
            return False
        if self.delisted_at and day > str(self.delisted_at)[:10]:
            return False
        return True

    @classmethod
    def from_ticker(
        cls,
        ticker: Any,
        *,
        lot_size: int = 100,
        listed_at: str | None = None,
        delisted_at: str | None = None,
        source: str = "canonical_ticker_classifier_v1",
        market: Market | None = None,
        kind: InstrumentKind | None = None,
        min_first_buy_size: int | None = None,
        incremental_step: int | None = None,
        sell_odd_lot_rule: str | None = None,
        settlement: str | None = None,
        trading_venue: str | None = None,
    ) -> "InstrumentProfile":
        code = normalize_ticker(ticker)
        return cls(
            ticker=code,
            instrument_type=classify_instrument(code),
            lot_size=lot_size,
            listed_at=listed_at,
            delisted_at=delisted_at,
            source=source,
            market=market,
            kind=kind,
            min_first_buy_size=min_first_buy_size,
            incremental_step=incremental_step,
            sell_odd_lot_rule=sell_odd_lot_rule,
            settlement=settlement,
            trading_venue=trading_venue,
        )
