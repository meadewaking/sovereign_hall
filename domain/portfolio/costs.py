"""One versioned economic cost model for simulation and evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from .instruments import InstrumentProfile, InstrumentType


DEFAULT_COST_MODEL_VERSION = "cn_a_share_etf_v1_20260814"


@dataclass(frozen=True)
class TradeCost:
    notional: float
    commission: float
    stamp_duty: float
    slippage: float
    total: float
    instrument_type: InstrumentType
    side: str
    model_version: str

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["instrument_type"] = self.instrument_type.value
        return value


@dataclass(frozen=True)
class CostSchedule:
    """Public default schedule for a small mainland China cash account.

    Broker-specific values remain configurable, but the minimum commission is
    never silently omitted.  Slippage is represented as a cash cost rather
    than by mutating the market quote, matching the existing durable ledger.
    """

    commission_rate: float = 0.0003
    minimum_commission: float = 5.0
    stock_sell_stamp_duty: float = 0.0005
    etf_sell_stamp_duty: float = 0.0
    slippage_rate: float = 0.0005
    version: str = DEFAULT_COST_MODEL_VERSION

    def __post_init__(self) -> None:
        for name in (
            "commission_rate",
            "minimum_commission",
            "stock_sell_stamp_duty",
            "etf_sell_stamp_duty",
            "slippage_rate",
        ):
            if float(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        if not str(self.version).strip():
            raise ValueError("cost model version is required")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "CostSchedule":
        data = values or {}
        return cls(
            commission_rate=float(data.get("trading_fee", data.get("commission_rate", 0.0003))),
            minimum_commission=float(data.get("minimum_commission", 5.0)),
            stock_sell_stamp_duty=float(data.get("stamp_duty", 0.0005)),
            etf_sell_stamp_duty=float(data.get("etf_stamp_duty", 0.0)),
            slippage_rate=float(data.get("slippage_rate", data.get("slippage", 0.0005))),
            version=str(data.get("cost_model_version", data.get("version", DEFAULT_COST_MODEL_VERSION))),
        )

    def calculate(
        self,
        instrument: InstrumentProfile | str,
        side: str,
        *,
        price: float | None = None,
        shares: int | None = None,
        notional: float | None = None,
    ) -> TradeCost:
        profile = (
            instrument
            if isinstance(instrument, InstrumentProfile)
            else InstrumentProfile.from_ticker(instrument)
        )
        normalized_side = str(side or "").strip().lower()
        if normalized_side in {"long", "buy"}:
            normalized_side = "buy"
        elif normalized_side in {"short", "sell"}:
            normalized_side = "sell"
        else:
            raise ValueError(f"unsupported trade side: {side!r}")

        if notional is None:
            if price is None or shares is None:
                raise ValueError("price and shares, or notional, are required")
            notional = float(price) * int(shares)
        gross = max(float(notional), 0.0)
        if gross <= 0:
            commission = stamp_duty = slippage = 0.0
        else:
            commission = max(gross * self.commission_rate, self.minimum_commission)
            stamp_rate = 0.0
            if normalized_side == "sell":
                stamp_rate = (
                    self.etf_sell_stamp_duty
                    if profile.instrument_type is InstrumentType.ETF
                    else self.stock_sell_stamp_duty
                )
            stamp_duty = gross * stamp_rate
            slippage = gross * self.slippage_rate
        total = commission + stamp_duty + slippage
        return TradeCost(
            notional=gross,
            commission=float(commission),
            stamp_duty=float(stamp_duty),
            slippage=float(slippage),
            total=float(total),
            instrument_type=profile.instrument_type,
            side=normalized_side,
            model_version=self.version,
        )

    def max_affordable_quote(
        self,
        instrument: InstrumentProfile | str,
        *,
        cash: float,
        shares: int,
    ) -> float:
        """Return the highest buy quote whose all-in cash cost is affordable.

        This is deliberately solved through ``calculate`` instead of dividing
        cash by a percentage fee.  The configured minimum commission makes the
        cost function piecewise, and omitting it materially overstates the
        executable one-lot boundary for small operational cash balances.
        """
        available_cash = float(cash)
        share_count = int(shares)
        if available_cash <= 0 or share_count <= 0:
            return 0.0
        low, high = 0.0, available_cash / share_count
        for _ in range(64):
            middle = (low + high) / 2.0
            trade_cost = self.calculate(
                instrument,
                "buy",
                price=middle,
                shares=share_count,
            )
            if trade_cost.notional + trade_cost.total <= available_cash:
                low = middle
            else:
                high = middle
        return low

    def one_lot_execution_quote_ceiling(
        self,
        instrument: InstrumentProfile | str,
        *,
        cash: float,
        total_assets: float,
        max_single_position: float,
        lot_size: int,
    ) -> float:
        """Combine the static position cap with the exact all-in cash cap."""
        assets = float(total_assets)
        position_cap = float(max_single_position)
        shares = int(lot_size)
        if assets <= 0 or position_cap <= 0 or shares <= 0:
            return 0.0
        max_quote_by_position = assets * position_cap / shares
        max_quote_by_cash = self.max_affordable_quote(
            instrument,
            cash=float(cash),
            shares=shares,
        )
        return max(0.0, min(max_quote_by_position, max_quote_by_cash))

    def assumption_text(self) -> str:
        return (
            f"commission={self.commission_rate:.4%}, minimum_commission="
            f"{self.minimum_commission:.2f}, stock_sell_stamp_duty="
            f"{self.stock_sell_stamp_duty:.4%}, etf_sell_stamp_duty="
            f"{self.etf_sell_stamp_duty:.4%}, slippage={self.slippage_rate:.4%}, "
            f"model={self.version}"
        )

    def small_account_pressure(self, account_capital: float = 10000.0) -> dict[str, Any]:
        """Describe the minimum-commission drag for the actual 10k account."""
        if float(account_capital) != 10000.0:
            raise ValueError("the current capacity report is intentionally limited to 10000 RMB")
        threshold = (
            self.minimum_commission / self.commission_rate
            if self.commission_rate > 0 else float("inf")
        )
        stock_round_trip = self.calculate(
            "600000", "buy", notional=account_capital
        ).total + self.calculate("600000", "sell", notional=account_capital).total
        etf_round_trip = self.calculate(
            "510300", "buy", notional=account_capital
        ).total + self.calculate("510300", "sell", notional=account_capital).total
        return {
            "account_capital": float(account_capital),
            "minimum_commission_notional_threshold": threshold,
            "full_account_below_minimum_commission_threshold": account_capital < threshold,
            "stock_full_turnover_round_trip_cost": stock_round_trip,
            "stock_full_turnover_round_trip_cost_ratio": stock_round_trip / account_capital,
            "etf_full_turnover_round_trip_cost": etf_round_trip,
            "etf_full_turnover_round_trip_cost_ratio": etf_round_trip / account_capital,
            "cost_model_version": self.version,
        }
