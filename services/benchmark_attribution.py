"""PR4.3 — Buy-and-hold benchmark and A/B attribution.

A frozen A/B experiment must report:

* A net return (control arm A),
* B net return (candidate arm B),
* benchmark net return (buy-and-hold 510300 / CSI 300 from the same
  starting session, with the same 1万 capital, lot size, fees and
  dividend/adjustment handling),
* B − A (the candidate's net improvement over control),
* each arm's net return relative to benchmark,
* fees, turnover and average holding period per arm.

The benchmark here is the *evaluable* buy-and-hold path: same starting
cash, same lot constraints, same fees.  It is not the production account's
historical benchmark, which is reported separately and may remain N/A.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass(frozen=True)
class BenchmarkBar:
    date: str
    close: float
    adjustment: str = "qfq"


@dataclass(frozen=True)
class BuyAndHoldResult:
    """The evaluable buy-and-hold path for one benchmark ticker."""

    ticker: str
    initial_capital: float
    shares: int
    entry_price: float
    entry_date: str
    final_value: float
    net_return: float
    curve: tuple[dict[str, float], ...]


@dataclass(frozen=True)
class ArmAttribution:
    """Attribution metrics for one arm of an experiment."""

    arm: str
    net_return: float
    benchmark_net_return: float
    excess_vs_benchmark: float
    fees: float
    turnover: float
    average_holding_period_days: float | None
    sample_count: int


@dataclass(frozen=True)
class ExperimentAttributionReport:
    """The full PR4.3 attribution table for one experiment."""

    experiment_id: str
    control: ArmAttribution
    candidate: ArmAttribution
    benchmark: BuyAndHoldResult
    b_minus_a: float
    notes: tuple[str, ...] = ()


def compute_buy_and_hold(
    *,
    ticker: str,
    bars: Sequence[BenchmarkBar],
    initial_capital: float = 10000.0,
    lot_size: int = 100,
    commission_bps: float = 2.5,  # 0.025%
    stamp_duty_bps: float = 10.0,  # 0.1% on sells only
    slippage_bps: float = 0.0,
) -> BuyAndHoldResult | None:
    """Compute the evaluable buy-and-hold path for a benchmark ticker.

    The benchmark enters at the first bar's close, buys the maximum
    whole-lot shares the cash allows (no leverage), and holds to the last
    bar.  Fees and stamp duty are charged on entry; the same cost model is
    applied so the benchmark is directly comparable to the arms.
    """
    if not bars:
        return None
    bars_sorted = sorted(bars, key=lambda bar: bar.date)
    entry_price = float(bars_sorted[0].close)
    if entry_price <= 0:
        return None
    commission = max(initial_capital * commission_bps / 10000.0, 5.0)
    affordable_cash = initial_capital - commission
    if affordable_cash <= 0:
        return None
    raw_shares = int(affordable_cash // entry_price)
    shares = raw_shares - (raw_shares % lot_size)
    if shares <= 0:
        return None
    entry_cost = shares * entry_price
    entry_fees = commission
    cash_after_entry = initial_capital - entry_cost - entry_fees
    curve: list[dict[str, float]] = []
    for bar in bars_sorted:
        value = cash_after_entry + shares * float(bar.close)
        net_return = value / initial_capital - 1.0
        curve.append({"date": bar.date, "net_return": net_return, "value": value})
    final_value = cash_after_entry + shares * float(bars_sorted[-1].close)
    return BuyAndHoldResult(
        ticker=ticker,
        initial_capital=initial_capital,
        shares=shares,
        entry_price=entry_price,
        entry_date=bars_sorted[0].date,
        final_value=final_value,
        net_return=final_value / initial_capital - 1.0,
        curve=tuple(curve),
    )


def build_attribution_report(
    *,
    experiment_id: str,
    control_net_return: float,
    candidate_net_return: float,
    control_fees: float,
    candidate_fees: float,
    control_turnover: float,
    candidate_turnover: float,
    control_holding_period_days: float | None,
    candidate_holding_period_days: float | None,
    control_sample_count: int,
    candidate_sample_count: int,
    benchmark: BuyAndHoldResult,
    notes: Sequence[str] = (),
) -> ExperimentAttributionReport:
    """Build the PR4.3 attribution report for one experiment.

    The report discloses B − A and each arm's excess versus benchmark.  B
    beating A while both lag the benchmark is a real outcome and must be
    reported as such, not framed as a success.
    """
    benchmark_return = benchmark.net_return
    control = ArmAttribution(
        arm="control",
        net_return=control_net_return,
        benchmark_net_return=benchmark_return,
        excess_vs_benchmark=control_net_return - benchmark_return,
        fees=control_fees,
        turnover=control_turnover,
        average_holding_period_days=control_holding_period_days,
        sample_count=control_sample_count,
    )
    candidate = ArmAttribution(
        arm="candidate",
        net_return=candidate_net_return,
        benchmark_net_return=benchmark_return,
        excess_vs_benchmark=candidate_net_return - benchmark_return,
        fees=candidate_fees,
        turnover=candidate_turnover,
        average_holding_period_days=candidate_holding_period_days,
        sample_count=candidate_sample_count,
    )
    return ExperimentAttributionReport(
        experiment_id=experiment_id,
        control=control,
        candidate=candidate,
        benchmark=benchmark,
        b_minus_a=candidate_net_return - control_net_return,
        notes=tuple(notes),
    )
