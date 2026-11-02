"""Canonical next-session, lot-aware shadow evaluation engine.

This engine is independent from the production simulation account.  It uses a
full trading-session sequence, executes targets no earlier than the following
session open, marks holdings every session, and applies the exact same
``CostSchedule`` used by simulated fills.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.instruments import InstrumentProfile, is_etf_ticker, normalize_ticker
from ..domain.portfolio.shadow import CandidateSignal, RiskDecision, RiskStatus, SleeveId, TargetPortfolio


ENGINE_VERSION = "canonical_shadow_event_engine_v2"


def _day(value: Any) -> str:
    return str(value or "")[:10]


@dataclass(frozen=True)
class DailyBar:
    ticker: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    source: str = ""
    provider: str = ""
    fetched_at: str = ""
    adjustment: str = "qfq"
    quality_status: str = "validated"
    trade_status: str = "normal"

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", normalize_ticker(self.ticker))
        object.__setattr__(self, "date", _day(self.date))
        if min(float(self.open), float(self.high), float(self.low), float(self.close)) <= 0:
            raise ValueError("OHLC prices must be positive")
        if float(self.high) < max(float(self.open), float(self.close), float(self.low)):
            raise ValueError("bar high is inconsistent")
        if float(self.low) > min(float(self.open), float(self.close), float(self.high)):
            raise ValueError("bar low is inconsistent")

    @property
    def tradable(self) -> bool:
        return (
            str(self.quality_status).strip().lower() == "validated"
            and str(self.trade_status).strip().lower() == "normal"
        )


@dataclass(frozen=True)
class EvaluationSpec:
    sleeve: SleeveId
    strategy_version: str
    initial_capital: float = 10000.0
    cagr_target: float = 0.15
    max_drawdown_budget: float = 0.15
    risk_reduction_drawdown: float = 0.10
    minimum_annualization_sessions: int = 252
    qualification_sessions: int = 756
    minimum_closed_trades: int = 0
    rebalance_band: float = 0.05
    lot_size: int = 100
    require_persistent_calendar: bool = True
    require_instrument_master: bool = True
    require_instrument_lifecycle_metadata: bool = False
    minimum_dsr_probability: float = 0.95
    maximum_pbo_probability: float = 0.50

    def __post_init__(self) -> None:
        if float(self.initial_capital) != 10000.0:
            raise ValueError("evaluation capital is fixed at 10000")
        if not 0 < float(self.risk_reduction_drawdown) < float(self.max_drawdown_budget) < 1:
            raise ValueError("drawdown thresholds must be ordered within (0, 1)")
        if int(self.minimum_annualization_sessions) < 2:
            raise ValueError("minimum annualization sessions must be at least 2")
        if not 0 < float(self.minimum_dsr_probability) < 1:
            raise ValueError("minimum_dsr_probability must be within (0, 1)")
        if not 0 < float(self.maximum_pbo_probability) < 1:
            raise ValueError("maximum_pbo_probability must be within (0, 1)")

    @property
    def strategy_hash(self) -> str:
        # Hash only choices that alter the generated portfolio path.  Reporting
        # gates (sample length, DSR/PBO thresholds, metadata requirements) do
        # not create a new strategy trial and must not inflate multiple-testing
        # evidence when an evaluator becomes stricter.
        payload = {
            "sleeve": self.sleeve.value,
            "strategy_version": self.strategy_version,
            "initial_capital": self.initial_capital,
            "max_drawdown_budget": self.max_drawdown_budget,
            "risk_reduction_drawdown": self.risk_reduction_drawdown,
            "rebalance_band": self.rebalance_band,
            "lot_size": self.lot_size,
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()


@dataclass
class _Position:
    shares: int
    average_cost: float
    acquired_by_date: dict[str, int] = field(default_factory=dict)
    last_mark: float | None = None
    stale_sessions: int = 0

    def available_to_sell(self, on_date: str) -> int:
        acquired_today = int(self.acquired_by_date.get(on_date, 0))
        return max(0, int(self.shares) - acquired_today)


@dataclass(frozen=True)
class EvaluationFill:
    ticker: str
    side: str
    shares: int
    price: float
    signal_date: str
    fill_date: str
    notional: float
    commission: float
    stamp_duty: float
    slippage_cost: float
    total_cost: float
    cost_model_version: str


@dataclass(frozen=True)
class EvaluationResult:
    spec: EvaluationSpec
    metrics: Mapping[str, Any]
    curve: tuple[Mapping[str, Any], ...]
    fills: tuple[EvaluationFill, ...]
    signals: tuple[CandidateSignal, ...]
    portfolios: tuple[TargetPortfolio, ...]
    status: str
    universe: tuple[str, ...]
    data_cutoff: str
    engine_version: str = ENGINE_VERSION

    def as_dict(self, *, include_curve: bool = True) -> dict[str, Any]:
        return {
            "spec": {
                **asdict(self.spec),
                "sleeve": self.spec.sleeve.value,
                "strategy_hash": self.spec.strategy_hash,
            },
            "metrics": dict(self.metrics),
            "curve": [dict(row) for row in self.curve] if include_curve else [],
            "fills": [asdict(fill) for fill in self.fills],
            "signals": [signal.as_dict() for signal in self.signals],
            "portfolios": [
                {
                    **asdict(portfolio),
                    "sleeve": portfolio.sleeve.value,
                    "targets": dict(portfolio.targets),
                    "risk_status": portfolio.risk_status.value,
                    "risk_reasons": list(portfolio.risk_reasons),
                }
                for portfolio in self.portfolios
            ],
            "status": self.status,
            "universe": list(self.universe),
            "data_cutoff": self.data_cutoff,
            "engine_version": self.engine_version,
        }


class CanonicalEvaluationEngine:
    def __init__(self, cost_schedule: CostSchedule | None = None):
        self.cost_schedule = cost_schedule or CostSchedule()

    def run(
        self,
        bars: Iterable[DailyBar],
        targets_by_signal_date: Mapping[str, Mapping[str, float]],
        spec: EvaluationSpec,
        *,
        signals: Sequence[CandidateSignal] = (),
        benchmark_ticker: str | None = None,
        trading_sessions: Sequence[str] | None = None,
        instruments: Mapping[str, InstrumentProfile] | None = None,
        qualification_evidence: Mapping[str, Any] | None = None,
    ) -> EvaluationResult:
        bar_list = sorted(bars, key=lambda bar: (bar.date, bar.ticker))
        by_date: dict[str, dict[str, DailyBar]] = {}
        for bar in bar_list:
            if str(bar.quality_status or "validated").strip().lower() != "validated":
                continue
            by_date.setdefault(bar.date, {})[bar.ticker] = bar
        persistent_calendar_supplied = trading_sessions is not None
        session_source = by_date.keys() if trading_sessions is None else trading_sessions
        sessions = sorted({_day(value) for value in session_source if _day(value)})
        if not sessions:
            return self._empty_result(spec, signals)

        normalized_targets = {
            _day(signal_date): {
                normalize_ticker(ticker): float(weight)
                for ticker, weight in targets.items()
                if float(weight) > 0
            }
            for signal_date, targets in targets_by_signal_date.items()
        }
        profiles = {
            normalize_ticker(ticker): profile
            for ticker, profile in (instruments or {}).items()
        }
        missing_instrument_profiles = sorted(
            {
                ticker
                for targets in normalized_targets.values()
                for ticker in targets
                if ticker not in profiles
            }
        )
        incomplete_lifecycle_profiles = sorted(
            {
                ticker
                for targets in normalized_targets.values()
                for ticker in targets
                if ticker in profiles and not profiles[ticker].listed_at
            }
        )
        execution_schedule: dict[str, tuple[str, dict[str, float]]] = {}
        unexecuted_signal_dates: list[str] = []
        for signal_date in sorted(normalized_targets):
            index = bisect_right(sessions, signal_date)
            if index >= len(sessions):
                unexecuted_signal_dates.append(signal_date)
                continue
            fill_session = sessions[index]
            # If several signals arrive before the same next session, only the
            # latest fully formed target is actionable at that open.
            execution_schedule[fill_session] = (
                signal_date,
                normalized_targets[signal_date],
            )
        positions: dict[str, _Position] = {}
        cash = float(spec.initial_capital)
        peak_assets = cash
        curve: list[dict[str, Any]] = []
        fills: list[EvaluationFill] = []
        portfolios: list[TargetPortfolio] = []
        last_marks: dict[str, float] = {}
        total_turnover = 0.0
        total_cost = 0.0
        missing_open_rejections = 0
        suspended_or_inactive_rejections = 0
        stale_mark_sessions = 0
        closed_trades = 0

        benchmark_code = normalize_ticker(benchmark_ticker) if benchmark_ticker else ""
        benchmark_first: float | None = None
        benchmark_last: float | None = None

        for session in sessions:
            session_bars = by_date.get(session, {})
            open_assets = self._portfolio_value(cash, positions, session_bars, last_marks, use_open=True)
            current_drawdown = open_assets / peak_assets - 1.0 if peak_assets > 0 else 0.0
            day_turnover = 0.0
            day_cost = 0.0
            day_fills = 0
            notes: list[str] = []

            scheduled = execution_schedule.get(session)
            if scheduled is not None:
                signal_date, requested = scheduled
                risk = self.apply_drawdown_risk(
                    requested,
                    positions,
                    open_assets,
                    session_bars,
                    last_marks,
                    current_drawdown,
                    spec,
                )
                target = self._apply_rebalance_band(
                    risk.approved_targets,
                    positions,
                    open_assets,
                    session_bars,
                    last_marks,
                    spec.rebalance_band,
                )
                portfolios.append(
                    TargetPortfolio(
                        sleeve=spec.sleeve,
                        as_of=signal_date,
                        strategy_version=spec.strategy_version,
                        targets=target,
                        initial_capital=spec.initial_capital,
                        risk_status=risk.status,
                        risk_reasons=risk.reasons,
                    )
                )
                trade_result = self._rebalance(
                    session,
                    signal_date,
                    target,
                    cash,
                    positions,
                    session_bars,
                    last_marks,
                    spec,
                    profiles,
                )
                cash = trade_result[0]
                positions = trade_result[1]
                new_fills = trade_result[2]
                missing_open_rejections += trade_result[3]
                suspended_or_inactive_rejections += trade_result[4]
                closed_trades += trade_result[5]
                fills.extend(new_fills)
                day_turnover = sum(fill.notional for fill in new_fills)
                day_cost = sum(fill.total_cost for fill in new_fills)
                day_fills = len(new_fills)
                total_turnover += day_turnover
                total_cost += day_cost
                notes.extend(risk.reasons)

            market_value = 0.0
            stale_tickers: list[str] = []
            stale_days: dict[str, int] = {}
            for ticker, position in positions.items():
                bar = session_bars.get(ticker)
                if bar is not None:
                    mark = float(bar.close)
                    position.last_mark = mark
                    position.stale_sessions = 0
                    last_marks[ticker] = mark
                else:
                    mark = position.last_mark or last_marks.get(ticker)
                    position.stale_sessions += 1
                    stale_mark_sessions += 1
                    stale_tickers.append(ticker)
                    stale_days[ticker] = position.stale_sessions
                if mark is not None:
                    market_value += position.shares * mark

            total_assets = cash + market_value
            peak_assets = max(peak_assets, total_assets)
            drawdown = total_assets / peak_assets - 1.0 if peak_assets > 0 else 0.0
            prior_assets = curve[-1]["total_assets"] if curve else spec.initial_capital
            net_return = total_assets / prior_assets - 1.0 if prior_assets > 0 else 0.0

            benchmark_bar = session_bars.get(benchmark_code) if benchmark_code else None
            if benchmark_bar is not None:
                benchmark_first = benchmark_first or float(benchmark_bar.close)
                benchmark_last = float(benchmark_bar.close)

            curve.append(
                {
                    "date": session,
                    "cash": cash,
                    "market_value": market_value,
                    "total_assets": total_assets,
                    "net_return": net_return,
                    "drawdown": drawdown,
                    "gross_exposure": market_value / total_assets if total_assets > 0 else 0.0,
                    "turnover": day_turnover,
                    "cost": day_cost,
                    "fill_count": day_fills,
                    "stale_tickers": sorted(stale_tickers),
                    "stale_days": stale_days,
                    "notes": sorted(set(notes)),
                }
            )

        metrics = self._metrics(
            curve,
            fills,
            total_turnover,
            total_cost,
            benchmark_first,
            benchmark_last,
            missing_open_rejections,
            suspended_or_inactive_rejections,
            stale_mark_sessions,
            closed_trades,
            spec,
            persistent_calendar_supplied,
            unexecuted_signal_dates,
            qualification_evidence or {},
            missing_instrument_profiles,
            incomplete_lifecycle_profiles,
        )
        universe = tuple(sorted({bar.ticker for bar in bar_list}))
        if not metrics["data_quality"]["usable"]:
            status = "data_unavailable"
        else:
            status = "qualified" if metrics["qualification"]["qualified"] else "not_proven"
        return EvaluationResult(
            spec=spec,
            metrics=metrics,
            curve=tuple(curve),
            fills=tuple(fills),
            signals=tuple(signals),
            portfolios=tuple(portfolios),
            status=status,
            universe=universe,
            data_cutoff=sessions[-1],
        )

    @staticmethod
    def apply_drawdown_risk(
        requested: Mapping[str, float],
        positions: Mapping[str, _Position],
        total_assets: float,
        bars: Mapping[str, DailyBar],
        last_marks: Mapping[str, float],
        drawdown: float,
        spec: EvaluationSpec,
    ) -> RiskDecision:
        requested_clean = {normalize_ticker(k): max(0.0, float(v)) for k, v in requested.items()}
        current_weights: dict[str, float] = {}
        if total_assets > 0:
            for ticker, position in positions.items():
                bar = bars.get(ticker)
                price = float(bar.open) if bar else last_marks.get(ticker)
                if price:
                    current_weights[ticker] = position.shares * price / total_assets

        if drawdown <= -spec.max_drawdown_budget:
            approved = {
                ticker: min(weight, current_weights.get(ticker, 0.0))
                for ticker, weight in requested_clean.items()
                if min(weight, current_weights.get(ticker, 0.0)) > 0
            }
            return RiskDecision(
                RiskStatus.BLOCKED_NEW_RISK,
                approved,
                ("drawdown_15pct_block_new_risk",),
                drawdown,
            )
        if drawdown <= -spec.risk_reduction_drawdown:
            approved: dict[str, float] = {}
            for ticker, requested_weight in requested_clean.items():
                current_weight = current_weights.get(ticker, 0.0)
                if requested_weight <= current_weight:
                    approved[ticker] = requested_weight
                else:
                    approved[ticker] = current_weight + (
                        requested_weight - current_weight
                    ) * 0.5
            return RiskDecision(
                RiskStatus.REDUCED,
                approved,
                ("drawdown_10pct_reduce_new_risk",),
                drawdown,
            )
        return RiskDecision(RiskStatus.APPROVED, requested_clean, (), drawdown)

    def _rebalance(
        self,
        session: str,
        signal_date: str,
        targets: Mapping[str, float],
        cash: float,
        positions: dict[str, _Position],
        bars: Mapping[str, DailyBar],
        last_marks: Mapping[str, float],
        spec: EvaluationSpec,
        profiles: Mapping[str, InstrumentProfile],
    ) -> tuple[float, dict[str, _Position], list[EvaluationFill], int, int, int]:
        positions = {
            ticker: _Position(
                shares=position.shares,
                average_cost=position.average_cost,
                acquired_by_date=dict(position.acquired_by_date),
                last_mark=position.last_mark,
                stale_sessions=position.stale_sessions,
            )
            for ticker, position in positions.items()
        }
        portfolio_value = self._portfolio_value(cash, positions, bars, last_marks, use_open=True)
        fills: list[EvaluationFill] = []
        missing = 0
        untradable = 0
        closed = 0

        desired_shares: dict[str, int] = {}
        for ticker, weight in targets.items():
            bar = bars.get(ticker)
            if bar is None:
                missing += 1
                continue
            profile = profiles.get(ticker) or InstrumentProfile.from_ticker(ticker)
            if not bar.tradable or not profile.is_active_on(session):
                untradable += 1
                continue
            lot_size = int(profile.lot_size or spec.lot_size)
            desired_shares[ticker] = int(
                portfolio_value * max(0.0, float(weight)) / bar.open / lot_size
            ) * lot_size

        # Exits and reductions release cash before any increase.
        for ticker in sorted(set(positions) | set(desired_shares)):
            position = positions.get(ticker)
            if position is None:
                continue
            wanted = desired_shares.get(ticker, 0)
            shares = min(max(position.shares - wanted, 0), position.available_to_sell(session))
            if shares <= 0:
                continue
            bar = bars.get(ticker)
            if bar is None:
                missing += 1
                continue
            profile = profiles.get(ticker) or InstrumentProfile.from_ticker(ticker)
            if not bar.tradable or not profile.is_active_on(session):
                untradable += 1
                continue
            cost = self.cost_schedule.calculate(ticker, "sell", price=bar.open, shares=shares)
            cash += cost.notional - cost.total
            position.shares -= shares
            fills.append(self._fill(ticker, "sell", shares, bar.open, signal_date, session, cost))
            if position.shares <= 0:
                del positions[ticker]
                closed += 1

        for ticker in sorted(desired_shares):
            current = positions.get(ticker)
            current_shares = current.shares if current else 0
            shares = max(desired_shares[ticker] - current_shares, 0)
            if shares <= 0:
                continue
            bar = bars.get(ticker)
            if bar is None:
                missing += 1
                continue
            profile = profiles.get(ticker) or InstrumentProfile.from_ticker(ticker)
            if not bar.tradable or not profile.is_active_on(session):
                untradable += 1
                continue
            lot_size = int(profile.lot_size or spec.lot_size)
            while shares > 0:
                cost = self.cost_schedule.calculate(ticker, "buy", price=bar.open, shares=shares)
                if cost.notional + cost.total <= cash + 1e-9:
                    break
                shares -= lot_size
            if shares <= 0:
                continue
            cost = self.cost_schedule.calculate(ticker, "buy", price=bar.open, shares=shares)
            cash -= cost.notional + cost.total
            if current is None:
                current = _Position(shares=0, average_cost=0.0, last_mark=bar.open)
                positions[ticker] = current
            old_notional = current.average_cost * current.shares
            current.shares += shares
            current.average_cost = (old_notional + cost.notional) / current.shares
            current.acquired_by_date[session] = current.acquired_by_date.get(session, 0) + shares
            fills.append(self._fill(ticker, "buy", shares, bar.open, signal_date, session, cost))
        return cash, positions, fills, missing, untradable, closed

    @staticmethod
    def _fill(ticker: str, side: str, shares: int, price: float, signal_date: str, fill_date: str, cost) -> EvaluationFill:
        return EvaluationFill(
            ticker=ticker,
            side=side,
            shares=shares,
            price=float(price),
            signal_date=signal_date,
            fill_date=fill_date,
            notional=cost.notional,
            commission=cost.commission,
            stamp_duty=cost.stamp_duty,
            slippage_cost=cost.slippage,
            total_cost=cost.total,
            cost_model_version=cost.model_version,
        )

    @staticmethod
    def _portfolio_value(cash, positions, bars, last_marks, *, use_open: bool) -> float:
        value = float(cash)
        for ticker, position in positions.items():
            bar = bars.get(ticker)
            price = (float(bar.open) if use_open else float(bar.close)) if bar else (position.last_mark or last_marks.get(ticker))
            if price:
                value += position.shares * price
        return value

    @staticmethod
    def _apply_rebalance_band(targets, positions, assets, bars, last_marks, band):
        if assets <= 0 or band <= 0:
            return dict(targets)
        adjusted = dict(targets)
        tickers = set(targets) | set(positions)
        for ticker in tickers:
            position = positions.get(ticker)
            bar = bars.get(ticker)
            price = float(bar.open) if bar else last_marks.get(ticker)
            current = position.shares * price / assets if position and price else 0.0
            requested = float(targets.get(ticker, 0.0))
            if abs(requested - current) < band:
                if current > 0:
                    adjusted[ticker] = current
                else:
                    adjusted.pop(ticker, None)
        gross = sum(adjusted.values())
        if gross > 1.0:
            adjusted = {ticker: weight / gross for ticker, weight in adjusted.items()}
        return adjusted

    def _metrics(
        self,
        curve,
        fills,
        total_turnover,
        total_cost,
        benchmark_first,
        benchmark_last,
        missing_open_rejections,
        suspended_or_inactive_rejections,
        stale_mark_sessions,
        closed_trades,
        spec,
        persistent_calendar_supplied,
        unexecuted_signal_dates,
        qualification_evidence,
        missing_instrument_profiles,
        incomplete_lifecycle_profiles,
    ):
        sessions = len(curve)
        ending = float(curve[-1]["total_assets"])
        total_return = ending / spec.initial_capital - 1.0
        annualized = None
        annualization_status = "insufficient_history"
        if sessions >= spec.minimum_annualization_sessions and total_return > -1:
            annualized = (1.0 + total_return) ** (252.0 / sessions) - 1.0
            annualization_status = "available"
        returns = [float(row["net_return"]) for row in curve[1:]]
        std = pstdev(returns) if len(returns) > 1 else 0.0
        sharpe = mean(returns) / std * math.sqrt(252) if std > 0 else None
        max_drawdown = min((float(row["drawdown"]) for row in curve), default=0.0)
        benchmark_return = (
            benchmark_last / benchmark_first - 1.0
            if benchmark_first and benchmark_last and benchmark_first > 0
            else None
        )
        excess = total_return - benchmark_return if benchmark_return is not None else None
        rolling = []
        if sessions >= 252:
            for index in range(251, sessions):
                start_assets = float(curve[index - 251]["total_assets"])
                end_assets = float(curve[index]["total_assets"])
                rolling.append(
                    {
                        "start": curve[index - 251]["date"],
                        "end": curve[index]["date"],
                        "return": end_assets / start_assets - 1.0 if start_assets > 0 else None,
                    }
                )
        qualification_reasons = []
        if sessions < spec.qualification_sessions:
            qualification_reasons.append("insufficient_sessions")
        if spec.minimum_closed_trades and closed_trades < spec.minimum_closed_trades:
            qualification_reasons.append("insufficient_closed_trades")
        if annualized is None or annualized < spec.cagr_target:
            qualification_reasons.append("cagr_target_not_proven")
        if max_drawdown < -spec.max_drawdown_budget:
            qualification_reasons.append("drawdown_budget_breached")
        if excess is None or excess <= 0:
            qualification_reasons.append("positive_benchmark_excess_not_proven")
        stress_total_return = qualification_evidence.get("stress_total_return")
        if stress_total_return is None or float(stress_total_return) <= 0:
            qualification_reasons.append("three_x_slippage_profit_not_proven")
        dsr_probability = qualification_evidence.get("dsr_probability")
        if dsr_probability is None or float(dsr_probability) < spec.minimum_dsr_probability:
            qualification_reasons.append("dsr_not_proven")
        pbo_probability = qualification_evidence.get("pbo_probability")
        if pbo_probability is None or float(pbo_probability) >= spec.maximum_pbo_probability:
            qualification_reasons.append("pbo_not_proven")
        calendar_ok = persistent_calendar_supplied or not spec.require_persistent_calendar
        data_reasons: list[str] = []
        if not calendar_ok:
            data_reasons.append("persistent_calendar_not_supplied")
        if spec.require_instrument_master and missing_instrument_profiles:
            data_reasons.append("instrument_master_incomplete")
        if (
            spec.require_instrument_lifecycle_metadata
            and incomplete_lifecycle_profiles
        ):
            data_reasons.append("instrument_lifecycle_metadata_incomplete")
        if missing_open_rejections:
            data_reasons.append("candidate_or_exit_open_missing")
        if stale_mark_sessions:
            data_reasons.append("held_mark_stale")
        if data_reasons:
            qualification_reasons.append("validated_price_lifecycle_incomplete")
        return {
            "initial_capital": spec.initial_capital,
            "ending_assets": ending,
            "sessions": sessions,
            "total_return": total_return,
            "net_cagr": annualized,
            "annualization_status": annualization_status,
            "max_drawdown": max_drawdown,
            "sharpe": sharpe,
            "turnover": total_turnover / spec.initial_capital,
            "cost_paid": total_cost,
            "cost_ratio": total_cost / spec.initial_capital,
            "fill_count": len(fills),
            "closed_trade_count": closed_trades,
            "benchmark_total_return": benchmark_return,
            "benchmark_excess_return": excess,
            "rolling_252_session_returns": rolling,
            "missing_open_rejections": missing_open_rejections,
            "suspended_or_inactive_rejections": suspended_or_inactive_rejections,
            "stale_mark_sessions": stale_mark_sessions,
            "unexecuted_signal_dates": list(unexecuted_signal_dates),
            "missing_instrument_profiles": list(missing_instrument_profiles),
            "incomplete_lifecycle_profiles": list(incomplete_lifecycle_profiles),
            "cost_assumption": self.cost_schedule.assumption_text(),
            "statistical_validation": {
                "stress_total_return": stress_total_return,
                "dsr_probability": dsr_probability,
                "pbo_probability": pbo_probability,
                "minimum_dsr_probability": spec.minimum_dsr_probability,
                "maximum_pbo_probability": spec.maximum_pbo_probability,
            },
            "data_quality": {
                "usable": not data_reasons,
                "reasons": data_reasons,
                "persistent_calendar_supplied": persistent_calendar_supplied,
            },
            "qualification": {
                "qualified": not qualification_reasons,
                "label": "已证明" if not qualification_reasons else "未证明",
                "reasons": qualification_reasons,
                "cagr_target": spec.cagr_target,
                "max_drawdown_budget": spec.max_drawdown_budget,
            },
        }

    @staticmethod
    def _empty_result(spec, signals):
        return EvaluationResult(
            spec=spec,
            metrics={
                "initial_capital": spec.initial_capital,
                "sessions": 0,
                "total_return": 0.0,
                "net_cagr": None,
                "annualization_status": "insufficient_history",
                "data_quality": {"usable": False, "reasons": ["no_validated_sessions"]},
                "qualification": {"qualified": False, "label": "未证明", "reasons": ["no_validated_sessions"]},
            },
            curve=(),
            fills=(),
            signals=tuple(signals),
            portfolios=(),
            status="data_unavailable",
            universe=(),
            data_cutoff="",
        )


class SystematicETFMomentumV1:
    """Frozen, transparent diagnostic baseline; no parameter search."""

    version = "systematic_etf_momentum_v1"

    def build(self, bars: Iterable[DailyBar]) -> tuple[dict[str, dict[str, float]], list[CandidateSignal]]:
        by_ticker: dict[str, list[DailyBar]] = {}
        by_date: dict[str, dict[str, DailyBar]] = {}
        for bar in sorted(bars, key=lambda item: (item.date, item.ticker)):
            if not is_etf_ticker(bar.ticker) or bar.quality_status != "validated":
                continue
            by_ticker.setdefault(bar.ticker, []).append(bar)
            by_date.setdefault(bar.date, {})[bar.ticker] = bar
        dates = sorted(by_date)
        month_ends = {
            current
            for index, current in enumerate(dates)
            if index == len(dates) - 1 or dates[index + 1][:7] != current[:7]
        }
        targets: dict[str, dict[str, float]] = {}
        signals: list[CandidateSignal] = []
        for current in sorted(month_ends):
            ranked: list[tuple[float, str]] = []
            for ticker, history in by_ticker.items():
                available = [bar for bar in history if bar.date <= current]
                # A historical last price is valid for marking an existing
                # holding, never for creating a fresh month-end candidate.
                if len(available) < 253 or available[-1].date != current:
                    continue
                close = available[-1].close
                sma200 = mean(bar.close for bar in available[-200:])
                momentum_12_1 = available[-22].close / available[-253].close - 1.0
                if close > sma200:
                    ranked.append((float(momentum_12_1), ticker))
            selected = sorted(ranked, reverse=True)[:10]
            if not selected:
                targets[current] = {}
                continue
            weight = min(0.10, 1.0 / len(selected))
            targets[current] = {ticker: weight for _score, ticker in selected}
            for score, ticker in selected:
                signals.append(
                    CandidateSignal(
                        ticker=ticker,
                        direction="long",
                        score=score,
                        signal_at=f"{current}T15:00:00",
                        available_at=f"{current}T15:00:00",
                        sleeve=SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1,
                        metadata={"rule": "12_1_momentum_above_sma200", "frozen": True},
                    )
                )
        return targets, signals


def bars_from_rows(rows: Iterable[Mapping[str, Any] | Sequence[Any]]) -> list[DailyBar]:
    result: list[DailyBar] = []
    for row in rows:
        try:
            if isinstance(row, Mapping):
                result.append(
                    DailyBar(
                        ticker=str(row["ticker"]),
                        date=str(row["date"]),
                        open=float(row.get("open") or row["close"]),
                        high=float(row.get("high") or row["close"]),
                        low=float(row.get("low") or row["close"]),
                        close=float(row["close"]),
                        volume=float(row.get("volume") or 0.0),
                        source=str(row.get("source") or ""),
                        provider=str(row.get("provider") or row.get("source") or ""),
                        fetched_at=str(row.get("fetched_at") or ""),
                        adjustment=str(row.get("adjustment") or "qfq"),
                        quality_status=str(row.get("quality_status") or "validated"),
                        trade_status=str(row.get("trade_status") or "normal"),
                    )
                )
            else:
                result.append(DailyBar(*row[:7]))
        except (KeyError, TypeError, ValueError):
            continue
    return result
