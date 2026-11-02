"""Orchestration for isolated systematic and forward-only LLM shadow sleeves."""

from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import date
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.instruments import InstrumentProfile, InstrumentType, normalize_ticker
from ..domain.portfolio.shadow import CandidateSignal, SleeveId
from .evaluation_engine import (
    CanonicalEvaluationEngine,
    DailyBar,
    EvaluationResult,
    EvaluationSpec,
    SystematicETFMomentumV1,
    bars_from_rows,
)
from .experiment_store import ExperimentStore, TrialReturnSeries, stable_hash
from .statistical_validation import validate_dated_trial_family


LLM_FORWARD_START = "2026-04-01"
LLM_STRATEGY_VERSION = "llm_research_forward_v1"
DEFAULT_BENCHMARK = "510300"


@dataclass(frozen=True)
class ProductionSimulationFingerprint:
    cash: str | None
    trade_count: int
    positions_json: str


@dataclass(frozen=True)
class ShadowRunOutcome:
    result: EvaluationResult
    data_hash: str
    persisted_run_id: str | None
    strategy_trial_count: int
    evaluation_run_count: int


class MarketTruthRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def sessions(self) -> list[str]:
        with closing(sqlite3.connect(self.db_path)) as conn, conn:
            if not _table_exists(conn, "trading_calendar"):
                return []
            return [
                str(row[0])[:10]
                for row in conn.execute(
                    """
                    SELECT trade_date FROM trading_calendar
                    WHERE market = 'CN' AND is_open = 1
                      AND quality_status = 'validated'
                    ORDER BY trade_date
                    """
                ).fetchall()
            ]

    def instruments(self) -> dict[str, InstrumentProfile]:
        with closing(sqlite3.connect(self.db_path)) as conn, conn:
            if not _table_exists(conn, "instrument_master"):
                return {}
            rows = conn.execute(
                """
                SELECT ticker, instrument_type, lot_size, listed_at,
                       delisted_at, source
                FROM instrument_master ORDER BY ticker
                """
            ).fetchall()
        profiles: dict[str, InstrumentProfile] = {}
        for ticker, instrument_type, lot_size, listed_at, delisted_at, source in rows:
            try:
                profiles[normalize_ticker(ticker)] = InstrumentProfile(
                    ticker=str(ticker),
                    instrument_type=InstrumentType(str(instrument_type)),
                    lot_size=int(lot_size or 100),
                    listed_at=str(listed_at)[:10] if listed_at else None,
                    delisted_at=str(delisted_at)[:10] if delisted_at else None,
                    source=str(source or "instrument_master"),
                )
            except (TypeError, ValueError):
                continue
        return profiles

    def validated_bars(
        self,
        *,
        etf_only: bool = False,
        start: str | None = None,
        end: str | None = None,
        tickers: Iterable[str] | None = None,
    ) -> list[DailyBar]:
        with closing(sqlite3.connect(self.db_path)) as conn, conn:
            if not _table_exists(conn, "daily_prices"):
                return []
            columns = _columns(conn, "daily_prices")
            required = {
                "ticker", "date", "open", "high", "low", "close",
                "provider", "fetched_at", "quality_status", "adjustment",
                "volume", "source",
            }
            if not required.issubset(columns):
                return []
            trade_status_sql = (
                "COALESCE(trade_status, 'normal')"
                if "trade_status" in columns else "'normal'"
            )
            clauses = [
                "quality_status = 'validated'",
                "provider IS NOT NULL AND trim(provider) <> ''",
                "fetched_at IS NOT NULL AND trim(fetched_at) <> ''",
                "adjustment = 'qfq'",
                "open > 0 AND high > 0 AND low > 0 AND close > 0",
            ]
            parameters: list[Any] = []
            if start:
                clauses.append("date >= ?")
                parameters.append(str(start)[:10])
            if end:
                clauses.append("date <= ?")
                parameters.append(str(end)[:10])
            normalized_tickers = sorted(
                {
                    normalize_ticker(ticker)
                    for ticker in (tickers or ())
                    if normalize_ticker(ticker)
                }
            )
            if normalized_tickers:
                clauses.append(
                    f"ticker IN ({','.join('?' for _ in normalized_tickers)})"
                )
                parameters.extend(normalized_tickers)
            rows = conn.execute(
                f"""
                SELECT ticker, date, open, high, low, close,
                       COALESCE(volume, 0), COALESCE(source, ''), provider,
                       fetched_at, adjustment, quality_status,
                       {trade_status_sql} AS trade_status
                FROM daily_prices
                WHERE {' AND '.join(clauses)}
                ORDER BY date, ticker
                """,
                parameters,
            ).fetchall()
        mappings = [
            {
                "ticker": row[0], "date": row[1], "open": row[2],
                "high": row[3], "low": row[4], "close": row[5],
                "volume": row[6], "source": row[7], "provider": row[8],
                "fetched_at": row[9], "adjustment": row[10],
                "quality_status": row[11], "trade_status": row[12],
            }
            for row in rows
        ]
        bars = bars_from_rows(mappings)
        if not etf_only:
            return bars
        profiles = self.instruments()
        return [
            bar
            for bar in bars
            if (
                profiles.get(bar.ticker, InstrumentProfile.from_ticker(bar.ticker))
            ).instrument_type is InstrumentType.ETF
        ]

    def llm_forward_targets(
        self,
        *,
        start: str = LLM_FORWARD_START,
    ) -> tuple[dict[str, dict[str, float]], list[CandidateSignal]]:
        """Use only timestamped records observed since the forward-test boundary."""
        with closing(sqlite3.connect(self.db_path)) as conn, conn:
            if not _table_exists(conn, "price_predictions"):
                return {}, []
            columns = _columns(conn, "price_predictions")
            wanted = [
                name for name in
                ("id", "ticker", "direction", "confidence", "predicted_at", "round_id", "decision_id")
                if name in columns
            ]
            if not {"ticker", "predicted_at"}.issubset(columns):
                return {}, []
            order_id_sql = "id" if "id" in columns else "rowid"
            rows = conn.execute(
                f"""
                SELECT {', '.join(wanted)} FROM price_predictions
                WHERE predicted_at IS NOT NULL
                  AND datetime(predicted_at) >= datetime(?)
                ORDER BY datetime(predicted_at), ticker, {order_id_sql}
                """,
                (start,),
            ).fetchall()
        records = [dict(zip(wanted, row)) for row in rows]
        signals: list[CandidateSignal] = []
        by_date: dict[str, list[CandidateSignal]] = {}
        for row in records:
            ticker = normalize_ticker(row.get("ticker"))
            if len(ticker) != 6 or not ticker.isdigit():
                continue
            raw_direction = str(row.get("direction") or "hold").lower()
            direction = (
                "long" if raw_direction in {"long", "buy"}
                else "sell" if raw_direction in {"short", "sell"}
                else "hold"
            )
            signal_at = str(row.get("predicted_at") or "")
            evidence = tuple(
                str(row[key])
                for key in ("id", "round_id", "decision_id")
                if row.get(key) not in (None, "")
            )
            signal = CandidateSignal(
                ticker=ticker,
                direction=direction,
                score=float(row.get("confidence") or 0.0),
                signal_at=signal_at,
                available_at=signal_at,
                sleeve=SleeveId.LLM_RESEARCH,
                evidence_ids=evidence,
                metadata={"forward_only_since": start, "source": "price_predictions"},
            )
            signals.append(signal)
            by_date.setdefault(signal_at[:10], []).append(signal)

        active_scores: dict[str, float] = {}
        targets: dict[str, dict[str, float]] = {}
        for signal_date in sorted(by_date):
            for signal in by_date[signal_date]:
                if signal.direction == "long":
                    active_scores[signal.ticker] = signal.score
                elif signal.direction == "sell":
                    active_scores.pop(signal.ticker, None)
            selected = sorted(
                active_scores,
                key=lambda ticker: (-active_scores[ticker], ticker),
            )[:10]
            targets[signal_date] = {ticker: 0.10 for ticker in selected}
        return targets, signals

    def data_hash(
        self,
        bars: Iterable[DailyBar],
        sessions: Iterable[str],
        instruments: Mapping[str, InstrumentProfile],
    ) -> str:
        payload = {
            "bars": [asdict(bar) for bar in sorted(bars, key=lambda item: (item.date, item.ticker))],
            "sessions": list(sessions),
            "instruments": {
                ticker: {
                    **asdict(profile),
                    "instrument_type": profile.instrument_type.value,
                }
                for ticker, profile in sorted(instruments.items())
            },
        }
        return stable_hash(payload)


class ShadowEvaluationService:
    def __init__(
        self,
        db_path: str | Path,
        *,
        cost_schedule: CostSchedule | None = None,
        benchmark_ticker: str = DEFAULT_BENCHMARK,
    ):
        self.db_path = Path(db_path)
        self.cost_schedule = cost_schedule or CostSchedule()
        self.benchmark_ticker = normalize_ticker(benchmark_ticker)
        self.repository = MarketTruthRepository(self.db_path)
        self.store = ExperimentStore(self.db_path)

    async def run(
        self,
        sleeve: SleeveId,
        *,
        persist: bool = True,
    ) -> ShadowRunOutcome:
        before = production_simulation_fingerprint(self.db_path)
        await self.store.initialize()
        all_sessions = self.repository.sessions()
        all_instruments = self.repository.instruments()
        if sleeve is SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1:
            bars = self.repository.validated_bars(etf_only=True)
            targets, signals = SystematicETFMomentumV1().build(bars)
            sessions = all_sessions
            relevant_tickers = {bar.ticker for bar in bars}
            instruments = {
                ticker: profile
                for ticker, profile in all_instruments.items()
                if ticker in relevant_tickers
            }
            spec = EvaluationSpec(
                sleeve=sleeve,
                strategy_version=SystematicETFMomentumV1.version,
                qualification_sessions=756,
                minimum_closed_trades=0,
                require_instrument_lifecycle_metadata=True,
            )
        elif sleeve is SleeveId.LLM_RESEARCH:
            targets, signals = self.repository.llm_forward_targets()
            # Forward-test age starts at the declared observation boundary.  It
            # must never inherit years of pre-signal cash-only calendar rows,
            # which would create a fictitious annualized sample.
            sessions = [day for day in all_sessions if day >= LLM_FORWARD_START]
            relevant_tickers = {signal.ticker for signal in signals}
            relevant_tickers.add(self.benchmark_ticker)
            bars = self.repository.validated_bars(
                etf_only=False,
                start=LLM_FORWARD_START,
                tickers=relevant_tickers,
            )
            instruments = {
                ticker: profile
                for ticker, profile in all_instruments.items()
                if ticker in relevant_tickers
            }
            spec = EvaluationSpec(
                sleeve=sleeve,
                strategy_version=LLM_STRATEGY_VERSION,
                qualification_sessions=252,
                minimum_closed_trades=100,
                require_instrument_lifecycle_metadata=True,
            )
        else:
            raise ValueError(f"unsupported shadow sleeve: {sleeve}")

        data_hash = self.repository.data_hash(bars, sessions, instruments)
        base_engine = CanonicalEvaluationEngine(self.cost_schedule)
        preliminary = base_engine.run(
            bars,
            targets,
            spec,
            signals=signals,
            benchmark_ticker=self.benchmark_ticker,
            trading_sessions=sessions,
            instruments=instruments,
        )
        stress_costs = replace(
            self.cost_schedule,
            slippage_rate=self.cost_schedule.slippage_rate * 3.0,
            version=f"{self.cost_schedule.version}_3x_slippage",
        )
        stress = CanonicalEvaluationEngine(stress_costs).run(
            bars,
            targets,
            spec,
            signals=signals,
            benchmark_ticker=self.benchmark_ticker,
            trading_sessions=sessions,
            instruments=instruments,
        )
        family = await self.store.unique_trial_returns_dated(
            sleeve.value, engine_version=preliminary.engine_version,
            cost_model_version=self.cost_schedule.version,
            current=TrialReturnSeries(spec.strategy_version, spec.strategy_hash,
                {str(row["date"]): float(row["net_return"]) for row in preliminary.curve},
                quality_status="validated" if (preliminary.metrics.get("data_quality") or {}).get("usable")
                else "data_unavailable"))
        current_returns = family.aligned_returns().get(spec.strategy_version, [])
        validation = validate_dated_trial_family(current_returns, family)
        final = base_engine.run(
            bars,
            targets,
            spec,
            signals=signals,
            benchmark_ticker=self.benchmark_ticker,
            trading_sessions=sessions,
            instruments=instruments,
            qualification_evidence={
                "stress_total_return": stress.metrics.get("total_return"),
                "dsr_probability": validation.dsr_probability,
                "pbo_probability": validation.pbo_probability,
            },
        )
        persisted_run_id: str | None = None
        evaluation_run_count = 0
        if persist:
            record = await self.store.persist(
                final,
                cost_schedule=self.cost_schedule,
                data_hash=data_hash,
                config={
                    "benchmark_ticker": self.benchmark_ticker,
                    "forward_start": LLM_FORWARD_START if sleeve is SleeveId.LLM_RESEARCH else None,
                    "stress_cost_model_version": stress_costs.version,
                    "statistical_validation": validation.as_dict(),
                },
            )
            persisted_run_id = record.run_id
            evaluation_run_count = record.trial_count
        after = production_simulation_fingerprint(self.db_path)
        if after != before:
            raise RuntimeError("shadow evaluation changed the production simulation account")
        return ShadowRunOutcome(
            result=final,
            data_hash=data_hash,
            persisted_run_id=persisted_run_id,
            strategy_trial_count=validation.trial_count,
            evaluation_run_count=evaluation_run_count,
        )


def production_simulation_fingerprint(
    db_path: str | Path,
) -> ProductionSimulationFingerprint:
    path = Path(db_path)
    if not path.exists():
        return ProductionSimulationFingerprint(None, 0, "[]")
    with closing(sqlite3.connect(path)) as conn, conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        cash: str | None = None
        if "system_stats" in tables:
            row = conn.execute(
                "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
            ).fetchone()
            cash = str(row[0]) if row else None
        trade_count = (
            int(conn.execute("SELECT COUNT(*) FROM simulation_trades").fetchone()[0])
            if "simulation_trades" in tables else 0
        )
        positions: list[tuple[Any, ...]] = []
        if "simulation_positions" in tables:
            columns = sorted(_columns(conn, "simulation_positions"))
            if columns:
                selected = ", ".join(f'"{column}"' for column in columns)
                positions = conn.execute(
                    f"SELECT {selected} FROM simulation_positions ORDER BY ticker"
                ).fetchall()
        return ProductionSimulationFingerprint(
            cash=cash,
            trade_count=trade_count,
            positions_json=json.dumps(positions, ensure_ascii=False, sort_keys=True),
        )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}
