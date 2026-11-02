"""Append-only persistence for shadow evaluations and their lineage."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

import aiosqlite

from ..domain.common.ids import new_id
from ..domain.portfolio.costs import CostSchedule
from ..infrastructure.sqlite.migrations import apply_architecture_migrations
from .evaluation_engine import EvaluationResult


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PersistedEvaluation:
    run_id: str
    trial_count: int
    strategy_hash: str
    data_hash: str
    status: str


@dataclass(frozen=True)
class TrialReturnSeries:
    """One strategy's dated trial return series with quality metadata.

    PR4.2: statistical validation must operate on dated series so two paths
    with the same length but different dates cannot be conflated.  Failed
    trials (``data_unavailable`` etc.) are kept here so the family retains
    them; the statistical gate decides whether to include them.
    """

    strategy_version: str
    strategy_hash: str
    dated_returns: Mapping[str, float] = field(default_factory=dict)
    quality_status: str = "validated"
    last_run_id: str | None = None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class TrialReturnsResult:
    """Dated, aligned trial return family with quality metadata.

    ``common_dates`` is the intersection of every series's dates, computed
    AFTER every series is loaded.  Two series with the same length but
    different dates must surface as a quality warning, not silent
    misalignment.
    """

    by_strategy: Mapping[str, TrialReturnSeries]
    common_dates: tuple[str, ...]
    quality_warnings: tuple[str, ...]
    family_status: str  # "aligned" / "misaligned_dates" / "insufficient_series"

    def aligned_returns(self) -> dict[str, list[float]]:
        """Return per-strategy numerical arrays over ``common_dates``.

        Empty when the family is misaligned or has fewer than two series.
        """
        if self.family_status != "aligned":
            return {}
        return {
            name: [series.dated_returns[day] for day in self.common_dates]
            for name, series in self.by_strategy.items()
        }


class ExperimentStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    async def persist(
        self,
        result: EvaluationResult,
        *,
        cost_schedule: CostSchedule,
        data_hash: str,
        config: Mapping[str, Any] | None = None,
        created_at: str | None = None,
        experiment_id: str | None = None,
        arm: str | None = None,
    ) -> PersistedEvaluation:
        """Append one immutable run; no prior result is updated or overwritten."""
        if float(result.spec.initial_capital) != 10000.0:
            raise ValueError("shadow evaluations are fixed to the 10000 RMB account")
        if not str(data_hash).strip():
            raise ValueError("data_hash is required")
        await self.initialize()
        run_id = new_id("eval")
        now = created_at or datetime.now().isoformat()
        universe = list(result.universe)
        universe_hash = stable_hash(universe)
        schedule_json = stable_json(asdict(cost_schedule))
        result_payload = result.as_dict(include_curve=True)
        config_payload = dict(config or {})

        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                if experiment_id is not None:
                    async with conn.execute("SELECT id,trial_count FROM evaluation_runs WHERE experiment_id=? "
                        "AND arm=? AND strategy_hash=? AND engine_version=? AND data_cutoff=? AND data_hash=? "
                        "AND cost_model_version=? ORDER BY created_at LIMIT 1",
                        (experiment_id, arm, result.spec.strategy_hash, result.engine_version,
                         result.data_cutoff, data_hash, cost_schedule.version)) as cursor:
                        duplicate = await cursor.fetchone()
                    if duplicate:
                        await conn.rollback()
                        return PersistedEvaluation(duplicate[0], duplicate[1], result.spec.strategy_hash,
                                                   data_hash, result.status)
                async with conn.execute(
                    "SELECT schedule_json FROM cost_model_registry WHERE version = ?",
                    (cost_schedule.version,),
                ) as cursor:
                    existing_cost = await cursor.fetchone()
                if existing_cost and str(existing_cost[0]) != schedule_json:
                    raise ValueError(
                        "a cost-model version cannot be reused with different parameters"
                    )
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO cost_model_registry (
                        version, schedule_json, created_at
                    ) VALUES (?, ?, ?)
                    """,
                    (cost_schedule.version, schedule_json, now),
                )
                async with conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM evaluation_runs
                    WHERE sleeve = ?
                    """,
                    (result.spec.sleeve.value,),
                ) as cursor:
                    row = await cursor.fetchone()
                trial_count = int(row[0] if row else 0) + 1
                await conn.execute(
                    """
                    INSERT INTO evaluation_runs (
                        id, sleeve, strategy_version, strategy_hash,
                        engine_version, data_cutoff, data_hash, universe_hash,
                        universe_json, cost_model_version, cost_schedule_json,
                        initial_capital, trial_count, status, config_json,
                        metrics_json, result_json, created_at, experiment_id, arm
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        result.spec.sleeve.value,
                        result.spec.strategy_version,
                        result.spec.strategy_hash,
                        result.engine_version,
                        result.data_cutoff,
                        data_hash,
                        universe_hash,
                        stable_json(universe),
                        cost_schedule.version,
                        schedule_json,
                        result.spec.initial_capital,
                        trial_count,
                        result.status,
                        stable_json(config_payload),
                        stable_json(dict(result.metrics)),
                        stable_json(result_payload),
                        now,
                        experiment_id,
                        arm,
                    ),
                )
                for index, signal in enumerate(result.signals):
                    signal_id = f"signal_{stable_hash([run_id, index, signal.as_dict()])[:32]}"
                    await conn.execute(
                        """
                        INSERT INTO shadow_signals (
                            id, evaluation_run_id, sleeve, ticker, direction,
                            score, signal_at, available_at, evidence_json,
                            metadata_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            signal_id,
                            run_id,
                            signal.sleeve.value,
                            signal.ticker,
                            signal.direction,
                            signal.score,
                            signal.signal_at,
                            signal.available_at,
                            stable_json(list(signal.evidence_ids)),
                            stable_json(dict(signal.metadata)),
                            now,
                        ),
                    )
                for index, portfolio in enumerate(result.portfolios):
                    portfolio_id = (
                        f"portfolio_{stable_hash([run_id, index, portfolio.as_of])[:32]}"
                    )
                    await conn.execute(
                        """
                        INSERT INTO shadow_portfolios (
                            id, evaluation_run_id, sleeve, as_of,
                            strategy_version, targets_json, risk_status,
                            risk_reasons_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            portfolio_id,
                            run_id,
                            portfolio.sleeve.value,
                            portfolio.as_of,
                            portfolio.strategy_version,
                            stable_json(dict(portfolio.targets)),
                            portfolio.risk_status.value,
                            stable_json(list(portfolio.risk_reasons)),
                            now,
                        ),
                    )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return PersistedEvaluation(
            run_id=run_id,
            trial_count=trial_count,
            strategy_hash=result.spec.strategy_hash,
            data_hash=data_hash,
            status=result.status,
        )

    async def unique_trial_returns(
        self,
        sleeve: str,
    ) -> dict[str, list[float]]:
        """Backwards-compatible numerical arrays over the common date window.

        DEPRECATED: this method loses dates and therefore cannot detect a
        same-length-different-dates family.  New callers should use
        ``unique_trial_returns_dated`` and inspect ``quality_warnings``.
        """
        result = await self.unique_trial_returns_dated(sleeve)
        return result.aligned_returns()

    async def unique_trial_returns_dated(
        self,
        sleeve: str,
        *, current: TrialReturnSeries | None = None,
        engine_version: str | None = None,
        cost_model_version: str | None = None,
    ) -> TrialReturnsResult:
        """Load dated, aligned trial paths with quality metadata.

        PR4.2: statistical validation must operate on dated series.  Two
        series with the same length but different dates surface as a
        ``misaligned_dates`` warning rather than silent misalignment.
        Re-running the same frozen version under a newer data cutoff is one
        trial, not another parameter search.  A genuinely changed rule set
        must receive a new strategy version.  Failed trials
        (``data_unavailable``) are kept in the family so the gate can decide;
        they are not silently dropped to make a winner look better.
        """
        if not self.db_path.exists():
            return TrialReturnsResult(
                by_strategy={},
                common_dates=(),
                quality_warnings=("store_not_initialized",),
                family_status="insufficient_series",
            )
        await self.initialize()
        latest_by_strategy: dict[str, tuple[str, str, str, str]] = {}
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT strategy_version, strategy_hash, id, status, result_json
                FROM evaluation_runs
                    WHERE sleeve = ?
                      AND (? IS NULL OR engine_version = ?)
                      AND (? IS NULL OR cost_model_version = ?)
                ORDER BY created_at, id
                """,
                (sleeve, engine_version, engine_version, cost_model_version, cost_model_version),
            ) as cursor:
                for (
                    strategy_version,
                    strategy_hash,
                    run_id,
                    status,
                    result_json,
                ) in await cursor.fetchall():
                    latest_by_strategy[str(strategy_version)] = (
                        str(strategy_hash),
                        str(run_id),
                        str(status or ""),
                        str(result_json or "{}"),
                    )
        series_by_strategy: dict[str, TrialReturnSeries] = {}
        warnings: list[str] = []
        for strategy_version, (hash_, run_id, status, raw) in (
            latest_by_strategy.items()
        ):
            notes: list[str] = []
            if status and status not in {"available", "not_proven", "qualified"}:
                notes.append(f"status={status}")
                warnings.append(
                    f"{strategy_version}: non-success status {status!r}; "
                    "kept in family but flagged"
                )
            try:
                curve = json.loads(raw).get("curve", [])
            except (AttributeError, json.JSONDecodeError):
                warnings.append(
                    f"{strategy_version}: result_json could not be parsed; "
                    "series excluded"
                )
                continue
            dated = {}
            try:
                for row in curve:
                    day, value = str(row["date"])[:10], float(row["net_return"])
                    if not day or day in dated or not math.isfinite(value):
                        raise ValueError("missing/duplicate date or nonfinite return")
                    dated[day] = value
            except (ValueError, TypeError, KeyError):
                warnings.append(f"{strategy_version}: invalid dated returns; qualification blocked")
                status = "invalid_returns"
            if not dated:
                warnings.append(
                    f"{strategy_version}: curve has no dated rows; "
                    "series excluded"
                )
                continue
            series_by_strategy[strategy_version] = TrialReturnSeries(
                strategy_version=strategy_version,
                strategy_hash=hash_,
                dated_returns=dated,
                quality_status=("validated" if status in {"available", "not_proven", "qualified"}
                                else status or "validated"),
                last_run_id=run_id,
                notes=tuple(notes),
            )
        if current is not None:
            series_by_strategy[current.strategy_version] = current
        if any(series.quality_status not in {"available", "validated"}
               for series in series_by_strategy.values()):
            return TrialReturnsResult(series_by_strategy, (), tuple(warnings), "data_blocked")
        if len(series_by_strategy) < 2:
            return TrialReturnsResult(
                by_strategy=series_by_strategy,
                common_dates=(),
                quality_warnings=tuple(warnings) + ("fewer_than_two_series",),
                family_status="insufficient_series",
            )
        # PR4.2: check date alignment BEFORE converting to numerical arrays.
        # Two series with the same length but different dates must surface as
        # a misalignment warning, not be silently aligned to whatever dates
        # they happen to share.
        date_sets = [set(series.dated_returns.keys()) for series in series_by_strategy.values()]
        common = sorted(set.intersection(*date_sets))
        date_lengths = {len(dates) for dates in date_sets}
        if len(date_lengths) > 1:
            warnings.append(
                "series have different date counts "
                f"({sorted(date_lengths)}); common-window alignment only"
            )
        if not common:
            warnings.append("no common dates across series; cannot align")
            return TrialReturnsResult(
                by_strategy=series_by_strategy,
                common_dates=(),
                quality_warnings=tuple(warnings),
                family_status="misaligned_dates",
            )
        # Detect same-length-different-dates: two series with equal length
        # but disjoint (or partly disjoint) date sets is a sign of mixed-up
        # families, not a genuine alignment.
        for index, dates in enumerate(date_sets):
            if any(len(dates) == len(other) and dates != other for other in date_sets[index + 1:]):
                warnings.append("same-length series have different dates; possible family mix-up")
                break
        family_status = "misaligned_dates" if any(
            "misalignment" in warning.lower() or "mix-up" in warning.lower()
            for warning in warnings
        ) else "aligned"
        return TrialReturnsResult(
            by_strategy=series_by_strategy,
            common_dates=tuple(common),
            quality_warnings=tuple(warnings),
            family_status=family_status,
        )
