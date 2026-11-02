"""PR3.4 — Register and track frozen A/B experiments.

A registered experiment pins:

* the control policy snapshot (frozen, immutable),
* the candidate policy snapshot (the proposed change),
* the execution clock (daily-next-open vs production-equivalent forward),
* the initial capital (always 10000 RMB),
* the universe policy (frozen investable set; cannot be downsampled),
* the fee, instrument-rule and benchmark versions,
* the primary metric, failure rule, minimum sample count and rollback rule.

Lifecycle is append-only.  ``eligible`` means the experiment has met its
data and sample preconditions; ``promoted`` is recorded separately and never
auto-applied to main config.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping

import aiosqlite

from ..domain.portfolio.shadow import (
    ArmKind,
    ExecutionClock,
    ExperimentStatus,
)
from ..infrastructure.sqlite.migrations import apply_architecture_migrations


@dataclass(frozen=True)
class RegisteredExperiment:
    experiment_id: str
    hypothesis: str
    frozen_at: str
    control_policy_snapshot_id: str
    candidate_policy_snapshot_id: str
    execution_clock: ExecutionClock
    initial_capital: float
    universe_policy: str
    primary_metric: str
    minimum_samples: int
    status: ExperimentStatus


@dataclass(frozen=True)
class ExperimentEvent:
    experiment_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]
    created_at: str


class ExperimentRegistryService:
    """Append-only registry for frozen A/B experiments."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    async def register(
        self,
        *,
        experiment_id: str,
        hypothesis: str,
        control_policy_snapshot_id: str,
        candidate_policy_snapshot_id: str,
        decision_node_changed: str,
        information_cutoff_rule: str,
        observation_start: str,
        benchmark_definition: str,
        execution_clock: ExecutionClock,
        universe_policy: str,
        fee_version: str,
        instrument_rule_version: str,
        primary_metric: str,
        failure_rule: str,
        minimum_samples: int,
        review_schedule: str,
        rollback_rule: str,
        source_runtime_manifest: str | None = None,
        initial_capital: float = 10000.0,
        frozen_at: str | None = None,
    ) -> RegisteredExperiment:
        if float(initial_capital) != 10000.0:
            raise ValueError("experiments must start from 10000 RMB")
        if not experiment_id:
            raise ValueError("experiment_id is required")
        if control_policy_snapshot_id == candidate_policy_snapshot_id:
            raise ValueError(
                "control and candidate policy snapshots must differ"
            )
        if not isinstance(execution_clock, ExecutionClock):
            execution_clock = ExecutionClock(str(execution_clock))
        await self.initialize()
        now = frozen_at or datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                await conn.execute(
                    """
                    INSERT INTO experiment_registry (
                        experiment_id, hypothesis, frozen_at,
                        control_policy_snapshot_id,
                        candidate_policy_snapshot_id,
                        source_runtime_manifest, input_scope,
                        decision_node_changed, information_cutoff_rule,
                        observation_start, benchmark_definition,
                        execution_clock, initial_capital, universe_policy,
                        fee_version, instrument_rule_version,
                        primary_metric, failure_rule, minimum_samples,
                        review_schedule, rollback_rule, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'shared_input_package',
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              'registered', ?, ?)
                    """,
                    (
                        experiment_id,
                        hypothesis,
                        now,
                        control_policy_snapshot_id,
                        candidate_policy_snapshot_id,
                        source_runtime_manifest,
                        decision_node_changed,
                        information_cutoff_rule,
                        observation_start,
                        benchmark_definition,
                        execution_clock.value,
                        initial_capital,
                        universe_policy,
                        fee_version,
                        instrument_rule_version,
                        primary_metric,
                        failure_rule,
                        int(minimum_samples),
                        review_schedule,
                        rollback_rule,
                        now,
                        now,
                    ),
                )
                await self._append_event(
                    conn,
                    experiment_id,
                    "ExperimentRegistered",
                    {
                        "hypothesis": hypothesis,
                        "control_policy_snapshot_id": control_policy_snapshot_id,
                        "candidate_policy_snapshot_id": candidate_policy_snapshot_id,
                        "execution_clock": execution_clock.value,
                        "decision_node_changed": decision_node_changed,
                        "primary_metric": primary_metric,
                        "minimum_samples": int(minimum_samples),
                    },
                    created_at=now,
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return RegisteredExperiment(
            experiment_id=experiment_id,
            hypothesis=hypothesis,
            frozen_at=now,
            control_policy_snapshot_id=control_policy_snapshot_id,
            candidate_policy_snapshot_id=candidate_policy_snapshot_id,
            execution_clock=execution_clock,
            initial_capital=initial_capital,
            universe_policy=universe_policy,
            primary_metric=primary_metric,
            minimum_samples=int(minimum_samples),
            status=ExperimentStatus.REGISTERED,
        )

    async def transition(
        self,
        experiment_id: str,
        next_status: ExperimentStatus,
        *,
        event_type: str,
        payload: Mapping[str, Any] | None = None,
        last_usable_session: str | None = None,
    ) -> None:
        if not isinstance(next_status, ExperimentStatus):
            next_status = ExperimentStatus(str(next_status))
        await self.initialize()
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    "SELECT status FROM experiment_registry WHERE experiment_id = ?",
                    (experiment_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise KeyError(f"experiment not found: {experiment_id}")
                await conn.execute(
                    """
                    UPDATE experiment_registry
                    SET status = ?, updated_at = ?,
                        last_usable_session = COALESCE(?, last_usable_session)
                    WHERE experiment_id = ?
                    """,
                    (next_status.value, now, last_usable_session, experiment_id),
                )
                await self._append_event(
                    conn,
                    experiment_id,
                    event_type,
                    dict(payload or {}),
                    created_at=now,
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def get(self, experiment_id: str) -> RegisteredExperiment | None:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT experiment_id, hypothesis, frozen_at,
                       control_policy_snapshot_id,
                       candidate_policy_snapshot_id, execution_clock,
                       initial_capital, universe_policy, primary_metric,
                       minimum_samples, status
                FROM experiment_registry
                WHERE experiment_id = ?
                """,
                (experiment_id,),
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            return None
        return RegisteredExperiment(
            experiment_id=str(row[0]),
            hypothesis=str(row[1]),
            frozen_at=str(row[2]),
            control_policy_snapshot_id=str(row[3]),
            candidate_policy_snapshot_id=str(row[4]),
            execution_clock=ExecutionClock(str(row[5])),
            initial_capital=float(row[6]),
            universe_policy=str(row[7]),
            primary_metric=str(row[8]),
            minimum_samples=int(row[9]),
            status=ExperimentStatus(str(row[10])),
        )

    async def events(self, experiment_id: str) -> list[ExperimentEvent]:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT experiment_id, sequence, event_type, payload_json,
                       created_at
                FROM experiment_events
                WHERE experiment_id = ?
                ORDER BY sequence
                """,
                (experiment_id,),
            ) as cursor:
                rows = await cursor.fetchall()
        events: list[ExperimentEvent] = []
        for row in rows:
            try:
                payload = json.loads(str(row[3] or "{}"))
            except json.JSONDecodeError:
                payload = {}
            events.append(
                ExperimentEvent(
                    experiment_id=str(row[0]),
                    sequence=int(row[1]),
                    event_type=str(row[2]),
                    payload=payload,
                    created_at=str(row[4]),
                )
            )
        return events

    async def _append_event(
        self,
        conn: aiosqlite.Connection,
        experiment_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        created_at: str | None = None,
    ) -> None:
        timestamp = created_at or datetime.now().isoformat()
        async with conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 "
            "FROM experiment_events WHERE experiment_id = ?",
            (experiment_id,),
        ) as cursor:
            row = await cursor.fetchone()
        sequence = int(row[0] if row else 1)
        await conn.execute(
            """
            INSERT INTO experiment_events (
                experiment_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                experiment_id,
                sequence,
                event_type,
                json.dumps(
                    dict(payload), ensure_ascii=False, sort_keys=True, default=str
                ),
                timestamp,
            ),
        )


__all__ = [
    "ArmKind",
    "ExecutionClock",
    "ExperimentStatus",
    "ExperimentRegistryService",
    "RegisteredExperiment",
    "ExperimentEvent",
]
