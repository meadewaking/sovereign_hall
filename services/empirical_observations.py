"""PR5.1 — Empirical observations register.

An empirical observation is a learning claim with explicit scope, supporting
and counter-examples, deduplication metadata, lifecycle and retraction
conditions.  Status transitions are append-only: a retraction records the
reason and timestamp but never deletes the row.

Status values:
* ``hypothesis``  — proposed but not yet under test
* ``under_test``  — currently being evaluated by a registered experiment
* ``supported``   — met its evidence bar (still retractable if counter-evidence appears)
* ``retracted``   — explicitly retracted with a reason

Observations never become prompt text directly.  Changing prompt text is a
strategy change and must go through a candidate experiment (PR5.2).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Mapping, Sequence

import aiosqlite

from ..domain.common.ids import new_id
from ..infrastructure.sqlite.migrations import apply_architecture_migrations


VALID_STATUSES = {"hypothesis", "under_test", "supported", "retracted"}


@dataclass(frozen=True)
class EmpiricalObservation:
    id: str
    observation_type: str
    scope: dict[str, object]
    supporting_examples: list[dict[str, object]]
    counter_examples: list[dict[str, object]]
    origin_prediction_ids: list[str]
    origin_round_ids: list[str]
    deduplicated_sample_count: int
    overlap_group_ids: list[str]
    first_recorded_at: str
    last_validated_at: str | None
    expiry_condition: str | None
    retraction_condition: str | None
    status: str
    retracted_reason: str | None
    retracted_at: str | None


class EmpiricalObservationService:
    """Append-only register of empirical observations."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    async def record(
        self,
        *,
        observation_type: str,
        scope: Mapping[str, object] | None = None,
        supporting_examples: Sequence[Mapping[str, object]] | None = None,
        counter_examples: Sequence[Mapping[str, object]] | None = None,
        origin_prediction_ids: Sequence[str] | None = None,
        origin_round_ids: Sequence[str] | None = None,
        deduplicated_sample_count: int = 0,
        overlap_group_ids: Sequence[str] | None = None,
        expiry_condition: str | None = None,
        retraction_condition: str | None = None,
        status: str = "hypothesis",
    ) -> EmpiricalObservation:
        if status not in VALID_STATUSES:
            raise ValueError(
                f"unknown observation status {status!r}; "
                f"expected one of {sorted(VALID_STATUSES)}"
            )
        await self.initialize()
        observation_id = new_id("obs")
        now = datetime.now().isoformat()
        scope_json = json.dumps(
            dict(scope or {}), ensure_ascii=False, sort_keys=True, default=str
        )
        supporting_json = json.dumps(
            list(supporting_examples or []), ensure_ascii=False, sort_keys=True, default=str
        )
        counter_json = json.dumps(
            list(counter_examples or []), ensure_ascii=False, sort_keys=True, default=str
        )
        prediction_ids = list(origin_prediction_ids or [])
        round_ids = list(origin_round_ids or [])
        overlap_groups = list(overlap_group_ids or [])
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                await conn.execute(
                    """
                    INSERT INTO empirical_observations (
                        id, observation_type, scope_json,
                        supporting_examples_json, counter_examples_json,
                        origin_prediction_ids_json, origin_round_ids_json,
                        deduplicated_sample_count, overlap_group_ids_json,
                        first_recorded_at, last_validated_at,
                        expiry_condition, retraction_condition, status,
                        retracted_reason, retracted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL)
                    """,
                    (
                        observation_id,
                        observation_type,
                        scope_json,
                        supporting_json,
                        counter_json,
                        json.dumps(prediction_ids, ensure_ascii=False, sort_keys=True),
                        json.dumps(round_ids, ensure_ascii=False, sort_keys=True),
                        int(deduplicated_sample_count),
                        json.dumps(overlap_groups, ensure_ascii=False, sort_keys=True),
                        now,
                        expiry_condition,
                        retraction_condition,
                        status,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return EmpiricalObservation(
            id=observation_id,
            observation_type=observation_type,
            scope=dict(scope or {}),
            supporting_examples=list(supporting_examples or []),
            counter_examples=list(counter_examples or []),
            origin_prediction_ids=prediction_ids,
            origin_round_ids=round_ids,
            deduplicated_sample_count=int(deduplicated_sample_count),
            overlap_group_ids=overlap_groups,
            first_recorded_at=now,
            last_validated_at=None,
            expiry_condition=expiry_condition,
            retraction_condition=retraction_condition,
            status=status,
            retracted_reason=None,
            retracted_at=None,
        )

    async def transition(
        self,
        observation_id: str,
        next_status: str,
        *,
        retracted_reason: str | None = None,
        last_validated_at: str | None = None,
    ) -> EmpiricalObservation:
        if next_status not in VALID_STATUSES:
            raise ValueError(
                f"unknown observation status {next_status!r}; "
                f"expected one of {sorted(VALID_STATUSES)}"
            )
        await self.initialize()
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    "SELECT status FROM empirical_observations WHERE id = ?",
                    (observation_id,),
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    raise KeyError(f"observation not found: {observation_id}")
                previous_status = str(row[0])
                if previous_status == "retracted":
                    raise RuntimeError(
                        f"observation {observation_id} is already retracted; "
                        "transition refused"
                    )
                await conn.execute(
                    """
                    UPDATE empirical_observations
                    SET status = ?,
                        last_validated_at = COALESCE(?, last_validated_at),
                        retracted_reason = ?,
                        retracted_at = CASE WHEN ? = 'retracted' THEN ? ELSE NULL END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        next_status,
                        last_validated_at,
                        retracted_reason,
                        next_status,
                        now if next_status == "retracted" else None,
                        now,
                        observation_id,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        observation = await self.get(observation_id)
        assert observation is not None
        return observation

    async def get(self, observation_id: str) -> EmpiricalObservation | None:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT id, observation_type, scope_json,
                       supporting_examples_json, counter_examples_json,
                       origin_prediction_ids_json, origin_round_ids_json,
                       deduplicated_sample_count, overlap_group_ids_json,
                       first_recorded_at, last_validated_at,
                       expiry_condition, retraction_condition, status,
                       retracted_reason, retracted_at
                FROM empirical_observations
                WHERE id = ?
                """,
                (observation_id,),
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            return None
        return self._from_row(row)

    async def list_observations(self, status: str | None = None) -> list[EmpiricalObservation]:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            if status is None:
                async with conn.execute(
                    """
                    SELECT id, observation_type, scope_json,
                           supporting_examples_json, counter_examples_json,
                           origin_prediction_ids_json, origin_round_ids_json,
                           deduplicated_sample_count, overlap_group_ids_json,
                           first_recorded_at, last_validated_at,
                           expiry_condition, retraction_condition, status,
                           retracted_reason, retracted_at
                    FROM empirical_observations
                    ORDER BY datetime(first_recorded_at), id
                    """
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                async with conn.execute(
                    """
                    SELECT id, observation_type, scope_json,
                           supporting_examples_json, counter_examples_json,
                           origin_prediction_ids_json, origin_round_ids_json,
                           deduplicated_sample_count, overlap_group_ids_json,
                           first_recorded_at, last_validated_at,
                           expiry_condition, retraction_condition, status,
                           retracted_reason, retracted_at
                    FROM empirical_observations
                    WHERE status = ?
                    ORDER BY datetime(first_recorded_at), id
                    """,
                    (status,),
                ) as cursor:
                    rows = await cursor.fetchall()
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _from_row(row) -> EmpiricalObservation:
        def _loads(raw, default):
            try:
                return json.loads(str(raw or default))
            except json.JSONDecodeError:
                return json.loads(default)
        return EmpiricalObservation(
            id=str(row[0]),
            observation_type=str(row[1]),
            scope=_loads(row[2], "{}"),
            supporting_examples=_loads(row[3], "[]"),
            counter_examples=_loads(row[4], "[]"),
            origin_prediction_ids=_loads(row[5], "[]"),
            origin_round_ids=_loads(row[6], "[]"),
            deduplicated_sample_count=int(row[7] or 0),
            overlap_group_ids=_loads(row[8], "[]"),
            first_recorded_at=str(row[9]),
            last_validated_at=str(row[10]) if row[10] else None,
            expiry_condition=str(row[11]) if row[11] else None,
            retraction_condition=str(row[12]) if row[12] else None,
            status=str(row[13]),
            retracted_reason=str(row[14]) if row[14] else None,
            retracted_at=str(row[15]) if row[15] else None,
        )
