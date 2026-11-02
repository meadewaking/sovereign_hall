"""PR5.2 — Structured evidence package for the single candidate experiment.

The first (and only) candidate experiment changes one decision node: the
"proposal evidence organizer" that turns raw research material into a
structured evidence package.  Search, candidate sources, committee roles,
position sizing, exit logic, quotes and fees are NOT changed in this
experiment.

Each claim extracted by the organizer is persisted as one row.  Numeric
inputs must trace back to a source excerpt; inference and fact are
separated by ``claim_type``; reprints of the same announcement share a
``shared_source_group_id`` so multi-seat same-source cannot be counted as
independent confirmation.
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


VALID_CLAIM_TYPES = {"fact", "inference", "unknown"}


@dataclass(frozen=True)
class StructuredEvidenceClaim:
    id: str
    claim_id: str
    source_document_id: str | None
    source_url: str | None
    original_excerpt: str
    publication_time: str | None
    first_seen_at: str
    effective_period: str | None
    claim_type: str
    ticker_relevance: list[str]
    valuation_inputs: dict[str, object]
    priced_in_counterargument: str | None
    catalyst: str | None
    catalyst_deadline: str | None
    failure_condition: str | None
    unknown_fields: list[str]
    shared_source_group_id: str | None
    experiment_id: str | None
    arm: str | None


class StructuredEvidenceService:
    """Persist structured evidence claims for the candidate experiment."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    async def record_claim(
        self,
        *,
        claim_id: str,
        source_document_id: str | None = None,
        source_url: str | None = None,
        original_excerpt: str,
        publication_time: str | None = None,
        first_seen_at: str | None = None,
        effective_period: str | None = None,
        claim_type: str = "unknown",
        ticker_relevance: Sequence[str] | None = None,
        valuation_inputs: Mapping[str, object] | None = None,
        priced_in_counterargument: str | None = None,
        catalyst: str | None = None,
        catalyst_deadline: str | None = None,
        failure_condition: str | None = None,
        unknown_fields: Sequence[str] | None = None,
        shared_source_group_id: str | None = None,
        experiment_id: str | None = None,
        arm: str | None = None,
    ) -> StructuredEvidenceClaim:
        if claim_type not in VALID_CLAIM_TYPES:
            raise ValueError(
                f"unknown claim_type {claim_type!r}; "
                f"expected one of {sorted(VALID_CLAIM_TYPES)}"
            )
        if not original_excerpt:
            raise ValueError("original_excerpt is required")
        if not claim_id:
            raise ValueError("claim_id is required")
        await self.initialize()
        row_id = new_id("evidence")
        first_seen = first_seen_at or datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                await conn.execute(
                    """
                    INSERT INTO structured_evidence_claims (
                        id, claim_id, source_document_id, source_url,
                        original_excerpt, publication_time, first_seen_at,
                        effective_period, claim_type, ticker_relevance,
                        valuation_inputs, priced_in_counterargument,
                        catalyst, catalyst_deadline, failure_condition,
                        unknown_fields, shared_source_group_id,
                        experiment_id, arm
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row_id,
                        claim_id,
                        source_document_id,
                        source_url,
                        original_excerpt,
                        publication_time,
                        first_seen,
                        effective_period,
                        claim_type,
                        json.dumps(
                            list(ticker_relevance or []),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        json.dumps(
                            dict(valuation_inputs or {}),
                            ensure_ascii=False,
                            sort_keys=True,
                            default=str,
                        ),
                        priced_in_counterargument,
                        catalyst,
                        catalyst_deadline,
                        failure_condition,
                        json.dumps(
                            list(unknown_fields or []),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        shared_source_group_id,
                        experiment_id,
                        arm,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return StructuredEvidenceClaim(
            id=row_id,
            claim_id=claim_id,
            source_document_id=source_document_id,
            source_url=source_url,
            original_excerpt=original_excerpt,
            publication_time=publication_time,
            first_seen_at=first_seen,
            effective_period=effective_period,
            claim_type=claim_type,
            ticker_relevance=list(ticker_relevance or []),
            valuation_inputs=dict(valuation_inputs or {}),
            priced_in_counterargument=priced_in_counterargument,
            catalyst=catalyst,
            catalyst_deadline=catalyst_deadline,
            failure_condition=failure_condition,
            unknown_fields=list(unknown_fields or []),
            shared_source_group_id=shared_source_group_id,
            experiment_id=experiment_id,
            arm=arm,
        )

    async def claims_for_experiment(
        self,
        experiment_id: str,
        *,
        arm: str | None = None,
    ) -> list[StructuredEvidenceClaim]:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            if arm is None:
                async with conn.execute(
                    """
                    SELECT id, claim_id, source_document_id, source_url,
                           original_excerpt, publication_time, first_seen_at,
                           effective_period, claim_type, ticker_relevance,
                           valuation_inputs, priced_in_counterargument,
                           catalyst, catalyst_deadline, failure_condition,
                           unknown_fields, shared_source_group_id,
                           experiment_id, arm
                    FROM structured_evidence_claims
                    WHERE experiment_id = ?
                    ORDER BY datetime(first_seen_at), claim_id
                    """,
                    (experiment_id,),
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                async with conn.execute(
                    """
                    SELECT id, claim_id, source_document_id, source_url,
                           original_excerpt, publication_time, first_seen_at,
                           effective_period, claim_type, ticker_relevance,
                           valuation_inputs, priced_in_counterargument,
                           catalyst, catalyst_deadline, failure_condition,
                           unknown_fields, shared_source_group_id,
                           experiment_id, arm
                    FROM structured_evidence_claims
                    WHERE experiment_id = ? AND arm = ?
                    ORDER BY datetime(first_seen_at), claim_id
                    """,
                    (experiment_id, arm),
                ) as cursor:
                    rows = await cursor.fetchall()
        return [self._from_row(row) for row in rows]

    async def independent_source_group_count(
        self,
        experiment_id: str,
        *,
        arm: str | None = None,
    ) -> int:
        """Count distinct ``shared_source_group_id`` values.

        Reprints of the same announcement share a group id, so multi-seat
        same-source confirmation cannot inflate this count.
        """
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            if arm is None:
                async with conn.execute(
                    """
                    SELECT COUNT(DISTINCT shared_source_group_id)
                    FROM structured_evidence_claims
                    WHERE experiment_id = ?
                      AND shared_source_group_id IS NOT NULL
                    """,
                    (experiment_id,),
                ) as cursor:
                    row = await cursor.fetchone()
            else:
                async with conn.execute(
                    """
                    SELECT COUNT(DISTINCT shared_source_group_id)
                    FROM structured_evidence_claims
                    WHERE experiment_id = ?
                      AND arm = ?
                      AND shared_source_group_id IS NOT NULL
                    """,
                    (experiment_id, arm),
                ) as cursor:
                    row = await cursor.fetchone()
        return int(row[0] if row and row[0] is not None else 0)

    @staticmethod
    def _from_row(row) -> StructuredEvidenceClaim:
        def _loads(raw, default):
            try:
                return json.loads(str(raw or default))
            except json.JSONDecodeError:
                return json.loads(default)
        return StructuredEvidenceClaim(
            id=str(row[0]),
            claim_id=str(row[1]),
            source_document_id=str(row[2]) if row[2] else None,
            source_url=str(row[3]) if row[3] else None,
            original_excerpt=str(row[4]),
            publication_time=str(row[5]) if row[5] else None,
            first_seen_at=str(row[6]),
            effective_period=str(row[7]) if row[7] else None,
            claim_type=str(row[8]),
            ticker_relevance=_loads(row[9], "[]"),
            valuation_inputs=_loads(row[10], "{}"),
            priced_in_counterargument=str(row[11]) if row[11] else None,
            catalyst=str(row[12]) if row[12] else None,
            catalyst_deadline=str(row[13]) if row[13] else None,
            failure_condition=str(row[14]) if row[14] else None,
            unknown_fields=_loads(row[15], "[]"),
            shared_source_group_id=str(row[16]) if row[16] else None,
            experiment_id=str(row[17]) if row[17] else None,
            arm=str(row[18]) if row[18] else None,
        )


# PR5.2 — pre-registration protocol for the single candidate experiment.
# This is a documentation-only constant: the experiment is registered
# through ExperimentRegistryService at runtime, but the protocol fields
# live here so reviewers can read them next to the implementation.
EVIDENCE_ORGANIZER_V1_PRE_REGISTRATION = {
    "hypothesis": (
        "结构化时效/估值/催化/反证证据能够改善对可交易候选的判断，"
        "进而改善扣费后的组合收益"
    ),
    "candidate": "evidence_organizer_v1",
    "control": "PR1-PR4 stable frozen policy snapshot",
    "decision_node_changed": "evidence_organizer",
    "first_operational_checkpoint": {
        "sessions": 20,
        "checks": [
            "claim fields traceable to source",
            "version pins intact",
            "cost model identical across arms",
            "execution_clock_mismatch absent",
        ],
        "is_acceptance_gate": False,
    },
    "interim_report_at_trading_days": 60,
    "primary_metric": "b_minus_a_net_return",
    "auxiliary_metrics": [
        "fact_traceability_rate",
        "known_publication_time_rate",
        "independent_source_group_count",
        "stale_evidence_ratio",
        "affordable_candidate_rate",
    ],
    "stop_conditions": [
        "time_leak",
        "evidence_fabrication",
        "safety_gate_changed",
        "cross_arm_contamination",
        "main_account_unexpected_write",
        "research_budget_exceeded",
    ],
    "failure_rule": (
        "B must satisfy the pre-registered control/benchmark/risk gates "
        "at the formal decision window with full data and independent "
        "sample minimum; otherwise reject. Insufficient data or samples "
        "remains insufficient_evidence, not failure or success."
    ),
    "rollback_rule": "stop_candidate_organizer_only; control and main account continue",
}
