"""Tests for PR5 — empirical observations, structured evidence and the
single-candidate pre-registration."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.services.empirical_observations import (
    EmpiricalObservationService,
)
from sovereign_hall.services.structured_evidence import (
    EVIDENCE_ORGANIZER_V1_PRE_REGISTRATION,
    StructuredEvidenceService,
)


@pytest.mark.asyncio
async def test_empirical_observation_lifecycle_supports_retraction(tmp_path: Path):
    db_path = tmp_path / "obs.db"
    service = EmpiricalObservationService(db_path)
    observation = await service.record(
        observation_type="high_confidence_error_subset",
        scope={"direction": "long", "confidence_bucket": "high"},
        supporting_examples=[{"prediction_id": "p1", "result": "wrong"}],
        counter_examples=[{"prediction_id": "p2", "result": "correct"}],
        origin_prediction_ids=["p1", "p2"],
        origin_round_ids=["r1"],
        deduplicated_sample_count=2,
        overlap_group_ids=["g1"],
        expiry_condition="90 days without new evidence",
        retraction_condition="two independent correct samples in same scope",
        status="hypothesis",
    )
    assert observation.status == "hypothesis"

    under_test = await service.transition(
        observation.id, "under_test", last_validated_at="2026-09-14T16:00:00"
    )
    assert under_test.status == "under_test"
    assert under_test.last_validated_at == "2026-09-14T16:00:00"

    retracted = await service.transition(
        observation.id,
        "retracted",
        retracted_reason="two independent correct samples observed",
    )
    assert retracted.status == "retracted"
    assert retracted.retracted_reason == "two independent correct samples observed"
    assert retracted.retracted_at is not None

    # A retracted observation cannot be transitioned again.
    with pytest.raises(RuntimeError, match="already retracted"):
        await service.transition(observation.id, "supported")


@pytest.mark.asyncio
async def test_empirical_observations_cannot_be_deleted(tmp_path: Path):
    db_path = tmp_path / "obs.db"
    service = EmpiricalObservationService(db_path)
    observation = await service.record(observation_type="t")
    async with aiosqlite.connect(db_path) as conn:
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "DELETE FROM empirical_observations WHERE id = ?",
                (observation.id,),
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_structured_evidence_separates_fact_from_inference(tmp_path: Path):
    db_path = tmp_path / "evidence.db"
    service = StructuredEvidenceService(db_path)
    fact = await service.record_claim(
        claim_id="claim_fact_1",
        original_excerpt="公司2026年H1营收同比增长12%",
        publication_time="2026-08-30T18:00:00",
        claim_type="fact",
        ticker_relevance=["600519"],
        valuation_inputs={"revenue_yoy": 0.12},
        catalyst="2026年9月15日业绩说明会",
        catalyst_deadline="2026-09-15",
        failure_condition="营收增速修正至低于10%",
        shared_source_group_id="group_announcement_2026_h1",
        experiment_id="exp_evidence_organizer_v1",
        arm="candidate",
    )
    inference = await service.record_claim(
        claim_id="claim_inference_1",
        original_excerpt="基于行业景气度推断下半年增速有望维持",
        claim_type="inference",
        ticker_relevance=["600519"],
        valuation_inputs={"assumption": "industry_trend_continues"},
        unknown_fields=["specific_q3_revenue_guidance"],
        shared_source_group_id="group_analyst_note",
        experiment_id="exp_evidence_organizer_v1",
        arm="candidate",
    )
    assert fact.claim_type == "fact"
    assert inference.claim_type == "inference"
    claims = await service.claims_for_experiment(
        "exp_evidence_organizer_v1", arm="candidate"
    )
    assert {c.claim_id for c in claims} == {"claim_fact_1", "claim_inference_1"}


@pytest.mark.asyncio
async def test_independent_source_group_count_deduplicates_reprints(tmp_path: Path):
    db_path = tmp_path / "evidence.db"
    service = StructuredEvidenceService(db_path)
    # Three claims, but two share a source group (reprints of the same
    # announcement).  Independent confirmation count must be 2, not 3.
    for idx in range(2):
        await service.record_claim(
            claim_id=f"claim_reprint_{idx}",
            original_excerpt=f"同一公告转载 {idx}",
            claim_type="fact",
            shared_source_group_id="group_announcement",
            experiment_id="exp_x",
            arm="candidate",
        )
    await service.record_claim(
        claim_id="claim_independent",
        original_excerpt="另一份独立调研",
        claim_type="fact",
        shared_source_group_id="group_research_note",
        experiment_id="exp_x",
        arm="candidate",
    )
    count = await service.independent_source_group_count("exp_x", arm="candidate")
    assert count == 2


@pytest.mark.asyncio
async def test_structured_evidence_rejects_unknown_claim_type(tmp_path: Path):
    db_path = tmp_path / "evidence.db"
    service = StructuredEvidenceService(db_path)
    with pytest.raises(ValueError, match="unknown claim_type"):
        await service.record_claim(
            claim_id="bad",
            original_excerpt="x",
            claim_type="speculation",
        )


def test_evidence_organizer_pre_registration_protocol_is_complete():
    """PR5.2: the single-candidate pre-registration must pin every field
    the plan requires, so a reviewer can read it next to the implementation."""
    protocol = EVIDENCE_ORGANIZER_V1_PRE_REGISTRATION
    assert protocol["candidate"] == "evidence_organizer_v1"
    assert protocol["primary_metric"] == "b_minus_a_net_return"
    assert protocol["interim_report_at_trading_days"] == 60
    assert "time_leak" in protocol["stop_conditions"]
    assert "evidence_fabrication" in protocol["stop_conditions"]
    assert "cross_arm_contamination" in protocol["stop_conditions"]
    # Auxiliary metrics are NOT success criteria on their own.
    for aux in protocol["auxiliary_metrics"]:
        assert aux != protocol["primary_metric"]
    # The first operational checkpoint is NOT an acceptance gate.
    assert protocol["first_operational_checkpoint"]["is_acceptance_gate"] is False


@pytest.mark.asyncio
async def test_apply_architecture_migrations_creates_pr5_tables(tmp_path: Path):
    db_path = tmp_path / "pr5.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('empirical_observations', 'structured_evidence_claims')"
        ) as cursor:
            names = {str(row[0]) for row in await cursor.fetchall()}
    assert {"empirical_observations", "structured_evidence_claims"} <= names
