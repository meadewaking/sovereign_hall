"""Tests for PR3 — experiment registry, replayable policy and execution clock."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import aiosqlite
import pytest

from sovereign_hall.domain.portfolio.execution_policy import ExecutionPolicyLimits
from sovereign_hall.domain.portfolio.instruments import InstrumentProfile
from sovereign_hall.domain.portfolio.shadow import (
    ArmKind,
    ExecutionClock,
    ExperimentStatus,
)
from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.services.experiment_registry_service import (
    ExperimentRegistryService,
)
from sovereign_hall.services.policy_snapshot_service import (
    PolicyManifest,
    PolicySnapshotService,
)
from sovereign_hall.services.replayable_policy import (
    ApprovedDecision,
    InstrumentRuleSet,
    MarketSnapshot,
    PolicyConfig,
    PositionState,
    PriceFreeIntent,
    build_targets,
    plan_execution,
    review_positions,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "experiment.db"


async def _register_snapshot(db_path: Path, family: str, version: str) -> str:
    service = PolicySnapshotService(db_path)
    persisted = await service.register(
        PolicyManifest(
            policy_family=family,
            version=version,
            effective_config={"max_single_position": 1.0},
        )
    )
    return persisted.snapshot_id


@pytest.mark.asyncio
async def test_register_experiment_pins_snapshots_and_clock(db_path: Path):
    control_id = await _register_snapshot(db_path, "run_discussion", "control_v1")
    candidate_id = await _register_snapshot(db_path, "run_discussion", "candidate_v1")
    registry = ExperimentRegistryService(db_path)
    experiment = await registry.register(
        experiment_id="exp_evidence_organizer_v1",
        hypothesis="structured evidence improves net return",
        control_policy_snapshot_id=control_id,
        candidate_policy_snapshot_id=candidate_id,
        decision_node_changed="evidence_organizer",
        information_cutoff_rule="shared_input_package",
        observation_start="2026-09-14",
        benchmark_definition="CSI300 buy_and_hold",
        execution_clock=ExecutionClock.PRODUCTION_EQUIVALENT_FORWARD,
        universe_policy="frozen_at_registration",
        fee_version="fee_v1",
        instrument_rule_version="instrument_v1",
        primary_metric="b_minus_a_net_return",
        failure_rule="fail_on_missed_window",
        minimum_samples=20,
        review_schedule="20_session_operational",
        rollback_rule="stop_candidate_only",
    )
    assert experiment.status is ExperimentStatus.REGISTERED
    assert experiment.execution_clock is ExecutionClock.PRODUCTION_EQUIVALENT_FORWARD
    assert experiment.minimum_samples == 20
    events = await registry.events(experiment.experiment_id)
    assert events[0].event_type == "ExperimentRegistered"


@pytest.mark.asyncio
async def test_register_rejects_identical_control_and_candidate(db_path: Path):
    snapshot_id = await _register_snapshot(db_path, "run_discussion", "v1")
    registry = ExperimentRegistryService(db_path)
    with pytest.raises(ValueError, match="control and candidate"):
        await registry.register(
            experiment_id="exp_bad",
            hypothesis="x",
            control_policy_snapshot_id=snapshot_id,
            candidate_policy_snapshot_id=snapshot_id,
            decision_node_changed="none",
            information_cutoff_rule="shared",
            observation_start="2026-09-14",
            benchmark_definition="CSI300",
            execution_clock=ExecutionClock.DAILY_NEXT_OPEN,
            universe_policy="frozen",
            fee_version="v1",
            instrument_rule_version="v1",
            primary_metric="b_minus_a",
            failure_rule="fail",
            minimum_samples=10,
            review_schedule="20",
            rollback_rule="stop",
        )


@pytest.mark.asyncio
async def test_transition_to_eligible_does_not_promote_automatically(db_path):
    control_id = await _register_snapshot(db_path, "run_discussion", "control_v1")
    candidate_id = await _register_snapshot(db_path, "run_discussion", "candidate_v1")
    registry = ExperimentRegistryService(db_path)
    experiment = await registry.register(
        experiment_id="exp_promote_check",
        hypothesis="x",
        control_policy_snapshot_id=control_id,
        candidate_policy_snapshot_id=candidate_id,
        decision_node_changed="evidence_organizer",
        information_cutoff_rule="shared",
        observation_start="2026-09-14",
        benchmark_definition="CSI300",
        execution_clock=ExecutionClock.PRODUCTION_EQUIVALENT_FORWARD,
        universe_policy="frozen",
        fee_version="v1",
        instrument_rule_version="v1",
        primary_metric="b_minus_a",
        failure_rule="fail",
        minimum_samples=5,
        review_schedule="20",
        rollback_rule="stop",
    )
    await registry.transition(
        experiment.experiment_id,
        ExperimentStatus.ELIGIBLE,
        event_type="ExperimentEligible",
        payload={"samples": 20},
    )
    reloaded = await registry.get(experiment.experiment_id)
    assert reloaded.status is ExperimentStatus.ELIGIBLE
    # Promoted is a separate, manual transition; it never auto-applies.
    await registry.transition(
        experiment.experiment_id,
        ExperimentStatus.PROMOTED,
        event_type="ExperimentPromoted",
        payload={"promoted_by": "manual_review"},
    )
    events = await registry.events(experiment.experiment_id)
    assert any(event.event_type == "ExperimentPromoted" for event in events)


def test_review_positions_is_deterministic_on_same_inputs():
    """A=A: the same inputs must produce byte-identical reviews."""
    state = [
        PositionState(
            ticker="600519",
            shares=100,
            avg_cost=1600.0,
            opened_at="2026-09-01T09:30:00",
        )
    ]
    snapshot = MarketSnapshot(
        quotes={
            "600519": {
                "price": 1700.0,
                "fetched_at": "2026-09-14T10:00:00",
                "source": "realtime_eastmoney",
            }
        },
        fetched_at="2026-09-14T10:00:00",
        provider="eastmoney",
    )
    config = PolicyConfig()
    first = review_positions(
        portfolio_state=state,
        market_snapshot=snapshot,
        policy_config=config,
        now=datetime.fromisoformat("2026-09-14T10:00:00"),
    )
    second = review_positions(
        portfolio_state=state,
        market_snapshot=snapshot,
        policy_config=config,
        now=datetime.fromisoformat("2026-09-14T10:00:00"),
    )
    assert [r.as_dict() for r in first] == [r.as_dict() for r in second]
    assert first[0].action == "hold"


def test_build_targets_caps_single_name_and_gross():
    decisions = [
        ApprovedDecision(
            ticker="600519",
            direction="long",
            target_weight=0.9,  # exceeds max_single_position=0.5
            confidence=0.7,
        ),
        ApprovedDecision(
            ticker="000858",
            direction="long",
            target_weight=0.4,
            confidence=0.6,
        ),
    ]
    portfolio_state: list[PositionState] = []
    config = PolicyConfig(max_single_position=0.5, max_gross=1.0)
    target = build_targets(
        approved_decisions=decisions,
        portfolio_state=portfolio_state,
        policy_config=config,
        sleeve="llm_research",
        as_of="2026-09-14",
        strategy_version="candidate_v1",
        total_assets=10000,
    )
    # The 0.9 weight must be capped at 0.5; the 0.4 stays.
    assert target.targets["600519"] == pytest.approx(0.5)
    assert target.targets["000858"] == pytest.approx(0.4)


def test_plan_execution_rejects_missing_quote_without_fallback():
    """PR1.2/PR3.2: a missing quote must reject the intent, not fall back to
    a stale or cost-basis price."""
    intents = [
        PriceFreeIntent(
            ticker="600519",
            direction="long",
            target_weight=0.5,
        )
    ]
    snapshot = MarketSnapshot(quotes={})  # no quote for 600519
    rules = InstrumentRuleSet(
        profiles={"600519": InstrumentProfile.from_ticker("600519")}
    )
    plan = plan_execution(
        price_free_intents=intents,
        market_snapshot=snapshot,
        instrument_rules=rules,
        limits=ExecutionPolicyLimits(max_single_position=0.5),
        cash_available=10000.0,
    )
    assert plan.steps[0].rejected is True
    assert plan.steps[0].reject_code == "quote_unavailable"
    assert plan.steps[0].target_shares == 0


def test_plan_execution_respects_lot_size_and_cash():
    intents = [
        PriceFreeIntent(
            ticker="600519",
            direction="long",
            target_weight=0.4,
        )
    ]
    snapshot = MarketSnapshot(
        quotes={
            "600519": {
                "price": 1700.0,
                "fetched_at": datetime.now().isoformat(),
                "quoted_at": datetime.now().isoformat(),
                "source": "realtime_eastmoney",
            }
        }
    )
    rules = InstrumentRuleSet(
        profiles={"600519": InstrumentProfile.from_ticker("600519", lot_size=100)}
    )
    plan = plan_execution(
        price_free_intents=intents,
        market_snapshot=snapshot,
        instrument_rules=rules,
        limits=ExecutionPolicyLimits(max_single_position=0.5),
        cash_available=10000.0,
    )
    # 0.4 * 10000 = 4000; 4000 / 1700 = 2.35; rounded down to multiple of
    # 100 -> 0 shares. So the intent is rejected for insufficient cash.
    assert plan.steps[0].rejected is True
    assert plan.steps[0].reject_code == "lot_or_cost_blocked"


@pytest.mark.asyncio
async def test_apply_architecture_migrations_creates_experiment_tables(tmp_path):
    db_path = tmp_path / "exp.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('experiment_registry', 'experiment_events')"
        ) as cursor:
            names = {str(row[0]) for row in await cursor.fetchall()}
    assert {"experiment_registry", "experiment_events"} <= names
