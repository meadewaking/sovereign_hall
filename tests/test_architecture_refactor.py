from __future__ import annotations

import ast
import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.application.get_system_status import (
    format_system_status,
    get_system_status,
)
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.core import Document
from sovereign_hall.domain.portfolio import (
    ExecutionPolicyLimits,
    cap_target_position,
)
from sovereign_hall.domain.research import ResearchRoundStatus
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
from sovereign_hall.run_discussion import (
    execute_durable_committee_stage,
    parse_committee_vote,
)


class OpenMarket:
    def __init__(
        self,
        price: float | None = 10.0,
        *,
        fetched_at: str | None = None,
    ):
        self.price = price
        self.fetched_at = fetched_at
        self.quote_calls = 0

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return True

    async def get_current_quote(self, ticker):
        self.quote_calls += 1
        if self.price is None:
            return None
        return {
            "ticker": ticker,
            "price": self.price,
            "source": "architecture_test_realtime",
            "fetched_at": self.fetched_at or datetime.now().isoformat(),
            "quoted_at": self.fetched_at or datetime.now().isoformat(),
        }


class ClosedMarket(OpenMarket):
    async def is_market_open(self):
        return False


async def build_simulation(tmp_path, monkeypatch, market):
    import sovereign_hall.services.market_data as market_data_module

    monkeypatch.setattr(market_data_module, "get_market_data", lambda: market)
    db = DatabaseService(str(tmp_path / "architecture.db"))
    await db._init_db()
    simulation = InvestmentSimulation(db)
    await simulation.initialize()
    return db, simulation


@pytest.mark.asyncio
async def test_fresh_database_applies_forward_migrations_without_fabricated_history(
    tmp_path,
):
    db = DatabaseService(str(tmp_path / "fresh.db"))
    await db._init_db()
    conn = await db._get_connection()
    async with conn.execute("PRAGMA foreign_keys") as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute("PRAGMA journal_mode") as cursor:
        assert str((await cursor.fetchone())[0]).lower() == "wal"
    async with conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ) as cursor:
        tables = {row[0] for row in await cursor.fetchall()}
    assert {
        "research_rounds",
        "round_events",
        "execution_intents",
        "quote_snapshots",
        "simulation_ledger_entries",
        "committee_meetings",
        "committee_tasks",
        "committee_turns",
    } <= tables
    for table in ("research_rounds", "execution_intents", "quote_snapshots"):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_committee_task_session_turn_is_durable_and_resumable(tmp_path):
    db = DatabaseService(str(tmp_path / "committee-protocol.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="durable committee",
        research_objective="resume completed seats",
    )
    store = CommitteeMeetingStore(db)
    proposal = {
        "proposal_id": "proposal-durable-1",
        "ticker": "600519",
        "direction": "long",
        "thesis": "现金流改善",
        "evidence": ["公告A", "公告B"],
    }
    meeting_id = await store.start_or_resume(
        round_id=round_state.id,
        proposal=proposal,
        review_depth="focused",
    )
    calls = 0

    async def vote():
        nonlocal calls
        calls += 1
        return '{"direction":"hold","confidence":0.4,"position":0}'

    specs = [
        {
            "label": "CIO综合视角",
            "seat_role": "cio",
            "model": "test-model",
            "provider": "test-provider",
            "input_material": "strict vote prompt",
            "factory": vote,
        }
    ]
    first_results, first_audit = await execute_durable_committee_stage(
        specs,
        timeout_seconds=1,
        stage="round4_vote",
        max_concurrency=1,
        meeting_store=store,
        meeting_id=meeting_id,
        parse_output=parse_committee_vote,
    )

    async def must_not_rerun():
        raise AssertionError("completed durable task should be resumed")

    resumed_specs = [{**specs[0], "factory": must_not_rerun}]
    resumed_results, resumed_audit = await execute_durable_committee_stage(
        resumed_specs,
        timeout_seconds=1,
        stage="round4_vote",
        max_concurrency=1,
        meeting_store=store,
        meeting_id=meeting_id,
        parse_output=parse_committee_vote,
    )
    await store.finish_meeting(
        meeting_id=meeting_id,
        status="completed",
        decision_id="decision-durable-1",
        outcome_code="committee_hold",
    )

    assert calls == 1
    assert resumed_results == first_results
    assert first_audit["checkpoint_resumed_count"] == 0
    assert resumed_audit["checkpoint_resumed_count"] == 1
    assert resumed_audit["tasks"][0]["session_id"] == first_audit["tasks"][0]["session_id"]

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT meeting.status, meeting.outcome_code, task.status,
               task.session_id, turn.raw_output, turn.parse_mode,
               turn.parsed_output_json
        FROM committee_meetings AS meeting
        JOIN committee_tasks AS task ON task.meeting_id = meeting.id
        JOIN committee_turns AS turn ON turn.task_id = task.id
        WHERE meeting.id = ?
        """,
        (meeting_id,),
    ) as cursor:
        row = await cursor.fetchone()
    assert row[0] == "completed"
    assert row[1] == "committee_hold"
    assert row[2] == "completed"
    assert str(row[3]).startswith("committee_session_")
    assert row[4] == first_results[0]
    assert row[5] == "structured_json"
    assert json.loads(row[6])["direction"] == "hold"
    await db.close()


@pytest.mark.asyncio
async def test_committee_resume_retries_absent_seat_without_overwriting_attempt(tmp_path):
    db = DatabaseService(str(tmp_path / "committee-retry.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="committee timeout",
        research_objective="retry only absent seats",
    )
    store = CommitteeMeetingStore(db)
    meeting_id = await store.start_or_resume(
        round_id=round_state.id,
        proposal={
            "proposal_id": "proposal-timeout",
            "ticker": "510300",
            "direction": "long",
        },
        review_depth="focused",
    )

    async def timeout_once():
        import asyncio

        await asyncio.sleep(0.02)
        return "too late"

    base_spec = {
        "label": "风控视角",
        "seat_role": "risk_officer",
        "input_material": "same durable input",
        "factory": timeout_once,
    }
    first_results, first_audit = await execute_durable_committee_stage(
        [base_spec],
        timeout_seconds=0.001,
        stage="round4_vote",
        max_concurrency=1,
        meeting_store=store,
        meeting_id=meeting_id,
        parse_output=parse_committee_vote,
    )

    async def recovered_vote():
        return '{"direction":"hold","confidence":0.3,"position":0}'

    second_results, second_audit = await execute_durable_committee_stage(
        [{**base_spec, "factory": recovered_vote}],
        timeout_seconds=1,
        stage="round4_vote",
        max_concurrency=1,
        meeting_store=store,
        meeting_id=meeting_id,
        parse_output=parse_committee_vote,
    )
    third_results, third_audit = await execute_durable_committee_stage(
        [{**base_spec, "factory": timeout_once}],
        timeout_seconds=1,
        stage="round4_vote",
        max_concurrency=1,
        meeting_store=store,
        meeting_id=meeting_id,
        parse_output=parse_committee_vote,
    )

    assert first_audit["timeout_count"] == 1
    assert "committee_task_absent" in first_results[0]
    assert second_audit["checkpoint_resumed_count"] == 0
    assert parse_committee_vote(second_results[0])["is_valid"] is True
    assert third_audit["checkpoint_resumed_count"] == 1
    assert third_results == second_results

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT attempt, status
        FROM committee_tasks
        WHERE meeting_id = ? AND stage = 'round4_vote'
        ORDER BY attempt
        """,
        (meeting_id,),
    ) as cursor:
        attempts = [tuple(row) for row in await cursor.fetchall()]
    assert attempts == [(0, "timeout"), (1, "completed")]
    await db.close()


@pytest.mark.asyncio
async def test_round_events_use_same_local_iso_clock_as_research_round(tmp_path):
    db = DatabaseService(str(tmp_path / "round_event_clock.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)

    round_state = await coordinator.start(
        base_topic="round event clock",
        research_objective="preserve chronological audit order",
    )
    await coordinator.record_event(
        round_state.id,
        "PreResearchPortfolioLifecycleReviewed",
        {"position_count": 0},
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
        event_type="HistoricalMemoryLoaded",
    )

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT r.started_at, e.created_at
        FROM research_rounds r
        JOIN round_events e ON e.round_id = r.id
        WHERE r.id = ?
        ORDER BY e.sequence
        """,
        (round_state.id,),
    ) as cursor:
        rows = await cursor.fetchall()

    assert len(rows) == 3
    started_at = datetime.fromisoformat(str(rows[0][0]))
    event_times = []
    for row in rows:
        created_at = str(row[1])
        assert "T" in created_at
        parsed = datetime.fromisoformat(created_at)
        assert parsed >= started_at
        event_times.append(parsed)
    assert event_times == sorted(event_times)
    await db.close()


@pytest.mark.asyncio
async def test_forward_migration_reconciles_only_rounds_with_linked_fills(
    tmp_path,
    monkeypatch,
):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        ROUND_FILL_TERMINAL_REPAIR_VERSION,
        apply_architecture_migrations,
    )

    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    coordinator = ResearchRoundCoordinator(db)
    filled_round = await coordinator.start(
        base_topic="legacy deferred round",
        research_objective="repair exact fill lineage",
    )
    untouched_round = await coordinator.start(
        base_topic="unlinked deferred round",
        research_objective="preserve unknown history",
    )
    conn = await db._get_connection()
    for round_id in (filled_round.id, untouched_round.id):
        await conn.execute(
            """
            UPDATE research_rounds
            SET status = 'completed', current_stage = 'completed',
                terminal_code = 'market_closed_pending',
                terminal_reason = 'legacy pending terminal',
                completed_at = updated_at
            WHERE id = ?
            """,
            (round_id,),
        )
    await conn.execute(
        """
        INSERT INTO simulation_trades (
            ticker, direction, shares, price, fee, reason, traded_at,
            round_id, idempotency_key
        ) VALUES (
            '600519', 'buy', 100, 10.0, 0.8, 'legacy linked fill',
            ?, ?, 'legacy-linked-fill'
        )
        """,
        (datetime.now().isoformat(), filled_round.id),
    )
    await conn.execute(
        "DELETE FROM schema_migrations WHERE version = ?",
        (ROUND_FILL_TERMINAL_REPAIR_VERSION,),
    )
    await conn.commit()

    await apply_architecture_migrations(conn)

    async with conn.execute(
        """
        SELECT id, terminal_code
        FROM research_rounds
        WHERE id IN (?, ?)
        ORDER BY id
        """,
        (filled_round.id, untouched_round.id),
    ) as cursor:
        terminals = {row[0]: row[1] for row in await cursor.fetchall()}
    assert terminals[filled_round.id] == "filled"
    assert terminals[untouched_round.id] == "market_closed_pending"
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM round_events
        WHERE round_id = ? AND event_type = 'RoundTerminalReconciled'
        """,
        (filled_round.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1

    await db.close()


@pytest.mark.asyncio
async def test_forward_migration_canonicalizes_only_known_failed_terminals(
    tmp_path,
):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        ROUND_FAILED_TERMINAL_REPAIR_VERSION,
        apply_architecture_migrations,
    )

    db = DatabaseService(str(tmp_path / "legacy_failed.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    known_failed = await coordinator.start(
        base_topic="cancelled legacy round",
        research_objective="canonicalize known failure terminal",
    )
    unknown_terminal = await coordinator.start(
        base_topic="unknown legacy round",
        research_objective="preserve unknown historical semantics",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        UPDATE research_rounds
        SET status = 'failed', current_stage = 'failed',
            terminal_code = 'cancelled', terminal_reason = 'legacy cancellation'
        WHERE id = ?
        """,
        (known_failed.id,),
    )
    await conn.execute(
        """
        UPDATE research_rounds
        SET status = 'failed', current_stage = 'failed',
            terminal_code = 'unknown_legacy_failure',
            terminal_reason = 'do not infer'
        WHERE id = ?
        """,
        (unknown_terminal.id,),
    )
    await conn.commit()
    async with conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (ROUND_FAILED_TERMINAL_REPAIR_VERSION,),
    ) as cursor:
        assert await cursor.fetchone() is not None

    await apply_architecture_migrations(conn)

    async with conn.execute(
        """
        SELECT id, terminal_code
        FROM research_rounds
        WHERE id IN (?, ?)
        """,
        (known_failed.id, unknown_terminal.id),
    ) as cursor:
        terminals = {row[0]: row[1] for row in await cursor.fetchall()}
    assert terminals[known_failed.id] == "failed"
    assert terminals[unknown_terminal.id] == "unknown_legacy_failure"
    async with conn.execute(
        """
        SELECT payload_json
        FROM round_events
        WHERE round_id = ? AND event_type = 'RoundTerminalReconciled'
        """,
        (known_failed.id,),
    ) as cursor:
        event = await cursor.fetchone()
    assert event is not None
    assert json.loads(event[0])["previous_terminal_code"] == "cancelled"
    await db.close()


@pytest.mark.asyncio
async def test_forward_migration_restores_only_exact_sigterm_overwrite(tmp_path):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        SIGTERM_BUSINESS_TERMINAL_REPAIR_VERSION,
        apply_architecture_migrations,
    )

    db = DatabaseService(str(tmp_path / "sigterm-overwrite.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    exact = await coordinator.start(
        base_topic="exact no evidence terminal",
        research_objective="restore exact SIGTERM overwrite",
    )
    await coordinator.advance(exact.id, ResearchRoundStatus.MEMORY_LOADED)
    await coordinator.advance(
        exact.id,
        ResearchRoundStatus.NO_EVIDENCE,
        event_type="NoEvidenceTerminal",
        payload={"source_count": 8, "proposal_count": 0},
        terminal_code="no_evidence",
        terminal_reason="no qualified proposal",
    )
    await coordinator.advance(
        exact.id,
        ResearchRoundStatus.FAILED,
        event_type="RoundFailed",
        payload={
            "code": "failed",
            "reason": "deployment stop",
            "shutdown_signal": "SIGTERM",
        },
        terminal_code="failed",
        terminal_reason="deployment stop",
    )

    unrelated = await coordinator.start(
        base_topic="ordinary pipeline failure",
        research_objective="do not infer a business terminal",
    )
    await coordinator.advance(
        unrelated.id,
        ResearchRoundStatus.FAILED,
        event_type="RoundFailed",
        payload={"code": "failed", "reason": "ordinary exception"},
        terminal_code="failed",
        terminal_reason="ordinary exception",
    )
    conn = await db._get_connection()
    await conn.commit()

    await apply_architecture_migrations(conn)
    await apply_architecture_migrations(conn)

    repaired = await coordinator.get(exact.id)
    untouched = await coordinator.get(unrelated.id)
    assert repaired.status == ResearchRoundStatus.NO_EVIDENCE
    assert repaired.terminal_code == "no_evidence"
    assert untouched.status == ResearchRoundStatus.FAILED
    assert untouched.terminal_code == "failed"
    async with conn.execute(
        """
        SELECT event_type, payload_json
        FROM round_events
        WHERE round_id = ?
          AND event_type IN (
              'RoundTerminalReconciled', 'RoundFinalizationInterrupted'
          )
        ORDER BY sequence
        """,
        (exact.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert [row[0] for row in events] == [
        "RoundTerminalReconciled",
        "RoundFinalizationInterrupted",
    ]
    assert json.loads(events[0][1])["business_terminal_sequence"] < json.loads(
        events[0][1]
    )["shutdown_event_sequence"]
    assert json.loads(events[1][1])["shutdown_signal"] == "SIGTERM"
    async with conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (SIGTERM_BUSINESS_TERMINAL_REPAIR_VERSION,),
    ) as cursor:
        assert await cursor.fetchone() is not None
    await db.close()


@pytest.mark.asyncio
async def test_forward_migration_clears_only_exact_executed_intent_blockers(
    tmp_path,
    monkeypatch,
):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        EXECUTED_INTENT_DEFER_REPAIR_VERSION,
        EXECUTED_PENDING_DEFER_REPAIR_VERSION,
        apply_architecture_migrations,
    )

    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    coordinator = ResearchRoundCoordinator(db)
    exact_round = await coordinator.start(
        base_topic="exact executed intent",
        research_objective="reconcile stale current blocker",
    )
    unknown_round = await coordinator.start(
        base_topic="unknown executed intent",
        research_objective="preserve uncertain legacy state",
    )
    exact_intent = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="exact replay fill",
        round_id=exact_round.id,
        idempotency_key="migration:exact-executed-intent",
    )
    unknown_intent = await simulation.create_execution_intent(
        ticker="000001",
        direction="long",
        target_position=0.10,
        reason="unknown legacy execution",
        round_id=unknown_round.id,
        idempotency_key="migration:unknown-executed-intent",
    )
    exact_pending = await simulation.record_pending_decision(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="exact replay pending",
        defer_code="market_closed",
        round_id=exact_round.id,
        intent_id=exact_intent,
    )
    unknown_pending = await simulation.record_pending_decision(
        ticker="000001",
        direction="long",
        target_position=0.10,
        reason="unknown legacy pending",
        defer_code="market_closed",
        round_id=unknown_round.id,
        intent_id=unknown_intent,
    )
    conn = await db._get_connection()
    # Model a row written by a pre-guard runner.  Current production databases
    # already have these triggers, so legacy-corruption setup must explicitly
    # remove them before re-running the forward migration.
    await conn.execute(
        "DROP TRIGGER IF EXISTS execution_intents_executed_defer_guard_insert"
    )
    await conn.execute(
        "DROP TRIGGER IF EXISTS execution_intents_executed_defer_guard_update"
    )
    await conn.execute(
        "DROP TRIGGER IF EXISTS simulation_pending_executed_defer_guard_insert"
    )
    await conn.execute(
        "DROP TRIGGER IF EXISTS simulation_pending_executed_defer_guard_update"
    )
    traded_at = datetime.now().isoformat()
    cursor = await conn.execute(
        """
        INSERT INTO simulation_trades (
            ticker, direction, shares, price, fee, reason, traded_at,
            round_id, intent_id, idempotency_key
        ) VALUES (
            '600519', 'buy', 100, 10.0, 0.8, 'exact linked fill',
            ?, ?, ?, 'migration:exact-linked-fill'
        )
        """,
        (traded_at, exact_round.id, exact_intent),
    )
    trade_id = int(cursor.lastrowid)
    await conn.execute(
        """
        UPDATE execution_intents
        SET status = 'executed', defer_code = 'market_closed',
            resolution = ?, executed_at = ?
        WHERE id = ?
        """,
        (f"simulation_trade:{trade_id}", traded_at, exact_intent),
    )
    await conn.execute(
        """
        UPDATE execution_intents
        SET status = 'executed', defer_code = 'market_closed',
            resolution = 'unverified_legacy_result', executed_at = ?
        WHERE id = ?
        """,
        (traded_at, unknown_intent),
    )
    await conn.execute(
        """
        UPDATE simulation_pending_decisions
        SET status = 'executed', resolved_at = ?, updated_at = ?,
            resolution = 'simulation_trade:' || ?
        WHERE id = ?
        """,
        (traded_at, traded_at, trade_id, exact_pending),
    )
    await conn.execute(
        """
        UPDATE simulation_pending_decisions
        SET status = 'executed', resolved_at = ?, updated_at = ?,
            resolution = 'unverified_legacy_result'
        WHERE id = ?
        """,
        (traded_at, traded_at, unknown_pending),
    )
    await conn.execute(
        "DELETE FROM schema_migrations WHERE version = ?",
        (EXECUTED_INTENT_DEFER_REPAIR_VERSION,),
    )
    await conn.execute(
        "DELETE FROM schema_migrations WHERE version = ?",
        (EXECUTED_PENDING_DEFER_REPAIR_VERSION,),
    )
    await conn.commit()

    await apply_architecture_migrations(conn)
    await apply_architecture_migrations(conn)

    async with conn.execute(
        "SELECT id, defer_code FROM execution_intents WHERE id IN (?, ?)",
        (exact_intent, unknown_intent),
    ) as cursor:
        codes = {row[0]: row[1] for row in await cursor.fetchall()}
    assert codes[exact_intent] is None
    assert codes[unknown_intent] == "market_closed"
    async with conn.execute(
        "SELECT id, defer_code FROM simulation_pending_decisions WHERE id IN (?, ?)",
        (exact_pending, unknown_pending),
    ) as cursor:
        pending_codes = {row[0]: row[1] for row in await cursor.fetchall()}
    assert pending_codes[exact_pending] == ""
    assert pending_codes[unknown_pending] == "market_closed"
    async with conn.execute(
        """
        SELECT payload_json FROM round_events
        WHERE round_id = ?
          AND event_type = 'ExecutionIntentTerminalReconciled'
        """,
        (exact_round.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert len(events) == 1
    payload = json.loads(events[0][0])
    assert payload["intent_id"] == exact_intent
    assert payload["previous_defer_code"] == "market_closed"
    assert payload["trade_id"] == trade_id
    async with conn.execute(
        """
        SELECT payload_json FROM round_events
        WHERE round_id = ?
          AND event_type = 'PendingDecisionTerminalReconciled'
        """,
        (exact_round.id,),
    ) as cursor:
        pending_events = await cursor.fetchall()
    assert len(pending_events) == 1
    pending_payload = json.loads(pending_events[0][0])
    assert pending_payload["pending_decision_id"] == exact_pending
    assert pending_payload["previous_defer_code"] == "market_closed"
    assert pending_payload["trade_id"] == trade_id
    await db.close()


@pytest.mark.asyncio
async def test_sqlite_guard_rejects_executed_intent_with_current_defer_code(
    tmp_path,
    monkeypatch,
):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        EXECUTED_INTENT_SEMANTIC_GUARD_VERSION,
        EXECUTED_PENDING_SEMANTIC_GUARD_VERSION,
    )

    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="intent terminal semantic guard",
        research_objective="surface SQLite enforcement in the shared read model",
    )
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="guard direct SQLite writers",
        round_id=round_state.id,
        idempotency_key="semantic-guard:executed-defer",
    )
    conn = await db._get_connection()
    pending_id = await simulation.record_pending_decision(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="guard direct pending writers",
        defer_code="market_closed",
        round_id=round_state.id,
        intent_id=intent_id,
    )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="executed execution_intent must clear defer_code",
    ):
        await conn.execute(
            """
            UPDATE execution_intents
            SET status = 'executed', defer_code = 'market_closed'
            WHERE id = ?
            """,
            (intent_id,),
        )

    async with conn.execute(
        "SELECT status, defer_code FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        row = await cursor.fetchone()
    assert tuple(row) == ("pending", None)

    with pytest.raises(
        sqlite3.IntegrityError,
        match="executed pending decision must clear defer_code",
    ):
        await conn.execute(
            """
            UPDATE simulation_pending_decisions
            SET status = 'executed'
            WHERE id = ?
            """,
            (pending_id,),
        )
    async with conn.execute(
        "SELECT status, defer_code FROM simulation_pending_decisions WHERE id = ?",
        (pending_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            "pending_next_trading_session",
            "market_closed",
        )

    await conn.execute(
        """
        UPDATE execution_intents
        SET status = 'executed', defer_code = NULL,
            resolution = 'simulation_trade:123'
        WHERE id = ?
        """,
        (intent_id,),
    )
    async with conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (EXECUTED_INTENT_SEMANTIC_GUARD_VERSION,),
    ) as cursor:
        assert await cursor.fetchone() is not None
    async with conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (EXECUTED_PENDING_SEMANTIC_GUARD_VERSION,),
    ) as cursor:
        assert await cursor.fetchone() is not None

    status = await get_system_status(db)
    assert status["execution_intent_terminal_guard"] == {
        "active": True,
        "trigger_count": 2,
        "violation_count": 0,
    }
    assert status["pending_decision_terminal_guard"] == {
        "active": True,
        "trigger_count": 2,
        "violation_count": 0,
    }
    assert "intent_terminal_guard=active, violations=0" in format_system_status(
        status
    )
    assert "pending_terminal_guard=active, violations=0" in format_system_status(
        status
    )
    await db.close()


@pytest.mark.asyncio
async def test_linked_exit_pending_is_nonexpiring_repaired_and_guarded(
    tmp_path,
    monkeypatch,
):
    from sovereign_hall.infrastructure.sqlite.migrations import (
        PENDING_EXIT_NONEXPIRING_GUARD_VERSION,
        PENDING_EXIT_NONEXPIRING_REPAIR_VERSION,
        apply_architecture_migrations,
    )

    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="durable lifecycle exit",
        research_objective="do not expire an open-position exit while quotes are unavailable",
    )
    exit_intent_id = await simulation.create_execution_intent(
        ticker="588860",
        direction="sell",
        target_position=0.0,
        reason="max duration exit",
        round_id=round_state.id,
        idempotency_key="lifecycle:588860:exit:episode-1",
    )
    exit_pending_id = await simulation.record_pending_decision(
        ticker="588860",
        direction="sell",
        target_position=0.0,
        reason="wait for fresh open-session quote",
        defer_code="market_closed",
        round_id=round_state.id,
        intent_id=exit_intent_id,
    )
    entry_intent_id = await simulation.create_execution_intent(
        ticker="516820",
        direction="long",
        target_position=0.1,
        reason="time-limited committee entry",
        round_id=round_state.id,
        idempotency_key="committee:516820:entry:episode-1",
    )
    entry_pending_id = await simulation.record_pending_decision(
        ticker="516820",
        direction="long",
        target_position=0.1,
        reason="wait for next session",
        defer_code="market_closed",
        round_id=round_state.id,
        intent_id=entry_intent_id,
    )
    conn = await db._get_connection()
    async with conn.execute(
        "SELECT expires_at FROM simulation_pending_decisions WHERE id = ?",
        (exit_pending_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] is None
    async with conn.execute(
        "SELECT expires_at FROM simulation_pending_decisions WHERE id = ?",
        (entry_pending_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] is not None

    # Recreate the exact legacy state from the pre-guard writer, then prove the
    # forward migration repairs only the linked active exit and appends its
    # known round lineage once.
    await conn.executescript(
        """
        DROP TRIGGER simulation_pending_exit_expiry_guard_insert;
        DROP TRIGGER simulation_pending_exit_expiry_guard_update;
        """
    )
    legacy_expiry = (datetime.now() + timedelta(days=7)).isoformat()
    await conn.execute(
        "UPDATE simulation_pending_decisions SET expires_at = ? WHERE id = ?",
        (legacy_expiry, exit_pending_id),
    )
    await conn.commit()

    await apply_architecture_migrations(conn)
    await apply_architecture_migrations(conn)

    async with conn.execute(
        "SELECT expires_at FROM simulation_pending_decisions WHERE id = ?",
        (exit_pending_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] is None
    async with conn.execute(
        """
        SELECT payload_json FROM round_events
        WHERE round_id = ? AND event_type = 'PendingExitExpiryReconciled'
        """,
        (round_state.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert len(events) == 1
    event_payload = json.loads(events[0][0])
    assert event_payload == {
        "intent_id": exit_intent_id,
        "pending_decision_id": exit_pending_id,
        "previous_expires_at": legacy_expiry,
        "status": "pending_next_trading_session",
    }

    with pytest.raises(
        sqlite3.IntegrityError,
        match="active linked exit pending decision must not expire",
    ):
        await conn.execute(
            "UPDATE simulation_pending_decisions SET expires_at = ? WHERE id = ?",
            (legacy_expiry, exit_pending_id),
        )
    for version in (
        PENDING_EXIT_NONEXPIRING_REPAIR_VERSION,
        PENDING_EXIT_NONEXPIRING_GUARD_VERSION,
    ):
        async with conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (version,),
        ) as cursor:
            assert await cursor.fetchone() is not None

    status = await get_system_status(db)
    assert status["pending_exit_expiry_guard"] == {
        "active": True,
        "trigger_count": 2,
        "violation_count": 0,
    }
    assert "exit_pending_expiry_guard=active, violations=0" in (
        format_system_status(status)
    )
    await db.close()


@pytest.mark.asyncio
async def test_committee_outcome_and_round_event_are_atomic_and_idempotent(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="durable committee decision",
        research_objective="persist each completed proposal immediately",
    )
    decision = {
        "decision_id": "decision-durable-1",
        "round_id": round_state.id,
        "ticker": "600519",
        "direction": "hold",
        "confidence": 0.7,
        "target_position": 0.0,
        "vote_summary": {"long": 0.0, "hold": 4.5},
        "vote_margin": 1.0,
        "vote_count": 4,
        "parsed_vote_count": 4,
        "invalid_vote_count": 0,
        "vote_quorum_required": 3,
        "vote_quorum_met": True,
        "stage_execution_audit": [
            {
                "stage": "round4_vote",
                "input_lineage": "persisted_round_proposal",
                "committee_network_search_requests": 0,
            }
        ],
    }

    await simulation.record_committee_outcomes(
        [decision],
        round_id=round_state.id,
        append_round_events=True,
    )
    await simulation.record_committee_outcomes(
        [decision],
        round_id=round_state.id,
        append_round_events=True,
    )

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM simulation_committee_outcomes
        WHERE decision_id = ?
        """,
        (decision["decision_id"],),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM round_events
        WHERE round_id = ? AND event_type = 'CommitteeDecisionPersisted'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1

    await conn.execute(
        """
        CREATE TRIGGER fail_committee_round_event
        BEFORE INSERT ON round_events
        WHEN NEW.event_type = 'CommitteeDecisionPersisted'
        BEGIN
            SELECT RAISE(ABORT, 'forced committee event failure');
        END
        """
    )
    await conn.commit()
    failed_decision = {
        **decision,
        "decision_id": "decision-durable-rollback",
        "ticker": "000001",
    }
    with pytest.raises(sqlite3.IntegrityError):
        await simulation.record_committee_outcomes(
            [failed_decision],
            round_id=round_state.id,
            append_round_events=True,
        )
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM simulation_committee_outcomes
        WHERE decision_id = ?
        """,
        (failed_decision["decision_id"],),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_database_service_is_scoped_by_resolved_path(tmp_path):
    first = await DatabaseService.get_instance(str(tmp_path / "first.db"))
    second = await DatabaseService.get_instance(str(tmp_path / "second.db"))
    assert first is not second
    assert first.db_path != second.db_path
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_legacy_conclusion_id_is_repaired_as_valid_foreign_key_parent(
    tmp_path,
):
    db_path = tmp_path / "legacy_parent.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE report_conclusions (
            id INT,
            question TEXT,
            conclusion TEXT
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX ux_report_conclusions_id "
        "ON report_conclusions(id) WHERE id IS NOT NULL"
    )
    conn.execute(
        """
        CREATE TABLE price_predictions (
            id TEXT PRIMARY KEY,
            conclusion_id TEXT,
            ticker TEXT NOT NULL,
            FOREIGN KEY(conclusion_id) REFERENCES report_conclusions(id)
        )
        """
    )
    conn.execute(
        "INSERT INTO report_conclusions(id, question, conclusion) "
        "VALUES (1, 'q', 'c')"
    )
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db._init_db()
    active = await db._get_connection()
    async with active.execute("PRAGMA foreign_key_check") as cursor:
        assert await cursor.fetchall() == []
    async with active.execute(
        "PRAGMA index_list('report_conclusions')"
    ) as cursor:
        indexes = [tuple(row) for row in await cursor.fetchall()]
    repaired = next(row for row in indexes if row[1] == "ux_report_conclusions_id")
    assert repaired[2] == 1
    assert repaired[4] == 0
    with pytest.raises(sqlite3.IntegrityError):
        await active.execute(
            "INSERT INTO price_predictions(id, conclusion_id, ticker) "
            "VALUES ('invalid-parent', '999', '600519')"
        )
    await active.rollback()
    await db.close()


@pytest.mark.asyncio
async def test_round_state_machine_is_durable_and_status_read_model_is_shared(
    tmp_path,
):
    db = DatabaseService(str(tmp_path / "round.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="test",
        research_objective="test evidence",
    )
    await coordinator.record_event(
        round_state.id,
        "PortfolioReviewBeforeResearch",
        {"positions": 0},
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.SOURCES_PERSISTED,
    )
    terminal = await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.NO_EVIDENCE,
        terminal_code="no_evidence",
        terminal_reason="no qualified proposal",
    )
    terminal_status = await get_system_status(db)
    assert terminal.status == ResearchRoundStatus.NO_EVIDENCE
    assert terminal_status["pipeline_health"] == "completed_no_evidence"
    assert terminal_status["round_finalization_pending"] is True
    await coordinator.advance(round_state.id, ResearchRoundStatus.REFLECTED)
    completed = await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.COMPLETED,
    )

    status = await get_system_status(db)
    assert completed.terminal_code == "no_evidence"
    assert status["pipeline_health"] == "completed_no_evidence"
    assert status["round_finalization_pending"] is False
    assert status["latest_round"]["id"] == round_state.id
    async with (await db._get_connection()).execute(
        "SELECT COUNT(*), MAX(sequence) FROM round_events WHERE round_id = ?",
        (round_state.id,),
    ) as cursor:
        count, maximum = await cursor.fetchone()
    assert count == maximum == 7
    with pytest.raises(ValueError):
        await coordinator.advance(
            round_state.id,
            ResearchRoundStatus.FILLED,
        )
    await db.close()


@pytest.mark.asyncio
async def test_sources_persist_with_traceable_wiki_lineage_and_shared_read_model(
    tmp_path,
):
    db = DatabaseService(str(tmp_path / "source_lineage.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="traceable sources",
        research_objective="exclude derived evidence without raw lineage",
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )

    source_by_id = Document(
        title="固态电池公司公告原文",
        content="公司公告披露固态电池中试线进度与订单验证。" * 8,
        url="https://example.com/solid-state-disclosure",
        source="unit",
        doc_id="doc_source_by_id",
    )
    source_by_title = Document(
        title="固态电池行业供需报告",
        content="行业报告记录供需、产能与现金流验证数据。" * 8,
        url="https://example.com/solid-state-supply",
        source="unit",
        doc_id="doc_source_by_title",
    )
    assert await db.add_document(source_by_id) is True
    assert await db.add_document(source_by_title) is True
    conn = await db._get_connection()
    async with conn.execute(
        "SELECT COUNT(*) FROM round_documents WHERE round_id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0

    wiki_source = Document(
        content="由 source page 提取的派生摘要",
        source="obsidian_wiki",
        doc_id="wiki:wiki/sources/source.md",
        metadata={
            "title": source_by_id.title,
            "wiki_type": "source",
            "source_id": source_by_id.id,
            "url": source_by_id.url,
            "source_ids": [source_by_id.url],
        },
    )
    wiki_topic = Document(
        content="由 topic page 汇总的派生摘要",
        source="obsidian_wiki",
        doc_id="wiki:wiki/topics/topic.md",
        metadata={
            "title": "固态电池研究",
            "wiki_type": "topic",
            "source_ids": [source_by_title.title],
        },
    )
    untraceable = Document(
        content="没有任何原始资料身份的派生摘要",
        source="obsidian_wiki",
        doc_id="wiki:wiki/topics/untraceable.md",
        metadata={"title": "无法追溯的派生页", "wiki_type": "topic"},
    )

    lineage = await db.resolve_document_lineage(
        [wiki_source, wiki_topic, untraceable]
    )
    assert lineage == [[source_by_id.id], [source_by_title.id], []]

    await coordinator.persist_sources(
        round_state.id,
        document_ids=[source_by_id.id, source_by_title.id],
        presented_document_count=3,
        traceable_document_count=2,
        untraceable_document_count=1,
    )
    status = await get_system_status(db)
    assert status["counts"]["documents"] == 2
    assert status["source_lineage"] == {
        "claimed_source_count": 2,
        "linked_source_count": 2,
        "presented_document_count": 3,
        "traceable_document_count": 2,
        "untraceable_document_count": 1,
        "resolved_document_count": 2,
        "capacity_excluded_document_count": 0,
        "resolved_source_count_before_limit": 2,
        "lineage_limit": 300,
        "consistent": True,
    }
    async with conn.execute(
        "SELECT payload_json FROM round_events "
        "WHERE round_id = ? AND event_type = 'SourcesPersisted'",
        (round_state.id,),
    ) as cursor:
        payload = json.loads((await cursor.fetchone())[0])
    assert payload["linked_source_count"] == 2
    assert payload["untraceable_document_count"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_source_lineage_prefers_explicit_identity_and_rejects_fanout(
    tmp_path,
    monkeypatch,
):
    import sovereign_hall.services.database as database_module

    db = DatabaseService(str(tmp_path / "source_lineage_fanout.db"))
    await db._init_db()
    sources = []
    for index in range(3):
        source = Document(
            title="重复但不能扩散的来源标题",
            content=f"独立原始资料 {index} 提供可验证事实、时间和数据。" * 8,
            url=f"https://example.com/source-{index}",
            source="unit",
            doc_id=f"doc_source_{index}",
        )
        assert await db.add_document(source) is True
        sources.append(source)

    explicit = Document(
        content="来源页派生摘要",
        source="obsidian_wiki",
        doc_id="wiki:wiki/sources/explicit.md",
        metadata={
            "title": sources[0].title,
            "wiki_type": "source",
            "source_id": sources[0].id,
        },
    )
    assert await db.resolve_document_lineage([explicit]) == [[sources[0].id]]

    monkeypatch.setattr(
        database_module,
        "MAX_CANONICAL_LINEAGE_PER_PRESENTED_DOCUMENT",
        2,
    )
    accumulated_topic = Document(
        content="聚合页不应把全部历史来源注入当前 round",
        source="obsidian_wiki",
        doc_id="wiki:wiki/topics/accumulated.md",
        metadata={
            "title": "累积来源聚合页",
            "wiki_type": "topic",
            "source_ids": [source.id for source in sources],
        },
    )
    assert await db.resolve_document_lineage([accumulated_topic]) == [[]]
    await db.close()


@pytest.mark.asyncio
async def test_source_lineage_transaction_rolls_back_missing_document(tmp_path):
    db = DatabaseService(str(tmp_path / "source_lineage_rollback.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="rollback",
        research_objective="source event and links are atomic",
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )

    with pytest.raises(RuntimeError, match="missing canonical documents"):
        await coordinator.persist_sources(
            round_state.id,
            document_ids=["doc_does_not_exist"],
            presented_document_count=1,
            traceable_document_count=1,
            untraceable_document_count=0,
        )

    current = await coordinator.get(round_state.id)
    assert current.status == ResearchRoundStatus.MEMORY_LOADED
    conn = await db._get_connection()
    async with conn.execute(
        "SELECT COUNT(*) FROM round_documents WHERE round_id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT COUNT(*) FROM round_events WHERE round_id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 2
    await db.close()


@pytest.mark.asyncio
async def test_status_exposes_legacy_sources_persisted_count_mismatch(tmp_path):
    db = DatabaseService(str(tmp_path / "legacy_source_mismatch.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="legacy mismatch",
        research_objective="make an old in-memory source claim visible",
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.SOURCES_PERSISTED,
        event_type="SourcesPersisted",
        payload={"source_count": 3},
    )

    status = await get_system_status(db)
    assert status["source_lineage"] == {
        "claimed_source_count": 3,
        "linked_source_count": 0,
        "presented_document_count": None,
        "traceable_document_count": None,
        "untraceable_document_count": None,
        "resolved_document_count": None,
        "capacity_excluded_document_count": 0,
        "resolved_source_count_before_limit": None,
        "lineage_limit": None,
        "consistent": False,
    }
    await db.close()


@pytest.mark.asyncio
async def test_status_exposes_repeated_failure_in_recent_round_window(tmp_path):
    db = DatabaseService(str(tmp_path / "recent_failure_recurrence.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    reason = (
        "研究轮失败：管线异常：round source lineage exceeds configured hard limit: "
        "actual=768 limit=300"
    )
    for index in range(3):
        round_state = await coordinator.start(
            base_topic=f"source-heavy-{index}",
            research_objective="surface repeated production failure",
        )
        await coordinator.fail(round_state.id, code="failed", reason=reason)

    active = await coordinator.start(
        base_topic="recovered",
        research_objective="latest round can be active while recurrence stays visible",
    )
    status = await get_system_status(db)

    assert status["latest_round"]["id"] == active.id
    recurrence = status["recent_failure_recurrence"]
    assert recurrence["reason"] == reason
    assert recurrence["count_in_recent_rounds"] == 3
    assert recurrence["last_seen"]
    assert recurrence["sample_size"] == 100
    assert "recent_failure_recurrence=3x/100rounds" in format_system_status(status)
    await db.close()


@pytest.mark.asyncio
async def test_status_exposes_round_linked_search_query_gate(tmp_path):
    db = DatabaseService(str(tmp_path / "search_query_gate.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    active = await coordinator.start(
        base_topic="query gate audit",
        research_objective="block repair prompt leakage before network research",
    )
    await coordinator.record_event(
        active.id,
        "SearchQueriesValidated",
        {
            "generated_candidate_count": 3,
            "generated_accepted_count": 2,
            "generator_rejection_counts": {"meta_instruction_leak": 1},
            "submitted_query_count": 4,
            "provider_accepted_count": 3,
            "provider_rejection_counts": {"duplicate_query": 1},
            "provider_health": {
                "circuit_open_sources": ["bing"],
                "skipped_open_circuit_counts": {"bing": 7},
            },
            "availability_status": "sources_available",
            "result_document_count": 8,
            "network_enabled": True,
        },
    )

    status = await get_system_status(db)

    assert status["search_query_gate"]["rejected_count"] == 2
    assert status["search_query_gate"]["rejection_counts"] == {
        "duplicate_query": 1,
        "meta_instruction_leak": 1,
    }
    formatted = format_system_status(status)
    assert "search_queries=submitted=4" in formatted
    assert "meta_instruction_leak:1" in formatted
    assert "availability=sources_available, result_docs=8" in formatted
    assert "search_providers=open=bing, circuit_skips=7" in formatted
    await db.close()


@pytest.mark.asyncio
async def test_single_instance_startup_recovers_only_unterminated_rounds(tmp_path):
    db = DatabaseService(str(tmp_path / "abandoned_round.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    abandoned = await coordinator.start(
        base_topic="abandoned search",
        research_objective="recover after prior process exit",
    )
    await coordinator.advance(
        abandoned.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    terminal = await coordinator.start(
        base_topic="durable pending execution",
        research_objective="preserve explicit terminal",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        UPDATE research_rounds
        SET status = 'market_closed_pending',
            current_stage = 'market_closed_pending',
            terminal_code = 'market_closed_pending',
            terminal_reason = 'fresh quote required next session'
        WHERE id = ?
        """,
        (terminal.id,),
    )
    await conn.commit()

    recovered = await coordinator.recover_abandoned_rounds()
    replay = await coordinator.recover_abandoned_rounds()

    assert [item["round_id"] for item in recovered] == [abandoned.id]
    assert replay == []
    recovered_round = await coordinator.get(abandoned.id)
    preserved_round = await coordinator.get(terminal.id)
    assert recovered_round.status == ResearchRoundStatus.FAILED
    assert recovered_round.terminal_code == "failed"
    assert "previous_stage=memory_loaded" in recovered_round.terminal_reason
    assert preserved_round.status == ResearchRoundStatus.MARKET_CLOSED_PENDING
    assert preserved_round.terminal_code == "market_closed_pending"
    async with conn.execute(
        """
        SELECT COUNT(*), payload_json
        FROM round_events
        WHERE round_id = ? AND event_type = 'AbandonedRoundRecovered'
        """,
        (abandoned.id,),
    ) as cursor:
        count, payload_json = await cursor.fetchone()
    assert count == 1
    assert json.loads(payload_json)["previous_stage"] == "memory_loaded"
    await db.close()


@pytest.mark.asyncio
async def test_abandoned_round_recovery_rolls_back_if_event_write_fails(tmp_path):
    db = DatabaseService(str(tmp_path / "abandoned_round_rollback.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    abandoned = await coordinator.start(
        base_topic="abandoned search",
        research_objective="prove atomic recovery rollback",
    )
    await coordinator.advance(
        abandoned.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TRIGGER fail_abandoned_round_event
        BEFORE INSERT ON round_events
        WHEN NEW.event_type = 'AbandonedRoundRecovered'
        BEGIN
            SELECT RAISE(ABORT, 'forced recovery event failure');
        END
        """
    )
    await conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        await coordinator.recover_abandoned_rounds()

    unchanged = await coordinator.get(abandoned.id)
    assert unchanged.status == ResearchRoundStatus.MEMORY_LOADED
    assert unchanged.terminal_code is None
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM round_events
        WHERE round_id = ? AND event_type = 'AbandonedRoundRecovered'
        """,
        (abandoned.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_execution_intent_creates_one_atomic_fill_quote_ledger_and_projection(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="atomic fill",
        research_objective="fresh quote execution",
    )
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        confidence=0.9,
        reason="qualified committee ruling",
        round_id=round_state.id,
        idempotency_key="architecture:fill:1",
    )
    result = await simulation.execute_intent(intent_id)
    replay = await simulation.execute_intent(intent_id)

    assert result["action"] == "buy"
    assert result["price_source"] == "architecture_test_realtime"
    assert replay["action"] == "already_executed"
    conn = await db._get_connection()
    for table in (
        "simulation_trades",
        "quote_snapshots",
        "simulation_ledger_entries",
        "simulation_positions",
    ):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        "SELECT status FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "executed"
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "filled"
    async with conn.execute(
        """
        SELECT COUNT(*)
        FROM round_events
        WHERE round_id = ? AND event_type = 'SimulatedFillCreated'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
    ) as cursor:
        persisted_cash = float((await cursor.fetchone())[0])
    assert persisted_cash == pytest.approx(simulation.cash)
    async with conn.execute(
        """
        SELECT fee, commission, stamp_duty, slippage_cost
        FROM simulation_trades
        """
    ) as cursor:
        fee, commission, stamp_duty, slippage = await cursor.fetchone()
    assert commission > 0
    assert slippage > 0
    assert stamp_duty == 0
    assert fee == pytest.approx(commission + slippage)
    await db.close()


@pytest.mark.asyncio
async def test_simulated_sell_deducts_commission_stamp_duty_and_slippage(
    tmp_path,
    monkeypatch,
):
    market = OpenMarket(price=10.0)
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    buy_intent = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="open position",
        idempotency_key="architecture:cost:buy",
    )
    assert (await simulation.execute_intent(buy_intent))["action"] == "buy"
    market.price = 11.0
    sell_intent = await simulation.create_execution_intent(
        ticker="600519",
        direction="sell",
        target_position=0.0,
        reason="close position",
        idempotency_key="architecture:cost:sell",
    )
    assert (await simulation.execute_intent(sell_intent))["action"] == "sell"

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT fee, commission, stamp_duty, slippage_cost
        FROM simulation_trades
        WHERE direction = 'sell'
        """
    ) as cursor:
        fee, commission, stamp_duty, slippage = await cursor.fetchone()
    assert commission > 0
    assert stamp_duty > 0
    assert slippage > 0
    assert fee == pytest.approx(commission + stamp_duty + slippage)
    await db.close()


@pytest.mark.asyncio
async def test_atomic_fill_failure_rolls_back_quote_trade_ledger_cash_and_position(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TRIGGER fail_architecture_ledger
        BEFORE INSERT ON simulation_ledger_entries
        BEGIN
            SELECT RAISE(ABORT, 'forced ledger failure');
        END
        """
    )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        confidence=0.9,
        reason="force rollback",
        idempotency_key="architecture:rollback:1",
    )
    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "error"
    assert "回滚" in result["reason"]
    assert simulation.cash == pytest.approx(simulation.initial_capital)
    assert simulation.positions == {}
    for table in (
        "simulation_trades",
        "quote_snapshots",
        "simulation_ledger_entries",
        "simulation_positions",
    ):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT status FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "pending"
    await db.close()


@pytest.mark.asyncio
async def test_pending_replay_atomically_resolves_fill_and_round_terminal(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, ClosedMarket())
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="deferred fill",
        research_objective="replay after market opens",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        UPDATE research_rounds
        SET status = 'completed', current_stage = 'completed',
            terminal_code = 'market_closed_pending',
            terminal_reason = 'waiting for market open',
            completed_at = updated_at
        WHERE id = ?
        """,
        (round_state.id,),
    )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        confidence=0.9,
        reason="qualified deferred ruling",
        round_id=round_state.id,
        idempotency_key="architecture:pending-fill:1",
    )
    deferred = await simulation.execute_intent(intent_id)
    assert deferred["action"] == "pending"

    simulation.quote_provider = OpenMarket(price=10.0)
    replay = await simulation.replay_pending_decisions()
    repeated = await simulation.replay_pending_decisions()

    assert replay["executed"] == 1
    assert replay["executed_buys"] == 1
    assert replay["executed_sells"] == 0
    assert repeated["status"] == "empty"
    async with conn.execute(
        """
        SELECT status, resolution, defer_code
        FROM simulation_pending_decisions
        WHERE intent_id = ?
        """,
        (intent_id,),
    ) as cursor:
        pending_status, pending_resolution, pending_defer_code = await cursor.fetchone()
    assert pending_status == "executed"
    assert str(pending_resolution).startswith("simulation_trade:")
    assert pending_defer_code == ""
    async with conn.execute(
        "SELECT status, resolution, defer_code FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        intent_status, intent_resolution, intent_defer_code = await cursor.fetchone()
    assert intent_status == "executed"
    assert str(intent_resolution).startswith("simulation_trade:")
    assert intent_defer_code is None
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "filled"
    async with conn.execute(
        "SELECT COUNT(*) FROM simulation_trades WHERE intent_id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        """
        SELECT status, last_trade_count, last_entry_trade_count,
               last_exit_trade_count, source
        FROM simulation_redeployment_state WHERE id = 1
        """
    ) as cursor:
        redeployment = await cursor.fetchone()
    assert tuple(redeployment) == (
        "partially_redeployed",
        1,
        1,
        0,
        f"simulation_buy_trade:1:600519",
    )
    await db.close()


@pytest.mark.asyncio
async def test_pending_replay_round_terminal_failure_rolls_back_and_stays_retryable(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, ClosedMarket())
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="deferred rollback",
        research_objective="round terminal failure injection",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        UPDATE research_rounds
        SET status = 'completed', current_stage = 'completed',
            terminal_code = 'market_closed_pending',
            terminal_reason = 'waiting for market open',
            completed_at = updated_at
        WHERE id = ?
        """,
        (round_state.id,),
    )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        confidence=0.9,
        reason="retryable deferred ruling",
        round_id=round_state.id,
        idempotency_key="architecture:pending-fill:rollback",
    )
    assert (await simulation.execute_intent(intent_id))["action"] == "pending"
    await conn.execute(
        """
        CREATE TRIGGER fail_round_terminal_update
        BEFORE UPDATE OF terminal_code ON research_rounds
        BEGIN
            SELECT RAISE(ABORT, 'forced round terminal failure');
        END
        """
    )
    await conn.commit()

    simulation.quote_provider = OpenMarket(price=10.0)
    replay = await simulation.replay_pending_decisions()

    assert replay["executed"] == 0
    assert replay["results"][0]["action"] == "error"
    for table in (
        "simulation_trades",
        "quote_snapshots",
        "simulation_ledger_entries",
        "simulation_positions",
    ):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT status FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "deferred"
    async with conn.execute(
        """
        SELECT status, resolution
        FROM simulation_pending_decisions
        WHERE intent_id = ?
        """,
        (intent_id,),
    ) as cursor:
        pending_status, pending_resolution = await cursor.fetchone()
    assert pending_status == "pending_next_trading_session"
    assert str(pending_resolution).startswith("retryable_after_atomic_failure:")
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "market_closed_pending"
    assert simulation.cash == pytest.approx(simulation.initial_capital)
    assert simulation.positions == {}
    await db.close()


@pytest.mark.asyncio
async def test_daily_fill_limit_is_checked_in_transaction_and_defers_intent(
    tmp_path,
    monkeypatch,
):
    market = OpenMarket()
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    conn = await db._get_connection()
    now = datetime.now().isoformat()
    for index in range(5):
        await conn.execute(
            """
            INSERT INTO simulation_trades (
                ticker, direction, shares, price, fee, reason, traded_at,
                idempotency_key
            ) VALUES (?, 'buy', 100, 10, 0, 'seed daily limit', ?, ?)
            """,
            (f"60051{index}", now, f"seed:{index}"),
        )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="must defer",
        idempotency_key="architecture:limit:1",
    )
    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "pending"
    assert market.quote_calls == 0
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 5
    async with conn.execute(
        "SELECT status, defer_code FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        status, defer_code = await cursor.fetchone()
    assert status == "deferred"
    assert defer_code == "daily_trade_limit"
    await db.close()


@pytest.mark.asyncio
async def test_closed_market_defers_without_quote_or_fill(tmp_path, monkeypatch):
    market = ClosedMarket()
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="closed market",
        idempotency_key="architecture:closed:1",
    )
    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "pending"
    assert market.quote_calls == 0
    conn = await db._get_connection()
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute("SELECT COUNT(*) FROM quote_snapshots") as cursor:
        assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_exact_pending_prediction_lineage_is_recovered_without_price(
    tmp_path,
    monkeypatch,
):
    market = ClosedMarket()
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="legacy pending prediction",
        research_objective="recover only exact durable lineage",
    )
    decision_id = "decision-exact-recovery"
    await db.add_proposal(
        {
            "proposal_id": "proposal-exact-recovery",
            "ticker": "520500",
            "direction": "long",
            "target_position": 0.10,
            "stop_loss": 5.0,
            "take_profit": 15.0,
            "holding_period": 30,
            "confidence": 0.7,
            "thesis": "exact durable evidence",
        },
        round_id=round_state.id,
    )
    await simulation.record_committee_outcomes(
        [
            {
                "ticker": "520500",
                "direction": "long",
                "confidence": 0.7,
                "target_position": 0.10,
                "vote_summary": {"long": 4.0},
                "vote_margin": 1.0,
                "vote_count": 4,
                "parsed_vote_count": 4,
                "vote_quorum_required": 3,
                "vote_quorum_met": True,
                "decision_id": decision_id,
            }
        ],
        round_id=round_state.id,
    )
    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="long",
        target_position=0.10,
        confidence=0.7,
        reason="legacy missing prediction",
        round_id=round_state.id,
        decision_id=decision_id,
        idempotency_key="architecture:prediction-recovery:1",
    )
    pending = await simulation.execute_intent(intent_id)
    assert pending["action"] == "pending"

    recorder = DecisionRecorder(db.db_path)
    first = await recorder.recover_deferred_prediction_lineage()
    second = await recorder.recover_deferred_prediction_lineage()

    assert first["recovered"] == 1
    assert second["recovered"] == 0
    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT prediction.current_price, prediction.status,
               prediction.round_id, prediction.decision_id,
               outcome.prediction_id
        FROM price_predictions prediction
        JOIN simulation_committee_outcomes outcome
          ON outcome.prediction_id = prediction.id
        WHERE prediction.id = ?
        """,
        (first["prediction_ids"][0],),
    ) as cursor:
        row = await cursor.fetchone()
    assert tuple(row[:4]) == (
        None,
        "awaiting_entry_quote",
        round_state.id,
        decision_id,
    )
    assert row[4] == first["prediction_ids"][0]
    assert market.quote_calls == 0
    async with conn.execute(
        """
        SELECT COUNT(*) FROM round_events
        WHERE round_id = ? AND event_type = 'PredictionLineageRecovered'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    await db.close()


@pytest.mark.asyncio
async def test_exact_terminal_deferred_predictions_are_reconciled_without_price(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, ClosedMarket())
    coordinator = ResearchRoundCoordinator(db)
    linked_round = await coordinator.start(
        base_topic="legacy rejected intent",
        research_objective="reconcile exact terminal lineage",
    )
    unlinked_round = await coordinator.start(
        base_topic="legacy missing intent",
        research_objective="expose exact historical write gap",
    )
    recorder = DecisionRecorder(db.db_path)
    prediction_ids = []
    for round_state, ticker, decision_id in (
        (linked_round, "520500", "decision-linked-terminal"),
        (unlinked_round, "515320", "decision-unlinked-terminal"),
    ):
        prediction_id = await recorder.record_decision(
            ticker=ticker,
            decision="short",
            confidence=0.7,
            target_price=15.0,
            stop_loss=5.0,
            round_id=round_state.id,
            decision_id=decision_id,
            defer_quote_until_execution=True,
        )
        prediction_ids.append(prediction_id)
        await simulation.record_committee_outcomes(
            [
                {
                    "ticker": ticker,
                    "direction": "short",
                    "confidence": 0.7,
                    "target_position": 0.0,
                    "vote_summary": {"short": 4.0},
                    "vote_margin": 1.0,
                    "vote_count": 4,
                    "parsed_vote_count": 4,
                    "vote_quorum_required": 3,
                    "vote_quorum_met": True,
                    "prediction_id": prediction_id,
                    "decision_id": decision_id,
                }
            ],
            round_id=round_state.id,
        )

    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="short",
        target_position=0.0,
        reason="legacy direct rejection",
        round_id=linked_round.id,
        decision_id="decision-linked-terminal",
        idempotency_key="architecture:legacy-terminal:linked",
    )
    conn = await db._get_connection()
    await conn.execute(
        "UPDATE execution_intents SET status = 'rejected', "
        "defer_code = 'realtime_quote_unavailable' WHERE id = ?",
        (intent_id,),
    )
    await conn.execute(
        "UPDATE research_rounds SET terminal_code = 'market_closed_pending' "
        "WHERE id = ?",
        (linked_round.id,),
    )
    await conn.execute(
        "UPDATE research_rounds SET terminal_code = 'execution_rejected' "
        "WHERE id = ?",
        (unlinked_round.id,),
    )
    await conn.commit()

    first = await recorder.reconcile_deferred_prediction_terminals()
    second = await recorder.reconcile_deferred_prediction_terminals()

    assert first["reconciled"] == 2
    assert set(first["prediction_ids"]) == set(prediction_ids)
    assert second["reconciled"] == 0
    async with conn.execute(
        "SELECT id, status, current_price, quote_source "
        "FROM price_predictions WHERE id IN (?, ?) ORDER BY id",
        tuple(prediction_ids),
    ) as cursor:
        rows = await cursor.fetchall()
    assert len(rows) == 2
    assert all(tuple(row[1:]) == ("execution_rejected", None, None) for row in rows)
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (linked_round.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "execution_rejected"
    async with conn.execute(
        """
        SELECT payload_json FROM round_events
        WHERE event_type = 'DeferredPredictionTerminalReconciled'
        ORDER BY id
        """
    ) as cursor:
        payloads = [json.loads(row[0]) for row in await cursor.fetchall()]
    assert len(payloads) == 2
    assert {payload["legacy_missing_intent"] for payload in payloads} == {
        False,
        True,
    }
    assert all(payload["price_fabricated"] is False for payload in payloads)
    await db.close()


@pytest.mark.asyncio
async def test_execution_quote_atomically_anchors_deferred_prediction(
    tmp_path,
    monkeypatch,
):
    market = OpenMarket(price=8.0)
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="deferred prediction",
        research_objective="anchor only after price-free intent",
    )
    decision_id = "decision-deferred-anchor"
    prediction_id = await DecisionRecorder(db.db_path).record_decision(
        ticker="520500",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        round_id=round_state.id,
        decision_id=decision_id,
        defer_quote_until_execution=True,
    )
    assert market.quote_calls == 0
    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="long",
        target_position=0.10,
        confidence=0.7,
        reason="committee approved",
        round_id=round_state.id,
        decision_id=decision_id,
        idempotency_key="architecture:prediction-anchor:1",
    )

    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "buy"
    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT current_price, target_price, stop_loss, status, entry_date,
               quote_source, quote_fetched_at
        FROM price_predictions WHERE id = ?
        """,
        (prediction_id,),
    ) as cursor:
        prediction = await cursor.fetchone()
    assert tuple(prediction[:4]) == (8.0, 9.2, 7.6, "pending")
    assert prediction[4]
    assert prediction[5] == "architecture_test_realtime"
    assert prediction[6]
    async with conn.execute(
        """
        SELECT COUNT(*) FROM round_events
        WHERE round_id = ? AND event_type = 'PredictionQuoteAnchored'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    await db.close()


@pytest.mark.asyncio
async def test_prediction_anchor_rolls_back_with_failed_fill(
    tmp_path,
    monkeypatch,
):
    market = OpenMarket(price=8.0)
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="deferred prediction rollback",
        research_objective="keep prediction price-free after failed fill",
    )
    decision_id = "decision-deferred-rollback"
    prediction_id = await DecisionRecorder(db.db_path).record_decision(
        ticker="520500",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        round_id=round_state.id,
        decision_id=decision_id,
        defer_quote_until_execution=True,
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TRIGGER fail_prediction_fill
        BEFORE INSERT ON simulation_trades
        BEGIN
            SELECT RAISE(ABORT, 'forced prediction fill failure');
        END
        """
    )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="long",
        target_position=0.10,
        confidence=0.7,
        reason="force rollback",
        round_id=round_state.id,
        decision_id=decision_id,
        idempotency_key="architecture:prediction-anchor:rollback",
    )

    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "error"
    async with conn.execute(
        """
        SELECT current_price, status, quote_source, quote_fetched_at
        FROM price_predictions WHERE id = ?
        """,
        (prediction_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            None,
            "awaiting_entry_quote",
            None,
            None,
        )
    async with conn.execute(
        """
        SELECT COUNT(*) FROM round_events
        WHERE round_id = ? AND event_type = 'PredictionQuoteAnchored'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT COUNT(*) FROM quote_snapshots WHERE intent_id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_missing_realtime_quote_rejects_intent_without_fallback(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=None),
    )
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="missing quote",
        idempotency_key="architecture:noquote:1",
    )
    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "hold"
    assert "不使用本地估值或历史价格兜底" in result["reason"]
    conn = await db._get_connection()
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT status FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "rejected"
    await db.close()


@pytest.mark.asyncio
async def test_lifecycle_exit_quote_miss_reuses_intent_then_fills_on_fresh_replay(
    tmp_path,
    monkeypatch,
):
    market = OpenMarket(price=None)
    db, simulation = await build_simulation(tmp_path, monkeypatch, market)
    opened_at = (datetime.now() - timedelta(days=31)).isoformat()
    simulation.cash = 1_000.0
    simulation.positions = {
        "588860": {
            "shares": 100,
            "avg_cost": 0.63,
            "opened_at": opened_at,
            "peak_price": 0.63,
        }
    }
    await simulation.save_state()

    first = await simulation.review_open_positions()
    second = await simulation.review_open_positions()
    conn = await db._get_connection()
    intents = await conn.execute_fetchall(
        """
        SELECT id, status, defer_code, idempotency_key
        FROM execution_intents
        WHERE ticker = '588860' AND direction = 'sell'
        """
    )
    pending = await conn.execute_fetchall(
        """
        SELECT id, status, defer_code, intent_id
        FROM simulation_pending_decisions
        WHERE ticker = '588860' AND direction = 'sell'
        """
    )

    assert first[0]["action"] == "exit_pending_execution"
    assert second[0]["action"] == "exit_pending_execution"
    assert first[0]["execution"]["action"] == "pending"
    assert second[0]["execution"]["action"] == "pending"
    assert len(intents) == 1
    assert tuple(intents[0][1:]) == (
        "deferred",
        "realtime_quote_unavailable",
        f"lifecycle:588860:exit:{opened_at}",
    )
    assert len(pending) == 1
    assert tuple(pending[0][1:]) == (
        "pending_next_trading_session",
        "realtime_quote_unavailable",
        intents[0][0],
    )
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute("SELECT COUNT(*) FROM quote_snapshots") as cursor:
        assert (await cursor.fetchone())[0] == 0

    market.price = 0.73
    replay = await simulation.replay_pending_decisions()

    assert replay["executed"] == 1
    assert replay["executed_buys"] == 0
    assert replay["executed_sells"] == 1
    assert replay["remaining"] == 0
    async with conn.execute(
        "SELECT status, defer_code FROM execution_intents WHERE id = ?",
        (intents[0][0],),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == ("executed", None)
    async with conn.execute(
        "SELECT status, defer_code FROM simulation_pending_decisions WHERE id = ?",
        (pending[0][0],),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == ("executed", "")
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute("SELECT COUNT(*) FROM quote_snapshots") as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute("SELECT COUNT(*) FROM simulation_ledger_entries") as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        "SELECT COUNT(*) FROM simulation_positions WHERE ticker = '588860'"
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        """
        SELECT status, last_trade_count, last_entry_trade_count,
               last_exit_trade_count, blocker_code
        FROM simulation_redeployment_state WHERE id = 1
        """
    ) as cursor:
        redeployment = await cursor.fetchone()
    assert tuple(redeployment) == (
        "pending_approved_candidates",
        1,
        0,
        1,
        "released_capital_pending_redeployment",
    )
    await db.close()


@pytest.mark.asyncio
async def test_unquoted_rejection_atomically_closes_prediction_pending_and_round(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=None),
    )
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="missing execution quote",
        research_objective="close exact price-free lineage atomically",
    )
    decision_id = "decision-unquoted-rejection"
    prediction_id = await DecisionRecorder(db.db_path).record_decision(
        ticker="520500",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        round_id=round_state.id,
        decision_id=decision_id,
        defer_quote_until_execution=True,
    )
    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="long",
        target_position=0.10,
        confidence=0.7,
        reason="committee approved",
        round_id=round_state.id,
        decision_id=decision_id,
        idempotency_key="architecture:unquoted-rejection:1",
    )
    conn = await db._get_connection()
    cursor = await conn.execute(
        """
        INSERT INTO simulation_pending_decisions (
            ticker, direction, target_position, confidence, reason,
            defer_code, source, status, created_at, updated_at, round_id,
            intent_id
        ) VALUES (
            '520500', 'long', 0.10, 0.7, 'replay me', 'market_closed',
            'architecture_test', 'replaying', ?, ?, ?, ?
        )
        """,
        (
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            round_state.id,
            intent_id,
        ),
    )
    pending_id = int(cursor.lastrowid)
    await conn.commit()

    result = await simulation.execute_intent(
        intent_id,
        pending_decision_id=pending_id,
    )
    replay = await simulation.execute_intent(
        intent_id,
        pending_decision_id=pending_id,
    )

    assert result["action"] == "hold"
    assert result["blocker_code"] == "realtime_quote_unavailable"
    assert replay["action"] == "rejected"
    async with conn.execute(
        "SELECT status, defer_code FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            "rejected",
            "realtime_quote_unavailable",
        )
    async with conn.execute(
        "SELECT status, current_price, quote_source FROM price_predictions "
        "WHERE id = ?",
        (prediction_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            "execution_rejected",
            None,
            None,
        )
    async with conn.execute(
        "SELECT status, defer_code FROM simulation_pending_decisions WHERE id = ?",
        (pending_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            "rejected",
            "realtime_quote_unavailable",
        )
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == "execution_rejected"
    async with conn.execute(
        """
        SELECT COUNT(*) FROM round_events
        WHERE round_id = ? AND event_type = 'ExecutionRejectedWithoutQuote'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 1
    async with conn.execute(
        "SELECT source FROM simulation_redeployment_state WHERE id = 1"
    ) as cursor:
        assert (await cursor.fetchone())[0] == (
            "atomic_execution_rejection_without_quote"
        )
    for table in (
        "quote_snapshots",
        "simulation_trades",
        "simulation_ledger_entries",
    ):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0
    await db.close()


@pytest.mark.asyncio
async def test_unquoted_rejection_failure_rolls_back_every_terminal_write(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=None),
    )
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="unquoted rejection rollback",
        research_objective="failure injection",
    )
    decision_id = "decision-unquoted-rollback"
    prediction_id = await DecisionRecorder(db.db_path).record_decision(
        ticker="520500",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        round_id=round_state.id,
        decision_id=decision_id,
        defer_quote_until_execution=True,
    )
    intent_id = await simulation.create_execution_intent(
        ticker="520500",
        direction="long",
        target_position=0.10,
        reason="force unquoted rollback",
        round_id=round_state.id,
        decision_id=decision_id,
        idempotency_key="architecture:unquoted-rejection:rollback",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TRIGGER fail_unquoted_rejection_event
        BEFORE INSERT ON round_events
        WHEN NEW.event_type = 'ExecutionRejectedWithoutQuote'
        BEGIN
            SELECT RAISE(ABORT, 'forced unquoted rejection failure');
        END
        """
    )
    await conn.commit()

    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "error"
    assert result["blocker_code"] == "execution_rejection_persistence_failed"
    async with conn.execute(
        "SELECT status, defer_code, resolution FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == ("pending", None, None)
    async with conn.execute(
        "SELECT status, current_price FROM price_predictions WHERE id = ?",
        (prediction_id,),
    ) as cursor:
        assert tuple(await cursor.fetchone()) == (
            "awaiting_entry_quote",
            None,
        )
    async with conn.execute(
        "SELECT terminal_code FROM research_rounds WHERE id = ?",
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] is None
    async with conn.execute(
        "SELECT COUNT(*) FROM round_events "
        "WHERE event_type = 'ExecutionRejectedWithoutQuote'"
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT source FROM simulation_redeployment_state WHERE id = 1"
    ) as cursor:
        assert (await cursor.fetchone())[0] != (
            "atomic_execution_rejection_without_quote"
        )
    await db.close()


@pytest.mark.asyncio
async def test_lot_cap_rejection_atomically_records_quote_blocker_and_round_event(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=32.15),
    )
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="lot feasibility",
        research_objective="auditable execution constraint",
    )
    intent_id = await simulation.create_execution_intent(
        ticker="601899",
        direction="long",
        target_position=0.10,
        confidence=0.8,
        reason="qualified but must obey static cap",
        round_id=round_state.id,
        idempotency_key="architecture:lot-cap:1",
    )

    result = await simulation.execute_intent(intent_id)
    replay = await simulation.execute_intent(intent_id)

    assert result["action"] == "hold"
    assert result["blocker_code"] == "lot_size_cap_infeasible"
    assert result["one_lot_value"] == pytest.approx(3215.0)
    assert result["max_target_value"] == pytest.approx(1000.0)
    assert result["max_executable_quote"] == pytest.approx(10.0)
    assert result["quote_snapshot_id"]
    assert replay["action"] == "rejected"

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT status, defer_code, resolution
        FROM execution_intents WHERE id = ?
        """,
        (intent_id,),
    ) as cursor:
        status, code, resolution = await cursor.fetchone()
    assert status == "rejected"
    assert code == "lot_size_cap_infeasible"
    assert "一手市值3215.00元" in resolution
    async with conn.execute(
        """
        SELECT ticker, price, provider, fetched_at, purpose
        FROM quote_snapshots WHERE intent_id = ?
        """,
        (intent_id,),
    ) as cursor:
        quote_rows = await cursor.fetchall()
    assert len(quote_rows) == 1
    assert tuple(quote_rows[0])[:3] == (
        "601899",
        32.15,
        "architecture_test_realtime",
    )
    assert quote_rows[0][3]
    assert quote_rows[0][4] == "execution_rejection"
    async with conn.execute(
        """
        SELECT event_type, payload_json
        FROM round_events
        WHERE round_id = ? AND event_type = 'ExecutionRejectedWithQuote'
        """,
        (round_state.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert len(events) == 1
    assert '"blocker_code": "lot_size_cap_infeasible"' in events[0][1]
    async with conn.execute(
        """
        SELECT blocker_code, source
        FROM simulation_redeployment_state WHERE id = 1
        """
    ) as cursor:
        blocker_code, source = await cursor.fetchone()
    assert blocker_code == "lot_size_cap_infeasible"
    assert source == "atomic_execution_rejection"
    for table in ("simulation_trades", "simulation_ledger_entries"):
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            assert (await cursor.fetchone())[0] == 0
    assert simulation.cash == pytest.approx(simulation.initial_capital)
    assert simulation.positions == {}

    status_model = await get_system_status(db)
    assert status_model["counts"]["quotes"] == 1
    assert status_model["latest_execution"]["defer_code"] == "lot_size_cap_infeasible"
    assert status_model["latest_execution"]["quote_price"] == pytest.approx(32.15)
    await db.close()


@pytest.mark.asyncio
async def test_lot_cap_rejection_write_failure_rolls_back_quote_intent_and_event(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=32.15),
    )
    round_state = await ResearchRoundCoordinator(db).start(
        base_topic="lot rejection rollback",
        research_objective="failure injection",
    )
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TRIGGER fail_rejection_quote
        BEFORE INSERT ON quote_snapshots
        WHEN NEW.purpose = 'execution_rejection'
        BEGIN
            SELECT RAISE(ABORT, 'forced rejection quote failure');
        END
        """
    )
    await conn.commit()
    intent_id = await simulation.create_execution_intent(
        ticker="601899",
        direction="long",
        target_position=0.10,
        reason="force rejection rollback",
        round_id=round_state.id,
        idempotency_key="architecture:lot-cap:rollback",
    )

    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "error"
    assert result["blocker_code"] == "execution_rejection_persistence_failed"
    assert "回滚" in result["reason"]
    async with conn.execute(
        "SELECT status, defer_code, resolution FROM execution_intents WHERE id = ?",
        (intent_id,),
    ) as cursor:
        intent_row = await cursor.fetchone()
    assert tuple(intent_row) == ("pending", None, None)
    async with conn.execute(
        "SELECT COUNT(*) FROM quote_snapshots WHERE intent_id = ?",
        (intent_id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        """
        SELECT COUNT(*) FROM round_events
        WHERE round_id = ? AND event_type = 'ExecutionRejectedWithQuote'
        """,
        (round_state.id,),
    ) as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute(
        "SELECT blocker_code FROM simulation_redeployment_state WHERE id = 1"
    ) as cursor:
        assert (await cursor.fetchone())[0] != "lot_size_cap_infeasible"
    assert simulation.cash == pytest.approx(simulation.initial_capital)
    assert simulation.positions == {}
    await db.close()


@pytest.mark.asyncio
async def test_redeployment_prompt_exposes_current_one_lot_execution_ceiling(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())

    # Explicit test scenario; do not inherit the user's current strategy cap.
    monkeypatch.setattr(simulation, "config", {**simulation.config, "max_single_position": 0.10})

    context = await simulation.format_redeployment_learning_context()

    assert "当前整手可执行边界" in context
    assert "新建仓一手要求交易时实时价<=10.0000元" in context
    assert "不得放宽静态单标的上限" in context
    await db.close()


@pytest.mark.asyncio
async def test_proposal_screen_removes_unbuyable_long_before_committee(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=32.15),
    )

    # Explicit test scenario; do not inherit the user's current strategy cap.
    monkeypatch.setattr(simulation, "config", {**simulation.config, "max_single_position": 0.10})

    feasible, rejected = await simulation.screen_proposal_lot_feasibility([
        {
            "ticker": "601899",
            "direction": "long",
            "confidence": 0.8,
            "target_position": 0.1,
        }
    ])

    assert feasible == []
    assert rejected[0]["code"] == "proposal_lot_infeasible"
    assert rejected[0]["reference_price"] == pytest.approx(32.15)
    assert rejected[0]["quote_source"] == "architecture_test_realtime"
    assert rejected[0]["quote_fetched_at"]
    assert rejected[0]["quote_purpose"] == "proposal_lot_feasibility_screening"
    assert rejected[0]["max_executable_quote"] == pytest.approx(10.0)
    await db.close()


@pytest.mark.asyncio
async def test_proposal_screen_keeps_executable_etf_candidate(
    tmp_path,
    monkeypatch,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=4.25),
    )
    proposal = {
        "ticker": "510300",
        "direction": "long",
        "confidence": 0.8,
        "target_position": 0.1,
    }

    feasible, rejected = await simulation.screen_proposal_lot_feasibility(
        [proposal]
    )

    assert feasible == [proposal]
    assert rejected == []
    await db.close()


@pytest.mark.asyncio
async def test_proposal_screen_cannot_reject_from_stale_quote(
    tmp_path,
    monkeypatch,
):
    stale_at = (datetime.now() - timedelta(minutes=10)).isoformat()
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=32.15, fetched_at=stale_at),
    )
    proposal = {
        "ticker": "601899",
        "direction": "long",
        "confidence": 0.8,
        "target_position": 0.1,
    }

    feasible, rejected = await simulation.screen_proposal_lot_feasibility(
        [proposal]
    )

    assert feasible == [proposal]
    assert rejected == []
    await db.close()


class SessionPauseMarket(ClosedMarket):
    async def get_current_quote(self, ticker):
        return None

    async def get_valuation_quote(self, ticker):
        if self.price is None:
            return None
        return {
            "ticker": ticker,
            "price": self.price,
            "name": "中国人寿",
            "source": "architecture_test_session_pause",
            "fetched_at": datetime.now().isoformat(),
            "quoted_at": datetime.now().replace(hour=11, minute=30).isoformat(),
            "valuation_only": True,
            "valuation_kind": "session_pause",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("price", "expected_code"),
    [(4.25, None), (32.15, "proposal_lot_infeasible"), (None, "proposal_ticker_unverifiable")],
)
async def test_proposal_screen_uses_session_reference_when_market_paused(
    tmp_path,
    monkeypatch,
    price,
    expected_code,
):
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        SessionPauseMarket(price=price),
    )
    monkeypatch.setattr(simulation, "config", {**simulation.config, "max_single_position": 0.10})
    proposal = {
        "ticker": "601628",
        "security_name": "中国人寿",
        "direction": "long",
        "confidence": 0.8,
        "target_position": 0.1,
    }

    feasible, rejected = await simulation.screen_proposal_lot_feasibility([proposal])

    if expected_code is None:
        assert feasible == [proposal]
        assert rejected == []
    else:
        assert feasible == []
        assert rejected[0]["code"] == expected_code
    await db.close()


@pytest.mark.asyncio
async def test_proposal_lot_rejection_quote_is_round_traceable(
    tmp_path,
    monkeypatch,
):
    from sovereign_hall.run_discussion import record_proposal_lot_screening_event

    db, _simulation = await build_simulation(tmp_path, monkeypatch, OpenMarket())
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="lot screening lineage",
        research_objective="persist fresh proposal screening quote",
    )

    await record_proposal_lot_screening_event(
        coordinator,
        round_state.id,
        [{
            "ticker": "601899",
            "code": "proposal_lot_infeasible",
            "reference_price": 32.15,
            "quote_source": "test_realtime",
            "quote_fetched_at": "2026-08-04T14:30:00",
            "quote_purpose": "proposal_lot_feasibility_screening",
            "max_executable_quote": 10.0,
        }],
    )

    async with db._connection.execute(
        """
        SELECT payload_json FROM round_events
        WHERE round_id = ? AND event_type = 'ProposalLotFeasibilityRejected'
        """,
        (round_state.id,),
    ) as cursor:
        row = await cursor.fetchone()
    payload = json.loads(row[0])
    quote = payload["rejections"][0]
    assert quote == {
        "code": "proposal_lot_infeasible",
        "fetched_at": "2026-08-04T14:30:00",
        "max_executable_quote": 10.0,
        "price": 32.15,
        "provider": "test_realtime",
        "purpose": "proposal_lot_feasibility_screening",
        "ticker": "601899",
    }
    await db.close()


@pytest.mark.asyncio
async def test_stale_realtime_quote_is_na_and_cannot_create_fill(
    tmp_path,
    monkeypatch,
):
    stale_at = (datetime.now() - timedelta(minutes=10)).isoformat()
    db, simulation = await build_simulation(
        tmp_path,
        monkeypatch,
        OpenMarket(price=10.0, fetched_at=stale_at),
    )
    intent_id = await simulation.create_execution_intent(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="stale quote must fail",
        idempotency_key="architecture:stale:1",
    )
    result = await simulation.execute_intent(intent_id)

    assert result["action"] == "hold"
    conn = await db._get_connection()
    async with conn.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
        assert (await cursor.fetchone())[0] == 0
    async with conn.execute("SELECT COUNT(*) FROM quote_snapshots") as cursor:
        assert (await cursor.fetchone())[0] == 0
    await db.close()


def test_live_execution_policy_is_pure_and_full_deployment_cap_is_not_offline_cash():
    capped, reasons = cap_target_position(
        0.20,
        current_position=0.05,
        current_gross_exposure=0.90,
        limits=ExecutionPolicyLimits(
            max_single_position=0.10,
            max_gross=1.0,
        ),
    )
    assert capped == pytest.approx(0.10)
    assert reasons == ("max_single_position",)


def test_config_file_load_is_idempotent_and_lists_override(tmp_path):
    from sovereign_hall.core.config import get_config

    config = get_config()
    original_config = copy.deepcopy(config._config)
    original_files = set(config._loaded_files)
    custom = tmp_path / "config.yaml"
    custom.write_text(
        """
analysts:
  enabled_roles:
    - risk_officer
    - quant_researcher
""".strip(),
        encoding="utf-8",
    )
    try:
        assert config.load_from_file(custom)
        assert config.load_from_file(custom)
        assert config.get_analyst_roles() == [
            "risk_officer",
            "quant_researcher",
        ]
    finally:
        config._config = original_config
        config._loaded_files = original_files
    assert config.validate_runtime_invariants() == []


def test_production_entry_and_execution_modules_do_not_import_offline_scripts():
    project_root = Path(__file__).resolve().parents[1]
    for relative in (
        "main.py",
        "run_discussion.py",
        "services/investment_simulation.py",
        "application/execute_simulation_cycle.py",
    ):
        tree = ast.parse((project_root / relative).read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(name.startswith("scripts") for name in imports), relative
    main_source = (project_root / "main.py").read_text(encoding="utf-8")
    assert "core.sovereign_hall" not in main_source


def test_live_cycle_artifacts_do_not_emit_best_metrics_alias():
    project_root = Path(__file__).resolve().parents[1]
    for relative in (
        "scripts/run_heuristic_cycle.py",
        "scripts/run_heuristic_cycle_stdlib.py",
    ):
        source = (project_root / relative).read_text(encoding="utf-8")
        assert 'write_json(run_dir / "best_metrics.json"' not in source


def test_heuristic_cycle_cli_uses_canonical_package_imports():
    project_root = Path(__file__).resolve().parents[1]
    tree = ast.parse(
        (project_root / "scripts/run_heuristic_cycle.py").read_text(
            encoding="utf-8"
        )
    )
    imported_modules = [
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    ]
    assert "services.heuristic_policy" not in imported_modules
    assert "sovereign_hall.services.heuristic_policy" in imported_modules


def test_production_round_failure_handlers_use_canonical_terminal_code():
    project_root = Path(__file__).resolve().parents[1]
    source = (project_root / "run_discussion.py").read_text(encoding="utf-8")
    assert source.count('code="failed"') >= 2
    assert "interrupt_for_shutdown" in source
    assert 'code="interrupted"' not in source
    assert 'code="cancelled"' not in source
    assert 'code="pipeline_exception"' not in source
