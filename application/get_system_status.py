"""Canonical read model shared by user-facing entries."""

from __future__ import annotations

import json
from typing import Any

from .run_research_round import ResearchRoundCoordinator


async def get_system_status(db_service) -> dict[str, Any]:
    """Return the latest durable research-to-simulation pipeline state."""
    latest = await ResearchRoundCoordinator(db_service).latest_status()
    if not latest:
        return {
            "latest_round": None,
            "counts": {},
            "pipeline_health": "no_canonical_rounds",
        }

    conn = await db_service._get_connection()
    round_id = str(latest["id"])
    queries = {
        "documents": (
            "SELECT COUNT(*) FROM round_documents "
            "WHERE round_id = ? AND usage = 'source'",
            round_id,
        ),
        "proposals": (
            "SELECT COUNT(*) FROM proposals WHERE round_id = ?",
            round_id,
        ),
        "meetings": (
            "SELECT COUNT(*) FROM meetings WHERE round_id = ?",
            round_id,
        ),
        "predictions": (
            "SELECT COUNT(*) FROM price_predictions WHERE round_id = ?",
            round_id,
        ),
        "intents": (
            "SELECT COUNT(*) FROM execution_intents WHERE round_id = ?",
            round_id,
        ),
        "quotes": (
            "SELECT COUNT(*) FROM quote_snapshots WHERE round_id = ?",
            round_id,
        ),
        "fills": (
            "SELECT COUNT(*) FROM simulation_trades WHERE round_id = ?",
            round_id,
        ),
    }
    counts: dict[str, int] = {}
    for name, (sql, value) in queries.items():
        try:
            async with conn.execute(sql, (value,)) as cursor:
                row = await cursor.fetchone()
            counts[name] = int(row[0] if row else 0)
        except Exception:
            # A partially migrated legacy database is itself visible through a
            # zero count; migration runs before production entries use this read model.
            counts[name] = 0

    source_lineage = None
    try:
        async with conn.execute(
            """
            SELECT payload_json
            FROM round_events
            WHERE round_id = ? AND event_type = 'SourcesPersisted'
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            payload = json.loads(str(row[0] or "{}"))
            claimed = int(payload.get("source_count") or 0)
            linked = int(counts.get("documents", 0))
            source_lineage = {
                "claimed_source_count": claimed,
                "linked_source_count": linked,
                "presented_document_count": payload.get(
                    "presented_document_count"
                ),
                "traceable_document_count": payload.get(
                    "traceable_document_count"
                ),
                "untraceable_document_count": payload.get(
                    "untraceable_document_count"
                ),
                "resolved_document_count": payload.get(
                    "resolved_document_count"
                ),
                "capacity_excluded_document_count": payload.get(
                    "capacity_excluded_document_count", 0
                ),
                "resolved_source_count_before_limit": payload.get(
                    "resolved_source_count_before_limit"
                ),
                "lineage_limit": payload.get("lineage_limit"),
                "consistent": claimed == linked,
            }
    except Exception:
        source_lineage = None

    evidence_time_context = None
    async with conn.execute(
        "SELECT payload_json, created_at FROM round_events "
        "WHERE round_id=? AND event_type='EvidenceTimeContextPrepared' "
        "ORDER BY sequence DESC LIMIT 1", (round_id,),
    ) as cursor:
        time_row = await cursor.fetchone()
    if time_row:
        time_payload = json.loads(time_row[0])
        time_docs = time_payload.get("documents") or []
        evidence_time_context = {
            "contract": time_payload.get("contract"),
            "research_as_of": time_payload.get("research_as_of"),
            "recorded_at": time_row[1],
            "context_chars_used": time_payload.get("context_chars_used"),
            "context_chars_limit": time_payload.get("context_chars_limit"),
            "context_sha256": time_payload.get("context_sha256"),
            "excluded_counts": time_payload.get("excluded_counts"),
            "excerpt_document_count": len(time_docs),
            "publication_time_known_count": sum(
                bool(doc.get("publication_time_known")) for doc in time_docs
            ),
            "publication_time_unknown_count": sum(
                not bool(doc.get("publication_time_known")) for doc in time_docs
            ),
            "retrieval_is_publication": False,
        }

    search_query_gate = None
    try:
        async with conn.execute(
            """
            SELECT payload_json, created_at
            FROM round_events
            WHERE round_id = ? AND event_type = 'SearchQueriesValidated'
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            payload = json.loads(str(row[0] or "{}"))
            generator_rejections = dict(
                payload.get("generator_rejection_counts") or {}
            )
            provider_rejections = dict(
                payload.get("provider_rejection_counts") or {}
            )
            combined_rejections: dict[str, int] = {}
            for source_counts in (generator_rejections, provider_rejections):
                for code, count in source_counts.items():
                    combined_rejections[str(code)] = (
                        combined_rejections.get(str(code), 0) + int(count or 0)
                    )
            search_query_gate = {
                **payload,
                "rejection_counts": combined_rejections,
                "rejected_count": sum(combined_rejections.values()),
                "recorded_at": row[1],
            }
    except Exception:
        search_query_gate = None

    latest_execution = None
    try:
        async with conn.execute(
            """
            SELECT ei.id AS intent_id, ei.ticker, ei.direction, ei.status,
                   ei.defer_code, ei.resolution, ei.updated_at,
                   qs.id AS quote_snapshot_id, qs.price AS quote_price,
                   qs.provider AS quote_provider, qs.fetched_at AS quote_fetched_at,
                   qs.purpose AS quote_purpose
            FROM execution_intents ei
            LEFT JOIN quote_snapshots qs
              ON qs.id = (
                  SELECT newest.id
                  FROM quote_snapshots newest
                  WHERE newest.intent_id = ei.id
                  ORDER BY newest.rowid DESC
                  LIMIT 1
              )
            WHERE ei.round_id = ?
            ORDER BY datetime(ei.created_at) DESC, ei.rowid DESC
            LIMIT 1
            """,
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        latest_execution = dict(row) if row else None
    except Exception:
        latest_execution = None

    execution_intent_terminal_guard = None
    try:
        async with conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN (
                  'execution_intents_executed_defer_guard_insert',
                  'execution_intents_executed_defer_guard_update'
              )
            """
        ) as cursor:
            guard_names = {str(row[0]) for row in await cursor.fetchall()}
        async with conn.execute(
            """
            SELECT COUNT(*)
            FROM execution_intents
            WHERE status = 'executed' AND defer_code IS NOT NULL
            """
        ) as cursor:
            violation_row = await cursor.fetchone()
        execution_intent_terminal_guard = {
            "active": guard_names
            == {
                "execution_intents_executed_defer_guard_insert",
                "execution_intents_executed_defer_guard_update",
            },
            "trigger_count": len(guard_names),
            "violation_count": int(violation_row[0] if violation_row else 0),
        }
    except Exception:
        execution_intent_terminal_guard = {
            "active": False,
            "trigger_count": 0,
            "violation_count": None,
        }

    pending_decision_terminal_guard = None
    try:
        async with conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN (
                  'simulation_pending_executed_defer_guard_insert',
                  'simulation_pending_executed_defer_guard_update'
              )
            """
        ) as cursor:
            pending_guard_names = {
                str(row[0]) for row in await cursor.fetchall()
            }
        async with conn.execute(
            """
            SELECT COUNT(*)
            FROM simulation_pending_decisions
            WHERE status = 'executed' AND defer_code <> ''
            """
        ) as cursor:
            pending_violation_row = await cursor.fetchone()
        pending_decision_terminal_guard = {
            "active": pending_guard_names
            == {
                "simulation_pending_executed_defer_guard_insert",
                "simulation_pending_executed_defer_guard_update",
            },
            "trigger_count": len(pending_guard_names),
            "violation_count": int(
                pending_violation_row[0] if pending_violation_row else 0
            ),
        }
    except Exception:
        pending_decision_terminal_guard = {
            "active": False,
            "trigger_count": 0,
            "violation_count": None,
        }

    pending_exit_expiry_guard = None
    try:
        async with conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN (
                  'simulation_pending_exit_expiry_guard_insert',
                  'simulation_pending_exit_expiry_guard_update'
              )
            """
        ) as cursor:
            exit_guard_names = {
                str(row[0]) for row in await cursor.fetchall()
            }
        async with conn.execute(
            """
            SELECT COUNT(*)
            FROM simulation_pending_decisions
            WHERE status IN ('pending_next_trading_session', 'replaying')
              AND direction IN ('sell', 'short')
              AND intent_id IS NOT NULL
              AND expires_at IS NOT NULL
            """
        ) as cursor:
            exit_expiry_violation_row = await cursor.fetchone()
        pending_exit_expiry_guard = {
            "active": exit_guard_names
            == {
                "simulation_pending_exit_expiry_guard_insert",
                "simulation_pending_exit_expiry_guard_update",
            },
            "trigger_count": len(exit_guard_names),
            "violation_count": int(
                exit_expiry_violation_row[0]
                if exit_expiry_violation_row
                else 0
            ),
        }
    except Exception:
        pending_exit_expiry_guard = {
            "active": False,
            "trigger_count": 0,
            "violation_count": None,
        }

    recent_failure_recurrence = None
    try:
        async with conn.execute(
            """
            SELECT terminal_reason, COUNT(*) AS failure_count,
                   MAX(updated_at) AS last_seen
            FROM (
                SELECT status, terminal_reason, updated_at
                FROM research_rounds
                ORDER BY datetime(started_at) DESC, rowid DESC
                LIMIT 100
            )
            WHERE status = 'failed'
            GROUP BY terminal_reason
            ORDER BY failure_count DESC, last_seen DESC
            LIMIT 1
            """
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            recent_failure_recurrence = {
                "reason": str(row[0] or "unknown"),
                "count_in_recent_rounds": int(row[1] or 0),
                "last_seen": row[2],
                "sample_size": 100,
            }
    except Exception:
        recent_failure_recurrence = None

    finalization_interruption = None
    try:
        async with conn.execute(
            """
            SELECT payload_json, created_at
            FROM round_events
            WHERE round_id = ?
              AND event_type = 'RoundFinalizationInterrupted'
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            finalization_interruption = {
                **json.loads(str(row[0] or "{}")),
                "recorded_at": row[1],
            }
    except Exception:
        finalization_interruption = None

    status = str(latest.get("status") or "unknown")
    terminal_code = str(latest.get("terminal_code") or "")
    if status == "failed" or terminal_code in {"failed", "pipeline_exception"}:
        health = "failed"
    elif terminal_code == "no_evidence":
        health = "completed_no_evidence"
    elif terminal_code == "execution_rejected":
        health = "completed_execution_rejected"
    elif terminal_code == "market_closed_pending":
        health = "completed_pending_market"
    elif terminal_code == "committee_hold":
        health = "completed_committee_hold"
    elif terminal_code == "committee_incomplete":
        health = "completed_committee_incomplete"
    elif terminal_code == "filled":
        health = "completed_filled"
    elif status != "completed":
        health = "incomplete"
    else:
        health = "completed"
    return {
        "latest_round": latest,
        "counts": counts,
        "latest_execution": latest_execution,
        "execution_intent_terminal_guard": execution_intent_terminal_guard,
        "pending_decision_terminal_guard": pending_decision_terminal_guard,
        "pending_exit_expiry_guard": pending_exit_expiry_guard,
        "source_lineage": source_lineage,
        "evidence_time_context": evidence_time_context,
        "search_query_gate": search_query_gate,
        "recent_failure_recurrence": recent_failure_recurrence,
        "finalization_interruption": finalization_interruption,
        "pipeline_health": health,
        # A terminal is already a durable business outcome even while the
        # canonical loop is persisting reflection and its final completed
        # stage.  Expose that short post-terminal window without falsely
        # warning that the round has no terminal.
        "round_finalization_pending": bool(
            terminal_code
            and status not in {"completed", "failed"}
            and not finalization_interruption
        ),
    }


def format_system_status(status: dict[str, Any]) -> str:
    """Format the canonical round read model for prompts and terminals."""
    latest = status.get("latest_round")
    if not latest:
        return "主循环记忆: 尚无 canonical research_round 记录。"
    counts = status.get("counts") or {}
    base = (
        "主循环记忆: "
        f"round={latest.get('id')} | status={latest.get('status')} | "
        f"terminal={latest.get('terminal_code') or 'N/A'} | "
        f"health={status.get('pipeline_health')} | "
        f"documents={counts.get('documents', 0)}, "
        f"proposals={counts.get('proposals', 0)}, "
        f"meetings={counts.get('meetings', 0)}, "
        f"predictions={counts.get('predictions', 0)}, "
        f"intents={counts.get('intents', 0)}, quotes={counts.get('quotes', 0)}, "
        f"fills={counts.get('fills', 0)} | "
        f"reason={latest.get('terminal_reason') or 'N/A'}"
    )
    lineage = status.get("source_lineage") or {}
    if lineage:
        presented = lineage.get("presented_document_count")
        traceable = lineage.get("traceable_document_count")
        lineage_text = (
            f" | source_lineage={lineage.get('linked_source_count', 0)}/"
            f"{lineage.get('claimed_source_count', 0)} linked"
        )
        if presented is not None and traceable is not None:
            lineage_text += f", presented={traceable}/{presented} traceable"
        capacity_excluded = int(
            lineage.get("capacity_excluded_document_count") or 0
        )
        if capacity_excluded:
            lineage_text += (
                f", capacity_excluded={capacity_excluded}, "
                f"resolved_sources={lineage.get('resolved_source_count_before_limit') or 'N/A'}"
                f"/limit={lineage.get('lineage_limit') or 'N/A'}"
            )
        lineage_text += (
            ", consistent" if lineage.get("consistent") else ", INCONSISTENT"
        )
        base += lineage_text
    query_gate = status.get("search_query_gate") or {}
    if query_gate:
        rejection_counts = dict(query_gate.get("rejection_counts") or {})
        rejection_text = ",".join(
            f"{code}:{count}"
            for code, count in sorted(rejection_counts.items())
        ) or "none"
        base += (
            " | search_queries="
            f"submitted={int(query_gate.get('submitted_query_count') or 0)}, "
            f"provider_accepted={int(query_gate.get('provider_accepted_count') or 0)}, "
            f"dropped={int(query_gate.get('rejected_count') or 0)}, "
            f"codes={rejection_text}, "
            f"availability={query_gate.get('availability_status') or 'legacy_unknown'}, "
            f"result_docs={int(query_gate.get('result_document_count') or 0)}"
        )
        provider_health = dict(query_gate.get("provider_health") or {})
        open_sources = list(provider_health.get("circuit_open_sources") or [])
        skipped_counts = dict(
            provider_health.get("skipped_open_circuit_counts") or {}
        )
        if provider_health:
            base += (
                " | search_providers="
                f"open={','.join(open_sources) if open_sources else 'none'}, "
                f"circuit_skips={sum(int(v or 0) for v in skipped_counts.values())}"
            )
            if "cancelled_counts" in provider_health:
                cancelled = dict(provider_health.get("cancelled_counts") or {})
                base += f", cancelled={sum(int(v or 0) for v in cancelled.values())}"
    recurrence = status.get("recent_failure_recurrence") or {}
    if recurrence:
        base += (
            " | recent_failure_recurrence="
            f"{recurrence.get('count_in_recent_rounds', 0)}x/100rounds "
            f"{str(recurrence.get('reason') or 'unknown')[:180]}"
        )
    finalization_interruption = status.get("finalization_interruption") or {}
    if finalization_interruption:
        base += (
            " | finalization_interrupted="
            f"{finalization_interruption.get('shutdown_signal') or 'unknown'}, "
            "business_terminal_preserved="
            f"{finalization_interruption.get('business_terminal_code') or 'N/A'}"
        )
    intent_guard = status.get("execution_intent_terminal_guard") or {}
    if intent_guard:
        violations = intent_guard.get("violation_count")
        base += (
            " | intent_terminal_guard="
            f"{'active' if intent_guard.get('active') else 'INACTIVE'}, "
            f"violations={violations if violations is not None else 'N/A'}"
        )
    pending_guard = status.get("pending_decision_terminal_guard") or {}
    if pending_guard:
        violations = pending_guard.get("violation_count")
        base += (
            " | pending_terminal_guard="
            f"{'active' if pending_guard.get('active') else 'INACTIVE'}, "
            f"violations={violations if violations is not None else 'N/A'}"
        )
    exit_expiry_guard = status.get("pending_exit_expiry_guard") or {}
    if exit_expiry_guard:
        violations = exit_expiry_guard.get("violation_count")
        base += (
            " | exit_pending_expiry_guard="
            f"{'active' if exit_expiry_guard.get('active') else 'INACTIVE'}, "
            f"violations={violations if violations is not None else 'N/A'}"
        )
    execution = status.get("latest_execution") or {}
    if not execution:
        return base
    quote_text = "quote=N/A"
    if execution.get("quote_price") is not None:
        quote_text = (
            f"quote={float(execution['quote_price']):.4f}"
            f"@{execution.get('quote_provider') or 'unknown'}"
            f" {execution.get('quote_fetched_at') or 'unknown'}"
        )
    return (
        base
        + " | latest_execution="
        + f"{execution.get('ticker')} {execution.get('direction')} "
        + f"{execution.get('status')} code={execution.get('defer_code') or 'N/A'} "
        + quote_text
    )
