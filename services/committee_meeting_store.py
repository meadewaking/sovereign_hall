"""Durable task/session protocol for investment-committee meetings."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime
from typing import Any, Mapping

from ..domain.common.ids import new_id


COMMITTEE_PROMPT_VERSION = "committee_roundtable_v1"
TERMINAL_TASK_STATUSES = frozenset({"completed", "timeout", "error"})


def committee_content_hash(value: Any) -> str:
    """Return a stable hash for prompt, proposal, and output lineage."""
    if isinstance(value, str):
        payload = value
    else:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class CommitteeMeetingStore:
    """Persist committee meetings without owning any LLM/network work.

    Writes are serialized because every task shares the canonical aiosqlite
    connection. Slow model calls happen outside the lock and outside database
    transactions.
    """

    def __init__(self, db_service):
        self.db_service = db_service
        self._write_lock = asyncio.Lock()

    async def start_or_resume(
        self,
        *,
        round_id: str,
        proposal: Mapping[str, Any],
        review_depth: str,
        prompt_version: str = COMMITTEE_PROMPT_VERSION,
    ) -> str:
        await self.db_service._ensure_initialized()
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        ticker = str(proposal.get("ticker") or "").strip().upper()
        proposal_key = proposal_id or committee_content_hash(
            {
                "ticker": ticker,
                "direction": proposal.get("direction"),
                "thesis": proposal.get("thesis"),
                "evidence": proposal.get("evidence") or [],
            }
        )
        input_hash = committee_content_hash(proposal)
        now = datetime.now().isoformat()
        async with self._write_lock:
            async with self.db_service.transaction(immediate=True) as conn:
                async with conn.execute(
                    """
                    SELECT id
                    FROM committee_meetings
                    WHERE round_id = ? AND proposal_key = ?
                      AND prompt_version = ? AND input_hash = ?
                    LIMIT 1
                    """,
                    (round_id, proposal_key, prompt_version, input_hash),
                ) as cursor:
                    row = await cursor.fetchone()
                if row:
                    meeting_id = str(row[0])
                    await conn.execute(
                        """
                        UPDATE committee_meetings
                        SET status = 'running', updated_at = ?, completed_at = NULL
                        WHERE id = ?
                        """,
                        (now, meeting_id),
                    )
                    return meeting_id

                meeting_id = new_id("committee_meeting")
                await conn.execute(
                    """
                    INSERT INTO committee_meetings (
                        id, round_id, proposal_key, proposal_id, ticker,
                        status, current_stage, review_depth, prompt_version,
                        input_hash, started_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'running', 'created', ?, ?, ?, ?, ?)
                    """,
                    (
                        meeting_id,
                        round_id,
                        proposal_key,
                        proposal_id or None,
                        ticker,
                        str(review_depth or ""),
                        str(prompt_version),
                        input_hash,
                        now,
                        now,
                    ),
                )
                return meeting_id

    async def load_task(
        self,
        *,
        meeting_id: str,
        stage: str,
        seat_label: str,
        input_hash: str,
        attempt: int = 0,
    ) -> dict[str, Any] | None:
        conn = await self.db_service._get_connection()
        async with conn.execute(
            """
            SELECT task.id, task.session_id, task.status, task.seat_role,
                   task.model, task.provider, task.error,
                   task.elapsed_seconds, task.token_usage_json,
                   turn.raw_output, turn.parsed_output_json, turn.parse_mode, turn.id
            FROM committee_tasks AS task
            JOIN committee_turns AS turn ON turn.task_id = task.id
            WHERE task.meeting_id = ? AND task.stage = ?
              AND task.seat_label = ? AND task.input_hash = ?
              AND task.attempt >= ? AND task.status = 'completed'
            ORDER BY task.attempt DESC
            LIMIT 1
            """,
            (meeting_id, stage, seat_label, input_hash, int(attempt)),
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        from .storage_archive import hydrate_payload
        restored = await hydrate_payload(conn, "committee_turn",
                                         {"id": row[12], "raw_output": row[9]}, "raw_output")
        parsed_output: Any = {}
        token_usage: Any = {}
        try:
            parsed_output = json.loads(str(row[10] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_output = {}
        if str(row[11] or "") in {"schema_invalid", "parser_error"} or (
            isinstance(parsed_output, dict)
            and parsed_output.get("schema_valid") is False
        ):
            # Completed transport is not a reusable final-vote checkpoint.
            # Preserve the old turn and let begin_task append a new attempt.
            return None
        try:
            token_usage = json.loads(str(row[8] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            token_usage = {}
        return {
            "task_id": str(row[0]),
            "session_id": str(row[1]),
            "status": str(row[2]),
            "seat_role": str(row[3]),
            "model": str(row[4] or ""),
            "provider": str(row[5] or ""),
            "error": str(row[6] or ""),
            "elapsed_seconds": float(row[7] or 0.0),
            "token_usage": token_usage,
            "raw_output": str(restored["raw_output"] or ""),
            "parsed_output": parsed_output,
            "parse_mode": str(row[11] or ""),
        }

    async def begin_task(
        self,
        *,
        meeting_id: str,
        stage: str,
        seat_role: str,
        seat_label: str,
        session_id: str,
        input_hash: str,
        prompt_version: str,
        model: str = "",
        provider: str = "",
        attempt: int = 0,
    ) -> str:
        now = datetime.now().isoformat()
        task_id = new_id("committee_task")
        async with self._write_lock:
            async with self.db_service.transaction(immediate=True) as conn:
                async with conn.execute(
                    """
                    SELECT MAX(attempt)
                    FROM committee_tasks
                    WHERE meeting_id = ? AND stage = ? AND seat_label = ?
                      AND input_hash = ?
                    """,
                    (meeting_id, stage, seat_label, input_hash),
                ) as cursor:
                    prior = await cursor.fetchone()
                prior_attempt = prior[0] if prior else None
                effective_attempt = int(attempt)
                if prior_attempt is not None:
                    effective_attempt = max(
                        effective_attempt,
                        int(prior_attempt) + 1,
                    )
                await conn.execute(
                    """
                    INSERT INTO committee_tasks (
                        id, meeting_id, stage, seat_role, seat_label,
                        session_id, status, attempt, input_hash,
                        prompt_version, model, provider, started_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        meeting_id,
                        stage,
                        str(seat_role or seat_label),
                        seat_label,
                        session_id,
                        effective_attempt,
                        input_hash,
                        prompt_version,
                        str(model or ""),
                        str(provider or ""),
                        now,
                        now,
                    ),
                )
                async with conn.execute(
                    """
                    SELECT id FROM committee_tasks
                    WHERE meeting_id = ? AND stage = ? AND seat_label = ?
                      AND input_hash = ? AND attempt = ?
                    """,
                    (
                        meeting_id,
                        stage,
                        seat_label,
                        input_hash,
                        effective_attempt,
                    ),
                ) as cursor:
                    row = await cursor.fetchone()
                if not row:
                    raise RuntimeError("committee task insert did not return a row")
                await conn.execute(
                    """
                    UPDATE committee_meetings
                    SET status = 'running', current_stage = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (stage, now, meeting_id),
                )
                return str(row[0])

    async def finish_task(
        self,
        *,
        task_id: str,
        meeting_id: str,
        stage: str,
        seat_label: str,
        status: str,
        raw_output: str,
        parsed_output: Any = None,
        parse_mode: str = "",
        error: str = "",
        elapsed_seconds: float = 0.0,
        token_usage: Mapping[str, Any] | None = None,
    ) -> None:
        if status not in TERMINAL_TASK_STATUSES:
            raise ValueError(f"invalid terminal committee task status: {status}")
        now = datetime.now().isoformat()
        raw = str(raw_output or "")
        parsed_json = json.dumps(
            parsed_output if parsed_output is not None else {},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        usage_json = json.dumps(
            dict(token_usage or {}),
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        async with self._write_lock:
            async with self.db_service.transaction(immediate=True) as conn:
                await conn.execute(
                    """
                    UPDATE committee_tasks
                    SET status = ?, token_usage_json = ?, error = ?,
                        elapsed_seconds = ?, updated_at = ?, completed_at = ?
                    WHERE id = ? AND meeting_id = ?
                    """,
                    (
                        status,
                        usage_json,
                        str(error or "") or None,
                        float(elapsed_seconds),
                        now,
                        now,
                        task_id,
                        meeting_id,
                    ),
                )
                await conn.execute(
                    """
                    INSERT INTO committee_turns (
                        meeting_id, task_id, stage, seat_label, raw_output,
                        parsed_output_json, parse_mode, output_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        raw_output = excluded.raw_output,
                        parsed_output_json = excluded.parsed_output_json,
                        parse_mode = excluded.parse_mode,
                        output_hash = excluded.output_hash,
                        created_at = excluded.created_at
                    """,
                    (
                        meeting_id,
                        task_id,
                        stage,
                        seat_label,
                        raw,
                        parsed_json,
                        str(parse_mode or "") or None,
                        committee_content_hash(raw),
                        now,
                    ),
                )
                await conn.execute(
                    """
                    UPDATE committee_meetings
                    SET current_stage = ?, updated_at = ? WHERE id = ?
                    """,
                    (stage, now, meeting_id),
                )

    async def finish_meeting(
        self,
        *,
        meeting_id: str,
        status: str,
        decision_id: str | None,
        outcome_code: str,
    ) -> None:
        if status not in {"completed", "incomplete", "failed", "cancelled"}:
            raise ValueError(f"invalid committee meeting status: {status}")
        now = datetime.now().isoformat()
        async with self._write_lock:
            async with self.db_service.transaction(immediate=True) as conn:
                await conn.execute(
                    """
                    UPDATE committee_meetings
                    SET status = ?, current_stage = ?, decision_id = ?,
                        outcome_code = ?, updated_at = ?, completed_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        status,
                        decision_id,
                        str(outcome_code or ""),
                        now,
                        now,
                        meeting_id,
                    ),
                )
