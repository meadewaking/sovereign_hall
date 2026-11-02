"""Canonical research-round persistence and state transitions."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping, Optional

from ..core.config import get_config
from ..domain.research import ResearchRound, ResearchRoundStatus


class ResearchRoundCoordinator:
    """Own durable round state without owning slow LLM or network operations."""

    def __init__(self, db_service):
        self.db_service = db_service

    async def start(
        self,
        *,
        base_topic: str,
        research_objective: str,
        policy_snapshot_id: Optional[str] = None,
        prompt_version: Optional[str] = None,
        input_hash: Optional[str] = None,
    ) -> ResearchRound:
        round_state = ResearchRound(
            base_topic=base_topic,
            research_objective=research_objective,
            policy_snapshot_id=policy_snapshot_id,
            prompt_version=prompt_version,
            input_hash=input_hash,
        )
        async with self.db_service.transaction(immediate=True) as conn:
            await conn.execute(
                """
                INSERT INTO research_rounds (
                    id, base_topic, research_objective, status, current_stage,
                    terminal_code, terminal_reason, policy_snapshot_id,
                    prompt_version, input_hash, engine_version, started_at,
                    updated_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'canonical_v1', ?, ?, ?)
                """,
                (
                    round_state.id,
                    round_state.base_topic,
                    round_state.research_objective,
                    round_state.status.value,
                    round_state.current_stage,
                    round_state.terminal_code,
                    round_state.terminal_reason,
                    round_state.policy_snapshot_id,
                    round_state.prompt_version,
                    round_state.input_hash,
                    round_state.started_at,
                    round_state.updated_at,
                    round_state.completed_at,
                ),
            )
            await self._append_event(
                conn,
                round_state.id,
                "RoundCreated",
                {
                    "base_topic": base_topic,
                    "research_objective": research_objective,
                },
            )
        return round_state

    async def get(self, round_id: str) -> Optional[ResearchRound]:
        conn = await self.db_service._get_connection()
        async with conn.execute(
            "SELECT * FROM research_rounds WHERE id = ?",
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return self._from_row(dict(row)) if row else None

    async def recover_abandoned_rounds(self) -> list[dict[str, Any]]:
        """Fail unterminated rounds left by a previous production process.

        The canonical runner calls this only after acquiring its process-wide
        single-instance lock.  At that point any non-terminal round belongs to
        a process that no longer owns the production loop.  Rounds that already
        carry a workflow terminal (for example ``filled`` or
        ``market_closed_pending``) are deliberately preserved: recovery must
        not rewrite an execution outcome or infer missing lineage.
        """
        recovered_at = datetime.now().isoformat()
        recovered: list[dict[str, Any]] = []
        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                """
                SELECT id, status, current_stage, updated_at
                FROM research_rounds
                WHERE status NOT IN ('completed', 'failed')
                  AND COALESCE(terminal_code, '') = ''
                ORDER BY datetime(started_at), rowid
                """
            ) as cursor:
                rows = await cursor.fetchall()

            for raw_row in rows:
                row = dict(raw_row)
                reason = (
                    "上一生产进程在写入明确终态前结束；"
                    "canonical 单实例启动恢复为 failed；"
                    f"previous_stage={row['current_stage']}"
                )
                await conn.execute(
                    """
                    UPDATE research_rounds
                    SET status = 'failed', current_stage = 'failed',
                        terminal_code = 'failed', terminal_reason = ?,
                        updated_at = ?, completed_at = ?
                    WHERE id = ?
                      AND status NOT IN ('completed', 'failed')
                      AND COALESCE(terminal_code, '') = ''
                    """,
                    (reason, recovered_at, recovered_at, row["id"]),
                )
                await self._append_event(
                    conn,
                    str(row["id"]),
                    "AbandonedRoundRecovered",
                    {
                        "previous_status": row["status"],
                        "previous_stage": row["current_stage"],
                        "previous_updated_at": row["updated_at"],
                        "terminal_code": "failed",
                        "recovery_scope": "canonical_single_instance_startup",
                    },
                )
                recovered.append(
                    {
                        "round_id": str(row["id"]),
                        "previous_status": str(row["status"]),
                        "previous_stage": str(row["current_stage"]),
                        "previous_updated_at": str(row["updated_at"]),
                        "recovered_at": recovered_at,
                    }
                )
        return recovered

    async def advance(
        self,
        round_id: str,
        next_status: ResearchRoundStatus,
        *,
        event_type: Optional[str] = None,
        payload: Optional[Mapping[str, Any]] = None,
        terminal_code: Optional[str] = None,
        terminal_reason: Optional[str] = None,
    ) -> ResearchRound:
        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                "SELECT * FROM research_rounds WHERE id = ?",
                (round_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                raise KeyError(f"research round not found: {round_id}")
            current = self._from_row(dict(row))
            updated = current.transition(
                next_status,
                terminal_code=terminal_code,
                terminal_reason=terminal_reason,
            )
            await conn.execute(
                """
                UPDATE research_rounds
                SET status = ?, current_stage = ?, terminal_code = ?,
                    terminal_reason = ?, updated_at = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    updated.status.value,
                    updated.current_stage,
                    updated.terminal_code,
                    updated.terminal_reason,
                    updated.updated_at,
                    updated.completed_at,
                    round_id,
                ),
            )
            await self._append_event(
                conn,
                round_id,
                event_type or _event_name(next_status),
                payload or {},
            )
        return updated

    async def persist_sources(
        self,
        round_id: str,
        *,
        document_ids: list[str],
        presented_document_count: int,
        traceable_document_count: int,
        untraceable_document_count: int,
        resolved_document_count: Optional[int] = None,
        capacity_excluded_document_count: int = 0,
        resolved_source_count_before_limit: Optional[int] = None,
    ) -> ResearchRound:
        """Atomically link canonical sources and advance the durable round.

        A ``SourcesPersisted`` event must never claim evidence that is absent
        from ``round_documents``.  Existing historical gaps remain untouched;
        this forward-only boundary makes new rounds self-consistent.
        """
        unique_ids = sorted({str(value) for value in document_ids if value})
        max_links = max(
            1,
            int(get_config().get("database", {}).get("max_round_document_links", 300)),
        )
        if len(unique_ids) > max_links:
            raise RuntimeError(
                "round source lineage exceeds configured hard limit: "
                f"actual={len(unique_ids)} limit={max_links}"
            )
        capacity_excluded = max(int(capacity_excluded_document_count), 0)
        untraceable = max(int(untraceable_document_count), 0)
        traceable = max(int(traceable_document_count), 0)
        presented = max(int(presented_document_count), 0)
        resolved = (
            max(int(resolved_document_count), 0)
            if resolved_document_count is not None
            else traceable + capacity_excluded
        )
        if traceable + untraceable + capacity_excluded != presented:
            raise RuntimeError(
                "source document classification mismatch: "
                f"presented={presented} traceable={traceable} "
                f"untraceable={untraceable} capacity_excluded={capacity_excluded}"
            )
        if resolved != traceable + capacity_excluded:
            raise RuntimeError(
                "resolved source document count mismatch: "
                f"resolved={resolved} selected={traceable} "
                f"capacity_excluded={capacity_excluded}"
            )
        resolved_source_count = int(
            resolved_source_count_before_limit
            if resolved_source_count_before_limit is not None
            else len(unique_ids)
        )
        if resolved_source_count < len(unique_ids):
            raise RuntimeError(
                "resolved source count is below persisted lineage: "
                f"resolved={resolved_source_count} persisted={len(unique_ids)}"
            )
        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                "SELECT * FROM research_rounds WHERE id = ?",
                (round_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                raise KeyError(f"research round not found: {round_id}")
            current = self._from_row(dict(row))
            updated = current.transition(ResearchRoundStatus.SOURCES_PERSISTED)

            if unique_ids:
                placeholders = ",".join("?" for _ in unique_ids)
                async with conn.execute(
                    f"SELECT id FROM documents WHERE id IN ({placeholders})",
                    unique_ids,
                ) as cursor:
                    existing = {str(item[0]) for item in await cursor.fetchall()}
            else:
                existing = set()
            if existing != set(unique_ids):
                missing = sorted(set(unique_ids) - existing)
                raise RuntimeError(
                    "source lineage references missing canonical documents: "
                    + ",".join(missing[:10])
                )

            linked_at = datetime.now().isoformat()
            for document_id in unique_ids:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO round_documents(
                        round_id, document_id, usage, linked_at
                    ) VALUES (?, ?, 'source', ?)
                    """,
                    (round_id, document_id, linked_at),
                )
            async with conn.execute(
                "SELECT COUNT(*) FROM round_documents "
                "WHERE round_id = ? AND usage = 'source'",
                (round_id,),
            ) as cursor:
                linked_row = await cursor.fetchone()
            linked_source_count = int(linked_row[0] if linked_row else 0)
            if linked_source_count != len(unique_ids):
                raise RuntimeError(
                    "round source lineage count mismatch: "
                    f"expected={len(unique_ids)} actual={linked_source_count}"
                )

            await conn.execute(
                """
                UPDATE research_rounds
                SET status = ?, current_stage = ?, terminal_code = ?,
                    terminal_reason = ?, updated_at = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    updated.status.value,
                    updated.current_stage,
                    updated.terminal_code,
                    updated.terminal_reason,
                    updated.updated_at,
                    updated.completed_at,
                    round_id,
                ),
            )
            await self._append_event(
                conn,
                round_id,
                "SourcesPersisted",
                {
                    "source_count": linked_source_count,
                    "linked_source_count": linked_source_count,
                    "presented_document_count": presented,
                    "resolved_document_count": resolved,
                    "traceable_document_count": traceable,
                    "untraceable_document_count": untraceable,
                    "capacity_excluded_document_count": capacity_excluded,
                    "resolved_source_count_before_limit": resolved_source_count,
                    "lineage_limit": max_links,
                },
            )
        return updated

    async def fail(
        self,
        round_id: str,
        *,
        code: str,
        reason: str,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> ResearchRound:
        current = await self.get(round_id)
        if current is None:
            raise KeyError(f"research round not found: {round_id}")
        if current.status in {ResearchRoundStatus.FAILED, ResearchRoundStatus.COMPLETED}:
            return current
        return await self.advance(
            round_id,
            ResearchRoundStatus.FAILED,
            event_type="RoundFailed",
            payload={"code": code, "reason": reason, **dict(payload or {})},
            terminal_code=code,
            terminal_reason=reason,
        )

    async def interrupt_for_shutdown(
        self,
        round_id: str,
        *,
        signal_name: str,
    ) -> ResearchRound:
        """Record process shutdown without overwriting a business terminal.

        A round can persist its business outcome before reflection and the
        final ``completed`` transition. Treating a SIGTERM in that short
        window as the round's outcome used to rewrite (for example)
        ``no_evidence`` to ``failed``. Keep the business terminal immutable
        and append one idempotent finalization-interruption event instead.
        Rounds with no terminal still become a canonical failure.
        """
        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                "SELECT * FROM research_rounds WHERE id = ?",
                (round_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                raise KeyError(f"research round not found: {round_id}")
            current = self._from_row(dict(row))
            if current.status in {
                ResearchRoundStatus.FAILED,
                ResearchRoundStatus.COMPLETED,
            }:
                return current

            if current.terminal_code:
                event_type = "RoundFinalizationInterrupted"
                async with conn.execute(
                    """
                    SELECT 1
                    FROM round_events
                    WHERE round_id = ? AND event_type = ?
                    LIMIT 1
                    """,
                    (round_id, event_type),
                ) as cursor:
                    already_recorded = await cursor.fetchone()
                if not already_recorded:
                    await self._append_event(
                        conn,
                        round_id,
                        event_type,
                        {
                            "business_terminal_code": current.terminal_code,
                            "business_terminal_reason": current.terminal_reason,
                            "interrupted_stage": current.current_stage,
                            "interrupted_status": current.status.value,
                            "shutdown_signal": signal_name,
                        },
                    )
                return current

            reason = f"研究轮失败：生产进程收到终止信号 {signal_name}"
            updated = current.transition(
                ResearchRoundStatus.FAILED,
                terminal_code="failed",
                terminal_reason=reason,
            )
            await conn.execute(
                """
                UPDATE research_rounds
                SET status = ?, current_stage = ?, terminal_code = ?,
                    terminal_reason = ?, updated_at = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    updated.status.value,
                    updated.current_stage,
                    updated.terminal_code,
                    updated.terminal_reason,
                    updated.updated_at,
                    updated.completed_at,
                    round_id,
                ),
            )
            await self._append_event(
                conn,
                round_id,
                "RoundFailed",
                {
                    "code": "failed",
                    "reason": reason,
                    "shutdown_signal": signal_name,
                },
            )
        return updated

    async def latest_status(self) -> Optional[dict[str, Any]]:
        conn = await self.db_service._get_connection()
        async with conn.execute(
            """
            SELECT *
            FROM research_rounds
            ORDER BY datetime(started_at) DESC, rowid DESC
            LIMIT 1
            """
        ) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        result = dict(row)
        async with conn.execute(
            """
            SELECT sequence, event_type, payload_json, created_at
            FROM round_events
            WHERE round_id = ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (result["id"],),
        ) as cursor:
            event = await cursor.fetchone()
        result["latest_event"] = dict(event) if event else None
        return result

    async def record_event(
        self,
        round_id: str,
        event_type: str,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Append an auditable event without changing the round state."""
        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                "SELECT 1 FROM research_rounds WHERE id = ?",
                (round_id,),
            ) as cursor:
                exists = await cursor.fetchone()
            if not exists:
                raise KeyError(f"research round not found: {round_id}")
            await self._append_event(conn, round_id, event_type, payload or {})

    async def _append_event(
        self,
        conn,
        round_id: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        # research_rounds and every other canonical production writer use
        # local ISO-8601 timestamps.  SQLite's datetime('now') is UTC and
        # returns a different, second-precision format; mixing it here made
        # an event appear eight hours before its own round on UTC+08 hosts and
        # broke chronological audits whenever another writer used the local
        # timestamp.  Keep this forward-only: historical rows remain intact.
        created_at = datetime.now().isoformat()
        async with conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 "
            "FROM round_events WHERE round_id = ?",
            (round_id,),
        ) as cursor:
            row = await cursor.fetchone()
        await conn.execute(
            """
            INSERT INTO round_events (
                round_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                round_id,
                int(row[0] if row else 1),
                event_type,
                json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, default=str),
                created_at,
            ),
        )

    @staticmethod
    def _from_row(row: Mapping[str, Any]) -> ResearchRound:
        return ResearchRound(
            id=str(row["id"]),
            base_topic=str(row["base_topic"]),
            research_objective=str(row["research_objective"]),
            status=ResearchRoundStatus(str(row["status"])),
            current_stage=str(row["current_stage"]),
            terminal_code=row.get("terminal_code"),
            terminal_reason=row.get("terminal_reason"),
            policy_snapshot_id=row.get("policy_snapshot_id"),
            prompt_version=row.get("prompt_version"),
            input_hash=row.get("input_hash"),
            started_at=str(row["started_at"]),
            updated_at=str(row["updated_at"]),
            completed_at=row.get("completed_at"),
        )


def _event_name(status: ResearchRoundStatus) -> str:
    return "".join(part.title() for part in status.value.split("_"))
