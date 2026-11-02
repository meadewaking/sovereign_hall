"""Auditable research memory and resumable, non-destructive compaction."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import time
from zoneinfo import ZoneInfo

from .storage_archive import ArchiveStore, canonical_bytes, content_hash

POLICY_VERSION = "lossless_memory_v1"


def utc_time(value: str) -> str:
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Legacy research timestamps were written with local datetime.now().
    if date.tzinfo is None:
        date = date.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return date.astimezone(timezone.utc).isoformat()


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}


def rows(conn, table: str, where: str, params=()) -> list[dict]:
    if table not in tables(conn):
        return []
    return [dict(r) for r in conn.execute(f"SELECT * FROM {table} WHERE {where}", params)]


def round_snapshot(conn, round_id: str, max_documents: int = 500) -> dict:
    rounds = rows(conn, "research_rounds", "id=?", (round_id,))
    if not rounds:
        raise ValueError(f"Unknown round {round_id}")
    links = rows(conn, "round_documents", "round_id=?", (round_id,))
    if len(links) > max_documents:
        raise RuntimeError(f"Oversized lineage ({len(links)} links); retain and review separately")
    document_bytes = conn.execute(
        "SELECT COALESCE(SUM(length(CAST(content AS BLOB))+COALESCE(length(embedding),0)),0) "
        "FROM documents WHERE id IN (SELECT document_id FROM round_documents WHERE round_id=?)", (round_id,)
    ).fetchone()[0]
    if document_bytes > 64 * 1024**2:
        raise RuntimeError("Oversized payload; retain and review separately")
    if "committee_turns" in tables(conn):
        turn_bytes = conn.execute(
            "SELECT COALESCE(SUM(length(CAST(raw_output AS BLOB))),0) FROM committee_turns "
            "WHERE meeting_id IN (SELECT id FROM committee_meetings WHERE round_id=?)", (round_id,)
        ).fetchone()[0]
        if turn_bytes > 64 * 1024**2:
            raise RuntimeError("Oversized committee payload; retain and review separately")
    result = {"round": rounds[0], "round_documents": links}
    for table in ("round_events", "report_conclusions", "reflection_summary",
                  "price_predictions", "execution_intents", "committee_meetings"):
        result[table] = rows(conn, table, "round_id=?", (round_id,))
    result["committee_tasks"] = rows(
        conn, "committee_tasks", "meeting_id IN (SELECT id FROM committee_meetings WHERE round_id=?)",
        (round_id,),
    )
    result["committee_turns"] = rows(
        conn, "committee_turns", "meeting_id IN (SELECT id FROM committee_meetings WHERE round_id=?)",
        (round_id,),
    )
    result["documents"] = rows(
        conn, "documents", "id IN (SELECT document_id FROM round_documents WHERE round_id=?)",
        (round_id,),
    )
    # Snapshot an already cold record by restoring the original payload.
    if "archived_payloads" in tables(conn):
        for kind, collection, field in (("document", "documents", "content"),
                                         ("committee_turn", "committee_turns", "raw_output")):
            for row in result[collection]:
                if not row.get(field):
                    locator = conn.execute(
                        "SELECT locator_json FROM archived_payloads WHERE kind=? AND record_id=? AND hot_evicted=1",
                        (kind, str(row["id"])),
                    ).fetchone()
                    if locator:
                        original = ArchiveStore.read(json.loads(locator[0]))
                        row[field] = original.get(field)
                        if kind == "document":
                            row["embedding"] = original.get("embedding")
    return result


def build_memory(snapshot: dict) -> dict:
    """Extract authoritative fields without LLM invention or success bias."""
    r = snapshot["round"]
    if r["status"] not in {"completed", "failed"} or not r.get("completed_at"):
        raise ValueError("Only closed rounds may be archived")
    return {
        "schema_version": 1, "round_id": r["id"], "topic": r["base_topic"],
        "objective": r["research_objective"], "status": r["status"],
        "completed_at": r["completed_at"], "terminal_code": r.get("terminal_code"),
        "failure_reason": r.get("terminal_reason"),
        "historical_prior": True,
        "conclusions": snapshot["report_conclusions"],
        "reflections": snapshot["reflection_summary"],
        "predictions": snapshot["price_predictions"],
        "decisions": snapshot["execution_intents"],
        "evidence": [{k: d.get(k) for k in (
            "id", "title", "url", "content_hash", "publish_time", "publish_time_source", "crawled_at"
        )} for d in snapshot["documents"]],
        "turn_ids": [t["id"] for t in snapshot["committee_turns"]],
        "event_ids": [e["id"] for e in snapshot["round_events"]],
        "source_hash": content_hash(snapshot),
        "coverage": {k: len(v) for k, v in snapshot.items() if isinstance(v, list)},
    }


class ProtectionPolicy:
    def __init__(self, conn, *, wiki_root: Path | None = None):
        self.conn = conn
        deadline = time.monotonic() + 30
        self.conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
        self.wiki_root = wiki_root
        self._wiki_ids = None
        self._wiki_urls = None
        self._wiki_unknown = False

    def _wiki_references(self):
        from .wiki_knowledge import parse_frontmatter
        self._wiki_ids, self._wiki_urls = set(), set()
        if self.wiki_root is None or not self.wiki_root.exists():
            return
        try:
            for p in (self.wiki_root / "wiki" / "sources").glob("*.md"):
                front, _ = parse_frontmatter(p.read_text(encoding="utf-8"))
                doc_id, url = front.get("source_id"), front.get("url")
                if doc_id:
                    self._wiki_ids.add(str(doc_id))
                if url:
                    self._wiki_urls.add(str(url))
                if not doc_id and not url:
                    self._wiki_unknown = True
            from .wiki_source_archive import SourcePageArchive
            for doc_id, url in SourcePageArchive(self.wiki_root).references():
                if doc_id:
                    self._wiki_ids.add(str(doc_id))
                if url:
                    self._wiki_urls.add(str(url))
                if not doc_id and not url:
                    self._wiki_unknown = True
        except (OSError, ValueError):
            self._wiki_unknown = True

    def document_reasons(self, doc: dict) -> list[str]:
        reasons = []
        available = tables(self.conn)
        if doc.get("round_id"):
            reasons.append("document_round_lineage")
        if "round_documents" in available and self.conn.execute(
            "SELECT 1 FROM round_documents WHERE document_id=? LIMIT 1", (doc["id"],)
        ).fetchone():
            reasons.append("round_document_reference")
        if "storage_pins" in available and self.conn.execute(
            "SELECT 1 FROM storage_pins WHERE kind='document' AND record_id=?", (doc["id"],)
        ).fetchone():
            reasons.append("explicit_pin")
        # Includes non-FK source_document_id fields and future direct consumers.
        for table in sorted(available):
            if table.startswith(("sqlite_", "document_search")):
                continue
            columns = {r[1] for r in self.conn.execute(f'PRAGMA table_info("{table}")')}
            for column in ("source_document_id", "document_id"):
                if column in columns and self.conn.execute(
                    f'SELECT 1 FROM "{table}" WHERE "{column}"=? LIMIT 1', (doc["id"],)
                ).fetchone():
                    reasons.append(f"reference:{table}.{column}")
            for column in columns:
                if column.endswith("_json") and table in {
                    "empirical_observations", "round_events", "experiment_events", "policy_snapshots"
                }:
                    if self.conn.execute(
                        f'SELECT 1 FROM "{table}" WHERE instr("{column}",?)>0 LIMIT 1',
                        (json.dumps(str(doc["id"]), ensure_ascii=False),),
                    ).fetchone():
                        reasons.append(f"json_reference:{table}.{column}")
        if self._wiki_ids is None:
            self._wiki_references()
        if self._wiki_unknown:
            reasons.append("unresolved_wiki_reference")
        if doc["id"] in self._wiki_ids or doc.get("url") in self._wiki_urls:
            reasons.append("wiki_source_reference")
        return sorted(set(reasons))

    def round_reasons(self, round_id: str) -> list[str]:
        reasons = []
        research = rows(self.conn, "research_rounds", "id=?", (round_id,))
        if not research or research[0].get("status") not in {"completed", "failed"}:
            reasons.append("round_not_closed")
        for task in rows(self.conn, "committee_tasks",
                         "meeting_id IN (SELECT id FROM committee_meetings WHERE round_id=?)", (round_id,)):
            if task.get("status") not in {"completed", "failed", "cancelled"}:
                reasons.append("unfinished_committee_task")
        for p in rows(self.conn, "price_predictions", "round_id=?", (round_id,)):
            if p.get("status") not in {"validated", "execution_rejected", "expired", "cancelled"}:
                reasons.append("unfinished_prediction")
        for i in rows(self.conn, "execution_intents", "round_id=?", (round_id,)):
            # Executed intents may still be tied to a position. Keep the raw
            # trajectory until an explicit lifecycle model establishes closure.
            if i.get("status") not in {"rejected", "cancelled", "expired"}:
                reasons.append("execution_or_position_dependency")
        available = tables(self.conn)
        for table in ("structured_evidence_claims", "experiment_registry", "empirical_observations", "policy_snapshots"):
            if table not in available:
                continue
            columns = {r[1] for r in self.conn.execute(f'PRAGMA table_info("{table}")')}
            for column in columns:
                if column == "round_id" or column.endswith("_json"):
                    if self.conn.execute(
                        f'SELECT 1 FROM "{table}" WHERE instr("{column}",?)>0 LIMIT 1', (round_id,)
                    ).fetchone():
                        reasons.append(f"dependency:{table}")
        if "storage_pins" in available and self.conn.execute(
            "SELECT 1 FROM storage_pins WHERE kind='round' AND record_id=?", (round_id,)
        ).fetchone():
            reasons.append("explicit_pin")
        return sorted(set(reasons))


class ResearchCompactor:
    def __init__(self, db, archive: ArchiveStore, wiki_root: Path | None = None):
        self.db = db
        self.archive = archive
        self.wiki_root = wiki_root
        self.policy_version = POLICY_VERSION
        self._wiki_reference_cache = None
        self._protected = {}

    async def _job(self, kind, record_id, state, locator=None, error=None):
        async with self.db.transaction(immediate=True) as conn:
            await conn.execute(
                """INSERT INTO storage_jobs VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(kind,record_id,policy_version) DO UPDATE SET
               state=excluded.state, manifest_hash=COALESCE(excluded.manifest_hash,manifest_hash),
               locator_json=COALESCE(excluded.locator_json,locator_json),
               updated_at=excluded.updated_at,error=excluded.error""",
                (kind, str(record_id), self.policy_version, state,
                 locator["revision"] if locator else None,
                 json.dumps(locator) if locator else None, now_utc(), error),
            )

    def _snapshot(self, round_id):
        conn = sqlite3.connect(self.db.db_path.resolve().as_uri() + "?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN")
            return round_snapshot(conn, round_id)
        finally:
            conn.close()

    async def process_round(self, round_id: str, *, evict_hot: bool = False,
                            completed_before: str | None = None) -> dict:
        self.policy_version = POLICY_VERSION + (":evict" if evict_hot else ":archive")
        self._protected = {}
        await self._job("round", round_id, "planned")
        try:
            snapshot = await asyncio.to_thread(self._snapshot, round_id)
            if completed_before and utc_time(snapshot["round"]["completed_at"]) >= completed_before:
                raise ValueError("Round no longer meets the planned age threshold")
            memory = build_memory(snapshot)
            locator = await asyncio.to_thread(self.archive.put, "round", round_id, snapshot)
            await self._job("round", round_id, "archive_verified", locator)
            conn = await self.db._get_connection()
            from .bounded_memory import BoundedMemoryStore
            memory_report = await BoundedMemoryStore(self.db, self.archive).remember(memory, source_locator=locator)
            evicted = 0
            reasons = []
            if evict_hot:
                read_conn = sqlite3.connect(self.db.db_path)
                read_conn.row_factory = sqlite3.Row
                try:
                    reasons = ProtectionPolicy(read_conn).round_reasons(round_id)
                finally:
                    read_conn.close()
                if not reasons:
                    for turn in snapshot["committee_turns"]:
                        if turn.get("raw_output"):
                            evicted += await self._evict_payload("committee_turn", turn, "raw_output")
                    reasons = sorted({reason for found in self._protected.values() for reason in found})
            await self._job("round", round_id, "completed", locator)
            return {"round_id": round_id, "state": "completed", "archive": locator,
                    "coverage": memory["coverage"], "evicted_turns": evicted, "protected_by": reasons,
                    'memory': memory_report}
        except Exception as exc:
            await self._job("round", round_id, "failed", error=str(exc) or type(exc).__name__)
            raise

    async def _evict_payload(self, kind: str, row: dict, field: str) -> int:
        locator = await asyncio.to_thread(self.archive.put, kind, str(row["id"]), row)
        table = "documents" if kind == "document" else "committee_turns"
        conn = await self.db._get_connection()
        async with self.db.transaction(immediate=True):
            async with conn.execute(f"SELECT * FROM {table} WHERE id=?", (row["id"],)) as cur:
                current = await cur.fetchone()
            async with conn.execute(
                "SELECT locator_json FROM archived_payloads WHERE kind=? AND record_id=? AND hot_evicted=1",
                (kind, str(row["id"])),
            ) as cur:
                existing = await cur.fetchone()
            if current is not None and not current[field] and existing:
                original = await asyncio.to_thread(ArchiveStore.read, json.loads(existing[0]))
                if canonical_bytes(original) == canonical_bytes(row):
                    return 0  # Resuming a verified, already evicted payload.
            if current is None or canonical_bytes(dict(current)) != canonical_bytes(row):
                raise RuntimeError("Source changed during archival; hot payload retained")
            protected = await asyncio.to_thread(self._check_roots, kind, row)
            if protected:
                self._protected[(kind, str(row["id"]))] = protected
                return 0
            await conn.execute(
                "INSERT INTO archived_payloads VALUES (?,?,?,1) "
                "ON CONFLICT(kind,record_id) DO UPDATE SET locator_json=excluded.locator_json,hot_evicted=1",
                (kind, str(row["id"]), json.dumps(locator)),
            )
            assignments = "content=NULL,embedding=NULL" if kind == "document" else "raw_output=''"
            await conn.execute(f"UPDATE {table} SET {assignments} WHERE id=?", (row["id"],))
        return 1

    def _check_roots(self, kind, row):
        check = sqlite3.connect(self.db.db_path)
        check.row_factory = sqlite3.Row
        try:
            policy = ProtectionPolicy(check, wiki_root=self.wiki_root)
            if kind == "document":
                if self._wiki_reference_cache is not None:
                    policy._wiki_ids, policy._wiki_urls, policy._wiki_unknown = self._wiki_reference_cache
                protected = policy.document_reasons(row)
                self._wiki_reference_cache = (policy._wiki_ids, policy._wiki_urls, policy._wiki_unknown)
            else:
                meeting = check.execute("SELECT round_id FROM committee_meetings WHERE id=?",
                                        (row["meeting_id"],)).fetchone()
                protected = policy.round_reasons(meeting[0]) if meeting else ["missing_round"]
        finally:
            check.close()
        return protected

    async def archive_document(self, row: dict, *, evict_hot=False,
                               created_before: str | None = None) -> dict:
        self.policy_version = POLICY_VERSION + (":evict" if evict_hot else ":archive")
        self._protected = {}
        await self._job("document", row["id"], "planned")
        try:
            if created_before and utc_time(row["created_at"]) >= created_before:
                raise ValueError("Document no longer meets the planned age threshold")
            locator = await asyncio.to_thread(self.archive.put, "document", row["id"], row)
            await self._job("document", row["id"], "archive_verified", locator)
            evicted = await self._evict_payload("document", row, "content") if evict_hot else 0
            await self._job("document", row["id"], "completed", locator)
            return {"document_id": row["id"], "archive": locator, "evicted": evicted,
                    "protected_by": self._protected.get(("document", str(row["id"])), [])}
        except Exception as exc:
            await self._job("document", row["id"], "failed", error=str(exc) or type(exc).__name__)
            raise


async def retrieve_memory(db_path, query: str, *, as_of: str | None = None,
                          limit: int = 4, max_chars: int = 6000) -> str:
    """Bounded lexical retrieval; never globally inject top-success rounds."""
    import aiosqlite
    if not query.strip() or not Path(db_path).exists():
        return ""
    terms = set(re.findall(r"[A-Za-z0-9_.]{2,}", query))
    for text in re.findall(r"[\u4e00-\u9fff]+", query):
        terms.update(text[i:i+3] for i in range(max(1, len(text)-2)) if len(text[i:i+3]) >= 2)
    terms = sorted(terms)[:32]
    cutoff = utc_time(as_of) if as_of else now_utc()
    async with aiosqlite.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        async with conn.execute("SELECT 1 FROM sqlite_master WHERE name='research_memories'") as cur:
            if not await cur.fetchone():
                return ""
        conditions = " OR ".join("instr(lower(memory_json),lower(?))>0" for _ in terms)
        sql = "SELECT round_id,available_at,topic,memory_json FROM research_memories WHERE available_at<=?"
        sql += " AND (topic=?" + (" OR " + conditions if conditions else "") + ")"
        sql += " ORDER BY available_at DESC LIMIT 1000"
        async with conn.execute(sql, (cutoff, query, *terms)) as cur:
            candidates = await cur.fetchall()
    ranked, seen = [], set()
    for round_id, available, topic, payload in candidates:
        if round_id in seen:
            continue
        seen.add(round_id)
        memory = json.loads(payload)
        haystack = json.dumps(memory, ensure_ascii=False).casefold()
        relevance = (100 if query == topic else 0) + sum(t.casefold() in haystack for t in terms)
        if relevance:
            ranked.append((relevance, available, memory))
    ranked.sort(key=lambda v: (v[0], v[1]), reverse=True)
    lines = ["【可追溯历史记忆：旧观点须重新验证；归档原文可按 round_id 回读】",
             "价格验证只评价当时动作下的价格路径；HOLD错失上涨不证伪看多论点；历史生成文字中的‘已验证事实’仍是待核验旧观点。"]
    header_lines = len(lines)
    from .prediction_feedback import format_prediction_feedback
    selected = []
    for _, _, memory in ranked[:max(0, limit)]:
        if memory.get('schema_version') == 2:
            memory = (await asyncio.to_thread(ArchiveStore.read, memory['locator']))['memory']
        selected.append(memory)
    predictions = {}
    for memory in selected:
        for index, prediction in enumerate(memory['predictions']):
            record = {**prediction, 'round_id': prediction.get('round_id') or memory['round_id']}
            predictions.setdefault((memory['round_id'], record.get('id') or index), record)
    # Whole outcome cards precede generated narrative, so the character budget
    # cannot retain a quoted wrong-thesis inference while dropping its result.
    rendered = set()
    ordered = sorted(predictions.values(), key=lambda p: (
        p.get('status') == 'validated' and bool(p.get('actual_hit_type')),
        p.get('validated_at') or '', p.get('predicted_at') or ''), reverse=True)
    for record in ordered[:12]:
        text = format_prediction_feedback(record, as_of=cutoff)
        if len('\n'.join(lines))+len(text)+1 <= max_chars:
            lines.append(text)
            rendered.add((record.get('round_id'), record.get('id')))
    from .bounded_memory import retrieve_units
    for unit in await retrieve_units(db_path, query, cutoff, limit=max(0, limit)):
        value = unit['value']
        text = (f"历史{unit['layer']}记忆（历史生成观点，未核验）: {value.get('text') or ''} | "
                f"来源轮次={unit['source']['round_id']} | 可获得时间={unit['available_at']}")
        if len('\n'.join(lines))+len(text)+1 <= max_chars:
            lines.append(text)
    for m in selected:
        items = [f"轮次={m['round_id']} | {m['completed_at']} | 议题={m['topic']} | 状态={m['status']}"]
        if m.get("failure_reason"):
            items.append("失败/终止原因: " + m["failure_reason"])
        for c in m["conclusions"]:
            items.append(f"历史判断[{c.get('id')}]: {c.get('conclusion') or ''}")
            for key, label in (("risks", "反对/风险"), ("key_points", "依据")):
                if c.get(key):
                    items.append(f"{label}[{c.get('id')}]: {c[key]}")
        for r in m["reflections"]:
            if r.get("lessons_learned"):
                items.append(f"历史经验（未自动认定有效）[{r.get('id')}]: {r['lessons_learned']}")
        for p in m["predictions"]:
            if (p.get('round_id') or m['round_id'], p.get('id')) not in rendered:
                items.append(format_prediction_feedback(p, as_of=cutoff))
        for d in m["decisions"]:
            items.append(f"决策[{d.get('id')}]: {d.get('ticker')} | 状态={d.get('status')} | "
                         f"理由={d.get('reason') or d.get('reason_json') or '参见原始记录'}")
        items.append("原始证据ID: " + ", ".join(str(e["id"]) for e in m["evidence"]))
        if len("\n".join(lines)) + len(items[0]) + 1 > max_chars:
            continue
        for item in items:
            if len("\n".join(lines)) + len(item) + 1 > max_chars:
                # Preserve complete semantic units; the header provides a stable
                # source pointer for omitted content, instead of slicing a risk.
                continue
            lines.append(item)
    return "\n".join(lines) if len(lines) > header_lines else ""
