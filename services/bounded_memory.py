"""Bounded learning caches backed by immutable, verified evidence.

Only derived hot memory is replaced. Raw rounds, predictions, execution records
and the immutable archive are never garbage-collected by this module.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
import hashlib
import json
import re

from ..core.config import get_config
from .storage_archive import ArchiveStore, canonical_bytes, content_hash


def normalized(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip().casefold()


def stable_locator(locator):
    return {key: locator[key] for key in ('archive_path', 'kind', 'key', 'revision')}


def byte_cost(value, *metadata):
    # Includes conservative row/index allocation, not character counts.
    size = len(canonical_bytes(value)) + sum(len(str(v).encode()) for v in metadata) + 2048
    return ((size+4095)//4096)*4096


def outcome(memory):
    if memory.get('status') == 'failed' or memory.get('failure_reason'):
        return 'failure'
    predictions = memory.get('predictions', [])
    if any((p.get('result') in ('incorrect', 'wrong', 'partial', 'failed') or p.get('is_correct') in (0, False))
           and p.get('status') == 'validated' for p in predictions):
        return 'counterexample'
    if any(p.get('status') not in ('validated', 'expired', 'cancelled', 'execution_rejected') for p in predictions):
        return 'uncertain'
    if any((p.get('result') in ('correct', 'success') or p.get('is_correct') in (1, True))
           and p.get('status') == 'validated' for p in predictions):
        return 'success'
    if any(c.get('risks') for c in memory.get('conclusions', [])):
        return 'counterexample'
    return 'neutral'


def episode_stub(memory, locator, available_at):
    """Search hints are whole source fields; retrieval restores full memory."""
    hints = []
    if memory.get('failure_reason') and len(str(memory['failure_reason']).encode()) <= 512:
        hints.append(memory['failure_reason'])
    for item in memory.get('conclusions', []) + memory.get('reflections', []):
        for field in ('conclusion', 'risks', 'lessons_learned', 'ticker'):
            value = item.get(field)
            if value and len(str(value).encode()) <= 512 and len(canonical_bytes(hints+[value])) <= 2048:
                hints.append(value)
    return {'schema_version': 2, 'round_id': memory['round_id'], 'source_hash': memory['source_hash'],
            'topic': memory['topic'], 'status': memory['status'], 'outcome': outcome(memory),
            'available_at': available_at, 'locator': locator, 'search_hints': hints,
            'sample_priority': content_hash([memory['round_id'], memory['source_hash']])}


async def ensure_memory_schema(conn):
    for sql in [
        '''CREATE TABLE IF NOT EXISTS memory_units(
            layer TEXT NOT NULL, memory_key TEXT NOT NULL, topic TEXT NOT NULL,
            available_at TEXT NOT NULL, value_json TEXT NOT NULL,
            locator_json TEXT NOT NULL, byte_cost INTEGER NOT NULL,
            PRIMARY KEY(layer,memory_key)) WITHOUT ROWID''',
        'CREATE INDEX IF NOT EXISTS idx_memory_units_topic ON memory_units(topic,available_at)',
        'CREATE TABLE IF NOT EXISTS memory_archive_paths(path TEXT PRIMARY KEY) WITHOUT ROWID',
    ]:
        await conn.execute(sql)


class BoundedMemoryStore:
    def __init__(self, db, archive: ArchiveStore):
        self.db, self.archive = db, archive
        cfg = get_config().get('agent_memory', {})
        self.budgets = {'episodic': max(0, int(cfg.get('episodic_max_bytes', 32*1024**2))),
                        'semantic': max(0, int(cfg.get('semantic_max_bytes', 8*1024**2))),
                        'procedural': max(0, int(cfg.get('procedural_max_bytes', 2*1024**2)))}
        self.max_item_bytes = max(1024, int(cfg.get('max_item_bytes', 8192)))

    async def remember(self, memory, *, available_at=None, source_locator=None):
        from .research_memory import now_utc, utc_time
        if source_locator is not None:
            from .research_memory import build_memory
            snapshot = await asyncio.to_thread(ArchiveStore.read, source_locator)
            if canonical_bytes(build_memory(snapshot)) != canonical_bytes(memory):
                raise ValueError('Learning memory does not match archived evidence')
        conn = await self.db._get_connection()
        async with conn.execute('SELECT available_at FROM research_memories WHERE round_id=? AND source_hash=?',
                                (memory['round_id'], memory['source_hash'])) as cursor:
            existing = await cursor.fetchone()
        key = memory['round_id']+':'+memory['source_hash']
        old_locator = await asyncio.to_thread(self.archive.latest_locator, 'research_memory', key)
        old = await asyncio.to_thread(ArchiveStore.read, old_locator) if old_locator else None
        available_at = (old['available_at'] if old else existing[0] if existing else
                        utc_time(available_at) if available_at else now_utc())
        full = {'layer': 'episodic', 'topic': memory['topic'], 'available_at': available_at,
                'memory': memory, 'source_archive': stable_locator(source_locator) if source_locator
                    else (old or {}).get('source_archive')}
        locator = stable_locator(await asyncio.to_thread(self.archive.put, 'research_memory', key, full))
        async with self.db.transaction(immediate=True):
            await conn.execute('INSERT OR IGNORE INTO memory_archive_paths VALUES (?)', (str(self.archive.path),))
        stub = episode_stub(memory, locator, available_at)
        cost = byte_cost(stub, memory['round_id'], memory['source_hash'], memory['topic'], available_at)
        if len(canonical_bytes(stub)) > self.max_item_bytes:
            return {'admitted': False, 'reason': 'oversized_memory', 'archive': locator}
        async with self.db.transaction(immediate=True):
            await conn.execute('''INSERT INTO research_memories VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(round_id,source_hash) DO UPDATE SET memory_json=excluded.memory_json''',
                (memory['round_id'], memory['source_hash'], memory['topic'], available_at,
                 memory['completed_at'], memory['status'], json.dumps(stub, ensure_ascii=False, separators=(',', ':'))))
            removed, blocked = await self._trim_episodes(conn)
        operations = []
        for layer, unit_key, value in self._extract_units(memory):
            operations.append(await self.upsert_unit(layer, unit_key, memory['topic'], value,
                {'round_id': memory['round_id'], 'memory': locator}, available_at=available_at))
        return {'admitted': (memory['round_id'], memory['source_hash']) not in removed,
                'operation': ('NOOP' if old and canonical_bytes(old) == canonical_bytes(full)
                              else 'UPDATE' if existing else 'ADD'), 'byte_cost': cost,
                'evicted_samples': len(removed), 'protected_budget_floor': blocked,
                'archive': locator, 'unit_operations': operations}

    @staticmethod
    def _extract_units(memory):
        # Semantic identity comes from structured scope, never similarity alone.
        grouped = {}
        for c in memory.get('conclusions', []):
            scope = [normalized(memory['topic']), normalized(c.get('ticker')), normalized(c.get('question'))]
            for field in ('conclusion', 'risks', 'key_points'):
                if c.get(field):
                    key = content_hash([scope, field])
                    item = grouped.setdefault(key, {'field': field, 'texts': [],
                        'ticker': c.get('ticker'), 'historical_prior': True})
                    if c[field] not in item['texts']:
                        item['texts'].append(c[field])
        for key, item in grouped.items():
            yield 'semantic', key, {'field': item['field'], 'text': '\n\n'.join(item['texts']),
                'ticker': item['ticker'], 'historical_prior': True}
        for r in memory.get('reflections', []):
            if r.get('lessons_learned'):
                value = {'field': 'lessons_learned', 'text': r['lessons_learned'], 'historical_prior': True}
                yield 'procedural', content_hash([normalized(memory['topic']), normalized(value['text'])]), value

    async def _trim_episodes(self, conn):
        async with conn.execute('SELECT round_id,source_hash,topic,available_at,memory_json FROM research_memories') as cur:
            rows = await cur.fetchall()
        async with conn.execute("SELECT record_id FROM storage_pins WHERE kind IN ('round','memory')") as cur:
            pins = {r[0] for r in await cur.fetchall()}
        candidates = []
        for row in rows:
            memory = json.loads(row[4])
            # Legacy records require proof before replacement; backfill handles them.
            protected = row[0] in pins or memory.get('schema_version') != 2
            candidates.append({'id': (row[0], row[1]), 'cost': byte_cost(memory, *row[:4]),
                'bucket': (normalized(row[2]), memory.get('outcome', 'neutral')),
                'priority': memory.get('sample_priority', ''), 'protected': protected,
                'locator': memory.get('locator')})
        used = sum(r['cost'] for r in candidates)
        removed = set()
        while used > self.budgets['episodic']:
            eligible = [r for r in candidates if not r['protected']]
            if not eligible:
                break
            totals, counts = defaultdict(int), defaultdict(int)
            for r in candidates:
                totals[r['bucket']] += r['cost']; counts[r['bucket']] += 1
            # Keep topic/outcome coverage while space permits; deterministic
            # random priorities avoid a successful/recent-only replay buffer.
            victim = max(eligible, key=lambda r: (counts[r['bucket']] > 1,
                         totals[r['bucket']], r['priority']))
            await asyncio.to_thread(ArchiveStore.read, victim['locator'])
            await conn.execute('DELETE FROM research_memories WHERE round_id=? AND source_hash=?', victim['id'])
            candidates.remove(victim); removed.add(victim['id']); used -= victim['cost']
        return removed, used > self.budgets['episodic']

    async def upsert_unit(self, layer, key, topic, value, source, *, available_at=None, deleted=False):
        from .research_memory import now_utc, utc_time
        if layer not in ('semantic', 'procedural'):
            raise ValueError('Only derived semantic/procedural units can be edited')
        if not source.get('memory'):
            raise ValueError('Memory updates require a verified source locator')
        # Validate that a caller cannot invent a source or use corrupt evidence.
        origin = await asyncio.to_thread(ArchiveStore.read, source['memory'])
        if source.get('round_id') != origin['memory']['round_id'] or topic != origin['topic']:
            raise ValueError('Memory source scope mismatch')
        if not deleted and (layer, key, value) not in list(self._extract_units(origin['memory'])):
            raise ValueError('Memory update is not supported by its source')
        conn = await self.db._get_connection()
        async with conn.execute('SELECT available_at,value_json FROM memory_units WHERE layer=? AND memory_key=?',
                                (layer, key)) as cur:
            existing = await cur.fetchone()
        available_at = utc_time(available_at) if available_at else now_utc()
        if available_at < origin['available_at']:
            raise ValueError('Memory cannot predate source availability')
        unit = {'layer': layer, 'key': key, 'topic': topic, 'available_at': available_at,
                'value': value, 'source': source, 'deleted': deleted}
        cost = byte_cost(unit, layer, key, topic, available_at)
        if len(canonical_bytes(unit)) > self.max_item_bytes:
            return 'REJECT_OVERSIZED'
        if deleted:
            async with self.db.transaction(immediate=True):
                async with conn.execute("SELECT 1 FROM storage_pins WHERE kind='memory_unit' AND record_id=?", (key,)) as cur:
                    if await cur.fetchone():
                        raise RuntimeError('Pinned memory unit retained')
                await asyncio.to_thread(self.archive.put, 'memory_unit', layer+':'+key, unit)
                await conn.execute('DELETE FROM memory_units WHERE layer=? AND memory_key=?', (layer, key))
            return 'DELETE'
        locator = stable_locator(await asyncio.to_thread(self.archive.put, 'memory_unit', layer+':'+key, unit))
        if existing and json.loads(existing[1]) == value and not deleted:
            return 'NOOP'
        if existing and existing[0] > available_at:
            return 'HISTORICAL_ONLY'
        async with self.db.transaction(immediate=True):
            await conn.execute('''INSERT INTO memory_units VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(layer,memory_key) DO UPDATE SET available_at=excluded.available_at,
                value_json=excluded.value_json,locator_json=excluded.locator_json,byte_cost=excluded.byte_cost''',
                (layer, key, topic, available_at, json.dumps(value, ensure_ascii=False), json.dumps(locator), cost))
            async with conn.execute('SELECT memory_key,byte_cost,locator_json FROM memory_units WHERE layer=?', (layer,)) as cur:
                entries = await cur.fetchall()
            async with conn.execute("SELECT record_id FROM storage_pins WHERE kind='memory_unit'") as cur:
                pins = {r[0] for r in await cur.fetchall()}
            used = sum(r[1] for r in entries)
            for victim_key, victim_cost, pointer in sorted(entries, key=lambda r: hashlib.sha256(r[0].encode()).hexdigest(), reverse=True):
                if used <= self.budgets[layer]:
                    break
                if victim_key in pins:
                    continue
                await asyncio.to_thread(ArchiveStore.read, json.loads(pointer))
                await conn.execute('DELETE FROM memory_units WHERE layer=? AND memory_key=?', (layer, victim_key))
                used -= victim_cost
        return 'UPDATE' if existing else 'ADD'

    async def backfill(self, *, limit=20):
        conn = await self.db._get_connection()
        async with conn.execute("SELECT memory_json,available_at FROM research_memories WHERE json_extract(memory_json,'$.schema_version') IS NOT 2 LIMIT ?", (limit,)) as cur:
            rows = await cur.fetchall()
        for raw, available_at in rows:
            memory = json.loads(raw)
            async with conn.execute("SELECT locator_json FROM storage_jobs WHERE kind='round' AND record_id=? AND manifest_hash=? AND locator_json IS NOT NULL LIMIT 1",
                                    (memory['round_id'], memory['source_hash'])) as cur:
                origin = await cur.fetchone()
            await self.remember(memory, available_at=available_at,
                                source_locator=json.loads(origin[0]) if origin else None)
        return {'converted': len(rows), 'can_continue': len(rows) == limit}

    async def usage(self):
        conn = await self.db._get_connection()
        async with conn.execute('SELECT round_id,source_hash,topic,available_at,memory_json FROM research_memories') as cur:
            episodes = await cur.fetchall()
        used = {'episodic': sum(byte_cost(json.loads(r[4]), *r[:4]) for r in episodes),
                'semantic': 0, 'procedural': 0}
        async with conn.execute('SELECT layer,sum(byte_cost) FROM memory_units GROUP BY layer') as cur:
            for layer, size in await cur.fetchall():
                used[layer] = size
        return {layer: {'charged_bytes': size, 'max_bytes': self.budgets[layer],
                        'within_budget': size <= self.budgets[layer]}
                for layer, size in used.items()}


async def retrieve_units(db_path, query, cutoff, *, limit=12):
    """Cold version lookup preserves point-in-time facts after hot replacement."""
    import aiosqlite
    units = []
    paths = set()
    from pathlib import Path
    async with aiosqlite.connect(Path(db_path).resolve().as_uri()+'?mode=ro', uri=True) as conn:
        async with conn.execute("SELECT 1 FROM sqlite_master WHERE name='memory_units'") as cur:
            if not await cur.fetchone():
                return []
        async with conn.execute('SELECT locator_json FROM memory_units WHERE topic=? AND available_at<=?', (query, cutoff)) as cur:
            for row in await cur.fetchall():
                locator = json.loads(row[0]); paths.add(locator['archive_path'])
                units.append(await asyncio.to_thread(ArchiveStore.read, locator))
        # Historical paths must survive replacement/deletion of a hot unit.
        async with conn.execute("SELECT DISTINCT json_extract(locator_json,'$.archive_path') FROM storage_jobs WHERE kind='round' AND locator_json IS NOT NULL") as cur:
            paths.update(r[0] for r in await cur.fetchall())
        async with conn.execute('SELECT path FROM memory_archive_paths') as cur:
            paths.update(r[0] for r in await cur.fetchall())
    for path in paths:
        units.extend(await asyncio.to_thread(ArchiveStore(path).find_memories, query, cutoff, limit=limit*4))
    latest = {}
    for unit in sorted(units, key=lambda v: v['available_at'], reverse=True):
        latest.setdefault((unit['layer'], unit['key']), unit)
    return [u for u in latest.values() if not u.get('deleted')][:limit]
