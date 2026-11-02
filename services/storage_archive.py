"""Lossless, content-addressed archives in one dense SQLite file.

Archives are independent of LLM summaries. Every write is read back and hashed
before its locator can be used to evict a hot payload. No persistent workers.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from pathlib import Path
import sqlite3
import time
import zlib
from contextlib import contextmanager


@contextmanager
def archive_write_lock(path):
    """Serialize short-lived writers and generation replacement, across processes."""
    import fcntl
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(path.suffix + '.lock').open('a+b') as handle:
        # Never block the owner thread behind a long archive generation job.
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class MaintenanceLock:
    """Use the runner's actual lock, closing the PID-check/startup race."""
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        import fcntl
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open('a+')
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.handle.close()
            self.handle = None
            raise RuntimeError('Research runner or another maintenance process holds the lock') from None
        return self

    def __exit__(self, *_):
        import fcntl
        if self.handle:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def _encode(value):
    if isinstance(value, bytes):
        return {"$binary": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encode(v) for v in value]
    return value


def _decode(value):
    if isinstance(value, dict):
        if set(value) == {"$binary"}:
            return base64.b64decode(value["$binary"], validate=True)
        return {k: _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    return value


def canonical_bytes(value) -> bytes:
    return json.dumps(_encode(value), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def content_hash(value) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


class ArchiveStore:
    def __init__(self, path: str | Path, max_bytes: int = 1024**3):
        self.path = Path(path).resolve()
        self.max_bytes = max_bytes

    def put(self, kind: str, key: str, value) -> dict:
        return self._put(kind, key, value)

    @staticmethod
    def _blob_plan(kind, value):
        """A round is a small manifest; immutable rows are shared across rounds."""
        blobs = {}
        def blob(item):
            raw = canonical_bytes(item)
            digest = hashlib.sha256(raw).hexdigest()
            blobs[digest] = (zlib.compress(raw, 9), len(raw))
            return digest
        if kind in ('round', 'research_memory') and isinstance(value, dict):
            shared = {'documents', 'committee_turns', 'report_conclusions', 'conclusions',
                      'reflection_summary', 'reflections', 'price_predictions', 'predictions',
                      'execution_intents', 'decisions'}
            def manifest(mapping):
                result = {'scalars': {}, 'collections': {}, 'objects': {}}
                for name, items in mapping.items():
                    if name == 'memory' and isinstance(items, dict):
                        result['objects'][name] = blob(manifest(items))
                    elif isinstance(items, list) and all(isinstance(i, dict) for i in items):
                        result['collections'][name] = [blob(item) if name in shared or
                            len(canonical_bytes(item))>=1024 else {'inline': item} for item in items]
                    else:
                        result['scalars'][name] = items
                return result
            return blob(manifest(value)), 2, blobs
        return blob(value), 1, blobs

    def _put(self, kind: str, key: str, value, *, rewrite_legacy=False) -> dict:
        with archive_write_lock(self.path):
            return self._put_unlocked(kind, key, value, rewrite_legacy=rewrite_legacy)

    def _put_unlocked(self, kind: str, key: str, value, *, rewrite_legacy=False) -> dict:
        raw = canonical_bytes(value)
        revision = hashlib.sha256(raw).hexdigest()
        packed = zlib.compress(raw, level=9)
        if not self.path.exists() and self.max_bytes < len(packed) + 65536:
            raise RuntimeError("Archive capacity exceeded; hot evidence retained")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            conn.executescript("""
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS records(
                    kind TEXT NOT NULL, record_key TEXT NOT NULL,
                    revision TEXT NOT NULL, payload BLOB NOT NULL,
                    raw_bytes INTEGER NOT NULL,
                    PRIMARY KEY(kind,record_key,revision)
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS document_search
                    USING fts5(content, content='', tokenize='trigram');
                CREATE TABLE IF NOT EXISTS archive_blobs(
                    digest TEXT PRIMARY KEY, payload BLOB NOT NULL, raw_bytes INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cas_records(
                    kind TEXT NOT NULL, record_key TEXT NOT NULL, revision TEXT NOT NULL,
                    root_hash TEXT NOT NULL, format INTEGER NOT NULL,
                    PRIMARY KEY(kind,record_key,revision)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS memory_index(
                    kind TEXT NOT NULL, record_key TEXT NOT NULL, revision TEXT NOT NULL,
                    topic TEXT NOT NULL, layer TEXT NOT NULL, available_at TEXT NOT NULL,
                    PRIMARY KEY(kind,record_key,revision)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_cold_memory_topic_time
                    ON memory_index(topic,available_at);
            """)
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            conn.execute(f"PRAGMA max_page_count={max(1, self.max_bytes // page_size)}")
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute(
                "SELECT rowid FROM records WHERE kind=? AND record_key=? AND revision=?",
                (kind, key, revision),
            ).fetchone()
            cas = conn.execute('SELECT 1 FROM cas_records WHERE kind=? AND record_key=? AND revision=?',
                               (kind, key, revision)).fetchone()
            added_bytes = 0
            if not exists or (rewrite_legacy and not cas):
                root_hash, format_version, blobs = self._blob_plan(kind, value)
                missing = {digest: data for digest, data in blobs.items()
                           if not conn.execute('SELECT 1 FROM archive_blobs WHERE digest=?', (digest,)).fetchone()}
                descriptor = canonical_bytes({'root': root_hash, 'format': format_version})
                packed = zlib.compress(descriptor, 9)
                # Reserve space for indexes as well as the compressed payload.
                additional = len(packed) + 65536 + sum(len(data[0])+4096 for data in missing.values())
                if kind == "document" and not exists:
                    additional += len(str(value.get("content") or "").encode("utf-8")) * 4
                free = conn.execute('PRAGMA freelist_count').fetchone()[0]*page_size
                if self.path.stat().st_size-free + additional > self.max_bytes:
                    raise RuntimeError("Archive capacity exceeded; hot evidence retained")
                for digest, (payload, raw_bytes) in missing.items():
                    conn.execute('INSERT INTO archive_blobs VALUES (?,?,?)', (digest, payload, raw_bytes))
                    added_bytes += len(payload)
                if exists:
                    conn.execute('UPDATE records SET payload=? WHERE kind=? AND record_key=? AND revision=?',
                                 (packed, kind, key, revision))
                else:
                    cursor = conn.execute('INSERT INTO records VALUES (?,?,?,?,?)',
                                          (kind, key, revision, packed, len(raw)))
                conn.execute('INSERT INTO cas_records VALUES (?,?,?,?,?)',
                             (kind, key, revision, root_hash, format_version))
                if kind == "document" and not exists:
                    conn.execute("INSERT INTO document_search(rowid,content) VALUES (?,?)",
                                 (cursor.lastrowid, str(value.get("content") or "")))
                added_bytes += len(packed)
                # Verify the entire reconstructed graph inside the transaction.
                if canonical_bytes(self._read_record(conn, kind, key, revision)) != raw:
                    raise RuntimeError('Archive restore verification failed; hot evidence retained')
            if kind in ('research_memory', 'memory_unit'):
                conn.execute('INSERT OR IGNORE INTO memory_index VALUES (?,?,?,?,?,?)',
                    (kind, key, revision, str(value.get('topic') or ''),
                     str(value.get('layer') or 'episodic'), str(value.get('available_at') or '')))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        locator = {"archive_path": str(self.path), "kind": kind, "key": key,
                   "revision": revision, "raw_bytes": len(raw), "packed_bytes": added_bytes}
        if canonical_bytes(self.read(locator)) != raw:
            raise RuntimeError("Archive restore verification failed; hot evidence retained")
        return locator

    @staticmethod
    def _read_record(conn, kind, key, revision, *, block_cache=None):
        row = conn.execute('SELECT payload FROM records WHERE kind=? AND record_key=? AND revision=?',
                           (kind, key, revision)).fetchone()
        if row is None:
            raise RuntimeError('Missing archive record; restore unavailable')
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='packed_records'").fetchone():
            packed = conn.execute('SELECT block_hash,raw_offset,raw_bytes FROM packed_records '
                                  'WHERE kind=? AND record_key=? AND revision=?',
                                  (kind, key, revision)).fetchone()
            if packed:
                from .archive_repack import read_packed
                return read_packed(conn, packed, revision, row[0], block_cache)
        has_cas = conn.execute("SELECT 1 FROM sqlite_master WHERE name='cas_records'").fetchone()
        cas = conn.execute('SELECT root_hash,format FROM cas_records WHERE kind=? AND record_key=? AND revision=?',
                           (kind, key, revision)).fetchone() if has_cas else None
        if cas:
            descriptor = _decode(json.loads(zlib.decompress(row[0])))
            if descriptor != {'root': cas[0], 'format': cas[1]}:
                raise RuntimeError('Archive checksum mismatch; restore refused')
            def load(digest):
                record = conn.execute('SELECT payload FROM archive_blobs WHERE digest=?', (digest,)).fetchone()
                if record is None:
                    raise RuntimeError('Missing archive blob; restore unavailable')
                data = zlib.decompress(record[0])
                if hashlib.sha256(data).hexdigest() != digest:
                    raise RuntimeError('Archive blob checksum mismatch; restore refused')
                return _decode(json.loads(data))
            value = load(cas[0])
            if cas[1] == 2:
                def expand(manifest):
                    return {**manifest['scalars'], **{name: [load(ref) if isinstance(ref, str)
                            else ref['inline'] for ref in refs] for name, refs in manifest['collections'].items()},
                            **{name: expand(load(ref)) for name, ref in manifest.get('objects', {}).items()}}
                value = expand(value)
            elif cas[1] != 1:
                raise RuntimeError('Unsupported archive format; restore refused')
            raw = canonical_bytes(value)
        else:
            raw = zlib.decompress(row[0])
            value = _decode(json.loads(raw))
        if hashlib.sha256(raw).hexdigest() != revision:
            raise RuntimeError('Archive checksum mismatch; restore refused')
        return value

    @staticmethod
    def read(locator: dict):
        path = Path(locator['archive_path'])
        conn = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)
        try:
            value = ArchiveStore._read_record(conn, locator['kind'], locator['key'], locator['revision'])
            if 'member' in locator:
                if locator['kind'] != 'payload_batch':
                    raise RuntimeError('Invalid archive member selector')
                member = value.get('members', {}).get(locator['member'])
                if (member is None or content_hash(member) != locator.get('member_hash')
                        or str(member.get('id')) != locator['member'].partition(':')[2]):
                    raise RuntimeError('Archive member checksum mismatch')
                return member
            return value
        finally:
            conn.close()

    def find_documents(self, query: str) -> list[tuple[str, str]]:
        if not self.path.exists():
            return []
        conn = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        try:
            if len(query) >= 3 and '%' not in query and '_' not in query:
                phrase = '"' + query.replace('"', '""') + '"'
                return conn.execute(
                    """SELECT r.record_key,r.revision FROM document_search f
                       JOIN records r ON r.rowid=f.rowid
                       WHERE document_search MATCH ? AND r.kind='document'""", (phrase,)
                ).fetchall()
            # Trigrams cannot index short queries. Preserve substring search
            # semantics by checking cold records rather than silently missing them.
            found = []
            for key, rev, payload in conn.execute(
                "SELECT record_key,revision,payload FROM records WHERE kind='document'"
            ):
                value = self._read_record(conn, 'document', key, rev)
                # Use SQLite itself for LIKE wildcard/case semantics.
                if conn.execute("SELECT ? LIKE ?", (str(value.get("content") or ""),
                                                     f"%{query}%")).fetchone()[0]:
                    found.append((key, rev))
            return found
        finally:
            conn.close()

    def find_memories(self, query: str, cutoff: str, *, kind='memory_unit', limit=40):
        if not self.path.exists():
            return []
        conn = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
        try:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='memory_index'").fetchone():
                return []
            matches = conn.execute('''SELECT i.record_key,i.revision FROM memory_index i
                JOIN records r ON r.kind=i.kind AND r.record_key=i.record_key AND r.revision=i.revision
                WHERE i.kind=? AND i.topic=? AND i.available_at<=?
                ORDER BY i.available_at DESC,r.rowid DESC LIMIT ?''',
                (kind, query, cutoff, max(0, limit))).fetchall()
            return [self._read_record(conn, kind, key, revision) for key, revision in matches]
        finally:
            conn.close()

    def latest_memory_time(self, round_id):
        if not self.path.exists():
            return None
        conn = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
        try:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='memory_index'").fetchone():
                return None
            prefix = round_id+':'
            return conn.execute('''SELECT max(available_at) FROM memory_index WHERE kind='research_memory'
                AND record_key>=? AND record_key<?''', (prefix,prefix+'\uffff')).fetchone()[0]
        finally:
            conn.close()

    def migrate_legacy(self, *, limit=20, budget_seconds=2):
        """Resumable in-place conversion; every locator and original hash survives."""
        if not self.path.exists():
            return {'converted': 0, 'can_continue': False}
        conn = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
        try:
            has_cas = conn.execute("SELECT 1 FROM sqlite_master WHERE name='cas_records'").fetchone()
            clause = ('WHERE NOT EXISTS(SELECT 1 FROM cas_records c WHERE c.kind=r.kind '
                      'AND c.record_key=r.record_key AND c.revision=r.revision)') if has_cas else ''
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='packed_records'").fetchone():
                clause += (' AND ' if clause else 'WHERE ') + ('NOT EXISTS(SELECT 1 FROM packed_records p '
                    'WHERE p.kind=r.kind AND p.record_key=r.record_key AND p.revision=r.revision)')
            selected = conn.execute('SELECT kind,record_key,revision FROM records r '+clause+
                " ORDER BY (r.kind='round') DESC,(r.kind='research_memory') DESC,r.rowid LIMIT ?",
                                    (max(1, limit),)).fetchall()
        finally:
            conn.close()
        deadline = time.monotonic()+budget_seconds
        converted = 0
        for kind, key, revision in selected:
            if time.monotonic() >= deadline:
                break
            locator = {'archive_path': str(self.path), 'kind': kind, 'key': key, 'revision': revision}
            value = self.read(locator)
            self._put(kind, key, value, rewrite_legacy=True)
            converted += 1
        return {'converted': converted, 'can_continue': converted<len(selected) or len(selected)==limit}

    def build_dense_copy(self, target_path):
        """Build and verify a dense generation; caller excludes archive writers.

        Record rowids survive so FTS references and version ordering stay valid.
        Large compressed blobs use rowid pages rather than small-key B-tree pages.
        The source file is never modified, and no VACUUM runs on the live DB.
        """
        target_path = Path(target_path).resolve()
        if target_path.exists():
            raise FileExistsError(target_path)
        source = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
        target = sqlite3.connect(target_path)
        success = False
        try:
            source.execute('BEGIN')
            schema = {name: sql for name, sql in source.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='table'")}
            owned = {'records','archive_blobs','cas_records','memory_index','document_search',
                     'packed_records','packed_blocks'}
            if any(name not in owned and not name.startswith(('sqlite_', 'document_search_')) for name in schema):
                raise RuntimeError('Unknown archive table; dense replacement refused')
            target.execute('PRAGMA auto_vacuum=INCREMENTAL')
            target.execute('PRAGMA synchronous=FULL')
            target.execute(f'PRAGMA max_page_count={max(1,self.max_bytes//4096)}')
            for name in ('records','archive_blobs','cas_records','memory_index','packed_blocks','packed_records'):
                if name not in schema:
                    continue
                sql = schema[name]
                if name == 'archive_blobs':
                    sql = re.sub(r'\s+WITHOUT\s+ROWID\s*$', '', sql, flags=re.I)
                target.execute(sql)
            target.execute("CREATE VIRTUAL TABLE document_search USING fts5(content,content='',tokenize='trigram')")
            target.execute('BEGIN IMMEDIATE')
            for name in ('records','archive_blobs','cas_records','memory_index','packed_blocks','packed_records'):
                if name not in schema:
                    continue
                fields = [r[1] for r in source.execute(f'PRAGMA table_info("{name}")')]
                if name == 'records':
                    fields.insert(0, 'rowid')
                columns = ','.join('"'+field+'"' for field in fields)
                target.executemany(f'INSERT INTO "{name}"({columns}) VALUES ({",".join("?" for _ in fields)})',
                                   source.execute(f'SELECT {columns} FROM "{name}"'))
            block_cache = {}
            for kind,key,revision,rowid in target.execute('SELECT kind,record_key,revision,rowid FROM records'):
                value = self._read_record(target,kind,key,revision,block_cache=block_cache)
                if kind == 'document':
                    target.execute('INSERT INTO document_search(rowid,content) VALUES (?,?)',
                                   (rowid, str(value.get('content') or '')))
            for (sql,) in source.execute("SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"):
                target.execute(sql)
            target.commit()
            if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise RuntimeError('Dense archive integrity check failed')
            success = True
            return {'before_bytes': self.path.stat().st_size, 'after_bytes': target_path.stat().st_size,
                    'records_verified': target.execute('SELECT count(*) FROM records').fetchone()[0]}
        finally:
            source.close(); target.close()
            if not success:
                target_path.unlink(missing_ok=True)

    def latest_locator(self, kind: str, key: str) -> dict | None:
        if not self.path.exists():
            return None
        conn = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT revision FROM records WHERE kind=? AND record_key=? ORDER BY rowid DESC LIMIT 1",
                (kind, key),
            ).fetchone()
            return {"archive_path": str(self.path), "kind": kind, "key": key,
                    "revision": row[0]} if row else None
        finally:
            conn.close()


async def ensure_storage_schema(conn):
    # execute individually: executescript would commit a caller's transaction.
    statements = [
        """CREATE TABLE IF NOT EXISTS archived_payloads(
            kind TEXT NOT NULL, record_id TEXT NOT NULL, locator_json TEXT NOT NULL,
            hot_evicted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(kind,record_id))""",
        """CREATE TABLE IF NOT EXISTS storage_jobs(
            kind TEXT NOT NULL, record_id TEXT NOT NULL, policy_version TEXT NOT NULL,
            state TEXT NOT NULL, manifest_hash TEXT, locator_json TEXT,
            updated_at TEXT NOT NULL, error TEXT,
            PRIMARY KEY(kind,record_id,policy_version))""",
        """CREATE TABLE IF NOT EXISTS research_memories(
            round_id TEXT NOT NULL, source_hash TEXT NOT NULL, topic TEXT NOT NULL,
            available_at TEXT NOT NULL, completed_at TEXT NOT NULL,
            status TEXT NOT NULL, memory_json TEXT NOT NULL,
            PRIMARY KEY(round_id,source_hash))""",
        """CREATE INDEX IF NOT EXISTS idx_research_memories_topic
            ON research_memories(topic,available_at DESC)""",
        """CREATE TABLE IF NOT EXISTS storage_pins(
            kind TEXT NOT NULL, record_id TEXT NOT NULL, reason TEXT NOT NULL,
            PRIMARY KEY(kind,record_id))""",
    ]
    for sql in statements:
        await conn.execute(sql)
    from .bounded_memory import ensure_memory_schema
    await ensure_memory_schema(conn)


async def hydrate_payload(conn, kind: str, row: dict, field: str) -> dict:
    import asyncio
    if row.get(field):
        return row
    async with conn.execute(
        "SELECT locator_json FROM archived_payloads "
        "WHERE kind=? AND record_id=? AND hot_evicted=1", (kind, str(row["id"])),
    ) as cursor:
        archived = await cursor.fetchone()
    if archived:
        original = await asyncio.to_thread(ArchiveStore.read, json.loads(archived[0]))
        row[field] = original.get(field)
        if kind == "document":
            row["embedding"] = original.get("embedding")
    return row


async def search_cold_documents(conn, query: str, *, sector=None, limit=100) -> list[dict]:
    """Search archived bodies while accepting only the currently active revision."""
    import asyncio
    async with conn.execute(
        "SELECT DISTINCT json_extract(locator_json,'$.archive_path') FROM archived_payloads "
        "WHERE kind='document' AND hot_evicted=1"
    ) as cursor:
        paths = [r[0] for r in await cursor.fetchall()]
    found = []
    for path in paths:
        matches = await asyncio.to_thread(ArchiveStore(path).find_documents, query)
        for start in range(0, len(matches), 300):
            batch = matches[start:start+300]
            sql = "WITH matches(id,revision) AS (VALUES " + ",".join("(?,?)" for _ in batch) + ") "
            sql += """SELECT d.* FROM matches m JOIN documents d ON d.id=m.id
                JOIN archived_payloads a ON a.kind='document' AND a.record_id=d.id
                WHERE d.content IS NULL AND a.hot_evicted=1
                AND json_extract(a.locator_json,'$.revision')=m.revision
                AND json_extract(a.locator_json,'$.archive_path')=?
                AND COALESCE(d.source,'')<>'obsidian_wiki'"""
            params = [value for pair in batch for value in pair] + [path]
            if sector:
                sql += " AND d.sector=?"
                params.append(sector)
            sql += " ORDER BY d.created_at DESC LIMIT ?"
            params.append(max(0, limit))
            async with conn.execute(sql, params) as cursor:
                found.extend(dict(r) for r in await cursor.fetchall())
    return sorted(found, key=lambda r: str(r.get("created_at") or ""), reverse=True)[:max(0, limit)]


def consistent_backup(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        raise FileExistsError(target_path)
    source = sqlite3.connect(source_path.resolve().as_uri() + "?mode=ro", uri=True)
    target = sqlite3.connect(target_path)
    try:
        source.backup(target)
        if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("Backup integrity check failed")
    finally:
        target.close()
        source.close()
