"""Lossless online generations for the Wiki's immutable evidence archive.

Related revisions share a bounded compression window. Locators, record rowids,
original bytes and revision hashes survive; no evidence is expired or summarized.
Only the independent archive is replaced, never the live primary database.
"""
from __future__ import annotations

import hashlib
import json
import logging
import lzma
import os
from pathlib import Path
import shutil
import sqlite3
import threading
import time
import zlib
from contextlib import closing

from .storage_archive import ArchiveStore, _decode, archive_write_lock, canonical_bytes

logger = logging.getLogger(__name__)
BLOCK_BYTES = 4 * 1024**2
MAX_BLOCK_BYTES = 64 * 1024**2
OWNED = {'records', 'archive_blobs', 'cas_records', 'memory_index',
         'document_search', 'packed_blocks', 'packed_records'}


def _unpack(payload, size):
    if not 0 <= size <= MAX_BLOCK_BYTES:
        raise RuntimeError('Invalid packed archive block size')
    decoder = lzma.LZMADecompressor(memlimit=128 * 1024**2)
    raw = decoder.decompress(payload, max_length=size + 1)
    if len(raw) != size or not decoder.eof or decoder.unused_data:
        raise RuntimeError('Packed archive block size mismatch')
    return raw


def read_packed(conn, entry, revision, descriptor, cache=None):
    digest, offset, size = entry
    if json.loads(zlib.decompress(descriptor)) != {'block': digest, 'format': 3}:
        raise RuntimeError('Packed archive descriptor checksum mismatch')
    cache = {} if cache is None else cache
    if digest not in cache:
        row = conn.execute('SELECT payload,raw_bytes FROM packed_blocks WHERE digest=?', (digest,)).fetchone()
        if row is None:
            raise RuntimeError('Missing packed archive block')
        raw = _unpack(*row)
        if hashlib.sha256(raw).hexdigest() != digest:
            raise RuntimeError('Packed archive block checksum mismatch')
        cache.clear()  # One bounded block; never cache the whole archive.
        cache[digest] = raw
    if offset < 0 or size < 0:
        raise RuntimeError('Invalid packed archive member range')
    raw = cache[digest][offset:offset + size]
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != revision:
        raise RuntimeError('Packed archive member checksum mismatch')
    return _decode(json.loads(raw))


def _fingerprint(path):
    stat = path.stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _cancelled(stop):
    if stop is not None and stop.is_set():
        raise InterruptedError('Archive repacking cancelled; original retained')


def build_packed_copy(store, target_path, *, stop=None, block_bytes=BLOCK_BYTES):
    """Build a verified generation without mutating the source. Caller locks writers."""
    target_path = Path(target_path)
    if target_path.exists():
        raise FileExistsError(target_path)
    source = sqlite3.connect(store.path.as_uri() + '?mode=ro', uri=True)
    target = sqlite3.connect(target_path)
    success = False
    started = time.monotonic()
    try:
        source.execute('BEGIN')
        schema = dict(source.execute("SELECT name,sql FROM sqlite_master WHERE type='table'"))
        if any(name not in OWNED and not name.startswith(('sqlite_', 'document_search_')) for name in schema):
            raise RuntimeError('Unknown archive table; packed replacement refused')
        if source.execute("SELECT 1 FROM sqlite_master WHERE type IN ('trigger','view')").fetchone():
            raise RuntimeError('Unknown archive behavior; packed replacement refused')
        if [r[1] for r in source.execute('PRAGMA table_info(records)')] != [
                'kind', 'record_key', 'revision', 'payload', 'raw_bytes']:
            raise RuntimeError('Unknown archive record schema')
        target.execute('PRAGMA synchronous=FULL')
        target.execute('PRAGMA auto_vacuum=INCREMENTAL')
        target.execute(f'PRAGMA max_page_count={max(1, store.max_bytes // 4096)}').fetchall()
        for name in ('records', 'archive_blobs', 'cas_records', 'memory_index'):
            if name in schema:
                target.execute(schema[name])
        target.executescript('''
            CREATE TABLE packed_blocks(digest TEXT PRIMARY KEY, payload BLOB NOT NULL, raw_bytes INTEGER NOT NULL);
            CREATE TABLE packed_records(kind TEXT NOT NULL, record_key TEXT NOT NULL, revision TEXT NOT NULL,
                block_hash TEXT NOT NULL, raw_offset INTEGER NOT NULL, raw_bytes INTEGER NOT NULL,
                PRIMARY KEY(kind,record_key,revision)) WITHOUT ROWID;
            CREATE VIRTUAL TABLE document_search USING fts5(content,content='',tokenize='trigram');
        ''')
        target.execute('BEGIN IMMEDIATE')
        batch, batch_size, count, cache = [], 0, 0, {}

        def flush():
            if not batch:
                return
            _cancelled(stop)
            raw = b''.join(item[-1] for item in batch)
            digest = hashlib.sha256(raw).hexdigest()
            packed = lzma.compress(raw, preset=6)
            if _unpack(packed, len(raw)) != raw:
                raise RuntimeError('Packed archive compression verification failed')
            target.execute('INSERT OR IGNORE INTO packed_blocks VALUES(?,?,?)', (digest, packed, len(raw)))
            offset = 0
            descriptor = zlib.compress(canonical_bytes({'block': digest, 'format': 3}), 9)
            for rowid, kind, key, revision, member in batch:
                target.execute('INSERT INTO records(rowid,kind,record_key,revision,payload,raw_bytes) VALUES(?,?,?,?,?,?)',
                               (rowid, kind, key, revision, descriptor, len(member)))
                target.execute('INSERT INTO packed_records VALUES(?,?,?,?,?,?)',
                               (kind, key, revision, digest, offset, len(member)))
                offset += len(member)
            batch.clear()

        last_notice = time.monotonic()
        for rowid, kind, key, revision in source.execute(
                'SELECT rowid,kind,record_key,revision FROM records ORDER BY kind,record_key,rowid'):
            _cancelled(stop)
            value = store._read_record(source, kind, key, revision, block_cache=cache)
            raw = canonical_bytes(value)
            if len(raw) > MAX_BLOCK_BYTES:
                raise RuntimeError('Archive member exceeds bounded compression window; original retained')
            if batch_size + len(raw) > block_bytes and batch:
                flush()
                batch_size = 0
            batch.append((rowid, kind, key, revision, raw))
            batch_size += len(raw)
            count += 1
            if kind == 'document':
                target.execute('INSERT INTO document_search(rowid,content) VALUES(?,?)',
                               (rowid, str(value.get('content') or '')))
            if time.monotonic() - last_notice >= 15:
                logger.info('WIKI_ARCHIVE_REPACK phase=building records=%s elapsed_seconds=%.1f',
                            count, time.monotonic() - started)
                last_notice = time.monotonic()
        flush()
        if 'memory_index' in schema:
            target.executemany('INSERT INTO memory_index VALUES(?,?,?,?,?,?)',
                               source.execute('SELECT * FROM memory_index'))
        for (sql,) in source.execute("SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"):
            target.execute(sql)
        target.commit()
        # Check every committed member through the ordinary restore path. A
        # compression round trip alone would miss broken pointers or offsets.
        cache = {}
        verified = 0
        for kind, key, revision in target.execute(
                'SELECT kind,record_key,revision FROM packed_records ORDER BY block_hash,raw_offset'):
            _cancelled(stop)
            store._read_record(target, kind, key, revision, block_cache=cache)
            verified += 1
        expected = source.execute('SELECT count(*) FROM records').fetchone()[0]
        if verified != expected or target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('Packed archive verification incomplete; original retained')
        success = True
        return {'records_verified': verified, 'before_bytes': store.path.stat().st_size,
                'after_bytes': target_path.stat().st_size,
                'elapsed_seconds': round(time.monotonic() - started, 3)}
    finally:
        source.close()
        target.close()
        if not success:
            target_path.unlink(missing_ok=True)


def repack_archive(path, *, max_bytes, staging_root, stop=None, minimum_raw_bytes=16 * 1024**2):
    """Publish only a fully verified, smaller generation under the writer lock."""
    path = Path(path).resolve()
    if not path.exists():
        return {'status': 'not_applicable', 'reclaimed_bytes': 0}
    with archive_write_lock(path):
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as source:
            has_packed = source.execute("SELECT 1 FROM sqlite_master WHERE name='packed_records'").fetchone()
            clause = (' WHERE NOT EXISTS(SELECT 1 FROM packed_records p WHERE p.kind=r.kind '
                      'AND p.record_key=r.record_key AND p.revision=r.revision)') if has_packed else ''
            raw_bytes = source.execute('SELECT coalesce(sum(raw_bytes),0) FROM records r' + clause).fetchone()[0]
        staging = Path(staging_root)
        candidate = staging / (hashlib.sha256(str(path).encode()).hexdigest()[:24] + '.sqlite3')
        # Reap crash leftovers even if the completed source no longer needs
        # repacking. Otherwise an ineligible archive can pin staging forever.
        if staging.exists():
            if staging.stat().st_dev != path.stat().st_dev:
                raise RuntimeError('Archive staging must be on the same filesystem')
            candidate.unlink(missing_ok=True)
            Path(str(candidate) + '-journal').unlink(missing_ok=True)
        if raw_bytes < minimum_raw_bytes:
            return {'status': 'complete', 'reclaimed_bytes': 0, 'unpacked_raw_bytes': raw_bytes}
        staging.mkdir(parents=True, exist_ok=True)
        # A fixed per-archive staging file is safe to clean only under its writer
        # lock, including after a crash. Staging is included in managed inventory.
        if staging.stat().st_dev != path.stat().st_dev:
            raise RuntimeError('Archive staging must be on the same filesystem')
        candidate.unlink(missing_ok=True)
        Path(str(candidate) + '-journal').unlink(missing_ok=True)
        if shutil.disk_usage(staging).free < max_bytes + 64 * 1024**2:
            raise RuntimeError('Insufficient temporary space for verified archive generation')
        original = _fingerprint(path)
        try:
            report = build_packed_copy(ArchiveStore(path, max_bytes), candidate, stop=stop)
            _cancelled(stop)
            if _fingerprint(path) != original or Path(str(path) + '-wal').exists():
                raise RuntimeError('Archive changed during repacking; replacement refused')
            if report['after_bytes'] >= report['before_bytes']:
                return {**report, 'status': 'no_savings', 'reclaimed_bytes': 0}
            with candidate.open('rb') as handle:
                os.fsync(handle.fileno())
            os.replace(candidate, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return {**report, 'status': 'completed',
                    'reclaimed_bytes': report['before_bytes'] - report['after_bytes']}
        finally:
            candidate.unlink(missing_ok=True)
            Path(str(candidate) + '-journal').unlink(missing_ok=True)


async def repack_archive_async(*args, **kwargs):
    """Cancellation joins the worker before the governor releases writer ownership."""
    from .storage_worker import storage_worker
    stop = threading.Event()
    return await storage_worker(repack_archive, *args, stop=stop, on_cancel=stop.set, **kwargs)
