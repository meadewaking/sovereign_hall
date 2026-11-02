"""Dense, lossless storage for cold source pages, with stable Wiki paths.

The caller serializes Wiki writers. A FULL-synchronous transaction and a
committed read-back precede every unlink. Old revisions remain recoverable.
"""
from __future__ import annotations

from datetime import date, timedelta
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import time
import zlib


def source_key(relative_path: str) -> str:
    path = Path(relative_path)
    if (path.is_absolute() or '..' in path.parts or len(path.parts) != 3
            or path.parts[:2] != ('wiki', 'sources') or path.suffix != '.md'):
        raise ValueError('Expected a wiki/sources/*.md path')
    return path.as_posix()


class SourcePageArchive:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.path = self.root / '.archives' / 'source_pages.sqlite3'

    def _reader(self):
        return sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)

    @staticmethod
    def _decode(row) -> bytes:
        packed, digest, size = row
        raw = zlib.decompress(packed)
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
            raise RuntimeError('Source page checksum mismatch')
        return raw

    def _page_bytes(self, conn, row, cache) -> bytes:
        block_id, offset, size, digest = row
        if block_id not in cache:
            block = conn.execute('SELECT payload,sha256,raw_bytes FROM source_blocks WHERE id=?',
                                 (block_id,)).fetchone()
            if block is None:
                raise RuntimeError('Source page block missing')
            cache.clear()  # One bounded batch of source text, never the whole vault.
            cache[block_id] = self._decode(block)
        raw = cache[block_id][offset:offset+size]
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
            raise RuntimeError('Source page checksum mismatch')
        return raw

    def _read(self, conn, key, cache, revision=None):
        if revision is None:
            row = conn.execute('''SELECT p.block_id,p.raw_offset,p.raw_bytes,p.sha256 FROM current_sources c
                JOIN source_versions p ON p.id=c.version_id WHERE c.path=?''', (key,)).fetchone()
        else:
            row = conn.execute('''SELECT block_id,raw_offset,raw_bytes,sha256 FROM source_versions
                WHERE path=? AND sha256=?''', (key, revision)).fetchone()
        if row is None:
            raise FileNotFoundError(key)
        return self._page_bytes(conn, row, cache)

    def read(self, relative_path: str, *, revision=None) -> bytes:
        key = source_key(relative_path)
        if not self.path.exists():
            raise FileNotFoundError(key)
        with closing(self._reader()) as conn:
            return self._read(conn, key, {}, revision)

    def verify_batch(self, entries):
        with closing(self._reader()) as conn:
            cache = {}
            for key, raw, _ in entries:
                if self._read(conn, key, cache) != raw:
                    raise RuntimeError('Committed source page verification failed; hot pages retained')

    def contains(self, relative_path: str) -> bool:
        key = source_key(relative_path)
        if not self.path.exists():
            return False
        with closing(self._reader()) as conn:
            return conn.execute('SELECT 1 FROM current_sources WHERE path=?', (key,)).fetchone() is not None

    def iter_pages(self):
        if not self.path.exists():
            return
        with closing(self._reader()) as conn:
            cache = {}
            for key, *row in conn.execute('''SELECT c.path,p.block_id,p.raw_offset,p.raw_bytes,p.sha256
                FROM current_sources c JOIN source_versions p ON p.id=c.version_id ORDER BY c.path'''):
                yield source_key(key), self._page_bytes(conn, row, cache)

    def references(self):
        if not self.path.exists():
            return
        with closing(self._reader()) as conn:
            yield from conn.execute('''SELECT p.source_id,p.url FROM current_sources c
                JOIN source_versions p ON p.id=c.version_id''')

    def put_batch(self, entries, *, max_bytes: int):
        """Publish exact bytes and metadata atomically; never overwrite a revision."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        committed = False
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            conn.execute('PRAGMA synchronous=FULL')
            conn.execute(f'PRAGMA max_page_count={max(1, max_bytes // 4096)}')
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS source_blocks(
                    id INTEGER PRIMARY KEY, payload BLOB NOT NULL,
                    sha256 TEXT NOT NULL, raw_bytes INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS source_versions(
                    id INTEGER PRIMARY KEY, path TEXT NOT NULL, sha256 TEXT NOT NULL,
                    block_id INTEGER NOT NULL, raw_offset INTEGER NOT NULL, raw_bytes INTEGER NOT NULL,
                    source_id TEXT, url TEXT, UNIQUE(path,sha256));
                CREATE TABLE IF NOT EXISTS current_sources(
                    path TEXT PRIMARY KEY, version_id INTEGER NOT NULL
                ) WITHOUT ROWID;
            ''')
            conn.execute('BEGIN IMMEDIATE')
            pending, known = [], {}
            for key, raw, front in entries:
                key = source_key(key)
                digest = hashlib.sha256(raw).hexdigest()
                row = conn.execute('SELECT id FROM source_versions WHERE path=? AND sha256=?', (key, digest)).fetchone()
                if row:
                    known[key] = row[0]
                else:
                    pending.append((key, raw, front, digest))
            if pending:
                combined = b''.join(item[1] for item in pending)
                packed = zlib.compress(combined, 9)
                digest = hashlib.sha256(combined).hexdigest()
                if self._decode((packed, digest, len(combined))) != combined:
                    raise RuntimeError('Source page compression verification failed')
                block_id = conn.execute('INSERT INTO source_blocks(payload,sha256,raw_bytes) VALUES(?,?,?)',
                    (packed, digest, len(combined))).lastrowid
                offset = 0
                for key, raw, front, digest in pending:
                    known[key] = conn.execute('''INSERT INTO source_versions
                        (path,sha256,block_id,raw_offset,raw_bytes,source_id,url) VALUES(?,?,?,?,?,?,?)''',
                        (key, digest, block_id, offset, len(raw), str(front.get('source_id') or ''),
                         str(front.get('url') or ''))).lastrowid
                    offset += len(raw)
            for key, version_id in known.items():
                conn.execute('INSERT OR REPLACE INTO current_sources VALUES(?,?)', (key, version_id))
            cache = {}
            for key, raw, _ in entries:
                if self._read(conn, key, cache) != raw:
                    raise RuntimeError('Source page archive verification failed')
            conn.commit()
            committed = True
        finally:
            conn.close()
            if not existed and not committed:
                self.path.unlink(missing_ok=True)
        # Make creation of the archive's directory entry durable too.
        handle = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)


def read_source(root: Path, relative_path: str) -> bytes:
    key = source_key(relative_path)
    path = root / key
    return path.read_bytes() if path.is_file() else SourcePageArchive(root).read(key)


def pack_sources(root: Path, *, max_bytes: int, used_bytes: int, target_bytes: int,
                 limit=500, budget_seconds=5, retention_days=30, include_recent=False,
                 after_name: str | None = None) -> dict:
    """Pack a bounded batch while retaining every byte and the original paths."""
    from .storage_governor import allocated_size
    from .wiki_knowledge import parse_frontmatter

    archive = SourcePageArchive(root)
    before_archive = allocated_size(archive.path.stat()) if archive.path.exists() else 0
    report = dict(pages_removed=0, retained_changed=0, scanned=0, scan_complete=False,
                  next_after=after_name, before_bytes=used_bytes, after_bytes=used_bytes, reclaimed_bytes=0)
    if used_bytes <= target_bytes:
        report['scan_complete'] = True
        return report
    paths = sorted((root / 'wiki' / 'sources').glob('*.md'))
    cutoff = date.today() - timedelta(days=retention_days)
    deadline = time.monotonic() + budget_seconds
    entries, files = [], []
    # Preserve enough headroom for the rollback journal and new B-tree pages.
    headroom = max(0, max_bytes - used_bytes - 65536) // 2
    planned = 32768
    batch_bytes = 0
    for path in paths:
        if after_name is not None and path.name <= after_name:
            continue
        if len(entries) >= limit or time.monotonic() >= deadline:
            break
        try:
            if path.is_symlink() or (root / 'raw' / path.name).exists():
                report['next_after'] = path.name
                report['scanned'] += 1
                continue  # Raw packing must finish before moving its source pointer.
            size = path.stat().st_size
            if size > 8*1024**2:
                report['retained_oversized'] = report.get('retained_oversized', 0) + 1
                report['next_after'] = path.name
                report['scanned'] += 1
                continue
            if batch_bytes + size > 8*1024**2:
                break
            raw = path.read_bytes()
            front, _ = parse_frontmatter(raw.decode('utf-8'))
            try:
                created = date.fromisoformat(str(front.get('created') or '')[:10])
            except ValueError:
                created = None
            if (created is not None and created <= date.today()
                    and (include_recent or created < cutoff)
                    and front.get('type') == 'source' and (front.get('source_id') or front.get('url'))):
                cost = len(zlib.compress(raw, 9)) + 4 * len(path.name.encode()) + 4096
                if planned + cost > headroom:
                    report['capacity_limited'] = True
                    break  # Retry this page after the preceding batch releases space.
                planned += cost
                entries.append((path.relative_to(root).as_posix(), raw, front))
                batch_bytes += len(raw)
                files.append((path, allocated_size(path.stat())))
            report['next_after'] = path.name
            report['scanned'] += 1
        except Exception as exc:
            report['next_after'] = path.name
            report['scanned'] += 1
            report['retained_errors'] = report.get('retained_errors', 0) + 1
            errors = report.setdefault('errors', [])
            if len(errors) < 10:
                errors.append({'path': path.name, 'error': str(exc)})
    else:
        report.update(scan_complete=True, next_after=None)
    if entries:
        archive.put_batch(entries, max_bytes=before_archive + headroom)
        # Read the committed generation before removing any hot file. Failure
        # leaves the complete batch hot, even if an earlier member verified.
        archive.verify_batch(entries)
        removed = 0
        for (key, raw, _), (path, size) in zip(entries, files):
            if path.read_bytes() != raw:
                report['retained_changed'] += 1
                continue
            path.unlink()
            removed += size
            report['pages_removed'] += 1
        handle = os.open(root / 'wiki' / 'sources', os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        report['after_bytes'] = used_bytes - removed + allocated_size(archive.path.stat()) - before_archive
        report['reclaimed_bytes'] = max(0, used_bytes - report['after_bytes'])
    return report
