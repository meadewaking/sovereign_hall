"""Lossless Wiki payload packing; derived excerpts never authorize deletion."""
from __future__ import annotations

from datetime import date, timedelta
import os
import time
from pathlib import Path

from .storage_archive import ArchiveStore
from .storage_governor import directory_size, allocated_size
from .wiki_knowledge import dump_markdown, parse_frontmatter


def wiki_archive(root: Path, *, max_bytes=1024**3) -> ArchiveStore:
    return ArchiveStore(root / '.archives' / 'wiki.sqlite3', max_bytes)


def read_raw(root: Path, relative_path: str) -> bytes:
    relative = Path(relative_path)
    if relative.is_absolute() or '..' in relative.parts or relative.parts[:1] != ('raw',):
        raise ValueError('Expected a relative raw/ path inside the Wiki')
    path = root / relative
    if path.exists():
        return path.read_bytes()
    source = root / 'wiki' / 'sources' / relative.name
    from .wiki_source_archive import read_source
    try:
        content = read_source(root, source.relative_to(root).as_posix())
    except FileNotFoundError:
        raise FileNotFoundError(path)
    front, _ = parse_frontmatter(content.decode('utf-8'))
    locator = front.get('raw_archive')
    if not isinstance(locator, dict) or locator.get('key') != relative.as_posix():
        raise FileNotFoundError(path)
    return ArchiveStore.read(locator)['content']


def pack_raw(root: Path, *, retention_days=30, limit=100, max_bytes=1024**3,
             target_bytes: int | None = None, progress=None, budget_seconds=None,
             used_bytes: int | None = None, source_updated=None,
             after_name: str | None = None, include_recent: bool = False) -> dict:
    """Caller must stop Wiki writers. Pointer is durable before unlinking raw."""
    archive = wiki_archive(root, max_bytes=max_bytes)
    cutoff = date.today() - timedelta(days=retention_days)
    result = {'archived': 0, 'raw_removed': 0, 'retained_changed': 0}
    used = directory_size(root) if used_bytes is None else used_bytes
    result['before_bytes'] = used
    result.update(scanned=0, scan_complete=False, next_after=after_name)
    paths = sorted((root / 'raw').glob('*.md'))
    # Directory enumeration is separate from the payload-processing budget.
    # Otherwise a slow listing can consume the entire budget before any work.
    deadline = time.monotonic() + budget_seconds if budget_seconds is not None else float('inf')
    for path in paths:
        if after_name is not None and path.name <= after_name:
            continue
        if result['archived'] >= limit or (target_bytes is not None and used <= target_bytes):
            break
        if time.monotonic() >= deadline:
            result['budget_exhausted'] = True
            break
        result['scanned'] += 1
        result['next_after'] = path.name
        try:
            if path.is_symlink() or path.stat().st_size > 8*1024**2:
                result['retained_oversized'] = result.get('retained_oversized', 0) + 1
                continue
            original = path.read_bytes()
            front, _ = parse_frontmatter(original.decode('utf-8'))
            try:
                created = date.fromisoformat(str(front.get('created') or '')[:10])
            except ValueError:
                continue  # Unknown age is not evidence of eligibility.
            source = root / 'wiki' / 'sources' / path.name
            # Under pressure only the hot-copy age rule is relaxed. Known age,
            # a completed source page, verified originals and durable pointers
            # remain mandatory. Future dates remain ineligible.
            if (created > date.today() or (created >= cutoff and not include_recent)
                    or not source.is_file()):
                continue
            old_size = allocated_size(archive.path.stat()) if archive.path.exists() else 0
            archive.max_bytes = min(max_bytes, old_size + max(0, max_bytes - used) - 8192)
            locator = archive.put('wiki_raw', path.relative_to(root).as_posix(), {'content': original})
            used += allocated_size(archive.path.stat()) - old_size
            result['archived'] += 1
            source_size = allocated_size(source.stat())
            original_source = source.read_bytes()
            source_front, source_body = parse_frontmatter(original_source.decode('utf-8'))
            source_front['raw_archive'] = locator
            pointer = dump_markdown(source_front, source_body)
            temporary = source.with_name(f'.{source.name}.{os.getpid()}.tmp')
            try:
                with temporary.open('w', encoding='utf-8') as out:
                    out.write(pointer)
                    out.flush()
                    os.fsync(out.fileno())
                if source.read_bytes() != original_source:
                    result['retained_changed'] += 1
                    used += allocated_size(source.stat()) - source_size
                    continue
                temporary.replace(source)
                # The directory entry must be durable before removing the raw file.
                directory_fd = os.open(source.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                temporary.unlink(missing_ok=True)
            used += allocated_size(source.stat()) - source_size
            if source_updated:
                source_updated(source, *parse_frontmatter(pointer))
            if read_raw(root, path.relative_to(root).as_posix()) != original:
                raise RuntimeError('Wiki raw verification failed; raw retained')
            # Compare both the restored archive and the current source before unlink.
            if ArchiveStore.read(locator)['content'] != original:
                raise RuntimeError('Wiki archive verification failed; raw retained')
            if path.read_bytes() != original:
                result['retained_changed'] += 1
                continue
            released = allocated_size(path.stat())
            path.unlink()
            result['raw_removed'] += 1
            used -= released
            if progress and result['raw_removed'] % 500 == 0:
                progress({**result, 'after_bytes': used})
        except Exception as exc:
            # Retain this original, advance the bounded scan and let other
            # independent originals release capacity. Never hide the failure.
            result['retained_errors'] = result.get('retained_errors', 0) + 1
            errors = result.setdefault('errors', [])
            if len(errors) < 10:
                errors.append({'path': path.name, 'error': str(exc)})
            # A committed archive or pointer may have grown before the error.
            # Do not grant subsequent writes headroom from a stale estimate.
            used = directory_size(root)
    else:
        result['scan_complete'] = True
        result['next_after'] = None
    result['after_bytes'] = used
    result['reclaimed_bytes'] = max(0, result['before_bytes'] - used)
    return result
