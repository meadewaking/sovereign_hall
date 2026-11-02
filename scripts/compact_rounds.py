#!/usr/bin/env python3
"""Bounded, resumable lossless research archival. Default: read-only preview.

--apply archives complete records and extracts versioned research memories.
--evict-hot additionally clears eligible large payloads, keeping all row IDs,
events, predictions, decisions and document links. No importance-score deletion.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sqlite3
import sys
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.research_memory import ResearchCompactor, POLICY_VERSION, utc_time
from sovereign_hall.services.storage_archive import ArchiveStore, consistent_backup, MaintenanceLock
from sovereign_hall.services.storage_governor import database_bundle_size, directory_size, managed_storage_inventory
from sovereign_hall.scripts.compact_storage import live_runner_pid

DB_PATH = PROJECT_ROOT / 'data' / 'sovereign_hall.db'


def plan(db_path: Path, *, cutoff_days: int, limit: int, orphan_limit: int,
         evict_hot=False, refresh=False) -> dict:
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=cutoff_days)).isoformat()
    conn = sqlite3.connect(db_path.resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    conn.create_function('research_utc', 1, lambda v: utc_time(v) if v else None)
    try:
        version = POLICY_VERSION + (':evict' if evict_hot else ':archive')
        has_jobs = conn.execute("SELECT 1 FROM sqlite_master WHERE name='storage_jobs'").fetchone()
        skip = "" if refresh or not has_jobs else (
            "AND NOT EXISTS(SELECT 1 FROM storage_jobs j WHERE j.kind='round' "
            "AND j.record_id=research_rounds.id AND j.policy_version=? AND j.state='completed')"
        )
        rounds = [dict(r) for r in conn.execute(
            """SELECT id,base_topic,status,completed_at,
               (SELECT COUNT(*) FROM round_documents WHERE round_id=research_rounds.id) AS document_links
               FROM research_rounds
               WHERE status IN ('completed','failed') AND completed_at IS NOT NULL
               AND research_utc(completed_at) < ? """ + skip + " ORDER BY completed_at,id LIMIT ?",
            (cutoff, *([version] if skip else []), limit),
        )]
        # Indexed date prefilter, bounded batch, then exact dependency checking
        # under the writer lock before eviction. NULL bodies are already cold.
        local_cutoff = datetime.fromisoformat(cutoff).astimezone(ZoneInfo('Asia/Shanghai')).replace(tzinfo=None).isoformat()
        doc_skip = skip.replace("j.kind='round'", "j.kind='document'").replace('research_rounds.id', 'd.id')
        documents = [r[0] for r in conn.execute(
            """SELECT d.id FROM documents d WHERE COALESCE(d.round_id,'')=''
               AND d.created_at < ? AND d.content IS NOT NULL
               AND NOT EXISTS(SELECT 1 FROM round_documents rd WHERE rd.document_id=d.id)
               """ + doc_skip + " ORDER BY d.created_at,d.id LIMIT ?",
            (local_cutoff, *([version] if doc_skip else []), orphan_limit),
        )]
        return {'cutoff': cutoff, 'rounds': rounds, 'orphan_candidates': documents}
    finally:
        conn.close()


async def main_async(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path).resolve()
    archive_path = Path(args.archive_path or db_path.parent / 'storage_archives' / 'research.sqlite3')
    knowledge_root = Path(args.knowledge_root or db_path.parent / 'knowledge')
    selection = await asyncio.to_thread(plan, db_path, cutoff_days=args.cutoff_days,
                                        limit=args.limit, orphan_limit=args.orphan_limit,
                                        evict_hot=args.evict_hot, refresh=args.refresh)
    result = {'applied': args.apply, 'evict_hot': args.evict_hot, 'plan': selection,
              'before_bytes': database_bundle_size(db_path), 'reports': [], 'errors': []}
    if not args.apply:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if not selection['rounds'] and not selection['orphan_candidates']:
        result['nothing_to_do'] = True
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if db_path == DB_PATH.resolve() and live_runner_pid() is not None:
        raise RuntimeError('Stop the research loop before applying maintenance; preview remains available')
    with MaintenanceLock(db_path.parent / 'run_discussion.lock'):
        return await apply_plan(args, db_path, archive_path, knowledge_root, selection, result)


async def apply_plan(args, db_path, archive_path, knowledge_root, selection, result):
    if archive_path.resolve() == db_path:
        raise ValueError('Archive must be a separate file')
    backup_dir = Path(args.backup_dir or db_path.parent / 'storage_backups')
    existing_archives = set()
    read = sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True)
    try:
        if read.execute("SELECT 1 FROM sqlite_master WHERE name='archived_payloads'").fetchone():
            existing_archives.update(Path(r[0]) for r in read.execute(
                "SELECT DISTINCT json_extract(locator_json,'$.archive_path') FROM archived_payloads"))
    finally:
        read.close()
    if archive_path.exists():
        existing_archives.add(archive_path.resolve())
    expected_backup = db_path.stat().st_size + sum(database_bundle_size(p) for p in existing_archives)
    backup_size = directory_size(backup_dir)
    if backup_size + expected_backup > args.backup_max_bytes:
        raise RuntimeError('Backup capacity exceeded; no source records changed')
    backup_dir.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(backup_dir).free < expected_backup + 65536:
        raise RuntimeError('Insufficient space for consistent backup; no source records changed')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')
    backup = backup_dir / f'{db_path.stem}_{stamp}.db'
    print('Creating consistent backup...', file=sys.stderr, flush=True)
    await asyncio.to_thread(consistent_backup, db_path, backup)
    result['backup'] = str(backup)
    result['archive_backups'] = {}
    for index, existing in enumerate(sorted(existing_archives)):
        destination = backup_dir / f'archive_{stamp}_{index}.sqlite3'
        await asyncio.to_thread(consistent_backup, existing, destination)
        result['archive_backups'][str(existing)] = str(destination)
    (backup_dir / f'manifest_{stamp}.json').write_text(
        json.dumps({'database': str(backup), 'archives': result['archive_backups']}, indent=2), encoding='utf-8')
    db = DatabaseService(str(db_path))
    try:
        await db._init_db()
        compactor = ResearchCompactor(db, ArchiveStore(archive_path, args.archive_max_bytes), knowledge_root)
        for index, r in enumerate(selection['rounds'], 1):
            print(f"Archiving round {index}/{len(selection['rounds'])}: {r['id']}", file=sys.stderr, flush=True)
            try:
                result['reports'].append(await compactor.process_round(
                    r['id'], evict_hot=args.evict_hot, completed_before=selection['cutoff']))
            except Exception as exc:
                result['errors'].append({'round_id': r['id'], 'error': str(exc)})
        for doc_id in selection['orphan_candidates']:
            # Re-fetch the full row, including embedding, instead of a sampled
            # excerpt. The service rechecks all DB roots before clearing it.
            conn = await db._get_connection()
            async with conn.execute('SELECT * FROM documents WHERE id=?', (doc_id,)) as cursor:
                row = await cursor.fetchone()
            if row is None:
                continue
            try:
                result['reports'].append(await compactor.archive_document(
                    dict(row), evict_hot=args.evict_hot, created_before=selection['cutoff']))
            except Exception as exc:
                result['errors'].append({'document_id': doc_id, 'error': str(exc)})
        conn = await db._get_connection()
        async with conn.execute('PRAGMA incremental_vacuum(256)') as cursor:
            await cursor.fetchall()
        await (await conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')).fetchall()
        result['after_bytes'] = database_bundle_size(db_path)
        result['archive_bytes'] = database_bundle_size(archive_path)
        result['backup_bytes'] = directory_size(backup_dir)
        result['managed_storage'] = await asyncio.to_thread(managed_storage_inventory, db_path, knowledge_root)
    finally:
        # aiosqlite workers must be closed on success, failure and cancellation.
        await db.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result['errors'] else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evict-hot', action='store_true', help='clear verified, unprotected large payloads')
    parser.add_argument('--refresh', action='store_true', help='revisit completed archives after source or dependency changes')
    parser.add_argument('--db-path', type=Path, default=DB_PATH)
    parser.add_argument('--archive-path', type=Path)
    parser.add_argument('--knowledge-root', type=Path)
    parser.add_argument('--backup-dir', type=Path)
    parser.add_argument('--archive-max-bytes', type=int, default=1024**3)
    parser.add_argument('--backup-max-bytes', type=int, default=4*1024**3)
    parser.add_argument('--cutoff-days', type=int, default=60)
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--orphan-limit', type=int, default=100)
    args = parser.parse_args()
    if args.cutoff_days < 1 or args.limit < 0 or args.orphan_limit < 0:
        parser.error('Age must be positive and batch limits nonnegative')
    if args.evict_hot and not args.apply:
        parser.error('--evict-hot requires --apply')
    return asyncio.run(main_async(args))


if __name__ == '__main__':
    sys.exit(main())
