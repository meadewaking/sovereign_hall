#!/usr/bin/env python3
"""Offline first compaction for the configured 2 GiB DB / 1 GiB wiki policy.

Dry-run is the default.  ``--apply`` refuses to run while the canonical
research loop is alive, creates consistent backups, and losslessly packs old
Wiki raw files. Lineage and full original pages are retained.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sovereign_hall.core.config import get_config
from sovereign_hall.services.storage_governor import database_bundle_size, directory_size, managed_storage_inventory


DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "sovereign_hall.db"
KNOWLEDGE_ROOT = DATA_DIR / "knowledge"
LOCK_PATH = DATA_DIR / "run_discussion.lock"


def live_runner_pid() -> int | None:
    if not LOCK_PATH.exists():
        return None
    match = re.search(r"\bpid=(\d+)", LOCK_PATH.read_text(encoding="utf-8", errors="ignore"))
    if not match:
        return None
    pid = int(match.group(1))
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return None
    return pid


def oversized_lineage(conn: sqlite3.Connection, limit: int) -> list[tuple[str, int]]:
    return [
        (str(row[0]), int(row[1]))
        for row in conn.execute(
            """
            SELECT round_id, COUNT(*) AS link_count
            FROM round_documents
            GROUP BY round_id
            HAVING COUNT(*) > ?
            ORDER BY link_count DESC
            """,
            (limit,),
        )
    ]


def wiki_inventory() -> dict[str, int]:
    paths = {
        "raw": KNOWLEDGE_ROOT / "raw",
        "sources": KNOWLEDGE_ROOT / "wiki" / "sources",
        "topics": KNOWLEDGE_ROOT / "wiki" / "topics",
        "entities": KNOWLEDGE_ROOT / "wiki" / "entities",
    }
    return {
        name: sum(1 for _ in path.glob("*.md")) if path.exists() else 0
        for name, path in paths.items()
    }


def create_backups(backup_dir: Path, *, max_bytes: int | None = None) -> dict[str, str]:
    backup_dir.mkdir(parents=True, exist_ok=False)
    db_backup = backup_dir / DB_PATH.name
    from sovereign_hall.services.storage_archive import consistent_backup
    reserved = database_bundle_size(DB_PATH) + directory_size(DATA_DIR / 'storage_archives')
    remaining = max_bytes - reserved if max_bytes is not None else None
    knowledge_backup = str(backup_dir / 'knowledge.tar.gz')
    class CapacityWriter:
        def __init__(self, stream):
            self.stream, self.written = stream, 0
        def write(self, value):
            if remaining is not None and self.written + len(value) > remaining:
                raise RuntimeError('Backup capacity exceeded; source data retained')
            self.written += len(value)
            return self.stream.write(value)
        def __getattr__(self, name):
            return getattr(self.stream, name)
    print('Backing up the complete knowledge vault...', file=sys.stderr, flush=True)
    with open(knowledge_backup, 'wb') as output:
        with tarfile.open(fileobj=CapacityWriter(output), mode='w:gz') as bundle:
            bundle.add(KNOWLEDGE_ROOT, arcname=KNOWLEDGE_ROOT.name)
        output.flush()
        os.fsync(output.fileno())
    print('Backing up the database, including committed WAL...', file=sys.stderr, flush=True)
    consistent_backup(DB_PATH, db_backup)
    result = {"database": str(db_backup), "knowledge": knowledge_backup}
    for archive in sorted((DATA_DIR / "storage_archives").glob("*.sqlite3")):
        target = backup_dir / "storage_archives" / archive.name
        consistent_backup(archive, target)
        result[f"archive:{archive.name}"] = str(target)
    (backup_dir / 'manifest.json').write_text(json.dumps({'complete': True, 'files': result}, indent=2))
    return result


def invalidate_legacy_fanout(conn: sqlite3.Connection, rows: list[tuple[str, int]]) -> int:
    # This secondary lookup index is fully derivable and makes a multi-million
    # row offline purge much slower because every deleted link otherwise
    # rewrites both the primary and document-first B-trees.  The primary key
    # still resolves each round efficiently.  Rebuild in ``finally`` so even
    # an interruption leaves the operational schema intact.
    conn.execute("DROP INDEX IF EXISTS idx_round_documents_document")
    conn.commit()
    removed = 0
    try:
        for round_id, link_count in rows:
            payload = json.dumps(
                {
                    "reason": "legacy_lineage_fanout",
                    "removed_link_count": link_count,
                    "retained_link_count": 0,
                    "sampling_forbidden": True,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM round_events WHERE round_id = ?",
                    (round_id,),
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO round_events(round_id, sequence, event_type, payload_json, created_at)
                VALUES (?, ?, 'HistoricalLineageInvalidated', ?, ?)
                """,
                (round_id, sequence, payload, datetime.now(timezone.utc).isoformat()),
            )
            conn.execute("DELETE FROM round_documents WHERE round_id = ?", (round_id,))
            removed += link_count
        # Event and invalidation links are one atomic offline repair batch.
        conn.commit()
    finally:
        if conn.in_transaction:
            conn.rollback()
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_round_documents_document
                ON round_documents(document_id, round_id)
            """
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return removed


def bound_list(value: object, limit: int) -> list[str]:
    values = value if isinstance(value, list) else ([value] if value else [])
    return [str(item) for item in values if str(item).strip()][-limit:]


def bound_page(path: Path, *, max_chars: int, source_limit: int, excerpt_chars: int) -> bool:
    raise RuntimeError("Lossy page truncation is disabled; use WikiStore.write_page with verified archival")


def compact_wiki(config: dict) -> dict[str, int]:
    from sovereign_hall.services.wiki_archive import pack_raw
    return pack_raw(
        KNOWLEDGE_ROOT,
        retention_days=int(config.get("raw_hot_retention_days", 30)),
        limit=int(config.get("archive_batch_limit", 100)),
        max_bytes=int(config.get("max_size_bytes", 1024**3)),
        target_bytes=int(config.get("max_size_bytes", 1024**3) * float(config.get("archive_target_ratio", 0.85))),
        progress=lambda report: print(json.dumps({'wiki_progress': report}), file=sys.stderr, flush=True),
    )


def vacuum_into_replacement() -> None:
    """Compatibility name: SQLite now vacuums its own file, never swaps it."""
    if live_runner_pid() is not None:
        raise RuntimeError("Stop the research loop before full vacuum")
    conn = sqlite3.connect(DB_PATH, timeout=5)
    try:
        print('Reclaiming physical database pages offline...', file=sys.stderr, flush=True)
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("VACUUM")
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("vacuum integrity check failed")
    finally:
        conn.close()


def compact_document_indexes(conn: sqlite3.Connection) -> list[str]:
    """Remove duplicate lookup indexes only with the matching constraint intact."""
    removed = []
    indexes = {r[1]: r for r in conn.execute('PRAGMA index_list(documents)')}
    for column in ('url', 'content_hash'):
        unique, duplicate = f'ux_documents_{column}', f'idx_documents_{column}'
        if unique not in indexes or duplicate not in indexes or not indexes[unique][2]:
            continue
        columns = lambda name: [r[2] for r in conn.execute(f'PRAGMA index_info("{name}")')]
        sql = conn.execute('SELECT sql FROM sqlite_master WHERE name=?', (unique,)).fetchone()[0]
        normalized = ' '.join(sql.split()).lower()
        expected = f'where {column} is not null and {column} <> \'\''
        if columns(unique) != [column] or columns(duplicate) != [column] or not normalized.endswith(expected):
            continue
        conn.execute(f'DROP INDEX "{duplicate}"')
        removed.append(duplicate)
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the compaction; default is dry-run")
    parser.add_argument("--fanout-limit", type=int, default=500)
    parser.add_argument("--backup-max-bytes", type=int, default=4 * 1024**3)
    parser.add_argument("--archive-limit", type=int, default=40000)
    parser.add_argument("--target-ratio", type=float, default=0.85)
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=DATA_DIR / "storage_backups" / datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
    parser.add_argument(
        "--resume-existing-backup",
        action="store_true",
        help=(
            "resume an interrupted apply using the already complete files in "
            "--backup-dir; refuses unless both database and knowledge backups exist"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="deprecated compatibility flag; never bypasses the live-runner protection",
    )
    args = parser.parse_args()
    if args.archive_limit < 1 or not 0 < args.target_ratio < 0.9:
        parser.error('Archive limit must be positive and target ratio below 0.9')
    config = get_config()
    db_cfg = config.get("database", {})
    wiki_cfg = {**config.get("knowledge_wiki", {}), 'archive_batch_limit': args.archive_limit,
                'archive_target_ratio': args.target_ratio}

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        fanout = oversized_lineage(conn, args.fanout_limit)
    finally:
        conn.close()
    before = {
        "managed_storage": managed_storage_inventory(DB_PATH, KNOWLEDGE_ROOT),
        "database_bytes": database_bundle_size(DB_PATH),
        "knowledge_bytes": directory_size(KNOWLEDGE_ROOT),
        "database_limit_bytes": int(db_cfg.get("max_bundle_size_bytes", 2 * 1024**3)),
        "knowledge_limit_bytes": int(wiki_cfg.get("max_size_bytes", 1024**3)),
        "oversized_rounds": len(fanout),
        "invalid_lineage_links": sum(count for _, count in fanout),
        "wiki": wiki_inventory(),
    }
    result: dict[str, object] = {"applied": args.apply, "before": before}
    if not args.apply:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    pid = live_runner_pid()
    if pid is not None:
        raise RuntimeError(
            f"Stop run_discussion.py before offline maintenance (pid={pid}); --force cannot bypass this"
        )
    from sovereign_hall.services.storage_archive import MaintenanceLock
    with MaintenanceLock(LOCK_PATH):
        return apply_plan(args, fanout, wiki_cfg, result)


def apply_plan(args, fanout, wiki_cfg, result):
    if args.resume_existing_backup:
        existing_db = args.backup_dir / DB_PATH.name
        existing_knowledge = args.backup_dir / "knowledge.tar.gz"
        if not existing_db.is_file() or not existing_knowledge.is_file():
            raise RuntimeError(
                "cannot resume without complete existing database and knowledge backups"
            )
        result["backups"] = {
            "database": str(existing_db),
            "knowledge": str(existing_knowledge),
        }
        result["resumed_existing_backup"] = True
    else:
        remaining = args.backup_max_bytes - directory_size(args.backup_dir.parent)
        expected = database_bundle_size(DB_PATH) + directory_size(DATA_DIR / "storage_archives")
        if expected > remaining:
            raise RuntimeError("Backup capacity exceeded; source data retained")
        result["backups"] = create_backups(args.backup_dir, max_bytes=remaining)
    result["oversized_lineage_retained"] = len(fanout)
    result["wiki_compaction"] = compact_wiki(wiki_cfg)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('BEGIN IMMEDIATE')
        result['duplicate_indexes_removed'] = compact_document_indexes(conn)
        conn.commit()
    finally:
        conn.close()
    vacuum_into_replacement()
    result["after"] = {
        "managed_storage": managed_storage_inventory(DB_PATH, KNOWLEDGE_ROOT),
        "database_bytes": database_bundle_size(DB_PATH),
        "knowledge_bytes": directory_size(KNOWLEDGE_ROOT),
        "wiki": wiki_inventory(),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
