"""Bounded storage policy for the autonomous research loop.

The governor deliberately separates admission control from destructive, offline
compaction.  Online maintenance only removes redundant time-series samples and
unreferenced quote snapshots; canonical decisions, trades, ledger entries, and
research evidence are never deleted here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ..core.config import get_config
from .storage_worker import storage_worker, await_storage_task


logger = logging.getLogger(__name__)
MIB = 1024 * 1024
ALLOCATION_BLOCK_BYTES = 4096
STORAGE_STATUS_FILENAME = "storage_status.json"


async def _pragma(conn, statement):
    """Finish row-returning settings before another connection can write.

    In particular, max_page_count returns a row even when setting a limit.
    An unfinished cursor can retain a WAL snapshot after in_transaction has
    become false, making the next BEGIN IMMEDIATE fail with BUSY_SNAPSHOT.
    """
    async with conn.execute(statement) as cursor:
        rows = await cursor.fetchall()
    return rows[0] if rows else None


class _RoundAuditBudgetExpired(Exception):
    """An unfinished read-only audit must never authorize payload eviction."""


CANONICAL_RUNTIME_SOURCE_DIRS = (
    "agents",
    "application",
    "core",
    "domain",
    "infrastructure",
    "ports",
    "services",
    "sovereign_hall",
    "utils",
)


def allocated_size(stat_result: os.stat_result) -> int:
    blocks = int(getattr(stat_result, "st_blocks", 0) or 0)
    return blocks * 512 if blocks else int(stat_result.st_size)


def directory_size(path: Path) -> int:
    """Return allocated file bytes without following directory symlinks."""
    total = 0
    pending = [path]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += allocated_size(entry.stat(follow_symlinks=False))
                    except (FileNotFoundError, OSError):
                        continue
        except (FileNotFoundError, OSError):
            continue
    return total


def database_bundle_size(db_path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (
            db_path,
            Path(f"{db_path}-wal"),
            Path(f"{db_path}-shm"),
        )
        if candidate.exists()
    )


def managed_storage_inventory(db_path: Path, knowledge_root: Path, *,
                              database_bytes: int | None = None,
                              knowledge_bytes: int | None = None) -> Dict[str, int]:
    """Account for relocated payloads and backup copies, without double counting Wiki archives."""
    primary = database_bundle_size(db_path) if database_bytes is None else database_bytes
    knowledge = directory_size(knowledge_root) if knowledge_bytes is None else knowledge_bytes
    archives = directory_size(db_path.parent / "storage_archives")
    backups = sum(directory_size(db_path.parent / name)
                  for name in ("storage_backups", "cleanup_backups"))
    staging = directory_size(db_path.parent / 'storage_staging')
    return {"database_bytes": primary, "knowledge_bytes": knowledge,
            "cold_archive_bytes": archives, "backup_bytes": backups,
            'staging_bytes': staging,
            "managed_total_bytes": primary + knowledge + archives + backups + staging}


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """Publish a small read model without exposing a partially written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.monotonic_ns()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)


def production_source_mtime_ns(runner_source: Path) -> int:
    """Return the newest mtime in the canonical production import tree.

    Monitoring only ``run_discussion.py`` misses forward fixes made in its
    imported services: a long-lived process keeps the old modules in memory
    while ``check_db`` incorrectly reports ``code_stale=false``.  The bounded
    source tree excludes tests, runs, data and generated artifacts.
    """
    source = Path(runner_source).resolve()
    candidates = [source]
    project_root = source.parent
    for directory_name in CANONICAL_RUNTIME_SOURCE_DIRS:
        directory = project_root / directory_name
        if not directory.is_dir():
            continue
        candidates.extend(directory.rglob("*.py"))

    newest = 0
    for candidate in candidates:
        try:
            newest = max(newest, int(candidate.stat().st_mtime_ns))
        except OSError:
            continue
    return newest


def _runner_runtime_status(lock_path: Path, runner_source: Path) -> Dict[str, Any]:
    fields: Dict[str, str] = {}
    try:
        for token in lock_path.read_text(encoding="utf-8", errors="ignore").split():
            if "=" in token:
                key, value = token.split("=", 1)
                fields[key.strip()] = value.strip()
    except OSError:
        pass

    try:
        pid = int(fields.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    pid_alive = False
    if pid > 0:
        try:
            os.kill(pid, 0)
            pid_alive = True
        except PermissionError:
            # A process that exists but is owned by another user is still a
            # live lock holder.  Treating EPERM as a dead PID would make the
            # read model claim that a second production runner is safe.
            pid_alive = True
        except (OSError, ProcessLookupError):
            pid_alive = False

    started_at = fields.get("started_at") or ""
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        started = None
    try:
        source_mtime_ns = production_source_mtime_ns(runner_source)
        source_updated = (
            datetime.fromtimestamp(source_mtime_ns / 1_000_000_000)
            if source_mtime_ns
            else None
        )
    except OSError:
        source_updated = None
        source_mtime_ns = 0
    if started is not None and started.tzinfo is not None and source_updated is not None:
        source_updated = source_updated.astimezone(started.tzinfo)
    try:
        lock_source_mtime_ns = int(fields.get("source_mtime_ns") or 0)
    except (TypeError, ValueError):
        lock_source_mtime_ns = 0
    code_stale = bool(
        pid_alive
        and source_mtime_ns
        and (
            source_mtime_ns > lock_source_mtime_ns
            if lock_source_mtime_ns
            else (
                started is not None
                and source_updated is not None
                and source_updated > started
            )
        )
    )
    return {
        "pid": pid or None,
        "pid_alive": pid_alive,
        "lock_present": lock_path.exists(),
        "stale_lock": bool(lock_path.exists() and pid > 0 and not pid_alive),
        "started_at": started_at or None,
        "runner_source_updated_at": source_updated.isoformat() if source_updated else None,
        "runner_source_mtime_ns": source_mtime_ns or None,
        "lock_source_mtime_ns": lock_source_mtime_ns or None,
        "code_stale": code_stale,
    }


def _unterminated_round_count(db_path: Path) -> Optional[int]:
    """Return rounds that canonical startup must atomically recover.

    This is deliberately a read-only, best-effort probe for ``check_db``.  It
    uses the exact predicate owned by ``ResearchRoundCoordinator`` so the
    entry-point diagnosis and the automatic startup recovery cannot disagree.
    """
    try:
        connection = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            timeout=1.0,
        )
        try:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM research_rounds
                WHERE status NOT IN ('completed', 'failed')
                  AND COALESCE(terminal_code, '') = ''
                """
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return None


def runtime_storage_read_model(
    *,
    db_path: Path,
    knowledge_root: Path,
    lock_path: Path,
    runner_source: Path,
    cache_max_age_seconds: int = 180,
) -> Dict[str, Any]:
    """Return storage and live-runner health for ``check_db``.

    The production loop publishes a cheap snapshot before every round.  A
    missing, stale, or foreign-process snapshot is recomputed from disk so the
    read model cannot silently repeat an obsolete capacity value.
    """
    config = get_config()
    db_config = config.get("database", {})
    wiki_config = config.get("knowledge_wiki", {})
    runner = _runner_runtime_status(lock_path, runner_source)
    status_path = db_path.parent / STORAGE_STATUS_FILENAME
    cached: Dict[str, Any] = {}
    try:
        candidate = json.loads(status_path.read_text(encoding="utf-8"))
        if isinstance(candidate, dict):
            cached = candidate
    except (OSError, ValueError, json.JSONDecodeError):
        cached = {}

    try:
        checked = datetime.fromisoformat(
            str(cached.get("checked_at") or "").replace("Z", "+00:00")
        )
        now = datetime.now(checked.tzinfo) if checked.tzinfo else datetime.now()
        cache_age = max(0.0, (now - checked).total_seconds())
    except ValueError:
        cache_age = float("inf")
    # A production snapshot belongs to the process that published it.  Once
    # that process is dead, even a recently written snapshot is not a live
    # heartbeat and must not be presented as production truth.
    cache_matches_runner = bool(
        runner.get("pid_alive")
        and int(cached.get("runner_pid") or 0) == int(runner.get("pid") or 0)
    )
    use_cache = bool(
        cached.get("database")
        and cached.get("knowledge")
        and cache_age <= max(1, int(cache_max_age_seconds))
        and cache_matches_runner
    )
    if use_cache:
        database = dict(cached["database"])
        knowledge = dict(cached["knowledge"])
        checked_at = str(cached.get("checked_at") or "")
        source = "production_snapshot"
    else:
        database = StorageGovernor._watermark(
            database_bundle_size(db_path),
            db_config,
            2 * 1024**3,
        ).to_dict()
        knowledge = StorageGovernor._watermark(
            directory_size(knowledge_root),
            wiki_config,
            1024**3,
        ).to_dict()
        checked_at = datetime.now(timezone.utc).isoformat()
        source = "direct_scan"

    research_round_allowed = bool(
        database.get("bulk_writes_allowed")
        and knowledge.get("bulk_writes_allowed")
    )
    if use_cache and cached.get('research_round_allowed') is False:
        research_round_allowed = False
    unterminated_round_count = _unterminated_round_count(db_path)
    blockers = []
    if not runner.get("pid_alive"):
        blockers.append("production_runner_not_running")
    if not runner.get("pid_alive") and unterminated_round_count:
        blockers.append("orphaned_rounds_pending_startup_recovery")
    if runner.get("code_stale"):
        blockers.append("stale_runner_code")
    if not database.get("bulk_writes_allowed"):
        blockers.append("database_storage_pressure")
    if not knowledge.get("bulk_writes_allowed"):
        blockers.append("knowledge_storage_pressure")
    return {
        "database": database,
        "knowledge": knowledge,
        "maintenance": cached.get("maintenance", {}) if use_cache else {
            "status": "unavailable" if runner.get("pid_alive") else "not_running"
        },
        'operation': cached.get('operation', {}) if use_cache else {
            'state': ('ready' if research_round_allowed else 'waiting_for_capacity')
                     if runner.get('pid_alive') else 'not_running',
            'bulk_writes_paused': not research_round_allowed,
            'automatic_resume': bool(runner.get('pid_alive')), 'process_exit_required': False,
        },
        "managed_storage": managed_storage_inventory(db_path, knowledge_root,
            database_bytes=database.get("used_bytes"), knowledge_bytes=knowledge.get("used_bytes")),
        "checked_at": checked_at,
        "status_source": source,
        "research_round_allowed": research_round_allowed,
        "runner": runner,
        "unterminated_round_count": unterminated_round_count,
        "automatic_recovery": "canonical_run_discussion_startup",
        "blockers": blockers,
        "health": "blocked" if blockers else "healthy",
    }


@dataclass(frozen=True)
class StoreWatermark:
    used_bytes: int
    max_bytes: int
    target_ratio: float
    maintenance_ratio: float
    bulk_write_stop_ratio: float

    @property
    def ratio(self) -> float:
        return self.used_bytes / self.max_bytes if self.max_bytes > 0 else 0.0

    @property
    def needs_maintenance(self) -> bool:
        return self.ratio >= self.maintenance_ratio

    @property
    def bulk_writes_allowed(self) -> bool:
        return self.ratio < self.bulk_write_stop_ratio

    @property
    def hard_limit_reached(self) -> bool:
        return self.used_bytes >= self.max_bytes

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result.update(
            ratio=self.ratio,
            needs_maintenance=self.needs_maintenance,
            bulk_writes_allowed=self.bulk_writes_allowed,
            hard_limit_reached=self.hard_limit_reached,
        )
        return result


@dataclass(frozen=True)
class StorageSnapshot:
    database: StoreWatermark
    knowledge: StoreWatermark
    checked_at: str
    recovery_targets: tuple[str, ...] = ()
    resume_pending: tuple[str, ...] = ()

    @property
    def research_round_allowed(self) -> bool:
        # Recovery keeps its 60% objective across rounds. Below the write gate,
        # research can continue rather than waiting hours for protected history.
        return self.database.bulk_writes_allowed and self.knowledge.bulk_writes_allowed and not self.resume_pending

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database.to_dict(),
            "knowledge": self.knowledge.to_dict(),
            "checked_at": self.checked_at,
            "research_round_allowed": self.research_round_allowed,
            'recovery_targets': list(self.recovery_targets),
            'resume_pending': list(self.resume_pending),
        }


def storage_pressure_message(snapshot: StorageSnapshot, maintenance: Dict[str, Any]) -> str:
    usage = (f"数据库 {snapshot.database.ratio:.2%}，知识库 {snapshot.knowledge.ratio:.2%}")
    archive = maintenance.get('archive', {})
    reclaimed = (int(maintenance.get("reclaimed_bytes", 0))
                 + int(archive.get('wiki', {}).get('reclaimed_bytes', 0))
                 + int(archive.get('wiki_sources', {}).get('reclaimed_bytes', 0))
                 + int(archive.get('wiki_archive', {}).get('reclaimed_bytes', 0))
                 + int(archive.get('cache', {}).get('reclaimed_bytes', 0))) / MIB
    stalled = (archive.get('status') in ('completed', 'backoff')
               and not archive.get('progressed') and not archive.get('can_continue'))
    if archive.get('error'):
        action = '在线归档失败，原始数据已保留；等待重试'
    elif maintenance.get('status') == 'waiting_for_reader':
        action = '有读取连接阻止释放日志空间，进程保持运行并稍后重试'
    elif stalled and not snapshot.research_round_allowed:
        action = '安全归档候选已用尽，容量仍不足；进程保持运行，保留原始证据并降低重试频率'
    elif snapshot.recovery_targets:
        action = (f'深度在线整理中，目标数据库 {snapshot.database.target_ratio:.0%}、'
                  f'知识库 {snapshot.knowledge.target_ratio:.0%}；进程保持运行')
        if maintenance.get('status') == 'waiting_for_reader':
            action += '，等待读取连接释放日志'
    elif archive.get('progressed'):
        action = '正在分批归档历史原文，达到写入条件后恢复研究'
    elif archive.get('can_continue') or archive.get('wiki', {}).get('budget_exhausted'):
        action = '在线归档扫描尚未完成，将接续扫描后恢复研究'
    elif not snapshot.knowledge.bulk_writes_allowed:
        action = "知识库达到写入限制，在线归档暂未解除限制"
    elif maintenance.get("status") in {"capacity_wait", "offline_compaction_required"}:
        action = "暂缓新研究写入，进程保持运行并等待在线容量恢复"
    elif maintenance.get("status") == "waiting_for_reader":
        action = "有读取连接阻止释放日志空间，暂缓研究并稍后重试"
    elif maintenance.get("status") == "reclaiming_pages":
        action = "正在分批回收空闲页面，达到写入条件后恢复研究"
    else:
        action = "在线维护暂未解除限制，稍后重试"
    return f"🧹 {action}；{usage}；本次释放 {reclaimed:.2f} MiB"


def storage_capacity_state(snapshot: StorageSnapshot, maintenance: Dict[str, Any]) -> str:
    """Capacity pressure pauses producers; it never requires process exit."""
    if snapshot.research_round_allowed and not snapshot.recovery_targets:
        return 'ready'
    archive = maintenance.get('archive', {})
    if archive.get('status') == 'archive_blocked' or archive.get('error'):
        return 'maintenance_error'
    if snapshot.recovery_targets and snapshot.research_round_allowed:
        return 'maintaining'
    if (archive.get('progressed') or archive.get('can_continue')
            or archive.get('wiki', {}).get('budget_exhausted')):
        return 'maintaining'
    if maintenance.get('status') == 'waiting_for_reader':
        return 'waiting_for_reader'
    if maintenance.get('status') in {'reclaiming_pages', 'retry_later', 'already_running'}:
        return 'maintaining'
    return 'waiting_for_capacity'


class StorageGovernor:
    """Admission control and bounded online maintenance for both stores."""

    def __init__(self, database_service: Any, knowledge_root: Any):
        config = get_config()
        self.database_service = database_service
        self.db_path = Path(database_service.db_path)
        self.knowledge_store = knowledge_root if hasattr(knowledge_root, "used_bytes") else None
        self.knowledge_root = Path(
            self.knowledge_store.root if self.knowledge_store is not None else knowledge_root
        )
        self.db_config = config.get("database", {})
        self.wiki_config = config.get("knowledge_wiki", {})
        self._last_snapshot: Optional[StorageSnapshot] = None
        self._last_optimize_date: Optional[str] = None
        self._round_checks = 0
        self._lock = asyncio.Lock()
        self._snapshot_lock = asyncio.Lock()
        self.last_maintenance: Dict[str, Any] = {}
        self._archive_retry_after = 0.0
        self._archive_retry_stores = None
        self._archive_retry_signature = None
        self._last_archive_report: Dict[str, Any] = {}
        self.archive_state_path = self.db_path.parent / 'storage_archive_state.json'
        self._wiki_archive_recent = False
        self._wiki_source_after = None
        self._recovery_targets = []
        self._resume_pending = []
        self._round_after = None
        self._recovery_result = {}
        self._research_utc_connection = None
        self._round_audit_budget = None
        self._round_audit_deferred_count = 0
        self._feedback_retry_after = {}
        self._background_task = None
        self._background_stop = asyncio.Event()
        try:
            archive_state = json.loads(self.archive_state_path.read_text())
            self._wiki_archive_after = archive_state.get('wiki_after')
            source_after = archive_state.get('wiki_source_after')
            self._wiki_source_after = source_after if isinstance(source_after, str) else None
            self._wiki_archive_recent = archive_state.get('wiki_include_recent') is True
            self._recovery_targets = [name for name in archive_state.get('recovery_targets', [])
                                      if name in ('database', 'knowledge')]
            self._resume_pending = [name for name in archive_state.get('resume_pending', [])
                                    if name in ('database','knowledge')]
            after = archive_state.get('round_after')
            if isinstance(after, list) and len(after) == 2 and all(isinstance(v, str) for v in after):
                self._round_after = after
            result = archive_state.get('recovery_result')
            if isinstance(result, dict):
                self._recovery_result = result
            audit = archive_state.get('round_audit_budget')
            if (isinstance(audit, dict) and isinstance(audit.get('round_id'), str)
                    and audit.get('seconds') in (5, 10, 20)):
                self._round_audit_budget = audit
            self._round_audit_deferred_count = max(0, int(archive_state.get('audit_deferred_rounds', 0)))
            if not isinstance(self._wiki_archive_after, str):
                self._wiki_archive_after = None
        except (OSError, ValueError, TypeError, AttributeError):
            self._wiki_archive_after = None
        self._archive_deferred_rounds: Dict[str, float] = {}
        self.status_path = self.db_path.parent / STORAGE_STATUS_FILENAME

    async def _save_archive_state(self):
        await storage_worker(_atomic_write_json, self.archive_state_path, {
            'wiki_after': self._wiki_archive_after, 'wiki_include_recent': self._wiki_archive_recent,
            'wiki_source_after': self._wiki_source_after,
            'round_after': self._round_after, 'recovery_targets': list(self._recovery_targets),
            'resume_pending': list(self._resume_pending),
            'recovery_result': self._recovery_result,
            'round_audit_budget': self._round_audit_budget,
            'audit_deferred_rounds': self._round_audit_deferred_count,
        })

    async def initialize(self) -> StorageSnapshot:
        conn = await self.database_service._get_connection()
        wal_pages = max(1, int(self.db_config.get("wal_autocheckpoint_pages", 1000)))
        journal_limit = max(0, int(self.db_config.get("journal_size_limit_bytes", 16 * MIB)))
        await _pragma(conn, f"PRAGMA wal_autocheckpoint = {wal_pages}")
        await _pragma(conn, f"PRAGMA journal_size_limit = {journal_limit}")

        page_size = int((await _pragma(conn, "PRAGMA page_size"))[0])
        page_count = int((await _pragma(conn, "PRAGMA page_count"))[0])
        max_main_bytes = int(self.db_config.get("max_main_file_bytes", 1984 * MIB))
        max_pages = max(1, max_main_bytes // page_size)
        if page_count <= max_pages:
            await _pragma(conn, f"PRAGMA max_page_count = {max_pages}")
        else:
            logger.warning(
                "SQLite main file already exceeds configured page ceiling: pages=%s ceiling=%s; "
                "growth remains gated while online archival reclaims capacity",
                page_count,
                max_pages,
            )
        return await self.snapshot(refresh=True, refresh_knowledge=True)

    def start_background(self):
        """Continue safe DB compaction while research awaits models/network."""
        if self._background_task is None:
            self._background_task = asyncio.create_task(self._background_loop(), name='storage-compaction')

    async def close(self):
        self._background_stop.set()
        if self._background_task is not None:
            # Do not abandon a running SQLite thread during an atomic switch.
            try:
                await await_storage_task(self._background_task)
            finally:
                self._background_task = None

    async def _background_loop(self):
        from .storage_reclaim import reclaim_pages
        while not self._background_stop.is_set():
            try:
                snap = self._last_snapshot
                connection = getattr(self.database_service,'_connection',None)
                if (snap is not None and 'database' in self._recovery_targets
                        and snap.database.ratio>snap.database.target_ratio
                        and not self._lock.locked() and not (connection and connection.in_transaction)):
                    async with self._lock:
                        result = await self._database_compaction_step()
                        freed = await storage_worker(reclaim_pages,self.db_path)
                        result['page_reclaim'] = freed
                        self.last_maintenance = {'status':'completed','background':True,'archive':result}
                        self.invalidate_snapshot()
                        await self.snapshot(refresh=True)
                        logger.info('STORAGE_BACKGROUND %s',result)
            except Exception as exc:
                # This worker owns only its separate connections. Never roll
                # back an unrelated research/account transaction on failure.
                logger.exception('STORAGE_BACKGROUND_ERROR %s',exc)
            try:
                await asyncio.wait_for(self._background_stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    async def _database_compaction_step(self):
        from .lineage_storage import repack_step
        from .cold_payloads import pack_payloads
        from .storage_reclaim import repack_payload_table
        from .storage_archive import ArchiveStore
        report = {'status':'completed'}
        until = time.monotonic()+3
        steps=[]
        while True:
            step=await storage_worker(repack_step,self.db_path,batch_rows=100000,
                budget_seconds=3,max_main_bytes=int(self.db_config.get('max_main_file_bytes',1984*MIB)))
            steps.append(step)
            if step['status'] not in ('copy','verify') or time.monotonic()>=until:
                break
        report['lineage']={**steps[-1],
            'copied_rows':sum(s['copied_rows'] for s in steps),
            'verified_rows':sum(s['verified_rows'] for s in steps)}
        if step['status'] in ('complete','not_applicable'):
            archive=ArchiveStore(self.db_path.parent/'storage_archives'/'research.sqlite3',
                int(self.db_config.get('archive_max_bytes',1024**3)))
            report['payloads']=await storage_worker(pack_payloads,self.db_path,archive,
                limit=max(1,int(self.db_config.get('cold_payload_batch_rows',400))),budget_seconds=4)
            if not report['payloads']['can_continue']:
                report['payload_layout']=await storage_worker(repack_payload_table,self.db_path)
        report['progressed']=(report['lineage']['copied_rows']>0 or report['lineage']['verified_rows']>0
            or report.get('payloads',{}).get('turns_packed',0)>0
            or report.get('payloads',{}).get('diagnostics_packed',0)>0
            or report.get('payload_layout',{}).get('rows_repacked',0)>0
            or report.get('payload_layout',{}).get('status')=='repacked')
        report['can_continue']=any(report.get(name,{}).get('can_continue',False)
                                   for name in ('lineage','payloads','payload_layout'))
        return report

    async def snapshot(
        self,
        *,
        refresh: bool = False,
        refresh_knowledge: bool = False,
    ) -> StorageSnapshot:
        async with self._snapshot_lock:
            return await self._snapshot(refresh=refresh, refresh_knowledge=refresh_knowledge)

    async def _snapshot(self, *, refresh=False, refresh_knowledge=False) -> StorageSnapshot:
        if self._last_snapshot is not None and not refresh:
            return self._last_snapshot
        database_bytes, knowledge_bytes = await asyncio.gather(
            asyncio.to_thread(database_bundle_size, self.db_path),
            self._knowledge_size(refresh=refresh_knowledge),
        )
        database = self._watermark(database_bytes, self.db_config, 2 * 1024**3)
        knowledge = self._watermark(knowledge_bytes, self.wiki_config, 1024**3)
        if not refresh_knowledge and (not knowledge.bulk_writes_allowed
                or 'knowledge' in self._resume_pending or 'knowledge' in self._recovery_targets):
            knowledge = self._watermark(await self._knowledge_size(refresh=True),self.wiki_config,1024**3)
        stores = {'database': database, 'knowledge': knowledge}
        pending_before = set(self._resume_pending)
        for name, store in stores.items():
            cfg = self.db_config if name == 'database' else self.wiki_config
            resume_ratio = min(store.bulk_write_stop_ratio, float(cfg.get('resume_write_ratio',.80)))
            if not store.bulk_writes_allowed and name not in self._resume_pending:
                self._resume_pending.append(name)
            elif name in self._resume_pending and store.ratio <= resume_ratio:
                self._resume_pending.remove(name)
        if set(self._resume_pending) != pending_before:
            await self._save_archive_state()
        def triggers(store):
            return (not store.bulk_writes_allowed if self._recovery_result.get('state') == 'protected_floor'
                    else store.needs_maintenance)
        if not self._recovery_targets and triggers(knowledge) and not refresh_knowledge:
            knowledge = self._watermark(await self._knowledge_size(refresh=True), self.wiki_config, 1024**3)
            stores['knowledge'] = knowledge
        if not self._recovery_targets and any(triggers(s) for s in stores.values()):
            self._recovery_targets = [name for name, s in stores.items() if s.ratio > s.target_ratio]
            self._wiki_archive_after = None
            self._wiki_source_after = None
            self._round_after = None
            self._round_audit_budget = None
            self._round_audit_deferred_count = 0
            self._archive_retry_after = 0
            self._recovery_result = {'state': 'running', 'target_met': False}
            await self._save_archive_state()
        if self._recovery_targets and all(not stores[name].needs_maintenance for name in self._recovery_targets):
            self._recovery_targets = []
            self._recovery_result = {'state': 'completed', 'target_met': True}
            await self._save_archive_state()
        self._last_snapshot = StorageSnapshot(
            database=database,
            knowledge=knowledge,
            checked_at=datetime.now(timezone.utc).isoformat(),
            recovery_targets=tuple(self._recovery_targets),
            resume_pending=tuple(self._resume_pending),
        )
        snapshot = self._last_snapshot
        payload = snapshot.to_dict()
        payload["maintenance"] = self.last_maintenance
        payload['operation'] = {
            'state': storage_capacity_state(self._last_snapshot, self.last_maintenance),
            'bulk_writes_paused': not self._last_snapshot.research_round_allowed,
            'automatic_resume': True, 'process_exit_required': False,
            'recovery_targets': list(self._recovery_targets), 'recovery': self._recovery_result,
        }
        payload.update(
            runner_pid=os.getpid(),
            runner_source_mtime_ns=production_source_mtime_ns(
                Path(__file__).resolve().parents[1] / "run_discussion.py"
            ),
        )
        await storage_worker(_atomic_write_json, self.status_path, payload)
        # Maintenance can invalidate the cache while status I/O yields. Return
        # this observation, not the subsequently invalidated cache reference.
        return snapshot

    def invalidate_snapshot(self) -> None:
        self._last_snapshot = None

    async def before_round(self) -> StorageSnapshot:
        """Refresh watermarks and run a small cleanup before admitting work."""
        self._round_checks += 1
        rescan_interval = max(
            1,
            int(self.db_config.get("knowledge_rescan_interval_rounds", 24)),
        )
        snapshot = await self.snapshot(
            refresh=True,
            refresh_knowledge=self._round_checks % rescan_interval == 0,
        )
        # Admission must never stop production based solely on an accumulated
        # allocation estimate; verify the actual Wiki footprint at the gate.
        if not snapshot.knowledge.bulk_writes_allowed:
            snapshot = await self.snapshot(refresh=True, refresh_knowledge=True)
        if snapshot.database.needs_maintenance or snapshot.knowledge.needs_maintenance or snapshot.recovery_targets or snapshot.resume_pending:
            await self.online_maintenance()
            snapshot = await self.snapshot(refresh=True)
            if (snapshot.database.needs_maintenance or snapshot.knowledge.needs_maintenance or snapshot.recovery_targets) and bool(
                self.db_config.get('automatic_lossless_archive', True)
            ):
                archive = await self.archive_history(snapshot)
                if archive:
                    await self.online_maintenance()
                    self.last_maintenance['archive'] = archive
                    snapshot = await self.snapshot(refresh=True)
                    # A target is best-effort only when all safe candidates
                    # were checked and both stores already allow new writes.
                    # Never manufacture 60% by deleting protected evidence.
                    if (self._recovery_targets and not archive.get('can_continue')
                            and not archive.get('progressed') and not archive.get('error')
                            and self.last_maintenance.get('free_pages_after', 0) == 0
                            and snapshot.research_round_allowed):
                        self._recovery_result = {'state': 'protected_floor', 'target_met': False,
                            'remaining': {name: {'ratio': getattr(snapshot, name).ratio,
                                                'target_ratio': getattr(snapshot, name).target_ratio}
                                          for name in self._recovery_targets},
                            'reason': ('audit_budget_deferred' if self._round_audit_deferred_count
                                       else 'safe_candidates_exhausted'),
                            'audit_deferred_rounds': self._round_audit_deferred_count}
                        self._recovery_targets = []
                        await self._save_archive_state()
                        logger.warning('STORAGE_TARGET_UNREACHABLE %s', self._recovery_result)
                        snapshot = await self.snapshot(refresh=True)
        return snapshot

    def _archive_inputs(self, snapshot):
        """Invalidate no-candidate backoff when reclaimable inputs change."""
        paths = (self.knowledge_root / 'raw', self.knowledge_root / 'wiki/sources',
                 self.knowledge_root / '.archives/wiki.sqlite3',
                 self.knowledge_root / '.archives/source_pages.sqlite3',
                 self.db_path.parent / 'storage_archives/research.sqlite3')
        signature = [snapshot.knowledge.used_bytes, snapshot.knowledge.max_bytes,
                     snapshot.database.max_bytes]
        for path in paths:
            try:
                stat = path.stat()
                signature.append((stat.st_ino, stat.st_size, stat.st_mtime_ns))
            except OSError:
                signature.append(None)
        return tuple(signature)

    async def _archive_step(self, report, name, operation, *args, **kwargs):
        """A failed reclaim strategy must not suppress independent strategies."""
        try:
            result = await operation(*args, **kwargs)
            report[name] = result
            return result
        except Exception as exc:
            report.setdefault('errors', {})[name] = str(exc)
            report[name] = {'status': 'blocked', 'error': str(exc)}
            logger.warning('STORAGE_ARCHIVE_STEP_FAILED step=%s reason=%s', name, exc)
            return None

    async def _rollback_owned_transaction(self):
        connection = getattr(self.database_service, '_connection', None)
        owner = getattr(self.database_service, '_transaction_owner', None)
        if connection is not None and connection.in_transaction:
            if owner is asyncio.current_task():
                await connection.rollback()
            elif owner is None:
                async with self.database_service.connection_scope() as connection:
                    if connection.in_transaction:
                        await connection.rollback()

    async def archive_history(self, snapshot: StorageSnapshot) -> Dict[str, Any]:
        """Archive originals between rounds, while the runner owns its lock.

        This does not acquire the offline lock again or perform a full VACUUM.
        Every eviction is separately verified and all evidence roots survive.
        No-candidate attempts are backed off instead of rescanning each minute.
        """
        recent_threshold = float(self.wiki_config.get(
            'archive_recent_at_ratio', snapshot.knowledge.bulk_write_stop_ratio - .02))
        include_recent = snapshot.knowledge.ratio >= recent_threshold or 'knowledge' in self._recovery_targets
        db_needed = snapshot.database.needs_maintenance or 'database' in self._recovery_targets
        wiki_needed = snapshot.knowledge.needs_maintenance or 'knowledge' in self._recovery_targets
        stores = (db_needed, wiki_needed, include_recent)
        if (stores == self._archive_retry_stores and time.monotonic() < self._archive_retry_after
                and self._archive_inputs(snapshot) == self._archive_retry_signature):
            return {**self._last_archive_report, 'status': 'backoff', 'progressed': False,
                    'can_continue': False,
                    'retry_after_seconds': max(0, round(self._archive_retry_after - time.monotonic(), 2))}
        from .wiki_archive import pack_raw
        from .wiki_source_archive import pack_sources
        report: Dict[str, Any] = {'status': 'completed'}
        async with self._lock:
            try:
                from .bounded_memory import BoundedMemoryStore
                from .storage_archive import ArchiveStore
                archive_store = ArchiveStore(self.db_path.parent/'storage_archives'/'research.sqlite3',
                    int(self.db_config.get('archive_max_bytes', 1024**3)))
                memory_store = BoundedMemoryStore(self.database_service, archive_store)
                memory_cfg = get_config().get('agent_memory', {})
                if getattr(self.database_service, '_connection', None) is not None:
                    memory = await self._archive_step(report, 'memory', memory_store.backfill,
                        limit=max(1, int(memory_cfg.get('backfill_batch_limit', 20))))
                    if memory is not None:
                        memory['layers'] = await memory_store.usage()
                await self._archive_step(report, 'archive_format', storage_worker, archive_store.migrate_legacy,
                    limit=max(1, int(memory_cfg.get('archive_migration_batch_limit', 20))), budget_seconds=2)
                if wiki_needed:
                    from .archive_repack import repack_archive_async
                    self.last_maintenance['archive'] = {'status': 'running', 'can_continue': True,
                        'wiki_archive': {'status': 'repacking'}}
                    await self.snapshot(refresh=True)
                    packed = await self._archive_step(report, 'wiki_archive', repack_archive_async,
                        self.knowledge_root / '.archives' / 'wiki.sqlite3',
                        max_bytes=snapshot.knowledge.max_bytes,
                        staging_root=self.db_path.parent / 'storage_staging')
                    if packed is not None:
                        report['wiki_archive'] = packed
                    if report['wiki_archive'].get('reclaimed_bytes', 0):
                        cache_after = await self._knowledge_size(refresh=True)
                        snapshot = StorageSnapshot(snapshot.database,
                            self._watermark(cache_after, self.wiki_config, 1024**3), snapshot.checked_at,
                            snapshot.recovery_targets, snapshot.resume_pending)
                    from .compressed_json import compress_existing
                    legacy_caches = [self.knowledge_root/'.state'/name for name in
                                     ('page_embeddings.json', 'ingest-cache.json')]
                    if any(path.is_file() for path in legacy_caches):
                        cache_before = await self._knowledge_size(refresh=True)
                        cache_reports = []
                        for path in legacy_caches:
                            cache_reports.append(await self._archive_step(report, 'cache_format', storage_worker,
                                compress_existing, path,
                                max_output_bytes=max(0, snapshot.knowledge.max_bytes-cache_before)))
                        if self.knowledge_store is not None:
                            self.knowledge_store._used_bytes = None
                        cache_after = await self._knowledge_size(refresh=True)
                        report['cache'] = {'files': cache_reports, 'reclaimed_bytes': max(0, cache_before-cache_after)}
                        snapshot = StorageSnapshot(snapshot.database,
                            self._watermark(cache_after, self.wiki_config, 1024**3), snapshot.checked_at,
                            snapshot.recovery_targets)
                    if include_recent != self._wiki_archive_recent:
                        # Previously skipped young hot copies become eligible
                        # under pressure; revisit the prefix on a policy change.
                        self._wiki_archive_after = None
                        self._wiki_source_after = None
                        self._wiki_archive_recent = include_recent
                    cached_pages = getattr(self.knowledge_store, '_pages_cache', None)
                    pages_by_path = {page.path: page for page in cached_pages} if cached_pages else {}
                    def source_updated(path, frontmatter, body):
                        if path in pages_by_path:
                            pages_by_path[path].frontmatter = frontmatter
                            pages_by_path[path].body = body
                    packed = await self._archive_step(report, 'wiki', storage_worker,
                        pack_raw, self.knowledge_root,
                        retention_days=max(1, int(self.wiki_config.get('raw_hot_retention_days', 30))),
                        limit=max(1, min(1000, int(self.wiki_config.get('archive_batch_limit', 500)))),
                        max_bytes=snapshot.knowledge.max_bytes,
                        target_bytes=int(snapshot.knowledge.max_bytes * snapshot.knowledge.target_ratio), budget_seconds=5,
                        used_bytes=snapshot.knowledge.used_bytes, source_updated=source_updated,
                        after_name=self._wiki_archive_after,
                        include_recent=include_recent,
                    )
                    if packed is not None:
                        report['wiki'] = packed
                    else:
                        report['wiki'].update(after_bytes=await self._knowledge_size(refresh=True),
                            raw_removed=0, retained_changed=0, scan_complete=True,
                            next_after=self._wiki_archive_after)
                    report['wiki']['hot_policy'] = 'pressure' if include_recent else 'age_based'
                    self._wiki_archive_after = report['wiki']['next_after']
                    if self.wiki_config.get('source_page_archive_enabled', True):
                        packed = await self._archive_step(report, 'wiki_sources', storage_worker,
                            pack_sources, self.knowledge_root,
                            retention_days=max(1, int(self.wiki_config.get('source_hot_retention_days', 30))),
                            limit=max(1, min(2000, int(self.wiki_config.get('source_archive_batch_limit', 500)))),
                            max_bytes=snapshot.knowledge.max_bytes,
                            target_bytes=int(snapshot.knowledge.max_bytes * snapshot.knowledge.target_ratio),
                            used_bytes=report['wiki']['after_bytes'], budget_seconds=5,
                            after_name=self._wiki_source_after, include_recent=include_recent)
                        if packed is not None:
                            report['wiki_sources'] = packed
                            self._wiki_source_after = packed['next_after']
                        else:
                            report['wiki_sources'].update(after_bytes=await self._knowledge_size(refresh=True),
                                scan_complete=True)
                    await self._save_archive_state()
                    if self.knowledge_store is not None:
                        self.knowledge_store._used_bytes = report.get('wiki_sources', report['wiki'])['after_bytes']
                        if report['wiki']['retained_changed'] or report.get('wiki_sources', {}).get('retained_changed'):
                            self.knowledge_store._used_bytes = None
                            self.knowledge_store._pages_cache = None
                if db_needed:
                    report.update(await self._database_compaction_step())
                    if not report.get('payloads',{}).get('can_continue') and report['lineage']['status'] in ('complete','not_applicable'):
                        report['research'] = await self._archive_closed_round()
            except Exception as exc:
                # Archive/checksum/capacity failure retains every hot payload.
                # Each database helper owns its transaction. Account duty can
                # now run while an archive worker is awaited; never roll back
                # another task's account transaction here.
                await self._rollback_owned_transaction()
                report.update(status='archive_blocked', error=str(exc))
                if self.knowledge_store is not None:
                    self.knowledge_store._used_bytes = None
            for name in ('wiki', 'wiki_sources'):
                if report.get(name, {}).get('retained_errors'):
                    report.setdefault('errors', {})[name] = report[name]['errors'][0]['error']
            if report.get('errors'):
                report.update(status='archive_blocked', error='; '.join(
                    f'{name}: {error}' for name, error in report['errors'].items()))
            progressed = (report.get('wiki', {}).get('raw_removed', 0) > 0
                          or report.get('wiki_sources', {}).get('pages_removed', 0) > 0
                          or report.get('wiki_archive', {}).get('reclaimed_bytes', 0) > 0
                          or report.get('research', {}).get('evicted_turns', 0) > 0
                          or report.get('cache', {}).get('reclaimed_bytes', 0) > 0)
            progressed |= (report.get('lineage',{}).get('copied_rows',0)>0
                           or report.get('lineage',{}).get('verified_rows',0)>0
                           or report.get('payloads',{}).get('turns_packed',0)>0
                           or report.get('payloads',{}).get('diagnostics_packed',0)>0
                           or report.get('payload_layout',{}).get('rows_repacked',0)>0)
            report['progressed'] = progressed
            can_continue = (report.get('wiki', {}).get('scan_complete') is False
                            or (report.get('wiki_sources', {}).get('scan_complete') is False
                                and not (report['wiki_sources'].get('capacity_limited')
                                         and not report['wiki_sources'].get('pages_removed')))
                            or report.get('research', {}).get('candidates_remaining', 0) > 0
                            or report.get('memory', {}).get('can_continue', False)
                            or report.get('archive_format', {}).get('can_continue', False))
            can_continue |= report.get('lineage',{}).get('can_continue',False) or report.get('payloads',{}).get('can_continue',False) or report.get('payload_layout',{}).get('can_continue',False)
            report['can_continue'] = can_continue
            if report.get('error') or (not progressed and not can_continue):
                retry_seconds = 30 if report.get('error') else 3600
                self._archive_retry_after = time.monotonic() + retry_seconds
                report['retry_after_seconds'] = retry_seconds
                self._archive_retry_stores = stores
                self._archive_retry_signature = self._archive_inputs(snapshot)
            else:
                self._archive_retry_after = 0
            self._last_archive_report = report
            self.invalidate_snapshot()
            logger.info('STORAGE_ARCHIVE %s', report)
        return report

    def _audit_round_for_archive(self, round_id: str, *, deep: bool, budget_seconds: float):
        from .research_memory import ProtectionPolicy, round_snapshot, build_memory
        # Separate read-only connection: cancellation cannot interrupt a write
        # transaction or accidentally clear the hot payload.
        check = sqlite3.connect(self.db_path.resolve().as_uri()+'?mode=ro', uri=True)
        timed_out = False
        deadline = time.monotonic()+budget_seconds
        def progress():
            nonlocal timed_out
            timed_out = time.monotonic() >= deadline
            return int(timed_out)
        try:
            check.row_factory = sqlite3.Row
            policy = ProtectionPolicy(check)
            check.set_progress_handler(progress, 1000)
            reasons = policy.round_reasons(round_id)
            count = check.execute('SELECT count(*) FROM round_documents WHERE round_id=?', (round_id,)).fetchone()[0]
            limit = min(500, max(1, int(self.db_config.get('max_round_document_links', 300))))
            if reasons or count > limit:
                return reasons or ['oversized_lineage']
            if deep:
                snapshot = round_snapshot(check, round_id)
                hot = [turn for turn in snapshot['committee_turns'] if turn.get('raw_output')]
                hot_bytes = sum(len(turn['raw_output'].encode()) for turn in hot)
                from .bounded_memory import byte_cost, episode_stub
                memory = build_memory(snapshot)
                stub = episode_stub(memory, {'archive_path': 'reserved', 'kind': 'research_memory',
                    'key': memory['round_id']+':'+memory['source_hash'], 'revision': memory['source_hash']},
                    datetime.now(timezone.utc).isoformat())
                derived_bytes = byte_cost(stub, memory['topic'], memory['round_id'], memory['source_hash'])
                from .bounded_memory import BoundedMemoryStore
                derived_bytes += sum(byte_cost(value, layer, key, memory['topic'])
                    for layer, key, value in BoundedMemoryStore._extract_units(memory)
                    if len(json.dumps(value, ensure_ascii=False).encode()) < 7000)
                if hot_bytes <= derived_bytes + 1024*len(hot) + 4096:
                    return ['archive_would_not_reclaim_primary_bytes']
            return []
        except sqlite3.OperationalError as exc:
            if timed_out and str(exc) == 'interrupted':
                raise _RoundAuditBudgetExpired(round_id) from exc
            raise
        finally:
            check.close()

    async def _archive_closed_round(self) -> Dict[str, Any]:
        from .research_memory import ResearchCompactor, POLICY_VERSION, utc_time
        from .storage_archive import ArchiveStore
        retention = max(1, int(self.db_config.get('raw_hot_retention_days', 30)))
        deep = 'database' in self._recovery_targets
        cutoff = (datetime.now(timezone.utc) - timedelta(days=0 if deep else retention)).isoformat()
        retry_cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        conn = await self.database_service._get_connection()
        if self._research_utc_connection is not conn:
            await conn.create_function('research_utc', 1, lambda value: utc_time(value) if value else None)
            self._research_utc_connection = conn
        # A bounded complete round; oversized histories remain an offline job.
        after_clause = 'AND (r.completed_at,r.id)>(?,?)' if self._round_after else ''
        deferred_clause = '' if deep else '''AND NOT EXISTS(SELECT 1 FROM storage_jobs j WHERE j.kind='round'
                AND j.record_id=r.id AND j.policy_version=? AND j.state='deferred' AND j.updated_at>?)'''
        parameters = (cutoff, *([] if deep else [POLICY_VERSION+':evict', retry_cutoff]),
                      *(self._round_after or []))
        async with conn.execute('''SELECT r.id,r.completed_at FROM research_rounds r
            JOIN committee_meetings m ON m.round_id=r.id
            JOIN committee_turns t ON t.meeting_id=m.id
            WHERE r.status IN ('completed','failed') AND research_utc(r.completed_at) < ?
            AND length(t.raw_output)>0
            ''' + deferred_clause + ' ' + after_clause + '''
            GROUP BY r.id HAVING count(*)<=?
                AND sum(length(t.raw_output))<=8388608
            ORDER BY r.completed_at,r.id LIMIT 20''', (*parameters, 128 if deep else 32)) as cursor:
            candidates = [(row[0], row[1]) for row in await cursor.fetchall()]
        worker = ResearchCompactor(self.database_service, ArchiveStore(
            self.db_path.parent/'storage_archives'/'research.sqlite3',
            int(self.db_config.get('archive_max_bytes', 1024**3))), self.knowledge_root)
        worker.policy_version = POLICY_VERSION+':evict'
        audit_deadline = time.monotonic()+5
        # One round per pass; audit batches yield after their soft deadline.
        # Each root gets its own budget rather than the previous root's remainder.
        checked = 0
        for round_id, completed_at in candidates:
            if checked and time.monotonic() >= audit_deadline:
                break
            audit = self._round_audit_budget or {}
            seconds = audit.get('seconds', 5) if audit.get('round_id') == round_id else 5
            try:
                reasons = await asyncio.to_thread(self._audit_round_for_archive, round_id,
                                                 deep=deep, budget_seconds=seconds)
            except _RoundAuditBudgetExpired:
                if seconds < 20:
                    self._round_audit_budget = {'round_id': round_id, 'seconds': seconds*2}
                    await self._save_archive_state()
                    return {'status': 'audit_budget_exhausted', 'round_id': round_id,
                            'evicted_turns': 0, 'roots_checked': checked,
                            'scan_complete': False, 'candidates_remaining': len(candidates)-checked,
                            'retry_budget_seconds': seconds*2}
                # Repeatedly expensive roots remain intact. Move on so one
                # root cannot monopolize the 24-hour runner; report this limit.
                reasons = ['audit_budget_exhausted']
                self._round_audit_deferred_count += 1
            self._round_audit_budget = None
            checked += 1
            if not reasons:
                result = await worker.process_round(round_id, evict_hot=True, completed_before=cutoff)
                self._round_after = [completed_at, round_id]
                await self._save_archive_state()
                result['candidates_remaining'] = max(1, len(candidates)-checked)
                return result
            self._archive_deferred_rounds[round_id] = time.monotonic() + 3600
            await worker._job('round', round_id, 'deferred', error=','.join(reasons))
            self._round_after = [completed_at, round_id]
            await self._save_archive_state()
            if checked >= (20 if deep else 3):
                break
        complete = len(candidates) <= checked and len(candidates) < 20
        if complete:
            self._round_after = None
            await self._save_archive_state()
        return {'status': 'no_eligible_round', 'evicted_turns': 0, 'roots_checked': checked,
                'scan_complete': complete,
                'candidates_remaining': max(0, len(candidates)-checked) if complete else max(1, len(candidates)-checked)}

    async def after_round(self) -> Dict[str, Any]:
        # Publish completed experience even when capacity is below 80%, so the
        # next round benefits from learning without waiting for storage pressure.
        conn = await self.database_service._get_connection()
        async with conn.execute('''SELECT id FROM research_rounds WHERE status IN ('completed','failed')
            AND completed_at IS NOT NULL ORDER BY completed_at DESC,id DESC LIMIT 1''') as cur:
            latest = await cur.fetchone()
        learning = None
        if latest:
            async with conn.execute("SELECT 1 FROM storage_jobs WHERE kind='round' AND record_id=? AND policy_version=? AND state='completed'",
                                    (latest[0], 'lossless_memory_v1:archive')) as cur:
                known = await cur.fetchone()
            if not known:
                from .research_memory import ResearchCompactor
                from .storage_archive import ArchiveStore
                try:
                    learning = await ResearchCompactor(self.database_service, ArchiveStore(
                        self.db_path.parent/'storage_archives'/'research.sqlite3',
                        int(self.db_config.get('archive_max_bytes', 1024**3))),
                        self.knowledge_root).process_round(latest[0], evict_hot=False)
                except Exception as exc:
                    logger.warning('MEMORY_PUBLISH_DEFERRED round=%s reason=%s', latest[0], exc)
                    await self._rollback_owned_transaction()
        # Newly validated outcomes revise immutable experience versions, even
        # when the original raw turn has already left the hot store.
        from .research_memory import ResearchCompactor, utc_time
        from .storage_archive import ArchiveStore
        feedback_archive = ArchiveStore(self.db_path.parent/'storage_archives'/'research.sqlite3',
            int(self.db_config.get('archive_max_bytes', 1024**3)))
        async with conn.execute('''SELECT round_id,max(validated_at) FROM price_predictions
            WHERE status='validated' AND round_id IS NOT NULL AND validated_at IS NOT NULL
            GROUP BY round_id ORDER BY max(validated_at) DESC LIMIT 100''') as cur:
            outcomes = await cur.fetchall()
        feedback_updated = 0
        feedback_attempted = 0
        self._feedback_retry_after = {key: deadline for key,deadline in self._feedback_retry_after.items()
                                      if deadline > time.monotonic()}
        for round_id, validated_at in outcomes:
            if feedback_attempted >= 3:
                break
            if self._feedback_retry_after.get(round_id, 0) > time.monotonic():
                continue
            async with conn.execute('SELECT max(available_at) FROM research_memories WHERE round_id=?', (round_id,)) as cur:
                last = (await cur.fetchone())[0]
            last = last or await asyncio.to_thread(feedback_archive.latest_memory_time, round_id)
            try:
                cutoff = utc_time(validated_at)
                if last and cutoff <= utc_time(last):
                    continue
                if not last and cutoff < (datetime.now(timezone.utc)-timedelta(days=1)).isoformat():
                    continue
                feedback_attempted += 1
                await ResearchCompactor(self.database_service,feedback_archive,self.knowledge_root).process_round(
                    round_id,evict_hot=False)
                feedback_updated += 1
            except Exception as exc:
                self._feedback_retry_after[round_id] = time.monotonic()+3600
                logger.warning('MEMORY_FEEDBACK_DEFERRED round=%s reason=%s',round_id,exc)
                await self._rollback_owned_transaction()
        report = await self.online_maintenance()
        report['memory_feedback_updated'] = feedback_updated
        if learning is not None:
            report['learning'] = learning.get('memory')
        report["snapshot"] = (await self.snapshot(refresh=True)).to_dict()
        return report

    async def online_maintenance(self) -> Dict[str, Any]:
        """Run only short, auditable operations that are safe between rounds."""
        if self._lock.locked():
            return {"status": "already_running"}
        async with self._lock:
            async with self.database_service.connection_scope() as conn:
                if conn.in_transaction:
                    raise RuntimeError("storage maintenance requires a committed connection")
                started = time.monotonic()
                budget = max(0.1, float(self.db_config.get("online_maintenance_budget_seconds", 5)))
                deadline = started + budget
                before = database_bundle_size(self.db_path)
                busy_timeout = int((await _pragma(conn, "PRAGMA busy_timeout"))[0])
                await _pragma(conn, "PRAGMA busy_timeout=100")
                await conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                try:
                    report = await self._maintain_connection(conn, before, deadline)
                except sqlite3.OperationalError as exc:
                    if conn.in_transaction:
                        await conn.rollback()
                    report = {"status": "retry_later", "error": str(exc),
                              "snapshot_rows_removed": 0, "quote_rows_removed": 0}
                except BaseException:
                    if conn.in_transaction:
                        await conn.rollback()
                    raise
                finally:
                    await conn.set_progress_handler(None, 0)
                    await _pragma(conn, f"PRAGMA busy_timeout={busy_timeout}")
                after = database_bundle_size(self.db_path)
                report.update(before_bytes=before, after_bytes=after,
                              reclaimed_bytes=max(0, before-after),
                              elapsed_seconds=round(time.monotonic()-started, 3))
                self.last_maintenance = report
                self.invalidate_snapshot()
                logger.info("STORAGE_MAINTENANCE %s", report)
                return report

    async def _maintain_connection(self, conn, before: int, deadline: float) -> Dict[str, Any]:
        # The producer has stopped between rounds. Atomic pruning is
        # followed by physical page/WAL reclamation, rather than merely
        # repeating PASSIVE checkpoints against a retained 4 MiB WAL.
        batch = max(1, int(self.db_config.get("online_maintenance_batch_rows", 1000)))
        full_days = max(1, int(self.db_config.get("snapshot_full_retention_days", 30)))
        daily_days = max(full_days + 1, int(self.db_config.get("snapshot_daily_retention_days", 180)))
        quote_days = max(1, int(self.db_config.get("non_trade_quote_retention_days", 7)))

        await conn.execute("BEGIN IMMEDIATE")
        deleted_daily = await self._delete_redundant_snapshots(
            conn,
            cutoff=datetime.now() - timedelta(days=full_days),
            bucket="date(snapshot_date)",
            batch=batch,
        )
        deleted_monthly = await self._delete_redundant_snapshots(
            conn,
            cutoff=datetime.now() - timedelta(days=daily_days),
            bucket="substr(snapshot_date, 1, 7)",
            batch=batch,
        )
        deleted_quotes = await self._delete_unreferenced_quotes(conn, quote_days, batch)
        await conn.commit()

        checkpoint = await _pragma(conn, "PRAGMA wal_checkpoint(PASSIVE)")
        auto_vacuum = int((await _pragma(conn, "PRAGMA auto_vacuum"))[0])
        free_before = int((await _pragma(conn, "PRAGMA freelist_count"))[0])
        free_after = free_before
        pressure = ('database' in self._recovery_targets or
                    before >= int(self.db_config.get("max_bundle_size_bytes", 2 * 1024**3)) * float(
                        self.db_config.get("maintenance_ratio", 0.80)))
        if pressure and auto_vacuum == 2 and free_before and time.monotonic() < deadline:
            pages = max(1, min(4096, int(self.db_config.get("incremental_vacuum_pages", 4096))))
            # incremental_vacuum returns rows as pages are reclaimed.
            # Consuming the cursor is necessary to finish the bounded batch.
            remaining = min(pages, free_before)
            while remaining and time.monotonic() < deadline:
                async with conn.execute(f"PRAGMA incremental_vacuum({remaining})") as cursor:
                    await cursor.fetchall()
                current = int((await _pragma(conn, 'PRAGMA freelist_count'))[0])
                released = free_after-current
                free_after = current
                if released <= 0:
                    break
                remaining = max(0, remaining-released)
        truncate = None
        if pressure and time.monotonic() < deadline:
            truncate = await _pragma(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
        today = datetime.now().date().isoformat()
        optimized = False
        if self._last_optimize_date != today and not pressure and time.monotonic() < deadline:
            await _pragma(conn, "PRAGMA optimize")
            self._last_optimize_date = today
            optimized = True
        watermark = self._watermark(database_bundle_size(self.db_path), self.db_config, 2 * 1024**3)
        status = "completed"
        if not watermark.bulk_writes_allowed:
            if truncate and truncate[0]:
                status = "waiting_for_reader"
            elif free_after and auto_vacuum == 2:
                status = "reclaiming_pages"
            else:
                status = "capacity_wait"
        return {
            "status": status,
            "snapshot_rows_removed": deleted_daily + deleted_monthly,
            "quote_rows_removed": deleted_quotes,
            "wal_checkpoint": list(checkpoint) if checkpoint else [],
            "wal_truncate": list(truncate) if truncate else [],
            "free_pages_before": free_before,
            "free_pages_after": free_after,
            "pages_reclaimed": max(0, free_before-free_after),
            "auto_vacuum": auto_vacuum,
            "optimized": optimized,
        }

    async def _delete_redundant_snapshots(
        self,
        conn: Any,
        *,
        cutoff: datetime,
        bucket: str,
        batch: int,
    ) -> int:
        if not await self._table_exists(conn, "simulation_snapshots"):
            return 0
        cutoff_text = cutoff.isoformat()
        async with conn.execute(
            f"""
            SELECT id
            FROM simulation_snapshots
            WHERE created_at < ?
              AND id NOT IN (
                  SELECT MAX(id)
                  FROM simulation_snapshots
                  WHERE created_at < ?
                  GROUP BY {bucket}
              )
            ORDER BY id
            LIMIT ?
            """,
            (cutoff_text, cutoff_text, batch),
        ) as cursor:
            ids = [int(row[0]) for row in await cursor.fetchall()]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        await conn.execute(
            f"DELETE FROM simulation_snapshots WHERE id IN ({placeholders})",
            ids,
        )
        return len(ids)

    async def _delete_unreferenced_quotes(self, conn: Any, days: int, batch: int) -> int:
        if not await self._table_exists(conn, "quote_snapshots"):
            return 0
        trade_columns = await self._table_columns(conn, "simulation_trades")
        reference_guard = ""
        if "quote_snapshot_id" in trade_columns:
            reference_guard = (
                "AND id NOT IN (SELECT quote_snapshot_id FROM simulation_trades "
                "WHERE quote_snapshot_id IS NOT NULL)"
            )
        async with conn.execute(
            f"""
            SELECT id
            FROM quote_snapshots
            WHERE fetched_at < datetime('now', ?)
              {reference_guard}
            ORDER BY fetched_at
            LIMIT ?
            """,
            (f"-{days} days", batch),
        ) as cursor:
            ids = [str(row[0]) for row in await cursor.fetchall()]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        await conn.execute(f"DELETE FROM quote_snapshots WHERE id IN ({placeholders})", ids)
        return len(ids)

    async def _table_exists(self, conn: Any, table: str) -> bool:
        async with conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ) as cursor:
            return await cursor.fetchone() is not None

    async def _knowledge_size(self, *, refresh: bool) -> int:
        if self.knowledge_store is not None:
            return await asyncio.to_thread(
                self.knowledge_store.used_bytes,
                refresh=refresh,
            )
        return await asyncio.to_thread(directory_size, self.knowledge_root)

    async def _table_columns(self, conn: Any, table: str) -> set[str]:
        if not await self._table_exists(conn, table):
            return set()
        async with conn.execute(f"PRAGMA table_info({table})") as cursor:
            return {str(row[1]) for row in await cursor.fetchall()}

    async def round_importance_score(self, round_id: str) -> int:
        """Score a round's importance (0-100) from existing signals.

        Used to decide which old rounds get LLM-summarized into a digest vs
        deleted outright.  Signals:
        - validated+correct prediction: +30
        - executed execution_intent: +25
        - committee outcome not cancelled/incomplete: +15
        - reflection with lessons_learned: +15
        - key events (CommitteeDecisionsRecorded/PolicySnapshotActivated/ExperimentPromoted): +15
        - referenced by a later reflection's previous_conclusions: +10
        """
        conn = await self.database_service._get_connection()
        score = 0

        # Predictions: validated + correct
        if await self._table_exists(conn, "price_predictions"):
            async with conn.execute(
                '''SELECT COUNT(*) FROM price_predictions
                   WHERE round_id = ? AND status = 'validated' AND result = 'correct' ''',
                (round_id,)
            ) as cursor:
                if (await cursor.fetchone())[0] > 0:
                    score += 30

        # Execution intents: actually executed
        if await self._table_exists(conn, "execution_intents"):
            async with conn.execute(
                '''SELECT COUNT(*) FROM execution_intents
                   WHERE round_id = ? AND executed_at IS NOT NULL''',
                (round_id,)
            ) as cursor:
                if (await cursor.fetchone())[0] > 0:
                    score += 25

        # Committee meeting outcome
        if await self._table_exists(conn, "committee_meetings"):
            async with conn.execute(
                '''SELECT outcome_code FROM committee_meetings
                   WHERE round_id = ? AND outcome_code IS NOT NULL
                   LIMIT 1''',
                (round_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0] not in ("committee_cancelled", "committee_incomplete"):
                    score += 15

        # Reflection with lessons
        if await self._table_exists(conn, "reflection_summary"):
            async with conn.execute(
                '''SELECT COUNT(*) FROM reflection_summary
                   WHERE round_id = ? AND lessons_learned IS NOT NULL
                     AND lessons_learned != '' ''',
                (round_id,)
            ) as cursor:
                if (await cursor.fetchone())[0] > 0:
                    score += 15

        # Key events
        if await self._table_exists(conn, "round_events"):
            key_events = (
                "CommitteeDecisionsRecorded",
                "PolicySnapshotActivated",
                "ExperimentPromoted",
            )
            placeholders = ",".join("?" for _ in key_events)
            async with conn.execute(
                f'''SELECT COUNT(*) FROM round_events
                    WHERE round_id = ? AND event_type IN ({placeholders})''',
                (round_id, *key_events)
            ) as cursor:
                if (await cursor.fetchone())[0] > 0:
                    score += 15

        # Referenced by a later reflection's previous_conclusions
        if await self._table_exists(conn, "reflection_summary"):
            async with conn.execute(
                '''SELECT COUNT(*) FROM reflection_summary
                   WHERE round_id != ? AND previous_conclusions LIKE ?''',
                (round_id, f"%{round_id}%")
            ) as cursor:
                if (await cursor.fetchone())[0] > 0:
                    score += 10

        return min(score, 100)

    @staticmethod
    def _watermark(used: int, config: Dict[str, Any], default_max: int) -> StoreWatermark:
        max_bytes = int(
            config.get("max_bundle_size_bytes", config.get("max_size_bytes", default_max))
        )
        return StoreWatermark(
            used_bytes=used,
            max_bytes=max(1, max_bytes),
            target_ratio=float(config.get("target_size_ratio", 0.60)),
            maintenance_ratio=float(config.get("maintenance_ratio", 0.80)),
            bulk_write_stop_ratio=float(config.get("bulk_write_stop_ratio", 0.90)),
        )
