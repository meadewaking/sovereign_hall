from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta

import pytest

from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.core.config import get_config
from sovereign_hall.domain.research import ResearchRoundStatus
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_governor import (
    STORAGE_STATUS_FILENAME,
    StorageGovernor,
    StoreWatermark,
    production_source_mtime_ns,
    runtime_storage_read_model,
)
from sovereign_hall.services.wiki_knowledge import KnowledgeCapacityError, WikiStore


def test_compaction_checkpoints_once_after_all_invalidated_rounds(tmp_path):
    from sovereign_hall.scripts.compact_storage import invalidate_legacy_fanout

    db_path = tmp_path / "compact.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE round_documents(round_id TEXT, document_id TEXT);
        CREATE TABLE round_events(
            round_id TEXT, sequence INTEGER, event_type TEXT,
            payload_json TEXT, created_at TEXT
        );
        """
    )
    for round_id in ("round-a", "round-b"):
        conn.executemany(
            "INSERT INTO round_documents VALUES (?, ?)",
            [(round_id, f"doc-{index}") for index in range(3)],
        )
    conn.commit()

    trace = []
    conn.set_trace_callback(trace.append)
    removed = invalidate_legacy_fanout(
        conn,
        [("round-a", 3), ("round-b", 3)],
    )

    assert removed == 6
    assert conn.execute("SELECT COUNT(*) FROM round_documents").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM round_events WHERE event_type='HistoricalLineageInvalidated'"
    ).fetchone()[0] == 2
    checkpoints = [statement for statement in trace if "wal_checkpoint" in statement.lower()]
    assert len(checkpoints) == 1
    conn.close()


def test_store_watermark_uses_maintenance_and_bulk_write_thresholds():
    watermark = StoreWatermark(
        used_bytes=900,
        max_bytes=1000,
        target_ratio=0.70,
        maintenance_ratio=0.80,
        bulk_write_stop_ratio=0.90,
    )
    assert watermark.ratio == pytest.approx(0.9)
    assert watermark.needs_maintenance is True
    assert watermark.bulk_writes_allowed is False
    assert watermark.hard_limit_reached is False


def test_live_status_does_not_admit_research_during_deep_recovery_below_write_gate(tmp_path, monkeypatch):
    db_path = tmp_path/'db.sqlite3'
    with sqlite3.connect(db_path) as conn:
        conn.execute('CREATE TABLE evidence(id INTEGER)')
    source = tmp_path/'run_discussion.py'
    source.write_text('# source')
    lock = tmp_path/'run_discussion.lock'
    lock.write_text(f'pid={os.getpid()} started_at=2026-10-14T00:00:00 source_mtime_ns={source.stat().st_mtime_ns}\n')
    watermark = StoreWatermark(75, 100, .6, .8, .9).to_dict()
    (tmp_path/STORAGE_STATUS_FILENAME).write_text(json.dumps({
        'database': watermark, 'knowledge': watermark,
        'checked_at': datetime.now().astimezone().isoformat(), 'runner_pid': os.getpid(),
        'recovery_targets': ['database', 'knowledge'], 'research_round_allowed': False,
        'operation': {'state': 'maintaining', 'bulk_writes_paused': True}}))
    status = runtime_storage_read_model(db_path=db_path, knowledge_root=tmp_path/'knowledge',
                                       lock_path=lock, runner_source=source)
    assert status['status_source'] == 'production_snapshot'
    assert status['database']['bulk_writes_allowed']
    assert not status['research_round_allowed']


def test_wiki_store_refuses_growth_at_bulk_write_watermark(tmp_path, monkeypatch):
    config = get_config().get("knowledge_wiki", {})
    monkeypatch.setitem(config, "max_size_bytes", 16384)
    monkeypatch.setitem(config, "bulk_write_stop_ratio", 0.90)
    root = tmp_path / "knowledge"
    root.mkdir()
    (root / "existing.bin").write_bytes(b"x" * 7200)
    store = WikiStore(root)

    with pytest.raises(KnowledgeCapacityError, match="bulk-write watermark"):
        store.write_text(root / "new.md", "x" * 7000)


@pytest.mark.asyncio
async def test_round_source_lineage_has_hard_row_cap(tmp_path, monkeypatch):
    database_config = get_config().get("database", {})
    monkeypatch.setitem(database_config, "max_round_document_links", 2)
    db = DatabaseService(str(tmp_path / "lineage-cap.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="capacity",
        research_objective="bounded lineage",
    )
    await coordinator.advance(round_state.id, ResearchRoundStatus.MEMORY_LOADED)

    with pytest.raises(RuntimeError, match="exceeds configured hard limit"):
        await coordinator.persist_sources(
            round_state.id,
            document_ids=["one", "two", "three"],
            presented_document_count=3,
            traceable_document_count=3,
            untraceable_document_count=0,
        )
    await db.close()


@pytest.mark.asyncio
async def test_online_maintenance_downsamples_old_snapshots(tmp_path, monkeypatch):
    database_config = get_config().get("database", {})
    monkeypatch.setitem(database_config, "snapshot_full_retention_days", 30)
    monkeypatch.setitem(database_config, "snapshot_daily_retention_days", 180)
    monkeypatch.setitem(database_config, "online_maintenance_batch_rows", 100)
    monkeypatch.setitem(database_config, "max_bundle_size_bytes", 2 * 1024**3)
    monkeypatch.setitem(database_config, "max_main_file_bytes", 1984 * 1024**2)
    wiki_config = get_config().get("knowledge_wiki", {})
    monkeypatch.setitem(wiki_config, "max_size_bytes", 1024**3)

    db = DatabaseService(str(tmp_path / "maintenance.db"))
    await db._init_db()
    conn = await db._get_connection()
    await conn.execute(
        """
        CREATE TABLE simulation_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_assets REAL NOT NULL,
            cash REAL NOT NULL,
            positions_value REAL NOT NULL,
            reflection TEXT,
            snapshot_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            round_id TEXT
        )
        """
    )
    old = datetime.now() - timedelta(days=60)
    for index in range(3):
        await conn.execute(
            """
            INSERT INTO simulation_snapshots(
                total_assets, cash, positions_value, reflection,
                snapshot_date, created_at, round_id
            ) VALUES (10000, 5000, 5000, ?, ?, ?, NULL)
            """,
            (
                f"reflection-{index}",
                old.date().isoformat(),
                (old + timedelta(minutes=index)).isoformat(),
            ),
        )
    await conn.commit()

    governor = StorageGovernor(db, tmp_path / "knowledge")
    await governor.initialize()
    report = await governor.online_maintenance()
    assert report["snapshot_rows_removed"] == 2
    async with conn.execute("SELECT COUNT(*) FROM simulation_snapshots") as cursor:
        assert int((await cursor.fetchone())[0]) == 1
    await db.close()


@pytest.mark.asyncio
async def test_storage_snapshot_is_persisted_for_check_db(tmp_path, monkeypatch):
    database_config = get_config().get("database", {})
    monkeypatch.setitem(database_config, "max_bundle_size_bytes", 1024**3)
    wiki_config = get_config().get("knowledge_wiki", {})
    monkeypatch.setitem(wiki_config, "max_size_bytes", 1024**3)
    knowledge = tmp_path / "knowledge"
    knowledge.mkdir()
    (knowledge / "evidence.md").write_text("durable evidence", encoding="utf-8")
    db = DatabaseService(str(tmp_path / "storage-read-model.db"))
    await db._init_db()

    snapshot = await StorageGovernor(db, knowledge).initialize()

    status_path = tmp_path / STORAGE_STATUS_FILENAME
    assert status_path.exists()
    persisted = json.loads(status_path.read_text(encoding="utf-8"))
    assert persisted["database"]["used_bytes"] == snapshot.database.used_bytes
    assert persisted["knowledge"]["used_bytes"] == snapshot.knowledge.used_bytes
    assert persisted["runner_pid"] > 0
    await db.close()


def test_runtime_storage_read_model_detects_stale_live_runner(tmp_path):
    db_path = tmp_path / "runtime.db"
    db_path.write_bytes(b"sqlite-placeholder")
    knowledge = tmp_path / "knowledge"
    knowledge.mkdir()
    runner_source = tmp_path / "run_discussion.py"
    runner_source.write_text("# updated source", encoding="utf-8")
    started_at = datetime.fromtimestamp(runner_source.stat().st_mtime - 60).isoformat()
    lock_path = tmp_path / "run_discussion.lock"
    lock_path.write_text(
        f"pid={os.getpid()} started_at={started_at}\n",
        encoding="utf-8",
    )

    status = runtime_storage_read_model(
        db_path=db_path,
        knowledge_root=knowledge,
        lock_path=lock_path,
        runner_source=runner_source,
        cache_max_age_seconds=1,
    )

    assert status["runner"]["pid_alive"] is True
    assert status["runner"]["code_stale"] is True
    assert "stale_runner_code" in status["blockers"]


def test_runtime_source_fingerprint_detects_changed_imported_service(tmp_path):
    runner_source = tmp_path / "run_discussion.py"
    runner_source.write_text("# runner", encoding="utf-8")
    services = tmp_path / "services"
    services.mkdir()
    dependency = services / "execution.py"
    dependency.write_text("# imported dependency", encoding="utf-8")
    runner_mtime_ns = runner_source.stat().st_mtime_ns
    dependency_mtime_ns = runner_mtime_ns + 10_000_000
    os.utime(dependency, ns=(dependency_mtime_ns, dependency_mtime_ns))

    assert production_source_mtime_ns(runner_source) == dependency_mtime_ns

    lock_path = tmp_path / "run_discussion.lock"
    lock_path.write_text(
        f"pid={os.getpid()} started_at={datetime.now().isoformat()} "
        f"source_mtime_ns={runner_mtime_ns}\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "runtime.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE research_rounds(id TEXT, status TEXT, terminal_code TEXT)"
        )
        conn.execute(
            "INSERT INTO research_rounds VALUES ('round-active', 'memory_loaded', NULL)"
        )
    knowledge = tmp_path / "knowledge"
    knowledge.mkdir()

    status = runtime_storage_read_model(
        db_path=db_path,
        knowledge_root=knowledge,
        lock_path=lock_path,
        runner_source=runner_source,
        cache_max_age_seconds=1,
    )

    assert status["runner"]["pid_alive"] is True
    assert status["runner"]["lock_source_mtime_ns"] == runner_mtime_ns
    assert status["runner"]["runner_source_mtime_ns"] == dependency_mtime_ns
    assert status["runner"]["code_stale"] is True
    assert "stale_runner_code" in status["blockers"]
    assert status["unterminated_round_count"] == 1
    assert "orphaned_rounds_pending_startup_recovery" not in status["blockers"]


def test_runtime_storage_read_model_rejects_dead_runner_snapshot_and_surfaces_recovery(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE research_rounds(
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                terminal_code TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO research_rounds VALUES ('round-orphan', 'memory_loaded', NULL)"
        )
    knowledge = tmp_path / "knowledge"
    knowledge.mkdir()
    runner_source = tmp_path / "run_discussion.py"
    runner_source.write_text("# current source", encoding="utf-8")
    lock_path = tmp_path / "run_discussion.lock"
    lock_path.write_text(
        "pid=424242 started_at=2026-08-15T14:00:00\n",
        encoding="utf-8",
    )
    cached_watermark = {
        "used_bytes": 1,
        "max_bytes": 1024**3,
        "target_ratio": 0.7,
        "maintenance_ratio": 0.8,
        "bulk_write_stop_ratio": 0.9,
        "ratio": 1 / 1024**3,
        "needs_maintenance": False,
        "bulk_writes_allowed": True,
        "hard_limit_reached": False,
    }
    (tmp_path / STORAGE_STATUS_FILENAME).write_text(
        json.dumps(
            {
                "database": cached_watermark,
                "knowledge": cached_watermark,
                "checked_at": datetime.now().astimezone().isoformat(),
                "research_round_allowed": True,
                "runner_pid": 424242,
            }
        ),
        encoding="utf-8",
    )

    def dead_pid(_pid, _signal):
        raise ProcessLookupError

    monkeypatch.setattr("sovereign_hall.services.storage_governor.os.kill", dead_pid)
    status = runtime_storage_read_model(
        db_path=db_path,
        knowledge_root=knowledge,
        lock_path=lock_path,
        runner_source=runner_source,
    )

    assert status["status_source"] == "direct_scan"
    assert status["database"]["used_bytes"] != 1
    assert status["runner"]["pid_alive"] is False
    assert status["runner"]["stale_lock"] is True
    assert status["unterminated_round_count"] == 1
    assert status["automatic_recovery"] == "canonical_run_discussion_startup"
    assert "production_runner_not_running" in status["blockers"]
    assert "orphaned_rounds_pending_startup_recovery" in status["blockers"]
    assert status["health"] == "blocked"
