from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import sqlite3
import time

import pytest

from sovereign_hall.core.config import get_config
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_governor import (
    StorageGovernor, database_bundle_size, storage_pressure_message,
    storage_capacity_state, StoreWatermark, StorageSnapshot,
)


@pytest.fixture
async def storage(tmp_path, monkeypatch):
    cfg = get_config().get('database', {})
    monkeypatch.setitem(cfg, 'online_maintenance_budget_seconds', 0.5)
    monkeypatch.setitem(cfg, 'incremental_vacuum_pages', 128)
    path = tmp_path / 'storage.db'
    with sqlite3.connect(path) as bootstrap:
        bootstrap.execute('PRAGMA auto_vacuum=INCREMENTAL')
    db = DatabaseService(str(path))
    try:
        await db._init_db()
        conn = await db._get_connection()
        await conn.execute('PRAGMA wal_autocheckpoint=0')
        await conn.execute('CREATE TABLE durable_evidence(id INTEGER PRIMARY KEY,body BLOB)')
        await conn.execute('INSERT INTO durable_evidence VALUES (1,?)', (b'original evidence'*40000,))
        await conn.commit()
        await (await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')).fetchall()
        yield db, conn, cfg
    finally:
        await db.close()


def pressure_from_retained_wal(db, cfg, monkeypatch):
    main = db.db_path.stat().st_size
    total = database_bundle_size(db.db_path)
    assert total > main
    # Real data fits; only a retained WAL pushes this fixture over the gate.
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', int((main+total)/2/0.9))


async def test_before_round_releases_retained_wal_and_resumes_without_deleting_evidence(storage, monkeypatch):
    db, conn, cfg = storage
    pressure_from_retained_wal(db, cfg, monkeypatch)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    before = await governor.initialize()
    assert not before.research_round_allowed
    after = await governor.before_round()
    assert after.research_round_allowed
    assert governor.last_maintenance['wal_truncate'][0] == 0
    assert governor.last_maintenance['reclaimed_bytes'] > 0
    async with conn.execute('SELECT body FROM durable_evidence') as cursor:
        assert (await cursor.fetchone())[0] == b'original evidence'*40000
    import json
    status = json.loads(governor.status_path.read_text())
    assert status['research_round_allowed']
    assert status['maintenance']['reclaimed_bytes'] > 0


async def test_vacuum_consumes_bounded_cursor_and_physically_releases_multiple_pages(storage, monkeypatch):
    db, conn, cfg = storage
    await conn.execute('INSERT INTO durable_evidence VALUES (2,?)', (b'temporary'*150000,))
    await conn.commit()
    await (await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')).fetchall()
    await conn.execute('DELETE FROM durable_evidence WHERE id=2')
    await conn.commit()
    await (await conn.execute('PRAGMA wal_checkpoint(PASSIVE)')).fetchall()
    before_pages = (await (await conn.execute('PRAGMA freelist_count')).fetchone())[0]
    assert before_pages > 128
    before_main = db.db_path.stat().st_size
    pressure_from_retained_wal(db, cfg, monkeypatch)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    await governor.before_round()
    report = governor.last_maintenance
    assert report['pages_reclaimed'] == 128
    assert db.db_path.stat().st_size < before_main
    assert report['free_pages_after'] == before_pages-128
    async with conn.execute('SELECT count(*) FROM durable_evidence') as cursor:
        assert (await cursor.fetchone())[0] == 1


async def test_reader_prevents_truncate_without_hanging_then_release_recovers(storage, monkeypatch):
    db, conn, cfg = storage
    reader = sqlite3.connect(db.db_path)
    try:
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM durable_evidence').fetchone()
        await conn.execute('UPDATE durable_evidence SET body=? WHERE id=1', (b'new durable evidence'*40000,))
        await conn.commit()
        pressure_from_retained_wal(db, cfg, monkeypatch)
        governor = StorageGovernor(db, db.db_path.parent/'knowledge')
        await governor.initialize()
        start = time.monotonic()
        await governor.before_round()
        assert time.monotonic()-start < 1.5
        assert governor.last_maintenance['status'] == 'waiting_for_reader'
        assert '读取连接' in storage_pressure_message(await governor.snapshot(), governor.last_maintenance)
        reader.rollback()
        after = await governor.before_round()
        assert after.research_round_allowed
        async with conn.execute('SELECT body FROM durable_evidence') as cursor:
            assert (await cursor.fetchone())[0] == b'new durable evidence'*40000
    finally:
        reader.close()


async def test_irreducible_main_file_waits_online_without_claiming_offline_requirement(storage, monkeypatch):
    db, _, cfg = storage
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', db.db_path.stat().st_size)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    snapshot = await governor.before_round()
    assert not snapshot.research_round_allowed
    assert governor.last_maintenance['status'] == 'capacity_wait'
    message = storage_pressure_message(snapshot, governor.last_maintenance)
    assert '进程保持运行' in message
    assert '离线' not in message
    assert '存储整理中' not in message


async def test_maintenance_deadline_rolls_back_partial_pruning_and_restores_connection(storage, monkeypatch):
    db, conn, cfg = storage
    monkeypatch.setitem(cfg, 'online_maintenance_budget_seconds', 0.1)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    async def slow_prune(*_, **__):
        await conn.execute('DELETE FROM durable_evidence')
        await (await conn.execute('''WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL
            SELECT x+1 FROM n WHERE x<10000000) SELECT SUM(x) FROM n''')).fetchone()
        return 1
    monkeypatch.setattr(governor, '_delete_redundant_snapshots', slow_prune)
    start = time.monotonic()
    report = await governor.online_maintenance()
    assert time.monotonic()-start < 1.5
    assert report['status'] == 'retry_later'
    assert not conn.in_transaction
    async with conn.execute('SELECT count(*) FROM durable_evidence') as cursor:
        assert (await cursor.fetchone())[0] == 1
    assert (await (await conn.execute('PRAGMA busy_timeout')).fetchone())[0] == 5000


async def test_cancelled_maintenance_rolls_back_and_releases_lock(storage, monkeypatch):
    db, conn, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    started = asyncio.Event()
    async def interrupted_prune(*_, **__):
        await conn.execute('DELETE FROM durable_evidence')
        started.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(governor, '_delete_redundant_snapshots', interrupted_prune)
    task = asyncio.create_task(governor.online_maintenance())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not governor._lock.locked()
    assert not conn.in_transaction
    assert (await (await conn.execute('SELECT count(*) FROM durable_evidence')).fetchone())[0] == 1


async def test_maintenance_only_closes_connection_without_starting_models(storage, monkeypatch, capsys):
    db, _, _ = storage
    import sovereign_hall.run_discussion as runner
    import sovereign_hall.services.llm_client as llm_module
    from sovereign_hall.services.storage_archive import consistent_backup
    root = db.db_path.parent/'recovery_entry'
    target = root/'data'/'sovereign_hall.db'
    consistent_backup(db.db_path, target)
    monkeypatch.setattr(runner, 'project_root', root)
    monkeypatch.setattr(runner, 'parse_args', lambda: runner.argparse.Namespace(storage_maintenance_only=True))
    def forbid_models(*_, **__):
        raise AssertionError('Local storage recovery must not start a model client')
    monkeypatch.setattr(llm_module, 'LLMClient', forbid_models)
    await runner.main()
    assert '"maintenance"' in capsys.readouterr().out
    assert DatabaseService(str(target))._connection is None


@pytest.mark.parametrize('status,archive,state', [
    ('capacity_wait', {}, 'waiting_for_capacity'),
    ('capacity_wait', {'progressed': True}, 'maintaining'),
    ('capacity_wait', {'progressed': False, 'status': 'archive_blocked'}, 'maintenance_error'),
    ('capacity_wait', {'wiki': {'budget_exhausted': True}}, 'maintaining'),
    ('waiting_for_reader', {}, 'waiting_for_reader'),
    ('reclaiming_pages', {}, 'maintaining'),
    ('retry_later', {}, 'maintaining'),
])
def test_pressure_states_pause_writes_and_keep_the_service_alive(status, archive, state):
    full = StoreWatermark(91, 100, .7, .8, .9)
    snapshot = StorageSnapshot(full, full, 'now')
    assert storage_capacity_state(snapshot, {'status': status, 'archive': archive}) == state


async def test_archive_no_candidates_is_backed_off_instead_of_repeated_scans(storage, monkeypatch):
    db, _, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    full = StoreWatermark(91, 100, .7, .8, .9)
    allowed = StoreWatermark(0, 100, .7, .8, .9)
    calls = []
    async def no_candidates():
        calls.append(1)
        return {'evicted_turns': 0}
    monkeypatch.setattr(governor, '_archive_closed_round', no_candidates)
    snapshot = StorageSnapshot(full, allowed, 'now')
    assert not (await governor.archive_history(snapshot))['progressed']
    assert (await governor.archive_history(snapshot))['status'] == 'backoff'
    assert len(calls) == 1


async def test_budget_exhaustion_preserves_online_scan_and_never_enters_offline_backoff(storage, monkeypatch):
    import sovereign_hall.services.wiki_archive as wiki_module
    db, _, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    full = StoreWatermark(91, 100, .7, .8, .9)
    allowed = StoreWatermark(0, 100, .7, .8, .9)
    snapshot = StorageSnapshot(allowed, full, 'now')
    seen = []
    def batch(*_, **kwargs):
        seen.append(kwargs['after_name'])
        return {'raw_removed': 0, 'retained_changed': 0, 'after_bytes': 91,
                'budget_exhausted': True, 'scan_complete': False, 'next_after': 'prefix.md'}
    monkeypatch.setattr(wiki_module, 'pack_raw', batch)
    first = await governor.archive_history(snapshot)
    second = await governor.archive_history(snapshot)
    assert first['can_continue'] and second['can_continue']
    assert governor._archive_retry_after == 0
    assert seen == [None, 'prefix.md']
    assert storage_capacity_state(snapshot, {'status': 'completed', 'archive': second}) == 'maintaining'
    assert '离线' not in storage_pressure_message(snapshot, {'archive': second})
    restarted = StorageGovernor(db, db.db_path.parent/'knowledge')
    assert restarted._wiki_archive_after == 'prefix.md'


async def test_admission_rechecks_actual_wiki_bytes_before_stopping_on_cached_usage(storage, monkeypatch):
    db, _, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    monkeypatch.setitem(governor.wiki_config, 'max_size_bytes', 100)
    calls = []
    async def size(*, refresh=False):
        calls.append(refresh)
        return 50 if refresh else 91
    monkeypatch.setattr(governor, '_knowledge_size', size)
    snapshot = await governor.before_round()
    assert snapshot.research_round_allowed
    assert calls == [False, True]
    assert governor.last_maintenance == {}


async def test_capacity_wait_keeps_connection_alive_and_automatically_resumes(storage, monkeypatch, caplog):
    import sovereign_hall.run_discussion as runner
    import json
    caplog.set_level('INFO', logger='sovereign_hall.run_discussion')
    db, conn, cfg = storage
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', db.db_path.stat().st_size)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    original = governor.before_round
    waiting = asyncio.Event()
    calls = 0
    async def check():
        nonlocal calls
        snapshot = await original()
        calls += 1
        if calls >= 2 and not snapshot.research_round_allowed:
            waiting.set()
        return snapshot
    monkeypatch.setattr(governor, 'before_round', check)
    task = asyncio.create_task(runner.await_storage_capacity(governor, .01))
    try:
        await asyncio.wait_for(waiting.wait(), 3)
        assert not task.done()
        assert db._connection is conn
        status = json.loads(governor.status_path.read_text())
        assert status['operation']['state'] == 'waiting_for_capacity'
        assert status['operation']['automatic_resume']
        assert not status['operation']['process_exit_required']
        monkeypatch.setitem(cfg, 'max_bundle_size_bytes', 2*1024**3)
        resumed = await asyncio.wait_for(task, 3)
        assert resumed.research_round_allowed
        assert db._connection is conn
        assert (await (await conn.execute('SELECT count(*) FROM durable_evidence')).fetchone())[0] == 1
        assert len([r for r in caplog.records if 'STORAGE_PRESSURE' in r.message]) == 1
        assert any('STORAGE_RECOVERED' in r.message for r in caplog.records)
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


async def test_capacity_wait_remains_cancellable_for_graceful_shutdown(storage, monkeypatch):
    import sovereign_hall.run_discussion as runner
    db, _, cfg = storage
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', db.db_path.stat().st_size)
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    original = governor.before_round
    waiting = asyncio.Event()
    async def check():
        snapshot = await original()
        waiting.set()
        return snapshot
    monkeypatch.setattr(governor, 'before_round', check)
    task = asyncio.create_task(runner.await_storage_capacity(governor, 60))
    await asyncio.wait_for(waiting.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert not governor._lock.locked()


async def test_new_databases_support_incremental_reclamation_without_full_rebuild(tmp_path):
    db = DatabaseService(str(tmp_path/'new.db'))
    try:
        await db._init_db()
        conn = await db._get_connection()
        assert (await (await conn.execute('PRAGMA auto_vacuum')).fetchone())[0] == 2
    finally:
        await db.close()


async def test_online_page_batches_resume_research_without_restart_or_full_vacuum(storage, monkeypatch):
    import sovereign_hall.run_discussion as runner
    db, conn, cfg = storage
    await conn.execute('INSERT INTO durable_evidence VALUES (2,?)', (b'temporary'*1000000,))
    await conn.commit()
    await (await conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')).fetchall()
    await conn.execute('DELETE FROM durable_evidence WHERE id=2')
    await conn.commit()
    await (await conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')).fetchall()
    page_size = (await (await conn.execute('PRAGMA page_size')).fetchone())[0]
    assert (await (await conn.execute('PRAGMA freelist_count')).fetchone())[0] > 600
    gate = db.db_path.stat().st_size - 400*page_size
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', int(gate/.9))
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    await governor.initialize()
    sql = []
    await conn.set_trace_callback(sql.append)
    inode = db.db_path.stat().st_ino
    resumed = await asyncio.wait_for(runner.await_storage_capacity(governor, .01), 3)
    assert resumed.research_round_allowed
    assert db.db_path.stat().st_ino == inode
    assert db._connection is conn
    assert sum('incremental_vacuum' in statement.lower() for statement in sql) >= 4
    assert not any(statement.strip().upper() == 'VACUUM' for statement in sql)
    assert (await (await conn.execute('SELECT count(*) FROM durable_evidence')).fetchone())[0] == 1


async def test_archive_failure_rolls_back_and_leaves_connection_usable_for_continuous_maintenance(storage, monkeypatch):
    db, conn, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    async def fail():
        await conn.execute("INSERT INTO durable_evidence VALUES (2,'uncommitted')")
        raise sqlite3.OperationalError('injected archive capacity failure')
    monkeypatch.setattr(governor, '_archive_closed_round', fail)
    full = StoreWatermark(91, 100, .7, .8, .9)
    empty = StoreWatermark(0, 100, .7, .8, .9)
    report = await governor.archive_history(StorageSnapshot(full, empty, 'now'))
    assert report['status'] == 'archive_blocked'
    assert not conn.in_transaction
    assert (await (await conn.execute('SELECT count(*) FROM durable_evidence')).fetchone())[0] == 1
    await governor.online_maintenance()
    assert db._connection is conn


async def test_archive_failure_does_not_rollback_concurrent_account_owner(storage, monkeypatch):
    db, conn, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    start, writing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def account():
        await start.wait()
        async with db.transaction(immediate=True) as active:
            await active.execute("INSERT INTO durable_evidence VALUES (2,'account evidence')")
            writing.set()
            await release.wait()

    async def fail():
        start.set()
        await writing.wait()
        raise sqlite3.OperationalError('archive failed while account owns transaction')

    monkeypatch.setattr(governor, '_archive_closed_round', fail)
    task = asyncio.create_task(account())
    full = StoreWatermark(91, 100, .7, .8, .9)
    empty = StoreWatermark(0, 100, .7, .8, .9)
    try:
        report = await asyncio.wait_for(governor.archive_history(StorageSnapshot(full, empty, 'now')), 5)
        assert report['status'] == 'archive_blocked'
        assert conn.in_transaction
        release.set()
        await task
        async with conn.execute('SELECT body FROM durable_evidence WHERE id=2') as cursor:
            assert (await cursor.fetchone())[0] == 'account evidence'
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_deep_recovery_continues_to_sixty_percent_while_research_resumes_at_lower_watermark(storage, monkeypatch):
    import sovereign_hall.services.wiki_archive as module
    db, _, _ = storage
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    monkeypatch.setitem(governor.wiki_config, 'max_size_bytes', 1000000)
    monkeypatch.setitem(governor.wiki_config, 'target_size_ratio', .6)
    level = [950000]
    async def size(*, refresh=False):
        return level[0]
    monkeypatch.setattr(governor, '_knowledge_size', size)
    targets = []
    def batch(*_, **kwargs):
        targets.append(kwargs['target_bytes'])
        return {'raw_removed': 0, 'retained_changed': 0, 'after_bytes': level[0],
                'scan_complete': False, 'budget_exhausted': True, 'next_after': 'cursor.md'}
    monkeypatch.setattr(module, 'pack_raw', batch)
    initial = await governor.initialize()
    assert initial.recovery_targets == ('knowledge',)
    level[0] = 850000
    middle = await governor.before_round()
    assert middle.knowledge.bulk_writes_allowed
    assert not middle.research_round_allowed
    assert middle.resume_pending == ('knowledge',)
    assert middle.recovery_targets == ('knowledge',)
    restarted = StorageGovernor(db, db.db_path.parent/'knowledge')
    assert restarted._recovery_targets == ['knowledge']
    level[0] = 650000
    continuing = await governor.before_round()
    assert continuing.research_round_allowed and continuing.recovery_targets == ('knowledge',)
    assert targets == [600000, 600000]
    level[0] = 590000
    finished = await governor.before_round()
    assert finished.research_round_allowed
    assert governor._recovery_result['target_met']


async def test_exhausted_safe_candidates_report_target_unreachable_without_fake_success(storage, monkeypatch):
    db, _, cfg = storage
    monkeypatch.setitem(cfg, 'resume_write_ratio',.85)
    # Actual durable data is safely below the write gate but above the target.
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', int(db.db_path.stat().st_size/.82))
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    governor._recovery_targets = ['database']
    snapshot = await governor.before_round()
    assert snapshot.research_round_allowed
    assert snapshot.database.ratio > snapshot.database.target_ratio
    assert governor._recovery_result['state'] == 'protected_floor'
    assert not governor._recovery_result['target_met']


async def test_budget_deferred_roots_resume_below_write_gate_with_honest_incomplete_audit_status(storage, monkeypatch):
    db, _, cfg = storage
    monkeypatch.setitem(cfg, 'resume_write_ratio',.85)
    monkeypatch.setitem(cfg, 'max_bundle_size_bytes', int(db.db_path.stat().st_size/.82))
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    governor._recovery_targets = ['database']
    governor._round_audit_deferred_count = 1
    snapshot = await governor.before_round()
    assert snapshot.research_round_allowed
    assert not governor._recovery_result['target_met']
    assert governor._recovery_result['reason'] == 'audit_budget_deferred'
    assert governor._recovery_result['audit_deferred_rounds'] == 1
