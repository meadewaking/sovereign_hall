"""Real SQLite failure/cancellation scenarios for the continuous runner."""
import asyncio
import sqlite3
from unittest.mock import AsyncMock

import aiosqlite
import pytest

from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.core import Document
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_governor import StorageGovernor


@pytest.fixture
async def db(tmp_path):
    service = DatabaseService(str(tmp_path/'reliability.db'))
    await service._init_db()
    try:
        yield service
    finally:
        await service.close()


async def scalar(conn, sql):
    async with conn.execute(sql) as cursor:
        return (await cursor.fetchone())[0]


async def test_concurrent_initial_use_has_one_configured_connection(tmp_path):
    service = DatabaseService(str(tmp_path/'initial.db'))
    try:
        connections = await asyncio.gather(*(service._get_connection() for _ in range(20)))
        assert len({id(conn) for conn in connections}) == 1
        await asyncio.gather(*(service._ensure_initialized() for _ in range(5)))
        assert await scalar(connections[0], 'PRAGMA foreign_keys') == 1
        assert await scalar(connections[0], 'PRAGMA journal_mode') == 'wal'
        assert await scalar(connections[0], 'PRAGMA auto_vacuum') == 2
    finally:
        await service.close()


async def test_deferred_constraint_failure_rolls_back_before_next_transaction(db):
    conn = db._connection
    await conn.execute('CREATE TABLE parent_test(id INTEGER PRIMARY KEY)')
    await conn.execute('CREATE TABLE child_test(parent INTEGER REFERENCES parent_test(id) DEFERRABLE INITIALLY DEFERRED)')
    with pytest.raises(sqlite3.IntegrityError):
        async with db.transaction(immediate=True):
            await conn.execute('INSERT INTO child_test VALUES(9)')
    assert not conn.in_transaction
    assert await scalar(conn, 'SELECT COUNT(*) FROM child_test') == 0
    async with db.transaction(immediate=True):
        await conn.execute('INSERT INTO parent_test VALUES(9)')
        await conn.execute('INSERT INTO child_test VALUES(9)')
    assert await scalar(conn, 'SELECT COUNT(*) FROM child_test') == 1


async def test_cancelling_queued_begin_does_not_leave_the_connection_writing(db):
    entered = asyncio.Event()
    async with aiosqlite.connect(db.db_path) as writer:
        await writer.execute('BEGIN IMMEDIATE')
        async def write():
            async with db.transaction(immediate=True):
                entered.set()
        task = asyncio.create_task(write())
        await asyncio.sleep(.05)
        assert not entered.is_set()
        task.cancel()
        await writer.rollback()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert not entered.is_set() and not db._connection.in_transaction
    async with db.transaction(immediate=True) as conn:
        await conn.execute("INSERT INTO system_stats(key,value) VALUES('after_cancel','ok')")


async def test_legacy_writes_serialize_and_cancelled_document_batch_is_atomic(db, monkeypatch):
    round_state = await ResearchRoundCoordinator(db).start(base_topic='atomic', research_objective='atomic')
    linked = asyncio.Event()
    original = db._link_round_document
    async def pause(*args, **kwargs):
        await original(*args, **kwargs)
        linked.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(db, '_link_round_document', pause)
    doc = Document(id='cancelled-document', title='Cancellation regression source',
                   content='Original evidence must roll back together with its round lineage.',
                   source='fixture', url='https://example.test/evidence')
    batch = asyncio.create_task(db.add_documents_batch([doc], round_id=round_state.id))
    await asyncio.wait_for(linked.wait(), 2)
    checkpoint = asyncio.create_task(db.save_checkpoint(7, {'fixture':True}, []))
    await asyncio.sleep(.01)
    assert not checkpoint.done()
    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch
    await checkpoint
    assert await scalar(db._connection, 'SELECT COUNT(*) FROM documents') == 0
    assert await scalar(db._connection, 'SELECT COUNT(*) FROM round_documents') == 0
    assert (await db.get_latest_checkpoint())['iteration'] == 7
    await asyncio.gather(*(db.add_to_blacklist('fixture', 'parallel increment') for _ in range(20)))
    assert await scalar(db._connection, "SELECT failure_count FROM blacklist WHERE ticker='fixture'") == 20


async def test_connection_maintenance_waits_for_transaction_without_committing_it(db, monkeypatch):
    governor = StorageGovernor(db, db.db_path.parent/'knowledge')
    started = asyncio.Event()
    release = asyncio.Event()
    async def write():
        async with db.transaction(immediate=True) as conn:
            await conn.execute("INSERT INTO system_stats(key,value) VALUES('rolled_back','x')")
            started.set()
            await release.wait()
            raise ValueError('rollback fixture')
    writer = asyncio.create_task(write())
    await started.wait()
    maintenance = asyncio.create_task(governor.online_maintenance())
    await asyncio.sleep(.01)
    assert not maintenance.done()
    release.set()
    with pytest.raises(ValueError):
        await writer
    await maintenance
    assert await scalar(db._connection, "SELECT COUNT(*) FROM system_stats WHERE key='rolled_back'") == 0


async def test_round_admission_recovers_from_real_writer_contention(db):
    from sovereign_hall.run_discussion import start_research_round_when_ready
    waits = []
    await db._connection.execute('PRAGMA busy_timeout=10')
    async with aiosqlite.connect(db.db_path) as writer:
        await writer.execute('BEGIN IMMEDIATE')
        async def pause(seconds, *, context):
            waits.append(context)
            await writer.rollback()
        duty = type('Duty', (), {'sleep':staticmethod(pause)})()
        created = await start_research_round_when_ready(ResearchRoundCoordinator(db), duty,
            pause_seconds=.01, base_topic='lock', research_objective='recover')
    assert waits == ['database_write_wait']
    assert await scalar(db._connection, 'SELECT COUNT(*) FROM research_rounds') == 1
    assert await scalar(db._connection, 'SELECT COUNT(*) FROM round_events') == 1
    assert await ResearchRoundCoordinator(db).get(created.id)


async def test_checkpoint_cleanup_retains_latest_rows(db):
    for number in range(5):
        await db.save_checkpoint(number, {}, [])
    await db.clear_old_checkpoints(keep=2)
    assert await scalar(db._connection, 'SELECT COUNT(*) FROM checkpoints') == 2
    assert (await db.get_latest_checkpoint())['iteration'] == 4


async def test_secondary_close_keeps_the_live_path_scoped_instance(tmp_path):
    owner = await DatabaseService.get_instance(str(tmp_path/'owner.db'))
    await owner._init_db()
    secondary = DatabaseService(str(owner.db_path))
    await secondary._get_connection()
    await secondary.close()
    try:
        assert await DatabaseService.get_instance(str(owner.db_path)) is owner
        async with owner.transaction():
            with pytest.raises(RuntimeError, match='cannot close'):
                await owner.close()
    finally:
        await owner.close()


async def test_supervisor_reopens_only_after_failed_attempt_releases_resources(tmp_path, monkeypatch):
    import sovereign_hall.run_discussion as runner
    path = tmp_path/'restart.db'
    attempts = []
    async def workload(resources):
        db = DatabaseService(str(path))
        resources.push_async_callback(db.close)
        await db._init_db()
        await db._connection.execute('PRAGMA busy_timeout=10')
        attempts.append(db._connection)
        if len(attempts) == 1:
            async with aiosqlite.connect(path) as writer:
                await writer.execute('BEGIN IMMEDIATE')
                await ResearchRoundCoordinator(db).start(base_topic='blocked', research_objective='retry')
        else:
            with pytest.raises(ValueError, match='closed|active connection'):
                await attempts[0].execute('SELECT 1')
            await ResearchRoundCoordinator(db).start(base_topic='recovered', research_objective='new round')
    monkeypatch.setattr(runner, '_main', workload)
    await runner.run_with_database_recovery(runner.main, retry_seconds=.01)
    assert len(attempts) == 2
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT base_topic FROM research_rounds').fetchall() == [('recovered',)]


async def test_supervisor_never_retries_schema_or_programming_errors():
    from sovereign_hall.run_discussion import run_with_database_recovery
    workload = AsyncMock(side_effect=sqlite3.OperationalError('no such table: invalid'))
    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        await run_with_database_recovery(workload, retry_seconds=.01)
    workload.assert_awaited_once()


async def test_closing_one_cached_path_does_not_forget_other_live_databases(tmp_path):
    first = await DatabaseService.get_instance(str(tmp_path/'first.db'))
    second = await DatabaseService.get_instance(str(tmp_path/'second.db'))
    await second.close()
    try:
        assert await DatabaseService.get_instance(str(first.db_path)) is first
    finally:
        await first.close()


@pytest.mark.parametrize('module_name,function_name', [('db_viewer','vacuum_db'),('db_inspector','vacuum_and_check')])
def test_legacy_compaction_refuses_an_active_producer_before_mutating(tmp_path,monkeypatch,module_name,function_name):
    import importlib
    from sovereign_hall.services.storage_archive import MaintenanceLock
    module = importlib.import_module('sovereign_hall.services.'+module_name)
    path = tmp_path/'legacy.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE checkpoints(id INTEGER PRIMARY KEY)')
        conn.executemany('INSERT INTO checkpoints VALUES(?)', [(i,) for i in range(8)])
    monkeypatch.setattr(module, 'DB_FILE', str(path))
    with MaintenanceLock(tmp_path/'run_discussion.lock'):
        with pytest.raises(RuntimeError, match='运行|active|lock|维护|maintenance'):
            getattr(module,function_name)()
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0] == 8
