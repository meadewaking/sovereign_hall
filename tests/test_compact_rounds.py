"""Compaction must finish, retain shared evidence, and fail without data loss."""
from argparse import Namespace
import asyncio
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest.mock import AsyncMock
import json

import pytest

from sovereign_hall.scripts import compact_rounds
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.research_memory import ResearchCompactor, ProtectionPolicy, retrieve_memory
from sovereign_hall.services.storage_archive import ArchiveStore, consistent_backup, MaintenanceLock
from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
from sovereign_hall.services.learning_engine import LearningEngine


@pytest.fixture
async def archive(tmp_path):
    db = DatabaseService(str(tmp_path / "archive.db"))
    try:
        await db._init_db()
        await db.init_report_tables()
        conn = await db._get_connection()
        for round_id in ("old", "kept"):
            await conn.execute(
                """INSERT INTO research_rounds
                   (id,base_topic,research_objective,status,current_stage,
                    started_at,updated_at,completed_at)
                   VALUES (?, 'topic','objective','completed','done',
                           '2020-01-01','2020-01-01','2020-01-01')""",
                (round_id,),
            )
            await conn.execute(
                """INSERT INTO committee_meetings
                   (id,round_id,proposal_key,ticker,status,current_stage,
                    prompt_version,input_hash,started_at,updated_at)
                   VALUES (?,?,'proposal','ticker','completed','done',
                           'v1','hash','2020-01-01','2020-01-01')""",
                (round_id, round_id),
            )
            await conn.execute(
                """INSERT INTO committee_tasks
                   (id,meeting_id,stage,seat_role,seat_label,session_id,status,
                    input_hash,prompt_version,started_at,updated_at)
                   VALUES (?,?,'vote','role','seat','session','completed',
                           'hash','v1','2020-01-01','2020-01-01')""",
                (round_id, round_id),
            )
            await conn.execute(
                """INSERT INTO committee_turns
                   (task_id,meeting_id,stage,seat_label,raw_output,parsed_output_json,
                    output_hash,created_at)
                   VALUES (?,?,'vote','seat','raw','{}','hash','2020-01-01')""",
                (round_id, round_id),
            )
            await conn.execute(
                """INSERT INTO round_events(round_id,sequence,event_type,created_at)
                   VALUES (?,1,'event','2020-01-01')""", (round_id,),
            )
        for doc_id in ("exclusive", "shared", "stale", "recent"):
            await conn.execute(
                "INSERT INTO documents(id,created_at,content_hash,content,embedding) VALUES (?,?,?,?,?)",
                (doc_id, "2999-01-01" if doc_id == "recent" else "2020-01-01", doc_id, "独立原文归档检索 alpha evidence", b"\x00\xff"),
            )
        await conn.executemany(
            "INSERT INTO round_documents(round_id,document_id,linked_at) VALUES (?,?,'2020-01-01')",
            [("old", "exclusive"), ("old", "shared"), ("kept", "shared")],
        )
        await conn.commit()
        yield db
    finally:
        await db.close()



def compactor(db, **kwargs):
    return ResearchCompactor(db, ArchiveStore(db.db_path.parent / 'cold.sqlite3'), **kwargs)


async def scalar(db, sql):
    conn = await db._get_connection()
    async with conn.execute(sql) as cursor:
        return (await cursor.fetchone())[0]


async def test_lossless_round_archive_keeps_identity_events_lineage_and_resume(archive):
    worker = compactor(archive)
    report = await worker.process_round('old', evict_hot=True)
    snapshot = ArchiveStore.read(report['archive'])
    assert report['coverage']['documents'] == 2
    assert report['coverage']['round_events'] == 1
    assert report['evicted_turns'] == 1
    assert snapshot['committee_turns'][0]['raw_output'] == 'raw'
    assert snapshot['documents'][0]['embedding'] == b'\x00\xff'
    for table, count in [('round_events', 2), ('round_documents', 3), ('documents', 4), ('committee_tasks', 2)]:
        assert await scalar(archive, f'SELECT COUNT(*) FROM {table}') == count
    task = await CommitteeMeetingStore(archive).load_task(
        meeting_id='old', stage='vote', seat_label='seat', input_hash='hash')
    assert task['raw_output'] == 'raw'
    assert (await worker.process_round('old', evict_hot=True))['evicted_turns'] == 0
    assert await scalar(archive, 'SELECT COUNT(*) FROM research_memories') == 1
    assert compact_rounds.plan(archive.db_path, cutoff_days=1, limit=10, orphan_limit=0,
                               evict_hot=True)['rounds'][0]['id'] == 'kept'
    conn = await archive._get_connection()
    async with conn.execute('PRAGMA foreign_key_check') as cursor:
        assert await cursor.fetchall() == []


async def test_audit_timeout_retries_with_persisted_budget_without_hour_backoff_or_eviction(archive, monkeypatch):
    from sovereign_hall.services.storage_governor import (
        StorageGovernor, StoreWatermark, StorageSnapshot, _RoundAuditBudgetExpired,
    )
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    governor._recovery_targets = ['database']
    budgets = []
    def audit(round_id, *, deep, budget_seconds):
        budgets.append((round_id, budget_seconds))
        if round_id == 'kept':
            raise _RoundAuditBudgetExpired(round_id)
        return ['explicit_pin']
    monkeypatch.setattr(governor, '_audit_round_for_archive', audit)
    full = StoreWatermark(91, 100, .6, .8, .9)
    empty = StoreWatermark(0, 100, .6, .8, .9)
    snapshot = StorageSnapshot(full, empty, 'now', ('database',))
    first = await governor.archive_history(snapshot)
    assert first['research']['status'] == 'audit_budget_exhausted'
    assert first['can_continue'] and 'error' not in first
    assert governor._round_after is None
    assert governor._archive_retry_after == 0
    restarted = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    assert restarted._round_audit_budget == {'round_id': 'kept', 'seconds': 10}
    second = await governor.archive_history(snapshot)
    assert second['research']['retry_budget_seconds'] == 20
    third = await governor.archive_history(snapshot)
    assert third['research']['scan_complete']
    assert governor._round_audit_deferred_count == 1
    assert budgets == [('kept', 5), ('kept', 10), ('kept', 20), ('old', 5)]
    assert await scalar(archive, "SELECT error FROM storage_jobs WHERE record_id='kept'") == 'audit_budget_exhausted'
    assert await scalar(archive, 'SELECT count(*) FROM committee_turns WHERE raw_output=\'raw\'') == 2
    assert await scalar(archive, 'SELECT count(*) FROM archived_payloads') == 0


async def test_sqlite_progress_timeout_is_resumable_and_does_not_interrupt_main_connection(archive, monkeypatch):
    from sovereign_hall.services.storage_governor import StorageGovernor, _RoundAuditBudgetExpired
    def slow_audit(policy, round_id):
        policy.conn.execute('''WITH RECURSIVE slow(n) AS (
            SELECT 1 UNION ALL SELECT n+1 FROM slow WHERE n<100000)
            SELECT sum(n) FROM slow''').fetchone()
        return []
    monkeypatch.setattr(ProtectionPolicy, 'round_reasons', slow_audit)
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    with pytest.raises(_RoundAuditBudgetExpired):
        await asyncio.to_thread(governor._audit_round_for_archive, 'old', deep=True, budget_seconds=0)
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'
    assert not (await archive._get_connection()).in_transaction


async def test_unrelated_audit_sqlite_errors_remain_errors(archive, monkeypatch):
    from sovereign_hall.services.storage_governor import StorageGovernor
    def broken_audit(policy, round_id):
        raise sqlite3.OperationalError('interrupted')
    monkeypatch.setattr(ProtectionPolicy, 'round_reasons', broken_audit)
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    with pytest.raises(sqlite3.OperationalError, match='interrupted'):
        await asyncio.to_thread(governor._audit_round_for_archive, 'old', deep=True, budget_seconds=5)


async def test_failed_eviction_is_atomic_and_retry_does_not_skip_existing_memory(archive):
    conn = await archive._get_connection()
    await conn.execute("CREATE TRIGGER reject_clear BEFORE UPDATE ON committee_turns BEGIN SELECT RAISE(ABORT,'injected'); END")
    await conn.commit()
    worker = compactor(archive)
    with pytest.raises(sqlite3.IntegrityError, match='injected'):
        await worker.process_round('old', evict_hot=True)
    assert await scalar(archive, 'SELECT COUNT(*) FROM archived_payloads') == 0
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'
    assert await scalar(archive, 'SELECT state FROM storage_jobs') == 'failed'
    await conn.execute('DROP TRIGGER reject_clear')
    await conn.commit()
    assert (await worker.process_round('old', evict_hot=True))['evicted_turns'] == 1
    assert await scalar(archive, 'SELECT COUNT(*) FROM research_memories') == 1


@pytest.mark.parametrize('dependency', ['pending_prediction', 'position', 'pin'])
async def test_unfinished_roots_protect_raw_trajectory(archive, dependency):
    conn = await archive._get_connection()
    if dependency == 'pending_prediction':
        await conn.execute("INSERT INTO price_predictions(id,round_id,ticker,status) VALUES ('p','old','ABC','pending')")
    elif dependency == 'position':
        await conn.execute("""INSERT INTO execution_intents(id,round_id,ticker,direction,target_position,reason,
                           status,idempotency_key,created_at,updated_at)
                           VALUES ('i','old','ABC','long',0.1,'test','executed','i','2020','2020')""")
    else:
        await conn.execute("INSERT INTO storage_pins VALUES ('round','old','review')")
    await conn.commit()
    report = await compactor(archive).process_round('old', evict_hot=True)
    assert report['protected_by']
    assert report['evicted_turns'] == 0
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'


async def test_document_body_and_embedding_are_searchable_after_eviction(archive):
    original = await archive.get_document('stale')
    report = await compactor(archive).archive_document(original, evict_hot=True)
    assert report['evicted'] == 1
    assert await scalar(archive, "SELECT content FROM documents WHERE id='stale'") is None
    assert await archive.get_document('stale') == original
    assert 'stale' in [r['id'] for r in await archive.search_documents('原文归档')]
    assert 'stale' in [r['id'] for r in await archive.search_documents('ALPHA')]
    assert 'stale' in [r['id'] for r in await archive.search_documents('原文')]
    assert 'stale' in [r['id'] for r in await archive.search_documents('alpha%')]
    assert len(await archive.search_documents('alpha', limit=1)) == 1


@pytest.mark.parametrize('dependency', ['shared', 'json', 'source_document', 'wiki', 'unknown_wiki'])
async def test_document_dependency_graph_protects_hot_body(archive, dependency):
    conn = await archive._get_connection()
    doc_id = 'shared' if dependency == 'shared' else 'stale'
    wiki_root = None
    if dependency == 'json':
        await conn.execute("UPDATE round_events SET payload_json=? WHERE round_id='old'", (json.dumps({'origin': ['stale']}),))
    elif dependency == 'source_document':
        await conn.execute('CREATE TABLE consumer(source_document_id TEXT)')
        await conn.execute("INSERT INTO consumer VALUES ('stale')")
    elif dependency in {'wiki', 'unknown_wiki'}:
        wiki_root = archive.db_path.parent / 'knowledge'
        source = wiki_root / 'wiki' / 'sources' / 'example.md'
        source.parent.mkdir(parents=True)
        source.write_text('---\nsource_id: stale\n---\ntext' if dependency == 'wiki' else 'no frontmatter')
    await conn.commit()
    report = await compactor(archive, wiki_root=wiki_root).archive_document(await archive.get_document(doc_id), evict_hot=True)
    assert report['evicted'] == 0
    assert await scalar(archive, f"SELECT content FROM documents WHERE id='{doc_id}'")


async def test_archive_capacity_failure_preserves_hot_source(archive):
    worker = ResearchCompactor(archive, ArchiveStore(archive.db_path.parent / 'small.sqlite3', 1))
    with pytest.raises(RuntimeError, match='capacity'):
        await worker.process_round('old', evict_hot=True)
    assert await scalar(archive, 'SELECT COUNT(*) FROM archived_payloads') == 0
    assert await scalar(archive, 'SELECT COUNT(*) FROM research_memories') == 0


async def test_concurrent_source_edit_prevents_eviction(archive, monkeypatch):
    worker = compactor(archive)
    original_put = worker.archive.put
    def editing_put(kind, key, value):
        locator = original_put(kind, key, value)
        if kind == 'committee_turn':
            with sqlite3.connect(archive.db_path) as connection:
                connection.execute("UPDATE committee_turns SET raw_output='new evidence' WHERE id=?", (key,))
        return locator
    monkeypatch.setattr(worker.archive, 'put', editing_put)
    with pytest.raises(RuntimeError, match='Source changed'):
        await worker.process_round('old', evict_hot=True)
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'new evidence'
    assert await scalar(archive, 'SELECT COUNT(*) FROM archived_payloads') == 0


async def test_dependency_added_after_preflight_is_rechecked_under_writer_lock(archive, monkeypatch):
    worker = compactor(archive)
    original_put = worker.archive.put
    def pinning_put(kind, key, value):
        locator = original_put(kind, key, value)
        if kind == 'committee_turn':
            with sqlite3.connect(archive.db_path) as connection:
                connection.execute("INSERT INTO storage_pins VALUES ('round','old','concurrent pin')")
        return locator
    monkeypatch.setattr(worker.archive, 'put', pinning_put)
    report = await worker.process_round('old', evict_hot=True)
    assert report['protected_by'] == ['explicit_pin']
    assert report['evicted_turns'] == 0
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'


async def test_changed_age_eligibility_cannot_clear_hot_payload(archive):
    worker = compactor(archive)
    with pytest.raises(ValueError, match='age threshold'):
        await worker.process_round('old', evict_hot=True, completed_before='2019-01-01T00:00:00+00:00')
    with pytest.raises(ValueError, match='age threshold'):
        await worker.archive_document(await archive.get_document('stale'), evict_hot=True,
                                      created_before='2019-01-01T00:00:00+00:00')
    assert await scalar(archive, 'SELECT COUNT(*) FROM archived_payloads') == 0


async def test_failed_cases_and_risks_retrieved_by_topic_and_time(archive):
    conn = await archive._get_connection()
    await conn.execute("UPDATE research_rounds SET status='failed',terminal_reason='证据不足不能下单' WHERE id='old'")
    await conn.execute("INSERT INTO report_conclusions(question,conclusion,risks,key_points,created_at,round_id) VALUES ('topic','结论文本','反例不能忽略','依据链','2020-01-01','old')")
    await conn.commit()
    await compactor(archive).process_round('old')
    memory = await LearningEngine(archive.db_path).generate_research_memory_prompt('topic')
    assert all(t in memory for t in ['证据不足不能下单', '反例不能忽略', '依据链', 'old', 'exclusive'])
    assert len(memory) <= 6000
    assert await retrieve_memory(archive.db_path, 'completely-unrelated') == ''
    assert await LearningEngine(archive.db_path).generate_research_memory_prompt('topic', as_of='2020-02-01') == ''
    assert await retrieve_memory(archive.db_path, 'topic', max_chars=30) == ''


async def test_round_end_publishes_bounded_learning_below_pressure_without_evicting_live_raw(archive):
    from sovereign_hall.services.storage_governor import StorageGovernor
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    report = await governor.after_round()
    assert report['learning']['admitted']
    assert await scalar(archive, 'SELECT COUNT(*) FROM research_memories') == 1
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'
    assert 'learning' not in await governor.after_round()


async def test_validated_feedback_updates_learning_without_leaking_into_earlier_memory(archive):
    from sovereign_hall.services.storage_governor import StorageGovernor
    from sovereign_hall.services.research_memory import now_utc
    conn = await archive._get_connection()
    await conn.execute("INSERT INTO price_predictions(id,round_id,ticker,status,predicted_at) VALUES ('feedback','old','ABC','pending','2020-01-01')")
    await conn.commit()
    governor = StorageGovernor(archive,archive.db_path.parent/'knowledge')
    await governor.after_round()
    before_validation = now_utc()
    await conn.execute("UPDATE price_predictions SET status='validated',result='wrong',validated_at=? WHERE id='feedback'",(now_utc(),))
    await conn.commit()
    report = await governor.after_round()
    assert report['memory_feedback_updated'] == 1
    previous = await retrieve_memory(archive.db_path,'topic',as_of=before_validation)
    current = await retrieve_memory(archive.db_path,'topic')
    assert '状态=pending' in previous and '结果=wrong' not in previous
    assert '状态=validated 结果=wrong' in current
    assert (await governor.after_round())['memory_feedback_updated'] == 0


def test_archive_corruption_is_detected(tmp_path):
    store = ArchiveStore(tmp_path / 'cold.sqlite3')
    locator = store.put('round', 'id', {'raw': 'evidence', 'embedding': b'\x00\xff'})
    assert store.read(locator)['embedding'] == b'\x00\xff'
    with sqlite3.connect(store.path) as conn:
        import zlib
        conn.execute('UPDATE records SET payload=?', (zlib.compress(b'{}'),))
    with pytest.raises(RuntimeError, match='checksum'):
        store.read(locator)


def test_consistent_backup_includes_uncheckpointed_wal(tmp_path):
    source = tmp_path / 'source.db'
    conn = sqlite3.connect(source)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA wal_autocheckpoint=0')
    conn.execute('CREATE TABLE evidence(value TEXT)')
    conn.execute("INSERT INTO evidence VALUES ('latest WAL evidence')")
    conn.commit()
    backup = tmp_path / 'backup.db'
    consistent_backup(source, backup)
    with sqlite3.connect(backup) as restored:
        assert restored.execute('SELECT value FROM evidence').fetchone()[0] == 'latest WAL evidence'
    conn.close()


def test_maintenance_lock_prevents_concurrent_runner_start(tmp_path):
    with MaintenanceLock(tmp_path / 'run_discussion.lock'):
        with pytest.raises(RuntimeError, match='holds the lock'):
            with MaintenanceLock(tmp_path / 'run_discussion.lock'):
                pass


async def test_duplicate_index_compaction_preserves_constraints_and_indexed_identity_queries(archive):
    from sovereign_hall.scripts.compact_storage import compact_document_indexes
    from sovereign_hall.services.storage_governor import StorageGovernor
    conn = await archive._get_connection()
    for column in ('url', 'content_hash'):
        await conn.execute(f'CREATE INDEX idx_documents_{column} ON documents({column})')
    await conn.commit()
    with sqlite3.connect(archive.db_path) as check:
        assert compact_document_indexes(check) == ['idx_documents_url', 'idx_documents_content_hash']
        for column in ('url', 'content_hash'):
            plan = check.execute(f"EXPLAIN QUERY PLAN SELECT id FROM documents WHERE {column}=? AND {column}<>''", ('identity',)).fetchall()
            assert f'ux_documents_{column}' in str(plan)
        assert check.execute('SELECT COUNT(*) FROM documents').fetchone()[0] == 4
        with pytest.raises(sqlite3.IntegrityError):
            check.execute("INSERT INTO documents(id,content_hash) VALUES ('duplicate','shared')")
    await archive._init_db()
    async with conn.execute("SELECT name FROM sqlite_master WHERE name IN ('idx_documents_url','idx_documents_content_hash')") as cursor:
        assert await cursor.fetchall() == []


async def test_runtime_archival_restores_complete_task_output_and_skips_pending_roots(archive):
    from sovereign_hall.services.storage_governor import StorageGovernor
    conn = await archive._get_connection()
    await conn.execute("UPDATE research_rounds SET completed_at='2020-02-01' WHERE id='kept'")
    await conn.execute("INSERT INTO price_predictions(id,round_id,ticker,status) VALUES ('protected','old','ABC','pending')")
    await conn.commit()
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    report = await governor._archive_closed_round()
    assert report['round_id'] == 'kept'
    assert report['evicted_turns'] == 1
    assert await scalar(archive, "SELECT raw_output FROM committee_turns WHERE meeting_id='old'") == 'raw'
    task = await CommitteeMeetingStore(archive).load_task(
        meeting_id='kept', stage='vote', seat_label='seat', input_hash='hash')
    assert task['raw_output'] == 'raw'
    assert await scalar(archive, "SELECT state FROM storage_jobs WHERE record_id='old'") == 'deferred'


async def test_resumed_archival_reuses_utc_function_with_an_active_read_cursor(archive, monkeypatch):
    from sovereign_hall.services.storage_governor import StorageGovernor
    conn = await archive._get_connection()
    register = AsyncMock(wraps=conn.create_function)
    monkeypatch.setattr(conn, 'create_function', register)
    governor = StorageGovernor(archive, archive.db_path.parent/'knowledge')
    first = await governor._archive_closed_round()
    cursor = await conn.execute('SELECT research_utc(completed_at) FROM research_rounds')
    try:
        await cursor.fetchone()  # Keep a statement using the function active.
        second = await governor._archive_closed_round()
        assert first['round_id'] != second['round_id']
        assert register.await_count == 1
    finally:
        await cursor.close()


async def test_cancelled_initialization_closes_database(archive, monkeypatch):
    monkeypatch.setattr(compact_rounds, 'DatabaseService', lambda _: archive)
    monkeypatch.setattr(archive, '_init_db', AsyncMock(side_effect=asyncio.CancelledError))
    args = Namespace(db_path=archive.db_path, archive_path=None, knowledge_root=None, apply=True,
                     evict_hot=False, refresh=False, cutoff_days=1, limit=1, orphan_limit=0,
                     backup_dir=None, archive_max_bytes=1000000, backup_max_bytes=10000000)
    with pytest.raises(asyncio.CancelledError):
        await compact_rounds.main_async(args)
    assert archive._connection is None


@pytest.mark.parametrize('fail', [False, True])
def test_process_exits_after_success_or_initialization_error(tmp_path, fail):
    script = f"""
import asyncio
from argparse import Namespace
from pathlib import Path
from sovereign_hall.scripts import compact_rounds as module
from sovereign_hall.services.database import DatabaseService
path=Path({str(tmp_path / 'cli.db')!r})
async def setup():
    db=DatabaseService(str(path))
    await db._init_db()
    conn=await db._get_connection()
    await conn.execute("INSERT INTO research_rounds(id,base_topic,research_objective,status,current_stage,started_at,updated_at,completed_at) VALUES ('cli','topic','objective','completed','done','2020-01-01','2020-01-01','2020-01-01')")
    await conn.commit()
    await db.close()
asyncio.run(setup())
if {fail!r}:
    original=module.DatabaseService._init_db
    async def failing_init(self):
        await original(self)
        raise RuntimeError('injected startup failure')
    module.DatabaseService._init_db=failing_init
asyncio.run(module.main_async(Namespace(db_path=path,archive_path=None,knowledge_root=None,
    apply=True,evict_hot=False,refresh=False,cutoff_days=90,limit=1,orphan_limit=0,
    backup_dir=None,archive_max_bytes=1000000,backup_max_bytes=10000000)))
"""
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                            cwd=Path(__file__).resolve().parents[1], timeout=30)
    assert result.returncode == (1 if fail else 0), result.stderr
    assert ('injected startup failure' in result.stderr) == fail
