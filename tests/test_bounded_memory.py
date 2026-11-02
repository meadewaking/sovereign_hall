import json
import sqlite3
import zlib

import pytest

from sovereign_hall.core.config import get_config
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_archive import ArchiveStore, canonical_bytes, content_hash
from sovereign_hall.services.bounded_memory import BoundedMemoryStore, retrieve_units
from sovereign_hall.services.research_memory import retrieve_memory


@pytest.fixture
async def memory_store(tmp_path, monkeypatch):
    cfg = get_config().get('agent_memory', {})
    monkeypatch.setitem(cfg, 'episodic_max_bytes', 32768)
    monkeypatch.setitem(cfg, 'semantic_max_bytes', 16384)
    monkeypatch.setitem(cfg, 'procedural_max_bytes', 8192)
    db = DatabaseService(str(tmp_path/'memory.db'))
    try:
        await db._init_db()
        yield BoundedMemoryStore(db, ArchiveStore(tmp_path/'cold.sqlite3'))
    finally:
        await db.close()


def episode(number, *, status='completed', text='有待重新验证的观点', risks='', prediction=None):
    result = {'schema_version': 1, 'round_id': str(number), 'topic': '半导体研究',
        'completed_at': '2020-01-01T00:00:00+00:00', 'status': status,
        'failure_reason': '证据不足' if status == 'failed' else None,
        'conclusions': [{'id': number, 'question': '同一命题', 'ticker': 'ABC',
                         'conclusion': text, 'risks': risks}],
        'reflections': [], 'predictions': [prediction] if prediction else [],
        'decisions': [], 'evidence': [{'id': 'evidence-'+str(number)}], 'coverage': {}}
    result['source_hash'] = content_hash(result)
    return result


def test_round_manifest_deduplicates_shared_documents_and_turn_copies(tmp_path):
    archive = ArchiveStore(tmp_path/'archive.sqlite3')
    document = {'id': 'd', 'content': '不可删除的原始证据'*5000, 'embedding': b'\x00\xff'*5000}
    turn = {'id': 1, 'raw_output': '完整输出'*5000}
    first = {'round': {'id': 'one'}, 'documents': [document], 'committee_turns': [turn]}
    second = {'round': {'id': 'two'}, 'documents': [document], 'committee_turns': [turn]}
    a = archive.put('round', 'one', first)
    b = archive.put('round', 'two', second)
    c = archive.put('committee_turn', '1', turn)
    assert archive.read(a) == first and archive.read(b) == second and archive.read(c) == turn
    with sqlite3.connect(archive.path) as conn:
        assert conn.execute('SELECT count(*) FROM archive_blobs').fetchone()[0] == 4
        assert conn.execute('SELECT count(*) FROM records').fetchone()[0] == 3
    assert c['packed_bytes'] < len(canonical_bytes(turn))/20


def test_legacy_archive_migration_preserves_locators_and_detects_missing_shared_blob(tmp_path):
    archive = ArchiveStore(tmp_path/'legacy.sqlite3')
    value = {'round': {'id': 'old'}, 'documents': [{'id': 'd', 'content': '中文原文'}]}
    raw = canonical_bytes(value)
    revision = content_hash(value)
    with sqlite3.connect(archive.path) as conn:
        conn.execute('CREATE TABLE records(kind TEXT,record_key TEXT,revision TEXT,payload BLOB,raw_bytes INTEGER,PRIMARY KEY(kind,record_key,revision))')
        conn.execute('INSERT INTO records VALUES (?,?,?,?,?)', ('round','old',revision,zlib.compress(raw),len(raw)))
    locator = {'archive_path': str(archive.path), 'kind': 'round', 'key': 'old', 'revision': revision}
    assert archive.read(locator) == value
    assert archive.migrate_legacy(limit=1)['converted'] == 1
    assert archive.read(locator) == value
    assert archive.migrate_legacy(limit=1)['converted'] == 0
    with sqlite3.connect(archive.path) as conn:
        conn.execute('DELETE FROM archive_blobs WHERE digest=?', (content_hash(value['documents'][0]),))
    with pytest.raises(RuntimeError, match='Missing archive blob'):
        archive.read(locator)


def test_dense_archive_copy_preserves_record_identity_search_and_restore(tmp_path):
    store = ArchiveStore(tmp_path/'source.sqlite3')
    first = store.put('document','one',{'id':'one','content':'共享原文 alpha evidence'})
    store.put('document','two',{'id':'two','content':'第二份 alpha evidence'})
    with sqlite3.connect(store.path) as c:
        c.execute("DELETE FROM records WHERE record_key='one'")
        expected = c.execute("SELECT rowid FROM records WHERE record_key='two'").fetchone()[0]
    copy = tmp_path/'dense.sqlite3'
    report = store.build_dense_copy(copy)
    assert report['records_verified'] == 1
    with sqlite3.connect(copy) as c:
        assert c.execute("SELECT rowid FROM records WHERE record_key='two'").fetchone()[0] == expected
        assert c.execute('PRAGMA auto_vacuum').fetchone()[0] == 2
        assert 'WITHOUT ROWID' not in c.execute("SELECT sql FROM sqlite_master WHERE name='archive_blobs'").fetchone()[0]
    assert ArchiveStore(copy).find_documents('alpha')[0][0] == 'two'
    pointer = store.latest_locator('document','two'); pointer['archive_path'] = str(copy)
    assert ArchiveStore.read(pointer)['id'] == 'two'


def test_dense_archive_copy_refuses_unknown_data_and_keeps_original(tmp_path):
    store = ArchiveStore(tmp_path/'source.sqlite3')
    locator = store.put('document','one',{'content':'证据'})
    with sqlite3.connect(store.path) as c:
        c.execute('CREATE TABLE user_annotations(body TEXT)')
    copy = tmp_path/'dense.sqlite3'
    with pytest.raises(RuntimeError,match='Unknown archive table'):
        store.build_dense_copy(copy)
    assert not copy.exists() and store.read(locator)['content'] == '证据'


async def test_long_stream_stays_in_byte_budgets_and_preserves_rare_failure_and_counterexample(memory_store):
    for number in range(80):
        prediction = {'status': 'validated', 'result': 'correct' if number else 'wrong'}
        await memory_store.remember(episode(number, status='failed' if number == 1 else 'completed',
            text='精确数字 '+str(number)+' 不能编造', prediction=prediction),
            available_at='2020-01-02T00:00:00+00:00')
    usage = await memory_store.usage()
    assert all(layer['within_budget'] for layer in usage.values())
    conn = await memory_store.db._get_connection()
    async with conn.execute('SELECT memory_json FROM research_memories') as cur:
        hot = [json.loads(r[0]) for r in await cur.fetchall()]
    assert len(hot) < 80
    assert {'failure','counterexample','success'} <= {r['outcome'] for r in hot}
    # Eviction from the hot replay buffer retains exact immutable experience.
    original = episode(0, text='精确数字 0 不能编造', prediction={'status':'validated','result':'wrong'})
    pointer = memory_store.archive.latest_locator('research_memory', '0:'+original['source_hash'])
    assert ArchiveStore.read(pointer)['memory']['predictions'][0]['result'] == 'wrong'
    prompt = await retrieve_memory(memory_store.db.db_path, '半导体研究', limit=8)
    assert '证据不足' in prompt and '状态=validated 结果=wrong' in prompt


async def test_revisions_noop_and_tombstone_preserve_point_in_time_retrieval(memory_store):
    a = await memory_store.remember(episode(1, text='收入为10亿元'), available_at='2020-01-02T00:00:00+00:00')
    again = await memory_store.remember(episode(2, text='收入为10亿元'), available_at='2020-01-03T00:00:00+00:00')
    b = await memory_store.remember(episode(3, text='收入为20亿元'), available_at='2020-02-02T00:00:00+00:00')
    assert a['unit_operations'] == ['ADD'] and again['unit_operations'] == ['NOOP']
    assert b['unit_operations'] == ['UPDATE']
    past = await retrieve_units(memory_store.db.db_path, '半导体研究', '2020-01-10T00:00:00+00:00')
    present = await retrieve_units(memory_store.db.db_path, '半导体研究', '2020-03-01T00:00:00+00:00')
    assert past[0]['value']['text'] == '收入为10亿元'
    assert present[0]['value']['text'] == '收入为20亿元'
    key = present[0]['key']
    assert await memory_store.upsert_unit('semantic', key, '半导体研究', None,
        {'round_id': '3', 'memory': b['archive']}, available_at='2020-03-02T00:00:00+00:00', deleted=True) == 'DELETE'
    assert not await retrieve_units(memory_store.db.db_path, '半导体研究', '2020-03-03T00:00:00+00:00')
    assert (await retrieve_units(memory_store.db.db_path, '半导体研究', '2020-01-10T00:00:00+00:00'))[0]['value']['text'] == '收入为10亿元'


async def test_memory_updates_require_faithful_source_and_cannot_leak_future_information(memory_store):
    original = episode(1, text='数字10')
    report = await memory_store.remember(original, available_at='2020-01-02T00:00:00+00:00')
    layer,key,value = next(memory_store._extract_units(original))
    source = {'round_id':'1', 'memory':report['archive']}
    with pytest.raises(ValueError, match='not supported'):
        await memory_store.upsert_unit(layer,key,original['topic'],{**value,'text':'编造999'},source)
    with pytest.raises(ValueError, match='predate'):
        await memory_store.upsert_unit(layer,key,original['topic'],value,source,available_at='2019-01-01')


async def test_pin_rejection_does_not_publish_deletion_tombstone(memory_store):
    original = episode(1)
    report = await memory_store.remember(original, available_at='2020-01-02T00:00:00+00:00')
    layer,key,_ = next(memory_store._extract_units(original))
    conn = await memory_store.db._get_connection()
    await conn.execute("INSERT INTO storage_pins VALUES ('memory_unit',?,'review')", (key,)); await conn.commit()
    with pytest.raises(RuntimeError, match='Pinned'):
        await memory_store.upsert_unit(layer,key,original['topic'],None,
            {'round_id':'1','memory':report['archive']}, deleted=True)
    assert (await retrieve_units(memory_store.db.db_path, original['topic'], '2099-01-01'))[0]['value']['text'] == original['conclusions'][0]['conclusion']


async def test_legacy_hot_memories_backfill_is_lossless_and_resumable(memory_store):
    original = episode(1, status='failed', risks='不能忽视的反例')
    conn = await memory_store.db._get_connection()
    await conn.execute('INSERT INTO research_memories VALUES (?,?,?,?,?,?,?)',
        ('1',original['source_hash'],original['topic'],'2020-01-02T00:00:00+00:00',original['completed_at'],original['status'],json.dumps(original)))
    await conn.commit()
    assert (await memory_store.backfill(limit=1))['converted'] == 1
    assert (await memory_store.backfill(limit=1))['converted'] == 0
    prompt = await retrieve_memory(memory_store.db.db_path, original['topic'])
    assert '证据不足' in prompt and '不能忽视的反例' in prompt


async def test_repeated_memory_publication_is_noop_and_does_not_duplicate_cold_versions(memory_store):
    original = episode(1)
    first = await memory_store.remember(original)
    with sqlite3.connect(memory_store.archive.path) as c:
        before = c.execute('SELECT count(*) FROM records').fetchone()[0]
    again = await memory_store.remember(original)
    assert again['operation'] == 'NOOP'
    assert first['archive'] == again['archive']
    with sqlite3.connect(memory_store.archive.path) as c:
        assert c.execute('SELECT count(*) FROM records').fetchone()[0] == before


async def test_pinned_replay_samples_are_preserved_when_new_memory_exceeds_budget(memory_store):
    original = episode(1, status='failed')
    await memory_store.remember(original)
    conn = await memory_store.db._get_connection()
    await conn.execute("INSERT INTO storage_pins VALUES ('round','1','review')"); await conn.commit()
    memory_store.budgets['episodic'] = (await memory_store.usage())['episodic']['charged_bytes']
    incoming = await memory_store.remember(episode(2))
    assert not incoming['admitted']
    async with conn.execute('SELECT round_id FROM research_memories') as cur:
        assert [row[0] for row in await cur.fetchall()] == ['1']
    assert ArchiveStore.read(incoming['archive'])['memory']['round_id'] == '2'
