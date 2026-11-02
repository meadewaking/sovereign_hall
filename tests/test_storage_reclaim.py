import sqlite3
import pytest

from sovereign_hall.services.storage_reclaim import reclaim_pages,repack_payload_table


def test_batched_incremental_vacuum_preserves_reader_and_releases_many_pages(tmp_path):
    path=tmp_path/'reclaim.db'
    conn=sqlite3.connect(path)
    conn.executescript('PRAGMA auto_vacuum=INCREMENTAL;PRAGMA journal_mode=WAL;CREATE TABLE evidence(id INTEGER PRIMARY KEY,body BLOB);')
    conn.executemany('INSERT INTO evidence VALUES(?,?)',[(1,b'original'),(2,b'x'*3*1024**2)])
    conn.commit();conn.execute('DELETE FROM evidence WHERE id=2');conn.commit()
    before=conn.execute('PRAGMA freelist_count').fetchone()[0]
    result=reclaim_pages(path,limit=256)
    assert result['pages_reclaimed']==256
    assert conn.execute('PRAGMA freelist_count').fetchone()[0]==before-256
    assert conn.execute('SELECT body FROM evidence').fetchone()[0]==b'original'
    conn.close()


async def test_payload_table_repack_preserves_ids_fields_and_autoincrement(tmp_path):
    from sovereign_hall.services.database import DatabaseService
    path=tmp_path/'dense.db';db=DatabaseService(str(path));await db._init_db()
    conn=await db._get_connection()
    for i in range(600):
        await conn.execute("INSERT INTO research_stage_diagnostics(topic,stage,status,raw_excerpt,created_at) VALUES('topic','stage2','failed',?,'2020-01-01')",('x'*1800,))
    await conn.execute('DELETE FROM research_stage_diagnostics WHERE id=600')
    await conn.execute("UPDATE research_stage_diagnostics SET raw_excerpt='' WHERE id%3<>0")
    await conn.commit()
    before=[tuple(row) for row in await (await conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id')).fetchall()]
    result=repack_payload_table(path)
    assert result['status']=='repacked' and result['table']=='research_stage_diagnostics'
    after=[tuple(row) for row in await (await conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id')).fetchall()]
    assert after==before
    cursor=await conn.execute("INSERT INTO research_stage_diagnostics(topic,stage,status,created_at) VALUES('new','stage2','ok','2020-01-01')")
    await conn.commit()
    assert cursor.lastrowid==601
    await db.close()


async def test_background_worker_continues_while_foreground_research_is_busy(tmp_path,monkeypatch):
    import asyncio
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.storage_governor import StorageGovernor
    db=DatabaseService(str(tmp_path/'background.db'));await db._init_db()
    governor=StorageGovernor(db,tmp_path/'knowledge')
    await governor.initialize()
    governor._recovery_targets=['database']
    from dataclasses import replace
    governor._last_snapshot=replace(governor._last_snapshot,
        database=replace(governor._last_snapshot.database,used_bytes=900,max_bytes=1000))
    called=asyncio.Event()
    async def step():
        called.set()
        return {'status':'completed','can_continue':False,'progressed':True}
    monkeypatch.setattr(governor,'_database_compaction_step',step)
    governor.start_background()
    await asyncio.wait_for(called.wait(),timeout=2)
    await governor.close()
    assert governor._background_task is None
    await db.close()


def _fragmented_payload_table(path):
    conn=sqlite3.connect(path)
    conn.executescript('''PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL;
        CREATE TABLE research_stage_diagnostics(id INTEGER PRIMARY KEY AUTOINCREMENT,body TEXT);''')
    conn.executemany('INSERT INTO research_stage_diagnostics(body) VALUES(?)',[('x'*1800,) for _ in range(2000)])
    conn.execute("UPDATE research_stage_diagnostics SET body='' WHERE id%10<>0")
    conn.commit()
    return conn


def test_payload_repack_resumes_short_batches_and_keeps_concurrent_changes(tmp_path):
    path=tmp_path/'resume.db';conn=_fragmented_payload_table(path)
    before_pages=conn.execute('SELECT sum(pgsize) FROM dbstat WHERE name="research_stage_diagnostics"').fetchone()[0]
    first=repack_payload_table(path,batch_rows=100,max_batches=1)
    assert first['status']=='repacking' and first['rows_repacked']==100
    assert conn.execute('SELECT last_id FROM payload_repack_progress').fetchone()[0]==100
    # Both already visited and future rows may change between maintenance passes.
    conn.execute("UPDATE research_stage_diagnostics SET body='changed' WHERE id IN (1,1200)")
    conn.execute('DELETE FROM research_stage_diagnostics WHERE id=2')
    conn.execute("INSERT INTO research_stage_diagnostics(body) VALUES('new research')")
    conn.commit()
    expected=conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id').fetchall()
    result=repack_payload_table(path,batch_rows=100)
    assert result['status']=='repacked' and result['rows_repacked']==1900
    assert conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id').fetchall()==expected
    after_pages=conn.execute('SELECT sum(pgsize) FROM dbstat WHERE name="research_stage_diagnostics"').fetchone()[0]
    assert after_pages<before_pages*.4
    assert conn.execute('PRAGMA foreign_key_check').fetchone() is None
    assert repack_payload_table(path)['status']=='dense'
    conn.close()


@pytest.mark.parametrize('dependency',['foreign_key','trigger'])
def test_payload_repack_skips_tables_with_delete_side_effects(tmp_path,dependency):
    path=tmp_path/'protected.db';conn=_fragmented_payload_table(path)
    if dependency=='foreign_key':
        conn.executescript('''CREATE TABLE downstream(parent_id INTEGER REFERENCES research_stage_diagnostics(id) ON DELETE CASCADE);
            INSERT INTO downstream VALUES(1);''')
    else:
        conn.executescript('''CREATE TABLE downstream(parent_id INTEGER);
            CREATE TRIGGER payload_delete AFTER DELETE ON research_stage_diagnostics BEGIN
                INSERT INTO downstream VALUES(old.id); END;''')
    before=conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id').fetchall()
    assert repack_payload_table(path)['status']=='dense'
    assert conn.execute('SELECT * FROM research_stage_diagnostics ORDER BY id').fetchall()==before
    assert conn.execute('SELECT count(*) FROM downstream').fetchone()[0]==(dependency=='foreign_key')
    conn.close()
