import sqlite3
from pathlib import Path

import pytest

from sovereign_hall.services.lineage_storage import repack_step, _prepare


def legacy(path, count=300):
    conn = sqlite3.connect(path)
    conn.executescript('''PRAGMA foreign_keys=ON;
        CREATE TABLE research_rounds(id TEXT PRIMARY KEY);
        CREATE TABLE documents(id TEXT PRIMARY KEY);
        CREATE TABLE round_documents(round_id TEXT NOT NULL,document_id TEXT NOT NULL,
            usage TEXT NOT NULL DEFAULT 'source',linked_at TEXT NOT NULL,
            PRIMARY KEY(round_id,document_id,usage),
            FOREIGN KEY(round_id) REFERENCES research_rounds(id),
            FOREIGN KEY(document_id) REFERENCES documents(id));
        CREATE INDEX idx_round_documents_document ON round_documents(document_id,round_id);''')
    conn.execute("INSERT INTO research_rounds VALUES('round')")
    for i in range(count):
        conn.execute('INSERT INTO documents VALUES(?)', (f'doc_{i}',))
        conn.execute("INSERT INTO round_documents VALUES('round',?,'source',?)", (f'doc_{i}',f'2020-01-01T00:00:{i%60:02}.000123'))
    _prepare(conn);conn.commit()
    return conn


def finish(path, limit=53):
    for _ in range(100):
        report = repack_step(path, batch_rows=limit)
        if report['status'] == 'complete':
            return report
    raise AssertionError('Repacking did not finish')


def test_copy_resumes_tracks_changes_and_preserves_every_link(tmp_path):
    path = tmp_path/'links.db';conn=legacy(path)
    repack_step(path,batch_rows=10)
    conn.execute("UPDATE round_documents SET linked_at='精确时间字符串' WHERE document_id='doc_0'")
    conn.execute("DELETE FROM round_documents WHERE document_id='doc_1'")
    conn.execute("INSERT INTO documents VALUES('new')")
    conn.execute("INSERT INTO round_documents(round_id,document_id,linked_at) VALUES('round','new','new time')")
    conn.commit()
    original = set(conn.execute('SELECT * FROM round_documents'))
    conn.close()
    assert finish(path)['rows_preserved'] == len(original)
    conn=sqlite3.connect(path)
    assert set(conn.execute('SELECT * FROM round_documents')) == original
    assert conn.execute("SELECT type FROM sqlite_master WHERE name='round_documents'").fetchone()[0] == 'view'
    assert conn.execute('PRAGMA foreign_key_check').fetchall() == []
    conn.close()


def test_view_supports_writes_dedup_reverse_lookup_and_parent_constraints(tmp_path):
    path=tmp_path/'links.db';conn=legacy(path,3);conn.close();finish(path)
    conn=sqlite3.connect(path);conn.execute('PRAGMA foreign_keys=ON')
    conn.execute("INSERT OR IGNORE INTO round_documents VALUES('round','doc_0','source','ignored')")
    assert conn.execute("SELECT linked_at FROM round_documents WHERE document_id='doc_0'").fetchone()[0] != 'ignored'
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO round_documents VALUES('round','doc_0','source','duplicate')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO round_documents VALUES('round','missing','source','date')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM documents WHERE id='doc_0'")
    conn.execute("UPDATE round_documents SET usage='counterexample',linked_at='new' WHERE document_id='doc_0'")
    assert conn.execute("SELECT usage,linked_at FROM round_documents WHERE document_id='doc_0'").fetchone() == ('counterexample','new')
    plan=str(conn.execute("EXPLAIN QUERY PLAN SELECT round_id FROM round_documents WHERE document_id='doc_0'").fetchall())
    assert 'lineage_edges_document' in plan
    conn.execute("DELETE FROM round_documents WHERE document_id='doc_0'")
    conn.execute("DELETE FROM documents WHERE id='doc_0'")
    conn.commit()
    assert conn.execute('PRAGMA foreign_key_check').fetchall() == []
    conn.close()


def test_corrupt_copy_cannot_replace_authoritative_table(tmp_path):
    path=tmp_path/'links.db';conn=legacy(path,3)
    repack_step(path,batch_rows=10)
    repack_step(path,batch_rows=10)
    conn.execute('UPDATE lineage_edges SET linked_at=(SELECT max(k) FROM lineage_linked_at)');conn.commit()
    with pytest.raises(RuntimeError,match='verification failed'):
        repack_step(path)
    assert conn.execute('SELECT count(*) FROM round_documents').fetchone()[0] == 3
    assert conn.execute("SELECT type FROM sqlite_master WHERE name='round_documents'").fetchone()[0] == 'table'
    conn.close()


async def test_normal_service_restart_snapshot_and_round_write_after_switch(tmp_path):
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.research_memory import round_snapshot
    path=tmp_path/'normal.db';db=DatabaseService(str(path));await db._init_db()
    conn=await db._get_connection()
    await conn.execute("INSERT INTO research_rounds(id,base_topic,research_objective,status,current_stage,started_at,updated_at,completed_at) VALUES('round','topic','objective','completed','done','2020','2020','2020')")
    await conn.execute("INSERT INTO documents(id,title,content) VALUES('doc','title','full evidence')")
    await db._link_round_document(conn,'round','doc');await conn.commit();await db.close()
    check=sqlite3.connect(path);_prepare(check);check.commit();check.close()
    finish(path)
    db=DatabaseService(str(path))
    try:
        await db._init_db();conn=await db._get_connection()
        await db._link_round_document(conn,'round','doc');await conn.commit()
        check=sqlite3.connect(path);check.row_factory=sqlite3.Row
        snapshot=round_snapshot(check,'round');check.close()
        assert len(snapshot['round_documents'])==1 and snapshot['documents'][0]['content']=='full evidence'
    finally:
        await db.close()
