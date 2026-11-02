import json
import sqlite3
from pathlib import Path

import pytest

from sovereign_hall.services.cold_payloads import pack_payloads
from sovereign_hall.services.storage_archive import ArchiveStore
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.tests.test_compact_rounds import archive

RAW='完整原始发言、失败与反例\n'*1000


@pytest.fixture(autouse=True)
async def meaningful_payload(archive):
    conn=await archive._get_connection()
    await conn.execute('UPDATE committee_turns SET raw_output=?',(RAW,))
    await conn.commit()


async def test_pending_prediction_keeps_exact_readable_trajectory(archive):
    from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
    from sovereign_hall.services.research_memory import round_snapshot
    conn=await archive._get_connection()
    await conn.execute("INSERT INTO price_predictions(id,round_id,ticker,direction,predicted_at,status) VALUES('pending','old','TEST','up','2020-01-01','pending')")
    await conn.commit()
    store=ArchiveStore(archive.db_path.parent/'cold.db')
    before=sqlite3.connect(archive.db_path);before.row_factory=sqlite3.Row
    original=round_snapshot(before,'old');before.close()
    report=pack_payloads(archive.db_path,store)
    assert report['turns_packed']==2
    task=await CommitteeMeetingStore(archive).load_task(meeting_id='old',stage='vote',seat_label='seat',input_hash='hash')
    assert task['raw_output']==RAW
    assert (await (await conn.execute("SELECT status FROM price_predictions WHERE id='pending'")).fetchone())[0]=='pending'
    after=sqlite3.connect(archive.db_path);after.row_factory=sqlite3.Row
    assert round_snapshot(after,'old')==original
    after.close()
    assert pack_payloads(archive.db_path,store)['turns_packed']==0


async def test_active_rounds_pins_and_changed_sources_keep_their_hot_copy(archive,monkeypatch):
    conn=await archive._get_connection()
    await conn.execute("INSERT INTO storage_pins VALUES('round','old','retain')")
    await conn.execute("UPDATE research_rounds SET status='running' WHERE id='kept'")
    await conn.commit()
    store=ArchiveStore(archive.db_path.parent/'cold.db')
    assert pack_payloads(archive.db_path,store)['turns_packed']==0
    await conn.execute("DELETE FROM storage_pins");await conn.commit()
    real=ArchiveStore.read
    def changed(locator):
        value=real(locator)
        if locator['kind']=='payload_batch':
            with sqlite3.connect(archive.db_path) as change:
                change.execute("UPDATE committee_turns SET raw_output='new raw' WHERE meeting_id='old'")
        return value
    monkeypatch.setattr(ArchiveStore,'read',staticmethod(changed))
    report=pack_payloads(archive.db_path,store)
    assert report['turns_packed']==0 and report['retained_changed']==1
    assert (await (await conn.execute("SELECT raw_output FROM committee_turns WHERE meeting_id='old'")).fetchone())[0]=='new raw'


async def test_failed_archive_and_member_corruption_never_remove_evidence(archive,monkeypatch):
    store=ArchiveStore(archive.db_path.parent/'cold.db')
    real=ArchiveStore.read
    def corrupt(locator):
        raise RuntimeError('injected corruption')
    monkeypatch.setattr(ArchiveStore,'read',staticmethod(corrupt))
    with pytest.raises(RuntimeError,match='injected corruption'):
        pack_payloads(archive.db_path,store)
    conn=await archive._get_connection()
    assert (await (await conn.execute("SELECT count(*) FROM committee_turns WHERE raw_output=?",(RAW,))).fetchone())[0]==2
    monkeypatch.setattr(ArchiveStore,'read',staticmethod(real))
    pack_payloads(archive.db_path,store)
    locator=json.loads((await (await conn.execute('SELECT locator_json FROM archived_payloads LIMIT 1')).fetchone())[0])
    locator['member_hash']='bad'
    with pytest.raises(RuntimeError,match='member checksum'):
        ArchiveStore.read(locator)


async def test_archived_diagnostic_is_exact_through_service_reader(archive):
    conn=await archive._get_connection()
    await conn.execute("INSERT INTO research_stage_diagnostics(topic,stage,status,raw_excerpt,created_at,round_id) VALUES('topic','stage2','failed',?,'2020-01-01','old')",('完整原始诊断反例'*300,))
    await conn.commit()
    before=await archive.get_recent_research_stage_diagnostics()
    result=pack_payloads(archive.db_path,ArchiveStore(archive.db_path.parent/'cold.db'))
    assert result['diagnostics_packed']==1
    assert await archive.get_recent_research_stage_diagnostics()==before


async def test_candidate_scan_does_not_sort_all_historical_tasks(archive,monkeypatch):
    from sovereign_hall.services import cold_payloads
    original_connect=sqlite3.connect
    selected=[]
    def traced(*args,**kwargs):
        conn=original_connect(*args,**kwargs)
        conn.set_trace_callback(lambda sql:selected.append(sql) if sql.startswith('SELECT ct.*') else None)
        return conn
    monkeypatch.setattr(cold_payloads.sqlite3,'connect',traced)
    pack_payloads(archive.db_path,ArchiveStore(archive.db_path.parent/'cold.db'))
    conn=original_connect(archive.db_path)
    conn.create_function('cold_old',1,lambda value:1)
    plan=conn.execute('EXPLAIN QUERY PLAN '+selected[0]).fetchall()
    conn.close()
    assert not any('TEMP B-TREE FOR ORDER BY' in step[3] for step in plan)
    assert 'SCAN ct' in plan[0][3]
