"""Move immutable payloads to verified cold storage without removing evidence.

Pending prediction outcomes need the original trajectory to remain readable;
they do not require a second hot copy of every completed task's raw output.
Structured results, IDs, predictions, positions and lineage stay in SQLite.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time

from .storage_archive import ArchiveStore, canonical_bytes, content_hash
from .research_memory import utc_time


def _old(value, cutoff):
    try:
        return bool(value) and utc_time(value) < cutoff
    except (ValueError, TypeError):
        return False


def _pinned(conn, kind, record_id, round_id):
    return conn.execute('''SELECT 1 FROM storage_pins WHERE
        (kind=? AND record_id=?) OR (kind='round' AND record_id=?) LIMIT 1''',
        (kind, str(record_id), round_id or '')).fetchone() is not None


def _eligible_turn(conn, turn, cutoff):
    parent = conn.execute('''SELECT r.id,r.status,r.completed_at,t.status
        FROM committee_tasks t JOIN committee_meetings m ON m.id=t.meeting_id
        JOIN research_rounds r ON r.id=m.round_id WHERE t.id=? AND m.id=?''',
        (turn['task_id'], turn['meeting_id'])).fetchone()
    if (not parent or parent[1] not in ('completed','failed') or parent[3] != 'completed'
            or not _old(parent[2], cutoff) or _pinned(conn,'committee_turn',turn['id'],parent[0])):
        return False
    # Active execution/position dependencies keep their hot trajectory too.
    return conn.execute('''SELECT 1 FROM execution_intents WHERE round_id=?
        AND status NOT IN ('rejected','cancelled','expired') LIMIT 1''', (parent[0],)).fetchone() is None


def pack_payloads(db_path, archive: ArchiveStore, *, limit=400, budget_seconds=4, retention_days=1):
    cutoff=(datetime.now(timezone.utc)-timedelta(days=max(1,retention_days))).isoformat()
    conn=sqlite3.connect(db_path,timeout=.1);conn.row_factory=sqlite3.Row
    conn.create_function('cold_old',1,lambda value:int(_old(value,cutoff)))
    deadline=time.monotonic()+budget_seconds
    entries=[]
    report={'turns_packed':0,'diagnostics_packed':0,'retained_changed':0,'payload_bytes':0,'can_continue':False}
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='archived_payloads'").fetchone():
            return report
        rows=conn.execute('''SELECT ct.* FROM committee_turns ct
            CROSS JOIN committee_tasks task ON task.id=ct.task_id
            CROSS JOIN committee_meetings m ON m.id=ct.meeting_id
            CROSS JOIN research_rounds r ON r.id=m.round_id
            WHERE length(CAST(ct.raw_output AS BLOB))>=1024 AND task.status='completed'
            AND r.status IN ('completed','failed') AND cold_old(r.completed_at)
            AND NOT EXISTS(SELECT 1 FROM storage_pins p WHERE
                (p.kind='round' AND p.record_id=r.id) OR (p.kind='committee_turn' AND p.record_id=CAST(ct.id AS TEXT)))
            AND NOT EXISTS(SELECT 1 FROM execution_intents e WHERE e.round_id=r.id
                AND e.status NOT IN ('rejected','cancelled','expired'))
            ORDER BY ct.id LIMIT ?''',(limit+1,)).fetchall()
        report['can_continue']=len(rows)>limit
        # Preserve rowid order so SQLite can stop at LIMIT. Starting from the
        # task-status index instead sorted the entire history on every batch.
        deadline=time.monotonic()+budget_seconds
        batch_bytes=0
        for row in rows[:limit]:
            cost=len(canonical_bytes(dict(row)))
            if entries and (time.monotonic()>deadline or batch_bytes+cost>4*1024**2):
                report['can_continue']=True;break
            entries.append(('committee_turn','committee_turns','raw_output',dict(row)))
            batch_bytes+=cost
        if len(entries)<limit and time.monotonic()<deadline:
            remaining=limit-len(entries)
            rows=conn.execute('''SELECT d.* FROM research_stage_diagnostics d
                LEFT JOIN research_rounds r ON r.id=d.round_id
                WHERE length(CAST(d.raw_excerpt AS BLOB))>=512 AND cold_old(d.created_at)
                AND (d.round_id IS NULL OR r.status IN ('completed','failed'))
                AND NOT EXISTS(SELECT 1 FROM storage_pins p WHERE
                    (p.kind='round' AND p.record_id=d.round_id) OR
                    (p.kind='research_diagnostic' AND p.record_id=CAST(d.id AS TEXT)))
                ORDER BY d.id LIMIT ?''',(remaining+1,)).fetchall()
            report['can_continue'] |= len(rows)>remaining
            entries.extend(('research_diagnostic','research_stage_diagnostics','raw_excerpt',dict(row)) for row in rows[:remaining])
        if not entries:
            return report
        members={kind+':'+str(row['id']):row for kind,_,_,row in entries}
        base=archive.put('payload_batch',content_hash(members),{'members':members})
        restored=ArchiveStore.read(base)
        if canonical_bytes(restored) != canonical_bytes({'members':members}):
            raise RuntimeError('Payload batch verification failed; all hot data retained')
        conn.execute('BEGIN IMMEDIATE')
        try:
            for kind,table,field,row in entries:
                current=conn.execute(f'SELECT * FROM {table} WHERE id=?',(row['id'],)).fetchone()
                if current is None or canonical_bytes(dict(current))!=canonical_bytes(row):
                    report['retained_changed']+=1;continue
                if _pinned(conn,kind,row['id'],row.get('round_id')):
                    report['retained_changed']+=1;continue
                if kind=='committee_turn' and not _eligible_turn(conn,row,cutoff):
                    report['retained_changed']+=1;continue
                if kind=='research_diagnostic' and row.get('round_id'):
                    parent=conn.execute('SELECT status FROM research_rounds WHERE id=?',(row['round_id'],)).fetchone()
                    if not parent or parent[0] not in ('completed','failed'):
                        report['retained_changed']+=1;continue
                key=kind+':'+str(row['id'])
                locator={**base,'member':key,'member_hash':content_hash(row)}
                conn.execute('''INSERT INTO archived_payloads VALUES(?,?,?,1)
                    ON CONFLICT(kind,record_id) DO UPDATE SET locator_json=excluded.locator_json,hot_evicted=1''',
                    (kind,str(row['id']),json.dumps(locator)))
                conn.execute(f"UPDATE {table} SET {field}='' WHERE id=?",(row['id'],))
                report['payload_bytes']+=len(row[field].encode())
                report['turns_packed' if kind=='committee_turn' else 'diagnostics_packed']+=1
            conn.commit()
        except BaseException:
            conn.rollback();raise
    finally:
        conn.close()
    return report
