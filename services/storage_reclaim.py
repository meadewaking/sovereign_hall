"""Bounded page reclamation and lossless repacking of fragmented payload tables."""
from __future__ import annotations

import sqlite3
import time


def reclaim_pages(db_path, *, limit=8192, budget_seconds=2):
    conn=sqlite3.connect(db_path,timeout=.1)
    report={'pages_reclaimed':0,'can_continue':False}
    try:
        before=conn.execute('PRAGMA freelist_count').fetchone()[0]
        if before and conn.execute('PRAGMA auto_vacuum').fetchone()[0]==2:
            deadline=time.monotonic()+budget_seconds
            conn.execute('BEGIN IMMEDIATE')
            while report['pages_reclaimed']<min(limit,before) and time.monotonic()<deadline:
                conn.execute('PRAGMA incremental_vacuum(1)').fetchall()
                current=conn.execute('PRAGMA freelist_count').fetchone()[0]
                released=before-current
                if released<=report['pages_reclaimed']:
                    break
                report['pages_reclaimed']=released
            conn.commit()
        report['wal_checkpoint']=list(conn.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
        report['free_pages_after']=conn.execute('PRAGMA freelist_count').fetchone()[0]
        report['can_continue']=report['free_pages_after']>0 or report['wal_checkpoint'][0]!=0
    except sqlite3.OperationalError as exc:
        conn.rollback()
        if 'locked' not in str(exc):
            raise
        report.update(status='writer_busy',can_continue=True)
    finally:
        conn.close()
    return report


def _can_cycle_rows(conn, table):
    columns = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    if [row[1:3] for row in columns if row[5]] != [('id', 'INTEGER')]:
        return False
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,)).fetchone():
        return False
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        name = name.replace('"', '""')
        if any(row[2] == table for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')):
            return False
    return True


def repack_payload_table(db_path, *, budget_seconds=4, batch_rows=200, max_batches=1000):
    """Rebalance sparse leaves in bounded transactions, keeping every original row.

    Deleting and reinserting a short ID range lets SQLite merge sparse leaves.
    No reader sees a missing row: both writes and the exact row comparison share
    one transaction. Inbound foreign keys or application triggers disallow this
    operation. A durable cursor makes interruption/restart safe and inexpensive.
    """
    conn = sqlite3.connect(db_path, timeout=.1)
    conn.execute('PRAGMA foreign_keys=ON')
    report = {'status': 'dense', 'table': None, 'rows_repacked': 0, 'can_continue': False}
    batch_rows = max(1, min(400, int(batch_rows)))
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS payload_repack_progress (
            table_name TEXT PRIMARY KEY, last_id INTEGER NOT NULL,
            ceiling_id INTEGER NOT NULL, completed_at REAL NOT NULL DEFAULT 0)''')
        conn.commit()
        for table in ('committee_turns', 'research_stage_diagnostics'):
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                continue
            state = conn.execute('SELECT last_id,ceiling_id,completed_at FROM payload_repack_progress WHERE table_name=?', (table,)).fetchone()
            if state and state[2] and time.time()-state[2] < 3600:
                continue
            if not _can_cycle_rows(conn, table):
                continue
            if not state or state[2]:
                unused, size = conn.execute('SELECT coalesce(sum(unused),0),coalesce(sum(pgsize),0) FROM dbstat WHERE name=?', (table,)).fetchone()
                if size < 1024**2 or unused < size*.25:
                    continue
                ceiling = conn.execute(f'SELECT coalesce(max(id),0) FROM {table}').fetchone()[0]
                conn.execute('INSERT OR REPLACE INTO payload_repack_progress VALUES(?,?,?,0)', (table, 0, ceiling))
                conn.commit()
            report.update(table=table, status='repacking', can_continue=True)
            # Read-only discovery does not consume the write-operation budget.
            deadline = time.monotonic()+budget_seconds
            columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
            id_offset = columns.index('id')
            insert = f'INSERT INTO {table} VALUES('+','.join('?' for _ in columns)+')'
            for _ in range(max_batches):
                if time.monotonic() >= deadline:
                    break
                conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 2000)
                conn.execute('BEGIN IMMEDIATE')
                last, ceiling, completed = conn.execute('SELECT last_id,ceiling_id,completed_at FROM payload_repack_progress WHERE table_name=?', (table,)).fetchone()
                if completed:
                    conn.rollback()
                    report.update(status='repacked', can_continue=True)
                    break
                rows = conn.execute(f'SELECT * FROM {table} WHERE id>? AND id<=? ORDER BY id LIMIT ?', (last, ceiling, batch_rows)).fetchall()
                if not rows:
                    conn.execute('UPDATE payload_repack_progress SET completed_at=? WHERE table_name=?', (time.time(), table))
                    conn.commit()
                    report.update(status='repacked', can_continue=True)
                    break
                ids = [row[id_offset] for row in rows]
                placeholders = ','.join('?' for _ in ids)
                conn.execute(f'DELETE FROM {table} WHERE id IN ({placeholders})', ids)
                conn.executemany(insert, rows)
                restored = conn.execute(f'SELECT * FROM {table} WHERE id IN ({placeholders}) ORDER BY id', ids).fetchall()
                if restored != rows:
                    raise RuntimeError('Payload repack verification failed; original retained')
                conn.execute('UPDATE payload_repack_progress SET last_id=? WHERE table_name=?', (ids[-1], table))
                conn.commit()
                report['rows_repacked'] += len(rows)
                conn.set_progress_handler(None, 0)
                # Yield the writer between batches, including to other processes.
                time.sleep(.005)
            break
    except sqlite3.OperationalError as exc:
        conn.set_progress_handler(None, 0)
        conn.rollback()
        if 'locked' in str(exc) or 'interrupted' in str(exc):
            report.update(status='retry', can_continue=True)
        else:
            raise
    finally:
        conn.close()
    return report
