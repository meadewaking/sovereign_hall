"""Online, restartable dictionary encoding of the document lineage relation.

The public SQL relation keeps its four columns. No lineage edge is pruned.
Copy and verification use bounded transactions; change-capture triggers keep
the new representation current until the atomic switch to a writable view.
"""
from __future__ import annotations

import sqlite3
import time


DIMS = (('round_id', 'research_rounds'), ('document_id', 'documents'),
        ('usage', None), ('linked_at', None))
COLS = tuple(name for name, _ in DIMS)


def _lookup(name, value):
    return f'(SELECT k FROM lineage_{name} WHERE value={value})'


def _insert(prefix, *, ignore=False):
    values = [f'{prefix}.{name}' for name in COLS]
    values[2] = f"COALESCE({values[2]},'source')"
    statements = [f'INSERT OR IGNORE INTO lineage_{name}(value) VALUES({value});'
                  for name, value in zip(COLS, values)]
    keys = ','.join(_lookup(name, value) for name, value in zip(COLS, values))
    statements.append(f"INSERT {'OR IGNORE ' if ignore else ''}INTO lineage_edges VALUES({keys});")
    return '\n'.join(statements)


def _delete(prefix):
    where = ' AND '.join(f'{name}={_lookup(name, prefix+"."+name)}' for name in COLS[:3])
    return f'DELETE FROM lineage_edges WHERE {where};'


VIEW_SQL = '''SELECT r.value AS round_id,d.value AS document_id,
    u.value AS usage,t.value AS linked_at FROM lineage_edges e
    JOIN lineage_round_id r ON r.k=e.round_id
    JOIN lineage_document_id d ON d.k=e.document_id
    JOIN lineage_usage u ON u.k=e.usage
    JOIN lineage_linked_at t ON t.k=e.linked_at'''


def _kind(conn):
    row = conn.execute("SELECT type FROM sqlite_master WHERE name='round_documents'").fetchone()
    return row[0] if row else None


def _prepare(conn):
    # Unknown additions or custom triggers require deliberate compatibility
    # work, rather than silently dropping user schema at the final switch.
    columns = [row[1] for row in conn.execute('PRAGMA table_info(round_documents)')]
    if columns != list(COLS):
        raise RuntimeError('Unknown lineage columns; original table retained')
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='round_documents'")}
    if indexes - {'sqlite_autoindex_round_documents_1', 'idx_round_documents_document'}:
        raise RuntimeError('Custom lineage index; original table retained')
    triggers = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='round_documents'").fetchall()
    if any(not row[0].startswith('lineage_sync_') for row in triggers):
        raise RuntimeError('Custom lineage trigger; original table retained')
    for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        if any(fk[2] == 'round_documents' for fk in conn.execute(f'PRAGMA foreign_key_list("{table}")')):
            raise RuntimeError('Inbound lineage foreign key; original table retained')
    conn.execute('''CREATE TABLE IF NOT EXISTS lineage_repack(
        id INTEGER PRIMARY KEY CHECK(id=1), phase TEXT NOT NULL,
        copied_through INTEGER NOT NULL DEFAULT 0, verified_through INTEGER NOT NULL DEFAULT 0)''')
    for name, parent in DIMS:
        fk = f',FOREIGN KEY(value) REFERENCES {parent}(id) ON DELETE CASCADE' if parent else ''
        conn.execute(f'''CREATE TABLE IF NOT EXISTS lineage_{name}(
            k INTEGER PRIMARY KEY,value TEXT NOT NULL UNIQUE{fk})''')
    conn.execute('''CREATE TABLE IF NOT EXISTS lineage_edges(
        round_id INTEGER NOT NULL,document_id INTEGER NOT NULL,
        usage INTEGER NOT NULL,linked_at INTEGER NOT NULL,
        PRIMARY KEY(round_id,document_id,usage),
        FOREIGN KEY(round_id) REFERENCES lineage_round_id(k),
        FOREIGN KEY(document_id) REFERENCES lineage_document_id(k),
        FOREIGN KEY(usage) REFERENCES lineage_usage(k),
        FOREIGN KEY(linked_at) REFERENCES lineage_linked_at(k)
        ) WITHOUT ROWID''')
    conn.execute('CREATE INDEX IF NOT EXISTS lineage_edges_document ON lineage_edges(document_id,round_id)')
    for event, body in [('INSERT', _insert('NEW', ignore=True)), ('DELETE', _delete('OLD')),
                        ('UPDATE', _delete('OLD')+'\n'+_insert('NEW', ignore=True))]:
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS lineage_sync_{event.lower()}
            AFTER {event} ON round_documents BEGIN {body} END''')
    conn.execute("INSERT OR IGNORE INTO lineage_repack(id,phase) VALUES(1,'copy')")


def _copy(conn, after, limit):
    rows = conn.execute('SELECT rowid FROM round_documents WHERE rowid>? ORDER BY rowid LIMIT ?',
                        (after, limit)).fetchall()
    if not rows:
        conn.execute("UPDATE lineage_repack SET phase='verify' WHERE id=1")
        return 0
    last = rows[-1][0]
    for name, _ in DIMS:
        conn.execute(f'''INSERT OR IGNORE INTO lineage_{name}(value)
            SELECT {name} FROM round_documents WHERE rowid>? AND rowid<=?''', (after, last))
    conn.execute('''INSERT OR IGNORE INTO lineage_edges
        SELECT r.k,d.k,u.k,t.k FROM round_documents old
        JOIN lineage_round_id r ON r.value=old.round_id
        JOIN lineage_document_id d ON d.value=old.document_id
        JOIN lineage_usage u ON u.value=old.usage
        JOIN lineage_linked_at t ON t.value=old.linked_at
        WHERE old.rowid>? AND old.rowid<=?''', (after, last))
    conn.execute('UPDATE lineage_repack SET copied_through=? WHERE id=1', (last,))
    return len(rows)


def _verify(conn, after, limit):
    rows = conn.execute('SELECT rowid FROM round_documents WHERE rowid>? ORDER BY rowid LIMIT ?',
                        (after, limit)).fetchall()
    if not rows:
        conn.execute("UPDATE lineage_repack SET phase='switch' WHERE id=1")
        return 0
    last = rows[-1][0]
    mismatch = conn.execute('''SELECT old.rowid FROM round_documents old
        LEFT JOIN lineage_round_id r ON r.value=old.round_id
        LEFT JOIN lineage_document_id d ON d.value=old.document_id
        LEFT JOIN lineage_usage u ON u.value=old.usage
        LEFT JOIN lineage_linked_at t ON t.value=old.linked_at
        LEFT JOIN lineage_edges e ON e.round_id=r.k AND e.document_id=d.k AND e.usage=u.k
        WHERE old.rowid>? AND old.rowid<=? AND (e.linked_at IS NULL OR e.linked_at<>t.k OR t.k IS NULL)
        LIMIT 1''', (after, last)).fetchone()
    if mismatch:
        raise RuntimeError('Lineage verification failed; original table retained')
    conn.execute('UPDATE lineage_repack SET verified_through=? WHERE id=1', (last,))
    return len(rows)


def _switch(conn):
    triggers = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='round_documents'")}
    if triggers != {'lineage_sync_insert', 'lineage_sync_update', 'lineage_sync_delete'}:
        raise RuntimeError('Lineage change capture changed; original table retained')
    old = conn.execute('SELECT count(*) FROM round_documents').fetchone()[0]
    new = conn.execute('SELECT count(*) FROM lineage_edges').fetchone()[0]
    if old != new:
        raise RuntimeError('Lineage count mismatch; original table retained')
    conn.execute('DROP TABLE round_documents')
    conn.execute('CREATE VIEW round_documents AS '+VIEW_SQL)
    for event, body in [('INSERT', _insert('NEW')), ('DELETE', _delete('OLD')),
                        ('UPDATE', _delete('OLD')+'\n'+_insert('NEW'))]:
        conn.execute(f'''CREATE TRIGGER lineage_write_{event.lower()}
            INSTEAD OF {event} ON round_documents BEGIN {body} END''')
    conn.execute("UPDATE lineage_repack SET phase='complete' WHERE id=1")
    return new


def repack_step(db_path, *, batch_rows=20000, budget_seconds=3, max_main_bytes=None):
    """One bounded atomic step; resumable on deadline, lock contention or crash."""
    conn = sqlite3.connect(db_path, timeout=.1)
    started = time.monotonic()
    result = {'status': 'not_applicable', 'copied_rows': 0, 'verified_rows': 0, 'can_continue': False}
    try:
        kind = _kind(conn)
        if kind == 'view':
            owned = conn.execute("SELECT 1 FROM sqlite_master WHERE name='lineage_repack'").fetchone()
            if not owned:
                return result
            free = conn.execute('PRAGMA freelist_count').fetchone()[0]
            return {**result, 'status': 'complete', 'can_continue': free > 0}
        if kind != 'table':
            return result
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE name='lineage_repack'").fetchone()
        # Tiny relations do not justify any layout migration.
        if not exists and conn.execute('SELECT count(*) FROM (SELECT 1 FROM round_documents LIMIT 10000)').fetchone()[0] < 10000:
            return result
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA synchronous=FULL')
        if max_main_bytes is not None:
            size = conn.execute('PRAGMA page_size').fetchone()[0]
            conn.execute(f'PRAGMA max_page_count={max_main_bytes//size}')
        deadline = started+budget_seconds
        conn.set_progress_handler(lambda: int(time.monotonic()>deadline), 2000)
        conn.execute('BEGIN IMMEDIATE')
        if not exists:
            _prepare(conn)
        phase, copied, verified = conn.execute('SELECT phase,copied_through,verified_through FROM lineage_repack WHERE id=1').fetchone()
        if phase == 'copy':
            result['copied_rows'] = _copy(conn, copied, batch_rows)
        elif phase == 'verify':
            result['verified_rows'] = _verify(conn, verified, batch_rows)
        elif phase == 'switch':
            conn.set_progress_handler(lambda: int(time.monotonic()>started+max(10,budget_seconds)), 2000)
            # Wiping the entire old B-tree would produce a huge WAL during a
            # layout-only switch. All logical values remain in the new tables.
            conn.execute('PRAGMA secure_delete=OFF')
            result['rows_preserved'] = _switch(conn)
        phase = conn.execute('SELECT phase FROM lineage_repack WHERE id=1').fetchone()[0]
        conn.commit()
        result.update(status=phase, can_continue=True)
    except sqlite3.OperationalError as exc:
        conn.set_progress_handler(None, 0)
        conn.rollback()
        if 'locked' in str(exc) or 'interrupted' in str(exc):
            result.update(status='retry', can_continue=True, copied_rows=0, verified_rows=0)
        else:
            raise
    finally:
        conn.close()
    result['elapsed_seconds'] = round(time.monotonic()-started, 3)
    return result
