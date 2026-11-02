"""Legacy initialization must not hold write locks for no-op data migrations."""
import sqlite3
import pytest
from sovereign_hall.services.database import DatabaseService


@pytest.mark.asyncio
@pytest.mark.parametrize('after_maintenance', [False, True])
async def test_storage_settings_release_snapshot_before_other_account_writes(tmp_path, after_maintenance):
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.services.storage_governor import StorageGovernor

    db = DatabaseService(str(tmp_path/'admission.db'))
    await db._init_db()
    await db.init_report_tables()
    account = DatabaseService(str(db.db_path))
    try:
        governor = StorageGovernor(db, tmp_path/'knowledge')
        await governor.initialize()
        # Match production: the account initializes and commits after capacity
        # settings, before the research owner opens its first round.
        await account._init_db()
        if after_maintenance:
            await governor.online_maintenance()
        async with account.transaction(immediate=True) as conn:
            await conn.execute("INSERT INTO system_stats(key,value) VALUES('account_observation','1')")
        round_state = await ResearchRoundCoordinator(db).start(
            base_topic='writer contention', research_objective='keep the research owner alive')
        assert await ResearchRoundCoordinator(db).get(round_state.id)
        assert not db._connection.in_transaction
        async with db._connection.execute('PRAGMA max_page_count') as cursor:
            assert (await cursor.fetchone())[0] == int(governor.db_config.get('max_main_file_bytes', 1984*1024**2)) // 4096
        with sqlite3.connect(db.db_path, timeout=.05) as observer:
            assert observer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        async with account._connection.execute("SELECT value FROM system_stats WHERE key='account_observation'") as cursor:
            assert (await cursor.fetchone())[0] == '1'
    finally:
        await account.close()
        await db.close()


@pytest.mark.asyncio
async def test_migrated_blacklist_does_not_contend_with_an_active_writer(tmp_path):
    db = DatabaseService(str(tmp_path/'lock.db'))
    await db._init_db()
    conn = await db._get_connection()
    await conn.execute('ALTER TABLE blacklist ADD COLUMN created_at TEXT')
    await conn.execute("INSERT INTO blacklist(ticker,failure_count,added_at,created_at) VALUES ('fixture',1,'2020-01-01','2020-01-01')")
    await conn.commit()
    writer = sqlite3.connect(db.db_path)
    writer.execute('BEGIN IMMEDIATE')
    await conn.execute('PRAGMA busy_timeout=10')
    try:
        # Reproduce the old no-op statement's real writer contention.
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            await conn.execute('UPDATE blacklist SET failure_count=1 WHERE failure_count IS NULL')
        await conn.rollback()
        await db._backfill_blacklist_fields(conn)
        assert not conn.in_transaction
    finally:
        writer.rollback(); writer.close()
        await db.close()


@pytest.mark.asyncio
async def test_legacy_blacklist_repairs_preserve_dates_and_repeat_without_writes(tmp_path):
    db = DatabaseService(str(tmp_path/'legacy.db'))
    await db._init_db()
    conn = await db._get_connection()
    await conn.execute('ALTER TABLE blacklist ADD COLUMN created_at TEXT')
    await conn.executemany('INSERT INTO blacklist(ticker,failure_count,added_at,created_at) VALUES (?,?,?,?)',
                           [('missing',None,None,'2020-01-01'), ('unknown',2,None,None), ('known',3,'2021-02-03','2020-01-01')])
    await conn.commit()
    try:
        await db._backfill_blacklist_fields(conn)
        await conn.commit()
        async with conn.execute('SELECT ticker,failure_count,added_at FROM blacklist ORDER BY ticker') as cursor:
            rows = [tuple(row) for row in await cursor.fetchall()]
        assert rows == [('known',3,'2021-02-03'),('missing',1,'2020-01-01'),('unknown',2,None)]
        changes = conn.total_changes
        await db._backfill_blacklist_fields(conn)
        assert conn.total_changes == changes and not conn.in_transaction
    finally:
        await db.close()
