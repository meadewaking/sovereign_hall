import asyncio
from contextlib import closing
import hashlib
import json
import random
import sqlite3
import threading
import zlib

import pytest

from sovereign_hall.services import archive_repack
from sovereign_hall.services.archive_repack import repack_archive, build_packed_copy
from sovereign_hall.services.storage_archive import ArchiveStore, archive_write_lock, canonical_bytes


def revisions(tmp_path, count=12):
    store = ArchiveStore(tmp_path / 'wiki.sqlite3')
    # Repeated revisions of poorly compressible evidence reproduce archive
    # growth after all hot originals have already been removed.
    shared = random.Random(7).randbytes(200_000)
    pointers = [store.put('wiki_page', 'wiki/entities/company.md',
                         {'content': shared + f'版本{i}'.encode()}) for i in range(count)]
    return store, pointers, shared


def repack(store, **kwargs):
    return repack_archive(store.path, max_bytes=1024**3,
                          staging_root=store.path.parent / 'staging', minimum_raw_bytes=1, **kwargs)


def test_lossless_repack_recovers_archive_only_pressure_and_accepts_future_writes(tmp_path):
    store, pointers, shared = revisions(tmp_path)
    document = store.put('document', 'source', {'content': 'alpha 原始证据'})
    before = store.path.stat().st_size
    with closing(sqlite3.connect(store.path)) as conn:
        rowids = conn.execute('SELECT rowid,kind,record_key,revision FROM records ORDER BY rowid').fetchall()
    report = repack(store)
    assert report['records_verified'] == 13
    assert report['after_bytes'] < before * .6
    assert report['reclaimed_bytes'] == before - store.path.stat().st_size
    for i, locator in enumerate(pointers):
        assert store.read(locator) == {'content': shared + f'版本{i}'.encode()}
    assert store.read(document) == {'content': 'alpha 原始证据'}
    assert store.find_documents('alpha')[0][0] == 'source'
    with closing(sqlite3.connect(store.path)) as conn:
        assert rowids == conn.execute('SELECT rowid,kind,record_key,revision FROM records ORDER BY rowid').fetchall()
    # Re-ingestion of a packed revision is a no-op, not a broken CAS rewrite.
    store.put('wiki_page', pointers[0]['key'], store.read(pointers[0]))
    assert store.migrate_legacy()['converted'] == 0
    new = store.put('wiki_page', pointers[0]['key'], {'content': shared + b'new'})
    assert store.read(new)['content'].endswith(b'new')
    assert repack(store)['records_verified'] == 14
    assert store.read(pointers[0])['content'] == shared + '版本0'.encode()
    assert not list((tmp_path / 'staging').iterdir())


def test_legacy_records_and_packed_dense_copy_keep_exact_locators(tmp_path):
    store, pointers, _ = revisions(tmp_path, 2)
    value = {'content': b'\x00\xfflegacy'}
    raw = canonical_bytes(value)
    revision = hashlib.sha256(raw).hexdigest()
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute('INSERT INTO records VALUES(?,?,?,?,?)', ('wiki_raw', 'raw/old.md', revision,
                                                              zlib.compress(raw), len(raw)))
        conn.commit()
    repack(store)
    locator = {'archive_path': str(store.path), 'kind': 'wiki_raw', 'key': 'raw/old.md', 'revision': revision}
    assert store.read(locator) == value
    dense = tmp_path / 'dense.sqlite3'
    assert store.build_dense_copy(dense)['records_verified'] == 3
    assert store.read({**locator, 'archive_path': str(dense)}) == value


def test_corruption_and_unknown_tables_keep_original_generation(tmp_path):
    store, pointers, _ = revisions(tmp_path, 2)
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute('CREATE TABLE annotations(note TEXT)')
        conn.commit()
    original = store.path.read_bytes()
    with pytest.raises(RuntimeError, match='Unknown archive table'):
        repack(store)
    assert store.path.read_bytes() == original
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute('DROP TABLE annotations')
        conn.execute("UPDATE archive_blobs SET payload=x'00'")
        conn.commit()
    original = store.path.read_bytes()
    with pytest.raises(zlib.error):
        repack(store)
    assert store.path.read_bytes() == original
    assert not list((tmp_path / 'staging').iterdir())


def test_packed_member_and_block_checksums_are_enforced(tmp_path):
    store, pointers, _ = revisions(tmp_path, 2)
    repack(store)
    with closing(sqlite3.connect(store.path)) as conn:
        conn.execute('UPDATE packed_records SET raw_offset=raw_offset+1')
        conn.commit()
    with pytest.raises(RuntimeError, match='member checksum'):
        store.read(pointers[0])


def test_existing_reader_keeps_its_generation_during_atomic_publish(tmp_path):
    store, pointers, _ = revisions(tmp_path, 2)
    with closing(sqlite3.connect(store.path.as_uri() + '?mode=ro', uri=True)) as reader:
        reader.execute('BEGIN')
        old = store._read_record(reader, pointers[0]['kind'], pointers[0]['key'], pointers[0]['revision'])
        repack(store)
        assert store._read_record(reader, pointers[0]['kind'], pointers[0]['key'], pointers[0]['revision']) == old
        assert store.read(pointers[0]) == old


def test_crash_staging_is_removed_even_when_archive_is_below_repack_threshold(tmp_path):
    store, pointers, _ = revisions(tmp_path, 2)
    staging = tmp_path / 'staging'
    staging.mkdir()
    candidate = staging / (hashlib.sha256(str(store.path).encode()).hexdigest()[:24] + '.sqlite3')
    candidate.write_bytes(b'interrupted generation')
    journal = candidate.with_name(candidate.name + '-journal')
    journal.write_bytes(b'interrupted journal')
    result = repack_archive(store.path, max_bytes=1024**3, staging_root=staging)
    assert result['status'] == 'complete'
    assert not candidate.exists() and not journal.exists()
    assert store.read(pointers[0])['content']


def test_writer_lock_and_changed_source_prevent_lost_records(tmp_path, monkeypatch):
    store, pointers, _ = revisions(tmp_path, 2)
    with archive_write_lock(store.path):
        with pytest.raises(BlockingIOError):
            store.put('wiki_raw', 'new', {'content': b'new'})
    build = archive_repack.build_packed_copy

    def changed(*args, **kwargs):
        report = build(*args, **kwargs)
        # Simulate an old, non-cooperating writer changing the source.
        with closing(sqlite3.connect(store.path)) as conn:
            conn.execute('UPDATE records SET raw_bytes=raw_bytes+1')
            conn.commit()
        return report

    monkeypatch.setattr(archive_repack, 'build_packed_copy', changed)
    with pytest.raises(RuntimeError, match='changed during repacking'):
        repack(store)
    assert store.read(pointers[0])['content']
    with closing(sqlite3.connect(store.path)) as conn:
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='packed_records'").fetchone()


async def test_cancellation_joins_worker_before_releasing_writer_lock(tmp_path, monkeypatch):
    store, pointers, _ = revisions(tmp_path, 2)
    started = threading.Event()
    finished = threading.Event()
    stopping = threading.Event()
    release = threading.Event()

    def build(*args, stop=None, **kwargs):
        started.set()
        assert stop.wait(5)
        stopping.set()
        assert release.wait(5)
        finished.set()
        raise InterruptedError('cancelled')

    monkeypatch.setattr(archive_repack, 'build_packed_copy', build)
    task = asyncio.create_task(archive_repack.repack_archive_async(
        store.path, max_bytes=1024**3, staging_root=tmp_path / 'staging', minimum_raw_bytes=1))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    assert await asyncio.to_thread(stopping.wait, 5)
    task.cancel()  # A second SIGTERM must still join the archive thread.
    await asyncio.sleep(.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    store.put('wiki_raw', 'new', {'content': b'new'})
    assert store.read(pointers[0])['content']


async def test_governor_recovers_to_sixty_percent_with_no_hot_files(tmp_path, monkeypatch):
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.storage_governor import StorageGovernor, directory_size
    root = tmp_path / 'knowledge'
    archive_dir = root / '.archives'
    archive_dir.mkdir(parents=True)
    store, pointers, _ = revisions(archive_dir, count=64)
    before = directory_size(root)
    cfg = get_config().get('knowledge_wiki', {})
    monkeypatch.setitem(cfg, 'max_size_bytes', int(before / .92))
    db = DatabaseService(str(tmp_path / 'main.sqlite3'))
    try:
        await db._init_db()
        governor = StorageGovernor(db, root)
        initial = await governor.initialize()
        assert not initial.research_round_allowed
        after = await governor.before_round()
        assert after.research_round_allowed
        assert after.knowledge.ratio <= .6
        assert governor.last_maintenance['archive']['wiki_archive']['records_verified'] == len(pointers)
        assert governor.last_maintenance['archive']['wiki']['raw_removed'] == 0
        assert not (await governor.before_round()).recovery_targets
        assert store.read(pointers[-1])['content'].endswith('版本63'.encode())
    finally:
        await db.close()
