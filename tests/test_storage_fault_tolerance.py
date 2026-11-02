"""Fault injection against real temporary stores; never production books."""
import asyncio
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_governor import StorageGovernor, StorageSnapshot, StoreWatermark, directory_size
from sovereign_hall.services.wiki_archive import pack_raw, read_raw
from sovereign_hall.services.wiki_source_archive import pack_sources, SourcePageArchive
from sovereign_hall.services.storage_archive import ArchiveStore


PAGE = '---\ncreated: 2020-01-01\ntype: source\nsource_id: original\n---\n' + '完整原始证据\n' * 70_000


@pytest.fixture
async def store(tmp_path):
    root = tmp_path / 'knowledge'
    (root / 'raw').mkdir(parents=True)
    (root / 'wiki/sources').mkdir(parents=True)
    (root / 'raw/one.md').write_text(PAGE)
    (root / 'wiki/sources/one.md').write_text(PAGE)
    (root / 'wiki/sources/two.md').write_text(PAGE)
    db = DatabaseService(str(tmp_path / 'main.sqlite3'))
    await db._init_db()
    governor = StorageGovernor(db, root)
    used = directory_size(root)
    snapshot = StorageSnapshot(StoreWatermark(0, 1024**3, .6, .8, .9),
        StoreWatermark(used, int(used / .92), .6, .8, .9), 'now')
    try:
        yield governor, snapshot
    finally:
        await db.close()


@pytest.mark.parametrize('failed', ['memory', 'archive_format', 'wiki_archive', 'wiki', 'wiki_sources'])
async def test_one_strategy_failure_does_not_block_others_and_recovers_after_short_backoff(store, monkeypatch, failed):
    import sovereign_hall.services.storage_governor as module
    import sovereign_hall.services.archive_repack as repack
    import sovereign_hall.services.bounded_memory as memory
    import sovereign_hall.services.wiki_archive as raw
    import sovereign_hall.services.wiki_source_archive as sources
    governor, snapshot = store
    clock = [100.0]
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda: clock[0], monotonic_ns=time.monotonic_ns))

    def fail(*args, **kwargs):
        raise BlockingIOError('injected temporary writer contention')

    async def async_fail(*args, **kwargs):
        fail()

    target, name, operation = {
        'memory': (memory.BoundedMemoryStore, 'backfill', async_fail),
        'archive_format': (ArchiveStore, 'migrate_legacy', fail),
        'wiki_archive': (repack, 'repack_archive_async', async_fail),
        'wiki': (raw, 'pack_raw', fail),
        'wiki_sources': (sources, 'pack_sources', fail),
    }[failed]
    original = getattr(target, name)
    monkeypatch.setattr(target, name, operation)
    report = await governor.archive_history(snapshot)
    assert failed in report['errors']
    assert report['progressed']
    assert directory_size(governor.knowledge_root) < snapshot.knowledge.used_bytes
    assert governor._archive_retry_after == 130.0
    assert (await governor.archive_history(snapshot))['status'] == 'backoff'
    monkeypatch.setattr(target, name, original)
    clock[0] = 131.0
    retried = await governor.archive_history(snapshot)
    assert retried['status'] == 'completed'
    assert not retried.get('error')


async def test_changed_inputs_bypass_no_candidate_backoff(store, monkeypatch):
    import sovereign_hall.services.wiki_archive as raw
    governor, snapshot = store
    for path in governor.knowledge_root.glob('**/*.md'):
        path.unlink()
    await governor.archive_history(snapshot)
    assert (await governor.archive_history(snapshot))['status'] == 'backoff'
    (governor.knowledge_root / 'raw/new.md').write_text(PAGE)
    (governor.knowledge_root / 'wiki/sources/new.md').write_text(PAGE)
    result = await governor.archive_history(snapshot)
    assert result['status'] == 'completed'
    assert result['wiki']['raw_removed'] == 1
    assert read_raw(governor.knowledge_root, 'raw/new.md') == PAGE.encode()


async def test_pending_resume_uses_actual_usage_even_when_cache_is_below_stop_gate(store, monkeypatch):
    governor, _ = store
    monkeypatch.setitem(governor.wiki_config, 'max_size_bytes', 1000)
    governor._resume_pending = ['knowledge']
    governor._recovery_targets = ['knowledge']
    calls = []

    async def size(*, refresh=False):
        calls.append(refresh)
        return 500 if refresh else 850

    monkeypatch.setattr(governor, '_knowledge_size', size)
    result = await governor.before_round()
    assert calls == [False, True]
    assert result.research_round_allowed
    assert not result.resume_pending
    assert not result.recovery_targets


async def test_concurrent_snapshots_cannot_publish_older_paused_state_after_recovery(store, monkeypatch):
    governor, _ = store
    monkeypatch.setitem(governor.wiki_config, 'max_size_bytes', 1000)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def size(*, refresh=False):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()
            return 950
        return 500

    monkeypatch.setattr(governor, '_knowledge_size', size)
    first = asyncio.create_task(governor.snapshot(refresh=True, refresh_knowledge=True))
    await entered.wait()
    second = asyncio.create_task(governor.snapshot(refresh=True, refresh_knowledge=True))
    await asyncio.sleep(.01)
    assert not second.done()
    release.set()
    assert not (await first).research_round_allowed
    assert (await second).research_round_allowed
    assert json.loads(governor.status_path.read_text())['research_round_allowed']


async def test_cancelled_raw_worker_keeps_governor_lock_until_thread_finishes(store, monkeypatch):
    import sovereign_hall.services.wiki_archive as raw
    governor, snapshot = store
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def worker(*args, **kwargs):
        started.set()
        assert release.wait(5)
        finished.set()
        return {'raw_removed': 0, 'retained_changed': 0, 'next_after': None,
                'scan_complete': True, 'after_bytes': snapshot.knowledge.used_bytes}

    monkeypatch.setattr(raw, 'pack_raw', worker)
    task = asyncio.create_task(governor.archive_history(snapshot))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
        assert governor._lock.locked()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        assert not governor._lock.locked()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_repeated_shutdown_waits_for_background_storage_to_finish(store):
    governor, _ = store
    started, release = asyncio.Event(), asyncio.Event()

    async def background():
        started.set()
        await release.wait()

    governor._background_task = asyncio.create_task(background())
    await started.wait()
    task = asyncio.create_task(governor.close())
    try:
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
        assert governor._background_task is not None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert governor._background_task is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_capacity_wait_honors_short_retry_deadline_without_busy_loop(monkeypatch):
    from sovereign_hall.run_discussion import await_storage_capacity
    full = StoreWatermark(95, 100, .6, .8, .9)
    ready = StoreWatermark(50, 100, .6, .8, .9)

    class Governor:
        calls = 0
        last_maintenance = {'archive': {'status': 'backoff', 'retry_after_seconds': .02}}

        async def before_round(self):
            self.calls += 1
            mark = full if self.calls == 1 else ready
            return StorageSnapshot(mark, mark, 'now')

    delays = []

    async def sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(asyncio, 'sleep', sleep)
    assert (await await_storage_capacity(Governor(), pause_seconds=60)).research_round_allowed
    assert delays == [.02]


@pytest.mark.parametrize('kind', ['raw', 'sources'])
def test_corrupt_first_file_does_not_starve_valid_later_files(tmp_path, kind):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'wiki/sources').mkdir(parents=True)
    directory = tmp_path / ('raw' if kind == 'raw' else 'wiki/sources')
    (directory / '000.md').write_bytes(b'\xff\x00invalid utf8')
    (directory / '001.md').write_text(PAGE)
    if kind == 'raw':
        (tmp_path / 'wiki/sources/001.md').write_text(PAGE)
        result = pack_raw(tmp_path, max_bytes=16*1024**2)
        assert result['raw_removed'] == 1
        assert read_raw(tmp_path, 'raw/001.md') == PAGE.encode()
    else:
        result = pack_sources(tmp_path, max_bytes=16*1024**2, used_bytes=directory_size(tmp_path), target_bytes=0)
        assert result['pages_removed'] == 1
        assert SourcePageArchive(tmp_path).read('wiki/sources/001.md') == PAGE.encode()
    assert result['retained_errors'] == 1
    assert result['errors'][0]['path'] == '000.md'
    assert (directory / '000.md').read_bytes() == b'\xff\x00invalid utf8'


def test_failed_post_commit_archive_write_recounts_space_before_next_candidate(tmp_path, monkeypatch):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'wiki/sources').mkdir(parents=True)
    for name in ('000.md', '001.md'):
        (tmp_path / 'raw' / name).write_text(PAGE)
        (tmp_path / 'wiki/sources' / name).write_text(PAGE)
    put = ArchiveStore.put

    def fail_after_commit(self, kind, key, value):
        result = put(self, kind, key, value)
        if key.endswith('000.md'):
            raise RuntimeError('injected committed readback failure')
        return result

    monkeypatch.setattr(ArchiveStore, 'put', fail_after_commit)
    report = pack_raw(tmp_path, max_bytes=16*1024**2)
    assert report['retained_errors'] == 1
    assert report['raw_removed'] == 1
    assert (tmp_path / 'raw/000.md').read_text() == PAGE
    assert report['after_bytes'] == directory_size(tmp_path)
