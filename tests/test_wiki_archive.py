from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from sovereign_hall.services.storage_archive import ArchiveStore
from sovereign_hall.services.wiki_archive import pack_raw, read_raw, wiki_archive
from sovereign_hall.services.wiki_knowledge import WikiStore, dump_markdown, parse_frontmatter
from sovereign_hall.scripts import compact_storage, cleanup_redundant_knowledge
from sovereign_hall.services.storage_governor import directory_size


@pytest.fixture
def vault(tmp_path):
    store = WikiStore(tmp_path / 'knowledge')
    store.ensure_vault()
    raw = store.raw_dir / 'source.md'
    content = '---\ncreated: 2020-01-01\n---\n完整证据、反例、原始引用\n' * 10
    raw.write_text(content)
    (store.sources_dir / raw.name).write_text('---\ntype: source\nsource_id: doc\n---\n仅有摘要')
    return store, raw, content


def test_pack_raw_and_restore_exact_bytes_without_many_small_archives(vault, tmp_path):
    store, raw, original = vault
    result = pack_raw(store.root, limit=1)
    assert result['raw_removed'] == 1
    assert not raw.exists()
    assert read_raw(store.root, 'raw/source.md') == original.encode()
    assert store.read_raw('raw/source.md') == original
    front, _ = parse_frontmatter((store.sources_dir / raw.name).read_text())
    locator = front['raw_archive']
    output = tmp_path / 'restored.md'
    completed = subprocess.run([sys.executable, 'scripts/restore_archive.py', '--archive-path', locator['archive_path'],
        '--kind', 'wiki_raw', '--key', locator['key'], '--revision', locator['revision'], '--output', str(output)],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=15)
    assert completed.returncode == 0, completed.stderr
    assert output.read_bytes() == original.encode()
    assert list((store.root / '.archives').glob('*.sqlite3')) == [wiki_archive(store.root).path]
    assert wiki_archive(store.root).path.with_suffix('.sqlite3.lock').exists()
    assert pack_raw(store.root)['raw_removed'] == 0


def test_pack_failure_preserves_original_and_source_pointer(vault, monkeypatch):
    store, raw, original = vault
    def failing_read(locator):
        raise RuntimeError('verification injected')
    monkeypatch.setattr(ArchiveStore, 'read', staticmethod(failing_read))
    report = pack_raw(store.root)
    assert report['retained_errors'] == 1
    assert report['errors'][0]['error'] == 'verification injected'
    assert raw.read_text() == original
    front, _ = parse_frontmatter((store.sources_dir / raw.name).read_text())
    assert 'raw_archive' not in front


def test_page_bounding_archives_full_source_and_keeps_whole_paragraphs(vault):
    store, _, _ = vault
    store.aggregate_page_max_chars = 1024
    store.aggregate_source_limit = 1
    store.all_wiki_pages()  # Exercise the incremental in-memory cache too.
    path = store.topics_dir / 'topic.md'
    full = dump_markdown({'type': 'topic', 'title': 'topic', 'sources': ['old', 'new']},
                         '# topic\n\n' + '较早的长段落' * 600 + '\n\n最近的完整段落')
    store.write_page(path, full)
    front, body = parse_frontmatter(path.read_text())
    original = ArchiveStore.read(front['full_page_archive'])['content'].decode()
    assert '较早的长段落' * 600 in original
    assert parse_frontmatter(original)[0]['sources'] == ['old', 'new']
    assert front['sources'] == ['new']
    assert body.endswith('最近的完整段落\n') or body.endswith('最近的完整段落')
    assert len(body) <= 1024
    assert any(page.body == body for page in store.all_wiki_pages() if page.path == path)


def test_raw_overwrite_preserves_previous_version(vault):
    store, raw, original = vault
    store.write_text(raw, 'new text')
    locator = wiki_archive(store.root).latest_locator('wiki_raw', 'raw/source.md')
    assert ArchiveStore.read(locator)['content'] == original.encode()
    assert raw.read_text() == 'new text'


def test_online_vacuum_never_swaps_file_or_unlinks_live_wal(tmp_path, monkeypatch):
    path = tmp_path / 'db.sqlite3'
    other = sqlite3.connect(path)
    other.execute('PRAGMA journal_mode=WAL')
    other.execute('CREATE TABLE records(value TEXT)')
    other.execute("INSERT INTO records VALUES ('original')")
    other.commit()
    inode = path.stat().st_ino
    monkeypatch.setattr(compact_storage, 'DB_PATH', path)
    monkeypatch.setattr(compact_storage, 'live_runner_pid', lambda: None)
    compact_storage.vacuum_into_replacement()
    assert path.stat().st_ino == inode
    other.execute("INSERT INTO records VALUES ('after vacuum')")
    other.commit()
    with sqlite3.connect(path) as check:
        assert check.execute('SELECT COUNT(*) FROM records').fetchone()[0] == 2
    other.close()
    monkeypatch.setattr(compact_storage, 'live_runner_pid', lambda: 123)
    with pytest.raises(RuntimeError, match='Stop'):
        compact_storage.vacuum_into_replacement()


def test_legacy_deletion_disabled_at_both_service_entrypoints():
    with pytest.raises(RuntimeError, match='disabled'):
        cleanup_redundant_knowledge.cleanup_database(False)
    with pytest.raises(RuntimeError, match='disabled'):
        cleanup_redundant_knowledge.cleanup_wiki(False)


def test_bulk_packing_tracks_actual_allocations_instead_of_accumulating_reserves(vault):
    store, raw, original = vault
    for number in range(100):
        (store.raw_dir/f'{number}.md').write_text(original)
        (store.sources_dir/f'{number}.md').write_text('---\nsource_id: retained\n---\n摘要')
    before = directory_size(store.root)
    report = pack_raw(store.root, limit=101, max_bytes=before+160*1024)
    assert report['raw_removed'] == 101
    assert report['after_bytes'] == directory_size(store.root)
    assert read_raw(store.root, 'raw/39.md') == original.encode()


def test_pack_stops_at_target_and_retains_the_remaining_hot_files(vault):
    store, raw, original = vault
    report = pack_raw(store.root, target_bytes=directory_size(store.root))
    assert report['raw_removed'] == 0
    assert raw.read_bytes() == original.encode()


def test_archive_budget_does_not_unlink_when_time_is_exhausted(vault):
    store, raw, original = vault
    report = pack_raw(store.root, budget_seconds=0)
    assert report['budget_exhausted']
    assert report['raw_removed'] == 0
    assert raw.read_bytes() == original.encode()


async def test_runtime_archive_updates_cached_source_without_reloading_every_page(vault, tmp_path):
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.storage_governor import StorageGovernor, StorageSnapshot, StoreWatermark
    store, raw, original = vault
    (store.root/'unrelated.cache').write_bytes(b'cache'*210000)
    cached = store.all_wiki_pages()
    db = DatabaseService(str(tmp_path/'database.db'))
    governor = StorageGovernor(db, store)
    used = directory_size(store.root)
    wiki = StoreWatermark(used, used+160*1024, .7, .8, .9)
    database = StoreWatermark(0, 1024**3, .7, .8, .9)
    report = await governor.archive_history(StorageSnapshot(database, wiki, 'now'))
    assert report['wiki']['raw_removed'] == 1
    assert store._pages_cache is cached
    page = next(page for page in cached if page.path == store.sources_dir/raw.name)
    assert page.frontmatter['raw_archive']['key'] == 'raw/source.md'
    assert store.used_bytes() == directory_size(store.root)
    assert store.read_raw('raw/source.md') == original


def test_changed_source_page_is_retained_instead_of_overwriting_new_evidence(vault, monkeypatch):
    from sovereign_hall.services import wiki_archive as module
    store, raw, original = vault
    source = store.sources_dir/raw.name
    dump = module.dump_markdown
    changed = '---\nsource_id: doc\n---\n刚刚补充的新证据'
    def edit_before_pointer_install(front, body):
        source.write_text(changed)
        return dump(front, body)
    monkeypatch.setattr(module, 'dump_markdown', edit_before_pointer_install)
    report = pack_raw(store.root)
    assert report['retained_changed'] == 1
    assert report['raw_removed'] == 0
    assert raw.read_bytes() == original.encode()
    assert source.read_text() == changed


def test_timed_scan_resumes_after_ineligible_prefix_instead_of_repeating_it(vault, monkeypatch):
    import time
    from sovereign_hall.services import wiki_archive as module
    store, raw, original = vault
    raw.rename(store.raw_dir/'z.md')
    (store.sources_dir/raw.name).rename(store.sources_dir/'z.md')
    (store.raw_dir/'a.md').write_text('---\ncreated: unknown\n---\nineligible prefix')
    parse = module.parse_frontmatter
    reads = []
    def slow_prefix(value):
        if 'ineligible prefix' in value:
            reads.append('prefix')
            time.sleep(.02)
        return parse(value)
    monkeypatch.setattr(module, 'parse_frontmatter', slow_prefix)
    first = pack_raw(store.root, budget_seconds=.01)
    assert first['budget_exhausted'] and not first['scan_complete']
    assert first['raw_removed'] == 0 and first['next_after'] == 'a.md'
    second = pack_raw(store.root, budget_seconds=1, after_name=first['next_after'])
    assert second['raw_removed'] == 1
    assert second['scan_complete'] and second['next_after'] is None
    assert reads == ['prefix']
    assert read_raw(store.root, 'raw/z.md') == original.encode()


def test_pressure_can_archive_today_hot_copy_without_losing_original(vault):
    from datetime import date
    store, raw, _ = vault
    original = dump_markdown({'created': date.today().isoformat(), 'source_id': 'doc'}, '今日完整原文、反例和引文')
    raw.write_text(original)
    assert pack_raw(store.root)['raw_removed'] == 0
    assert raw.read_text() == original
    report = pack_raw(store.root, include_recent=True)
    assert report['raw_removed'] == 1
    assert read_raw(store.root, 'raw/source.md') == original.encode()


def test_pressure_still_protects_future_dates_and_incomplete_source_pages(vault):
    store, raw, _ = vault
    raw.write_text('---\ncreated: 2999-01-01\n---\nfuture')
    assert pack_raw(store.root, include_recent=True)['raw_removed'] == 0
    raw.write_text('---\ncreated: 2020-01-01\n---\nnot compiled yet')
    (store.sources_dir/raw.name).unlink()
    assert pack_raw(store.root, include_recent=True)['raw_removed'] == 0
    assert raw.exists()
