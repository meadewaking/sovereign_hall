from datetime import date
from pathlib import Path
import sqlite3

import pytest

from sovereign_hall.services.storage_governor import directory_size, StorageGovernor, StorageSnapshot, StoreWatermark
from sovereign_hall.services.wiki_archive import pack_raw, read_raw
from sovereign_hall.services.wiki_source_archive import SourcePageArchive, pack_sources, read_source
from sovereign_hall.services.wiki_knowledge import WikiStore, dump_markdown, parse_frontmatter


def source(store, name='old.md', **front):
    path = store.sources_dir / name
    path.write_text(dump_markdown(dict(type='source', title='罕见失败证据', source_id='doc_old',
                                      url='https://example.org/old', created='2020-01-01', **front),
                                  '# 罕见失败证据\n\n失败、反例和完整引用。\n'))
    return path


def pack(store, **kwargs):
    return pack_sources(store.root, max_bytes=16*1024**2, used_bytes=directory_size(store.root),
                        target_bytes=0, **kwargs)


def test_source_pack_preserves_bytes_paths_search_inputs_and_roots(tmp_path):
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store); original = page.read_bytes()
    cached = store.all_wiki_pages()
    report = pack(store)
    assert report['pages_removed'] == 1 and not page.exists()
    assert store.all_wiki_pages() is cached
    assert store.read_page('wiki/sources/old.md').encode() == original
    assert store.page_exists('wiki/sources/old.md')
    reloaded = WikiStore(tmp_path)
    assert [(p.rel_path, p.body, p.frontmatter) for p in reloaded.all_wiki_pages()] == [
        (p.rel_path, p.body, p.frontmatter) for p in cached]
    from sovereign_hall.services.research_memory import ProtectionPolicy
    conn = sqlite3.connect(':memory:')
    policy = ProtectionPolicy(conn, wiki_root=tmp_path)
    policy._wiki_references()
    assert 'doc_old' in policy._wiki_ids and 'https://example.org/old' in policy._wiki_urls
    assert report['after_bytes'] == directory_size(tmp_path)
    conn.close()


def test_raw_pointer_remains_readable_when_its_source_is_cold(tmp_path):
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store)
    raw = store.raw_dir / page.name
    original = dump_markdown({'created': '2020-01-01'}, '所有原文与反例\n' * 300).encode()
    raw.write_bytes(original)
    assert pack(store)['pages_removed'] == 0  # Raw pointer must be published first.
    assert pack_raw(tmp_path)['raw_removed'] == 1
    assert pack(store)['pages_removed'] == 1
    assert read_raw(tmp_path, 'raw/old.md') == original


def test_failed_committed_verification_keeps_entire_batch_hot(tmp_path, monkeypatch):
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store); original = page.read_bytes()
    def broken(self, entries):
        raise RuntimeError('injected verification failure')
    monkeypatch.setattr(SourcePageArchive, 'verify_batch', broken)
    with pytest.raises(RuntimeError, match='verification failure'):
        pack(store)
    assert page.read_bytes() == original


def test_failed_archive_creation_leaves_hot_source_and_no_broken_reader(tmp_path):
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store); original = page.read_bytes()
    archive = SourcePageArchive(tmp_path)
    with pytest.raises(sqlite3.DatabaseError):
        archive.put_batch([('wiki/sources/old.md', original, {})], max_bytes=4096)
    assert not archive.path.exists()
    assert WikiStore(tmp_path).read_page('wiki/sources/old.md').encode() == original


async def test_search_and_page_update_work_after_cold_reload(tmp_path):
    from sovereign_hall.services.wiki_knowledge import WikiSearchIndex
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store)
    pack(store)
    store = WikiStore(tmp_path)
    index = WikiSearchIndex(store, embedding_enabled=False)
    hits = await index.search('罕见失败证据', top_k=2)
    assert hits and hits[0].page.rel_path == 'wiki/sources/old.md'
    store.write_page(page, dump_markdown({'type': 'source', 'title': '罕见失败证据'}, '新补充的材料'))
    updated = store.read_page('wiki/sources/old.md')
    assert '失败、反例和完整引用' in updated and '新补充的材料' in updated
    assert len(store.all_wiki_pages()) == 1


async def test_reingesting_unchanged_cold_source_does_not_regrow_hot_files(tmp_path):
    from sovereign_hall.core import Document
    from sovereign_hall.services.wiki_knowledge import WikiKnowledgeBase
    kb = WikiKnowledgeBase(root=tmp_path, embedding_enabled=False)
    await kb.initialize()
    doc = Document(doc_id='evidence-1', title='可核验的罕见反例', url='https://example.org/case',
                   content='原始出处和完整反例。' * 80)
    await kb.add_document(doc)
    assert pack_raw(tmp_path, include_recent=True)['raw_removed'] == 1
    assert pack(kb.store, include_recent=True)['pages_removed'] == 1
    log_before = kb.store.log_path.read_bytes()
    kb = WikiKnowledgeBase(root=tmp_path, embedding_enabled=False)
    await kb.initialize()
    await kb.add_document(doc)
    assert not list(kb.store.sources_dir.glob('*.md'))
    assert not list(kb.store.raw_dir.glob('*.md'))
    assert kb.store.log_path.read_bytes() == log_before
    assert kb.get_stats()['sources'] == 1


async def test_search_finds_cold_evidence_beyond_first_path_window(tmp_path, monkeypatch):
    from sovereign_hall.services import wiki_knowledge as module
    store = WikiStore(tmp_path); store.ensure_vault()
    for name in ('a', 'b', 'c'):
        (store.entities_dir / f'{name}.md').write_text(dump_markdown({'type': 'entity', 'title': name},
            '罕见 档案 失败 实验 证据。' * 200))
    source(store)
    pack(store)
    monkeypatch.setattr(module, 'MAX_SEARCH_PAGES', 2)
    hits = await module.WikiSearchIndex(WikiStore(tmp_path), embedding_enabled=False).search('罕见失败证据', top_k=1)
    assert hits and hits[0].page.rel_path == 'wiki/sources/old.md'


def test_safe_frontmatter_keeps_types_and_rejects_object_construction():
    import yaml
    metadata = "title: 罕见反例\ncreated: 2026-01-01\ntags: [a, b]\ncount: 4\nvalid: true\n"
    front, body = parse_frontmatter(f'---\n{metadata}---\n正文')
    assert front == yaml.safe_load(metadata) and body == '正文'
    front, body = parse_frontmatter('---\nvalue: !!python/object/apply:builtins.eval ["1+1"]\n---\n正文')
    assert front == {} and body == '正文'


async def test_new_recovery_cycle_revisits_new_files_before_old_cursor(tmp_path):
    from sovereign_hall.services.database import DatabaseService
    store = WikiStore(tmp_path / 'knowledge'); store.ensure_vault()
    source(store, 'a.md')
    db = DatabaseService(str(tmp_path / 'db.sqlite'))
    governor = StorageGovernor(db, store)
    governor.wiki_config = {**governor.wiki_config, 'max_size_bytes': int(directory_size(store.root) / .85)}
    governor._wiki_source_after = 'zzzz.md'
    governor._wiki_archive_after = 'zzzz.md'
    snapshot = await governor.snapshot(refresh=True)
    assert 'knowledge' in snapshot.recovery_targets
    assert governor._wiki_source_after is None and governor._wiki_archive_after is None
    import json
    state = json.loads(governor.archive_state_path.read_text())
    assert state['wiki_source_after'] is None


def test_changed_source_is_retained_and_hot_override_wins(tmp_path, monkeypatch):
    store = WikiStore(tmp_path); store.ensure_vault()
    page = source(store)
    verify = SourcePageArchive.verify_batch
    def changed(self, entries):
        verify(self, entries)
        page.write_text(dump_markdown(parse_frontmatter(page.read_text())[0], '刚刚更新的反例'))
    monkeypatch.setattr(SourcePageArchive, 'verify_batch', changed)
    report = pack(store)
    assert report['retained_changed'] == 1 and report['pages_removed'] == 0
    assert '刚刚更新的反例' in store.read_page('wiki/sources/old.md')
    assert len(store.all_wiki_pages()) == 1


def test_revisions_noop_corruption_and_export(tmp_path):
    archive = SourcePageArchive(tmp_path)
    key = 'wiki/sources/旧来源.md'
    front = {'source_id': 'doc', 'url': 'https://example.org'}
    one = (key, b'first\r\n', front); two = (key, b'second\n', front)
    for entry in (one, two, one, one):
        archive.put_batch([entry], max_bytes=1024**2)
    assert archive.read(key) == one[1]  # Reverting to an older revision publishes that revision.
    with sqlite3.connect(archive.path) as conn:
        assert conn.execute('SELECT count(*) FROM source_versions').fetchone()[0] == 2
        assert conn.execute('SELECT count(*) FROM source_blocks').fetchone()[0] == 2
    import subprocess, sys
    output = tmp_path / 'export.md'
    result = subprocess.run([sys.executable, 'scripts/restore_wiki_source.py', '--root', str(tmp_path),
        '--path', key, '--output', str(output)], capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert output.read_bytes() == one[1]
    with sqlite3.connect(archive.path) as conn:
        conn.execute("UPDATE source_blocks SET sha256='corrupt'")
    with pytest.raises(RuntimeError, match='checksum'):
        archive.read(key)
    for invalid in ('../escape.md', '/absolute.md', 'wiki/entities/e.md'):
        with pytest.raises(ValueError):
            read_source(tmp_path, invalid)


def test_resume_cursor_age_target_and_capacity(tmp_path):
    store = WikiStore(tmp_path); store.ensure_vault()
    for name in ('a.md', 'b.md', 'c.md'):
        source(store, name)
    first = pack(store, limit=1)
    assert first['pages_removed'] == 1 and not first['scan_complete']
    second = pack(store, after_name=first['next_after'])
    assert second['pages_removed'] == 2 and second['scan_complete']
    today = source(store, 'today.md')
    front, body = parse_frontmatter(today.read_text());front['created'] = date.today().isoformat()
    today.write_text(dump_markdown(front, body))
    assert pack(store)['pages_removed'] == 0
    used = directory_size(tmp_path)
    assert pack_sources(tmp_path, used_bytes=used, max_bytes=used+1, target_bytes=0,
                        include_recent=True)['pages_removed'] == 0
    assert today.exists()
    assert pack_sources(tmp_path, used_bytes=used, max_bytes=16*1024**2, target_bytes=used,
                        include_recent=True)['pages_removed'] == 0
    assert pack(store, include_recent=True)['pages_removed'] == 1


async def test_governor_recovers_when_only_source_pages_are_reclaimable(tmp_path):
    from sovereign_hall.services.database import DatabaseService
    store = WikiStore(tmp_path / 'knowledge'); store.ensure_vault()
    for i in range(1000):
        source(store, f'{i:03}.md')
    db = DatabaseService(str(tmp_path / 'db.sqlite'))
    governor = StorageGovernor(db, store)
    before = directory_size(store.root)
    cap = int(before / .92)
    snapshot = StorageSnapshot(StoreWatermark(0, 1024**3, .6, .8, .9),
        StoreWatermark(before, cap, .6, .8, .9), 'now')
    assert not snapshot.research_round_allowed
    for _ in range(20):
        report = await governor.archive_history(snapshot)
        used = directory_size(store.root)
        snapshot = StorageSnapshot(snapshot.database, StoreWatermark(used, cap, .6, .8, .9), 'now')
        if snapshot.research_round_allowed:
            break
    assert report['wiki']['raw_removed'] == 0
    assert report['wiki_sources']['pages_removed'] > 0
    assert report['progressed']
    assert store.used_bytes() == directory_size(store.root) < before
    assert snapshot.research_round_allowed
    assert governor.archive_state_path.exists()
