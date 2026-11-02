import gzip
import json
from pathlib import Path

import pytest

from sovereign_hall.services.compressed_json import compress_existing, compressed_path, read_json, write_json
from sovereign_hall.services.wiki_knowledge import WikiStore, WikiSearchIndex


def test_legacy_cache_compression_preserves_every_original_byte(tmp_path):
    path = tmp_path/'cache.json'
    original = json.dumps({'中文': [1.2345678901234567, -0.0, None]*1000}, ensure_ascii=False).encode()
    path.write_bytes(original)
    report = compress_existing(path)
    assert report['compressed']
    assert not path.exists()
    assert gzip.decompress(compressed_path(path).read_bytes()) == original
    assert read_json(path) == json.loads(original)


def test_capacity_failure_retains_legacy_cache(tmp_path):
    path = tmp_path/'cache.json'
    original = b'{"keep": "original"}'
    path.write_bytes(original)
    with pytest.raises(RuntimeError, match='capacity'):
        compress_existing(path, max_output_bytes=1)
    assert path.read_bytes() == original
    assert not compressed_path(path).exists()


def test_restarting_embedding_and_ingest_readers_uses_compressed_caches(tmp_path):
    store = WikiStore(tmp_path/'knowledge')
    store.ensure_vault()
    ingest = {'entries': {'source': {'source_id': 'id', 'files': ['wiki/sources/source.md']}}}
    store.save_cache(ingest)
    assert store.load_cache() == ingest
    assert not store.cache_path.exists()
    index = WikiSearchIndex(store, embedding_enabled=False)
    vectors = {'hash': [1.2345678901234567, -0.0, 1e-30]}
    write_json(index._embeddings_path, vectors)
    restarted = WikiSearchIndex(store, embedding_enabled=False)
    restarted._load_disk_embeddings()
    assert restarted._page_embeddings == vectors
    restarted._page_embeddings['new'] = [3.14]
    restarted._embeddings_dirty = True
    restarted._flush_disk_embeddings()
    assert read_json(restarted._embeddings_path) == restarted._page_embeddings


def test_vector_cache_enforces_physical_byte_budget_and_preserves_surviving_floats(tmp_path, monkeypatch):
    import random
    from sovereign_hall.core.config import get_config
    monkeypatch.setitem(get_config().get('knowledge_wiki', {}), 'embedding_cache_max_bytes', 8192)
    store = WikiStore(tmp_path/'knowledge'); store.ensure_vault()
    index = WikiSearchIndex(store)
    rng = random.Random(123)
    vectors = {str(k): [rng.random() for _ in range(128)] for k in range(30)}
    index._page_embeddings = vectors.copy(); index._embeddings_dirty = True
    index._flush_disk_embeddings()
    remaining = read_json(index._embeddings_path)
    assert 0 < len(remaining) < len(vectors)
    assert '29' in remaining
    assert all(value == vectors[key] for key,value in remaining.items())
    assert compressed_path(index._embeddings_path).stat().st_blocks*512 <= 8192


def test_zero_cache_budget_discards_only_rebuildable_cache(tmp_path):
    store = WikiStore(tmp_path/'knowledge'); store.ensure_vault()
    source = store.sources_dir/'evidence.md'; source.write_text('完整原始依据')
    store.save_cache({'entries': {'id': {'source_id': 'id'}}})
    store.ingest_cache_max_bytes = 0
    store.save_cache({'entries': {'other': {'source_id': 'other'}}})
    assert store.load_cache() == {'entries': {}}
    assert source.read_text() == '完整原始依据'
