import json

import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.vector_db import VectorDatabase
from sovereign_hall.services.wiki_knowledge import (
    WikiKnowledgeBase,
    chunk_markdown,
    merge_markdown_page,
)
from sovereign_hall.run_discussion import stage2_deep_research
from sovereign_hall.services.research_discussion import ResearchDiscussionSystem


class FakeEmbeddingLLM:
    async def get_embedding(self, text: str):
        text = text or ""
        if "半导体" in text or "芯片" in text:
            return [1.0, 0.0, 0.0]
        if "消费" in text:
            return [0.0, 1.0, 0.0]
        return [0.0, 0.0, 1.0]


class FakeProposalLLM:
    async def chat(self, **kwargs):
        return json.dumps(
            [
                {
                    "ticker": "512880",
                    "direction": "long",
                    "target_position": 0.1,
                    "stop_loss": 5.0,
                    "take_profit": 15.0,
                    "holding_period": 30,
                    "confidence": 0.7,
                    "thesis": "事实: 半导体景气改善；推断: ETF有弹性；新增性: wiki来源确认",
                    "sector": "半导体",
                    "evidence": ["wiki source"],
                }
            ],
            ensure_ascii=False,
        )


@pytest.fixture
def sample_doc():
    return Document(
        title="600519 半导体产业链调研",
        content="半导体设备国产替代正在加速，芯片产业链订单改善。" * 30,
        url="https://example.com/semis",
        source="unit",
        sector="半导体",
        keywords=["芯片", "国产替代"],
    )


@pytest.mark.asyncio
async def test_wiki_ingest_creates_obsidian_vault_and_links(tmp_path, sample_doc):
    kb = WikiKnowledgeBase(root=tmp_path / "knowledge", embedding_enabled=False)
    await kb.initialize()
    await kb.add_document(sample_doc)

    root = tmp_path / "knowledge"
    assert (root / "raw").is_dir()
    assert (root / "wiki" / "topics").is_dir()
    assert (root / "wiki" / "entities").is_dir()
    assert (root / "wiki" / "sources").is_dir()
    assert (root / ".obsidian" / "graph.json").exists()
    assert (root / "index.md").exists()
    assert (root / "log.md").exists()

    wiki_text = "\n".join(path.read_text(encoding="utf-8") for path in (root / "wiki").rglob("*.md"))
    assert "[[半导体研究]]" in wiki_text
    assert "[[600519]]" in wiki_text
    assert "[[600519 半导体产业链调研]]" in wiki_text


@pytest.mark.asyncio
async def test_wiki_ingest_cache_skips_unchanged_document(tmp_path, sample_doc):
    kb = WikiKnowledgeBase(root=tmp_path / "knowledge", embedding_enabled=False)
    await kb.initialize()

    await kb.add_document(sample_doc)
    first_log = (tmp_path / "knowledge" / "log.md").read_text(encoding="utf-8")
    await kb.add_document(sample_doc)
    second_log = (tmp_path / "knowledge" / "log.md").read_text(encoding="utf-8")

    assert first_log == second_log
    cache = json.loads((tmp_path / "knowledge" / ".state" / "ingest-cache.json").read_text(encoding="utf-8"))
    assert len(cache["entries"]) == 1


def test_page_merge_unions_arrays_and_locks_fields():
    existing = """---
type: topic
title: 半导体研究
created: 2026-01-01
sources:
- old
tags:
- topic
related:
- 600519
---

# 半导体研究

旧内容
"""
    incoming = """---
type: entity
title: 被错误改名
created: 2026-06-01
sources:
- new
tags:
- 芯片
related:
- 512880
---

# 半导体研究

新内容
"""
    merged = merge_markdown_page(existing, incoming)

    assert "type: topic" in merged
    assert "title: 半导体研究" in merged
    assert "created: 2026-01-01" in merged
    assert "- old" in merged
    assert "- new" in merged
    assert "600519" in merged
    assert "512880" in merged
    assert "旧内容" in merged
    assert "新内容" in merged


def test_markdown_chunker_keeps_heading_path_code_and_tables():
    markdown = """---
title: Demo
---

# Root

## 半导体

段落一很长。段落二继续补充半导体材料。

```python
def demo():
    return "不要切开代码块"
```

| 公司 | 指标 |
| --- | --- |
| 600519 | 测试 |
"""
    chunks = chunk_markdown(markdown, target_chars=80, max_chars=120, min_chars=20, overlap_chars=10)

    assert chunks
    assert any("## 半导体" in chunk.heading_path for chunk in chunks)
    assert any("def demo" in chunk.text and "不要切开代码块" in chunk.text for chunk in chunks)
    assert any("| 公司 | 指标 |" in chunk.text and "| 600519 | 测试 |" in chunk.text for chunk in chunks)


@pytest.mark.asyncio
async def test_hybrid_search_returns_wiki_documents_with_vector_scores(tmp_path, sample_doc):
    kb = WikiKnowledgeBase(root=tmp_path / "knowledge", embedding_enabled=True, min_wiki_hits=1)
    await kb.initialize(FakeEmbeddingLLM())
    await kb.add_document(sample_doc)

    results = await kb.search("半导体 芯片", top_k=5, llm_client=FakeEmbeddingLLM())

    assert results
    assert results[0].source == "obsidian_wiki"
    assert results[0].metadata["wiki_path"].startswith("wiki/")
    assert results[0].metadata["vector_score"] is not None


@pytest.mark.asyncio
async def test_lazy_migration_compiles_old_sqlite_documents(tmp_path):
    db = DatabaseService(str(tmp_path / "legacy.db"))
    await db._init_db()
    await db.add_document(
        Document(
            title="旧库半导体资料",
            content="半导体旧资料仍然应该按需迁移到 Obsidian wiki。" * 20,
            url="https://example.com/legacy",
            source="legacy",
            sector="半导体",
            keywords=["芯片"],
        )
    )

    kb = WikiKnowledgeBase(
        root=tmp_path / "knowledge",
        embedding_enabled=False,
        database_service=db,
        min_wiki_hits=1,
        lazy_migration_batch_size=5,
    )
    await kb.initialize()
    results = await kb.search("半导体", top_k=3)

    assert results
    assert list((tmp_path / "knowledge" / "wiki" / "sources").glob("*.md"))
    await db.close()


@pytest.mark.asyncio
async def test_database_skips_wiki_feedback_and_duplicate_urls(tmp_path):
    db = DatabaseService(str(tmp_path / "dedupe.db"))
    await db._init_db()

    first = Document(
        title="云计算产业更新",
        content="云计算产业链需求改善，AI 推理带动算力基础设施扩容。" * 10,
        url="https://example.com/cloud?utm_source=test#frag",
        source="duckduckgo",
        sector="TMT",
        keywords=["云计算"],
    )
    duplicate = Document(
        title="云计算产业更新 重复",
        content="云计算产业链需求改善，AI 推理带动算力基础设施扩容。" * 8,
        url="https://example.com/cloud",
        source="duckduckgo",
        sector="TMT",
        keywords=["云计算"],
    )
    wiki_doc = Document(
        title="Wiki 云计算产业更新",
        content="这是从 wiki 检索出来的派生内容。" * 10,
        source="obsidian_wiki",
        doc_id="wiki:wiki/sources/cloud.md",
    )

    assert await db.add_document(first) is True
    assert await db.add_document(duplicate) is False
    assert await db.add_document(wiki_doc) is False

    rows = await db.search_documents(query="云计算", limit=10)
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/cloud"
    assert await db.count_documents() == 1
    await db.close()


@pytest.mark.asyncio
async def test_vector_database_adapter_uses_wiki_backend(tmp_path, sample_doc):
    vector_db = VectorDatabase(wiki_root=str(tmp_path / "knowledge"), embedding_enabled=False)
    await vector_db.initialize()
    await vector_db.add_document(sample_doc)

    results = await vector_db.search("半导体", top_k=5)
    stats = vector_db.get_stats()

    assert results
    assert stats["provider"] == "obsidian_wiki"
    assert stats["wiki_pages"] >= 3
    assert len(vector_db) == 1


@pytest.mark.asyncio
async def test_stage2_deep_research_consumes_wiki_documents(tmp_path, sample_doc):
    kb = WikiKnowledgeBase(root=tmp_path / "knowledge", embedding_enabled=False)
    await kb.initialize()
    await kb.add_document(sample_doc)
    docs = await kb.search("半导体", top_k=5)

    proposals = await stage2_deep_research(
        FakeProposalLLM(),
        docs,
        "半导体投资机会",
        db_service=type("FakeDb", (), {"get_blacklist": lambda self: []})(),
    )

    assert proposals
    assert proposals[0]["ticker"] == "512880"


@pytest.mark.asyncio
async def test_research_discussion_context_prefers_wiki_results():
    wiki_doc = Document(
        title="Wiki半导体研究",
        content="wiki内容",
        source="obsidian_wiki",
        metadata={"title": "Wiki半导体研究", "wiki_path": "wiki/topics/半导体研究.md", "wiki_type": "topic", "snippet": "wiki摘要"},
    )

    class FakeVector:
        async def search(self, query, top_k=8, llm_client=None):
            return [wiki_doc]

    class FakeDb:
        async def get_proposals(self, limit=10):
            return []

        async def search_documents(self, query=None, limit=15):
            return []

        async def get_blacklist(self):
            return []

    class FakeSelf:
        vector_db = FakeVector()
        llm = None

        async def _get_db(self):
            return FakeDb()

    context = await ResearchDiscussionSystem._build_context_with_db_priority(FakeSelf(), ["半导体"])

    assert "【知识库Wiki（高权重）】" in context
    assert "wiki/topics/半导体研究.md" in context
