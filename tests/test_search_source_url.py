"""Source identity repair: no network, model calls or production database."""
import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import quote

import httpx
import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.evidence_time import build_evidence_context
from sovereign_hall.services.search_source_url import resolve_bing_result_url
from sovereign_hall.services.spider_service import SpiderSwarm
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.run_discussion import merge_documents_prefer_richer, stage1_mass_search


def wrap(url, tracking="one"):
    encoded = base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")
    return f"https://www.bing.com/ck/a?p={tracking}&u=a1{encoded}&ntb=1"


@pytest.mark.parametrize('url', [
    'https://finance.sina.cn/2026-09-01/detail-test.d.html',
    'https://example.com/article?code=000001&view=full#section',
    'https://example.com/中文?name=%E9%93%B6%E8%A1%8C',
])
def test_exact_destination_without_query_or_fragment_rewriting(url):
    assert resolve_bing_result_url(wrap(url)) == url
    assert resolve_bing_result_url(wrap(url, 'two')) == url
    assert resolve_bing_result_url(url) == url


@pytest.mark.parametrize('url', [
    'https://www.bing.com/ck/a?u=a1!!!!',
    'https://www.bing.com/ck/a?u=a1_w',  # invalid UTF8
    'https://www.bing.com/ck/a?u=https%3A%2F%2Fexample.com',
    'https://www.bing.com/ck/a?u=a1aA&u=a1aA',
    wrap('javascript:alert(1)'), wrap('file:///etc/passwd'),
    wrap('https://user:pass@example.com/a'), wrap('https:///missing-host'),
    wrap('https://example.com/a\nb'), wrap('https://example.com/a b'),
    wrap('https://example.com/a').replace('www.bing.com', 'www.bing.com.evil.invalid'),
    wrap('https://example.com/a').replace('/ck/a?', '/search?'),
])
def test_unsupported_or_ambiguous_links_are_not_reinterpreted(url):
    assert resolve_bing_result_url(url) == url


@pytest.mark.asyncio
async def test_identity_flows_through_provider_dedupe_fulltext_sqlite_and_context(tmp_path):
    url = 'https://finance.sina.cn/2026-09-01/detail-test.d.html'
    wrappers = [wrap(url), wrap(url, 'two')]
    snippet = '银行经营现金流与半年报营收数据待核验。' * 5
    html = ''.join(f'<li class="b_algo"><h2><a href="{quote(w, safe=":/?=&")}">银行报告</a></h2>'
                   f'<div class="b_caption"><p>{snippet}</p></div></li>' for w in wrappers)
    spider = SpiderSwarm(timeout=1, retry_times=1)
    await spider.client.aclose()
    spider.client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, text=html)))
    db = DatabaseService(str(tmp_path/'source.db'))
    await db._init_db()
    try:
        docs = await spider._do_search('银行 半年报', 10, ['bing'])
        assert len(docs) == 1
        doc = docs[0]
        assert doc.url == url
        assert doc.metadata['search_result_url'] == wrappers[0]
        assert doc.metadata['source_url_resolution'] == 'bing_embedded_destination_v1'
        assert doc.metadata['publish_time'] == '2026-09-01'
        assert doc.metadata['publish_time_source'] == 'url_path_date'
        coordinator = ResearchRoundCoordinator(db)
        state = await coordinator.start(base_topic='银行', research_objective='来源回链')
        await stage1_mass_search(
            SimpleNamespace(chat=AsyncMock(return_value='["银行 财报"]')),
            SimpleNamespace(aggressive_search=AsyncMock(return_value=docs)),
            '银行', query_count=1, round_coordinator=coordinator, round_id=state.id,
        )
        conn = await db._get_connection()
        async with conn.execute(
            "SELECT payload_json FROM round_events WHERE round_id=? AND event_type='SearchQueriesValidated'",
            (state.id,),
        ) as cursor:
            event = json.loads((await cursor.fetchone())[0])
        assert event['source_url_resolutions'] == [{
            'document_id': doc.id, 'url': url, 'search_result_url': wrappers[0],
            'method': 'bing_embedded_destination_v1',
        }]
        full = Document(id='full', url=url, title=doc.title, content=snippet*3,
                        metadata=doc.metadata)
        merged = merge_documents_prefer_richer(docs, [full])
        assert merged == [full]
        assert await db.add_document(doc)
        assert await db.add_document(full)
        conn = await db._get_connection()
        async with conn.execute('SELECT * FROM documents WHERE url=?', (url,)) as cursor:
            rows = await cursor.fetchall()
        assert len(rows) == 1
        stored = Document.from_dict(dict(rows[0]))
        context, audit = build_evidence_context(
            [stored], max_docs=30, doc_chars=1200, context_chars=24000,
        )
        assert url in context and wrappers[0] not in context
        assert audit['documents'][0]['publication_time_known'] is True
        assert stored.content == full.content
    finally:
        await db.close()
        await spider.close()


@pytest.mark.asyncio
async def test_snippet_date_priority_is_preserved():
    url = 'https://example.com/2026-09-01/article'
    html = f'<li class="b_algo"><h2><a href="{wrap(url)}">银行报告</a></h2><div class="b_caption"><p>2026-08-31 · 实际摘要日期</p></div></li>'
    spider = SpiderSwarm(timeout=1, retry_times=1)
    await spider.client.aclose()
    spider.client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, text=html)))
    try:
        docs = await spider._bing_search('银行 半年报', 1)
        assert docs[0].publish_time == '2026-08-31'
        assert docs[0].metadata['publish_time_source'] == 'search_snippet_date'
    finally:
        await spider.close()
