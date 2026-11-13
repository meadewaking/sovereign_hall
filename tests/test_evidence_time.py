"""Publication provenance regressions; all HTTP and LLM boundaries are offline."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import sys

import httpx
import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.evidence_time import (
    source_time_metadata, document_time_audit, format_document_time,
)
from sovereign_hall.services.spider_service import SpiderSwarm
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.application.get_system_status import get_system_status
from sovereign_hall.run_discussion import stage2_deep_research

NOW = datetime(2026, 9, 7, 6, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize('snippet,expected', [
    ('2022年8月23日· 旧报告', '2022-08-23'),
    ('2026-09-01 • 现金流数据', '2026-09-01'),
    ('Mar 24, 2026 · 国补政策', '2026-03-24'),
    ('September 1, 2026 · 经营数据', '2026-09-01'),
    ('2021年8月23日订单签署，今天披露', None),
    ('4 天之前· 摘要', None),
    ('2027年9月7日· 未来日期', None),
    ('2026年2月30日· 无效日期', None),
    ('未注明日期的报告', None),
])
def test_source_date_never_defaults_to_retrieval(snippet, expected):
    metadata = source_time_metadata(snippet=snippet, now=NOW)
    assert metadata.get('publish_time') == expected
    assert metadata['crawled_at'] == NOW.isoformat()
    assert metadata['publish_time_source'] == ('search_snippet_date' if expected else 'unknown')


def test_page_metadata_excludes_modification_and_article_event_dates():
    html = '<meta property="article:modified_time" content="2026-09-07"><p>2021-01-01 event</p>'
    assert 'publish_time' not in source_time_metadata(html=html, now=NOW)
    html += '<meta property="article:published_time" content="2022-01-02T10:20:00+08:00">'
    metadata = source_time_metadata(html=html, now=NOW)
    assert metadata['publish_time'] == '2022-01-02T10:20:00+08:00'
    assert metadata['publish_time_source'] == 'page_publication_metadata'


@pytest.mark.parametrize('url,expected', [
    ('https://finance.sina.cn/2026-02-05/detail-inhkszrt9275480.d.html', '2026-02-05'),
    ('https://finance.eastmoney.com/a/202609103870412793.html', '2026-09-10'),
    ('https://stock.10jqka.com.cn/20250821/c670541413.shtml', '2025-08-21'),
    ('https://m.nbd.com.cn/articles/2026-04-09/4330931.html', '2026-04-09'),
    ('https://news.china.com/socialgd/10000169/20260310/49309882.html', '2026-03-10'),
    # Article-id digit runs that look like YYYYMMDD but parse to invalid
    # month/day must NOT be promoted to a publish time.
    ('https://www.jiemian.com/article/14721952.html', None),
    ('https://example.com/article/99991331/x', None),
    # Future-dated URL paths are rejected same as future snippet dates.
    ('https://example.com/2030-01-01/x', None),
])
def test_url_path_date_recovers_cms_embedded_publish_date(url, expected):
    # NOW is set after the latest URL date so the future-date guard does not
    # reject legitimate recent publications.
    as_of = datetime(2026, 9, 14, tzinfo=timezone.utc)
    metadata = source_time_metadata(url=url, now=as_of)
    assert metadata.get('publish_time') == expected
    if expected:
        assert metadata['publish_time_source'] == 'url_path_date'
    else:
        assert metadata['publish_time_source'] == 'unknown'


def test_snippet_date_takes_priority_over_url_path_date():
    # Snippet date is the search-engine-rendered display date and wins over
    # the CMS route path; the two can differ by a day for after-midnight
    # publication, where the snippet is the authoritative display date.
    metadata = source_time_metadata(
        snippet='2026-04-10 · 数据',
        url='https://stock.10jqka.com.cn/20260411/c679150116.shtml',
        now=NOW,
    )
    assert metadata['publish_time'] == '2026-04-10'
    assert metadata['publish_time_source'] == 'search_snippet_date'


def test_document_time_audit_recognizes_url_path_date_as_known():
    doc = Document.from_dict({
        'id': 'wiki',
        'url': 'https://finance.sina.cn/2026-02-05/detail-x.d.html',
        'content': 'audit content',
        'metadata': {'publish_time': '2026-02-05', 'publish_time_source': 'url_path_date'},
    })
    audit = document_time_audit(doc)
    assert audit['publish_time'] == '2026-02-05'
    assert audit['publish_time_source'] == 'url_path_date'
    assert audit['publication_time_known'] is True


def test_legacy_publish_date_is_not_promoted_to_verified_source_time():
    doc = Document.from_dict({'id': 'old', 'publish_time': NOW.isoformat(), 'content': 'old'})
    assert document_time_audit(doc)['publish_time'] is None
    assert 'legacy_unverified' in format_document_time(doc)
    assert doc.publish_time == NOW.isoformat()  # historical audit is not rewritten


@pytest.mark.asyncio
@pytest.mark.parametrize('provider,body', [
    ('bing', '<li class="b_algo"><h2><a href="https://offline.invalid/doc">旧报告</a></h2><div class="b_caption"><p>2022年8月23日· '+ '旧业务数据'*20 + '</p></div></li>'),
    ('baidu', '<div class="result"><h3><a href="https://offline.invalid/doc">旧报告</a></h3><div class="c-abstract">2022年8月23日· '+ '旧业务数据'*20 + '</div></div>'),
    ('sogou', '<div class="vrwrap"><h3><a href="https://offline.invalid/doc">旧报告</a></h3><p class="space-txt">2022年8月23日· '+ '旧业务数据'*20 + '</p></div>'),
])
async def test_search_adapters_keep_old_source_date(provider, body):
    swarm = SpiderSwarm(timeout=1, retry_times=1)
    await swarm.client.aclose()
    swarm.client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, text=body)))
    try:
        docs = await getattr(swarm, f'_{provider}_search')('离线行业', 1)
        assert len(docs) == 1
        assert docs[0].publish_time == '2022-08-23'
        assert docs[0].crawled_at
    finally:
        await swarm.close()


@pytest.mark.asyncio
async def test_ddg_deep_fetch_preserves_publication_metadata(monkeypatch):
    monkeypatch.setitem(sys.modules, 'ddgs', SimpleNamespace(DDGS=lambda **kw: SimpleNamespace(
        text=lambda *args, **kwargs: [{'title': '旧报告', 'href': 'https://offline.invalid/doc', 'body': '2022年8月23日· 数据'}])))
    swarm = SpiderSwarm(timeout=1, retry_times=1)
    await swarm.client.aclose()
    swarm.client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, text='<html><title>旧报告</title><meta property="article:published_time" content="2022-08-23"><article><p>'+ '可审计旧经营数据'*80 + '</p></article></html>')))
    try:
        deep = await swarm._ddg_search('离线行业', 1, deep_fetch=True)
        quick = await swarm._ddg_search('离线行业', 1, deep_fetch=False)
        assert deep[0].publish_time == quick[0].publish_time == '2022-08-23'
        assert deep[0].metadata['publish_time_source'] == 'page_publication_metadata'
        assert quick[0].metadata['publish_time_source'] == 'search_snippet_date'
    finally:
        await swarm.close()


@pytest.mark.asyncio
async def test_time_contract_survives_sqlite_round_context_and_read_model(tmp_path):
    db = DatabaseService(str(tmp_path / 'time.db'))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(base_topic='离线行业', research_objective='日期审计')
    metadata = source_time_metadata(snippet='2022年8月23日· 旧报告', now=NOW)
    old = Document(id='old', title='历史经营报告', content='历史经营现金流收入毛利率数据'*30,
                   url='https://offline.invalid/old', metadata=metadata)
    unknown = Document(id='unknown', title='无日期报告', content='未注明日期的行业营收现金流数据'*30,
                       url='https://offline.invalid/unknown', metadata=source_time_metadata(now=NOW))
    legacy = Document(id='legacy', title='历史未核验', content='历史抓取时间未经核验的经营数据'*30,
                      url='https://offline.invalid/legacy', publish_time=NOW)
    try:
        for doc in [old, unknown, legacy]:
            assert await db.add_document(doc, round_id=round_state.id)
        conn = await db._get_connection()
        async with conn.execute('SELECT * FROM documents ORDER BY id') as cursor:
            rows = [dict(row) for row in await cursor.fetchall()]
        stored = {row['id']: row for row in rows}
        assert stored['old']['publish_time'] == '2022-08-23'
        assert stored['old']['crawled_at'] == NOW.isoformat()
        assert stored['unknown']['publish_time'] is None
        assert stored['legacy']['publish_time_source'] is None
        # Richer same-URL updates must retain their actual source time as well.
        old.content += '扩展经营数据'*30
        assert await db.add_document(old, round_id=round_state.id)
        async with conn.execute("SELECT * FROM documents WHERE id='old'") as cursor:
            richer = Document.from_dict(dict(await cursor.fetchone()))
        assert document_time_audit(richer)['publish_time'] == '2022-08-23'
        llm = SimpleNamespace(chat=AsyncMock(return_value='[]'))
        proposals = await stage2_deep_research(llm, [richer, unknown, legacy], '离线行业',
                                             db_service=db, round_id=round_state.id,
                                             require_publication_time=False)
        assert proposals == []
        prompt = llm.chat.call_args.kwargs['user']
        assert '2022-08-23' in prompt and '抓取时间不是发布时间' in prompt
        assert '来源报告日期: N/A' in prompt and '研究时点' in prompt
        state = await get_system_status(db)
        assert state['evidence_time_context']['publication_time_known_count'] == 1
        assert state['evidence_time_context']['publication_time_unknown_count'] == 2
        assert state['counts']['fills'] == 0
        assert state['counts']['proposals'] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_deployment_search_reserves_current_month_slots_without_ticker_seed():
    from sovereign_hall.run_discussion import stage1_mass_search
    from sovereign_hall.services.spider_service import search_query_rejection_code
    llm = SimpleNamespace(chat=AsyncMock(return_value='["乳制品 财报 现金流"]'))
    swarm = SimpleNamespace(aggressive_search=AsyncMock(return_value=[]), network_enabled=True)
    assert await stage1_mass_search(llm, swarm, '乳制品｜资金部署候选证据比较', query_count=5) == []
    queries = swarm.aggressive_search.call_args.args[0]
    assert len(queries) == 5
    for query in queries[:2]:
        assert datetime.now().strftime('%Y年%m月') in query
        assert '乳制品' in query
        assert search_query_rejection_code(query) is None
    assert '乳制品 上市公司 营收 毛利率' in queries  # background research retained


def test_restart_cleanup_cannot_prefix_match_own_replacement_screen(monkeypatch):
    import sovereign_hall.run_discussion as runner
    calls=[]
    ps_output='\n'.join([
        '800 1 SCREEN -dmS prod_time python -m sovereign_hall.run_discussion',
        '801 800 python -m sovereign_hall.run_discussion',
        '900 1 SCREEN -dmS prod_time_final python -m sovereign_hall.run_discussion',
        '901 900 python -m sovereign_hall.run_discussion',
    ])
    def run(command, **kwargs):
        calls.append(command)
        # Simulate screen's prefix lookup after the old wrapper is already gone.
        if command[0]=='screen':
            assert command[2] != 'prod_time', 'would match prod_time_final and kill own runner'
            assert command[2]=='800.prod_time'
        return SimpleNamespace(stdout=ps_output)
    def kill(pid, sig):
        assert pid==801
        if sig==0:raise ProcessLookupError
    monkeypatch.setattr(__import__('subprocess'),'run',run)
    monkeypatch.setattr(runner.os,'getpid',lambda:901)
    monkeypatch.setattr(runner.os,'kill',kill)
    monkeypatch.setattr(runner.time,'sleep',lambda _:None)
    assert runner.kill_existing_run_discussion_instances()==[801]
    assert ['screen','-S','800.prod_time','-X','quit'] in calls


@pytest.mark.parametrize('url,html,expected', [
    ('https://www.egsea.com/news/detail/2273950.html',
     '<div class="detail-wrapper"><div class="detail-info"><span>来源：e公司</span><span>2026-04-19 12:38</span></div><p>2020-01-01事件</p></div>',
     '2026-04-19T12:38:00'),
    ('https://m.jiemian.com/article/9796005.html',
     '<div class="article-detail"><div class="info" data-article-publish-time="1690177499"></div></div>',
     '2023-07-24T05:44:59+00:00'),
    ('https://stock.finance.sina.com.cn/stock/go.php/vReport_Show/kind/search/rptid/832525308853/index.phtml',
     '<input name="pubdate" value="2026-09-07"><div class="creab"><span>日期：2026-05-19</span></div>',
     '2026-05-19'),
])
def test_article_header_publication_fields_are_source_dates(url, html, expected):
    result=source_time_metadata(html=html,url=url,now=NOW)
    assert result['publish_time']==expected
    assert result['publish_time_source']=='page_publication_metadata'
    assert result['crawled_at']==NOW.isoformat()
    assert source_time_metadata(html=html,url='https://unrelated.invalid/article',now=NOW)['publish_time_source']=='unknown'


@pytest.mark.parametrize('html', [
    '<div class="detail-info"><span>2026-04-19 12:38</span></div>',
    '<div class="detail-wrapper"><p>2026-04-19 12:38</p></div>',
    '<div class="detail-wrapper"><div class="detail-info"><span>2027-04-19 12:38</span></div></div>',
    '<div class="detail-wrapper"><div class="detail-info"><span>2026-02-30</span></div></div>',
    '<div class="detail-wrapper"><div class="detail-info"><span>2026-04-19</span><span>2026-04-20</span></div></div>',
])
def test_article_header_fails_closed_for_missing_future_invalid_or_conflicting_dates(html):
    assert source_time_metadata(html=html,url='https://www.egsea.com/news/detail/2273950.html',now=NOW)['publish_time_source']=='unknown'


@pytest.mark.asyncio
async def test_default_fulltext_audit_records_failure_without_changing_stage1(monkeypatch):
    from sovereign_hall.core import config
    from sovereign_hall.run_discussion import stage1_mass_search
    monkeypatch.setattr(config,'get_config',lambda:{'research':{'deployment_deep_fetch_max_docs':2}})
    source=Document(id='input',title='银行600000财报',content='600000营收现金流'*30,url='https://offline.invalid/source')
    extra=Document(id='extra',title='银行半年报',content='经营财报现金流'*30,url='https://offline.invalid/extra')
    swarm=SimpleNamespace(aggressive_search=AsyncMock(side_effect=[[source],[extra]]),parallel_fetch=AsyncMock(return_value=[]),network_enabled=True)
    llm=SimpleNamespace(chat=AsyncMock(return_value='["银行 财报"]'))
    coordinator=SimpleNamespace(record_event=AsyncMock())
    out=await stage1_mass_search(llm,swarm,'银行｜资金部署候选证据比较',query_count=8,round_coordinator=coordinator,round_id='offline')
    assert out==[source,extra]
    assert swarm.parallel_fetch.call_args.args[0]==[extra.url,source.url]
    event=coordinator.record_event.call_args_list[-1].args
    assert event[1]=='ResearchFulltextAcquired'
    assert [row['url'] for row in event[2]['selected']]==[extra.url,source.url]
    assert event[2]['fetched']==[]
    assert event[2]['output_document_count']==2
