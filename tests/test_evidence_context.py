"""Exact model-input lineage and current research time; no network or fills."""
import hashlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from sovereign_hall.core import Document
from sovereign_hall.services.evidence_time import build_evidence_context
from sovereign_hall.services.spider_service import SearchQueryGenerator
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.application.get_system_status import get_system_status
from sovereign_hall.run_discussion import stage1_mass_search, stage2_deep_research


def source(key, size=300):
    return Document(id=key, title=f'经营现金流报告{key}',
                    content=(f'{key}营收同比增加现金流改善毛利率订单增长。' * size),
                    url=f'https://offline.invalid/{key}',
                    metadata={'publish_time_source': 'unknown'})


@pytest.mark.parametrize('limit', [0, 50, 250, 700, 24000])
def test_budget_never_severs_source_or_overstates_seen_documents(limit):
    docs = [source('one'), source('two'), source('three')]
    context, audit = build_evidence_context(docs, max_docs=2, doc_chars=1200, context_chars=limit)
    assert len(context) <= limit
    assert audit['context_chars_used'] == len(context)
    assert audit['context_sha256'] == hashlib.sha256(context.encode()).hexdigest()
    assert len(audit['documents']) + sum(audit['excluded_counts'].values()) == len(docs)
    for doc in docs:
        rows = [r for r in audit['documents'] if r['document_id'] == doc.id]
        assert (doc.url in context) == bool(rows)
        if rows:
            row = rows[0]
            block = context[row['context_start']:row['context_end']]
            assert block.endswith('来源: '+doc.url)
            assert row['excerpt_chars'] > 50
            assert row['publication_time_known'] is False


def test_oversized_header_does_not_starve_smaller_following_source():
    oversized, small = source('oversized'), source('small')
    oversized.metadata['url'] = oversized.url + 'x' * 2000
    context, audit = build_evidence_context([oversized, small], max_docs=2, doc_chars=300, context_chars=700)
    assert [r['document_id'] for r in audit['documents']] == ['small']
    assert audit['excluded_counts']['context_budget'] == 1
    assert oversized.url not in context


@pytest.mark.asyncio
async def test_retry_keeps_fixed_research_time_and_neutral_query_contract():
    llm = SimpleNamespace(chat=AsyncMock(side_effect=['输出被截断', '["乳制品 最新财务数据"]']))
    generator = SearchQueryGenerator(llm)
    when = datetime(2030, 1, 2, tzinfo=timezone.utc)
    queries = await generator.generate_queries(count=5, topic='乳制品', research_as_of=when)
    assert queries == ['乳制品 最新财务数据']
    for call in llm.chat.call_args_list:
        prompt = call.kwargs['user']
        assert when.isoformat() in prompt and '2030年01月' in prompt
        assert '不能假定未结束季度/年度' in prompt
        assert '中性待核验问题' in prompt
        assert '5-15字' not in prompt
    assert generator.last_validation_report['research_as_of'] == when.isoformat()


@pytest.mark.asyncio
async def test_submitted_query_list_matches_durable_round_input(tmp_path):
    db = DatabaseService(str(tmp_path/'search.db'))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    state = await coordinator.start(base_topic='乳制品', research_objective='日期审计')
    llm = SimpleNamespace(chat=AsyncMock(return_value='["乳制品 最新财务数据"]'))
    spiders = SimpleNamespace(aggressive_search=AsyncMock(return_value=[]), network_enabled=True)
    try:
        await stage1_mass_search(llm, spiders, '乳制品｜资金部署候选证据比较', query_count=5,
                                 round_coordinator=coordinator, round_id=state.id)
        status = await get_system_status(db)
        audit = status['search_query_gate']
        assert audit['submitted_queries'] == spiders.aggressive_search.call_args.args[0]
        assert audit['research_as_of'] in llm.chat.call_args.kwargs['user']
        assert audit['network_enabled'] is True
        assert audit['generator_output_contract'] == 'query_array_final_content_v1'
        assert audit['generator_mode'] == 'primary'
        assert audit['generator_attempts'][0]['status'] == 'array_valid'
        assert status['counts']['fills'] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('budget', [20, 700])
async def test_stage2_prompt_audit_and_read_model_are_identical(tmp_path, monkeypatch, budget):
    from sovereign_hall.core import config
    db = DatabaseService(str(tmp_path/'context.db'))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    state = await coordinator.start(base_topic='乳制品', research_objective='上下文审计')
    monkeypatch.setattr(config, 'get_config', lambda: {'research': {'stage2_context_chars': budget}})
    llm = SimpleNamespace(chat=AsyncMock(return_value='[]'))
    docs = [source('visible'), source('excluded')]
    try:
        for doc in docs:
            assert await db.add_document(doc, round_id=state.id)
        assert await stage2_deep_research(llm, docs, '乳制品', db_service=db, round_id=state.id, require_publication_time=False) == []
        conn = await db._get_connection()
        async with conn.execute("SELECT payload_json FROM round_events WHERE round_id=? AND event_type='EvidenceTimeContextPrepared'", (state.id,)) as cursor:
            payload = json.loads((await cursor.fetchone())[0])
        status = await get_system_status(db)
        view = status['evidence_time_context']
        assert view['context_sha256'] == payload['context_sha256']
        assert view['excerpt_document_count'] == len(payload['documents'])
        assert view['context_chars_used'] <= budget
        assert status['counts']['fills'] == status['counts']['proposals'] == 0
        if budget == 20:
            llm.chat.assert_not_called()
            assert payload['documents'] == []
        else:
            prompt = llm.chat.call_args.kwargs['user']
            context = prompt.split('资料：\n', 1)[1].split('\n\n筛选规则：', 1)[0]
            assert hashlib.sha256(context.encode()).hexdigest() == payload['context_sha256']
            assert docs[0].url in context and docs[1].url not in context
            assert payload['documents'][0]['excerpt_truncated'] is True
    finally:
        await db.close()
