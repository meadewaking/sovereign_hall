"""Query feedback tests use isolated SQLite/LLM fixtures, not investment evidence."""
from datetime import datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiosqlite
import pytest

from sovereign_hall.services.research_query_policy import load_topic_research_gaps, select_research_queries
from sovereign_hall.services.spider_service import SearchQueryGenerator
from sovereign_hall.run_discussion import stage1_mass_search


@pytest.mark.asyncio
async def test_feedback_excludes_future_unfinished_other_topic_and_duplicate_ticker(tmp_path):
    async with aiosqlite.connect(tmp_path/'feedback.db') as c:
        c.row_factory = aiosqlite.Row
        await c.executescript('''CREATE TABLE research_rounds(id TEXT, base_topic TEXT, completed_at TEXT);
        CREATE TABLE simulation_committee_outcomes(id INTEGER, round_id TEXT, ticker TEXT,
        evidence_gaps TEXT, reconsider_if TEXT, created_at TEXT, direction TEXT, outcome_code TEXT);''')
        entries = [
            ('past', '银行', '2026-09-23', '2026-09-23', '512800'),
            ('older', '银行', '2026-09-22', '2026-09-22', '512800'),
            ('future', '银行', '2026-09-25', '2026-09-23', '512801'),
            ('future_outcome', '银行', '2026-09-23', '2026-09-25', '512802'),
            ('unfinished', '银行', None, '2026-09-23', '512803'),
            ('other', '医药', '2026-09-23', '2026-09-23', '512804'),
            ('stale', '银行', '2026-09-01', '2026-09-01', '512805'),
        ]
        for i, (rid, topic, completed, created, ticker) in enumerate(entries):
            await c.execute('INSERT INTO research_rounds VALUES (?,?,?)', (rid, topic, completed))
            await c.execute('INSERT INTO simulation_committee_outcomes VALUES (?,?,?,?,?,?,?,?)',
                            (i, rid, ticker, '["营收和拨备覆盖率缺失"]', '[]', created, 'hold', 'committee_hold'))
        await c.commit()
        before = c.total_changes
        result = await load_topic_research_gaps(SimpleNamespace(_get_connection=AsyncMock(return_value=c)),
                                               '银行', as_of=datetime(2026,9,24,14))
        assert [r['round_id'] for r in result] == ['past']
        assert result[0]['evidence_gaps'] == ['营收和拨备覆盖率缺失']
        assert c.total_changes == before


@pytest.mark.parametrize('mode', ['fallback', 'primary', 'repaired'])
def test_budget_preserves_exploration_and_failure_fallback(mode):
    generated = [f'银行 经营指标{i}' for i in range(8)]
    templates = [f'银行 查询维度{i}' for i in range(8)]
    selected, audit = select_research_queries(generated, templates, count=8, deployment=True,
                                             policy='feedback_gap_v1', generation_mode=mode)
    assert len(selected) == 8
    assert selected == (templates if mode == 'fallback' else generated[:4] + templates[:4])
    assert audit['selected_generated_count'] == (0 if mode == 'fallback' else 4)
    legacy, audit = select_research_queries(generated, templates, count=8, deployment=True)
    assert legacy == templates and audit['discarded_generated_count'] == 8


def test_dedup_refills_budget_and_non_deployment_preserves_legacy_order():
    queries, _ = select_research_queries(['银行 财报', '银行 净息差'], ['银行 财报', '银行 分红', '银行 现金流'],
                                        count=4, deployment=True, policy='feedback_gap_v1')
    assert len(queries) == len(set(queries)) == 4
    assert select_research_queries(['银行 财报'], ['银行 分红'], count=2, deployment=False,
                                   policy='feedback_gap_v1')[0] == ['银行 财报', '银行 分红']


@pytest.mark.asyncio
async def test_stage1_default_and_candidate_reach_spider_with_exact_audit(monkeypatch):
    from sovereign_hall.core import config
    from sovereign_hall.services import research_query_policy as policy
    generated = [f'银行 净息差 2026半年报 指标{i}' for i in range(8)]
    feedback = [{'outcome_id': 7, 'round_id': 'past', 'ticker': '512800', 'evidence_gaps': ['净息差绝对数值缺失']}]
    read = AsyncMock(return_value=feedback)
    monkeypatch.setattr(policy, 'load_topic_research_gaps', read)
    for strategy in ('legacy', 'feedback_gap_v1'):
        monkeypatch.setattr(config, 'get_config', lambda: {'research': {'query_selection_policy': strategy}})
        llm = SimpleNamespace(chat=AsyncMock(return_value=json.dumps(generated)))
        spider = SimpleNamespace(aggressive_search=AsyncMock(return_value=[]), network_enabled=True)
        coordinator = SimpleNamespace(db_service=object(), record_event=AsyncMock())
        assert await stage1_mass_search(llm, spider, '银行｜资金部署候选证据比较', query_count=8,
                                       round_coordinator=coordinator, round_id='current') == []
        audit = coordinator.record_event.call_args.args[2]
        assert audit['submitted_queries'] == spider.aggressive_search.call_args.args[0]
        selection = audit['query_selection']
        if strategy == 'legacy':
            read.assert_not_called()
            assert selection['selected_generated_count'] == 0
            assert '同议题已完成审议' not in llm.chat.call_args.kwargs['user']
        else:
            assert audit['submitted_queries'][:4] == generated[:4]
            assert selection['feedback_outcome_ids'] == [7]
            assert '净息差绝对数值缺失' in llm.chat.call_args.kwargs['user']
            assert '不是已验证事实' in llm.chat.call_args.kwargs['user']


@pytest.mark.asyncio
async def test_retry_preserves_feedback_and_research_time():
    llm = SimpleNamespace(chat=AsyncMock(side_effect=['输出被截断', '["银行 净息差 公告"]']))
    await SearchQueryGenerator(llm).generate_queries(topic='银行', research_as_of=datetime(2026,9,24),
                                                   research_feedback=[{'evidence_gaps':['净息差缺失']}])
    assert len(llm.chat.call_args_list) == 2
    for call in llm.chat.call_args_list:
        assert '净息差缺失' in call.kwargs['user'] and '2026-09-24' in call.kwargs['user']
