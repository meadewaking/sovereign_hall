"""Offline query transport failures cannot become provider inputs or fills."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SearchQueryGenerator

QUERY = '乳制品 2026年09月 最新公告'


@pytest.mark.asyncio
@pytest.mark.parametrize('content,reasoning,finish,accepted', [
    (json.dumps([QUERY]), 'unused reasoning', 'stop', True),
    (json.dumps([QUERY]), '', 'length', False),
    ('', json.dumps([QUERY]), 'stop', False),
    ('', json.dumps([QUERY]), 'length', False),
    (json.dumps([QUERY]), '', None, False),
])
async def test_query_transport_enforces_complete_final_array(content, reasoning, finish, accepted):
    requests = []
    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={
            'choices': [{'message': {'content': content, 'reasoning_content': reasoning},
                         'finish_reason': finish}],
            'usage': {'prompt_tokens': 10, 'completion_tokens': 20, 'total_tokens': 30},
        })
    client = LLMClient(provider='local')
    client.structured_output_reasoning_effort = 'none'
    # Array transport must work even if object grammar is enabled for votes.
    client.structured_output_use_response_format = True
    client._get_cached = lambda key: 'stale unconstrained query'
    original = client._http_client
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
            client._http_client = transport
            generator = SearchQueryGenerator(client)
            queries = await generator.generate_queries(count=5, topic='乳制品')
        audit = generator.last_validation_report
        assert len(requests) == 1  # No repair of an incomplete transport envelope.
        assert requests[0]['stream'] is False
        assert requests[0]['reasoning_effort'] == 'none'
        assert 'response_format' not in requests[0]
        assert 'stream_options' not in requests[0]
        if accepted:
            assert queries == [QUERY]
            assert audit['generation_mode'] == 'primary'
            assert audit['attempts'][0]['status'] == 'array_valid'
        else:
            assert QUERY not in queries
            assert audit['generation_mode'] == 'fallback'
            assert audit['attempts'][0]['status'] == 'incomplete'
            assert all(q.startswith('乳制品 ') for q in queries)
    finally:
        client._http_client = original
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('raw', [
    f'建议检索 "{QUERY}"',
    json.dumps([QUERY])[:-1],
    json.dumps({'queries': [QUERY]}),
    json.dumps([QUERY, 123]),
])
async def test_query_parser_requires_whole_string_array(raw):
    llm = SimpleNamespace(chat=AsyncMock(side_effect=[raw, '[]']))
    generator = SearchQueryGenerator(llm)
    result = await generator.generate_queries(count=5, topic='乳制品')
    assert QUERY not in result
    assert generator.last_validation_report['generation_mode'] == 'fallback'
    assert generator.last_validation_report['attempts'][0]['status'] == 'invalid_array'
    assert len(llm.chat.call_args_list) == 2
    assert all(c.kwargs['json_array_output'] is True for c in llm.chat.call_args_list)


@pytest.mark.asyncio
async def test_completed_repair_is_audited_and_still_filters_provider_input():
    llm = SimpleNamespace(chat=AsyncMock(side_effect=[
        '建议检索乳制品最新公告。', json.dumps(['query1', QUERY, QUERY]),
    ]))
    generator = SearchQueryGenerator(llm)
    assert await generator.generate_queries(topic='乳制品') == [QUERY]
    audit = generator.last_validation_report
    assert audit['generation_mode'] == 'repaired'
    assert audit['rejection_counts']['duplicate_query'] == 1
    assert audit['accepted_count'] == 1
    assert [a['status'] for a in audit['attempts']] == ['invalid_array', 'array_valid']
    assert all(len(a['response_sha256']) == 64 for a in audit['attempts'])
