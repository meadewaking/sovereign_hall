"""Interface/information-flow tests; synthetic evidence is not quality proof."""
import copy
import json

import pytest

from sovereign_hall.services.blind_source_council import (
    compile_blind_result, run_blind_source_council,
)
from test_source_council import packet, response


@pytest.mark.asyncio
async def test_blind_readers_never_receive_prior_answers_and_synthesis_gets_both():
    calls, events = [], []
    class Client:
        async def chat(self, **kwargs):
            calls.append(kwargs)
            answer = response()
            answer['answers']['capital']['interpretation'] = 'PRIVATE_' + str(len(calls))
            return json.dumps(answer)
    async def record(*args):
        events.append(args)
    result = await run_blind_source_council(Client(), packet(), topic='synthetic', record=record)
    assert len(calls) == 3 and len(events) == 6
    assert calls[0]['user'] == calls[1]['user']
    assert all('PRIVATE_' not in c['user'] for c in calls[:2])
    assert 'PRIVATE_1' in calls[2]['user'] and 'PRIVATE_2' in calls[2]['user']
    assert all(packet()['pages']['1'] in json.loads(c['user'].split('\n两份独立初评=')[0])['packet']['pages']['1'] for c in calls)
    assert all(not c['use_cache'] for c in calls)
    assert result['status'] == 'completed'
    assert result['decision']['action'] == 'HOLD'
    assert not result['decision']['executable']


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_call,expected_calls', [(1, 2), (3, 3)])
async def test_invalid_reader_or_synthesis_fails_closed(bad_call, expected_calls):
    calls = []
    class Client:
        async def chat(self, **kwargs):
            calls.append(kwargs)
            return '{}' if len(calls) == bad_call else json.dumps(response())
    async def record(*args):
        pass
    result = await run_blind_source_council(Client(), packet(), topic='synthetic', record=record)
    assert len(calls) == expected_calls
    assert result['status'].startswith('invalid_')
    assert not result['decision']['quorum_met']
    assert result['decision']['action'] == 'HOLD'


@pytest.mark.parametrize('missing,disputed', [(True, False), (False, True), (False, False)])
def test_synthesis_cannot_erase_unknowns_or_disputes(missing, disputed):
    p, reader = packet(), response()
    p['unavailable_inputs'] = []  # synthetic boundary only
    reader['vote'] = 'REFER'
    for a in reader['answers'].values():
        a.update(status='supported', citations=[dict(page=1, quote='净利润与利润总额是不同指标')])
    synthesis = copy.deepcopy(reader)
    if missing:
        synthesis['answers']['capital']['status'] = 'unknown'
    if disputed:
        synthesis['proposal_errors'] = ['unresolved capital interpretation']
    result = compile_blind_result(p, synthesis, [reader, reader])
    assert result['action'] == ('HOLD' if missing or disputed else 'REFER')
    assert not result['interpretations_verified'] and not result['executable']
