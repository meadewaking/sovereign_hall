"""Opt-in experiment: two blind source ballots, then a source-aware synthesis.

Three calls, full original pages for every seat, no execution dependencies.
Readers cannot see a prior proposal or one another's output. A final model
forms the proposal only after both source assessments exist. This changes
information flow, not the evidence questions, quote checks, or risk boundary.
"""
from __future__ import annotations

import asyncio
import json

from .source_council import SCHEMA, compile_result, validate_response

VERSION = 'blind_source_council_v1'


def compile_blind_result(packet, synthesis, readers):
    result = compile_result(packet, synthesis, readers)
    result['version'] = VERSION
    result['information_flow'] = 'blind_readers_then_synthesis'
    # The last seat is the proposal. Its own unknowns/disputes must not disappear
    # just because compile_result historically checks only the reviewer seats.
    proposal = result['proposal']
    if proposal is not None:
        unknown = {q for q, a in proposal['answers'].items() if a['status'] == 'unknown'}
        result['unknown_questions'] = sorted(set(result['unknown_questions']) | unknown)
        if result['quorum_met'] and (unknown or proposal['proposal_errors']):
            result.update(action='HOLD', reason='missing_evidence' if unknown else 'proposal_disputed')
    return result


async def run_blind_source_council(client, packet, *, topic, record):
    public = json.dumps({'topic': topic, 'packet': packet}, ensure_ascii=False)

    async def ask(role, readers=None):
        request = dict(system=role + '\n' + SCHEMA,
                       user=public + ('\n两份独立初评=' + json.dumps(readers, ensure_ascii=False)
                                      if readers is not None else ''),
                       temperature=0, max_tokens=6000, json_output=True, use_cache=False)
        await record(role, 'request', request)
        raw = await client.chat(**request)
        parsed, errors = validate_response(raw, packet)
        await record(role, 'response', dict(raw=raw, validated=parsed, errors=errors))
        return parsed

    readers = await asyncio.gather(
        ask('财务初评席：没有预先提案；独立阅读完整原文，重答所有问题并投HOLD/REFER。'),
        ask('风险初评席：没有预先提案；独立阅读完整原文，重答所有问题，审查反证、资本和分红估值缺口，并投HOLD/REFER。'))
    if any(r is None for r in readers):
        return dict(status='invalid_reader', decision=compile_blind_result(packet, None, readers))
    synthesis = await ask(
        '提案兼汇总审议席：根据完整原文审查两份初评，形成逐项提案与最终HOLD/REFER判断。'
        '初评也是待核验的模型解释，不是真实事实。保留有原文依据的反证和缺口；'
        '无法解决的分歧写入proposal_errors。', readers)
    return dict(status='completed' if synthesis is not None else 'invalid_synthesis',
                decision=compile_blind_result(packet, synthesis, readers))
