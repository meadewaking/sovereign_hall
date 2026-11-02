"""Synthetic interface/safety regressions, never investment quality evidence."""
import copy
from datetime import datetime, timezone
import json
import pytest
from sovereign_hall.services.source_council import (
    QUESTIONS, packet_from_pages, validate_response, compile_result, run_source_council,
)

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def documents():
    return [dict(id='issuer_filings_v1_000001_' + 'a'*16 + '_p1',
                 content='PDF物理页=1/1。\n净利润与利润总额是不同指标，单位为人民币百万元。',
                 metadata=dict(pdf_page=1, pdf_sha256='a'*64, publish_time='2026-08-15',
                               report_period='2026-06-30', crawled_at=NOW.isoformat(),
                               source_kind='issuer_filing', filing_version='issuer_filings_v1',
                               url='https://static.cninfo.com.cn/finalpage/2026-08-15/123.PDF#page=1'))]


def packet():
    return packet_from_pages(documents(), cutoff=NOW)


def response():
    return dict(ticker='000001', vote='HOLD', answers={q: dict(status='unknown', interpretation='缺少可核验数值', citations=[]) for q in QUESTIONS}, proposal_errors=[])


def test_excerpt_validation_does_not_certify_semantic_truth():
    p=packet(); b=response()
    b['answers']['earnings']=dict(status='supported', interpretation='语义未经验证', citations=[dict(page=1, quote='净利润与利润总额是不同指标，单位为人民币百万元。')])
    v,e=validate_response(json.dumps(b),p)
    assert not e
    result=compile_result(p,v,[v,v])
    assert result['quorum_met'] and not result['interpretations_verified']
    assert result['action']=='HOLD' and not result['executable']


@pytest.mark.parametrize('mutation', [
    lambda b: b.update(ticker='600015'),
    lambda b: b.update(vote='BUY'),
    lambda b: b['answers'].pop('capital'),
    lambda b: b['answers']['earnings'].update(status='supported'),
    lambda b: b['answers']['earnings'].update(citations=[dict(page=1, quote='不存在于原文的数值净利润999')]),
    lambda b: b['answers']['earnings'].update(citations=[dict(page=True, quote='净利润与利润总额是不同指标')]),
    lambda b: b.update(proposal_errors=[{}]),
    lambda b: b.update(answers=[]),
    lambda b: b.update(vote={}),
])
def test_malformed_or_untraceable_output_fails_closed(mutation):
    p=packet(); b=response(); mutation(b)
    assert validate_response(json.dumps(b),p)[0] is None
    assert compile_result(p,response(),[b,response()])['reason']=='invalid_model_output'


def test_duplicate_keys_and_markdown_rejected():
    for raw in ['{"vote":"HOLD","vote":"REFER"}', '```json\n{}\n```']:
        assert validate_response(raw,packet())[0] is None


@pytest.mark.parametrize('mutation', [
    lambda d: d[0]['metadata'].update(crawled_at='2027-01-01T00:00:00+00:00'),
    lambda d: d[0]['metadata'].update(crawled_at='2026-10-01T00:00:00'),
    lambda d: d[0]['metadata'].update(pdf_sha256='b'*64),
    lambda d: d[0]['metadata'].update(url='https://evil.test/123.PDF#page=1'),
    lambda d: d[0].update(content='PDF物理页=1/2。未提供完整文件'),
    lambda d: d[0]['metadata'].update(pdf_page=2),
    lambda d: d.append(copy.deepcopy(d[0])),
])
def test_corrupt_source_or_unavailable_time_rejected(mutation):
    d=documents(); mutation(d)
    with pytest.raises(ValueError): packet_from_pages(d,cutoff=NOW)


def test_votes_cannot_erase_missing_inputs_and_quorum():
    b=response(); b['vote']='REFER'; p=packet()
    for a in b['answers'].values():
        a.update(status='supported',citations=[dict(page=1,quote='净利润与利润总额是不同指标')])
    result=compile_result(p,b,[b,b])
    assert result['action']=='HOLD' and result['reason']=='missing_evidence'
    p['unavailable_inputs']=[] # synthetic referral boundary only
    assert compile_result(p,b,[b,b])['action']=='REFER'
    b['proposal_errors']=['指标口径存在分歧']
    assert compile_result(p,b,[b,b])['action']=='HOLD'
    assert not compile_result(p,b,[b])['quorum_met']


@pytest.mark.asyncio
async def test_all_seats_see_full_source_but_not_each_others_votes():
    calls=[]; records=[]
    class Client:
        async def chat(self,**kw):
            calls.append(kw); return json.dumps(response())
    async def record(*args): records.append(args)
    result=await run_source_council(Client(),packet(),topic='合成测试',record=record)
    assert result['status']=='completed' and len(calls)==3 and len(records)==6
    assert calls[1]['user']==calls[2]['user']
    assert all('净利润与利润总额是不同指标' in c['user'] and not c['use_cache'] for c in calls)


@pytest.mark.asyncio
async def test_bad_proposal_does_not_call_reviewers():
    calls=[]
    class Client:
        async def chat(self,**kw): calls.append(kw); return '{}'
    async def record(*args): pass
    result=await run_source_council(Client(),packet(),topic='合成测试',record=record)
    assert len(calls)==1 and result['status']=='invalid_proposal'


def test_short_table_heading_requires_longer_data_citation():
    p=packet();b=response();a=b['answers']['earnings']
    a.update(status='supported',citations=[dict(page=1,quote='净利润')])
    assert validate_response(json.dumps(b),p)[0] is None
    a['citations'].append(dict(page=1,quote='单位为人民币百万元。'))
    assert validate_response(json.dumps(b),p)[0] is not None


@pytest.mark.asyncio
async def test_resume_uses_exact_prior_proposal_without_resampling():
    calls=[];records=[]
    class Client:
        async def chat(self,**kw): calls.append(kw);return json.dumps(response())
    async def record(*args):records.append(args)
    raw=json.dumps(response());result=await run_source_council(Client(),packet(),topic='synthetic',record=record,proposal_raw=raw)
    assert len(calls)==2 and result['status']=='completed'
    assert records[0][0]=='resumed_proposal' and records[0][2]['raw']==raw
