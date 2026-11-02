"""Compiler/safety regressions; synthetic cases are not investment evidence."""
from copy import deepcopy
import json
import pytest

from sovereign_hall.services.action_research import build_packet, _hash
from sovereign_hall.services.decision_basis import (
    compile_basis, compile_decision, common_document, run_decision_basis, validate_program,
)

WHEN = '2026-10-14T14:10:00+08:00'


def inputs():
    policy = dict(execution_mode='simulation', realtime_quotes_required=True,
        trade_during_market_hours_only=True, max_daily_trades=5, stop_loss_pct=-.08,
        max_single_position=1., max_gross=1., max_realtime_quote_age_seconds=120,
        min_holding_days=10, max_holding_days=30, min_committee_confidence=.55, min_risk_reward=.8)
    state = dict(cutoff=WHEN, cash=7000., total_assets=7000., positions=[], trades_today=0,
        policy=policy, quotes={'000001': dict(ticker='000001', price=10., source='sina_realtime_quote',
                                            fetched_at=WHEN, quoted_at=WHEN)})
    source = dict(id='synthetic', content='000001 界面测试合成报告；非投资证据。\n每股净资产 20.00 19.00\n利润 100 90\n资本 9.32 9.36\n每 10 股股利 2.49 元',
        metadata=dict(crawled_at=WHEN, publish_time='2026-08-15', url='https://example.com/synthetic'))
    definitions = [
        ('book', '每股净资产 20.00 19.00', 0, 20., 'CNY_per_share', 'pricing'),
        ('profit', '利润 100 90', 0, 100., 'CNY', 'driver'),
        ('capital', '资本 9.32 9.36', 0, 9.32, 'percentage_points', 'counterevidence'),
        ('capital_previous', '资本 9.32 9.36', 1, 9.36, 'percentage_points', 'counterevidence'),
        ('dividend_unit', '每 10 股股利 2.49 元', 0, 10, 'shares', 'pricing'),
        ('dividend_cash', '每 10 股股利 2.49 元', 1, 2.49, 'CNY', 'pricing')]
    certificate = dict(bindings=[dict(id=key, label=key, ticker='000001', excerpt=text,
        token_index=ix, value=value, unit=unit, role=role, document_id='synthetic',
        period='2025-12-31' if key == 'capital_previous' else '2026-06-30', column='synthetic reviewed column')
        for key,text,ix,value,unit,role in definitions], unobserved=[
            dict(id='unknown_driver', label='window catalyst not established', ticker='000001', role='driver')],
        calculations=[dict(id='pb', label='cross-period PB', op='divide', args=['price.000001','book'], role='pricing'),
                      dict(id='capital_delta', label='change in percentage points', op='subtract',
                           args=['capital','capital_previous'], role='counterevidence'),
                      dict(id='dividend_ps', label='proposed dividend per share', op='divide',
                           args=['dividend_cash','dividend_unit'], role='pricing')], dividend_per_share_id='dividend_ps')
    certificate['review'] = dict(status='coding_agent_reviewed', reviewed_at=WHEN, bindings_sha256=_hash(certificate))
    packet = build_packet(state, [source], ['000001'], objective='synthetic compiler boundary')
    return packet, certificate


def program(p, action='REFER'):
    return dict(disposition=action, action_id=next(iter(p['actions'])),
        basis_ids=['pb','profit','capital_delta'], comparisons=[], policy_refs=['max_gross'],
        hypothesis='synthetic unverified hypothesis', requests=[],
        scenario=dict(target_price=12., holding_days=30) if action=='REFER' else None, confidence=.6)


def test_numeric_binding_and_period_preserving_arithmetic():
    p,c=inputs(); b=compile_basis(p,c)
    assert b['nodes']['pb']['value'] == .5
    assert b['nodes']['pb']['period'] == [WHEN, '2026-06-30']
    assert b['nodes']['capital_delta']['value'] == -.04
    assert b['nodes']['dividend_ps']['value'] == .249
    assert b['nodes']['capital']['period'] == '2026-06-30'
    assert b['nodes']['unknown_driver']['kind'] == 'unobserved'


def test_dividend_above_cost_does_not_establish_ex_dividend_alpha():
    p,c=inputs(); b=compile_basis(p,c); aid=next(iter(p['actions']))
    cost='econ.'+aid+'.flat_cost'; gross='econ.'+aid+'.gross_dividend'
    assert b['nodes'][gross]['value'] == 24.9 and b['nodes'][cost]['value'] == 11.5
    assert b['nodes']['econ.'+aid+'.dividend_only_net']['value'] < 0
    r=program(p,'HOLD'); r['basis_ids'] += [cost,gross]
    r['comparisons']=[dict(left=cost,op='lt',right=gross,expected=True)]
    assert validate_program(json.dumps(r),p,b)[0] is not None
    r['comparisons'][0]['expected']=False
    assert 'false numeric comparison' in validate_program(json.dumps(r),p,b)[1][0]


@pytest.mark.parametrize('edit', ['missing_excerpt','wrong_number','bool_number','changed_receipt','post_cutoff','wrong_ticker','bad_index','unit_mix','zero_divisor'])
def test_changed_or_invalid_reviewed_binding_rejected(edit):
    p,c=inputs()
    if edit=='missing_excerpt': c['bindings'][0]['excerpt']='not in original'
    if edit=='wrong_number': c['bindings'][0]['value']=21.
    if edit=='bool_number': c['bindings'][0]['value']=True
    if edit=='post_cutoff': c['review']['reviewed_at']='2026-10-15T14:00:00+08:00'
    if edit=='wrong_ticker': c['bindings'][0]['ticker']='600036'
    if edit=='bad_index': c['bindings'][0]['token_index']=False
    if edit=='unit_mix': c['calculations'][0]['args']=['price.000001','capital']
    if edit=='zero_divisor': c['bindings'][0]['value']=0.
    if edit!='changed_receipt':
        bare={k:v for k,v in c.items() if k!='review'}; c['review']['bindings_sha256']=_hash(bare)
    else: c['bindings'][0]['label']='unreviewed relabeling'
    with pytest.raises((ValueError,KeyError)): compile_basis(p,c)


@pytest.mark.parametrize('edit', ['invented_fact','invented_policy','bad_comparison','unit_mix','bool_confidence',
                                  'missing_request','bad_window','negative_net','changed_basis','duplicate_json'])
def test_invalid_decision_program_cannot_become_an_admitted_vote(edit):
    p,c=inputs(); b=compile_basis(p,c); r=program(p)
    if edit=='invented_fact': r['basis_ids'].append('current_PBOC_easing_is_verified')
    if edit=='invented_policy': r['policy_refs']=['global_drawdown_10pct']
    if edit=='bad_comparison':
        r['basis_ids'] += ['capital','capital_previous']
        r['comparisons']=[dict(left='capital',op='gt',right='capital_previous',expected=True)]
    if edit=='unit_mix': r['comparisons']=[dict(left='pb',op='gt',right='profit',expected=True)]
    if edit=='bool_confidence': r['confidence']=True
    if edit=='missing_request': r['disposition']='RESEARCH'
    if edit=='bad_window': r['scenario']['holding_days']=31
    if edit=='negative_net': r['scenario']['target_price']=10.
    if edit=='changed_basis': b['nodes']['pb']['value']=.1
    raw=json.dumps(r)
    if edit=='duplicate_json': raw=raw[:-1]+',"confidence":.9}'
    assert validate_program(raw,p,b)[0] is None


def test_admission_does_not_lock_every_case_to_hold_and_preserves_unverified_prose():
    p,c=inputs(); b=compile_basis(p,c); r=program(p)
    output=compile_decision(p,b,r,[r,r])
    assert output['action']=='REFER' and output['quorum_met']
    assert not output['executable'] and output['return_improvement'] is None
    assert not output['seats'][0]['model_prose_verified']
    assert output['seats'][0]['decision_basis'][0]['label']=='cross-period PB'
    r['disposition']='RESEARCH'; r['scenario']=None
    r['basis_ids'].append('unknown_driver')
    r['requests']=[dict(kind='mechanism_test',requirement='driver',question='test a hypothesis',
                       resolve_if='observation rejects hypothesis',basis_ids=['unknown_driver'])]
    output=compile_decision(p,b,r,[r,r])
    assert output['action']=='RESEARCH' and len(output['research_queue'])==1
    assert output['research_queue'][0]['basis_sha256']==b['basis_sha256']
    assert compile_decision(p,b,r,[r,None])['action']=='HOLD'


def test_common_control_supplement_contains_same_numbers_and_information_status():
    p,c=inputs(); b=compile_basis(p,c); d=common_document(b,WHEN)
    assert 'conditional_scenario' in d['content'] and 'unobserved' in d['content']
    assert '0.249' in d['content'] and '2026-06-30' in d['content']
    assert d['metadata']['crawled_at']==WHEN


@pytest.mark.asyncio
async def test_actual_route_interface_passes_checked_program_to_reviewers():
    p,c=inputs(); calls=[]; events=[]
    class Client:
        async def chat(self,**request):
            calls.append(request); return json.dumps(program(p,'HOLD'))
    async def record(*event): events.append(event)
    output=await run_decision_basis(Client(),p,c,record=record)
    assert output['action']=='HOLD' and len(calls)==3 and len(events)==6
    assert '程序校验后的待审提案' in calls[1]['user'] and 'decision_basis' in calls[1]['user']
    assert all(not r['use_cache'] for r in calls)


def test_daily_limit_still_produces_no_action_and_no_executable_result():
    p,c=inputs(); state=deepcopy(p['state']); state['trades_today']=5
    restricted=build_packet(state,list(p['sources'].values()),['000001'],objective='daily five')
    b=compile_basis(restricted,c)
    r=program(p,'HOLD'); r['action_id']=None
    r['basis_ids']=['book']
    assert compile_decision(restricted,b,r,[r,r])['action']=='HOLD'
    assert not restricted['actions']


def test_missing_quote_remains_unobserved_without_a_numeric_fallback():
    p,c=inputs(); state=deepcopy(p['state']); state['quotes']={}
    restricted=build_packet(state,list(p['sources'].values()),['000001'],objective='missing quote')
    b=compile_basis(restricted,c)
    assert b['nodes']['price.000001']['value'] is None and b['nodes']['pb']['value'] is None
    assert not restricted['actions']
