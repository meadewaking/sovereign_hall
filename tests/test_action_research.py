"""Synthetic boundary regressions; these do not establish investment quality."""
from copy import deepcopy
import json
import pytest

from sovereign_hall.services.action_research import (
    build_packet, compile_result, economics, run_action_research, validate_choice, verify_packet,
)

WHEN = '2026-10-13T14:16:06+08:00'


def packet():
    policy = dict(execution_mode='simulation', realtime_quotes_required=True,
                  trade_during_market_hours_only=True, max_daily_trades=5,
                  stop_loss_pct=-.08, max_single_position=1., max_gross=1.,
                  max_realtime_quote_age_seconds=120, min_holding_days=10,
                  max_holding_days=30, min_committee_confidence=.55, min_risk_reward=.8)
    state = dict(cutoff=WHEN, cash=7000., total_assets=7000., positions=[], trades_today=0,
                 policy=policy, quotes={'000001': dict(ticker='000001', price=10., source='sina_realtime_quote',
                    fetched_at=WHEN, quoted_at=WHEN)})
    docs = [dict(id='source1', content='这是界面测试合成数据，不是投资证据。账面价值不保证价格上涨。',
                 metadata=dict(crawled_at=WHEN, publish_time='2026-08-15', url='https://example.com/source'))]
    return build_packet(state, docs, ['000001'], objective='synthetic boundary')


def choice(p, disposition='REFER'):
    return dict(disposition=disposition, action_id=next(iter(p['actions'])),
                mechanism='synthetic hypothesis', inputs={k: dict(status='supported', interpretation='synthetic',
                    citations=[dict(document_id='source1', quote='账面价值不保证价格上涨')]) for k in p['requirements']},
                policy_refs=['max_gross'], requests=[], target_price_scenario=12. if disposition=='REFER' else None,
                holding_days=30, confidence=.6, reason='synthetic boundary only')


def test_frontier_costs_and_referral_are_nonexecuting_and_do_not_force_hold():
    p = packet(); verify_packet(p)
    a = p['actions'][next(iter(p['actions']))]
    e = economics(p['state'], a, target_price=10.)
    assert a['shares'] == 100 and e['cash_required'] == 1005.5
    assert e['net_gain_scenario'] == -11.5  # two commissions, two slips, stock sell tax
    c = choice(p)
    r = compile_result(p, c, [c, c])
    assert r['action'] == 'REFER' and r['quorum_met']
    assert not r['executable'] and r['return_improvement'] is None


@pytest.mark.parametrize('field,value', [('cash', -1.), ('cash', True), ('total_assets', 7001.),
                                       ('trades_today', True)])
def test_account_invariants(field, value):
    p = packet(); s = deepcopy(p['state']); s[field] = value
    with pytest.raises(ValueError): build_packet(s, list(p['sources'].values()), ['000001'], objective='test')


@pytest.mark.parametrize('field,value', [('max_daily_trades', 6), ('max_realtime_quote_age_seconds', 121),
    ('execution_mode', 'live'), ('realtime_quotes_required', False), ('trade_during_market_hours_only', False)])
def test_safety_policy_cannot_be_weakened(field, value):
    p = packet(); s = deepcopy(p['state']); s['policy'][field] = value
    with pytest.raises(ValueError): build_packet(s, list(p['sources'].values()), ['000001'], objective='test')


@pytest.mark.parametrize('mode', ['stale', 'missing_source_time', 'wrong_ticker'])
def test_controlled_quotes_fail_closed(mode):
    p=packet(); s=deepcopy(p['state']); q=s['quotes']['000001']
    if mode=='stale': q['quoted_at']='2026-10-13T14:10:00+08:00'
    if mode=='missing_source_time': q.pop('quoted_at')
    if mode=='wrong_ticker': q['ticker']='600036'
    if mode=='wrong_ticker':
        with pytest.raises(ValueError): build_packet(s, list(p['sources'].values()), ['000001'], objective='test')
    else:
        p2=build_packet(s,list(p['sources'].values()),['000001'],objective='test')
        assert not p2['actions'] and p2['exclusions'][0]['reason']=='no_controlled_quote_at_cutoff'


def test_daily_five_and_expensive_lots_exclude_actions_without_quotes_or_orders():
    p=packet(); s=deepcopy(p['state']); s['trades_today']=5
    assert not build_packet(s,list(p['sources'].values()),['000001'],objective='test')['actions']
    s['trades_today']=0; s['quotes']['000001']['price']=100.
    assert not build_packet(s,list(p['sources'].values()),['000001'],objective='test')['actions']


@pytest.mark.parametrize('edit', ['source', 'price', 'shares', 'policy'])
def test_input_or_derived_actions_cannot_change_after_freeze(edit):
    p=packet()
    if edit=='source': p['sources']['source1']['content'] += 'altered'
    if edit=='price': p['state']['quotes']['000001']['price']=1.
    if edit=='shares': p['actions'][next(iter(p['actions']))]['shares']=1
    if edit=='policy': p['policy_refs']['max_gross']=2.
    with pytest.raises(ValueError): verify_packet(p)


@pytest.mark.parametrize('edit', ['invented_gate', 'invented_quote', 'missing_input', 'negative_net', 'bad_window', 'duplicate'])
def test_untraceable_or_unqualified_referral_is_rejected(edit):
    p=packet(); c=choice(p)
    if edit=='invented_gate': c['policy_refs']=['global_drawdown_10pct']
    if edit=='invented_quote': c['inputs']['pricing']['citations'][0]['quote']='原文没有的盈利和涨价事实'
    if edit=='missing_input': c['inputs']['driver']['status']='missing'
    if edit=='negative_net': c['target_price_scenario']=10.
    if edit=='bad_window': c['holding_days']=31
    raw=json.dumps(c,ensure_ascii=False)
    if edit=='duplicate': raw=raw[:-1]+',"confidence":0.9}'
    assert validate_choice(raw,p)[0] is None


def test_research_queue_is_bound_to_decision_and_review_disagreement_is_hold():
    p=packet(); c=choice(p,'RESEARCH')
    c['inputs']['driver']['status']='missing'
    c['requests']=[dict(kind='mechanism_test',requirement='driver',question='what changes this scenario?',resolve_if='observation rejects mechanism')]
    r=compile_result(p,c,[c,c])
    assert r['action']=='RESEARCH' and len(r['research_queue'])==1
    assert r['research_queue'][0]['packet_sha256']==p['packet_sha256']
    veto=choice(p,'HOLD')
    assert compile_result(p,c,[c,veto])['action']=='HOLD'
    c['requests']=[]
    assert validate_choice(json.dumps(c),p)[0] is None


@pytest.mark.asyncio
async def test_real_pipeline_interface_preserves_sources_and_records_every_call():
    p=packet(); calls=[]; events=[]
    class Client:
        async def chat(self,**request):
            calls.append(request)
            return json.dumps(choice(p,'HOLD'))
    async def record(*event): events.append(event)
    r=await run_action_research(Client(),p,record=record)
    assert r['action']=='HOLD' and len(calls)==3 and len(events)==6
    assert all(p['sources']['source1']['content'] in c['user'] for c in calls)
    assert all(not c['use_cache'] for c in calls)
