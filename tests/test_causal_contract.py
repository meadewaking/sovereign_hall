"""Synthetic boundary regressions; these do not supply investment evidence."""
import copy
import json

import pytest

from sovereign_hall.services.causal_contract import (
    build_causal_packet, compile_judgment, digest, evidence_proof, finalize,
)


@pytest.fixture
def packet():
    from sovereign_hall.services.action_research import build_packet
    cutoff = '2026-10-30T14:00:00+08:00'
    def quote(ticker, price):
        return dict(ticker=ticker, name='测试主体', price=price, source='sina_realtime_quote',
                    quoted_at=cutoff, fetched_at=cutoff)
    state = dict(cutoff=cutoff, cash=7077.61023, total_assets=8676.01023, trades_today=0,
        positions=[dict(ticker='512800', shares=1800)],
        quotes={'512800':quote('512800', .888), '002415':quote('002415', 34.71)},
        policy=dict(execution_mode='simulation', realtime_quotes_required=True,
            trade_during_market_hours_only=True, max_daily_trades=5, stop_loss_pct=-.08,
            take_profit_pct=.15, max_single_position=1, max_gross=1,
            max_realtime_quote_age_seconds=120, min_holding_days=10, max_holding_days=30,
            min_committee_confidence=.55, min_risk_reward=.8))
    rows = [dict(SECURITY_CODE='002415', SECURITY_NAME_ABBR='测试主体', NOTICE_DATE='2026-10-17',
        REPORT_DATE=period, TOTAL_OPERATE_INCOME=rev, PARENT_NETPROFIT=profit)
        for period,rev,profit in [('2026-09-30',740,129),('2026-06-30',468,79),('2025-09-30',658,93)]]
    source = dict(id='income', content=json.dumps(dict(table='income',ticker='002415',name='测试主体',rows=rows)),
        metadata=dict(crawled_at=cutoff,publish_time='2026-10-17',url='https://example.invalid/fixture',source_kind='synthetic_test_only'))
    return build_causal_packet(dict(state=state, universe=['002415'], documents=[source],
        financial_document_ids=['income'], prior_hypothesis={'thesis':'synthetic'}, objective='test'))


def judgment(**changes):
    value = dict(disposition='PASS', claim='test causal claim', evidence_ids=['quarter:parent_profit:2026-09-30'],
        price_expectation='unknown', repricing_mechanism='not established', falsifier='new counterevidence',
        scenario=None, action_id=None, horizon_days=None, tests=[], reason='not investment evidence')
    value.update(changes)
    return value


def observation():
    return dict(gap_id='market_expectations',observation='obtain a dated forward expectation',
        supports_if='disclosure exceeds expectation', rejects_if='already anticipated',
        decision_change='review the former; reject the latter')


def review(**changes):
    value = dict(action='PASS', evidence_ids=['controlled_price'], causal_findings='not established',
                 unsupported_premises=[],tests=[],reason='independent rejection')
    value.update(changes)
    return json.dumps(value)


def referral(packet, **changes):
    value = judgment(disposition='REFER', action_id=next(iter(packet['frontier']['actions'])),
        horizon_days=30, scenario=dict(assumed_forward_eps=2., assumed_pe=20.,
            basis_ids=['quarter:parent_profit:2026-09-30'], assumptions=['hypothetical only']))
    value.update(changes)
    return value


def test_proof_expands_original_operands_and_not_model_rephrased_quotes(packet):
    proof = evidence_proof(['quarter:parent_profit:2026-09-30'], packet)
    assert set(proof) == {'quarter:parent_profit:2026-09-30','parent_profit:2026-09-30','parent_profit:2026-06-30'}
    assert proof['parent_profit:2026-09-30']['original_row']['PARENT_NETPROFIT'] == 129
    assert proof['parent_profit:2026-06-30']['original_row']['REPORT_DATE'] == '2026-06-30'
    assert proof['parent_profit:2026-09-30']['original_url'].startswith('https://')
    assert proof['quarter:parent_profit:2026-09-30']['value'] == 50


def test_forged_registry_operand_does_not_rewrite_evidence(packet):
    packet['registry']['parent_profit:2026-09-30']['value'] = 130
    with pytest.raises(ValueError, match='original operand'):
        evidence_proof(['quarter:parent_profit:2026-09-30'], packet)


@pytest.mark.parametrize('ids', [['invented'], ['controlled_price','controlled_price'], []])
def test_unknown_duplicate_or_absent_references_are_rejected(packet, ids):
    with pytest.raises(ValueError):
        compile_judgment(json.dumps(judgment(evidence_ids=ids)), packet)


def test_research_must_specify_two_opposing_branches(packet):
    value = judgment(disposition='RESEARCH',tests=[observation()])
    compiled = compile_judgment(json.dumps(value),packet)
    result = finalize(compiled,review(action='RESEARCH',tests=[observation()]),packet)
    assert result['action'] == 'RESEARCH'
    assert result['investment_contract'] is None
    value['tests'][0]['rejects_if'] = value['tests'][0]['supports_if']
    with pytest.raises(ValueError,match='opposing'):
        compile_judgment(json.dumps(value),packet)


@pytest.mark.parametrize('change', [dict(horizon_days=31),dict(horizon_days=True),dict(action_id='invented')])
def test_referral_cannot_create_size_or_exit_rule(packet, change):
    with pytest.raises(ValueError):
        compile_judgment(json.dumps(referral(packet,**change)),packet)


def test_costs_can_veto_referral_even_if_model_accepts(packet):
    value = referral(packet)
    value['scenario'].update(assumed_forward_eps=1.,assumed_pe=34.71)
    compiled = compile_judgment(json.dumps(value),packet)
    assert compiled['scenario']['net_gain_scenario'] == pytest.approx(-15.2065)
    assert compiled['admissible_referral'] is False
    with pytest.raises(ValueError,match='rejected premises'):
        finalize(compiled,review(action='ACCEPT'),packet)


def test_research_or_unresolved_premises_cannot_be_promoted(packet):
    compiled = compile_judgment(json.dumps(judgment()),packet)
    with pytest.raises(ValueError,match='promote'):
        finalize(compiled,review(action='ACCEPT'),packet)
    compiled = compile_judgment(json.dumps(referral(packet)),packet)
    with pytest.raises(ValueError,match='promote'):
        finalize(compiled,review(action='ACCEPT',unsupported_premises=['unverified cause']),packet)


def test_reviewed_contract_keeps_size_horizon_sources_and_never_executes(packet):
    compiled = compile_judgment(json.dumps(referral(packet)),packet)
    result = finalize(compiled,review(action='ACCEPT'),packet)
    contract = copy.deepcopy(result['investment_contract'])
    seal = contract.pop('contract_sha256')
    assert seal == digest(contract)
    assert contract['shares'] == 100
    assert contract['holding_period_days'] == 30
    assert contract['strategy_stop_loss_pct'] == -.08
    assert contract['strategy_take_profit_pct'] == .15
    assert contract['approved_budget'] == pytest.approx(3477.7355)
    assert contract['maximum_weight'] == pytest.approx(3471/8676.01023)
    assert contract['evidence']['parent_profit:2026-09-30']['original_row']['PARENT_NETPROFIT'] == 129
    assert not result['executable'] and not contract['executable']
    assert result['return_improvement'] is None
