from datetime import datetime, timedelta, timezone
import pytest
from sovereign_hall.services.confidence_calibration import summarize
from sovereign_hall.services.investment_design import fact_package, size_candidate, compare_exit


def test_calibration_deduplicates_overlapping_forecasts_and_excludes_future():
    base = dict(ticker='510300', direction='long', confidence=.8, expected_days=10,
                status='validated', result='wrong', validated_at='2026-01-12T16:00:00+08:00',
                policy_snapshot_id='v1')
    result = summarize([{**base, 'predicted_at': f'2026-01-0{day}T16:00:00+08:00'} for day in [1,2]],
                       cutoff='2026-02-01T16:00:00+08:00')
    assert result['groups'][0]['count'] == 1
    assert result['excluded']['overlapping_event'] == 1
    assert result['groups'][0]['strict_correct_rate'] == 0
    assert result['probability_calibrated'] is False


def test_fact_identity_and_temporal_boundaries():
    identity = dict(ticker='002186', name='全聚德', kind='a_share')
    fact = dict(ticker='002285', name='全聚德', kind='a_share', category='issuer_driver',
                published_at='2026-01-01T10:00:00+08:00', captured_at='2026-01-02T10:00:00+08:00',
                source_url='https://example.com/filing', quote='营业收入', source_text='营业收入增加', max_age_days=30)
    result = fact_package(identity, [fact], cutoff='2026-01-03T10:00:00+08:00')
    assert result['rejected'][0]['rejection'] == 'subject_mismatch'
    assert not result['research_ready']
    future = {**fact, 'ticker':'002186', 'captured_at':'2026-02-01T00:00:00+08:00'}
    assert fact_package(identity,[future],cutoff='2026-01-03T10:00:00+08:00')['rejected'][0]['rejection']=='unavailable_at_cutoff'


def test_sizing_accounts_for_stop_fees_and_does_not_force_lot():
    args = dict(ticker='510300', assets=10000, cash=10000, price=1,
                annual_volatility=.2, stop_fraction=.08, approved_weight=.5,
                current_gross=0, single_cap=.5, gross_cap=1)
    result = size_candidate(**args)
    assert result['shares'] == 1100
    assert result['stop_loss_cash'] <= 100
    assert size_candidate(**{**args, 'price':100})['shares'] == 0
    with pytest.raises(ValueError):
        size_candidate(**{**args, 'annual_volatility':float('nan')})


def test_contract_exit_contrast_preserves_price_stop():
    now = datetime(2026,10,28,10,tzinfo=timezone(timedelta(hours=8)))
    pos = dict(ticker='510300',avg_cost=1,opened_at=(now-timedelta(days=40)).isoformat(),holding_period_days=90)
    quote = dict(ticker='510300',price=1,source='sina_realtime_quote',quoted_at=now.isoformat(),fetched_at=now.isoformat())
    policy = dict(stop_loss_pct=-.08,take_profit_pct=.15,max_holding_days=30)
    result = compare_exit(pos,quote,policy,cutoff=now.isoformat())
    assert result['control']['action']=='exit'
    assert result['candidate']['action']=='hold'
    result = compare_exit(pos,{**quote,'price':.9},policy,cutoff=now.isoformat())
    assert result['candidate']['action']=='exit'
    assert result['return_improvement'] is None
