"""Synthetic interface/safety cases, never evidence of investment quality."""
from copy import deepcopy
import hashlib
import json

import pytest

from sovereign_hall.services.exchange_calendar import load_exchange_calendar
from sovereign_hall.services.market_state_policy import (
    UNIVERSE, build_plan, diagnostic_prefixes, parse_tape, trend_state,
)

CUTOFF = '2026-10-16T14:05:00+08:00'


@pytest.fixture
def context():
    sessions = load_exchange_calendar(2026).sessions
    days = sorted(d for d, opened in sessions.items() if opened and d < '2026-10-16')[-70:]
    rows = [[d, str(100+i), str(100+i), str(101+i), str(99+i), '1000'] for i, d in enumerate(days)]
    payload = dict(code=0, data={'sh510300': dict(qfqday=rows)})
    def receipt(body):
        return dict(ticker='510300', status=200, requested_at='2026-10-16T14:04:00+08:00',
            captured_at='2026-10-16T14:04:01+08:00', sha256=hashlib.sha256(body).hexdigest(),
            url=f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh510300,day,{days[0]},2026-10-15,200,qfq')
    return sessions, payload, receipt


def parse(context, change=None, receipt_change=None):
    sessions, payload, receipt = context
    payload = deepcopy(payload)
    if change:
        change(payload)
    body = json.dumps(payload).encode()
    r = receipt(body)
    if receipt_change:
        r.update(receipt_change)
    return parse_tape(body, r, ticker='510300', cutoff=CUTOFF, sessions=sessions)


def state():
    return dict(cutoff=CUTOFF, latest_completed_session='2026-10-15', cash=10000.0,
        positions=[], trades_today=0,
        quotes={t: dict(ticker=t, price=3.0, source='tencent_realtime_quote',
            fetched_at='2026-10-16T14:04:59+08:00', quoted_at='2026-10-16T14:04:58+08:00') for t in UNIVERSE},
        policy=dict(execution_mode='simulation', max_single_position=1.0, max_gross=1.0,
            stop_loss_pct=-0.08, take_profit_pct=0.15, max_holding_days=30,
            min_holding_days=10, min_unit=100, max_daily_trades=5,
            trading_fee=0.0003, minimum_commission=5, stamp_duty=0.0005,
            etf_stamp_duty=0, slippage_rate=0.0005))


def tapes(context, *, downward=False):
    base = parse(context)
    result = {t: deepcopy(base) for t in UNIVERSE}
    for ticker, tape in result.items():
        tape['ticker'] = ticker
        if downward:
            for i, bar in enumerate(tape['bars']):
                bar['close'] = 300 - i
    return result


def test_explicit_original_qfq_and_causal_prefixes(context):
    tape = parse(context)
    signal = trend_state(tape)
    assert signal['eligible'] and signal['return20'] > 0
    assert tape['point_in_time_historical_returns_verified'] is False
    earlier = trend_state(tape, through=tape['bars'][-2]['date'])
    altered = deepcopy(tape)
    altered['bars'][-1]['close'] *= 10
    assert trend_state(altered, through=tape['bars'][-2]['date']) == earlier


@pytest.mark.parametrize('change', [
    lambda p: p['data']['sh510300'].update(day=p['data']['sh510300'].pop('qfqday')),
    lambda p: p['data'].update(sz510300=p['data'].pop('sh510300')),
    lambda p: p['data']['sh510300']['qfqday'].pop(-1),
    lambda p: p['data']['sh510300']['qfqday'].pop(30),
    lambda p: p['data']['sh510300']['qfqday'].append(p['data']['sh510300']['qfqday'][-1]),
    lambda p: p['data']['sh510300']['qfqday'][-1].__setitem__(0, '2026-10-16'),
    lambda p: p['data']['sh510300']['qfqday'][-1].__setitem__(2, 'nan'),
    lambda p: p['data']['sh510300']['qfqday'][-1].__setitem__(3, '1'),
    lambda p: p['data']['sh510300']['qfqday'][-1].__setitem__(5, '0'),
])
def test_rejects_ambiguous_incomplete_or_invalid_tape(context, change):
    with pytest.raises(ValueError):
        parse(context, change=change)


@pytest.mark.parametrize('change', [
    dict(ticker='510500'), dict(status=503), dict(sha256='changed'),
    dict(captured_at='2026-10-16T14:06:00+08:00'),
    dict(requested_at='2026-10-16T14:06:00+08:00'),
    dict(captured_at='2026-10-16T14:04:01'),
    dict(url='https://example.com/?param=sh510300,day,2026-01-01,2026-10-15,200,qfq'),
])
def test_rejects_receipt_drift_and_post_cutoff_capture(context, change):
    with pytest.raises(ValueError):
        parse(context, receipt_change=change)


def test_lawful_integer_lots_costs_ranking_and_no_input_mutation(context):
    data, account = tapes(context), state()
    before = deepcopy((data, account))
    result = build_plan(data, account)
    buys = [a for a in result['actions'] if a['action'] == 'buy_research_plan']
    assert [b['ticker'] for b in buys] == ['159915', '510300']
    assert all(b['shares'] == 800 and b['target_weight'] <= 0.25 for b in buys)
    assert sum(b['all_in_buy_cash'] for b in buys) + result['cash_after_research_plans'] == pytest.approx(10000)
    assert all(b['costs']['commission'] == 5 and b['costs']['stamp_duty'] == 0 for b in buys)
    assert result['planned_gross'] <= 0.5 and result['executable'] is False
    assert result['confidence'] is result['return_improvement'] is None
    assert result['account_writes'] == 0 and (data, account) == before
    assert all('price' not in key for draft in result['intent_drafts'] for key in draft)
    assert build_plan(data, account) == result


def test_false_trend_holds_without_inventing_opportunity(context):
    result = build_plan(tapes(context, downward=True), state())
    assert all(a['reason'] == 'trend_conditions_false' for a in result['actions'])
    assert result['intent_drafts'] == [] and result['model_quality_test_eligible'] is False
    diag = diagnostic_prefixes(tapes(context, downward=True))
    assert diag['entry_branch_sessions'] == 0 and diag['historical_return'] is None


@pytest.mark.parametrize('kind', ['stale', 'future', 'uncontrolled', 'wrong_ticker'])
def test_quote_source_time_and_identity_gate(context, kind):
    account = state()
    quote = account['quotes']['159915']
    if kind == 'stale':
        quote['quoted_at'] = '2026-10-16T14:00:00+08:00'
    elif kind == 'future':
        quote['fetched_at'] = '2026-10-16T14:05:01+08:00'
    elif kind == 'uncontrolled':
        quote['source'] = 'caller_supplied_price'
    else:
        quote['ticker'] = '510300'
    result = build_plan(tapes(context), account)
    assert next(a for a in result['actions'] if a['ticker'] == '159915')['reason'] == 'fresh_research_quote_missing'


def test_missing_holding_mark_blocks_all_expansion(context):
    account = state()
    account['positions'] = [dict(ticker='159366', shares=100, avg_cost=1, opened_at='2026-10-15')]
    result = build_plan(tapes(context), account)
    assert result['assets'] is None and result['valuation_complete'] is False
    assert result['intent_drafts'] == []
    assert all(a['action'] == 'hold' for a in result['actions'])


def test_minimum_commission_is_included_before_declaring_one_lot_affordable(context):
    account = state()
    # Deliberately tiny synthetic account: after the first buy, remaining cash
    # covers another lot's notional but cannot cover its minimum commission.
    account['cash'] = 10
    for ticker in UNIVERSE:
        account['quotes'][ticker]['price'] = 0.01
    result = build_plan(tapes(context), account)
    buys = [a for a in result['actions'] if a['action'] == 'buy_research_plan']
    assert len(buys) == 1 and buys[0]['shares'] == 200
    assert 1 < result['cash_after_research_plans'] < 5
    assert next(a for a in result['actions'] if a['ticker'] == '510300')['reason'] == 'no_affordable_incremental_lot'


def test_daily_hard_limit_and_live_mode(context):
    account = state()
    account['trades_today'] = 5
    result = build_plan(tapes(context), account)
    assert result['intent_drafts'] == []
    account['policy']['execution_mode'] = 'live'
    with pytest.raises(RuntimeError):
        build_plan(tapes(context), account)


def test_existing_stop_exit_does_not_spend_unfilled_sale_proceeds(context):
    account = state()
    account['cash'] = 100
    account['positions'] = [dict(ticker='159366', shares=100, avg_cost=10, opened_at='2026-10-15')]
    account['quotes']['159366'] = {**account['quotes']['510300'], 'ticker': '159366', 'price': 9.0}
    result = build_plan(tapes(context), account)
    assert result['intent_drafts'][0]['direction'] == 'sell'
    assert result['cash_after_research_plans'] <= 100
    assert not any(a['action'] == 'buy_research_plan' for a in result['actions'])


def test_minimum_holding_protects_young_position_from_discretionary_trend_exit(context):
    account = state()
    account['positions'] = [dict(ticker='510300', shares=100, avg_cost=3.0, opened_at='2026-10-15')]
    result = build_plan(tapes(context, downward=True), account)
    assert not any(d['direction'] == 'sell' for d in result['intent_drafts'])


def test_maximum_holding_can_draft_unpriced_exit_but_never_buy_on_missing_mark(context):
    account = state()
    account['positions'] = [dict(ticker='159366', shares=100, avg_cost=1, opened_at='2026-09-01')]
    result = build_plan(tapes(context), account)
    assert result['assets'] is None
    assert [(d['ticker'], d['direction']) for d in result['intent_drafts']] == [('159366', 'sell')]
