"""Opt-in market-state HS candidate, with no model, database or order writer.

Trend persistence is an explicit unproved investment hypothesis. This module
does not turn a trend into a claimed expected return or calibrated confidence.
It consumes preserved, current-observed adjusted tape, emits integer-lot
research plans and price-free intent drafts, and explains abstentions. Only
the existing simulation pipeline may turn a future intent into a fill.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import math
from statistics import fmean
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.execution_policy import ExecutionPolicyLimits, cap_target_position
from ..domain.portfolio.quote_freshness import is_fresh_quote
from ..ports.broker import assert_safe_execution_mode
from .portfolio_policy import review_position

VERSION = 'market_state_policy_v1'
UNIVERSE = ('510300', '510500', '159915')
WEIGHT = 0.25
MAX_GROSS = 0.50
ZONE = ZoneInfo('Asia/Shanghai')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def instant(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('explicit source/capture timezone required')
    return result.astimezone(ZONE)


def positive(value, *, zero=False):
    if isinstance(value, bool):
        raise ValueError('boolean is not a financial number')
    result = float(value)
    if not math.isfinite(result) or result < 0 or (not zero and result == 0):
        raise ValueError('nonfinite/nonpositive financial number')
    return result


def completed_sessions(sessions, cutoff):
    current = instant(cutoff)
    # Always use the previous session, even after today's close. This fixed
    # rule avoids ambiguity about finalized provider daily bars.
    available = sorted(day for day, opened in sessions.items()
                       if opened == 1 and day < current.date().isoformat())
    if not available or current.date().isoformat() not in sessions:
        raise ValueError('calendar does not cover cutoff')
    return available


def parse_tape(body: bytes, receipt: dict, *, ticker: str, cutoff: str, sessions: dict):
    """Verify actual response identity/adjustment, not a decorated qfq label.

    This is a current snapshot of qfq history. Capturing it now does not prove
    that the adjusted values were available at any historical decision time.
    """
    if ticker not in UNIVERSE or receipt.get('ticker') != ticker or receipt.get('status') != 200:
        raise ValueError('universe/response identity mismatch')
    current = instant(cutoff)
    captured = instant(receipt['captured_at'])
    if not instant(receipt['requested_at']) <= captured <= current:
        raise ValueError('capture beyond cutoff or before request')
    if (current - captured).total_seconds() > 86400:
        raise ValueError('capture too old for current research')
    if hashlib.sha256(body).hexdigest() != receipt['sha256']:
        raise ValueError('original response bytes changed')
    symbol = ('sh' if ticker.startswith('5') else 'sz') + ticker
    url = urlparse(receipt['url'])
    params = parse_qs(url.query)
    parts = params.get('param', [''])[0].split(',')
    if (url.scheme != 'https' or url.hostname != 'web.ifzq.gtimg.cn'
            or url.path != '/appstock/app/fqkline/get' or set(params) != {'param'}
            or len(parts) != 6 or parts[0] != symbol or parts[1] != 'day' or parts[-1] != 'qfq'):
        raise ValueError('untrusted/mismatched history request')
    start, end = date.fromisoformat(parts[2]), date.fromisoformat(parts[3])
    if not parts[4].isdigit() or not 61 <= int(parts[4]) <= 640:
        raise ValueError('unbounded/short history request')
    expected = completed_sessions(sessions, cutoff)
    if end.isoformat() != expected[-1] or start >= end:
        raise ValueError('request does not end at latest completed session')
    payload = json.loads(body)
    data = payload.get('data')
    if payload.get('code') != 0 or not isinstance(data, dict) or set(data) != {symbol}:
        raise ValueError('provider response instrument mismatch')
    rows = data[symbol].get('qfqday')
    if not isinstance(rows, list) or not 61 <= len(rows) <= int(parts[4]):
        raise ValueError('explicit adjusted tape required; raw fallback refused')
    bars = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            raise ValueError('invalid OHLC row')
        day = date.fromisoformat(row[0]).isoformat()
        if not start.isoformat() <= day <= end.isoformat() or sessions.get(day) != 1:
            raise ValueError('unfinished/noncalendar/out-of-window bar')
        opening, close, high, low = (positive(v) for v in row[1:5])
        volume = positive(row[5])
        if high < max(opening, close, low) or low > min(opening, close, high):
            raise ValueError('impossible OHLC')
        bars.append(dict(date=day, open=opening, close=close, high=high, low=low, volume=volume))
    days = [bar['date'] for bar in bars]
    if days != sorted(set(days)):
        raise ValueError('duplicate/unordered sessions')
    needed = [day for day in expected if day >= days[0]]
    if days != needed:
        raise ValueError('missing latest or interior trading session')
    return dict(ticker=ticker, bars=bars, provider='tencent_explicit_qfqday',
        captured_at=receipt['captured_at'], source_sha256=receipt['sha256'],
        source_url=receipt['url'], adjustment='qfq',
        point_in_time_historical_returns_verified=False)


def trend_state(tape, *, through=None):
    bars = [b for b in tape['bars'] if through is None or b['date'] <= through]
    if len(bars) < 61:
        raise ValueError('insufficient completed sessions')
    closes = [positive(b['close']) for b in bars]
    average = fmean(closes[-60:])
    ret = closes[-1] / closes[-21] - 1.0
    return dict(ticker=tape['ticker'], session=bars[-1]['date'], close=closes[-1],
        sma60=average, return20=ret, above_trend=closes[-1] > average,
        positive_return=ret > 0, eligible=closes[-1] > average and ret > 0,
        hypothesis='trend_persistence_unproved', source_sha256=tape['source_sha256'])


def _fresh(state, ticker, now):
    quote = state['quotes'].get(ticker)
    if (not isinstance(quote, dict) or quote.get('ticker') != ticker
            or quote.get('source') not in {'tencent_realtime_quote', 'eastmoney_realtime_quote', 'sina_realtime_quote'}
            or not is_fresh_quote(quote, now=now,
                max_age_seconds=min(120, state['policy'].get('max_realtime_quote_age_seconds', 120)))):
        return None
    # The shared quote helper tolerates a small clock skew. A frozen input
    # package may never include information captured after its own cutoff.
    for field in ('fetched_at', 'quoted_at'):
        value = datetime.fromisoformat(str(quote[field]).replace('Z', '+00:00'))
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZONE)
        if value > now:
            return None
    return {**quote, 'price': positive(quote['price'])}


def build_plan(tapes, state):
    """Compile current research actions; price-free drafts are not persisted intents.

    No fill, forward return or calibrated confidence is emitted. Fresh quotes
    only establish research affordability; executing later requires persistence
    of a real ExecutionIntent and a new quote through the existing executor.
    """
    now = instant(state['cutoff'])
    if set(tapes) != set(UNIVERSE):
        raise ValueError('whole fixed universe required')
    signals = [trend_state(tapes[t]) for t in UNIVERSE]
    if len({s['session'] for s in signals}) != 1:
        raise ValueError('cross-asset session mismatch')
    policy = state['policy']
    assert_safe_execution_mode(policy['execution_mode'])
    if type(state['trades_today']) is not int or state['trades_today'] < 0:
        raise ValueError('invalid daily trade counter')
    if signals[0]['session'] != state['latest_completed_session']:
        raise ValueError('signal is not from latest completed session')
    schedule = CostSchedule.from_mapping(policy)
    limits = ExecutionPolicyLimits(float(policy['max_single_position']),
        min(MAX_GROSS, float(policy.get('max_gross', 1))))
    cash = positive(state['cash'], zero=True)
    positions = state['positions']
    if len({p['ticker'] for p in positions}) != len(positions):
        raise ValueError('duplicate holding identity')
    held = {}
    for position in positions:
        shares = position['shares']
        if type(shares) is not int or shares <= 0:
            raise ValueError('invalid holding quantity')
        positive(position['avg_cost'])
        held[position['ticker']] = position
    quotes = {t: _fresh(state, t, now) for t in set(held) | set(UNIVERSE)}
    missing = sorted(t for t in held if quotes[t] is None)
    value = None if missing else sum(p['shares'] * quotes[t]['price'] for t, p in held.items())
    assets = None if value is None else cash + value
    gross = None if assets is None or assets <= 0 else value / assets
    selected = sorted((s for s in signals if s['eligible']), key=lambda s: (-s['return20'], s['ticker']))[:2]
    selected_ids = {s['ticker'] for s in selected}
    actions, drafts = [], []
    exits = set()
    # Existing protective exits take priority, including unrelated holdings.
    # Their expected proceeds are never spent before an actual fill.
    for ticker, position in sorted(held.items()):
        quote = quotes[ticker]
        review = review_position(ticker, position['avg_cost'], position.get('opened_at'),
            quote['price'] if quote else None, quote['quoted_at'] if quote else '',
            quote['source'] if quote else '', now=now,
            stop_loss_pct=policy['stop_loss_pct'], take_profit_pct=policy['take_profit_pct'],
            max_holding_days=policy['max_holding_days'])
        held_days = review.holding_days
        trend_exit = (ticker in UNIVERSE and ticker not in selected_ids
            and held_days is not None and held_days >= policy['min_holding_days'])
        if review.action == 'exit' or trend_exit:
            exits.add(ticker)
            reason = review.reason if review.action == 'exit' else 'trend_exit_after_minimum_holding'
            drafts.append(dict(ticker=ticker, direction='sell', target_position=0.0, reason=reason))
            actions.append(dict(ticker=ticker, action='exit_draft', reason=reason,
                shares=position['shares'], quote_available=quote is not None))
    remaining_cash = cash
    planned_gross = gross
    trade_slots = max(0, min(5, int(policy.get('max_daily_trades', 5))) - int(state['trades_today']))
    trade_slots = max(0, trade_slots - len(exits))
    for signal in sorted(signals, key=lambda s: (s['ticker'] not in selected_ids, -s['return20'], s['ticker'])):
        ticker = signal['ticker']
        if ticker in exits:
            continue
        reason = None
        if ticker not in selected_ids:
            reason = 'trend_conditions_false' if not signal['eligible'] else 'outside_top_two'
        elif missing or assets is None or assets <= 0:
            reason = 'holding_valuation_incomplete'
        elif quotes[ticker] is None:
            reason = 'fresh_research_quote_missing'
        elif not trade_slots:
            reason = 'daily_trade_limit'
        if reason:
            actions.append(dict(ticker=ticker, action='hold', reason=reason))
            continue
        price = quotes[ticker]['price']
        current_shares = held.get(ticker, {}).get('shares', 0)
        current_weight = current_shares * price / assets
        weight, caps = cap_target_position(WEIGHT, current_position=current_weight,
            current_gross_exposure=planned_gross, limits=limits)
        # Floor to lots. Never round an unaffordable target up to create action.
        lot = int(policy.get('min_unit', 100))
        if lot != 100:
            raise ValueError('fixed ETF universe requires explicit 100-share lots')
        target = math.floor(weight * assets / (price * lot)) * lot
        shares = max(0, target - current_shares)
        shares = shares // lot * lot
        low, high = 0, shares // lot
        while low < high:
            middle = (low + high + 1) // 2
            costs = schedule.calculate(ticker, 'buy', price=price, shares=middle * lot)
            if costs.notional + costs.total <= remaining_cash:
                low = middle
            else:
                high = middle - 1
        shares = low * lot
        if shares == 0:
            actions.append(dict(ticker=ticker, action='hold', reason='no_affordable_incremental_lot',
                target_weight_cap=weight, caps=list(caps)))
            continue
        # Keep strictly within the cap despite floating-point multiplication.
        realized_weight = (current_shares + shares) * price / assets
        assert realized_weight <= weight + 1e-12
        costs = schedule.calculate(ticker, 'buy', price=price, shares=shares)
        remaining_cash -= costs.notional + costs.total
        planned_gross += shares * price / assets
        trade_slots -= 1
        actions.append(dict(ticker=ticker, action='buy_research_plan', shares=shares,
            target_shares=current_shares + shares, research_price=price,
            all_in_buy_cash=costs.notional + costs.total, costs=costs.as_dict(),
            target_weight=realized_weight, caps=list(caps), reason='declared_trend_policy'))
        drafts.append(dict(ticker=ticker, direction='long', target_position=realized_weight,
            reason='declared_trend_policy_unproved; persist_and_requote_before_execution'))
    result = dict(version=VERSION, cutoff=state['cutoff'], input_state_sha256=digest(state),
        signals=signals, actions=actions, intent_drafts=drafts, assets=assets,
        valuation_complete=not missing, missing_holding_quotes=missing,
        cash_after_research_plans=remaining_cash, planned_gross=planned_gross,
        cost_model_version=schedule.version, executable=False, account_writes=0,
        return_improvement=None, confidence=None, promotion='isolated',
        required_before_any_execution='actual_approval_and_all_existing_risk_gates_then_persist_and_requote',
        model_quality_test_eligible=any(s['eligible'] for s in signals) and any(not s['eligible'] for s in signals))
    result['output_sha256'] = digest(result)
    return result


def diagnostic_prefixes(tapes):
    """Causal branch inspection on now-observed qfq prefixes, never a backtest.

    No historical quotes, account actions, prices, returns or LLM forecasts are
    fabricated. These prefixes have now been seen and cannot become holdout.
    """
    common = sorted(set.intersection(*(set(b['date'] for b in t['bars'][60:]) for t in tapes.values())))
    rows = []
    for day in common:
        signals = [trend_state(tapes[t], through=day) for t in UNIVERSE]
        eligible = [s['ticker'] for s in sorted(signals, key=lambda s: (-s['return20'], s['ticker'])) if s['eligible']][:2]
        rows.append(dict(session=day, eligible=eligible, kind='seen_prefix_branch_diagnostic_only'))
    return dict(rows=rows, entry_branch_sessions=sum(bool(r['eligible']) for r in rows),
        abstention_branch_sessions=sum(not r['eligible'] for r in rows),
        historical_return=None, independent_investment_samples=0)
