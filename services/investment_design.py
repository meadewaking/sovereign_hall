"""Pure opt-in investment design: sourced facts, loss budgets and exit contrast.

No fills, account mutation or implicit strategy promotion. Callers must route
accepted drafts through the existing intent/quote/atomic execution pipeline.
"""
import math
from datetime import datetime
from .confidence_calibration import instant
from .portfolio_policy import review_position
from ..domain.portfolio.costs import CostSchedule

VERSION = 'investment_design_v1'


def fact_package(identity, facts, *, cutoff):
    """Require explicit subject, source excerpt and as-of times for each fact."""
    now = instant(cutoff)
    ticker, kind = identity['ticker'], identity['kind']
    if kind not in {'a_share', 'etf'}:
        raise ValueError('unsupported instrument kind')
    names = {identity['name']}
    for alias in identity.get('aliases', []):
        if (alias.get('source') and instant(alias['valid_from']) <= now
                and (not alias.get('valid_to') or now <= instant(alias['valid_to']))):
            names.add(alias['name'])
    accepted, rejected = [], []
    for fact in facts:
        reason = None
        try:
            published, captured = instant(fact['published_at']), instant(fact['captured_at'])
            if not published <= captured <= now:
                reason = 'unavailable_at_cutoff'
            elif fact['ticker'] != ticker or fact['kind'] != kind or fact['name'] not in names:
                reason = 'subject_mismatch'
            elif not fact.get('source_url') or not fact.get('quote') or fact['quote'] not in fact.get('source_text', ''):
                reason = 'untraceable_excerpt'
            elif not 0 <= (now-published).total_seconds() <= fact['max_age_days']*86400:
                reason = 'stale_fact'
        except (KeyError, TypeError, ValueError):
            reason = 'invalid_metadata'
        (rejected if reason else accepted).append({**fact, 'rejection': reason} if reason else fact)
    required = ({'valuation', 'issuer_driver', 'counterevidence'} if kind == 'a_share'
                else {'index_identity', 'exposure', 'liquidity', 'valuation', 'counterevidence'})
    missing = sorted(required - {f.get('category') for f in accepted})
    return dict(version=VERSION, identity=identity, facts=accepted, rejected=rejected,
                missing=missing, research_ready=not missing, approved=False)


def size_candidate(*, ticker, assets, cash, price, annual_volatility, stop_fraction,
                   approved_weight, current_gross, single_cap, gross_cap,
                   loss_budget=.01, volatility_budget=.03, max_roundtrip_drag=.015,
                   lot_size=100, costs=None):
    """Choose largest affordable lot under loss/vol budgets, including stop fees.

    Model confidence is intentionally absent. Missing volatility produces no
    draft rather than a guessed risk estimate. Budgets are experimental policy.
    """
    values = (assets, cash, price, annual_volatility, stop_fraction, approved_weight,
              current_gross, single_cap, gross_cap, loss_budget, volatility_budget, max_roundtrip_drag)
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
        raise ValueError('finite inputs required')
    if not (assets > 0 and cash >= 0 and price > 0 and annual_volatility > 0
            and 0 < stop_fraction < 1 and 0 < loss_budget <= 1 and volatility_budget > 0
            and 0 <= approved_weight <= 1 and 0 <= current_gross <= 1
            and 0 < single_cap <= 1 and 0 < gross_cap <= 1
            and 0 < max_roundtrip_drag < 1 and type(lot_size) is int and lot_size > 0):
        raise ValueError('invalid risk inputs')
    costs = costs or CostSchedule()
    cap = min(approved_weight, single_cap, max(0, gross_cap-current_gross),
              volatility_budget/annual_volatility)
    shares = int(min(cash, assets*cap)/(price*lot_size))*lot_size
    while shares:
        buy = costs.calculate(ticker, 'buy', price=price, shares=shares)
        stop = costs.calculate(ticker, 'sell', price=price*(1-stop_fraction), shares=shares)
        sell = costs.calculate(ticker, 'sell', price=price, shares=shares)
        risk = shares*price*stop_fraction + buy.total + stop.total
        drag = (buy.total+sell.total)/(price*shares)
        if price*shares+buy.total <= cash and risk <= assets*loss_budget:
            if drag > max_roundtrip_drag:
                return dict(shares=0, reason='cost_exceeds_budget', executable=False)
            return dict(shares=shares, target_weight=shares*price/assets,
                        stop_loss_cash=risk, roundtrip_drag=drag, executable=False)
        shares -= lot_size
    return dict(shares=0, reason='risk_or_lot_budget', executable=False)


def compare_exit(position, quote, policy, *, cutoff):
    """Only the candidate's time exit changes; price protection stays identical."""
    now = instant(cutoff)
    from ..domain.portfolio.quote_freshness import is_fresh_quote
    fresh = (quote and quote.get('ticker') == position['ticker']
             and not quote.get('valuation_only') and is_fresh_quote(quote, now=now, max_age_seconds=120))
    common = dict(ticker=position['ticker'], avg_cost=position['avg_cost'],
        opened_at=position['opened_at'], price=quote['price'] if fresh else None,
        price_at=quote.get('quoted_at') if fresh else None,
        price_source=quote.get('source') if fresh else 'unavailable', now=now,
        stop_loss_pct=policy['stop_loss_pct'], take_profit_pct=policy['take_profit_pct'])
    horizon = position.get('holding_period_days')
    if horizon is not None and (type(horizon) is not int or not 1 <= horizon <= 180):
        raise ValueError('invalid contract horizon')
    return dict(control=review_position(**common, max_holding_days=policy['max_holding_days']).as_dict(),
        candidate=review_position(**common, max_holding_days=horizon or policy['max_holding_days']).as_dict(),
        executable=False, return_improvement=None)
