"""Opt-in portfolio hypothesis: apply observed equity abstention to inherited risk.

This is an unproved investment policy, not a safety repair or a calibrated
forecast. The existing lifecycle is the control. The candidate additionally
exits a declared, fixed equity ETF sleeve when none of the three existing
market-state signals is eligible. It can pay fees and miss a recovery.
Neither policy writes orders, invents a model vote, or changes execution gates.
"""
from __future__ import annotations

from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.quote_freshness import is_fresh_quote
from ..ports.broker import assert_safe_execution_mode
from .market_state_policy import UNIVERSE, digest, instant, positive, trend_state
from .portfolio_policy import review_position

VERSION = "portfolio_regime_review_v1"
# This is a policy scope, not an inference that every ETF holds domestic equity.
# 512800 is the inherited bank ETF. Unknown/new instruments are not liquidated
# by guessing their exposure from a numeric prefix.
COVERED_EQUITY_ETFS = (*UNIVERSE, "512800")
QUOTE_SOURCES = {
    "tencent_realtime_quote", "eastmoney_realtime_quote", "sina_realtime_quote",
}


def _quote(state, ticker, now):
    quote = state["quotes"].get(ticker)
    if (not isinstance(quote, dict) or quote.get("ticker") != ticker
            or quote.get("source") not in QUOTE_SOURCES
            or quote.get("valuation_only")
            or not is_fresh_quote(quote, now=now, max_age_seconds=min(
                120, int(state["policy"].get("max_realtime_quote_age_seconds", 120))))):
        return None
    if any(instant(quote[key]) > now for key in ("quoted_at", "fetched_at")):
        return None
    return quote


def build_review(tapes, state):
    """Return two explicit portfolio decisions against one common observation.

    ``tapes`` must come from ``parse_tape`` with the packet's receipts and
    verified calendar. The experiment runner verifies these at its boundary.
    Missing execution prices can never prevent a price-free risk-reduction
    draft; the unchanged simulator must re-quote after intent persistence.
    """
    if set(tapes) != set(UNIVERSE):
        raise ValueError("complete fixed market universe required")
    now = instant(state["cutoff"])
    policy = state["policy"]
    assert_safe_execution_mode(policy["execution_mode"])
    positive(state["cash"], zero=True)
    signals = [trend_state(tapes[ticker]) for ticker in UNIVERSE]
    if ({signal["session"] for signal in signals} != {state["latest_completed_session"]}
            or state["latest_completed_session"] >= now.date().isoformat()):
        raise ValueError("common prior completed session required")
    positions = state["positions"]
    if len({p["ticker"] for p in positions}) != len(positions):
        raise ValueError("duplicate position")
    defensive = not any(signal["eligible"] for signal in signals)
    schedule = CostSchedule.from_mapping(policy)
    arms = {name: {"reviews": [], "intent_drafts": [], "estimated_exit_fees": 0.0,
                   "unpriced_exits": []} for name in ("control", "candidate")}
    for position in sorted(positions, key=lambda p: p["ticker"]):
        ticker = position["ticker"]
        if type(position["shares"]) is not int or position["shares"] <= 0:
            raise ValueError("positive integer holding quantity required")
        positive(position["avg_cost"])
        quote = _quote(state, ticker, now)
        existing = review_position(ticker, position["avg_cost"], position.get("opened_at"),
            quote["price"] if quote else None, quote["quoted_at"] if quote else "",
            quote["source"] if quote else "realtime_quote_unavailable", now=now,
            stop_loss_pct=policy["stop_loss_pct"], take_profit_pct=policy["take_profit_pct"],
            max_holding_days=policy["max_holding_days"]).as_dict()
        for name, arm in arms.items():
            review = dict(existing)
            if (name == "candidate" and defensive and ticker in COVERED_EQUITY_ETFS
                    and review["action"] != "exit"):
                review.update(action="exit", reason="declared_equity_regime_abstention",
                              hypothesis="defensive_exit_unproved")
            arm["reviews"].append(review)
            if review["action"] != "exit":
                continue
            arm["intent_drafts"].append(dict(ticker=ticker, direction="sell",
                target_position=0.0, reason=review["reason"], confidence=None))
            if quote:
                arm["estimated_exit_fees"] += schedule.calculate(
                    ticker, "sell", price=quote["price"], shares=position["shares"]).total
            else:
                arm["unpriced_exits"].append(ticker)
    return dict(version=VERSION, input_state_sha256=digest(state), cutoff=state["cutoff"],
        signals=signals, regime="defensive" if defensive else "ordinary",
        covered_equity_etfs=list(COVERED_EQUITY_ETFS), arms=arms,
        changes_action=arms["control"]["intent_drafts"] != arms["candidate"]["intent_drafts"],
        promotion="isolated_candidate", executable=False, account_writes=0,
        model_calls=0, investment_quality_improvement=None, return_improvement=None,
        limitations=["unproved trend hypothesis", "exit fees and missed recovery",
                     "fixed covered sleeve only", "no new-entry or research-alpha comparison"])
