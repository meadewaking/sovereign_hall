"""Separate registered risk-budget/contract-lifecycle forward hypothesis."""
import math
from datetime import datetime, timedelta
from .paired_allocation import PairedAllocationEvaluation
from .investment_design import size_candidate


class RiskBudgetForwardEvaluation(PairedAllocationEvaluation):
    experiment_prefix = 'risk_contract_forward_'
    allocation_names = ('approved_budget_control_v1', 'cost_risk_contract_v1')
    hypothesis = 'Loss/volatility/cost budgets plus contract horizons improve net return'
    decision_node = 'allocation and lifecycle; common genuine committee decisions'
    protocol_extension = {
        'engine_version': 'risk_contract_forward_v1',
        'scope': 'allocation/lifecycle only; not independent research alpha',
        'loss_budget': .01, 'annual_volatility_budget': .03,
        'max_roundtrip_drag': .015, 'candidate_max_contract_days': 180,
        'execution_phases': 'all durable intents before shared fresh execution quote',
    }

    async def initialize(self):
        await super().initialize()
        # Only private candidate books; protective price exits remain unchanged.
        for name in ('candidate', 'stress'):
            self.books[name].max_holding_days = 180

    def contract_terms(self, name, decision):
        if name == 'control':
            return {}
        days = decision.get('expected_days')
        return {'holding_period_days': days if type(days) is int and 1 <= days <= 180 else 30}

    async def allocation_target(self, name, decision, assets, review):
        if name == 'control':
            return await super().allocation_target(name, decision, assets, review)
        ticker = decision['ticker']
        if ticker in self.books[name].positions:
            return 0  # entry-only loss budget; no accidental repeated risk allocation
        quote = review.quotes.get(ticker)
        if not quote or not assets.get('valuation_complete'):
            return 0
        end = datetime.now().date()-timedelta(days=1)
        bars = await self.market.get_unadjusted_ohlc(ticker, (end-timedelta(days=70)).isoformat(), end.isoformat())
        eligible = sorted((b for b in bars if b.get('adjustment') == 'raw'
                           and b.get('date', '') <= end.isoformat()), key=lambda b:b['date'])
        if len(eligible) < 21 or len({b['date'] for b in eligible}) != len(eligible):
            return 0
        prices = [float(b['close']) for b in eligible[-21:]]
        if any(not math.isfinite(p) or p <= 0 for p in prices):
            return 0
        returns = [math.log(b/a) for a,b in zip(prices,prices[1:])]
        mean = sum(returns)/len(returns)
        vol = math.sqrt(sum((r-mean)**2 for r in returns)/(len(returns)-1)*252)
        if vol <= 0:
            return 0
        result = size_candidate(ticker=ticker, assets=assets['total_assets'], cash=assets['cash'],
            price=quote['price'], annual_volatility=vol, stop_fraction=abs(self.policy['stop_loss_pct']),
            approved_weight=float(decision.get('target_position') or 0),
            current_gross=float(assets.get('invested_ratio') or 0),
            single_cap=self.policy['max_single_position'], gross_cap=self.policy['max_gross'],
            costs=self.books[name].cost_schedule)
        self._write_packet(self.directory/'sizing'/f"{name}_{decision['decision_id']}.json",
                           dict(bars=eligible, volatility=vol, result=result))
        return result.get('target_weight', 0)
