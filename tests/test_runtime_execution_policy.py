"""Regression for the observed invented-NAV-ban feedback loop."""
import json
import pytest

from sovereign_hall.services.runtime_execution_policy import (
    format_runtime_execution_policy, sanitize_account_policy_memory, unconfigured_account_gate_claims,
)
from sovereign_hall.services.heuristic_policy import sanitize_candidate_rejection_reason
from sovereign_hall.run_discussion import (
    aggregate_committee_decision, parse_strict_committee_vote,
    build_structured_vote_prompt, build_lessons_with_heuristic_context,
    prioritize_deployment_research,
)


@pytest.mark.parametrize('text', [
    '账户净值-12.89%已穿透-10%全局止损线2.89pct，新建仓被强制禁止',
    '距-15%全账户清仓线仅2.11pct；账户净值低于-10%导致投委会强制禁止开新仓',
    '账户净值恢复至-10%以上且取得新财报方可转为long',
    '当前已穿透-10%全局止损线，必须禁止新建仓',
])
def test_observed_nonexistent_account_gates_are_rejected(text):
    assert unconfigured_account_gate_claims(text)


@pytest.mark.parametrize('text', [
    '持仓跌幅-8%触发逐仓止损，待新鲜报价执行',
    '账户净值-12.89%；标的缺乏最新现金流，保持HOLD',
    '全局止损线不存在，不能当作开仓禁令',
    '当前没有配置全局清仓线，按逐仓止损复核',
    '框架协议弹性从±10%至±15%，订单不保证利润增长',
    '因现金不足买入一手，拒绝执行',
])
def test_real_risks_losses_contract_percentages_and_policy_denials_remain_valid(text):
    assert not unconfigured_account_gate_claims(text)


def vote(text, direction='hold'):
    return json.dumps(dict(direction=direction,confidence=.6,position=.2 if direction=='long' else 0.,
        key_evidence=[text],risk_flags=[],invalid_if='取得最新盈利和现金流后重新审议'),ensure_ascii=False)


def test_invalid_account_policy_never_becomes_a_directional_vote_or_approval():
    wrong=vote('账户净值-12.89%已穿透-10%全局止损线，新建仓被强制禁止')
    assert parse_strict_committee_vote(wrong)['schema_error']=='unconfigured_account_execution_gate'
    outcome=aggregate_committee_decision({},[wrong,wrong,wrong,wrong])
    assert not outcome['vote_quorum_met'] and outcome['direction']=='hold'
    assert outcome['parsed_vote_count']==0
    genuine=vote('最新经营性现金流数据缺失，无法验证盈利质量')
    assert parse_strict_committee_vote(genuine)['schema_valid']
    assert aggregate_committee_decision({},[genuine]*4)['vote_quorum_met']


def test_memory_view_preserves_genuine_evidence_and_raw_audit():
    raw='投委会本轮维持观望；risk_flags=账户净值已穿透-10%全局止损线,缺少最新现金流,股权质押风险待核验'
    clean,removed=sanitize_candidate_rejection_reason(raw)
    assert removed and '全局止损线' not in clean
    assert '缺少最新现金流' in clean and '股权质押' in clean
    assert '全局止损线' in raw
    clean,removed=sanitize_account_policy_memory('最新利润增长仍需核验。账户净值-12.89%已穿透-10%全局止损线。须补充现金流。')
    assert removed and '已穿透' not in clean
    assert '最新利润增长' in clean and '须补充现金流' in clean


def test_production_prompt_has_actual_config_and_quarantines_historical_gate():
    prompt=build_lessons_with_heuristic_context('账户净值已穿透-10%全局止损线，强制禁止新开仓。需要核验当季毛利。')
    assert '已穿透-10%' not in prompt and '需要核验当季毛利' in prompt
    assert '逐仓止损' in prompt and 'max_daily_trades' in prompt
    final=build_structured_vote_prompt('000001','风控','原讨论意见','')
    assert '当前模拟执行配置' in final and 'min_committee_confidence' in final


def test_authority_is_current_simulation_not_offline_return_config():
    block=format_runtime_execution_policy(dict(stop_loss_pct=-.08,max_daily_trades=5,
        min_committee_confidence=.55,execution_mode='simulation'))
    assert '"stop_loss_pct": -0.08' in block and '"max_daily_trades": 5' in block
    assert 'max_drawdown_budget' not in block


def test_ledger_cash_research_survives_missing_marks_without_fabricated_nav():
    topic=prioritize_deployment_research('银行',dict(valuation_complete=False,
        cash=6991.43,total_assets=None,invested_ratio=None),dict(status='blocked_valuation_incomplete'))
    assert '账本现金6991.43元' in topic and '资金部署候选证据比较' in topic
    assert '不推算净值或投入率' in topic and '已投入' not in topic
    assert prioritize_deployment_research('银行',dict(valuation_complete=False,cash=0))=='银行'
    assert prioritize_deployment_research('银行',dict(valuation_complete=False,cash=20))=='银行'
