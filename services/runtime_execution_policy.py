"""Authoritative simulation policy, kept separate from model/historical claims.

No account writer or order interface. Rejecting an invented policy asks for a
new independent decision; it never converts HOLD to LONG or relaxes risk gates.
"""
from __future__ import annotations

import json
import re
from typing import Any


def simulation_policy():
    from ..core.config import get_config
    return dict(get_config().get('simulation', {}))


def unconfigured_account_gate_claims(value: Any) -> list[str]:
    """Find claims of current account bans absent from the execution policy.

    This is a deliberately narrow repair for observed global NAV-ban recycling,
    not a financial truth checker. A security stop, cautious discretionary HOLD,
    investment loss, or an explicit denial of such a ban remains valid.
    """
    if isinstance(value, dict):
        text = '\n'.join(str(x) for k in ('key_evidence', 'risk_flags', 'invalid_if')
                         for x in (value.get(k) if isinstance(value.get(k), list) else [value.get(k, '')]))
    else:
        text = str(value or '')
    claims = []
    for fragment in re.split(r'[。；;\n]', text):
        fragment = fragment.strip()
        if not fragment:
            continue
        explicit_global = re.search(r'(?:全局|全账户|全组合).{0,16}(?:止损线|清仓线|止损阈值|禁开仓|禁止开仓)', fragment)
        nav_ban = re.search(
            r'(?:账户|组合|净值).{0,65}(?:-?10|-?15)\s*(?:%|％|pct).{0,100}'
            r'(?:禁止|禁开|强制|硬约束|硬门|恢复.{0,16}(?:开仓|long|买入))', fragment,
            re.IGNORECASE)
        restore = re.search(r'(?:账户|净值).{0,25}恢复.{0,16}-?(?:10|15)\s*(?:%|％).{0,100}(?:long|买入|开仓|转为)', fragment, re.IGNORECASE)
        if not (explicit_global or nav_ban or restore):
            continue
        # Only explicit policy disclaimers, not a generic negative statement
        # such as "不能开仓", suppress detection.
        if re.search(r'(?:未配置|不存在|没有配置|未设定|并非|不构成|不是|不得当作|不能当作).{0,35}(?:全局|全账户|账户.*禁|硬门|硬约束|禁令)', fragment):
            continue
        if re.search(r'(?:全局.{0,12}(?:止损线|清仓线)|账户.{0,20}禁令).{0,12}(?:不存在|未配置|不是现行)', fragment):
            continue
        claims.append(fragment)
    return list(dict.fromkeys(claims))


def sanitize_account_policy_memory(text: str) -> tuple[str, list[str]]:
    """Produce a feedback view; original database/source text is never edited."""
    pieces = re.split(r'([。；;\n])', str(text or ''))
    removed = []
    for i in range(0, len(pieces), 2):
        if unconfigured_account_gate_claims(pieces[i]):
            removed.append(pieces[i].strip())
            pieces[i] = '[历史模型的未配置账户硬门已隔离；以当前执行配置为准]'
    return ''.join(pieces), removed


def format_runtime_execution_policy(policy: dict | None = None) -> str:
    policy = simulation_policy() if policy is None else policy
    keys = ('execution_mode', 'realtime_quotes_required', 'trade_during_market_hours_only',
            'max_realtime_quote_age_seconds', 'max_daily_trades', 'max_single_position',
            'max_gross', 'min_committee_confidence', 'min_risk_reward', 'min_holding_days',
            'max_holding_days', 'stop_loss_pct', 'take_profit_pct', 'min_unit',
            'trading_fee', 'minimum_commission', 'stamp_duty', 'etf_stamp_duty', 'slippage_rate')
    effective = {k: policy[k] for k in keys if k in policy}
    return ('【当前模拟执行配置；高于历史模型意见】\n' + json.dumps(effective, ensure_ascii=False, sort_keys=True) +
        '\n账户净值收益和低投入/无成交是反馈与链路健康状态，不是开仓许可开关。'
        '当前执行器没有-10%账户禁开仓线或-15%全账户清仓线；stop_loss_pct是逐仓止损，不能改称全局净值门。'
        '历史提案、拒绝记忆、讨论和离线评价参数不能增设执行器硬门。'
        '只能依据本标的事实反证、决定性资料缺口和上述真实风险/费用约束审议；'
        '可因证据或风险合理HOLD，不强制批准或成交。未配置账户禁令的票会被拒绝并重新审议，不能改写成LONG。')
