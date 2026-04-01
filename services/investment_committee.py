"""
🏛️ Sovereign Hall - Investment Committee
投资委员会 - 对抗性辩论系统
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core import (
    AgentRole,
    InvestmentProposal,
    ChallengeQuestion,
    DefenseResponse,
    ICMeetingMinutes,
    VerdictDecision,
)
from ..services.llm_client import LLMClient
from ..agents.agent import Agent
from ..agents import AgentPersona, get_persona
from ..utils import safe_parse_json, generate_id, truncate_text

logger = logging.getLogger(__name__)


class InvestmentCommittee:
    """投资委员会 - Token焚烧炉"""

    def __init__(
        self,
        llm_client: LLMClient,
        max_rounds: int = 3,
        voting_weights: Dict[str, float] = None,
    ):
        """
        初始化投委会

        Args:
            llm_client: LLM客户端
            max_rounds: 最大辩论轮数
            voting_weights: 投票权重
        """
        self.llm = llm_client
        self.max_rounds = max_rounds
        self.voting_weights = voting_weights or {
            'cio': 2.0,
            'risk_officer': 1.5,
            'quant_researcher': 1.0,
            'macro_strategist': 1.0,
            'analyst': 1.0,
        }

        # 创建参会成员
        self.risk_officer = Agent(AgentRole.RISK_OFFICER, llm_client)
        self.quant = Agent(AgentRole.QUANT_RESEARCHER, llm_client)
        self.macro = Agent(AgentRole.MACRO_STRATEGIST, llm_client)
        self.cio = Agent(AgentRole.CIO, llm_client)

        logger.info("Investment Committee initialized")

    async def hold_meeting(
        self,
        proposal: InvestmentProposal,
        proposer: Agent,
        lessons_context: str = "",
    ) -> ICMeetingMinutes:
        """
        召开投委会会议

        Args:
            proposal: 投资提案
            proposer: 提案人
            lessons_context: 历史教训上下文

        Returns:
            会议纪要
        """
        meeting_id = f"mtg_{generate_id('mtg')}"
        start_time = datetime.now()

        print(f"\n{'='*80}")
        print(f"🔔 投委会会议开始 | 提案：{proposal.direction.upper()} {proposal.ticker}")
        print(f"   提案人：{proposer.persona.name}")
        print(f"{'='*80}\n")

        # 初始化会议记录
        challenges: List[ChallengeQuestion] = []
        defenses: List[DefenseResponse] = []

        # 提案陈述（使用提案的thesis）
        proposal_text = truncate_text(proposal.thesis, 3000)
        print(f"📊 提案人陈述 ({len(proposal_text)} chars)")

        # =========================================================================
        # 质询阶段
        # =========================================================================
        print(f"\n⚔️  质询阶段：三方并发质询")

        challenge_tasks = [
            {
                "agent": self.risk_officer,
                "task": self._create_risk_challenge(proposal, lessons_context),
            },
            {
                "agent": self.quant,
                "task": self._create_quant_challenge(proposal),
            },
            {
                "agent": self.macro,
                "task": self._create_macro_challenge(proposal),
            },
        ]

        # 并发执行质询
        challenge_results = await asyncio.gather(*[
            task["agent"].think(
                task["task"],
                context="从你的专业角度进行质询",
                temperature=0.8,
                max_tokens=3000,
            )
            for task in challenge_tasks
        ])

        # 记录质询
        for i, (task, result) in enumerate(zip(challenge_tasks, challenge_results)):
            challenge = ChallengeQuestion.create(
                questioner=task["agent"].role,
                question=result,
                severity=self._assess_severity(result),
                referenced_data=[proposal.ticker],
            )
            challenges.append(challenge)
            print(f"\n【{task['agent'].persona.name}的质询】")
            print(f"   严重程度: {challenge.severity}")
            print(f"   内容摘要: {truncate_text(result, 200)}...")

        # =========================================================================
        # 答辩阶段
        # =========================================================================
        print(f"\n\n🛡️  答辩阶段：提案人回应")

        all_challenges = "\n\n".join([
            f"## {c.questioner.value}的质询（{c.severity}）\n{c.question}"
            for c in challenges
        ])

        defense_task = f"""
提案人{proposer.persona.name}，你的提案受到了以下质询，请逐一反驳：

{all_challenges}

【要求】
1. 针对每个质询，提供具体反证或解释
2. 承认合理的顾虑，但说明风险可控
3. 如果需要，调整你的建议仓位或止损线
4. 重申核心逻辑的稳健性

【输出格式】
## 针对风控的答辩
（回应风控官的问题）

## 针对量化的答辩
（回应量化研究员的问题）

## 针对宏观的答辩
（回应宏观策略师的问题）

## 修正后的建议
- 仓位调整：XX%（如有变化）
- 止损调整：XX元（如有变化）
- 置信度调整：XX（如有变化）
"""

        defense_result = await proposer.think(
            defense_task,
            context="回应质询，为自己的提案辩护",
            temperature=0.5,
            max_tokens=5000,
        )

        defense = DefenseResponse.create(
            challenge_id="all",
            defender=proposer.role,
            response=defense_result,
            revised_confidence=proposal.confidence,
        )
        defenses.append(defense)

        print(f"\n【提案人答辩】")
        print(f"   置信度: {defense.revised_confidence:.0%}")
        print(f"   内容摘要: {truncate_text(defense_result, 300)}...")

        # =========================================================================
        # 裁决阶段
        # =========================================================================
        print(f"\n\n⚖️  裁决阶段：CIO综合评定")

        full_minutes = f"""
【提案摘要】
标的：{proposal.ticker}
方向：{proposal.direction}
建议仓位：{proposal.target_position:.1%}
入场价：{proposal.entry_price}
止损价：{proposal.stop_loss}
止盈价：{proposal.take_profit}
置信度：{proposal.confidence:.0%}

【核心逻辑】
{truncate_text(proposal.thesis, 2000)}

【风控质询】
{truncate_text(challenges[0].question if challenges else "无", 1500)}

【量化质询】
{truncate_text(challenges[1].question if len(challenges) > 1 else "无", 1500)}

【宏观质询】
{truncate_text(challenges[2].question if len(challenges) > 2 else "无", 1500)}

【提案人答辩】
{truncate_text(defense_result, 2000)}

【历史教训】
{lessons_context if lessons_context else "无相关历史记录"}
"""

        verdict_task = f"""
{self._get_cio_persona().get_system_prompt(task_context="做出最终裁决")}

{full_minutes}

【决策框架】
请基于以上完整的会议记录，从以下5个维度进行评估：

1. **逻辑自洽性**：投资逻辑是否严密，论据是否充分？
2. **风险可控性**：风控官提出的风险是否已有效回应？
3. **风险收益比**：预期收益vs潜在风险的吸引力？
4. **组合匹配度**：是否符合当前组合配置需求？
5. **退出机制**：止损线和跟踪机制是否明确？

【重要】
- 考虑投票权重：CIO权重2.0，其他角色权重1.0
- 最终决策必须包含"最终仓位"、"止损线"、"置信度"三个关键参数

【输出JSON格式】
{{
    "decision": "approve/reject/defer",
    "final_position": 0.05,
    "final_stop_loss": 100.0,
    "final_take_profit": 130.0,
    "final_confidence": 0.65,
    "key_concerns": ["风险点1", "风险点2"],
    "monitoring_points": ["需要关注的事项1", "需要关注的事项2"],
    "rationale": "详细的决策理由（300-500字）",
    "voting": {{
        "cio_vote": "approve",
        "risk_officer_vote": "reject",
        "quant_vote": "hold",
        "macro_vote": "approve"
    }}
}}
"""

        verdict_raw = await self.cio.think(
            verdict_task,
            context="做出最终裁决",
            temperature=0.3,
            max_tokens=4000,
        )

        # 解析裁决
        verdict = safe_parse_json(verdict_raw, {})
        verdict = self._normalize_verdict(verdict, proposal)

        print(f"\n【CIO裁决】{verdict.get('decision', 'defer').upper()}")
        print(f"   仓位: {verdict.get('final_position', 0):.1%}")
        print(f"   置信度: {verdict.get('final_confidence', 0):.0%}")
        print(f"   关键顾虑: {', '.join(verdict.get('key_concerns', ['无'])[:3])}")

        # =========================================================================
        # 生成会议纪要
        # =========================================================================
        duration = (datetime.now() - start_time).total_seconds()

        # 获取token统计
        stats = self.llm.get_stats()

        # 简化投票结果
        voting_results = {
            'cio': verdict.get('voting', {}).get('cio_vote', verdict.get('decision', 'defer')),
            'risk_officer': verdict.get('voting', {}).get('risk_officer', 'reject' if challenges else 'abstain'),
            'quant': verdict.get('voting', {}).get('quant_vote', 'abstain'),
            'macro': verdict.get('voting', {}).get('macro_vote', 'abstain'),
        }

        minutes = ICMeetingMinutes(
            meeting_id=meeting_id,
            proposal_id=proposal.proposal_id,
            participants=[
                proposer.role,
                AgentRole.RISK_OFFICER,
                AgentRole.QUANT_RESEARCHER,
                AgentRole.MACRO_STRATEGIST,
                AgentRole.CIO
            ],
            rounds=3,
            total_tokens_consumed=stats.get('total_tokens', 0),
            proposal=proposal,
            challenges=challenges,
            defenses=defenses,
            final_verdict=verdict,
            voting_results=voting_results,
            meeting_duration=duration,
            created_at=datetime.now(),
        )

        print(f"\n{'='*80}")
        print(f"✅ 会议结束 | 耗时 {duration:.1f}秒 | 消耗Token {stats.get('total_tokens', 0):,}")
        print(f"{'='*80}\n")

        return minutes

    def _create_risk_challenge(self, proposal: InvestmentProposal, lessons_context: str) -> str:
        """创建风控质询任务"""
        lesson_header = f'【历史教训】\n{lessons_context}' if lessons_context else ''
        return f"""
你是风控官{self.risk_officer.persona.name}，请从风控角度对以下投资提案进行严格质询：

【提案】
标的：{proposal.ticker}
方向：{proposal.direction}
仓位：{proposal.target_position:.1%}
入场：{proposal.entry_price}
止损：{proposal.stop_loss}
止盈：{proposal.take_profit}
置信度：{proposal.confidence:.0%}

核心逻辑：
{proposal.thesis[:2000]}

{lesson_header if lesson_header else ''}

【质询要求】
1. 寻找3个最致命的风险点（财务、流动性、估值等）
2. 引用具体的反例或历史暴雷案例
3. 质疑乐观假设的合理性
4. 给出具体的反对理由

【输出格式】
## 风控质询报告

### 风险点1：[致命缺陷]
- 具体表现：
- 潜在后果：
- 所需证据：

### 风险点2：[...]
### 风险点3：[...]

### 总体评估：[支持/反对/有条件支持]
"""

    def _create_quant_challenge(self, proposal: InvestmentProposal) -> str:
        """创建量化质询任务"""
        return f"""
你是量化研究员{self.quant.persona.name}，请从量化角度审视以下投资提案：

【提案】
标的：{proposal.ticker}
方向：{proposal.direction}
入场价：{proposal.entry_price}
止损价：{proposal.stop_loss}
止盈价：{proposal.take_profit}
风险收益比：{proposal.risk_reward_ratio:.2f}

核心逻辑摘要：
{proposal.thesis[:1000]}

【质询要求】
1. 从技术面分析当前价格位置和趋势
2. 从资金面分析市场情绪和资金流向
3. 计算并评估风险收益比
4. 指出与技术指标的潜在矛盾

【输出格式】
## 量化分析报告

### 技术面分析
- 当前价格位置：
- 技术指标信号：
- 趋势状态：

### 资金面分析
- 资金流向：
- 情绪指标：

### 风险收益评估
- 建议的风险收益比下限：
- 实际风险收益比评估：

### 总体评估：[强烈推荐/推荐/中立/谨慎/不推荐]
"""

    def _create_macro_challenge(self, proposal: InvestmentProposal) -> str:
        """创建宏观质询任务"""
        return f"""
你是宏观策略师{self.macro.persona.name}，请从宏观角度评估以下投资提案：

【提案】
标的：{proposal.ticker}
方向：{proposal.direction}
持有期：{proposal.holding_period}天
核心逻辑摘要：
{proposal.thesis[:1000]}

【质询要求】
1. 分析宏观环境对该标的的影响路径
2. 识别未来可能影响该标的的政策风险
3. 评估当前的市场流动性环境
4. 考虑汇率和国际市场影响（如适用）

【输出格式】
## 宏观评估报告

### 宏观环境影响
- 流动性环境：
- 政策影响：

### 风险因素
- 政策风险：
- 地缘风险：
- 汇率风险：

### 市场时机评估
- 当前宏观环境是否支持该投资：
- 最佳介入时机判断：

### 总体建议：[强烈看好/看好/中性/谨慎/看淡]
"""

    def _assess_severity(self, question: str) -> str:
        """评估质询严重程度"""
        critical_keywords = ['致命', '暴雷', '造假', '欺诈', '崩溃', '腰斩', '归零']
        high_keywords = ['严重', '风险', '担忧', '质疑', '不确定']

        question_lower = question.lower()

        if any(kw in question_lower for kw in critical_keywords):
            return 'critical'
        elif any(kw in question_lower for kw in high_keywords):
            return 'high'
        elif '?' in question:
            return 'medium'
        else:
            return 'low'

    def _normalize_verdict(self, verdict: Dict, proposal: InvestmentProposal) -> Dict:
        """标准化裁决结果"""
        if not verdict:
            verdict = {}

        # 确保关键字段存在
        defaults = {
            'decision': 'defer',
            'final_position': proposal.target_position,
            'final_stop_loss': proposal.stop_loss,
            'final_take_profit': proposal.take_profit,
            'final_confidence': proposal.confidence,
            'key_concerns': [],
            'monitoring_points': [],
            'rationale': '无法解析决策理由',
            'voting': {
                'cio_vote': 'defer',
                'risk_officer': 'reject',
                'quant': 'abstain',
                'macro': 'abstain',
            }
        }

        for key, default in defaults.items():
            if key not in verdict:
                verdict[key] = default

        # 验证数值范围
        if not 0 <= verdict['final_position'] <= 1:
            verdict['final_position'] = 0.05
        if not 0 <= verdict['final_confidence'] <= 1:
            verdict['final_confidence'] = proposal.confidence

        return verdict

    def _get_cio_persona(self):
        """获取CIO人格"""
        return get_persona(AgentRole.CIO)

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'max_rounds': self.max_rounds,
            'voting_weights': self.voting_weights,
            'risk_officer': self.risk_officer.get_stats(),
            'quant': self.quant.get_stats(),
            'macro': self.macro.get_stats(),
            'cio': self.cio.get_stats(),
        }