#!/usr/bin/env python3
"""
Sovereign Hall - 深度辩论系统
解决：多Agent讨论流于形式，缺乏深度交叉验证
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class Stance(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass
class DebateStatement:
    """辩论发言"""
    round_num: int
    agent_id: str
    agent_name: str
    stance: Stance
    content: str
    target_agent: Optional[str] = None
    conviction: float = 0.5


@dataclass
class DebateRound:
    """辩论轮次"""
    round_num: int
    statements: List[DebateStatement] = field(default_factory=list)
    consensus_score: float = 0.0


class DeepDebateSystem:
    """深度辩论系统 - 多轮交叉质询"""
    
    def __init__(self, max_rounds: int = 10, consensus_threshold: float = 0.8):
        self.max_rounds = max_rounds
        self.consensus_threshold = consensus_threshold
        self.rounds: List[DebateRound] = []
        self.agents: Dict[str, Dict] = {}
    
    def register_agent(self, agent_id: str, name: str, initial_stance: Stance):
        """注册Agent"""
        self.agents[agent_id] = {
            'name': name,
            'stance': initial_stance,
            'conviction': 0.7,
            'has_changed': False
        }
    
    def run_debate(self, topic: str, context: str) -> Dict:
        """运行完整辩论"""
        logger.info(f"Starting deep debate on: {topic}")
        
        for round_num in range(1, self.max_rounds + 1):
            debate_round = DebateRound(round_num=round_num)
            
            for agent_id, agent_info in self.agents.items():
                statement = self._generate_statement(
                    agent_id, agent_info, round_num, context
                )
                debate_round.statements.append(statement)
            
            debate_round.consensus_score = self._calculate_consensus(debate_round.statements)
            self.rounds.append(debate_round)
            
            logger.info(f"Round {round_num}: consensus = {debate_round.consensus_score:.2f}")
            
            if debate_round.consensus_score >= self.consensus_threshold:
                logger.info(f"Consensus reached at round {round_num}")
                break
        
        return self._generate_conclusion()
    
    def _generate_statement(self, agent_id: str, agent_info: Dict, 
                           round_num: int, context: str) -> DebateStatement:
        """生成辩论发言"""
        if round_num == 1:
            content = self._opening_statement(agent_id, agent_info['stance'], context)
            return DebateStatement(
                round_num=round_num,
                agent_id=agent_id,
                agent_name=agent_info['name'],
                stance=agent_info['stance'],
                content=content,
                conviction=agent_info['conviction']
            )
        
        target = self._find_opponent(agent_id, round_num)
        if target:
            content = self._rebuttal(agent_id, agent_info['stance'], target)
        else:
            content = self._reinforce(agent_id, agent_info['stance'])
        
        return DebateStatement(
            round_num=round_num,
            agent_id=agent_id,
            agent_name=agent_info['name'],
            stance=agent_info['stance'],
            content=content,
            target_agent=target['agent_id'] if target else None,
            conviction=agent_info['conviction']
        )
    
    def _opening_statement(self, agent_id: str, stance: Stance, context: str) -> str:
        """开场陈述"""
        statements = {
            ('value_investor', Stance.BULLISH): "从价值投资角度，当前估值具备安全边际，长期逻辑未变。",
            ('growth_investor', Stance.NEUTRAL): "增速放缓需要警惕，但长期空间仍在，需等待催化剂。",
            ('contrarian', Stance.BEARISH): "市场过于乐观，忽视了竞争加剧和政策风险。",
            ('risk_manager', Stance.NEUTRAL): "建议严格风控，设置明确止损线，控制仓位。"
        }
        return statements.get((agent_id, stance), f"[{agent_id}] 发表观点")
    
    def _find_opponent(self, agent_id: str, round_num: int) -> Optional[Dict]:
        """寻找反驳对象"""
        my_stance = self.agents[agent_id]['stance']
        
        for other_id, other_info in self.agents.items():
            if other_id == agent_id:
                continue
            if other_info['stance'] != my_stance:
                return {'agent_id': other_id, 'stance': other_info['stance']}
        
        return None
    
    def _rebuttal(self, agent_id: str, stance: Stance, target: Dict) -> str:
        """反驳"""
        return f"质疑{target['agent_id']}：你的观点忽视了{self._get_risk_factor(stance)}"
    
    def _reinforce(self, agent_id: str, stance: Stance) -> str:
        """强化观点"""
        return f"重申立场：{self.agents[agent_id]['name']}坚持{stance.value}判断"
    
    def _get_risk_factor(self, stance: Stance) -> str:
        """获取风险因素"""
        factors = {
            Stance.BULLISH: "政策风险和竞争压力",
            Stance.BEARISH: "估值修复机会和长期价值",
            Stance.NEUTRAL: " timing 和仓位管理"
        }
        return factors.get(stance, "关键变量")
    
    def _calculate_consensus(self, statements: List[DebateStatement]) -> float:
        """计算共识度"""
        if not statements:
            return 0.0
        
        bullish = sum(1 for s in statements if s.stance == Stance.BULLISH)
        bearish = sum(1 for s in statements if s.stance == Stance.BEARISH)
        neutral = sum(1 for s in statements if s.stance == Stance.NEUTRAL)
        
        max_count = max(bullish, bearish, neutral)
        return max_count / len(statements)
    
    def _generate_conclusion(self) -> Dict:
        """生成最终结论"""
        final_stances = {aid: info['stance'] for aid, info in self.agents.items()}
        
        return {
            'total_rounds': len(self.rounds),
            'final_consensus': self.rounds[-1].consensus_score if self.rounds else 0,
            'final_stances': final_stances,
            'debate_log': [
                {
                    'round': r.round_num,
                    'statements': [
                        {
                            'agent': s.agent_name,
                            'stance': s.stance.value,
                            'content': s.content[:100] + '...'
                        }
                        for s in r.statements
                    ]
                }
                for r in self.rounds
            ]
        }
