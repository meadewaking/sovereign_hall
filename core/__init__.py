"""
🏛️ Sovereign Hall - Core Data Structures
核心数据结构定义
"""

import asyncio
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import logging
import re

logger = logging.getLogger(__name__)

# 项目根目录 - 统一数据存储路径
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
DATA_DIR = PROJECT_ROOT / "data"


# ============================================================================
# 枚举类型
# ============================================================================

class AgentRole(Enum):
    """智能体角色枚举"""
    TMT_ANALYST = "tmt_analyst"           # TMT首席分析师
    CONSUMER_ANALYST = "consumer_analyst"  # 消费/医药分析师
    CYCLE_ANALYST = "cycle_analyst"        # 周期/制造分析师
    MACRO_STRATEGIST = "macro_strategist"  # 宏观策略师
    RISK_OFFICER = "risk_officer"          # 风控官
    QUANT_RESEARCHER = "quant_researcher"  # 量化研究员
    CIO = "cio"                            # 投资总监
    JUNIOR_ANALYST = "junior_analyst"      # 初级分析师(数据清洗)


class MarketSentiment(Enum):
    """市场情绪"""
    EXTREMELY_BULLISH = 5
    BULLISH = 4
    NEUTRAL = 3
    BEARISH = 2
    EXTREMELY_BEARISH = 1


class DecisionType(Enum):
    """决策类型"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    REDUCE = "reduce"
    SELL = "sell"


class VerdictDecision(Enum):
    """投委会裁决类型"""
    APPROVE = "approve"
    REJECT = "reject"
    DEFER = "defer"


# ============================================================================
# 文档与数据类
# ============================================================================

@dataclass
class Document:
    """原始文档"""
    id: str
    title: str
    content: str
    url: str
    source: str
    publish_time: datetime
    sector: str
    keywords: List[str] = field(default_factory=list)
    sentiment_score: float = 0.0
    importance_score: float = 0.0
    embedding: Optional[List[float]] = None
    crawled_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """后置初始化"""
        if isinstance(self.publish_time, str):
            # 尝试解析日期字符串
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d"]:
                try:
                    self.publish_time = datetime.strptime(self.publish_time, fmt)
                    break
                except ValueError:
                    continue

    @classmethod
    def from_dict(cls, data: Dict) -> 'Document':
        """从字典创建"""
        required_fields = ['id', 'title', 'content', 'url', 'source', 'publish_time', 'sector']
        for field_name in required_fields:
            if field_name not in data:
                raise ValueError(f"Missing required field: {field_name}")
        return cls(
            id=data['id'],
            title=data['title'],
            content=data['content'],
            url=data['url'],
            source=data['source'],
            publish_time=data['publish_time'],
            sector=data['sector'],
            keywords=data.get('keywords', []),
            sentiment_score=data.get('sentiment_score', 0.0),
            importance_score=data.get('importance_score', 0.0),
        )

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'id': self.id,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'source': self.source,
            'publish_time': self.publish_time.isoformat() if isinstance(self.publish_time, datetime) else self.publish_time,
            'sector': self.sector,
            'keywords': self.keywords,
            'sentiment_score': self.sentiment_score,
            'importance_score': self.importance_score,
            'crawled_at': self.crawled_at.isoformat() if isinstance(self.crawled_at, datetime) else self.crawled_at,
        }

    @property
    def summary(self) -> str:
        """获取摘要（前200字）"""
        return self.content[:200] + "..." if len(self.content) > 200 else self.content


@dataclass
class ResearchQuery:
    """研究查询请求"""
    query_id: str
    query_text: str
    sector: str
    priority: int  # 1-10, 10最高
    generated_time: datetime
    status: str = "pending"  # pending/processing/completed/failed
    results: List[str] = field(default_factory=list)
    error_message: Optional[str] = None

    @classmethod
    def create(cls, query_text: str, sector: str, priority: int = 5) -> 'ResearchQuery':
        """创建查询"""
        return cls(
            query_id=hashlib.md5(f"{query_text}{datetime.now().isoformat()}".encode()).hexdigest()[:16],
            query_text=query_text,
            sector=sector,
            priority=priority,
            generated_time=datetime.now(),
        )


@dataclass
class StockCandidate:
    """股票候选"""
    ticker: str
    name: str
    sector: str
    current_price: float
    market_cap: float
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    roe: Optional[float] = None
    trigger_reason: str = ""
    confidence_score: float = 0.0
    research_time: datetime = field(default_factory=datetime.now)

    @property
    def market_cap_str(self) -> str:
        """格式化市值"""
        if self.market_cap >= 1e12:
            return f"{self.market_cap/1e12:.2f}T"
        elif self.market_cap >= 1e10:
            return f"{self.market_cap/1e10:.2f}B"
        elif self.market_cap >= 1e8:
            return f"{self.market_cap/1e8:.2f}M"
        return f"{self.market_cap:.0f}"


@dataclass
class InvestmentProposal:
    """投资提案"""
    proposal_id: str
    ticker: str
    analyst_role: AgentRole
    direction: str  # long/short
    target_position: float  # 建议仓位 (0-1)
    entry_price: float
    stop_loss: float
    take_profit: float
    holding_period: int  # 持有天数
    thesis: str  # 核心逻辑
    supporting_evidence: List[str]
    risks: List[str]
    catalysts: List[str]
    created_at: datetime
    confidence: float = 0.0
    revised_position: Optional[float] = None
    revised_stop_loss: Optional[float] = None
    revised_confidence: Optional[float] = None

    def __post_init__(self):
        """验证数据"""
        if self.direction not in ['long', 'short']:
            raise ValueError(f"Invalid direction: {self.direction}")
        if not 0 <= self.target_position <= 1:
            raise ValueError(f"Invalid target_position: {self.target_position}")
        if not 0 <= self.confidence <= 1:
            raise ValueError(f"Invalid confidence: {self.confidence}")

    @property
    def risk_reward_ratio(self) -> float:
        """计算风险收益比"""
        if self.direction == 'long':
            return (self.take_profit - self.entry_price) / (self.entry_price - self.stop_loss)
        else:
            return (self.entry_price - self.stop_loss) / (self.take_profit - self.entry_price)

    @property
    def upside_potential(self) -> float:
        """计算上涨/下跌潜力"""
        if self.direction == 'long':
            return (self.take_profit - self.entry_price) / self.entry_price * 100
        else:
            return (self.entry_price - self.take_profit) / self.entry_price * 100

    @classmethod
    def create(
        cls,
        ticker: str,
        analyst_role: AgentRole,
        direction: str,
        target_position: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        holding_period: int,
        thesis: str,
        supporting_evidence: List[str],
        risks: List[str],
        catalysts: List[str],
        confidence: float = 0.7,
    ) -> 'InvestmentProposal':
        """创建提案的工厂方法"""
        return cls(
            proposal_id=f"prop_{hashlib.md5(f'{ticker}{datetime.now().isoformat()}'.encode()).hexdigest()[:12]}",
            ticker=ticker,
            analyst_role=analyst_role,
            direction=direction,
            target_position=target_position,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            holding_period=holding_period,
            thesis=thesis,
            supporting_evidence=supporting_evidence,
            risks=risks,
            catalysts=catalysts,
            created_at=datetime.now(),
            confidence=confidence,
        )

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'proposal_id': self.proposal_id,
            'ticker': self.ticker,
            'analyst_role': self.analyst_role.value,
            'direction': self.direction,
            'target_position': self.target_position,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'holding_period': self.holding_period,
            'thesis': self.thesis,
            'supporting_evidence': self.supporting_evidence,
            'risks': self.risks,
            'catalysts': self.catalysts,
            'created_at': self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            'confidence': self.confidence,
            'risk_reward_ratio': self.risk_reward_ratio,
        }


@dataclass
class ChallengeQuestion:
    """质询问题"""
    question_id: str
    questioner: AgentRole
    question: str
    severity: str  # low/medium/high/critical
    referenced_data: List[str]
    created_at: datetime

    @classmethod
    def create(cls, questioner: AgentRole, question: str, severity: str = "medium", referenced_data: List[str] = None) -> 'ChallengeQuestion':
        """创建质询"""
        return cls(
            question_id=f"chal_{hashlib.md5(f'{questioner.value}{question[:50]}'.encode()).hexdigest()[:12]}",
            questioner=questioner,
            question=question,
            severity=severity,
            referenced_data=referenced_data or [],
            created_at=datetime.now(),
        )


@dataclass
class DefenseResponse:
    """答辩回复"""
    response_id: str
    challenge_id: str
    defender: AgentRole
    response: str
    new_evidence: List[str]
    revised_confidence: float
    created_at: datetime

    @classmethod
    def create(cls, challenge_id: str, defender: AgentRole, response: str, revised_confidence: float, new_evidence: List[str] = None) -> 'DefenseResponse':
        """创建答辩"""
        return cls(
            response_id=f"def_{hashlib.md5(f'{challenge_id}{defender.value}'.encode()).hexdigest()[:12]}",
            challenge_id=challenge_id,
            defender=defender,
            response=response,
            new_evidence=new_evidence or [],
            revised_confidence=revised_confidence,
            created_at=datetime.now(),
        )


@dataclass
class ICMeetingMinutes:
    """投委会会议纪要"""
    meeting_id: str
    proposal_id: str
    participants: List[AgentRole]
    rounds: int
    total_tokens_consumed: int
    proposal: InvestmentProposal
    challenges: List[ChallengeQuestion]
    defenses: List[DefenseResponse]
    final_verdict: Dict[str, Any]
    voting_results: Dict[str, str]  # role -> vote (approve/reject/abstain)
    meeting_duration: float  # 秒
    created_at: datetime

    @property
    def decision(self) -> str:
        """获取最终决定"""
        return self.final_verdict.get("decision", "defer")

    @property
    def is_approved(self) -> bool:
        """是否通过"""
        return self.decision == "approve"

    @property
    def is_rejected(self) -> bool:
        """是否拒绝"""
        return self.decision == "reject"

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'meeting_id': self.meeting_id,
            'proposal_id': self.proposal_id,
            'ticker': self.proposal.ticker,
            'decision': self.decision,
            'final_position': self.final_verdict.get('final_position'),
            'confidence': self.final_verdict.get('final_confidence'),
            'duration': self.meeting_duration,
            'tokens': self.total_tokens_consumed,
            'participants': [p.value for p in self.participants],
            'created_at': self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
        }


@dataclass
class PlaybookEntry:
    """投资手册条目"""
    entry_id: str
    category: str  # mistake/success/principle
    situation: str
    action_taken: str
    outcome: str
    lesson: str
    confidence_delta: float
    created_at: datetime
    references: List[str]
    ticker: Optional[str] = None

    @classmethod
    def create(
        cls,
        category: str,
        situation: str,
        action_taken: str,
        outcome: str,
        lesson: str,
        confidence_delta: float,
        ticker: str = None,
        references: List[str] = None,
    ) -> 'PlaybookEntry':
        """创建手册条目"""
        return cls(
            entry_id=f"pb_{hashlib.md5(f'{situation}{lesson}'.encode()).hexdigest()[:12]}",
            category=category,
            situation=situation,
            action_taken=action_taken,
            outcome=outcome,
            lesson=lesson,
            confidence_delta=confidence_delta,
            created_at=datetime.now(),
            references=references or [],
            ticker=ticker,
        )


@dataclass
class TokenStats:
    """Token使用统计"""
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_cost_usd: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)
    # Token速率统计
    peak_token_rate: float = 0.0  # 峰值Token速率 (tokens/秒)
    avg_token_rate: float = 0.0  # 平均Token速率 (tokens/秒)
    _last_request_time: datetime = field(default_factory=datetime.now)
    _token_rates: list = field(default_factory=list)

    def add_request(self, prompt_len: int, completion_len: int, success: bool = True, cost_usd: float = 0.0):
        """添加请求记录"""
        tokens = (prompt_len + completion_len) // 4  # 估算
        self.total_tokens += tokens
        self.prompt_tokens += prompt_len // 4
        self.completion_tokens += completion_len // 4
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        self.total_cost_usd += cost_usd

        # 计算Token速率
        now = datetime.now()
        time_diff = (now - self._last_request_time).total_seconds()
        if time_diff > 0 and tokens > 0:
            rate = tokens / time_diff  # tokens/秒
            self._token_rates.append(rate)
            if rate > self.peak_token_rate:
                self.peak_token_rate = rate
            # 计算移动平均（最近100次）
            if len(self._token_rates) > 100:
                self._token_rates = self._token_rates[-100:]
            self.avg_token_rate = sum(self._token_rates) / len(self._token_rates)

        self._last_request_time = now

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    @property
    def avg_tokens_per_request(self) -> float:
        """平均每次请求Token数"""
        if self.total_requests == 0:
            return 0.0
        return self.total_tokens / self.total_requests

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_tokens': self.total_tokens,
            'prompt_tokens': self.prompt_tokens,
            'completion_tokens': self.completion_tokens,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': f"{self.success_rate:.2%}",
            'total_cost_usd': f"${self.total_cost_usd:.4f}",
            'avg_tokens_per_request': f"{self.avg_tokens_per_request:.0f}",
            'peak_token_rate': f"{self.peak_token_rate:.1f}",
            'avg_token_rate': f"{self.avg_token_rate:.1f}",
            'last_updated': self.last_updated.isoformat(),
        }


@dataclass
class SystemStats:
    """系统运行统计"""
    iteration: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    total_proposals: int = 0
    approved_proposals: int = 0
    rejected_proposals: int = 0
    total_meetings: int = 0
    token_stats: TokenStats = field(default_factory=TokenStats)
    spider_success: int = 0
    spider_failed: int = 0
    documents_indexed: int = 0
    playbook_entries: int = 0
    blacklisted_tickers: int = 0

    @property
    def approval_rate(self) -> float:
        """提案通过率"""
        if self.total_proposals == 0:
            return 0.0
        return self.approved_proposals / self.total_proposals

    @property
    def uptime_seconds(self) -> float:
        """运行时间（秒）"""
        return (datetime.now() - self.start_time).total_seconds()

    @property
    def uptime_formatted(self) -> str:
        """格式化运行时间"""
        seconds = self.uptime_seconds
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        elif seconds < 86400:
            return f"{seconds/3600:.1f}h"
        else:
            return f"{seconds/86400:.1f}d"

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'iteration': self.iteration,
            'uptime': self.uptime_formatted,
            'total_proposals': self.total_proposals,
            'approved': self.approved_proposals,
            'rejected': self.rejected_proposals,
            'approval_rate': f"{self.approval_rate:.2%}",
            'total_meetings': self.total_meetings,
            'documents_indexed': self.documents_indexed,
            'playbook_entries': self.playbook_entries,
            'blacklisted': self.blacklisted_tickers,
            'token_stats': self.token_stats.to_dict(),
            'spider': {
                'success': self.spider_success,
                'failed': self.spider_failed,
                'rate': f"{self.spider_success / max(1, self.spider_success + self.spider_failed):.2%}",
            },
        }