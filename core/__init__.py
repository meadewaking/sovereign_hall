"""
🏛️ Sovereign Hall - Core Module
核心模块导出
"""

from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from enum import Enum
import threading
from datetime import datetime

# 项目路径常量
PROJECT_ROOT = Path(__file__).parent.parent  # sovereign_hall 根目录
DATA_DIR = PROJECT_ROOT / "data"


class AgentRole(Enum):
    """智能体角色枚举"""
    TMT_ANALYST = "tmt_analyst"
    CONSUMER_ANALYST = "consumer_analyst"
    CYCLE_ANALYST = "cycle_analyst"
    MACRO_STRATEGIST = "macro_strategist"
    RISK_OFFICER = "risk_officer"
    QUANT_RESEARCHER = "quant_researcher"
    PORTFOLIO_MANAGER = "portfolio_manager"


@dataclass
class Document:
    """文档模型"""
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    doc_id: str = ""
    
    def __post_init__(self):
        if not self.doc_id:
            import uuid
            self.doc_id = str(uuid.uuid4())


@dataclass  
class InvestmentProposal:
    """投资提案"""
    ticker: str
    direction: str  # long / short
    sector: str
    thesis: str
    confidence: float = 0.0
    entry_price: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    analyst: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    proposal_id: str = ""
    
    def __post_init__(self):
        if not self.proposal_id:
            import uuid
            self.proposal_id = str(uuid.uuid4())


@dataclass
class ICMeetingMinutes:
    """投委会会议记录"""
    meeting_id: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    proposals: List[InvestmentProposal] = field(default_factory=list)
    votes: Dict[str, Any] = field(default_factory=dict)
    minutes: str = ""
    next_actions: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.meeting_id:
            import uuid
            self.meeting_id = str(uuid.uuid4())


@dataclass
class PlaybookEntry:
    """策略手册条目"""
    entry_id: str = ""
    pattern: str = ""
    conditions: List[str] = field(default_factory=list)
    action: str = ""
    examples: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        if not self.entry_id:
            import uuid
            self.entry_id = str(uuid.uuid4())


@dataclass
class SystemStats:
    """系统统计信息"""
    total_documents: int = 0
    total_proposals: int = 0
    total_meetings: int = 0
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class TokenStats:
    """Token使用统计"""
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_cost: float = 0.0
    request_count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    
    def add_usage(self, prompt_tokens: int, completion_tokens: int, cost: float = 0.0):
        with self._lock:
            self.prompt_tokens += prompt_tokens
            self.completion_tokens += completion_tokens
            self.total_tokens += prompt_tokens + completion_tokens
            self.total_cost += cost
            self.request_count += 1
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_tokens": self.total_tokens,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_cost": self.total_cost,
                "request_count": self.request_count
            }


__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "AgentRole",
    "Document",
    "InvestmentProposal",
    "ICMeetingMinutes",
    "PlaybookEntry",
    "SystemStats",
    "TokenStats"
]
