"""
🏛️ Sovereign Hall - Core Module
核心模块导出
"""

from pathlib import Path
from dataclasses import dataclass, field, MISSING
from typing import Dict, Any, List, Optional
from enum import Enum
import threading
from datetime import datetime
import logging

logger = logging.getLogger("sovereign_hall")

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
    CIO = "cio"
    JUNIOR_ANALYST = "junior_analyst"


class Document:
    """文档模型（兼容旧代码）"""

    def __init__(self, content: str = "", metadata: Dict[str, Any] = None,
                 source: str = "", timestamp = None, doc_id: str = "",
                 id: str = "", title: str = "", url: str = "",
                 sector: str = "", keywords: List[str] = None,
                 publish_time = None, **kwargs):
        # 设置基本字段
        self.content = content
        self.metadata = metadata or {}
        self.source = source
        self.timestamp = timestamp if timestamp else datetime.now()
        self.doc_id = doc_id or id or ""

        # 兼容旧字段（存到 metadata）
        if title and "title" not in self.metadata:
            self.metadata["title"] = title
        if url and "url" not in self.metadata:
            self.metadata["url"] = url
        if sector and "sector" not in self.metadata:
            self.metadata["sector"] = sector
        if keywords and "keywords" not in self.metadata:
            self.metadata["keywords"] = keywords
        if publish_time and "publish_time" not in self.metadata:
            self.metadata["publish_time"] = publish_time.isoformat() if isinstance(publish_time, datetime) else str(publish_time)

        # 如果没有 doc_id，生成一个
        if not self.doc_id:
            import uuid
            self.doc_id = str(uuid.uuid4())

    @property
    def id(self) -> str:
        """兼容旧代码的 id 属性"""
        return self.doc_id

    @property
    def url(self) -> str:
        """获取 url（从 metadata 中）"""
        return self.metadata.get("url", "")

    @property
    def title(self) -> str:
        """获取 title（从 metadata 中）"""
        return self.metadata.get("title", "")

    @property
    def sector(self) -> str:
        """获取 sector（从 metadata 中）"""
        return self.metadata.get("sector", "")

    @property
    def keywords(self) -> List[str]:
        """获取 keywords（从 metadata 中）"""
        return self.metadata.get("keywords", [])

    @property
    def embedding(self):
        """获取 embedding（从 metadata 中）"""
        return self.metadata.get("embedding")

    @embedding.setter
    def embedding(self, value):
        """设置 embedding（存到 metadata 中）"""
        self.metadata["embedding"] = value

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.doc_id,
            "content": self.content,
            "metadata": self.metadata,
            "source": self.source,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else str(self.timestamp),
            "doc_id": self.doc_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Document":
        """从字典创建"""
        # 处理时间字段
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except:
                timestamp = datetime.now()
        elif timestamp is None:
            timestamp = datetime.now()

        # 处理旧格式字段（兼容）
        content = data.get("content", "")
        if not content and data.get("title"):
            content = data.get("title", "") + " " + data.get("content", "")

        metadata = data.get("metadata", {})
        if not metadata:
            # 从旧格式迁移
            metadata = {
                "title": data.get("title", ""),
                "url": data.get("url", ""),
                "sector": data.get("sector", ""),
                "keywords": data.get("keywords", []),
                "sentiment_score": data.get("sentiment_score", 0.0),
                "importance_score": data.get("importance_score", 0.0),
                "publish_time": data.get("publish_time"),
                "crawled_at": data.get("crawled_at"),
            }

        return cls(
            content=content,
            metadata=metadata,
            source=data.get("source", ""),
            timestamp=timestamp,
            doc_id=data.get("doc_id") or data.get("id", ""),
        )

    @classmethod
    def create(cls, content: str = "", title: str = "", url: str = "",
               source: str = "", sector: str = "", keywords: List[str] = None,
               publish_time = None, id: str = "", **kwargs) -> "Document":
        """创建文档的便捷方法（兼容旧代码）"""
        metadata = {
            "title": title,
            "url": url,
            "sector": sector,
            "keywords": keywords or [],
            "publish_time": publish_time.isoformat() if publish_time else None,
        }
        # 合并额外参数（排除已处理的参数）
        for k, v in kwargs.items():
            if k not in ("doc_id", "id"):
                metadata[k] = v

        # 处理时间
        timestamp = publish_time if publish_time else datetime.now()

        # doc_id 优先级：id 参数 > kwargs 中的 id > kwargs 中的 doc_id
        final_id = id or kwargs.get("id", "") or kwargs.get("doc_id", "")

        return cls(
            content=content,
            metadata=metadata,
            source=source,
            timestamp=timestamp,
            doc_id=final_id,
        )


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
    total_requests: int = 0  # request_count 的别名
    successful_requests: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # 非dataclass字段
    _persistence_loaded: bool = field(default=False, repr=False)
    _first_request_time: float = field(default=None, repr=False)
    _last_request_time: float = field(default=None, repr=False)
    _peak_token_rate: float = field(default=0.0, repr=False)
    _request_timestamps: list = field(default_factory=list, repr=False)

    def __post_init__(self):
        # 尝试从持久化加载
        self._persistence_loaded = False
        self._try_load_from_disk()

    def _try_load_from_disk(self):
        """尝试从磁盘加载之前的统计"""
        try:
            from ..services.persistence import get_persistence
            persistence = get_persistence()
            prev_stats = persistence.load_previous_stats()
            if prev_stats and prev_stats.get('total_tokens', 0) > 0:
                self.total_tokens = prev_stats.get('total_tokens', 0)
                self.prompt_tokens = prev_stats.get('prompt_tokens', 0)
                self.completion_tokens = prev_stats.get('completion_tokens', 0)
                self.total_cost = prev_stats.get('total_cost_usd', 0.0)
                self.request_count = prev_stats.get('total_requests', 0)
                self.total_requests = self.request_count
                self._persistence_loaded = True
                logger.info(f"已加载历史Token统计: {self.total_tokens:,} tokens, ${self.total_cost:.2f}")
        except Exception as e:
            logger.debug(f"加载历史统计失败: {e}")

    def add_usage(self, prompt_tokens: int, completion_tokens: int, cost: float = 0.0, success: bool = True):
        import time
        current_time = time.time()
        tokens_this_request = prompt_tokens + completion_tokens

        with self._lock:
            self.prompt_tokens += prompt_tokens
            self.completion_tokens += completion_tokens
            self.total_tokens += tokens_this_request
            self.total_cost += cost
            self.request_count += 1
            self.total_requests = self.request_count
            if success:
                self.successful_requests += 1

            # 追踪时间戳用于计算速率
            if self._first_request_time is None:
                self._first_request_time = current_time
            self._last_request_time = current_time
            self._request_timestamps.append((current_time, tokens_this_request))

            # 只保留最近60秒的时间戳
            cutoff = current_time - 60
            self._request_timestamps = [(t, tok) for t, tok in self._request_timestamps if t > cutoff]

            # 计算当前速率 (tokens/秒)
            if len(self._request_timestamps) >= 2:
                time_span = self._request_timestamps[-1][0] - self._request_timestamps[0][0]
                if time_span > 0:
                    current_rate = sum(tok for _, tok in self._request_timestamps) / time_span
                    if current_rate > self._peak_token_rate:
                        self._peak_token_rate = current_rate

        # 持久化（每次更新都保存）
        self._save_to_disk()

    def _save_to_disk(self):
        """保存到磁盘"""
        try:
            from ..services.persistence import get_persistence
            persistence = get_persistence()
            persistence.accumulate_token_usage(
                prompt_tokens=0,  # 这里传0，因为是累加
                completion_tokens=0,
                cost=0
            )
            # 直接更新持久化文件
            persistence._stats.token_stats.total_tokens = self.total_tokens
            persistence._stats.token_stats.prompt_tokens = self.prompt_tokens
            persistence._stats.token_stats.completion_tokens = self.completion_tokens
            persistence._stats.token_stats.total_cost_usd = self.total_cost
            persistence._stats.token_stats.total_requests = self.request_count
            persistence._save_stats()
        except Exception as e:
            logger.debug(f"保存Token统计失败: {e}")

    def add_request(self, prompt_len: int, completion_len: int, success: bool = True, cost_usd: float = 0.0):
        """兼容旧代码的 add_request 方法（参数是字符数）"""
        # 转换为 token 估算（约 4 字符 = 1 token）
        prompt_tokens = prompt_len // 4
        completion_tokens = completion_len // 4
        self.add_usage(prompt_tokens, completion_tokens, cost_usd, success)

    def get_stats(self) -> Dict[str, Any]:
        import time
        with self._lock:
            # 计算平均 token 速率
            avg_token_rate = "0/s"
            if self._first_request_time and self._last_request_time:
                elapsed = self._last_request_time - self._first_request_time
                if elapsed > 0:
                    avg_rate = self.total_tokens / elapsed
                    avg_token_rate = f"{avg_rate:.1f}/s"

            return {
                "total_tokens": self.total_tokens,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_cost": self.total_cost,
                "request_count": self.request_count,
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "peak_token_rate": f"{self._peak_token_rate:.1f}/s",
                "avg_token_rate": avg_token_rate,
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
