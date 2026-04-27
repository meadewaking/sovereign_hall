"""
🏛️ Sovereign Hall - 持久化模块
保存统计信息和会话历史，支持重启后继续累加
"""
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict, field
import threading


DATA_DIR = Path(__file__).parent.parent / "data"
STATS_FILE = DATA_DIR / "session_stats.json"
HISTORY_DIR = DATA_DIR / "session_history"


@dataclass
class TokenStats:
    """Token统计"""
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    total_requests: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0


@dataclass
class SessionStats:
    """会话统计"""
    start_time: str = ""
    total_rounds: int = 0
    total_time_seconds: float = 0.0
    topics_discussed: List[str] = field(default_factory=list)
    proposals_generated: int = 0
    winning_proposals: int = 0
    token_stats: TokenStats = field(default_factory=TokenStats)
    last_updated: str = ""


@dataclass
class ConversationMessage:
    """对话消息"""
    role: str
    content: str
    timestamp: str
    token_count: int = 0


class PersistenceManager:
    """持久化管理器"""

    def __init__(self):
        self._lock = threading.Lock()
        self._ensure_directories()
        self._stats = self._load_stats()
        self._current_topic = None

    def _ensure_directories(self):
        """确保目录存在"""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    def _load_stats(self) -> SessionStats:
        """加载统计信息"""
        if STATS_FILE.exists():
            try:
                with open(STATS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 重建 TokenStats
                    if 'token_stats' in data:
                        data['token_stats'] = TokenStats(**data['token_stats'])
                    return SessionStats(**data)
            except Exception as e:
                print(f"加载统计失败: {e}")
        return SessionStats(start_time=datetime.now().isoformat())

    def _save_stats(self):
        """保存统计信息"""
        with self._lock:
            self._stats.last_updated = datetime.now().isoformat()
            try:
                # 转换为可序列化的字典
                data = {
                    'start_time': self._stats.start_time,
                    'total_rounds': self._stats.total_rounds,
                    'total_time_seconds': self._stats.total_rounds,
                    'topics_discussed': self._stats.topics_discussed,
                    'proposals_generated': self._stats.proposals_generated,
                    'winning_proposals': self._stats.winning_proposals,
                    'token_stats': {
                        'total_tokens': self._stats.token_stats.total_tokens,
                        'total_cost_usd': self._stats.token_stats.total_cost_usd,
                        'total_requests': self._stats.token_stats.total_requests,
                        'prompt_tokens': self._stats.token_stats.prompt_tokens,
                        'completion_tokens': self._stats.token_stats.completion_tokens,
                    },
                    'last_updated': self._stats.last_updated,
                }
                with open(STATS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"保存统计失败: {e}")

    def load_previous_stats(self) -> Dict:
        """获取上次会话的统计（用于显示）"""
        stats = self._stats.token_stats
        return {
            'total_tokens': stats.total_tokens,
            'total_cost_usd': stats.total_cost_usd,
            'total_requests': stats.total_requests,
            'total_rounds': self._stats.total_rounds,
            'topics_discussed': self._stats.topics_discussed,
        }

    def accumulate_token_usage(self, prompt_tokens: int = 0, completion_tokens: int = 0, cost: float = 0):
        """累加Token使用（用于已有数据加载或更新）"""
        with self._lock:
            # 这个方法主要用于触发保存，实际数据由外部更新
            pass
        self._save_stats()

    def increment_rounds(self):
        """增加轮次"""
        with self._lock:
            self._stats.total_rounds += 1
        self._save_stats()

    def add_topic(self, topic: str):
        """添加讨论过的话题"""
        with self._lock:
            if topic not in self._stats.topics_discussed:
                self._stats.topics_discussed.append(topic)
        self._save_stats()

    def increment_proposals(self, count: int = 1):
        """增加提案数"""
        with self._lock:
            self._stats.proposals_generated += count
        self._save_stats()

    def increment_winning(self, count: int = 1):
        """增加获胜提案数"""
        with self._lock:
            self._stats.winning_proposals += count
        self._save_stats()

    def add_time(self, seconds: float):
        """增加时间"""
        with self._lock:
            self._stats.total_time_seconds += seconds
        self._save_stats()

    # ========== 会话历史相关 ==========

    def get_history_path(self, topic: str = None) -> Path:
        """获取历史文件路径"""
        if topic:
            # 清理话题名称作为文件名
            safe_name = "".join(c for c in topic if c.isalnum() or c in (' ', '-', '_')).strip()[:30]
            safe_name = safe_name.replace(' ', '_')
            return HISTORY_DIR / f"{safe_name}.json"
        return HISTORY_DIR / "global_history.json"

    def save_conversation_history(self, topic: str, messages: List[Dict], summary: str = ""):
        """保存对话历史"""
        with self._lock:
            history_file = self.get_history_path(topic)
            history_data = {
                'topic': topic,
                'saved_at': datetime.now().isoformat(),
                'summary': summary,
                'messages': messages,
            }
            try:
                with open(history_file, 'w', encoding='utf-8') as f:
                    json.dump(history_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"保存对话历史失败: {e}")

    def load_conversation_history(self, topic: str) -> Optional[List[Dict]]:
        """加载对话历史"""
        history_file = self.get_history_path(topic)
        if history_file.exists():
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('messages', [])
            except Exception as e:
                print(f"加载对话历史失败: {e}")
        return None

    def get_topic_summary(self, topic: str) -> str:
        """获取话题摘要"""
        history_file = self.get_history_path(topic)
        if history_file.exists():
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('summary', '')
            except:
                pass
        return ""

    def list_topics(self) -> List[str]:
        """列出所有讨论过的话题"""
        topics = []
        if HISTORY_DIR.exists():
            for f in HISTORY_DIR.glob("*.json"):
                try:
                    with open(f, 'r', encoding='utf-8') as file:
                        data = json.load(file)
                        topic = data.get('topic', f.stem)
                        topics.append(topic)
                except:
                    pass
        return topics


# 全局单例
_persistence = None


def get_persistence() -> PersistenceManager:
    """获取持久化管理器单例"""
    global _persistence
    if _persistence is None:
        _persistence = PersistenceManager()
    return _persistence