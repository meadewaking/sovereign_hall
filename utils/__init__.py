"""
🏛️ Sovereign Hall - Utility Functions
工具函数集合
"""

import asyncio
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from concurrent.futures import ThreadPoolExecutor
import threading

from ..core import DATA_DIR

logger = logging.getLogger(__name__)

T = TypeVar('T')

# ============================================================================
# 线程安全工具
# ============================================================================

class ThreadSafeCounter:
    """线程安全计数器"""

    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()

    def increment(self, delta: int = 1) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def decrement(self, delta: int = 1) -> int:
        with self._lock:
            self._value -= delta
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def reset(self, value: int = 0):
        with self._lock:
            self._value = value


class RateLimiter:
    """速率限制器"""

    def __init__(self, rate: float, burst: int = None):
        """
        rate: 每秒允许的请求数
        burst: 突发请求数限制
        """
        self.rate = rate
        self.burst = burst if burst is not None else int(rate * 2)
        self.tokens = self.burst
        self.last_update = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> float:
        """获取token，返回等待时间"""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            # 补充tokens
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # 需要等待
            wait_time = (tokens - self.tokens) / self.rate
            self.tokens = 0
            return wait_time


# ============================================================================
# JSON工具
# ============================================================================

def safe_parse_json(text: str, default: Any = None) -> Any:
    """
    安全解析JSON，处理各种格式错误

    Args:
        text: 要解析的文本
        default: 解析失败时返回的默认值

    Returns:
        解析后的对象或默认值
    """
    if not text or not isinstance(text, str):
        return default

    text = text.strip()
    if not text:
        return default

    # 尝试提取Markdown代码块
    match = re.search(r'```(?:json)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if match:
        text = match.group(1).strip()

    # 尝试提取数组或对象
    match = re.search(r'[\{\[].*[\}\]]', text, re.DOTALL)
    if match:
        text = match.group(0).strip()

    # 尝试解析
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 最后的尝试：修复常见错误
    try:
        fixed = text
        fixed = re.sub(r'(\w+):', r'"\1":', fixed)  # 给key加引号
        fixed = fixed.replace("'", '"')  # 单引号转双引号
        fixed = fixed.replace("True", "true").replace("False", "false")
        fixed = fixed.replace("None", "null")
        return json.loads(fixed)
    except (json.JSONDecodeError, AttributeError):
        pass

    return default


def format_json(data: Any, indent: int = 2) -> str:
    """格式化JSON输出"""
    try:
        return json.dumps(data, ensure_ascii=False, indent=indent, default=str)
    except (TypeError, ValueError):
        return str(data)


# ============================================================================
# 文本处理工具
# ============================================================================

def truncate_text(text: str, max_length: int = 1000, suffix: str = "...") -> str:
    """截断文本"""
    if not text or len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def clean_text(text: str) -> str:
    """清理文本"""
    if not text:
        return ""
    # 移除多余空白
    text = re.sub(r'\s+', ' ', text)
    # 移除HTML标签
    text = re.sub(r'<[^>]+>', '', text)
    # 移除特殊字符（保留中文、英文、数字、常用标点）
    text = re.sub(r'[^\w\s\u4e00-\u9fff.,!?;:()[\]{}""\'\'-]', '', text)
    return text.strip()


def extract_numbers(text: str) -> List[float]:
    """从文本中提取数字"""
    pattern = r'-?\d+\.?\d*(?:[eE][+-]?\d+)?'
    matches = re.findall(pattern, text)
    numbers = []
    for match in matches:
        try:
            numbers.append(float(match))
        except ValueError:
            pass
    return numbers


def extract_percentages(text: str) -> List[float]:
    """从文本中提取百分比"""
    pattern = r'(\d+\.?\d*)%'
    matches = re.findall(pattern, text)
    percentages = []
    for match in matches:
        try:
            percentages.append(float(match) / 100)
        except ValueError:
            pass
    return percentages


def extract_tickers(text: str) -> List[str]:
    """从文本中提取股票代码"""
    # 匹配各种格式的股票代码
    patterns = [
        r'(?:股票|代码| ticker)[:：]?\s*([A-Z]{2,5})',  # 大写字母代码
        r'\b([A-Z]{2,5})\b',  # 独立的大写字母代码
        r'(?:600|000|300|688|00\d{3}|60\d{3}|300\d{3}|688\d{3})\.\d{3}',  # A股代码
    ]

    tickers = set()
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            if 2 <= len(match) <= 6:
                tickers.add(match.upper())

    return list(tickers)


# ============================================================================
# 哈希工具
# ============================================================================

def generate_id(prefix: str = "") -> str:
    """生成唯一ID"""
    timestamp = datetime.now().isoformat()
    random_suffix = uuid.uuid4().hex[:8]
    content = f"{timestamp}{random_suffix}"
    short_hash = hashlib.md5(content.encode()).hexdigest()[:12]
    if prefix:
        return f"{prefix}_{short_hash}"
    return short_hash


def short_hash(text: str, length: int = 8) -> str:
    """生成短哈希"""
    return hashlib.md5(text.encode()).hexdigest()[:length]


# ============================================================================
# 时间工具
# ============================================================================

def format_duration(seconds: float) -> str:
    """格式化时长"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def time_since(dt: datetime) -> str:
    """返回自某个时间点以来的描述"""
    now = datetime.now()
    delta = now - dt

    if delta.total_seconds() < 60:
        return f"{int(delta.total_seconds())}秒前"
    elif delta.total_seconds() < 3600:
        return f"{int(delta.total_seconds()/60)}分钟前"
    elif delta.total_seconds() < 86400:
        return f"{int(delta.total_seconds()/3600)}小时前"
    elif delta.total_seconds() < 604800:
        return f"{int(delta.total_seconds()/86400)}天前"
    else:
        return dt.strftime("%Y-%m-%d")


# ============================================================================
# 文件工具
# ============================================================================

def ensure_dir(path: str) -> Path:
    """确保目录存在"""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_json(data: Any, filepath: str, indent: int = 2) -> bool:
    """保存JSON文件"""
    try:
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent, default=str)
        return True
    except Exception as e:
        logger.error(f"Failed to save JSON: {e}")
        return False


def load_json(filepath: str, default: Any = None) -> Any:
    """加载JSON文件"""
    try:
        filepath = Path(filepath)
        if not filepath.exists():
            return default
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON: {e}")
        return default


# ============================================================================
# 重试工具
# ============================================================================

import random

async def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential: bool = True,
    jitter: bool = True,
    exceptions: Tuple = (Exception,),
) -> Any:
    """
    带退避的重试机制

    Args:
        func: 异步函数
        max_retries: 最大重试次数
        base_delay: 基础延迟（秒）
        max_delay: 最大延迟（秒）
        exponential: 是否指数退避
        jitter: 是否添加随机抖动
        exceptions: 需要捕获的异常类型

    Returns:
        函数返回值
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return await func()
        except exceptions as e:
            last_exception = e

            if attempt >= max_retries:
                logger.warning(f"Max retries ({max_retries}) reached, giving up")
                raise

            # 计算延迟
            if exponential:
                delay = base_delay * (2 ** attempt)
            else:
                delay = base_delay

            delay = min(delay, max_delay)

            # 添加抖动
            if jitter:
                delay = delay * (0.5 + random.random())

            logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. Retrying in {delay:.1f}s...")
            await asyncio.sleep(delay)

    raise last_exception


def sync_retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential: bool = True,
    jitter: bool = True,
    exceptions: Tuple = (Exception,),
) -> Any:
    """同步版本的重试机制"""
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e

            if attempt >= max_retries:
                logger.warning(f"Max retries ({max_retries}) reached, giving up")
                raise

            if exponential:
                delay = base_delay * (2 ** attempt)
            else:
                delay = base_delay

            delay = min(delay, max_delay)

            if jitter:
                delay = delay * (0.5 + random.random())

            logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)

    raise last_exception


# ============================================================================
# 异步工具
# ============================================================================

async def gather_with_concurrency(n: int, *tasks, return_exceptions: bool = False) -> List:
    """
    限制并发数量的异步gather

    Args:
        n: 最大并发数
        tasks: 异步任务
        return_exceptions: 是否返回异常而非抛出
    """
    semaphore = asyncio.Semaphore(n)

    async def run_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(
        *(run_task(task) for task in tasks),
        return_exceptions=return_exceptions
    )


def run_sync_in_thread(func: Callable, *args, **kwargs) -> Any:
    """在独立线程中运行同步函数"""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        return future.result()


# ============================================================================
# 日志工具
# ============================================================================

def setup_logging(
    name: str = None,
    level: str = "INFO",
    log_format: str = "text",
    log_dir: str = None,
    filename: str = None
) -> logging.Logger:
    """
    设置日志配置

    Args:
        name: 日志名称
        level: 日志级别
        log_format: 格式 (text) - 统一使用文本格式
        log_dir: 日志目录
        filename: 日志文件名
    """
    logger = logging.getLogger(name)

    # 避免重复添加handler
    if logger.handlers:
        return logger

    # 设置级别
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # 统一使用简洁文本格式
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 单一日志文件输出
    log_dir = Path(log_dir) if log_dir else DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    filename = filename or "sovereign_hall.log"
    log_file = log_dir / filename

    # 使用 RotatingFileHandler 自动轮转
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ============================================================================
# 估算工具
# ============================================================================

def estimate_tokens(text: str) -> int:
    """估算文本的token数量（中英文混合）"""
    if not text:
        return 0
    # 简单估算：中文约0.7 token/字符，英文约0.25 token/字符
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    english_chars = len(text) - chinese_chars
    return int(chinese_chars * 0.7 + english_chars * 0.25)


def estimate_tokens_for_messages(messages: List[Dict[str, str]]) -> int:
    """估算消息列表的token数量"""
    total = 0
    for msg in messages:
        total += estimate_tokens(msg.get("content", ""))
        total += estimate_tokens(msg.get("role", ""))
        total += estimate_tokens(msg.get("name", ""))
    return total


# ============================================================================
# 验证工具
# ============================================================================

def validate_ticker(ticker: str) -> bool:
    """验证股票代码格式"""
    if not ticker:
        return False
    # A股: 6位数字
    if re.match(r'^\d{6}$', ticker):
        return True
    # 美股/港股: 2-5位大写字母
    if re.match(r'^[A-Z]{2,5}$', ticker):
        return True
    return False


def sanitize_filename(filename: str) -> str:
    """清理文件名"""
    # 移除不安全字符
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # 限制长度
    filename = filename[:100]
    return filename


# ============================================================================
# 统计工具
# ============================================================================

class TokenCalculator:
    """Token计算器"""

    # 不同模型的token估算参数
    MODEL_PARAMS = {
        'claude': {'chars_per_token': 4, 'prompt_overhead': 50},
        'gpt-4': {'chars_per_token': 4, 'prompt_overhead': 30},
        'gpt-3.5': {'chars_per_token': 4, 'prompt_overhead': 20},
        'default': {'chars_per_token': 4, 'prompt_overhead': 30},
    }

    @classmethod
    def estimate(cls, text: str, model: str = 'default') -> int:
        """估算token数量"""
        params = cls.MODEL_PARAMS.get(model.lower(), cls.MODEL_PARAMS['default'])
        chars = len(text)
        return max(1, int(chars / params['chars_per_token']))

    @classmethod
    def estimate_messages(cls, messages: List[Dict], model: str = 'default') -> Tuple[int, int]:
        """
        估算消息的prompt和completion tokens

        Returns:
            (prompt_tokens, estimated_completion_tokens)
        """
        prompt_text = ""
        for msg in messages:
            prompt_text += f"{msg.get('role', '')}: {msg.get('content', '')}\n"

        prompt_tokens = cls.estimate(prompt_text, model)
        # 估算completion为prompt的30%-50%
        completion_tokens = int(prompt_tokens * 0.4)

        return prompt_tokens, completion_tokens

    @classmethod
    def calculate_cost(
        cls,
        prompt_tokens: int,
        completion_tokens: int,
        pricing: Dict[str, float]
    ) -> float:
        """
        计算API调用成本

        Args:
            prompt_tokens: prompt token数
            completion_tokens: completion token数
            pricing: 价格字典 {'input_per_1k': X, 'output_per_1k': Y}
        """
        input_cost = (prompt_tokens / 1000) * pricing.get('input_per_1k', 0)
        output_cost = (completion_tokens / 1000) * pricing.get('output_per_1k', 0)
        return input_cost + output_cost


# ============================================================================
# Token 格式化工具
# ============================================================================

def format_token(num: int) -> str:
    """格式化token显示（自动选择单位）"""
    if num >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.2f}T"
    elif num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}G"
    elif num >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.2f}k"
    else:
        return str(num)


def extract_actual_response(text: str, max_length: int = 8000) -> str:
    """
    从LLM输出中提取实际回复内容
    去除可能包含的系统提示词和模板标记，以及思考过程
    """
    if not text:
        return ""

    import re
    # 删除 <thinking>...</thinking>
    text = re.sub(r'<thinking>.*?</thinking>', '', text, flags=re.DOTALL)
    # 删除 <thought>...</thought>
    text = re.sub(r'<thought>.*?</thought>', '', text, flags=re.DOTALL)
    # 删除 **Thinking:** 或 **思考：**
    text = re.sub(r'\*\*Thinking[:：].*?\*\*', '', text, flags=re.DOTALL)
    text = re.sub(r'\*\*思考[:：].*?\*\*', '', text, flags=re.DOTALL)
    # 删除 "Thinking:" 开头的行
    text = re.sub(r'^Thinking:.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^思考:.*$', '', text, flags=re.MULTILINE)
    # 删除过渡语
    text = re.sub(r'经过分析，?', '', text)
    text = re.sub(r'让我思考，?', '', text)
    text = re.sub(r'首先，?', '', text)
    text = re.sub(r'其次，?', '', text)
    text = re.sub(r'最后，?', '', text)

    # 找到实际回复的开始位置
    markers = [
        '## 投资提案陈述报告', '## 风控质询报告', '## 量化分析报告',
        '## 宏观策略分析报告', '## 答辩报告', '## 决策框架', '【CIO裁决】',
        '## 投资主题概述', '## 核心投资逻辑', '## 风险收益分析',
        '## 风控质疑回应', '## 量化分析回应', '## 宏观因素回应',
        '## 核心逻辑重申', '## 修正后的投资方案',
        '## 一、', '## 二、', '## 三、', '## 四、', '## 五、', '## 六、',
        '### 一、', '### 二、', '### 三、', '### 四、', '### 五、'
    ]

    for marker in markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[idx:]
            break

    # 清理多余的空行
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    # 截取并返回
    if len(text) > max_length:
        return text[:max_length] + "\n... [内容被截断]"
    return text


def truncate_for_context(text: str, max_chars: int = 6000) -> str:
    """
    截断文本用于传递给LLM作为上下文
    保留开头和结尾（摘要式截断）
    """
    if not text or len(text) <= max_chars:
        return text

    # 保留前1/3和后2/3
    head = max_chars // 3
    tail = max_chars - head

    return text[:head] + f"\n... [中间内容省略 {len(text) - max_chars} 字] ...\n" + text[-tail:]