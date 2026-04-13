"""
🏛️ Sovereign Hall - LLM Client Service
LLM客户端服务 - 管理并发调用和Token统计
"""

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

import httpx
import openai
import requests

from ..core.config import get_config
from ..core import TokenStats
from ..utils import (
    safe_parse_json,
    TokenCalculator,
    ThreadSafeCounter,
    RateLimiter,
    setup_logging,
    generate_id,
    retry_with_backoff,
)

logger = logging.getLogger(__name__)


class LLMClient:
    """LLM客户端 - 管理并发调用和Token统计"""

    def __init__(
        self,
        max_concurrent: int = 16,
        model: str = None,
        provider: str = None,
        api_key: str = None,
        base_url: str = None,
        timeout: int = 120,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        """
        初始化LLM客户端

        Args:
            max_concurrent: 最大并发数
            model: 模型名称
            provider: 提供商 (openai/anthropic/local)
            api_key: API密钥
            base_url: API基础URL
            timeout: 超时时间（秒）
            max_retries: 最大重试次数
            retry_delay: 重试基础延迟（秒）
        """
        config = get_config()

        # 从config.yaml加载配置
        import os
        from pathlib import Path
        project_root = Path(__file__).parent.parent.parent
        config_file = project_root / "config.yaml"
        if os.path.exists(config_file):
            config.load_from_file(config_file)

        llm_config = config.get_llm_config()

        self.model = model or llm_config.get('model', 'claude-sonnet-4-5')
        self.provider = provider or llm_config.get('provider', 'anthropic')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # API配置
        self.api_key = api_key or llm_config.get('api_key', 'empty')
        self.base_url = base_url or llm_config.get('base_url')
        self.model_uuid = llm_config.get('model_uuid')  # 用于本地API的Host header

        # 并发控制
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.max_concurrent = max_concurrent

        # 统计
        self.token_stats = TokenStats()
        self._stats_lock = threading.Lock()

        # 缓存
        self.cache: Dict[str, Tuple[str, datetime]] = {}
        self.cache_lock = threading.Lock()

        # 速率限制
        rate_config = config.get_spider_config()
        self.rate_limiter = RateLimiter(
            rate=rate_config.get('requests_per_minute', 100) / 60,
            burst=rate_config.get('burst', 20)
        )

        # 初始化API客户端
        self._http_client: Optional[httpx.AsyncClient] = None  # 复用的HTTP客户端
        self._init_client()

        logger.info(f"LLM Client initialized: provider={self.provider}, model={self.model}, max_concurrent={max_concurrent}")

    def _init_client(self):
        """初始化API客户端"""
        config = get_config()
        pricing_config = config.get_pricing(self.provider, self.model)
        self.pricing = pricing_config if pricing_config else {'input_per_1k': 0.003, 'output_per_1k': 0.015}

        # 根据提供商初始化不同的客户端
        if self.provider == 'openai':
            self._init_openai_client()
        elif self.provider == 'anthropic':
            self._init_anthropic_client()
        elif self.provider == 'local':
            self._init_local_client()
        else:
            logger.warning(f"Unknown provider: {self.provider}, using local mock")
            self._init_local_client()

    def _init_openai_client(self):
        """初始化OpenAI客户端"""
        default_headers = None
        if self.model_uuid:
            default_headers = {"Host": self.model_uuid}
        self.client = openai.OpenAI(
            api_key=self.api_key or "dummy",
            base_url=self.base_url,
            timeout=self.timeout,
            default_headers=default_headers,
        )
        # 创建复用的异步HTTP客户端
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

    def _init_anthropic_client(self):
        """初始化Anthropic客户端"""
        try:
            import anthropic
            self.client = anthropic.Anthropic(
                api_key=self.api_key or "dummy",
                timeout=self.timeout,
            )
            self._use_anthropic_sdk = True
        except ImportError:
            logger.warning("Anthropic SDK not installed, falling back to OpenAI-compatible API")
            self.client = openai.OpenAI(
                api_key=self.api_key or "dummy",
                base_url=self.base_url or "https://api.anthropic.com",
                timeout=self.timeout,
            )
            self._use_anthropic_sdk = False

    def _init_local_client(self):
        """初始化本地/模拟客户端"""
        self._use_anthropic_sdk = False
        self.client = None
        logger.info("Using local mock LLM client")

    async def chat(
        self,
        system: str,
        user: str,
        temperature: float = 0.7,
        max_tokens: int = 4000,
        use_cache: bool = True,
        stream: bool = False,
    ) -> str:
        """
        单次对话（带重试机制）

        Args:
            system: 系统提示词
            user: 用户消息
            temperature: 温度（0-2）
            max_tokens: 最大输出token
            use_cache: 是否使用缓存
            stream: 是否流式输出

        Returns:
            模型响应文本
        """
        # 速率限制
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        async with self.semaphore:
            # 构建消息
            messages = [{"role": "system", "content": system}]
            if user:
                messages.append({"role": "user", "content": user})

            # 缓存检查
            if use_cache:
                cache_key = self._get_cache_key(messages, temperature)
                cached = self._get_cached(cache_key)
                if cached:
                    logger.debug(f"[CACHE HIT] {cache_key[:16]}")
                    return cached

            # 估算tokens（作为后备）
            estimated_prompt_tokens, _ = TokenCalculator.estimate_messages(messages, self.model)

            # 定义实际的API调用（用于重试）
            # 返回 (response, usage_dict) 元组
            async def _call_llm():
                if self.provider == 'anthropic' and getattr(self, '_use_anthropic_sdk', False):
                    result, usage = await self._anthropic_chat(system, user, temperature, max_tokens)
                else:
                    result, usage = await self._openai_chat(messages, temperature, max_tokens)
                return result, usage

            # 带重试的执行调用
            last_exception = None
            for attempt in range(self.max_retries + 1):
                try:
                    response, usage = await _call_llm()

                    # 检查响应是否为 None
                    if response is None:
                        raise ValueError("LLM returned None")

                    # 优先使用 API 返回的 usage 信息
                    if usage and usage.get('prompt_tokens') and usage.get('completion_tokens'):
                        prompt_tokens = usage['prompt_tokens']
                        completion_tokens = usage['completion_tokens']
                    else:
                        # 后备：使用估算
                        prompt_tokens = estimated_prompt_tokens
                        completion_tokens = TokenCalculator.estimate(response, self.model)

                    # 更新统计
                    cost = TokenCalculator.calculate_cost(
                        prompt_tokens, completion_tokens, self.pricing
                    )
                    with self._stats_lock:
                        self.token_stats.add_request(
                            prompt_len=prompt_tokens * 4,  # 还原为字符数
                            completion_len=completion_tokens * 4,
                            success=True,
                            cost_usd=cost
                        )

                    # 写入缓存
                    if use_cache:
                        self._set_cache(cache_key, response)

                    return response

                except Exception as e:
                    last_exception = e
                    is_retryable = self._is_retryable_error(e)

                    if attempt < self.max_retries and is_retryable:
                        delay = self.retry_delay * (2 ** attempt)  # 指数退避
                        logger.warning(f"LLM call failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. Retrying in {delay:.1f}s...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"LLM call failed after {attempt + 1} attempts: {e}")
                        # 使用估算值作为后备，避免变量未定义错误
                        fallback_prompt_tokens = estimated_prompt_tokens if 'estimated_prompt_tokens' in dir() else 0
                        with self._stats_lock:
                            self.token_stats.add_request(
                                prompt_len=fallback_prompt_tokens * 4,
                                completion_len=0,
                                success=False,
                                cost_usd=0
                            )
                        raise

            # 理论上不会到这里，但以防万一
            raise last_exception

    def _is_retryable_error(self, error: Exception) -> bool:
        """判断错误是否可重试"""
        error_str = str(error).lower()

        # 网络相关错误
        retryable_patterns = [
            'timeout',
            'timed out',
            'connection',
            'reset',
            'refused',
            'temporary failure',
            '429',
            '500',
            '502',
            '503',
            '504',
            'rate limit',
            'overloaded',
            'service unavailable',
            'bad gateway',
            'gateway timeout',
        ]

        return any(pattern in error_str for pattern in retryable_patterns)

    async def _openai_chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
    ) -> Tuple[str, Dict]:
        """OpenAI兼容API调用"""
        try:
            import httpx

            # 准备请求
            url = f"{self.base_url}/chat/completions" if self.base_url else "https://api.openai.com/v1/chat/completions"

            headers = {
                "Content-Type": "application/json",
            }

            # 如果有model_uuid，添加Host header
            if self.model_uuid:
                headers["Host"] = self.model_uuid

            # 本地API使用api_key，官方API使用Authorization header
            if self.base_url and self.base_url != "https://api.openai.com/v1":
                headers["Authorization"] = f"Bearer {self.api_key}"

            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }

            # 复用HTTP客户端
            resp = await self._http_client.post(
                url,
                json=payload,
                headers=headers
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"].get("content")
            if not content:
                raise ValueError("LLM returned empty content")

            # 提取 usage 信息
            usage = {}
            if 'usage' in data:
                usage = {
                    'prompt_tokens': data['usage'].get('prompt_tokens', 0),
                    'completion_tokens': data['usage'].get('completion_tokens', 0),
                    'total_tokens': data['usage'].get('total_tokens', 0),
                }

            return content, usage

        except Exception as e:
            logger.error(f"OpenAI chat failed: {e}")
            raise

    async def _anthropic_chat(
        self,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
    ) -> Tuple[str, Dict]:
        """Anthropic API调用"""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system,
                messages=[{"role": "user", "content": user}]
            )
            if not response.content or not response.content[0].text:
                raise ValueError("Anthropic returned empty content")

            # 提取 usage 信息
            usage = {}
            if hasattr(response, 'usage'):
                usage = {
                    'prompt_tokens': getattr(response.usage, 'input_tokens', 0),
                    'completion_tokens': getattr(response.usage, 'output_tokens', 0),
                    'total_tokens': getattr(response.usage, 'input_tokens', 0) + getattr(response.usage, 'output_tokens', 0),
                }

            return response.content[0].text, usage
        except Exception as e:
            logger.error(f"Anthropic chat failed: {e}")
            raise

    async def parallel_chat(
        self,
        requests: List[Dict[str, Any]],
        max_concurrent: int = None,
    ) -> List[str]:
        """
        并行批量对话

        Args:
            requests: 请求列表，每个包含 system, user, temperature, max_tokens
            max_concurrent: 最大并发数

        Returns:
            响应列表
        """
        if not requests:
            return []

        max_concurrent = max_concurrent or self.max_concurrent

        # 创建任务
        tasks = []
        for req in requests:
            task = self.chat(
                system=req.get("system", ""),
                user=req["user"],
                temperature=req.get("temperature", 0.7),
                max_tokens=req.get("max_tokens", 4000),
                use_cache=req.get("use_cache", True)
            )
            tasks.append(task)

        # 并发执行
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理异常
        responses = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Request {i} failed: {result}")
                responses.append("")
            else:
                responses.append(result)

        return responses

    async def get_embedding(self, text: str) -> List[float]:
        """获取文本向量"""
        config = get_config()
        embedding_config = config.get_llm_config()

        model = embedding_config.get('embedding_model', 'text-embedding-3-small')
        dimension = embedding_config.get('embedding_dim', 1024)
        embedding_uuid = embedding_config.get('embedding_uuid', '')

        # 如果有API密钥，调用真实API
        if embedding_uuid and self.base_url:
            try:
                import httpx
                # 使用正确的 encode 端点（去掉 /v1 前缀）
                base = self.base_url.rstrip('/v1').rstrip('/')
                url = f"{base}/encode"
                payload = {"sentence": [text[:8000]]}  # 限制长度
                headers = {
                    "Host": embedding_uuid,
                    "Content-Type": "application/json"
                }
                # 复用HTTP客户端
                resp = await self._http_client.post(
                    url,
                    json=payload,
                    headers=headers
                )
                resp.raise_for_status()
                data = resp.json()
                # 检查返回格式
                if 'embedding' in data and data['embedding']:
                    embeddings = data['embedding']
                    if embeddings and len(embeddings) > 0:
                        return embeddings[0]
            except Exception as e:
                logger.warning(f"Embedding API failed: {e}, using mock")

        # 返回模拟向量
        import random
        random.seed(hash(text) % (2**32))
        return [random.gauss(0, 1) for _ in range(dimension)]

    # =========================================================================
    # 缓存管理
    # =========================================================================

    def _get_cache_key(self, messages: List[Dict], temperature: float) -> str:
        """生成缓存键"""
        # 使用 json.dumps 代替 str，效率更高且结果更稳定
        content = json.dumps(messages, sort_keys=True, ensure_ascii=False) + json.dumps(temperature)
        return hashlib.md5(content.encode()).hexdigest()

    def _get_cached(self, cache_key: str) -> Optional[str]:
        """获取缓存"""
        with self.cache_lock:
            if cache_key in self.cache:
                content, timestamp = self.cache[cache_key]
                # 24小时过期
                if (datetime.now() - timestamp).total_seconds() < 86400:
                    return content
                else:
                    del self.cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, content: str):
        """设置缓存"""
        with self.cache_lock:
            # 限制缓存大小
            if len(self.cache) > 1000:
                # 移除最旧的条目
                oldest_key = min(
                    self.cache.keys(),
                    key=lambda k: self.cache[k][1]
                )
                del self.cache[oldest_key]
            self.cache[cache_key] = (content, datetime.now())

    def clear_cache(self):
        """清空缓存"""
        with self.cache_lock:
            self.cache.clear()
        logger.info("Cache cleared")

    # =========================================================================
    # 统计信息
    # =========================================================================

    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._stats_lock:
            return {
                'total_requests': self.token_stats.total_requests,
                'successful_requests': self.token_stats.successful_requests,
                'failed_requests': self.token_stats.failed_requests,
                'success_rate': f"{self.token_stats.success_rate:.2%}",
                'total_tokens': self.token_stats.total_tokens,
                'prompt_tokens': self.token_stats.prompt_tokens,
                'completion_tokens': self.token_stats.completion_tokens,
                'total_cost_usd': f"¥{self.token_stats.total_cost_usd:.4f}",
                'avg_tokens_per_request': f"{self.token_stats.avg_tokens_per_request:.0f}",
                'peak_token_rate': f"{self.token_stats.peak_token_rate:.1f}/s",
                'avg_token_rate': f"{self.token_stats.avg_token_rate:.1f}/s",
                'cache_size': len(self.cache),
            }

    def reset_stats(self):
        """重置统计"""
        with self._stats_lock:
            self.token_stats = TokenStats()
        logger.info("Stats reset")

    def __repr__(self):
        return f"LLMClient(provider={self.provider}, model={self.model}, concurrent={self.max_concurrent})"


# ============================================================================
# Token追踪装饰器
# ============================================================================

class TokenTracker:
    """Token使用追踪器"""

    def __init__(self):
        self.total_tokens = 0
        self.total_cost = 0.0
        self.request_count = 0
        self._lock = threading.Lock()

    def track(self, prompt_tokens: int, completion_tokens: int, cost: float):
        """追踪一次调用"""
        with self._lock:
            self.total_tokens += prompt_tokens + completion_tokens
            self.total_cost += cost
            self.request_count += 1

    def get_stats(self) -> Dict:
        """获取统计"""
        return {
            'requests': self.request_count,
            'total_tokens': self.total_tokens,
            'total_cost': self.total_cost,
            'avg_tokens_per_request': self.total_tokens / max(1, self.request_count),
        }

    def reset(self):
        """重置"""
        with self._lock:
            self.total_tokens = 0
            self.total_cost = 0.0
            self.request_count = 0