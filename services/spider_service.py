"""
🏛️ Sovereign Hall - Spider Swarm Service
分布式爬虫集群服务
"""

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
import threading

import httpx
from bs4 import BeautifulSoup
from ..core import Document as Doc

from ..core.config import get_config
from ..utils import (
    safe_parse_json,
    ThreadSafeCounter,
    RateLimiter,
    generate_id,
    clean_text,
    extract_tickers,
    retry_with_backoff,
)
from .llm_client import LLMClient

logger = logging.getLogger(__name__)


class SpiderSwarm:
    """分布式爬虫集群"""

    # 类级别的连续失败计数（所有实例共享）
    _consecutive_failures = 0
    _failure_threshold = 5  # 连续失败5次后进入告警模式
    _alarm_mode = False
    _alarm_start_time = 0  # 进入告警模式的时间
    _alarm_timeout = 30  # 告警模式30秒后自动恢复（缩短）

    # 内容质量过滤配置
    MIN_CONTENT_LENGTH = 30  # 最小内容长度
    MIN_UNIQUE_CHARS = 8  # 最少不同字符数
    MIN_TITLE_LENGTH = 4  # 最小标题长度

    @staticmethod
    def _is_valid_content(doc) -> bool:
        """检查文档内容是否有效（过滤垃圾信息）"""
        content = doc.content or ""
        title = doc.title or ""

        # 1. 内容长度检查
        if len(content) < SpiderSwarm.MIN_CONTENT_LENGTH:
            return False

        # 2. 检查是否是模板化内容（纯标题重复）
        if content == f"关于{title.replace('的搜索结果', '')}的搜索结果":
            return False

        # 3. 检查是否是占位符内容
        placeholder_patterns = [
            "关于{query}的搜索结果",
            "关于{query}的深度分析报告",
            "暂无内容",
            "内容获取失败",
        ]
        for p in placeholder_patterns:
            if p.format(query=title) in content or content == p:
                return False

        # 4. 检查内容是否有实际信息量（不同字符数）
        unique_chars = len(set(content))
        if unique_chars < SpiderSwarm.MIN_UNIQUE_CHARS:
            return False

        # 5. 检查标题是否过短
        if len(title) < SpiderSwarm.MIN_TITLE_LENGTH:
            return False

        return True

    @staticmethod
    def _filter_documents(docs: list) -> list:
        """过滤无效文档"""
        valid_docs = []
        for doc in docs:
            if SpiderSwarm._is_valid_content(doc):
                valid_docs.append(doc)

        if len(docs) > 0:
            logger.info(f"Document filter: {len(valid_docs)}/{len(docs)} valid")

        return valid_docs

    def __init__(
        self,
        max_concurrent: int = 50,
        timeout: int = 30,
        user_agent: str = None,
        retry_times: int = 3,
        cache_ttl: int = 3600,  # 缓存有效期（秒），默认1小时
    ):
        """
        初始化爬虫集群

        Args:
            max_concurrent: 最大并发数
            timeout: 请求超时时间（秒）
            user_agent: User-Agent
            retry_times: 重试次数
            cache_ttl: 搜索结果缓存有效期（秒）
        """
        config = get_config()
        spider_config = config.get_spider_config()

        self.max_concurrent = max_concurrent
        self.timeout = timeout or spider_config.get('timeout', 30)
        self.user_agent = user_agent or spider_config.get('user_agent', 'SovereignHall/1.0 (Research Bot)')
        self.retry_times = retry_times or spider_config.get('retry_times', 3)
        self.search_interval = spider_config.get('search_interval', 0.5)  # 搜索间隔

        # 搜索结果缓存（类级别共享，同一轮次内有效）
        self._search_cache: Dict[str, Tuple[List[Doc], float]] = {}  # query -> (docs, timestamp)
        self._cache_ttl = cache_ttl

        # 并发控制
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # 统计
        self.success_count = ThreadSafeCounter()
        self.fail_count = ThreadSafeCounter()

        # 速率限制
        rate_config = config.get_spider_config()
        self.rate_limiter = RateLimiter(
            rate=rate_config.get('requests_per_minute', 30) / 60,
            burst=rate_config.get('burst', 10)
        )

        # HTTP客户端
        self._init_client()

        logger.info(f"Spider Swarm initialized: max_concurrent={max_concurrent}, timeout={timeout}, cache_ttl={cache_ttl}s")

    def _init_client(self):
        """初始化HTTP客户端（走代理）"""
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=100),
            follow_redirects=True,
            proxy="http://127.0.0.1:7890"
        )

    async def close(self):
        """关闭客户端"""
        await self.client.aclose()

    def clear_cache(self):
        """清空搜索缓存"""
        self._search_cache.clear()
        logger.info("Search cache cleared")

    async def aggressive_search(
        self,
        queries: List[str],
        max_results_per_query: int = 20,
        sources: List[str] = None,
    ) -> List[Doc]:
        """
        激进式搜索 - 并发抓取（带缓存）

        Args:
            queries: 搜索词列表
            max_results_per_query: 每个搜索词的最大结果数
            sources: 数据源列表

        Returns:
            文档列表
        """
        import time
        if not queries:
            return []

        current_time = time.time()
        queries_to_search = []
        cached_results: Dict[str, List[Doc]] = {}

        # 检查缓存，分类需要搜索和已缓存的查询
        cache_hits = 0
        for query in queries:
            if query in self._search_cache:
                cached_docs, cached_time = self._search_cache[query]
                # 检查缓存是否过期
                if current_time - cached_time < self._cache_ttl:
                    cached_results[query] = cached_docs
                    cache_hits += 1
                    logger.info(f"🔍 Cache HIT: {query} ({len(cached_docs)} docs)")
                else:
                    # 缓存过期，需要重新搜索
                    queries_to_search.append(query)
            else:
                queries_to_search.append(query)

        if queries_to_search:
            logger.info(f"Starting aggressive search: {len(queries_to_search)} queries ({len(cached_results)} from cache)")
        else:
            logger.info(f"Aggressive search: all {len(queries)} queries from cache")

        # 创建搜索任务（只对需要搜索的查询）
        tasks = []
        for query in queries_to_search:
            task = self._search_single_query(query, max_results_per_query, sources)
            tasks.append(task)

        # 并发执行
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 展平并去重
        all_docs = []
        seen_urls: Set[str] = set()
        newly_cached = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Search task failed: {result}")
                self.fail_count.increment()
                continue

            query = queries_to_search[i]
            query_docs = []

            for doc in result:
                if doc.url not in seen_urls:
                    seen_urls.add(doc.url)
                    all_docs.append(doc)
                    query_docs.append(doc)
                    self.success_count.increment()

            # 缓存搜索结果
            if query_docs:
                self._search_cache[query] = (query_docs, current_time)
                newly_cached += 1

        # 添加缓存结果到最终结果（去重）
        for query, docs in cached_results.items():
            for doc in docs:
                if doc.url not in seen_urls:
                    seen_urls.add(doc.url)
                    all_docs.append(doc)

        logger.info(f"Aggressive search complete: {len(all_docs)} unique docs from {len(seen_urls)} URLs (cached: {len(cached_results)}, newly: {newly_cached})")

        # 过滤无效文档
        filtered_docs = self._filter_documents(all_docs)

        return filtered_docs

    async def _search_single_query(
        self,
        query: str,
        max_results: int,
        sources: List[str] = None,
    ) -> List[Doc]:
        """单个查询的搜索（带重试）"""
        # 速率限制
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        # 搜索间隔 + 随机抖动（0.5-1.5倍）
        import random
        jitter = self.search_interval * (0.5 + random.random())
        await asyncio.sleep(jitter)

        async with self.semaphore:
            logger.debug(f"Searching: {query}")

            # 定义搜索操作（用于重试）
            async def _do_search_with_timeout():
                return await asyncio.wait_for(
                    self._do_search(query, max_results, sources),
                    timeout=self.timeout
                )

            # 带重试的执行
            last_error = None
            for attempt in range(self.retry_times):
                try:
                    results = await _do_search_with_timeout()
                    return results
                except asyncio.TimeoutError:
                    last_error = f"Timeout after {self.timeout}s"
                    logger.warning(f"Search timeout for '{query}' (attempt {attempt + 1}/{self.retry_times})")
                except Exception as e:
                    last_error = str(e)
                    is_retryable = self._is_retryable_error(e)
                    if not is_retryable:
                        break

                if attempt < self.retry_times - 1:
                    delay = 1.0 * (2 ** attempt)  # 指数退避
                    await asyncio.sleep(delay)

            logger.error(f"Search failed for '{query}' after {self.retry_times} attempts: {last_error}")
            self.fail_count.increment()
            return []

    def _is_retryable_error(self, error: Exception) -> bool:
        """判断错误是否可重试"""
        error_str = str(error).lower()
        retryable_patterns = [
            'timeout', 'connection', 'reset', 'refused',
            '429', '500', '502', '503', '504',
            'rate limit', 'overloaded', 'temporary',
            'nodename', 'servname', 'dns', 'name or service',
            'network', 'unreachable', 'too many open files'
        ]
        return any(pattern in error_str for pattern in retryable_patterns)

    async def _do_search(
        self,
        query: str,
        max_results: int,
        sources: List[str] = None,
    ) -> List[Doc]:
        """
        执行搜索 - 优先级：DDG → 百度 → 搜狗
        搜索失败时进入告警模式，不再生成假数据
        """
        import time
        docs = []
        sources = sources or ['ddg']  # 百度被封禁频繁，默认只用DDG

        # 检查是否处于告警模式，如果是，检查是否超时需要恢复
        if SpiderSwarm._alarm_mode:
            elapsed = time.time() - SpiderSwarm._alarm_start_time
            if elapsed > SpiderSwarm._alarm_timeout:
                # 超时自动恢复
                SpiderSwarm._alarm_mode = False
                SpiderSwarm._consecutive_failures = 0
                logger.info(f"Spider alarm mode auto-recovered after {elapsed:.1f}s")
            else:
                logger.warning(f"Spider in alarm mode - search skipped for '{query}'")
                return []

        # 1. 尝试 DuckDuckGO 搜索（带代理）
        if 'ddg' in sources:
            try:
                ddg_results = await self._ddg_search(query, max_results)
                if ddg_results:
                    docs.extend(ddg_results)
                    logger.info(f"DDG search for '{query}': found {len(ddg_results)} results")
            except Exception as e:
                logger.warning(f"DDG search failed: {e}")

        # 2. 尝试百度搜索
        if 'baidu' in sources:
            try:
                baidu_results = await self._baidu_search(query, max_results)
                if baidu_results:
                    docs.extend(baidu_results)
                    logger.info(f"Baidu search for '{query}': found {len(baidu_results)} results")
            except ConnectionError as e:
                logger.warning(f"Baidu blocked/captcha: {e}")
            except Exception as e:
                logger.warning(f"Baidu search failed: {e}")

        # 2. 如果结果不足，尝试搜狗
        if 'sogou' in sources and len(docs) < max_results:
            remaining = max_results - len(docs)
            try:
                sogou_results = await self._sogou_search(query, remaining)
                if sogou_results:
                    docs.extend(sogou_results)
                    logger.info(f"Sogou search for '{query}': found {len(sogou_results)} results")
            except Exception as e:
                logger.warning(f"Sogou search failed: {e}")

        # 3. 记录失败并更新告警状态
        if not docs:
            SpiderSwarm._consecutive_failures += 1
            logger.warning(f"Search failed for '{query}', consecutive failures: {SpiderSwarm._consecutive_failures}")

            if SpiderSwarm._consecutive_failures >= SpiderSwarm._failure_threshold:
                SpiderSwarm._alarm_mode = True
                SpiderSwarm._alarm_start_time = time.time()
                logger.warning("⚠️ Search engine failure threshold reached - entering alarm mode")
                logger.warning("⚠️ Will return empty results for 60s until search succeeds")
        else:
            # 搜索成功，重置失败计数
            if SpiderSwarm._consecutive_failures > 0 or SpiderSwarm._alarm_mode:
                logger.info("Search succeeded - resetting failure counter and exiting alarm mode")
            SpiderSwarm._consecutive_failures = 0
            SpiderSwarm._alarm_mode = False

        # 去重
        seen_urls = set()
        unique_docs = []
        for doc in docs:
            if doc.url not in seen_urls:
                seen_urls.add(doc.url)
                unique_docs.append(doc)

        return unique_docs[:max_results]

    async def _bing_search(self, query: str, max_results: int) -> List[Doc]:
        """Bing搜索"""
        import httpx

        url = f"https://www.bing.com/search?q={query}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        }

        try:
            # 复用类实例的HTTP客户端
            resp = await self.client.get(url, headers=headers)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            results = soup.find_all('li', class_='b_algo')

            docs = []
            for result in results[:max_results]:
                try:
                    title_elem = result.find('h2')
                    if title_elem:
                        title_link = title_elem.find('a')
                        if title_link:
                            title = title_link.get_text(strip=True)
                            link_url = title_link.get('href', '')

                            desc_elem = result.find('div', class_='b_caption')
                            snippet = ""
                            if desc_elem:
                                p_elem = desc_elem.find('p')
                                if p_elem:
                                    snippet = p_elem.get_text(strip=True)

                            if link_url and link_url.startswith('http'):
                                doc = Doc(
                                    id=generate_id('doc'),
                                    title=title,
                                    content=snippet,
                                    url=link_url,
                                    source='bing',
                                    publish_time=datetime.now(),
                                    sector=self._infer_sector(query),
                                    keywords=[query],
                                )
                                docs.append(doc)
                except Exception as e:
                    logger.debug(f"Failed to parse Bing result: {e}")
                    continue

            return docs

        except httpx.ConnectError as e:
            logger.warning(f"Bing connection failed (DNS/network): {e}")
            return []
        except httpx.TimeoutException:
            logger.warning(f"Bing search timeout for: {query}")
            return []
        except OSError as e:
            # DNS errors, socket errors, etc.
            logger.warning(f"Bing network error: {e}")
            return []
        except Exception as e:
            logger.warning(f"Bing search failed: {e}")
            return []

    async def _baidu_search(self, query: str, max_results: int) -> List[Doc]:
        """百度搜索"""
        try:
            import httpx

            # 百度搜索
            url = f"https://www.baidu.com/s?wd={query}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Referer': 'https://www.baidu.com/',
            }

            # 复用类实例的HTTP客户端
            resp = await self.client.get(url, headers=headers, follow_redirects=True)
            resp.encoding = 'utf-8'

            # 检查是否被重定向到验证码页面
            if 'wappass.baidu.com' in str(resp.url) or 'captcha' in resp.text[:500].lower():
                logger.warning(f"Baidu blocked by captcha for query: {query}")
                raise ConnectionError("Baidu captcha/blocked")

            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # 检查页面是否有效（包含搜索结果容器）
            if not soup.find('div', {'c-container'}) and not soup.find('div', class_='result'):
                if '验证' in resp.text[:1000] or '验证码' in resp.text[:1000]:
                    logger.warning(f"Baidu verification required")
                    raise ConnectionError("Baidu verification required")

            # 百度搜索结果结构
            docs = []

            # 方法1: 查找通用的结果容器
            results = soup.find_all('div', class_='result')
            if not results:
                results = soup.find_all('div', {'c-container'})

            for result in results[:max_results]:
                try:
                    # 标题
                    title_elem = result.find('h3') or result.find('a')
                    if title_elem:
                        title_link = title_elem if title_elem.name == 'a' else title_elem.find('a')
                        if title_link:
                            title = title_link.get_text(strip=True)
                            link_url = title_link.get('href', '')

                            # 摘要
                            content_elem = result.find('div', class_='c-abstract') or result.find('span', class_='c-gap-right')
                            snippet = ""
                            if content_elem:
                                snippet = content_elem.get_text(strip=True)
                            else:
                                # 尝试其他方式获取摘要
                                all_text = result.get_text(separator=' ', strip=True)
                                if len(all_text) > 50:
                                    snippet = all_text[:200]

                            if link_url and (link_url.startswith('http') or link_url.startswith('/')):
                                if link_url.startswith('/'):
                                    link_url = 'https://www.baidu.com' + link_url

                                doc = Doc(
                                    id=generate_id('doc'),
                                    title=title,
                                    content=snippet,
                                    url=link_url,
                                    source='baidu',
                                    publish_time=datetime.now(),
                                    sector=self._infer_sector(query),
                                    keywords=[query],
                                )
                                docs.append(doc)
                except Exception as e:
                    logger.debug(f"Failed to parse Baidu result: {e}")
                    continue

            # 如果解析失败，尝试直接获取链接
            if not docs:
                all_links = soup.find_all('a', href=True)
                for link in all_links[:max_results * 2]:
                    href = link.get('href', '')
                    if href.startswith('http') and 'baidu.com' not in href:
                        title = link.get_text(strip=True)
                        if title and len(title) > 5:
                            doc = Doc(
                                id=generate_id('doc'),
                                title=title,
                                content=f"关于{query}的搜索结果",
                                url=href,
                                source='baidu',
                                publish_time=datetime.now(),
                                sector=self._infer_sector(query),
                                keywords=[query],
                            )
                            if doc not in docs:
                                docs.append(doc)
                            if len(docs) >= max_results:
                                break

            return docs[:max_results]

        except httpx.TimeoutException:
            logger.warning(f"Baidu search timeout for: {query}")
            raise  # 重新抛出，让上层重试
        except ConnectionError as e:
            # 验证码或被封禁，可重试
            logger.warning(f"Baidu connection error (captcha/blocked): {e}")
            raise
        except Exception as e:
            logger.error(f"Baidu search failed: {e}")
            raise

    async def _sogou_search(self, query: str, max_results: int) -> List[Doc]:
        """搜狗搜索（备用搜索引擎）"""
        try:
            import httpx

            # 搜狗搜索
            url = f"https://www.sogou.com/web?query={query}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Referer': 'https://www.sogou.com/',
            }

            resp = await self.client.get(url, headers=headers, follow_redirects=True)
            resp.encoding = 'utf-8'
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # 搜狗搜索结果结构
            docs = []

            # 查找结果容器
            results = soup.find_all('div', class_='vrwrap')
            if not results:
                results = soup.find_all('div', class_='rb')

            for result in results[:max_results]:
                try:
                    # 标题和链接
                    title_elem = result.find('h3') or result.find('a')
                    if title_elem:
                        title_link = title_elem if title_elem.name == 'a' else title_elem.find('a')
                        if title_link:
                            title = title_link.get_text(strip=True)
                            link_url = title_link.get('href', '')

                            # 处理相对链接
                            if link_url.startswith('/'):
                                link_url = 'https://www.sogou.com' + link_url
                            elif link_url.startswith('?'):
                                link_url = 'https://www.sogou.com/web' + link_url

                            # 摘要
                            snippet_elem = result.find('p', class_='space-txt') or result.find('div', class_='str_text')
                            snippet = ""
                            if snippet_elem:
                                snippet = snippet_elem.get_text(strip=True)
                            else:
                                all_text = result.get_text(separator=' ', strip=True)
                                if len(all_text) > 50:
                                    snippet = all_text[:200]

                            if link_url and (link_url.startswith('http')):
                                doc = Doc(
                                    id=generate_id('doc'),
                                    title=title,
                                    content=snippet,
                                    url=link_url,
                                    source='sogou',
                                    publish_time=datetime.now(),
                                    sector=self._infer_sector(query),
                                    keywords=[query],
                                )
                                docs.append(doc)
                except Exception as e:
                    logger.debug(f"Failed to parse Sogou result: {e}")
                    continue

            return docs[:max_results]

        except Exception as e:
            logger.warning(f"Sogou search failed: {e}")
            return []

    async def _ddg_search(self, query: str, max_results: int) -> List[Doc]:
        """DuckDuckGO 搜索（使用 ddgs 库 + 代理）"""
        try:
            from ddgs import DDGS

            # 使用代理
            ddgs = DDGS(proxy="http://127.0.0.1:7890", timeout=15)
            results = ddgs.text(query, max_results=max_results)

            docs = []
            for r in results:
                try:
                    title = r.get('title', '')
                    url = r.get('href', '')
                    body = r.get('body', '')

                    if title and url and url.startswith('http'):
                        doc = Doc(
                            id=generate_id('doc'),
                            title=title,
                            content=body or f"关于{query}的搜索结果",
                            url=url,
                            source='duckduckgo',
                            publish_time=datetime.now(),
                            sector=self._infer_sector(query),
                            keywords=[query],
                        )
                        docs.append(doc)
                except Exception as e:
                    logger.debug(f"Failed to parse DDG result: {e}")
                    continue

            return docs[:max_results]

        except Exception as e:
            logger.warning(f"DuckDuckGO search failed: {e}")
            return []

    def _generate_fallback_docs(self, query: str, max_results: int) -> List[Doc]:
        """当所有搜索引擎失败时，生成基于主题的备用文档"""
        docs = []

        # 基于查询词生成主题相关的文档
        topics = [
            f"{query}行业分析",
            f"{query}投资策略",
            f"{query}市场趋势",
            f"{query}产业链分析",
            f"{query}龙头企业",
        ]

        for i, topic in enumerate(topics[:max_results]):
            doc = Doc(
                id=generate_id('doc'),
                title=topic,
                content=f"关于{topic}的深度分析报告。当前市场环境下，{query}领域面临新的发展机遇。",
                url=f"fallback://{query}/{i}",
                source='fallback',
                publish_time=datetime.now(),
                sector=self._infer_sector(query),
                keywords=[query],
            )
            docs.append(doc)

        logger.info(f"Generated {len(docs)} fallback docs for query: {query}")
        return docs

    async def deep_fetch(self, url: str, extract_full_text: bool = True) -> Optional[Doc]:
        """
        深度抓取单个URL的完整内容

        Args:
            url: 目标URL
            extract_full_text: 是否提取完整文本（使用readability）

        Returns:
            文档对象或None
        """
        # 速率限制
        wait_time = self.rate_limiter.acquire()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        async with self.semaphore:
            try:
                headers = {
                    'User-Agent': self.user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                }

                resp = await self.client.get(url, headers=headers, follow_redirects=True)
                resp.raise_for_status()

                # 检测编码
                resp.encoding = resp.apparent_encoding or 'utf-8'

                if extract_full_text:
                    # 使用readability提取正文
                    doc = Document(resp.text)
                    content = BeautifulSoup(doc.summary(), 'html.parser').get_text(separator='\n')
                    content = clean_text(content)

                    title = doc.title() or url
                else:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    title = soup.title().get_text(strip=True) if soup.title() else url
                    content = soup.get_text(separator='\n')
                    content = clean_text(content)

                # 提取股票代码
                tickers = extract_tickers(title + ' ' + content)

                sector = self._infer_sector(content)

                return Doc(
                    id=generate_id('doc'),
                    title=title[:500],  # 限制标题长度
                    content=content[:10000],  # 限制内容长度
                    url=url,
                    source=self._extract_domain(url),
                    publish_time=datetime.now(),
                    sector=sector,
                    keywords=tickers,
                )

            except httpx.TimeoutException:
                logger.warning(f"Timeout fetching {url}")
                return None
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error {e.response.status_code} for {url}")
                return None
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return None

    async def parallel_fetch(
        self,
        urls: List[str],
        extract_full_text: bool = True,
        max_concurrent: int = None,
    ) -> List[Doc]:
        """
        并行抓取多个URL

        Args:
            urls: URL列表
            extract_full_text: 是否提取完整文本
            max_concurrent: 最大并发数

        Returns:
            文档列表
        """
        if not urls:
            return []

        max_concurrent = min(max_concurrent or 10, len(urls))

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(url: str) -> Doc:
            async with semaphore:
                return await self.deep_fetch(url, extract_full_text)

        tasks = [fetch_with_semaphore(url) for url in urls]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        docs = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Fetch failed: {result}")
                self.fail_count.increment()
            elif result:
                docs.append(result)
                self.success_count.increment()

        return docs

    def _infer_sector(self, text: str) -> str:
        """从文本推断行业分类"""
        text_lower = text.lower()

        sector_keywords = {
            "TMT": ["technology", "tech", "ai", "人工智能", "芯片", "半导体", "software", "internet", "互联网", "云计算"],
            "消费": ["consumer", "消费", "retail", "零售", "food", "餐饮", "beverage", "饮料", "医药", "医药", "healthcare"],
            "医药": ["pharma", "医药", "medical", "医疗", "biotech", "生物"],
            "周期": ["commodity", "大宗", "steel", "钢铁", "煤炭", "有色", "化工", "energy", "能源"],
            "制造": ["manufacturing", "制造", "industrial", "工业", "汽车", "新能源车"],
            "金融": ["finance", "金融", "bank", "银行", "保险", "券商", "securities"],
            "地产": ["real estate", "地产", "property", "房地产", "housing", "房产"],
        }

        for sector, keywords in sector_keywords.items():
            for kw in keywords:
                if kw in text_lower:
                    return sector

        return "其他"

    def _extract_domain(self, url: str) -> str:
        """提取域名"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '')
        except:
            return "unknown"

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'success': self.success_count.value,
            'fail': self.fail_count.value,
            'total': self.success_count.value + self.fail_count.value,
            'success_rate': f"{self.success_count.value / max(1, self.success_count.value + self.fail_count.value):.2%}",
        }

    def reset_stats(self):
        """重置统计"""
        self.success_count.reset()
        self.fail_count.reset()

    def __repr__(self):
        return f"SpiderSwarm(concurrent={self.max_concurrent}, timeout={self.timeout}s)"


# ============================================================================
# 搜索查询生成器
# ============================================================================

class SearchQueryGenerator:
    """搜索查询生成器"""

    # 默认种子词
    DEFAULT_SEEDS = {
        "macro": ["美联储议息", "央行政策", "经济数据", "CPI", "PPI", "社融"],
        "sector": ["AI芯片", "新能源汽车", "光伏", "医药创新", "消费升级"],
        "stocks": ["宁德时代", "比亚迪", "茅台", "英伟达", "特斯拉"],
    }

    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    async def generate_queries(
        self,
        count: int = 50,
        seeds: Dict[str, List[str]] = None,
        _retry_count: int = 0,
    ) -> List[str]:
        """生成搜索查询词

        Args:
            count: 要生成的查询词数量
            seeds: 种子词字典
            _retry_count: 内部重试计数器，防止无限递归
        """
        MAX_RETRIES = 3  # 最大重试次数
        seeds = seeds or self.DEFAULT_SEEDS

        prompt = f"""
基于以下种子词，生成{count}个具体的搜索引擎查询词，用于发现当前的投资机会。

【种子词】
宏观：{', '.join(seeds.get('macro', []))}
行业：{', '.join(seeds.get('sector', []))}
个股：{', '.join(seeds.get('stocks', []))}

【要求】
1. 覆盖宏观、行业、个股三个层面
2. 包含异动、政策、技术突破、财务业绩等关键词
3. 中英文混合
4. 每个词3-8个字或单词
5. 精确描述，便于搜索

【输出格式】
仅返回JSON数组：
["查询词1", "查询词2", ...]
"""

        try:
            response = await self.llm.chat(
                system="你是信息搜索专家，擅长发现投资机会",
                user=prompt,
                temperature=0.8,
                max_tokens=2000,
            )

            # 安全解析JSON，增加空值检查
            if not response or not isinstance(response, str):
                logger.warning(f"LLM returned non-string response: {type(response)}")
                raise ValueError(f"Empty or invalid response from LLM: {type(response)}")

            # 清理响应
            response = response.strip()
            # 检查截断响应
            if "输出被截断" in response or "缩短输入" in response:
                logger.warning(f"LLM response truncated, reducing count")
                # 减少数量重试，但不超过最大重试次数
                if _retry_count < MAX_RETRIES:
                    return await self.generate_queries(
                        count=max(5, count // 2),
                        seeds=seeds,
                        _retry_count=_retry_count + 1
                    )
                else:
                    logger.warning(f"Max retries reached for query generation, using fallback")

            if not response or response == "null" or response == "None" or response == "[]":
                raise ValueError(f"Empty response after strip: '{response[:100]}'")

            # 解析JSON，增加更宽松的处理
            queries = safe_parse_json(response, [])

            # 如果解析失败，尝试直接用 eval 或 ast.literal_eval
            if not queries:
                try:
                    import ast
                    queries = ast.literal_eval(response)
                    if not isinstance(queries, list):
                        queries = []
                except:
                    pass

            # 如果还是失败，尝试提取所有引号内的内容
            if not queries:
                import re
                # 提取引号内的中文或英文词
                matches = re.findall(r'"([^"]+)"', response)
                if matches:
                    queries = matches[:30]

            # 调试：记录原始响应
            if not queries:
                logger.warning(f"Failed to parse queries. Raw response (first 200 chars): {response[:200]}")
                raise ValueError("Failed to parse queries from response")

            if isinstance(queries, list) and len(queries) >= 1:
                logger.info(f"Generated {len(queries)} queries successfully")
                return queries[:count]

            raise ValueError(f"Invalid queries result: {type(queries)}")

        except Exception as e:
            logger.error(f"Failed to generate queries: {e}")

        # 降级：返回种子词的变体，确保不为空
        try:
            all_seeds = []
            for category in seeds.values():
                if category:
                    all_seeds.extend(category)
            return all_seeds[:count] if all_seeds else ["投资机会", "A股市场", "股票推荐"]
        except:
            return ["投资机会", "A股市场", "股票推荐"]

    def get_default_seeds(self) -> Dict[str, List[str]]:
        """获取默认种子词"""
        return self.DEFAULT_SEEDS.copy()