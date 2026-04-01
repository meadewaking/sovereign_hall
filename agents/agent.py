"""
🏛️ Sovereign Hall - Agent Base Class
智能体基类
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..core import AgentRole, Document, InvestmentProposal, DATA_DIR
from ..services.llm_client import LLMClient
from . import AgentPersona, get_persona
from ..utils import (
    estimate_tokens,
    truncate_text,
    safe_parse_json,
)

logger = logging.getLogger(__name__)


class Agent:
    """智能体基类"""

    def __init__(
        self,
        role: AgentRole,
        llm_client: LLMClient,
        memory_limit: int = 20,
        spider_service=None,
    ):
        """
        初始化智能体

        Args:
            role: 角色
            llm_client: LLM客户端
            memory_limit: 对话历史限制
            spider_service: 爬虫服务（可选，用于搜索）
        """
        self.role = role
        self.llm = llm_client
        self.memory_limit = memory_limit
        self.spider = spider_service

        # 获取人格
        self.persona = get_persona(role)

        # 议题级别的记忆：按议题隔离，防止跨议题污染
        # 结构: {topic: List[Dict[str, str]]}
        self._topic_memories: Dict[str, List[Dict[str, str]]] = {}
        self._current_topic: Optional[str] = None

        # 当前议题的记忆引用
        self.memory: List[Dict[str, str]] = []

        # 统计数据
        self.stats = {
            'total_thinks': 0,
            'total_tokens': 0,
        }

        logger.info(f"Agent initialized: {self.persona.name} ({role.value})")

    def set_topic(self, topic: str):
        """
        切换到指定议题的记忆上下文

        Args:
            topic: 议题名称
        """
        if topic != self._current_topic:
            self._current_topic = topic
            # 获取或创建该议题的记忆
            if topic not in self._topic_memories:
                self._topic_memories[topic] = []
            self.memory = self._topic_memories[topic]
            logger.info(f"Agent {self.persona.name} switched to topic: {topic}")

    def clear_memory(self):
        """清空当前议题的记忆（仅清空当前议题，不影响其他议题）"""
        if self._current_topic and self._current_topic in self._topic_memories:
            self._topic_memories[self._current_topic].clear()
            logger.info(f"Agent {self.persona.name} cleared memory for topic: {self._current_topic}")
        self.memory = []

    def clear_all_memories(self):
        """清空所有议题的记忆"""
        self._topic_memories.clear()
        self.memory = []
        self._current_topic = None
        logger.info(f"Agent {self.persona.name} cleared all memories")

    def get_memory_count(self) -> int:
        """获取当前议题的记忆数量"""
        return len(self.memory)

    async def think(
        self,
        task: str,
        context: str = "",
        temperature: float = None,
        max_tokens: int = None,
        use_memory: bool = True,
        additional_rules: List[str] = None,
    ) -> str:
        """
        深度思考

        Args:
            task: 任务描述
            context: 背景信息
            temperature: 温度
            max_tokens: 最大输出token
            use_memory: 是否使用记忆
            additional_rules: 额外规则

        Returns:
            思考结果
        """
        self.stats['total_thinks'] += 1

        # 确定温度和token限制
        if temperature is None:
            temperature = self._get_default_temperature()
        if max_tokens is None:
            max_tokens = self._get_default_max_tokens()

        # 构建系统提示词
        system_prompt = self.persona.get_system_prompt(
            task_context=context,
            additional_rules=additional_rules
        )

        # 构建用户消息（包含历史记忆）
        user_message = self._build_user_message(task)

        # 调用LLM
        try:
            response = await self.llm.chat(
                system=system_prompt,
                user=user_message,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            # 记录到记忆
            if use_memory:
                self._add_to_memory(task, response)

            # 统计
            self.stats['total_tokens'] += estimate_tokens(response)

            return response

        except Exception as e:
            logger.error(f"Agent {self.persona.name} think failed: {e}")
            return f"思考失败：{str(e)}"

    async def batch_think(
        self,
        tasks: List[Dict[str, Any]],
    ) -> List[str]:
        """
        批量思考

        Args:
            tasks: 任务列表，每个包含 task, context, temperature, max_tokens

        Returns:
            响应列表
        """
        if not tasks:
            return []

        # 准备并行请求
        requests = []
        for task_dict in tasks:
            system_prompt = self.persona.get_system_prompt(
                task_context=task_dict.get("context", ""),
                additional_rules=task_dict.get("additional_rules")
            )

            user_message = self._build_user_message(task_dict.get("task", ""))

            requests.append({
                "system": system_prompt,
                "user": user_message,
                "temperature": task_dict.get("temperature", self._get_default_temperature()),
                "max_tokens": task_dict.get("max_tokens", self._get_default_max_tokens()),
            })

        # 并发执行
        responses = await self.llm.parallel_chat(requests)

        # 记录到记忆（只记录第一个任务的摘要）
        if requests:
            self._add_to_memory(str(tasks[0].get("task", ""))[:100], responses[0] if responses else "")

        return responses

    async def search_and_analyze(
        self,
        query: str,
        task_context: str = "",
        max_results: int = 3,
    ) -> str:
        """
        搜索相关信息并进行分析

        Args:
            query: 搜索关键词
            task_context: 任务背景
            max_results: 最大搜索结果数

        Returns:
            分析结果
        """
        if not self.spider:
            return "无可用搜索服务"

        try:
            # 执行搜索
            docs = await self.spider.aggressive_search(
                queries=[query],
                max_results_per_query=max_results,
            )

            if not docs:
                return f"未找到关于'{query}'的搜索结果"

            # 提取文档内容
            content_parts = []
            for doc in docs[:max_results]:
                title = getattr(doc, 'title', '') or ''
                content = getattr(doc, 'content', '') or ''
                if content and len(content) > 50:
                    content_parts.append(f"【{title}】{content[:500]}")

            if not content_parts:
                return f"关于'{query}'的搜索结果内容不足"

            # 用LLM分析
            search_context = "\n\n".join(content_parts)

            prompt = f"""作为{self.persona.name}，请基于以下搜索结果进行分析：

【搜索主题】
{query}

【搜索结果】
{search_context}

【分析任务】
{task_context}

请结合搜索结果给出专业分析，输出1000字左右的深度观点。
"""

            response = await self.llm.chat(
                system=self.persona.get_system_prompt(),
                user=prompt,
                temperature=0.7,
                max_tokens=5000,
            )

            return response

        except Exception as e:
            logger.error(f"Agent search failed: {e}")
            return f"搜索分析失败：{str(e)}"

    async def think_with_search(
        self,
        task: str,
        search_queries: List[str],
        context: str = "",
        temperature: float = None,
        max_tokens: int = None,
    ) -> str:
        """
        搜索相关信息后再思考（自动执行搜索 + 分析）

        Args:
            task: 任务描述
            search_queries: 搜索关键词列表
            context: 背景信息
            temperature: 温度
            max_tokens: 最大输出token

        Returns:
            思考结果
        """
        if not self.spider:
            # 无搜索服务，降级为普通思考
            return await self.think(task, context, temperature, max_tokens)

        try:
            # 合并多个搜索查询的结果
            all_docs = []
            for query in search_queries[:3]:  # 最多搜索3个关键词
                docs = await self.spider.aggressive_search(
                    queries=[query],
                    max_results_per_query=2,
                )
                all_docs.extend(docs)

            # 去重
            seen_urls = set()
            unique_docs = []
            for doc in all_docs:
                url = getattr(doc, 'url', '')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_docs.append(doc)

            # 提取内容
            content_parts = []
            for doc in unique_docs[:6]:  # 最多用6个文档
                title = getattr(doc, 'title', '') or ''
                content = getattr(doc, 'content', '') or ''
                source = getattr(doc, 'source', '') or ''
                if len(content) > 100:
                    content_parts.append(f"【来源:{source}】{title}\n{content[:800]}")

            if not content_parts:
                # 无搜索结果，降级为普通思考
                return await self.think(task, context, temperature, max_tokens)

            search_context = "\n\n".join(content_parts)

            # 构建完整prompt
            full_prompt = f"""【背景信息】
{context}

【任务】
{task}

【最新搜索信息】
{search_context}

请结合以上背景、任务和最新搜索信息，给出专业的投资分析观点。"""

            return await self.think(
                full_prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                use_memory=False,  # 搜索任务不记记忆
            )

        except Exception as e:
            logger.error(f"Agent think_with_search failed: {e}")
            # 降级为普通思考
            return await self.think(task, context, temperature, max_tokens)

    async def research_sector(
        self,
        sector: str,
        documents: List[Document],
        max_output_tokens: int = 8000,
    ) -> str:
        """
        研究特定行业

        Args:
            sector: 行业名称
            documents: 相关文档
            max_output_tokens: 最大输出token

        Returns:
            深度研究报告
        """
        # 构建上下文
        context_parts = []
        for i, doc in enumerate(documents[:20]):  # 限制数量
            context_parts.append(
                f"【文档{i+1}】{doc.title}\n"
                f"来源：{doc.source}\n"
                f"内容：{truncate_text(doc.content, 500)}"
            )
        context = "\n\n".join(context_parts)

        task = f"""
基于以下{len(documents)}篇最新资料，撰写一份关于【{sector}】的深度行业分析。

【资料】
{context}

【要求】
1. 分析技术路线和竞争格局
2. 识别3-5个值得关注的投资标的
3. 给出投资建议和风险提示
4. 5000字以上
"""

        return await self.think(
            task=task,
            context="撰写深度行业研究报告",
            temperature=0.6,
            max_tokens=max_output_tokens,
            use_memory=False,
        )

    def _build_user_message(self, task: str) -> str:
        """构建用户消息"""
        messages = []

        # 添加任务
        messages.append(f"【任务】\n{task}")

        # 添加历史记忆
        if self.memory:
            memory_text = "\n【历史对话】\n" + self._format_memory()
            messages.append(memory_text)

        return "\n\n".join(messages)

    def _format_memory(self) -> str:
        """格式化记忆"""
        if not self.memory:
            return "暂无历史对话"

        recent_memory = self.memory[-10:]  # 只保留最近10条
        lines = []
        for i, entry in enumerate(recent_memory):
            lines.append(f"{i+1}. {entry['task'][:50]}...")
        return "\n".join(lines)

    def _add_to_memory(self, task: str, response: str):
        """添加记忆到当前议题"""
        # 确保当前有议题上下文
        if self._current_topic is None:
            logger.warning(f"Agent {self.persona.name}: No topic set, using default")
            self.set_topic("default")

        self.memory.append({
            "task": task[:100],
            "response": response[:200],
        })
        # 限制记忆长度
        if len(self.memory) > self.memory_limit:
            self.memory = self.memory[-self.memory_limit:]

    def _save_memory(self):
        """保存记忆 - 内存中已保存，无需磁盘操作"""
        pass

    def _load_memory(self):
        """加载记忆 - 现在按议题管理，不需要从磁盘加载"""
        pass

    def _get_default_temperature(self) -> float:
        """获取默认温度"""
        return 0.7

    def _get_default_max_tokens(self) -> int:
        """获取默认最大输出token"""
        return 4000

    def get_stats(self) -> Dict:
        """获取统计数据"""
        return {
            'role': self.role.value,
            'name': self.persona.name,
            'total_thinks': self.stats['total_thinks'],
            'total_tokens': self.stats['total_tokens'],
            'memory_size': len(self.memory),
        }

    def clear_memory(self):
        """清空记忆"""
        self.memory.clear()
        logger.info(f"Agent {self.persona.name} memory cleared")

    def __repr__(self):
        return f"Agent({self.persona.name}, {self.role.value})"


class AnalystTeam:
    """分析师团队"""

    def __init__(self, llm_client: LLMClient, enabled_roles: List[str] = None):
        """
        初始化分析师团队

        Args:
            llm_client: LLM客户端
            enabled_roles: 启用的角色列表
        """
        self.llm = llm_client
        self.agents: Dict[AgentRole, Agent] = {}

        # 初始化默认角色
        default_roles = [
            AgentRole.TMT_ANALYST,
            AgentRole.CONSUMER_ANALYST,
            AgentRole.CYCLE_ANALYST,
            AgentRole.MACRO_STRATEGIST,
        ]

        enabled = enabled_roles or [r.value for r in default_roles]

        for role in default_roles:
            if role.value in enabled:
                self.agents[role] = Agent(role, llm_client)

        logger.info(f"Analyst team initialized with {len(self.agents)} analysts")

    def get_agent(self, role: AgentRole) -> Optional[Agent]:
        """获取分析师"""
        return self.agents.get(role)

    def get_specialist(self, sector: str) -> Agent:
        """根据行业获取专业分析师"""
        from ..core.config import get_config
        config = get_config()

        sector_mapping = config.get('analysts.sector_mapping', {})
        role_name = sector_mapping.get(sector, 'tmt_analyst')

        # 查找对应的角色
        for role, agent in self.agents.items():
            if role.value == role_name:
                return agent

        # 默认返回TMT分析师
        return self.agents.get(AgentRole.TMT_ANALYST)

    def get_all_stats(self) -> List[Dict]:
        """获取所有分析师统计"""
        return [agent.get_stats() for agent in self.agents.values()]

    def __len__(self):
        return len(self.agents)

    def __getitem__(self, role: AgentRole) -> Agent:
        return self.agents[role]