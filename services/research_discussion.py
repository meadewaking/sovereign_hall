#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - Research Discussion System
研究讨论系统 - 基于数据库内容和搜索的智能问答

功能：
1. 从数据库检索相关历史数据（文档、提案、会议记录、投资手册）
2. 通过搜索引擎补充最新信息
3. 多智能体讨论辩论
4. 生成最终结论

用法：
python -m sovereign_hall.services.research_discussion "你对A股市场近期走势怎么看？"
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sovereign_hall.core import AgentRole, Document, InvestmentProposal
from sovereign_hall.agents import AgentPersona, get_persona
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SpiderSwarm
from sovereign_hall.services.vector_db import VectorDatabase
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.utils import extract_actual_response, truncate_text, format_token

# 设置日志
logger = logging.getLogger(__name__)

# 延迟导入，避免循环导入问题
Agent = None
def _get_agent():
    global Agent
    if Agent is None:
        from sovereign_hall.agents.agent import Agent
    return Agent


@dataclass
class ResearchContext:
    """研究上下文"""
    question: str
    relevant_docs: List[Dict] = field(default_factory=list)
    relevant_proposals: List[Dict] = field(default_factory=list)
    relevant_playbook: List[Dict] = field(default_factory=list)
    search_results: List[Document] = field(default_factory=list)
    discussion_history: List[str] = field(default_factory=list)
    final_conclusion: str = ""


class ResearchDiscussionSystem:
    """研究讨论系统"""

    def __init__(self, enable_search: bool = True, enable_web: bool = False):
        self.db_service = None  # 延迟初始化
        self.llm = LLMClient(max_concurrent=16)
        self.spider = SpiderSwarm(max_concurrent=30)
        self.vector_db = VectorDatabase()

        # 延迟导入 Agent，避免循环导入
        AgentCls = _get_agent()

        # 创建所有智能体
        self.agents = {
            AgentRole.TMT_ANALYST: AgentCls(AgentRole.TMT_ANALYST, self.llm),
            AgentRole.CONSUMER_ANALYST: AgentCls(AgentRole.CONSUMER_ANALYST, self.llm),
            AgentRole.CYCLE_ANALYST: AgentCls(AgentRole.CYCLE_ANALYST, self.llm),
            AgentRole.MACRO_STRATEGIST: AgentCls(AgentRole.MACRO_STRATEGIST, self.llm),
            AgentRole.RISK_OFFICER: AgentCls(AgentRole.RISK_OFFICER, self.llm),
            AgentRole.QUANT_RESEARCHER: AgentCls(AgentRole.QUANT_RESEARCHER, self.llm),
        }
        self.cio = AgentCls(AgentRole.CIO, self.llm)

        self.enable_search = enable_search
        self.enable_web = enable_web

    async def _get_db(self) -> DatabaseService:
        """获取数据库服务（延迟初始化）"""
        if self.db_service is None:
            self.db_service = await DatabaseService.get_instance()
            # 初始化向量数据库
            await self.vector_db.initialize(self.llm)
        return self.db_service

    async def research(self, question: str) -> ResearchContext:
        """研究问题"""
        print(f"\n{'='*70}")
        print(f"🔍 研究问题: {question}")
        print(f"{'='*70}\n")

        context = ResearchContext(question=question)

        # 反思历史结论
        reflection_context = await self._reflect_on_history(question)
        if reflection_context:
            context.discussion_history.append(f"【历史反思】\n{reflection_context}")
            print(f"   📜 已完成对最近5次结论的反思")

        # 多智能体深度讨论
        print("🗣️  步骤1: 多智能体深度讨论...")
        await self._run_extended_discussion(context)

        # 生成最终结论
        print("\n⚖️  步骤2: 生成最终结论...")
        context.final_conclusion = await self._generate_conclusion(context)

        # 保存结论到数据库
        print("\n💾 步骤3: 保存结论到数据库...")
        await self._save_conclusion_to_db(question, context.final_conclusion)

        return context

    async def _reflect_on_history(self, question: str) -> str:
        """反思最近的历史结论"""
        db = await self._get_db()
        recent_conclusions = await db.get_recent_conclusions(limit=5)
        if not recent_conclusions:
            return ""

        print(f"   检索到 {len(recent_conclusions)} 条历史结论")

        history_text = "\n\n".join([
            f"【历史结论 {i+1}】(时间: {c.get('created_at', '')[:19]})\n结论: {c.get('conclusion', '')[:300]}..."
            for i, c in enumerate(recent_conclusions[:3])
        ])

        verification_results = ""
        if self.enable_search and recent_conclusions:
            keywords = []
            for c in recent_conclusions[:3]:
                ticker = c.get('ticker', '')
                if ticker:
                    keywords.append(ticker)
                import re
                tickers = re.findall(r'[A-Z]{2,6}', c.get('conclusion', ''))
                keywords.extend(tickers[:3])

            if keywords:
                search_query = " ".join(set(keywords[:5]))
                try:
                    results = await self.spider.aggressive_search([search_query], max_results_per_query=3)
                    if results:
                        verification_results = "\n".join([
                            f"- {r.title}: {r.content[:200]}..."
                            for r in results[:2]
                        ])
                        print(f"   🔍 验证搜索完成")
                except:
                    pass

        prompt = f"""
基于以下历史结论，请进行反思和验证：

【当前问题】
{question}

【最近5次历史结论】
{history_text}

【关键事实验证结果】
{verification_results if verification_results else "无验证结果"}

请进行反思分析（800字以内）：
1. 这些历史结论的准确性如何？事后验证是否正确？
2. 有什么模式和规律？
3. 有什么教训和经验？
4. 对当前问题有什么启示？

输出反思总结。
"""
        response = await self.cio.think(prompt, max_tokens=15000)
        reflection = extract_actual_response(response)

        db = await self._get_db()
        await db.save_reflection_summary(
            question=question,
            previous_conclusions=history_text,
            reflection_text=reflection,
            verification_results=verification_results
        )

        return reflection

    async def _save_conclusion_to_db(self, question: str, conclusion: str):
        """解析并保存结论到数据库"""
        import re
        ticker_match = re.search(r'(?:标的|代码)[：:]\s*([A-Z0-9]{2,10})', conclusion)
        position_match = re.search(r'仓位[：:]\s*(\d+(?:\.\d+)?%?)', conclusion)
        stop_loss_match = re.search(r'止损[：:]\s*(\d+(?:\.\d+)?%?)', conclusion)
        take_profit_match = re.search(r'止盈[：:]\s*(\d+(?:\.\d+)?%?)', conclusion)
        confidence_match = re.search(r'置信度[：:]\s*(\d+(?:\.\d+)?%?)', conclusion)

        ticker = ticker_match.group(1) if ticker_match else ""
        ticker = ticker.split()[0].strip().upper()
        position = float(re.sub(r'%', '', position_match.group(1))) / 100 if position_match else 0
        stop_loss = float(re.sub(r'%', '', stop_loss_match.group(1))) / 100 if stop_loss_match else 0
        take_profit = float(re.sub(r'%', '', take_profit_match.group(1))) / 100 if take_profit_match else 0
        confidence = float(re.sub(r'%', '', confidence_match.group(1))) / 100 if confidence_match else 0

        db = await self._get_db()
        await db.save_report_conclusion(
            question=question,
            conclusion=conclusion[:3000],
            ticker=ticker,
            position=position,
            stop_loss=stop_loss,
            take_profit=take_profit,
            confidence=confidence
        )
        print(f"   ✓ 结论已保存 (标的: {ticker or 'N/A'})")

        if ticker and confidence > 0 and take_profit > 0 and stop_loss > 0:
            try:
                from .decision_tracker import DecisionRecorder
                direction = "short" if any(word in conclusion for word in ["卖出", "做空", "看空"]) else "long"
                recorder = DecisionRecorder()
                await recorder.record_decision(
                    ticker=ticker,
                    decision=direction,
                    confidence=confidence,
                    target_price=take_profit,
                    stop_loss=stop_loss,
                    discussion_context=conclusion[:1000],
                    expected_days=90 if "半年" in question else 30,
                )
                print("   ✓ 可验证预测已记录")
            except Exception as e:
                logger.warning(f"记录可验证预测失败: {e}")

    async def _agent_think_with_retrieval(self, agent, question: str,
                                          additional_instruction: str = "",
                                          is_risk_officer: bool = False,
                                          is_quant: bool = False,
                                          is_macro: bool = False) -> str:
        """智能体思考（带动态检索）"""
        keywords = self._generate_search_keywords(question, agent.role)
        db_context = await self._build_context_with_db_priority(keywords)

        web_context = ""
        if self.enable_search:
            web_context = await self._search_with_keywords(keywords)

        context_str = f"""
【数据库内容（高权重）】
{db_context}

【网络搜索结果（补充）】
{web_context}
"""

        if is_risk_officer:
            prompt = f"""
{additional_instruction}

【问题】
{question}

【背景资料】
{context_str}

请以资深风控官{agent.persona.name}的身份，提出最严格的风险质疑。要求：
1. 找出每个观点的漏洞和风险点
2. 引用历史暴雷案例
3. 问"如果你错了怎么办"
4. 1500字以上，深入分析
"""
        elif is_quant:
            prompt = f"""
{additional_instruction}

【问题】
{question}

【背景资料】
{context_str}

请以量化研究员{agent.persona.name}的身份，从数据分析角度补充。要求：
1. 技术面信号
2. 资金流向
3. 统计规律
4. 历史相似情况
5. 1500字以上，数据说话
"""
        elif is_macro:
            prompt = f"""
{additional_instruction}

【问题】
{question}

【背景资料】
{context_str}

请以宏观策略师{agent.persona.name}的身份，从宏观角度分析。要求：
1. 美联储货币政策
2. 全球流动性
3. 地缘政治风险
4. 汇率波动
5. 2000字以上
"""
        else:
            prompt = f"""
{additional_instruction}

【问题】
{question}

【背景资料】
{context_str}

请以{agent.persona.name}的身份发表专业分析。要求：
1. 结合你的投资风格和专业背景
2. 引用支撑你观点的证据
3. 给出明确结论
4. 1500字以上

你的投资哲学：{agent.persona.personality}
"""
        response = await agent.think(prompt, max_tokens=15000)
        return extract_actual_response(response)

    def _generate_search_keywords(self, question: str, role: AgentRole) -> List[str]:
        """根据问题和角色生成检索关键词"""
        base_keywords = list(question)

        role_keywords = {
            AgentRole.TMT_ANALYST: ["科技", "AI", "半导体", "芯片", "软件", "互联网", "电子", "通信"],
            AgentRole.CONSUMER_ANALYST: ["消费", "白酒", "医药", "食品", "饮料", "家电", "汽车", "零售"],
            AgentRole.CYCLE_ANALYST: ["周期", "钢铁", "煤炭", "有色", "化工", "能源", "大宗商品"],
            AgentRole.MACRO_STRATEGIST: ["宏观", "美联储", "利率", "流动性", "GDP", "通胀", "汇率"],
            AgentRole.RISK_OFFICER: ["风险", "黑天鹅", "暴雷", "财务造假", "债务", "质押"],
            AgentRole.QUANT_RESEARCHER: ["技术面", "资金流", "估值", "PE", "PB", "成交量"],
        }

        keywords = set(base_keywords)
        if role in role_keywords:
            keywords.update(role_keywords[role])

        return list(keywords)

    async def _build_context_with_db_priority(self, keywords: List[str]) -> str:
        """构建上下文，数据库内容优先"""
        db = await self._get_db()
        parts = []
        keyword_query = ' '.join(keywords[:5]) if keywords else ""

        proposals = await db.get_proposals(limit=10)
        if proposals:
            parts.append("【历史投资提案】")
            for i, prop in enumerate(proposals[:5]):
                parts.append(f"\n提案 {i+1}: {prop.get('ticker', '未知')} ({prop.get('direction', 'N/A')})")
                parts.append(f"   置信度: {prop.get('confidence', 0):.0%}")
                thesis = prop.get('thesis', '')[:3000]
                parts.append(f"   核心逻辑: {thesis}...")
            parts.append("")

        docs = await db.search_documents(query=keyword_query, limit=15)
        if docs:
            parts.append("【历史分析文档】")
            for i, doc in enumerate(docs[:5]):
                parts.append(f"\n文档 {i+1}: {doc.get('title', '无标题')}")
                parts.append(f"   来源: {doc.get('source', '未知')} | 行业: {doc.get('sector', '未知')}")
                content = doc.get('content', '')[:2000]
                parts.append(f"   摘要: {content}...")
            parts.append("")

        blacklist = await db.get_blacklist()
        if blacklist:
            parts.append("【风控黑名单】")
            parts.append(f"以下标的需要特别注意：{', '.join(blacklist[:10])}")

        if not parts:
            return "数据库中暂无相关信息"

        return "\n".join(parts)

    async def _search_with_keywords(self, keywords: List[str]) -> str:
        """搜索网络"""
        query = ' '.join(keywords[:3])
        if not query:
            return ""

        try:
            results = await self.spider.aggressive_search([query], max_results_per_query=5)
            if not results:
                return ""

            parts = ["【网络最新信息】"]
            for i, doc in enumerate(results[:3]):
                parts.append(f"\n{i+1}. {doc.title}")
                parts.append(f"   来源: {doc.source}")
                content = doc.content[:1000]
                parts.append(f"   摘要: {content}...")
            return "\n".join(parts)
        except:
            return ""

    async def _run_extended_discussion(self, context: ResearchContext):
        """运行多智能体讨论"""
        question = context.question

        all_agents = {
            AgentRole.TMT_ANALYST: self.agents[AgentRole.TMT_ANALYST],
            AgentRole.CONSUMER_ANALYST: self.agents[AgentRole.CONSUMER_ANALYST],
            AgentRole.CYCLE_ANALYST: self.agents[AgentRole.CYCLE_ANALYST],
            AgentRole.MACRO_STRATEGIST: self.agents[AgentRole.MACRO_STRATEGIST],
            AgentRole.RISK_OFFICER: self.agents[AgentRole.RISK_OFFICER],
            AgentRole.QUANT_RESEARCHER: self.agents[AgentRole.QUANT_RESEARCHER],
        }

        print(f"   参与讨论: {[a.persona.name for a in all_agents.values()]}")

        perspectives = {}

        # 第一轮
        print("\n  【第一轮】各分析师发表独立观点...")
        analyst_roles = [AgentRole.TMT_ANALYST, AgentRole.CONSUMER_ANALYST, AgentRole.CYCLE_ANALYST]

        for role in analyst_roles:
            agent = all_agents[role]
            response = await self._agent_think_with_retrieval(agent, question)
            perspectives[role] = response
            context.discussion_history.append(f"【第一轮 · {agent.persona.name}】\n{response[:8000]}...")
            print(f"   ✓ {agent.persona.name} 已发表观点")

        # 第二轮
        print("\n  【第二轮】风控官深度质疑...")
        risk_agent = all_agents[AgentRole.RISK_OFFICER]
        all_views = "\n\n".join([f"【{role.value}】\n{v[:8000]}" for role, v in perspectives.items()])

        risk_instruction = f"""
请对上述所有分析师的观点进行最严格的风险审视和质疑。

【各方观点】
{all_views}

要求：
1. 找出每个分析逻辑中的漏洞和盲点
2. 引用历史暴雷案例和市场规律作为反证
3. 提出最坏情况下的风险敞口
4. 如果你是CIO，会要求什么条件才通过这个投资建议？
5. 特别关注：流动性风险、估值风险、基本面风险、黑天鹅事件
"""
        risk_response = await self._agent_think_with_retrieval(
            risk_agent, question, additional_instruction=risk_instruction, is_risk_officer=True
        )
        perspectives[AgentRole.RISK_OFFICER] = risk_response
        context.discussion_history.append(f"【第二轮 · 风控官质疑】\n{risk_response[:8000]}...")
        print(f"   ⚠️ 刘挑刺 提出深度风险质疑")

        # 第三轮
        print("\n  【第三轮】量化视角深度分析...")
        quant_agent = all_agents[AgentRole.QUANT_RESEARCHER]

        quant_instruction = f"""
从纯数据角度对这个问题进行深度分析。

【各方观点】
{all_views}

【风控质疑摘要】
{risk_response[:6000]}...

要求：
1. 技术面信号：趋势、动量、支撑阻力位
2. 资金流向：主力资金、北上资金、融资融券
3. 统计规律：胜率、赔率、夏普比率
4. 历史相似情况的表现统计
5. 如果数据不支持上述分析师的观点，请明确指出
"""
        quant_response = await self._agent_think_with_retrieval(
            quant_agent, question, additional_instruction=quant_instruction, is_quant=True
        )
        perspectives[AgentRole.QUANT_RESEARCHER] = quant_response
        context.discussion_history.append(f"【第三轮 · 量化视角】\n{quant_response[:8000]}...")
        print(f"   📊 钱量化 补充量化分析")

        # 第四轮
        print("\n  【第四轮】宏观策略深度分析...")
        macro_agent = all_agents[AgentRole.MACRO_STRATEGIST]

        macro_instruction = f"""
从宏观角度深度分析这个问题。

要求：
1. 美联储货币政策走向及利率路径
2. 全球流动性环境（QE/QT）
3. 地缘政治风险（中美关系等）
4. 汇率波动及跨境资本流动
5. 全球经济周期定位
6. 通胀预期与实际走势
分析宏观因素对这个问题的影响路径。
"""
        macro_response = await self._agent_think_with_retrieval(
            macro_agent, question, additional_instruction=macro_instruction, is_macro=True
        )
        perspectives[AgentRole.MACRO_STRATEGIST] = macro_response
        context.discussion_history.append(f"【第四轮 · 宏观视角】\n{macro_response[:8000]}...")
        print(f"   🌍 赵宏观 补充宏观分析")

        # 第五轮
        print("\n  【第五轮】分析师回应质疑...")
        for role in analyst_roles:
            agent = all_agents[role]
            other_views = "\n\n".join([
                f"【{r.value}】: {v[:6000]}..."
                for r, v in perspectives.items() if r != role
            ])

            reply_instruction = f"""
请回应其他分析师和风控官的质疑，并坚持或修正你的观点。

【你的原始观点】
{perspectives[role][:6000]}...

【其他观点和质疑】
{other_views}

要求：
1. 逐条回应风控官的质疑，承认合理部分，反驳不合理部分
2. 回应量化分析师的数据质疑
3. 回应宏观策略师的宏观风险担忧
4. 坚持核心逻辑，修正细节判断
5. 如果观点有调整，说明调整原因
"""
            reply_response = await self._agent_think_with_retrieval(agent, question, reply_instruction)
            context.discussion_history.append(f"【第五轮 · {agent.persona.name}回应质疑】\n{reply_response[:8000]}...")
            print(f"   🔄 {agent.persona.name} 回应质疑")

        # 第六轮
        print("\n  【第六轮】综合辩论...")
        all_perspectives_text = "\n\n".join([f"【{r.value}】: {v[:6000]}..." for r, v in perspectives.items()])

        tmt_agent = all_agents[AgentRole.TMT_ANALYST]
        final_instruction = f"""
请做最后的总结陈词。

【完整讨论记录】
{all_perspectives_text}

要求：
1. 综合所有观点，做出最终判断
2. 承认反对意见的合理之处
3. 强调支持你观点的核心逻辑
4. 给出最终投资建议和置信度
"""
        final_response = await self._agent_think_with_retrieval(tmt_agent, question, final_instruction)
        context.discussion_history.append(f"【第六轮 · 总结陈词】\n{final_response[:8000]}...")
        print(f"   🎯 张科技 做总结陈词")

    async def _generate_conclusion(self, context: ResearchContext) -> str:
        """生成最终结论"""
        all_views = "\n\n".join(context.discussion_history)

        db = await self._get_db()
        recent_reflections = await db.get_recent_reflections(limit=2)
        reflection_text = "\n".join([r.get('reflection_text', '')[:500] for r in recent_reflections])

        prompt = f"""
基于多智能体深度讨论，请作为投资总监陈总监给出结论（1500字以内）。

【原始问题】
{context.question}

【历史反思参考】
{reflection_text if reflection_text else "无"}

【讨论摘要】
{all_views[:10000]}...

请输出极简结论：

## 结论
### 核心判断
[一句话：买/卖/观望 + 标的 + 仓位]

### 逻辑（2条）
1. 核心逻辑
2. 关键依据

### 风险
- 1条主要风险

### 操作
标的：XXX | 仓位：XX% | 止损：XX% | 止盈：XX% | 持有：X月

### 置信度
XX%（核心不确定性：XXX）

---
越简洁越好，不超过1500字
"""
        response = await self.cio.think(prompt, max_tokens=5000)
        return extract_actual_response(response, max_length=20000)


async def print_report(context: ResearchContext):
    """打印研究报告"""
    print(f"\n{'#'*70}")
    print(f"# 研究报告")
    print(f"# 问题: {context.question}")
    print(f"{'#'*70}\n")

    print("="*70)
    print("📊 摘要")
    print("="*70)
    db = await DatabaseService.get_instance()
    stats = await db.get_stats_summary()
    recent_conclusions = await db.get_recent_conclusions(limit=5)
    print(f"数据库: {stats.get('documents', 0)} 篇文档, {stats.get('proposals', 0)} 个提案")
    print(f"历史结论: {len(recent_conclusions)} 条")
    print(f"本次讨论轮次: {len(context.discussion_history)} 轮")

    print("\n" + "="*70)
    print("🗣️  讨论过程")
    print("="*70)
    for i, view in enumerate(context.discussion_history, 1):
        print(f"\n【第{i}轮】")
        print(view[:8000] + "..." if len(view) > 8000 else view)

    print("\n" + "="*70)
    print("⚖️  最终结论")
    print("="*70)
    print(context.final_conclusion)

    print("\n" + "="*70)


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="研究讨论系统 - 基于数据库和搜索的智能问答",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m sovereign_hall.services.research_discussion "A股市场近期走势"
  python -m sovereign_hall.services.research_discussion "宁德时代投资价值"
  python -m sovereign_hall.services.research_discussion "美联储加息影响" --no-search
        """
    )
    parser.add_argument("question", nargs="?",
                        default="从现在出发，找出目前a股中一支适合持有三个月到半年的股票，并预估他的止盈止损价格",
                        help="要研究的问题")
    parser.add_argument("--no-search", action="store_true", help="禁用网络搜索")
    parser.add_argument("--no-web", action="store_true", help="禁用深度网页抓取")
    parser.add_argument("--stats", action="store_true", help="只显示数据库统计")

    args = parser.parse_args()

    if args.stats or not args.question:
        db = await DatabaseService.get_instance()
        stats = await db.get_stats_summary()
        print("\n📊 数据库统计")
        print("="*40)
        print(f"  文档: {stats.get('documents', 0)} 篇")
        print(f"  提案: {stats.get('proposals', 0)} 个")
        print(f"  经验: {stats.get('playbook', 0)} 条")
        print(f"  统计: {stats.get('stats', 0)} 条")
        print("="*40)
        if not args.question:
            print("\n用法: python -m sovereign_hall.services.research_discussion '你的问题'")
        return

    system = ResearchDiscussionSystem(
        enable_search=not args.no_search,
        enable_web=not args.no_web
    )

    context = await system.research(args.question)
    await print_report(context)

    report_file = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"# 研究报告\n\n")
        f.write(f"**问题**: {context.question}\n\n")
        f.write(f"**时间**: {datetime.now().isoformat()}\n\n")
        f.write("---\n\n")
        f.write("## 结论\n\n")
        f.write(context.final_conclusion)
        f.write("\n\n---\n\n")
        f.write("## 讨论过程\n\n")
        for i, view in enumerate(context.discussion_history, 1):
            f.write(f"### 第{i}轮\n\n")
            f.write(view)
            f.write("\n\n")

    print(f"\n💾 报告已保存到: {report_file}")


if __name__ == "__main__":
    asyncio.run(main())
