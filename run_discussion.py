#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - 无限 Token 焚化炉
功能：持续自动研究，预设议题池 + 多路并发 + 结构化存储
用法：直接运行此脚本（Ctrl+C 停止）
"""

import asyncio
import sys
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import logging
from logging.handlers import RotatingFileHandler

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 配置日志系统
log_dir = project_root / "data" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

# 日志文件：按日期命名 + 轮转
log_file = log_dir / f"sovereign_hall_{datetime.now().strftime('%Y%m%d')}.log"

# 配置根 logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("sovereign_hall")

from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SpiderSwarm, SearchQueryGenerator
from sovereign_hall.core import AgentRole
from sovereign_hall.core.config import get_config
from sovereign_hall.utils import safe_parse_json, estimate_tokens

# 延迟导入Agent避免循环引用
Agent = None
def _get_agent():
    global Agent
    if Agent is None:
        from sovereign_hall.agents.agent import Agent
    return Agent


# 基于议题的默认提案映射
TOPIC_PROPOSALS = {
    "AI算力": [
        {"ticker": "159995", "direction": "long", "sector": "科技", "thesis": "AI算力需求爆发"},
        {"ticker": "512880", "direction": "long", "sector": "半导体", "thesis": "算力芯片国产替代"},
    ],
    "半导体": [
        {"ticker": "512880", "direction": "long", "sector": "半导体", "thesis": "半导体国产替代"},
        {"ticker": "688981", "direction": "long", "sector": "半导体", "thesis": "芯片龙头"},
    ],
    "消费": [
        {"ticker": "159928", "direction": "long", "sector": "消费", "thesis": "消费复苏"},
        {"ticker": "600519", "direction": "long", "sector": "白酒", "thesis": "白酒龙头"},
    ],
    "医药": [
        {"ticker": "159915", "direction": "long", "sector": "医药", "thesis": "医药创新"},
    ],
    "新能源": [
        {"ticker": "159825", "direction": "long", "sector": "新能源", "thesis": "新能源车"},
        {"ticker": "159985", "direction": "long", "sector": "光伏", "thesis": "光伏产业链"},
    ],
    "银行": [
        {"ticker": "159919", "direction": "long", "sector": "银行", "thesis": "高股息银行"},
    ],
}


def generate_default_proposals(topic: str) -> List[Dict]:
    """基于议题生成默认提案"""
    proposals = []

    # 匹配议题关键词
    for key, default_list in TOPIC_PROPOSALS.items():
        if key in topic:
            for p in default_list:
                proposals.append({
                    'ticker': p['ticker'],
                    'direction': p.get('direction', 'long'),
                    'target_position': 0.1,
                    'stop_loss': 5.0,
                    'take_profit': 15.0,
                    'holding_period': 30,
                    'confidence': 0.5,
                    'thesis': p.get('thesis', topic),
                    'sector': p.get('sector', '未知'),
                })
            break
    else:
        # 默认使用科技ETF
        proposals.append({
            'ticker': '159995',
            'direction': 'long',
            'target_position': 0.1,
            'stop_loss': 5.0,
            'take_profit': 15.0,
            'holding_period': 30,
            'confidence': 0.5,
            'thesis': topic,
            'sector': '综合',
        })

    return proposals

logger = logging.getLogger(__name__)

# ============================================================================
# 预设议题池 - 定期轮换，避免重复
# ============================================================================
TOPIC_POOL = [
    # 科技赛道
    "AI算力产业链投资机会",
    "半导体国产替代进程分析",
    "云计算行业发展趋势",
    "新能源汽车智能化方向",
    "消费电子复苏前景",
    # 消费赛道
    "白酒行业库存周期",
    "免税店行业竞争格局",
    "餐饮连锁扩张逻辑",
    "乳制品需求变化",
    "家电以旧换新政策效果",
    # 医药赛道
    "创新药出海前景",
    "医疗器械国产替代",
    "中药配方颗粒集采",
    "CXO行业景气度",
    "医疗服务价格改革",
    # 金融赛道
    "银行股高股息价值",
    "保险负债端改善",
    "券商财富管理转型",
    # 周期赛道
    "有色金属供需格局",
    "化工景气度分化",
    "地产政策效果评估",
    "工程机械周期位置",
    # 宏观策略
    "美联储加息路径影响",
    "人民币汇率走势",
    "A股市场风格切换",
    "机构仓位分析",
    # 新兴赛道
    "低空经济发展前景",
    "氢能产业链机会",
    "固态电池技术路线",
    "AI应用落地场景",
]

# 已完成议题记录文件
COMPLETED_TOPICS_FILE = project_root / "data" / "completed_topics.json"


def load_completed_topics() -> set:
    """加载已完成的议题"""
    try:
        if COMPLETED_TOPICS_FILE.exists():
            with open(COMPLETED_TOPICS_FILE, 'r', encoding='utf-8') as f:
                return set(json.load(f))
    except Exception:
        pass
    return set()


def save_completed_topics(topics: set):
    """保存已完成的议题"""
    try:
        COMPLETED_TOPICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(COMPLETED_TOPICS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(topics), f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"保存已完成议题失败: {e}")


def select_next_topic(completed_topics: set) -> str:
    """选择下一个议题：优先选未完成的，其次循环"""
    # 先尝试从未完成的议题中选择
    remaining = [t for t in TOPIC_POOL if t not in completed_topics]
    if remaining:
        return remaining[0]
    # 如果都完成了，重置并随机选一个
    return TOPIC_POOL[0]


# ============================================================================
# 阶段1：海量信息搜索（高并发）
# ============================================================================
async def stage1_mass_search(llm, spiders, topic: str, query_count: int = 30) -> list:
    """阶段1：海量信息搜索"""
    logger.info(f"========== 阶段1：海量信息搜索 - 议题: {topic} ==========")
    print("\n" + "="*60)
    print(f"📡 阶段1：海量信息搜索 - 议题: {topic}")
    print("="*60)

    # 清理议题关键词
    topic_keyword = topic.replace("分析", "").replace("研究", "").replace("投资机会", "").replace("行业", "").strip()

    # 构建更丰富的种子词
    seeds = {
        "sector": [topic_keyword, f"{topic_keyword}行业", f"{topic_keyword}产业链"],
        "macro": ["A股", "股票市场", "投资策略"],
        "stocks": [],
    }

    # 添加更多变体
    extra_queries = [
        f"{topic_keyword} 最新消息",
        f"{topic_keyword} 政策",
        f"{topic_keyword} 行情",
        f"{topic_keyword} 研报",
        f"{topic_keyword} 龙头",
    ]

    # 生成搜索词
    query_gen = SearchQueryGenerator(llm)
    queries = await query_gen.generate_queries(count=query_count, seeds=seeds)

    print(f"\n生成 {len(queries)} 个搜索词")
    print(f"示例: {queries[:5]}")

    # 合并额外查询词并去重
    all_queries = list(set(queries + extra_queries))[:query_count]

    # 降低搜索量以减少被封风险 - 减少查询数和结果数
    raw_docs = await spiders.aggressive_search(
        all_queries,
        max_results_per_query=5,  # 从10降到5
    )

    print(f"\n抓取 {len(raw_docs)} 篇文档")
    return raw_docs


# ============================================================================
# 阶段2：深度研报生成
# ============================================================================
async def stage2_deep_research(llm, docs: list, topic: str, db_service=None) -> list:
    """阶段2：从文档中提取投资提案"""
    if not docs:
        print("\n⚠️ 没有文档，跳过深度研究")
        logger.warning("阶段2：没有文档，跳过深度研究")
        return []

    logger.info("========== 阶段2：深度研报生成 ==========")
    print("\n" + "="*60)
    print("📖 阶段2：深度研报生成")
    print("="*60)

    # 获取黑名单
    blacklist = []
    if db_service:
        try:
            blacklist = await db_service.get_blacklist()
        except Exception as e:
            logger.warning(f"Failed to get blacklist: {e}")

    # 构建黑名单提示
    blacklist_prompt = ""
    if blacklist:
        blacklist_prompt = f"""
【重要风险提示 - 必须排除以下标的】
以下标的曾有过重大风险事件或投资失败，请勿推荐：
{', '.join(blacklist[:20])}

请确保不要推荐上述任何标的。
"""

    # 过滤有效文档（降低阈值，因为搜索结果snippet通常较短）
    valid_docs = []
    for doc in docs:
        content = getattr(doc, 'content', '') or ''
        # 改为50字符阈值，并检查是否为有效内容
        if len(content) > 50 and content and content != 'None':
            valid_docs.append(doc)

    print(f"有效文档: {len(valid_docs)} / {len(docs)}")

    if len(valid_docs) < 3:
        print("⚠️ 有效文档不足")
        return []

    AgentCls = _get_agent()

    # 构建文档摘要
    doc_contents = []
    for doc in valid_docs[:15]:
        content = getattr(doc, 'content', '') or ''
        title = getattr(doc, 'title', '') or ''
        url = getattr(doc, 'url', '') or ''
        if len(content) > 200:
            doc_contents.append(f"【{title}】\n{content[:800]}\n来源: {url}")

    content_text = "\n\n".join(doc_contents)

    # 一次性生成多个提案
    prompt = f"""
作为资深行业投资分析师，基于以下新闻/研报资料，提取3-5个具体的投资提案。

研究议题：{topic}
{blacklist_prompt}

资料：
{content_text[:8000]}

请直接输出JSON数组格式（不要输出思考过程，只要JSON）：
[
    {{
        "ticker": "推荐标的代码",
        "direction": "long或short",
        "target_position": 0.1,
        "stop_loss": 5.0,
        "take_profit": 15.0,
        "confidence": 0.7,
        "thesis": "一句话核心逻辑",
        "sector": "行业分类"
    }}
]

如果无法确定具体标的，使用：159995(科技)、159928(消费)、159915(医药)、159990(周期)、512880(半导体)

重要：必须排除黑名单中的标的！
"""

    try:
        print(f"   🔄 调用LLM批量生成提案...")
        response = await asyncio.wait_for(
            llm.chat(
                system=f"你是资深投资分析师，擅长从公开资料中挖掘投资机会。",
                user=prompt,
                temperature=0.3,
                max_tokens=5000
            ),
            timeout=600
        )

        print(f"   📥 LLM响应: {response[:200]}...")

        # 解析JSON
        proposals = safe_parse_json(response, [])
        if not isinstance(proposals, list):
            proposals = [proposals]

        # 清洗数据（同时过滤黑名单）
        cleaned = []
        for p in proposals:
            ticker = str(p.get('ticker', '')).strip().upper()
            if ticker and ticker != 'NULL' and len(ticker) >= 2:
                # 过滤黑名单中的标的
                if blacklist and ticker in blacklist:
                    logger.warning(f"Filtered blacklisted ticker: {ticker}")
                    continue
                cleaned.append({
                    'ticker': ticker,
                    'direction': p.get('direction', 'long'),
                    'target_position': float(p.get('target_position', 0.1)),
                    'stop_loss': float(p.get('stop_loss', 5.0)),
                    'take_profit': float(p.get('take_profit', 15.0)),
                    'holding_period': int(p.get('holding_period', 30)),
                    'confidence': float(p.get('confidence', 0.6)),
                    'thesis': p.get('thesis', '')[:100],
                    'sector': p.get('sector', '未知'),
                })

        print(f"\n   ✅ 生成 {len(cleaned)} 个提案（过滤黑名单后）")
        for p in cleaned:
            print(f"      {p['ticker']} | {p['direction']} | 置信度: {p['confidence']:.0%} | {p['thesis'][:30]}")

        # 如果没有生成有效提案，使用基于议题的默认提案
        if not cleaned:
            print("   ⚠️ 使用基于议题的默认提案")
            default_proposals = generate_default_proposals(topic)
            # 过滤默认提案中的黑名单
            cleaned = [p for p in default_proposals if not blacklist or p['ticker'] not in blacklist]

        return cleaned

    except asyncio.TimeoutError:
        print(f"   ⏰ 超时")
        return []
    except Exception as e:
        print(f"   ❌ 错误: {str(e)[:80]}")
        return []


# ============================================================================
# 阶段3：投委会审议（多轮辩论）
# ============================================================================
async def stage3_ic_discussion(llm, spiders, proposals: list, topic: str):
    """阶段3：投委会审议"""
    if not proposals:
        logger.warning("阶段3：无提案，跳过审议")
        return "", []

    logger.info("========== 阶段3：投委会审议 ==========")
    print("\n" + "="*60)
    print("🔥 阶段3：投委会审议")
    print("="*60)

    AgentCls = _get_agent()

    # 创建7个智能体并设置议题上下文
    agents = {}
    for role in [AgentRole.TMT_ANALYST, AgentRole.CONSUMER_ANALYST, AgentRole.CYCLE_ANALYST,
                 AgentRole.MACRO_STRATEGIST, AgentRole.RISK_OFFICER, AgentRole.QUANT_RESEARCHER,
                 AgentRole.CIO]:
        agent = AgentCls(role, llm, spider_service=spiders)
        agent.set_topic(topic)  # 绑定到当前议题
        agents[role] = agent

    all_discussions = []
    final_decisions = []

    # 每轮只讨论前3个提案，避免太长
    for i, proposal in enumerate(proposals[:3]):
        ticker = proposal.get('ticker', '')
        thesis = proposal.get('thesis', '')
        sector = proposal.get('sector', '')

        print(f"\n### 提案 {i+1}: {ticker} ({proposal.get('direction')}) | 置信度: {proposal.get('confidence', 0):.0%}")

        # ============================================================
        # 第一轮：14路并发分析
        # ============================================================
        print("   📝 第一轮：14路并发分析...")

        round1_tasks = [
            (agents[AgentRole.RISK_OFFICER], "风控-财务风险", f"作为风控官，分析{ticker}的财务造假风险。核心观点：{thesis}。请找出潜在风险。", [f"{ticker} 财务", f"{ticker} 风险"]),
            (agents[AgentRole.RISK_OFFICER], "风控-最坏情况", f"作为风控官，分析{ticker}最坏情况可能跌多少。", [f"{ticker} 历史跌幅"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-技术面", f"作为量化分析师，分析{ticker}的技术走势。", [f"{ticker} K线", f"{ticker} 技术分析"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-估值", f"作为量化分析师，分析{ticker}的估值水平PE/PB。", [f"{ticker} 估值", f"{ticker} PE"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-政策风险", f"作为宏观策略师，分析{ticker}面临的政策风险。", [f"{ticker} 政策", f"{sector} 政策"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-时机", f"作为宏观策略师，分析当前是否是买入{ticker}的时机。", ["A股 买入时机", "2025 投资"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-行业", f"作为TMT分析师，从行业角度点评{ticker}。", [f"{sector} 行业", f"{ticker} 动态"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-行业", f"作为消费分析师，从行业角度点评{ticker}。", [f"{sector} 消费", f"{ticker} 消费"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-行业", f"作为周期分析师，从行业周期角度点评{ticker}。", [f"{sector} 周期"]),
            (agents[AgentRole.CIO], "CIO-综合", f"作为CIO，综合分析{ticker}的投资价值。", [f"{ticker} 机构观点", f"{ticker} 评级"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-机会", f"作为TMT分析师，分析{ticker}的增长机会。", [f"{ticker} 增长", f"{ticker} 前景"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-机会", f"作为消费分析师，分析{ticker}的增长机会。", [f"{ticker} 业绩", f"{ticker} 增长"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-机会", f"作为周期分析师，分析{ticker}的周期位置。", [f"{sector} 供需"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-资金", f"作为量化分析师，分析{ticker}的资金流向。", [f"{ticker} 主力资金"]),
        ]

        try:
            round1_results = await asyncio.wait_for(
                asyncio.gather(*[
                    agent.think_with_search(
                        task=task,
                        search_queries=queries,
                        context=thesis,
                        temperature=0.8,
                        max_tokens=8000
                    )
                    for agent, name, task, queries in round1_tasks
                ]),
                timeout=900
            )

            task_names = [name for _, name, _, _ in round1_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第一轮分析\n{'='*50}")
            for name, result in zip(task_names, round1_results):
                all_discussions.append(f"\n[{name}]\n{result[:500]}")

            print(f"      ✅ 第一轮完成")

            # ============================================================
            # 第二轮：7路深度辩论
            # ============================================================
            print("   📝 第二轮：深度辩论...")

            round1_summary = "\n".join([f"{name}: {r[:300]}" for name, r in zip(task_names, round1_results)])

            debate_tasks = [
                (agents[AgentRole.RISK_OFFICER], "质疑", f"基于以下分析提出最尖锐的质疑：\n{round1_summary[:500]}", [f"{ticker} 风险", f"{ticker} 问题"]),
                (agents[AgentRole.QUANT_RESEARCHER], "数据质疑", f"基于以下分析指出数据问题：\n{round1_summary[:500]}", [f"{ticker} 数据"]),
                (agents[AgentRole.MACRO_STRATEGIST], "宏观质疑", f"基于以下分析指出宏观风险：\n{round1_summary[:500]}", ["宏观经济 风险"]),
                (agents[AgentRole.TMT_ANALYST], "行业反驳", f"从行业角度反驳其他观点：\n{round1_summary[:500]}", [f"{sector} 趋势"]),
                (agents[AgentRole.CONSUMER_ANALYST], "消费反驳", f"从消费角度反驳其他观点：\n{round1_summary[:500]}", [f"{sector} 消费"]),
                (agents[AgentRole.CYCLE_ANALYST], "周期反驳", f"从周期角度反驳其他观点：\n{round1_summary[:500]}", [f"{sector} 周期"]),
                (agents[AgentRole.CIO], "CIO回应", f"回应各方质疑，给出最终立场：\n{round1_summary[:500]}", [f"{ticker} 机构"]),
            ]

            round2_results = await asyncio.wait_for(
                asyncio.gather(*[
                    agent.think_with_search(
                        task=task,
                        search_queries=queries,
                        context=round1_summary[:300],
                        temperature=0.7,
                        max_tokens=6000
                    )
                    for agent, name, task, queries in debate_tasks
                ]),
                timeout=600
            )

            debate_names = [name for _, name, _, _ in debate_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第二轮辩论\n{'='*50}")
            for name, result in zip(debate_names, round2_results):
                all_discussions.append(f"\n[{name}]\n{result[:500]}")

            print(f"      ✅ 第二轮完成")

            # ============================================================
            # 第三轮：投票裁决
            # ============================================================
            print("   📊 第三轮：投票...")

            full_context = f"【第一轮】{round1_summary[:800]}\n\n【第二轮】" + "\n".join([f"{n}: {r[:200]}" for n, r in zip(debate_names, round2_results)])

            vote_tasks = [
                (agents[AgentRole.CIO], f"CIO投票-买入/卖出/观望 | 置信度 | 仓位建议"),
                (agents[AgentRole.TMT_ANALYST], f"TMT分析师投票"),
                (agents[AgentRole.CONSUMER_ANALYST], f"消费分析师投票"),
                (agents[AgentRole.CYCLE_ANALYST], f"周期分析师投票"),
                (agents[AgentRole.MACRO_STRATEGIST], f"宏观策略师投票"),
                (agents[AgentRole.RISK_OFFICER], f"风控官投票"),
                (agents[AgentRole.QUANT_RESEARCHER], f"量化研究员投票"),
            ]

            vote_prompts = [
                f"基于以下所有讨论，对{ticker}给出最终投票：\n{full_context[:1000]}\n\n输出格式：【投票】买入/卖出/观望 | 置信度: XX% | 仓位: XX% | 止损: XX%",
                f"从TMT行业角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
                f"从消费行业角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
                f"从周期行业角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
                f"从宏观角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
                f"从风控角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
                f"从量化角度，对{ticker}投票：\n{full_context[:800]}\n\n【投票】",
            ]

            round3_results = await asyncio.wait_for(
                asyncio.gather(*[
                    agent.think(task=prompt, temperature=0.6, max_tokens=3000)
                    for agent, prompt in zip([a for a, _ in vote_tasks], vote_prompts)
                ]),
                timeout=300
            )

            vote_names = [name for _, name in vote_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第三轮投票\n{'='*50}")
            for name, result in zip(vote_names, round3_results):
                all_discussions.append(f"\n[{name}]\n{result[:300]}")

            # 记录最终决策
            final_decisions.append({
                'ticker': ticker,
                'direction': proposal.get('direction'),
                'confidence': proposal.get('confidence', 0),
                'thesis': thesis,
                'cio_vote': round3_results[0][:200],
            })

            print(f"      ✅ 投票完成")

        except asyncio.TimeoutError:
            print(f"      ⏰ 讨论超时，跳过")
            all_discussions.append(f"\n【{ticker}】讨论超时")
        except asyncio.CancelledError:
            print(f"      ⚠️ 讨论被取消")
            all_discussions.append(f"\n【{ticker}】讨论被取消")
            raise  # 重新抛出 CancelledError，让上层处理
        except Exception as e:
            print(f"      ❌ 错误: {str(e)[:50]}")
            all_discussions.append(f"\n【{ticker}】错误: {str(e)[:100]}")

    # 议题结束时清理记忆，防止跨议题污染
    for agent in agents.values():
        agent.clear_memory()

    return "\n".join(all_discussions), final_decisions


# ============================================================================
# 阶段4：综合结论 + 结构化存储
# ============================================================================
async def stage4_final_conclusion(llm, discussions: str, decisions: List[Dict], topic: str) -> Dict:
    """阶段4：生成综合结论并结构化"""
    logger.info("========== 阶段4：综合结论 ==========")
    print("\n" + "="*60)
    print("⚖️ 阶段4：综合结论")
    print("="*60)

    if not decisions:
        return {
            'topic': topic,
            'conclusion': '无有效提案',
            'key_ticker': '',
            'direction': '',
            'confidence': 0,
            'key_reasons': [],
            'action': '观望',
        }

    # 提取关键信息
    key_ticker = decisions[0].get('ticker', '')
    key_direction = decisions[0].get('direction', 'long')
    key_confidence = decisions[0].get('confidence', 0)
    key_thesis = decisions[0].get('thesis', '')

    try:
        response = await asyncio.wait_for(
            llm.chat(
                system="你是投资总监，负责综合各方观点给出最终裁决。",
                user=f"""
研究议题：{topic}

讨论内容：
{discussions[:6000]}

请输出简洁的结构化结论：
## 核心判断
[买/卖/观望] [标的] | 置信度: XX%

## 关键逻辑（3条）
1.
2.
3.

## 操作建议
仓位: XX% | 止损: XX% | 止盈: XX%

## 风险提示
（1-2条）
""",
                temperature=0.5,
                max_tokens=3000
            ),
            timeout=300
        )

        # 解析结论
        conclusion_data = {
            'topic': topic,
            'conclusion': response,
            'key_ticker': key_ticker,
            'direction': key_direction,
            'confidence': key_confidence,
            'key_reasons': [],  # 后续可提取
            'action': '买入' if key_direction == 'long' else ('卖出' if key_direction == 'short' else '观望'),
        }

        print(f"\n✅ 结论: {key_ticker} | {key_direction} | 置信度: {key_confidence:.0%}")

        return conclusion_data

    except Exception as e:
        print(f"   ❌ 生成结论失败: {e}")
        return {
            'topic': topic,
            'conclusion': f"生成失败: {str(e)[:100]}",
            'key_ticker': key_ticker,
            'direction': key_direction,
            'confidence': key_confidence,
            'key_reasons': [],
            'action': '观望',
        }


# ============================================================================
# 主循环
# ============================================================================
async def main():
    print("\n" + "="*60)
    print("🔥 Sovereign Hall - 无限 Token 焚化炉")
    print("="*60)
    print("设计目标：")
    print("  - 预设议题池，循环研究")
    print("  - 高并发搜索 + 多轮辩论")
    print("  - 结构化存储结论")
    print("  - 0.1秒间隔，持续燃烧Token")
    print("="*60 + "\n")

    # ========== 启动自检 ==========
    print("🔍 系统自检...")

    # 1. 清理旧日志（保留最近10份）
    log_dir = project_root / "data" / "logs"
    if log_dir.exists():
        log_files = sorted(log_dir.glob("sovereign_hall_*.log*"), key=lambda x: x.stat().st_mtime, reverse=True)
        for i, f in enumerate(log_files):
            if i >= 10:  # 保留最近10份
                try:
                    f.unlink()
                    print(f"   🗑️  删除旧日志: {f.name}")
                except:
                    pass

    # 2. 重置 Spider 告警状态（避免启动时无法搜索）
    from sovereign_hall.services.spider_service import SpiderSwarm
    SpiderSwarm._consecutive_failures = 0
    SpiderSwarm._alarm_mode = False
    print("   ✅ Spider 告警状态已重置")

    # 3. 初始化 Vector DB
    from sovereign_hall.services.vector_db import VectorDatabase
    config = get_config()
    vector_config = config.get('vector_db', {})
    vector_dim = vector_config.get('dimension', 1024)
    vector_db = VectorDatabase(dimension=vector_dim)
    await vector_db.initialize()
    print(f"   ✅ Vector DB 已初始化 (当前: {len(vector_db.documents)} 条)")

    print("✅ 自检完成\n")

    db_path = project_root / "data" / "sovereign_hall.db"

    config = get_config()
    llm_config = config.get_llm_config()

    llm = LLMClient(
        max_concurrent=16,  # 高并发
        model=llm_config.get('model'),
        provider=llm_config.get('provider'),
    )
    # 从配置中读取 Spider 并发数（已降低防止被封）
    spider_config = config.get_spider_config()
    spiders = SpiderSwarm(max_concurrent=spider_config.get('max_concurrent', 10))

    db_service = DatabaseService()
    await db_service._init_db()
    await db_service.init_report_tables()

    # 初始化投资模拟
    from sovereign_hall.services.investment_simulation import InvestmentSimulation
    simulation = InvestmentSimulation(db_service)
    await simulation.initialize()
    await simulation.init_tables()
    print(f"   ✅ 投资模拟已初始化 (初始资金: {simulation.initial_capital}元)")

    query_gen = SearchQueryGenerator(llm)

    # 加载已完成议题
    completed_topics = load_completed_topics()

    iteration = 0
    start_time = datetime.now()

    try:
        # 连续无结果计数
        empty_rounds = 0
        docs = []
        proposals = []

        while True:
            iteration += 1
            round_start = datetime.now()

            # 连续无结果时增加延迟，防止空转
            if empty_rounds >= 3:
                wait_seconds = min(60, 10 * (empty_rounds - 2))  # 最多等60秒
                logger.warning(f"连续{empty_rounds}轮无结果，等待{wait_seconds}秒...")
                await asyncio.sleep(wait_seconds)

            # 选择议题
            topic = select_next_topic(completed_topics)
            logger.info(f"🔥 第 {iteration} 轮开始 | 议题: {topic}")
            print(f"\n{'='*60}")
            print(f"🔥 第 {iteration} 轮 | 议题: {topic}")
            print(f"{'='*60}")

            try:
                # 阶段1：按需搜索（先检查本地数据是否足够）
                # 检查 VectorDB 中是否有相关数据
                existing_docs = []
                try:
                    # 简单搜索已有数据
                    existing_docs = vector_db.search(topic, limit=20)
                except:
                    pass

                # 如果有足够数据，跳过搜索
                if existing_docs and len(existing_docs) >= 10:
                    print(f"\n📚 阶段1：使用本地数据 ({len(existing_docs)} 条相关文档)")
                    docs = []
                    for d in existing_docs:
                        docs.append({
                            'title': d.get('title', '') or '无标题',
                            'content': d.get('content', '') or d.get('text', ''),
                            'url': d.get('url', ''),
                            'source': 'vector_db'
                        })
                else:
                    # 数据不足，执行搜索（减少搜索量以降低被封风险）
                    print(f"\n📚 阶段1：本地数据不足，进行搜索补充...")
                    docs = await stage1_mass_search(llm, spiders, topic, query_count=6)

                # 保存文档
                if docs:
                    print(f"\n💾 保存 {len(docs)} 篇文档...")
                    # 先保存到数据库
                    for doc in docs:
                        try:
                            await db_service.add_document(doc)
                        except Exception as e:
                            pass

                    # 批量添加到 VectorDB（带 embedding）
                    await vector_db.add_documents_batch(docs, llm_client=llm)

                    print(f"   ✅ 文档已保存 (DB + VectorDB)")

                # 阶段2：深度研报 → 提案
                proposals = await stage2_deep_research(llm, docs, topic, db_service)

                # 💰 每日投资模拟（基于提案+反思执行交易）
                # 检查是否需要执行每日交易（每天第一次运行或有新提案时）
                should_trade = (
                    not simulation.last_trade_date or
                    (datetime.now() - simulation.last_trade_date).days >= 1
                )
                if should_trade:
                    print(f"\n💰 执行每日投资模拟...")

                    # 获取历史反思（用于决策参考）
                    history_reflection = await simulation.get_recent_reflection(limit=2)

                    # 获取当前资产
                    assets = await simulation.calculate_assets()
                    print(f"   当前资产: {assets['total_assets']:.2f}元 | 现金: {assets['cash']:.2f}元 | 持仓: {assets['positions_value']:.2f}元")

                    # 当前持仓
                    current_positions = assets.get('positions', {})
                    current_tickers = set(current_positions.keys())

                    # 如果有新提案，做出交易决策（纳入反思）
                    if proposals:
                        # 优先处理排名前3的提案
                        for proposal in proposals[:3]:
                            ticker = proposal.get('ticker')
                            if not ticker:
                                continue

                            # 检查冷却期
                            if simulation.is_in_cooldown(ticker):
                                print(f"   ⏳ {ticker} 在冷却期内，跳过交易")
                                continue

                            direction = proposal.get('direction', 'long')
                            confidence = proposal.get('confidence', 0.5)
                            target_position = proposal.get('target_position', 0.1)
                            entry_price = proposal.get('entry_price', 10.0)

                            # 决策考虑因素
                            has_position = ticker in current_tickers
                            trade_reason = ""

                            # 结合反思判断
                            if "卖出" in history_reflection and has_position:
                                # 反思建议卖出，降低买入意愿
                                trade_position = target_position * 0.3
                                trade_reason = f"反思建议谨慎，小幅建仓"
                            elif has_position:
                                # 已有持仓，不重复买入
                                print(f"   ⏭️ {ticker} 已有持仓，跳过买入")
                                continue
                            elif confidence < 0.4:
                                trade_position = target_position * 0.3
                                trade_reason = f"低置信度{confidence:.0%}，轻仓尝试"
                            elif confidence < 0.6:
                                trade_position = target_position * 0.5
                                trade_reason = f"中等置信度{confidence:.0%}，半仓"
                            else:
                                trade_position = target_position
                                trade_reason = f"高置信度{confidence:.0%}，按提案执行"

                            result = await simulation.execute_trade(
                                ticker=ticker,
                                direction=direction,
                                target_position=trade_position,
                                current_price=entry_price,
                                llm=llm,
                                reason=trade_reason
                            )

                            if result.get('success') == False:
                                print(f"   ⏭️ {ticker}: {result.get('reason', '交易失败')}")
                            elif result.get('action') == 'buy':
                                print(f"   📈 买入 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                            elif result.get('action') == 'sell':
                                print(f"   📉 卖出 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                            elif result.get('action') == 'hold':
                                if result.get('reason'):
                                    print(f"   ➖ 持有 {ticker}: {result['reason']}")
                    else:
                        # 没有新提案，保持不动
                        if current_positions:
                            print(f"   💤 无新提案，保持当前持仓不动")
                            for ticker, pos in current_positions.items():
                                days_held = 0
                                if ticker in simulation.last_trade_records:
                                    try:
                                        last_date = datetime.fromisoformat(simulation.last_trade_records[ticker])
                                        days_held = (datetime.now() - last_date).days
                                    except:
                                        pass
                                print(f"      - {ticker}: {pos['shares']}股 @ 成本{pos['avg_cost']:.2f} (持有{days_held}天)")
                        else:
                            print(f"   💤 无新提案且空仓，保持观望")

                    # 更新最终资产状态
                    final_assets = await simulation.calculate_assets()
                    print(f"   📊 交易后: 现金 {final_assets['cash']:.2f}元 | 持仓 {final_assets['positions_value']:.2f}元")

                    # 生成每日反思并保存
                    reflection = await simulation.daily_reflection(llm)
                    if reflection:
                        print(f"\n📝 每日投资反思:")
                        print(reflection[:500] + "...")
                    # 保存快照
                    await simulation.save_snapshot(reflection)

                # 阶段3：投委会讨论
                discussions, decisions = await stage3_ic_discussion(llm, spiders, proposals, topic)

                # 阶段4：综合结论
                conclusion_data = await stage4_final_conclusion(llm, discussions, decisions, topic)

                # 保存结论（包含结构化数据）
                await db_service.save_report_conclusion(
                    question=topic,
                    conclusion=conclusion_data.get('conclusion', '')[:30000],
                    ticker=conclusion_data.get('key_ticker', ''),
                    confidence=conclusion_data.get('confidence', 0.5)
                )

                # 更新已完成议题
                completed_topics.add(topic)
                save_completed_topics(completed_topics)

                # 打印结论
                print("\n" + "="*60)
                print("📋 综合结论")
                print("="*60)
                print(conclusion_data.get('conclusion', '')[:1500])
                print("="*60)

            except KeyboardInterrupt:
                raise
            except asyncio.CancelledError:
                print(f"\n⚠️ 任务被取消，保存进度...")
                raise
            except Exception as e:
                print(f"\n❌ 本轮错误: {e}")

            # 统计
            round_time = (datetime.now() - round_start).total_seconds()
            total_time = (datetime.now() - start_time).total_seconds()
            llm_stats = llm.get_stats()

            # 检查本轮是否有有效结果
            has_valid_result = bool(docs and proposals)

            if has_valid_result:
                if empty_rounds > 0:
                    logger.info(f"恢复搜索，成功获取{len(docs)}篇文档和{len(proposals)}个提案")
                empty_rounds = 0
            else:
                empty_rounds += 1
                logger.warning(f"第{iteration}轮无有效结果 (连续{empty_rounds}轮)")

            stats_msg = f"⏱️  本轮用时: {round_time:.1f}秒 | 累计Token: {llm_stats.get('total_tokens', 0):,} | 成本: {llm_stats.get('total_cost_usd', '$0')}"
            logger.info(stats_msg)
            print(f"\n⏱️  本轮用时: {round_time:.1f}秒")
            print(f"🔥  累计Token: {llm_stats.get('total_tokens', 0):,}")
            print(f"🚀  峰值Token速率: {llm_stats.get('peak_token_rate', '0/s')}")
            print(f"📊  平均Token速率: {llm_stats.get('avg_token_rate', '0/s')}")
            print(f"💰  累计成本: {llm_stats.get('total_cost_usd', '$0')}")

            # 根据是否有结果决定休息时间
            if has_valid_result:
                print(f"\n💤 休息 1 秒后继续...")
                await asyncio.sleep(1)
            else:
                wait_time = min(10, 2 ** (empty_rounds - 1))
                print(f"\n💤 搜索失败，休息 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)

    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print("🛑 已停止")
        print(f"{'='*60}")
        print(f"完成轮次: {iteration}")
        # 打印最终统计
        llm_stats = llm.get_stats()
        print(f"\n📊 最终统计:")
        print(f"   累计Token: {llm_stats.get('total_tokens', 0):,}")
        print(f"   峰值Token速率: {llm_stats.get('peak_token_rate', '0/s')}")
        print(f"   平均Token速率: {llm_stats.get('avg_token_rate', '0/s')}")
        print(f"   累计成本: {llm_stats.get('total_cost_usd', '$0')}")

    finally:
        await spiders.close()
        print("🔒 资源已释放")


if __name__ == "__main__":
    asyncio.run(main())