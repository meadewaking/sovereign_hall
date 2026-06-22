#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - 无限 Token 焚化炉
功能：持续自动研究，预设议题池 + 多路并发 + 结构化存储
用法：直接运行此脚本（Ctrl+C 停止）
"""

import asyncio
import argparse
import contextlib
import fcntl
import os
import sys
import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional
import logging
from logging.handlers import RotatingFileHandler

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root.parent))

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
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger("sovereign_hall")
from sovereign_hall.services.heuristic_policy import (
    apply_heuristic_risk_cap,
    format_heuristic_prompt_context,
    load_latest_heuristic_context,
    recent_prediction_observation_count,
)

# 延迟导入Agent避免循环引用
Agent = None
def _get_agent():
    global Agent
    if Agent is None:
        from sovereign_hall.agents.agent import Agent
    return Agent


def _normalize_expected_days(value, context: str) -> int:
    from sovereign_hall.services.decision_tracker import DecisionRecorder

    return DecisionRecorder.normalize_expected_days(value, context)


def _safe_parse_json(text: str, default=None):
    from sovereign_hall.utils import safe_parse_json

    return safe_parse_json(text, default)


def build_proposal_thesis(raw: Dict) -> str:
    """Preserve the model's full rationale and structured guardrails for storage."""
    thesis = str(raw.get("thesis") or "").strip()
    parts = [thesis] if thesis else []

    evidence = raw.get("evidence")
    if isinstance(evidence, list) and evidence:
        evidence_text = "；".join(str(item).strip() for item in evidence if str(item).strip())
        if evidence_text:
            parts.append(f"证据: {evidence_text}")

    reject_if = str(raw.get("reject_if") or "").strip()
    if reject_if:
        parts.append(f"否决条件: {reject_if}")

    return "\n".join(parts)


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
                    'holding_period': infer_default_holding_period(topic),
                    'holding_period_reason': '根据议题性质自动推断验证窗口',
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
            'holding_period': infer_default_holding_period(topic),
            'holding_period_reason': '根据议题性质自动推断验证窗口',
            'confidence': 0.5,
            'thesis': topic,
            'sector': '综合',
        })

    return proposals


def infer_default_holding_period(topic: str) -> int:
    """根据议题给默认验证窗口，作为模型缺省值的后备。"""
    return _normalize_expected_days(None, topic)


def normalize_proposal_holding_period(proposal: Dict, topic: str) -> int:
    context = " ".join(
        str(proposal.get(key, ""))
        for key in ("thesis", "sector", "holding_period_reason")
    )
    return _normalize_expected_days(
        proposal.get("holding_period"),
        f"{topic} {context}",
    )

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Sovereign Hall continuous discussion runner")
    parser.add_argument("--once", action="store_true", help="只运行一轮后退出")
    parser.add_argument("--max-rounds", type=int, default=0, help="最多运行轮数，0 表示无限")
    parser.add_argument("--skip-preflight", action="store_true", help="跳过 LLM/Embedding/搜索联通性检查")
    return parser.parse_args()


async def run_startup_preflight(llm, spiders) -> bool:
    """Verify external dependencies before burning a research round."""
    print("\n🔌 启动前联通性检查...")
    checks = []

    async def _check_llm():
        response = await asyncio.wait_for(
            llm.chat(
                system="只输出最终答案。",
                user="联通性检查：只回复 OK",
                temperature=0.0,
                max_tokens=80,
                use_cache=False,
            ),
            timeout=90,
        )
        if not response or "OK" not in str(response).upper():
            raise RuntimeError(f"LLM 响应异常: {str(response)[:120]}")

    async def _check_embedding():
        vector = await asyncio.wait_for(
            llm.get_embedding("联通性检查"),
            timeout=60,
        )
        if not isinstance(vector, list) or not vector:
            raise RuntimeError("Embedding 返回空向量")

    async def _check_search():
        docs = await asyncio.wait_for(
            spiders.aggressive_search(["A股 最新消息"], max_results_per_query=1, sources=["ddg"]),
            timeout=90,
        )
        if not docs:
            raise RuntimeError("搜索返回空结果")

    for name, check in [
        ("LLM", _check_llm),
        ("Embedding", _check_embedding),
        ("搜索", _check_search),
    ]:
        try:
            await check()
            checks.append((name, True, "OK"))
            print(f"   ✅ {name}: OK")
        except Exception as exc:
            detail = str(exc)[:300] or exc.__class__.__name__
            checks.append((name, False, detail))
            print(f"   ❌ {name}: {detail}")

    failed = [item for item in checks if not item[1]]
    if failed:
        print("\n❌ 联通性检查未通过，本次不启动 run_discussion。")
        for name, _, detail in failed:
            print(f"   - {name}: {detail}")
        return False

    print("✅ 联通性检查通过\n")
    return True


class SingleInstanceLock:
    """Prevent two discussion runners from writing the same SQLite DB at once."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._handle = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._handle.seek(0)
            holder = self._handle.read().strip() or "unknown"
            self._handle.close()
            self._handle = None
            raise RuntimeError(
                f"run_discussion.py is already running (lock: {self.lock_path}, holder: {holder})"
            )

        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(f"pid={os.getpid()} started_at={datetime.now().isoformat()}\n")
        self._handle.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._handle:
            with contextlib.suppress(Exception):
                self._handle.seek(0)
                self._handle.truncate()
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
                self._handle.close()
        self._handle = None


def count_open_file_handles(target_path: Path) -> int:
    """Best-effort fd count for long-running resource leak diagnostics."""
    target = str(target_path.resolve())
    fd_dir = Path("/proc/self/fd")
    if not fd_dir.exists():
        fd_dir = Path("/dev/fd")
    if not fd_dir.exists():
        return -1

    count = 0
    for fd in fd_dir.iterdir():
        try:
            if os.path.realpath(fd) == target:
                count += 1
        except OSError:
            continue
    return count

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
TOKEN_BUDGET_FILE = project_root / "data" / "token_budget.json"
DEFAULT_TOPIC_COOLDOWN_HOURS = 24


class DailyTokenBudget:
    """按自然日限制 token 使用，防止异常循环无人值守失控。"""

    def __init__(self, path: Path, budget: int = None):
        self.path = path
        self.budget = int(budget or 0)
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.baseline_tokens = 0
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("date") == self.today:
                self.baseline_tokens = int(data.get("baseline_tokens", 0))
        except Exception as exc:
            logger.debug(f"加载Token预算状态失败: {exc}")

    def sync(self, total_tokens: int):
        if not self.budget:
            return
        current_day = datetime.now().strftime("%Y-%m-%d")
        if current_day != self.today or total_tokens < self.baseline_tokens:
            self.today = current_day
            self.baseline_tokens = total_tokens
            self._save(total_tokens)
            return
        if self.baseline_tokens <= 0:
            self.baseline_tokens = total_tokens
            self._save(total_tokens)

    def _save(self, total_tokens: int):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "date": self.today,
                "baseline_tokens": self.baseline_tokens,
                "last_total_tokens": total_tokens,
                "updated_at": datetime.now().isoformat(),
            }
            self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.debug(f"保存Token预算状态失败: {exc}")

    def used_today(self, total_tokens: int) -> int:
        if not self.budget:
            return 0
        self.sync(total_tokens)
        used = max(0, total_tokens - self.baseline_tokens)
        self._save(total_tokens)
        return used

    def exceeded(self, total_tokens: int) -> bool:
        return bool(self.budget and self.used_today(total_tokens) >= self.budget)


def load_completed_topics() -> set:
    """加载已完成的议题"""
    try:
        if COMPLETED_TOPICS_FILE.exists():
            with open(COMPLETED_TOPICS_FILE, 'r', encoding='utf-8') as f:
                return set(json.load(f))
    except Exception as exc:
        logger.warning("加载已完成议题失败，将从空集合开始: %s", exc)
    return set()


def save_completed_topics(topics: set):
    """保存已完成的议题"""
    try:
        COMPLETED_TOPICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(COMPLETED_TOPICS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(topics), f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"保存已完成议题失败: {e}")


def load_recent_topics(db_path: Path, hours: int = DEFAULT_TOPIC_COOLDOWN_HOURS) -> Dict[str, str]:
    """加载近期已讨论议题和最后讨论时间，用于避免短时间重复消耗 token。"""
    if hours <= 0 or not db_path.exists():
        return {}
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT question, MAX(created_at) AS last_discussed_at
                FROM report_conclusions
                WHERE created_at >= ?
                  AND question IS NOT NULL
                  AND question != ''
                GROUP BY question
                """,
                (cutoff,),
            ).fetchall()
        return {row[0]: row[1] for row in rows}
    except sqlite3.Error as exc:
        logger.debug(f"加载近期议题失败: {exc}")
        return {}


def _recent_topic_names(recent_topics) -> set:
    if not recent_topics:
        return set()
    if isinstance(recent_topics, dict):
        return set(recent_topics.keys())
    return set(recent_topics)


def _oldest_recent_topic(recent_topics) -> Optional[str]:
    if not recent_topics:
        return None
    recent_names = _recent_topic_names(recent_topics)
    if isinstance(recent_topics, dict):
        candidates = [
            (recent_topics.get(topic) or "", index, topic)
            for index, topic in enumerate(TOPIC_POOL)
            if topic in recent_names
        ]
        if candidates:
            return min(candidates)[2]
    return next((topic for topic in TOPIC_POOL if topic in recent_names), None)


def select_next_topic(completed_topics: set, recent_topics=None) -> Optional[str]:
    """选择下一个议题：优先选未完成的，其次循环"""
    recent_names = _recent_topic_names(recent_topics)

    remaining = [t for t in TOPIC_POOL if t not in completed_topics and t not in recent_names]
    if remaining:
        return remaining[0]

    if completed_topics:
        logger.info("议题池已完成一轮，重置完成记录并进入下一轮")
        completed_topics.clear()
        save_completed_topics(completed_topics)
        remaining = [t for t in TOPIC_POOL if t not in recent_names]
        if remaining:
            return remaining[0]

    fallback = _oldest_recent_topic(recent_topics)
    if fallback:
        logger.warning("所有议题都在近期冷却期内，选择最久未讨论议题继续: %s", fallback)
        return fallback

    logger.warning("没有可用议题，暂停新研究轮次")
    return None


def dedupe_proposals(proposals: List[Dict]) -> List[Dict]:
    """同一轮内按标的和方向去重，保留置信度最高的提案。"""
    by_key = {}
    for proposal in proposals:
        ticker = str(proposal.get("ticker", "")).strip().upper()
        direction = str(proposal.get("direction", "long")).strip().lower()
        if not ticker:
            continue
        key = (ticker, direction)
        previous = by_key.get(key)
        if previous is None or float(proposal.get("confidence", 0)) > float(previous.get("confidence", 0)):
            by_key[key] = proposal | {"ticker": ticker, "direction": direction}
    return list(by_key.values())


def build_lessons_with_heuristic_context(lessons_prompt: str = "") -> str:
    """Append the latest local heuristic risk constraints to agent prompts."""
    parts = []
    if lessons_prompt and lessons_prompt.strip():
        parts.append(lessons_prompt.strip())
    heuristic_prompt = format_heuristic_prompt_context()
    if heuristic_prompt:
        parts.append(heuristic_prompt)
    return "\n\n".join(parts)


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
    from sovereign_hall.services.spider_service import SearchQueryGenerator

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
async def stage2_deep_research(llm, docs: list, topic: str, db_service=None, lessons_prompt: str = "") -> list:
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
{lessons_prompt}

资料：
{content_text[:8000]}

筛选规则：
1. 只推荐资料中有明确新增证据支持的标的；证据不足时宁可少输出
2. 不要重复同一行业逻辑，不要为了凑数输出相似提案
3. 必须把“已验证事实”和“推断”分开写入thesis
4. 不能确定具体标的时，才允许使用ETF，并把confidence降到0.55以下
5. 黑名单标的一律排除

请直接输出JSON数组格式（不要输出思考过程，只要JSON）：
[
    {{
        "ticker": "推荐标的代码",
        "direction": "long或short",
        "target_position": 0.1,
        "stop_loss": 5.0,
        "take_profit": 15.0,
        "holding_period": 30,
        "holding_period_reason": "验证窗口选择理由，例如短线催化14天、财报/政策落地30天、产业趋势90-180天",
        "confidence": 0.7,
        "thesis": "事实: ...；推断: ...；新增性: ...",
        "sector": "行业分类",
        "evidence": ["来源标题或关键事实1", "来源标题或关键事实2"],
        "reject_if": "若出现什么情况应否决该提案"
    }}
]

如果无法确定具体标的，可使用：159995(科技)、159928(消费)、159915(医药)、159990(周期)、512880(半导体)，但必须说明这是替代ETF而非个股机会。

重要：必须排除黑名单中的标的！
重要：holding_period 必须根据投资逻辑动态决定，范围3-180天，不要一律填30。
"""

    try:
        print(f"   🔄 调用LLM批量生成提案...")
        response = await asyncio.wait_for(
            llm.chat(
                system="你是严谨的投资提案抽取器。只输出合法JSON；不编造资料中没有的事实；证据不足时输出空数组。",
                user=prompt,
                temperature=0.3,
                max_tokens=8000
            ),
            timeout=600
        )

        print(f"   📥 LLM响应: {response[:200]}...")

        # 解析JSON
        proposals = _safe_parse_json(response, [])
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
                cleaned_proposal = {
                    'ticker': ticker,
                    'direction': p.get('direction', 'long'),
                    'target_position': float(p.get('target_position', 0.1)),
                    'stop_loss': float(p.get('stop_loss', 5.0)),
                    'take_profit': float(p.get('take_profit', 15.0)),
                    'confidence': float(p.get('confidence', 0.6)),
                    'thesis': build_proposal_thesis(p),
                    'sector': p.get('sector', '未知'),
                    'holding_period_reason': p.get('holding_period_reason', '')[:200],
                }
                cleaned_proposal['holding_period'] = normalize_proposal_holding_period(cleaned_proposal | {'holding_period': p.get('holding_period')}, topic)
                cleaned.append(cleaned_proposal)

        print(f"\n   ✅ 生成 {len(cleaned)} 个提案（过滤黑名单后）")
        for p in cleaned:
            print(f"      {p['ticker']} | {p['direction']} | {p['holding_period']}天 | 置信度: {p['confidence']:.0%} | {p['thesis'][:30]}")

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


def parse_committee_vote(text: str) -> Dict:
    """Parse a loose committee vote into a small structured signal."""
    parsed_json = _safe_parse_json(str(text or ""), None)
    if isinstance(parsed_json, dict):
        direction = normalize_vote_direction(
            parsed_json.get("direction")
            or parsed_json.get("vote")
            or parsed_json.get("action")
            or parsed_json.get("decision")
        )
        confidence = parse_ratio_value(parsed_json.get("confidence"))
        position = parse_ratio_value(parsed_json.get("position") or parsed_json.get("target_position"))
        risk_flags = parsed_json.get("risk_flags") or parsed_json.get("risks") or []
        if isinstance(risk_flags, str):
            risk_flags = [risk_flags]
        return {
            "direction": direction,
            "confidence": confidence,
            "position": position,
            "risk_flags": [str(flag)[:80] for flag in risk_flags[:5]] if isinstance(risk_flags, list) else [],
            "invalid_if": str(parsed_json.get("invalid_if") or parsed_json.get("reject_if") or "")[:200],
            "key_evidence": parsed_json.get("key_evidence") or parsed_json.get("evidence") or [],
        }

    value = str(text or "").lower()
    if not value:
        return {"direction": "hold", "confidence": None, "position": None, "risk_flags": []}

    has_sell = any(word in value for word in ("卖出", "看空", "做空", "short", "sell"))
    has_hold = any(word in value for word in ("观望", "暂缓", "不建议", "反对", "拒绝", "hold", "defer"))
    has_buy = any(word in value for word in ("买入", "看多", "做多", "long", "buy"))

    if has_sell:
        direction = "short"
    elif has_hold and not has_buy:
        direction = "hold"
    elif has_buy:
        direction = "long"
    else:
        direction = "hold"

    confidence = None
    confidence_match = re.search(r"(?:置信度|confidence)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%?", value)
    if confidence_match:
        confidence = parse_ratio_value(confidence_match.group(1))

    position = None
    position_match = re.search(r"(?:仓位|position)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*%?", value)
    if position_match:
        position = parse_ratio_value(position_match.group(1))

    return {"direction": direction, "confidence": confidence, "position": position, "risk_flags": []}


def normalize_vote_direction(value: Any) -> str:
    """Normalize structured or natural-language vote direction."""
    text = str(value or "").strip().lower()
    if any(word in text for word in ("short", "sell", "卖出", "看空", "做空")):
        return "short"
    if any(word in text for word in ("long", "buy", "买入", "看多", "做多")):
        return "long"
    return "hold"


def parse_ratio_value(value: Any) -> Optional[float]:
    """Parse percentages or decimal ratios into [0, 1]."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        match = re.search(r"-?\d+(?:\.\d+)?", str(value))
        if not match:
            return None
        number = float(match.group(0))
    if number > 1:
        number /= 100
    return max(0.0, min(1.0, number))


def proposal_priority_score(proposal: Dict) -> float:
    """Score how much deliberation a proposal deserves."""
    confidence = float(proposal.get("confidence", 0.5) or 0.5)
    position = float(proposal.get("target_position", 0.0) or 0.0)
    thesis = str(proposal.get("thesis", "") or "")
    ticker = str(proposal.get("ticker", "") or "")
    score = confidence * 0.45 + min(position, 0.25) * 1.2
    score += min(len(thesis) / 1200, 0.2)
    if not is_substitute_etf(ticker):
        score += 0.12
    if "证据:" in thesis or "事实:" in thesis:
        score += 0.08
    if "否决条件:" in thesis:
        score += 0.05
    return round(score, 4)


def is_substitute_etf(ticker: str) -> bool:
    return str(ticker or "").startswith(("159", "510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588"))


def choose_review_depth(proposal: Dict) -> str:
    """Choose review depth by impact and evidence quality, not by token budget."""
    score = proposal_priority_score(proposal)
    confidence = float(proposal.get("confidence", 0.5) or 0.5)
    if score >= 0.58 or confidence >= 0.72:
        return "full"
    if score >= 0.42 or confidence >= 0.55:
        return "focused"
    return "light"


def select_committee_proposals(proposals: List[Dict], limit: int = 3) -> List[Dict]:
    """Discuss the strongest proposals first, preserving all proposal details."""
    ranked = sorted(
        proposals,
        key=lambda item: (proposal_priority_score(item), float(item.get("confidence", 0.0) or 0.0)),
        reverse=True,
    )
    return ranked[:limit]


def build_structured_vote_prompt(ticker: str, role_view: str, context: str, learned_context: str) -> str:
    """Ask each committee role for a machine-readable vote."""
    return f"""
基于以下讨论，对 {ticker} 从{role_view}给出最终投票。

讨论摘要：
{context}
{learned_context}

只输出JSON对象，不要Markdown，不要解释。字段：
{{
  "direction": "long/short/hold",
  "confidence": 0.0,
  "position": 0.0,
  "key_evidence": ["最关键证据1", "最关键证据2"],
  "risk_flags": ["主要风险1", "主要风险2"],
  "invalid_if": "什么情况会推翻该判断"
}}

约束：
- 证据不足或反证更强时 direction 必须是 hold，position 必须是 0。
- confidence 用0到1小数，position 用0到1小数。
""".strip()


def aggregate_committee_decision(proposal: Dict, vote_results: List[str], vote_weights: Optional[List[float]] = None) -> Dict:
    """Aggregate loose text votes into the decision used by downstream systems."""
    parsed = [parse_committee_vote(vote) for vote in vote_results]
    weights = vote_weights or [2.0, 1.0, 1.0, 1.0, 1.0, 1.5, 1.0]
    scores = {"long": 0.0, "short": 0.0, "hold": 0.0}
    for index, vote in enumerate(parsed):
        scores[vote["direction"]] += weights[index] if index < len(weights) else 1.0

    if scores["long"] > scores["short"] and scores["long"] > scores["hold"]:
        direction = "long"
    elif scores["short"] > scores["long"] and scores["short"] > scores["hold"]:
        direction = "short"
    else:
        direction = "hold"

    confidences = [vote["confidence"] for vote in parsed if vote["confidence"] is not None]
    positions = [vote["position"] for vote in parsed if vote["position"] is not None]
    confidence = sum(confidences) / len(confidences) if confidences else float(proposal.get("confidence", 0.5))
    target_position = sum(positions) / len(positions) if positions else float(proposal.get("target_position", 0.1))
    if direction == "hold":
        target_position = 0.0
    total_weight = sum(weights[:len(parsed)]) if parsed else 0.0
    sorted_scores = sorted(scores.values(), reverse=True)
    margin = (sorted_scores[0] - sorted_scores[1]) / total_weight if total_weight and len(sorted_scores) > 1 else 0.0
    risk_flags = []
    for vote in parsed:
        risk_flags.extend(vote.get("risk_flags") or [])

    return {
        "direction": direction,
        "confidence": max(0.0, min(1.0, confidence)),
        "target_position": max(0.0, min(1.0, target_position)),
        "vote_summary": scores,
        "vote_margin": round(margin, 4),
        "vote_count": len(parsed),
        "risk_flags": list(dict.fromkeys(risk_flags))[:8],
    }


# ============================================================================
# 阶段3：投委会审议（多轮辩论）
# ============================================================================
async def stage3_ic_discussion(llm, spiders, proposals: list, topic: str, lessons_prompt: str = ""):
    """阶段3：投委会审议"""
    if not proposals:
        logger.warning("阶段3：无提案，跳过审议")
        return "", []

    from sovereign_hall.core import AgentRole
    from sovereign_hall.services.decision_tracker import DecisionRecorder

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

    # 每轮优先讨论最高价值提案；弱提案走轻量裁决，避免低信号内容稀释学习样本。
    committee_proposals = select_committee_proposals(proposals, limit=3)
    for i, proposal in enumerate(committee_proposals):
        ticker = proposal.get('ticker', '')
        thesis = proposal.get('thesis', '')
        sector = proposal.get('sector', '')
        review_depth = choose_review_depth(proposal)
        priority_score = proposal_priority_score(proposal)
        learned_context = f"\n\n{lessons_prompt}" if lessons_prompt else ""
        analysis_format = (
            "\n\n输出要求：只讲新增判断，不复述提案；"
            "按【证据】【风险/机会】【反证/压力测试】【结论】输出；"
            "不限制必要展开，但每条都必须承担不同验证角度；结论必须含买入/卖出/观望、置信度和否决条件。"
        )

        print(f"\n### 提案 {i+1}: {ticker} ({proposal.get('direction')}) | 置信度: {proposal.get('confidence', 0):.0%} | 深度: {review_depth} | score={priority_score:.2f}")

        # ============================================================
        # 第一轮：14路并发分析
        # ============================================================
        print("   📝 第一轮：14路并发分析...")

        round1_tasks = [
            (agents[AgentRole.RISK_OFFICER], "风控-财务风险", f"作为风控官，分析{ticker}的财务造假风险。核心观点：{thesis}。请找出潜在风险。{learned_context}{analysis_format}", [f"{ticker} 财务", f"{ticker} 风险"]),
            (agents[AgentRole.RISK_OFFICER], "风控-最坏情况", f"作为风控官，分析{ticker}最坏情况可能跌多少。{learned_context}{analysis_format}", [f"{ticker} 历史跌幅"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-技术面", f"作为量化分析师，分析{ticker}的技术走势。{learned_context}{analysis_format}", [f"{ticker} K线", f"{ticker} 技术分析"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-估值", f"作为量化分析师，分析{ticker}的估值水平PE/PB。{learned_context}{analysis_format}", [f"{ticker} 估值", f"{ticker} PE"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-政策风险", f"作为宏观策略师，分析{ticker}面临的政策风险。{learned_context}{analysis_format}", [f"{ticker} 政策", f"{sector} 政策"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-时机", f"作为宏观策略师，分析当前是否是买入{ticker}的时机。{learned_context}{analysis_format}", ["A股 买入时机", "2025 投资"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-行业", f"作为TMT分析师，从行业角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 行业", f"{ticker} 动态"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-行业", f"作为消费分析师，从行业角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 消费", f"{ticker} 消费"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-行业", f"作为周期分析师，从行业周期角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 周期"]),
            (agents[AgentRole.CIO], "CIO-综合", f"作为CIO，综合分析{ticker}的投资价值。{learned_context}{analysis_format}", [f"{ticker} 机构观点", f"{ticker} 评级"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-机会", f"作为TMT分析师，分析{ticker}的增长机会。{learned_context}{analysis_format}", [f"{ticker} 增长", f"{ticker} 前景"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-机会", f"作为消费分析师，分析{ticker}的增长机会。{learned_context}{analysis_format}", [f"{ticker} 业绩", f"{ticker} 增长"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-机会", f"作为周期分析师，分析{ticker}的周期位置。{learned_context}{analysis_format}", [f"{sector} 供需"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-资金", f"作为量化分析师，分析{ticker}的资金流向。{learned_context}{analysis_format}", [f"{ticker} 主力资金"]),
        ]
        if review_depth == "focused":
            focused_names = {"风控-财务风险", "风控-最坏情况", "量化-技术面", "量化-估值", "宏观-时机", "CIO-综合", "TMT-机会", "消费-机会", "周期-机会"}
            round1_tasks = [task for task in round1_tasks if task[1] in focused_names]
        elif review_depth == "light":
            light_names = {"风控-最坏情况", "量化-技术面", "宏观-时机", "CIO-综合"}
            round1_tasks = [task for task in round1_tasks if task[1] in light_names]

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

            round1_summary = "\n".join([f"{name}: {r[:300]}" for name, r in zip(task_names, round1_results)])

            debate_tasks = [
                (agents[AgentRole.RISK_OFFICER], "质疑", f"基于以下分析提出最尖锐的质疑：\n{round1_summary[:500]}{analysis_format}", [f"{ticker} 风险", f"{ticker} 问题"]),
                (agents[AgentRole.QUANT_RESEARCHER], "数据质疑", f"基于以下分析指出数据问题：\n{round1_summary[:500]}{analysis_format}", [f"{ticker} 数据"]),
                (agents[AgentRole.MACRO_STRATEGIST], "宏观质疑", f"基于以下分析指出宏观风险：\n{round1_summary[:500]}{analysis_format}", ["宏观经济 风险"]),
                (agents[AgentRole.TMT_ANALYST], "行业反驳", f"从行业角度反驳其他观点：\n{round1_summary[:500]}{analysis_format}", [f"{sector} 趋势"]),
                (agents[AgentRole.CONSUMER_ANALYST], "消费反驳", f"从消费角度反驳其他观点：\n{round1_summary[:500]}{analysis_format}", [f"{sector} 消费"]),
                (agents[AgentRole.CYCLE_ANALYST], "周期反驳", f"从周期角度反驳其他观点：\n{round1_summary[:500]}{analysis_format}", [f"{sector} 周期"]),
                (agents[AgentRole.CIO], "CIO回应", f"回应各方质疑，给出最终立场：\n{round1_summary[:500]}{analysis_format}", [f"{ticker} 机构"]),
            ]
            if review_depth == "focused":
                focused_debate_names = {"质疑", "数据质疑", "宏观质疑", "CIO回应"}
                debate_tasks = [task for task in debate_tasks if task[1] in focused_debate_names]
            elif review_depth == "light":
                debate_tasks = []

            # ============================================================
            # 第二轮：按需深度辩论
            # ============================================================
            debate_names = []
            round2_results = []
            if debate_tasks:
                print(f"   📝 第二轮：深度辩论 ({len(debate_tasks)}路)...")
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
            else:
                print("   📝 第二轮：轻量提案，跳过深度辩论")

            # ============================================================
            # 第三轮：投票裁决
            # ============================================================
            print("   📊 第三轮：投票...")

            full_context = f"【第一轮】{round1_summary[:800]}\n\n【第二轮】" + "\n".join([f"{n}: {r[:200]}" for n, r in zip(debate_names, round2_results)])

            vote_tasks = [
                (agents[AgentRole.CIO], "CIO综合视角", 2.0),
                (agents[AgentRole.TMT_ANALYST], "TMT行业视角", 1.0),
                (agents[AgentRole.CONSUMER_ANALYST], "消费行业视角", 1.0),
                (agents[AgentRole.CYCLE_ANALYST], "周期行业视角", 1.0),
                (agents[AgentRole.MACRO_STRATEGIST], "宏观策略视角", 1.0),
                (agents[AgentRole.RISK_OFFICER], "风控视角", 1.5),
                (agents[AgentRole.QUANT_RESEARCHER], "量化视角", 1.0),
            ]
            if review_depth == "focused":
                vote_tasks = [task for task in vote_tasks if task[1] in {"CIO综合视角", "宏观策略视角", "风控视角", "量化视角"}]
            elif review_depth == "light":
                vote_tasks = [task for task in vote_tasks if task[1] in {"CIO综合视角", "风控视角", "量化视角"}]

            vote_prompts = [
                build_structured_vote_prompt(ticker, role_view, full_context[:1200], learned_context)
                for _, role_view, _ in vote_tasks
            ]

            # 第三轮投票 - 增加错误处理和日志
            print("      🔄 等待投票结果...")
            try:
                round3_results = await asyncio.wait_for(
                    asyncio.gather(*[
                        agent.think(task=prompt, temperature=0.4, max_tokens=3000)
                        for agent, prompt in zip([a for a, _, _ in vote_tasks], vote_prompts)
                    ]),
                    timeout=300
                )
            except Exception as e:
                logger.error(f"第三轮投票失败: {e}")
                print(f"      ⚠️ 投票出错: {e}")
                # 返回空结果继续
                round3_results = [f"投票失败: {str(e)[:100]}" for _ in vote_tasks]

            vote_names = [name for _, name, _ in vote_tasks]
            vote_weights = [weight for _, _, weight in vote_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第三轮投票\n{'='*50}")
            for name, result in zip(vote_names, round3_results):
                all_discussions.append(f"\n[{name}]\n{result[:300]}")

            committee_decision = aggregate_committee_decision(proposal, round3_results, vote_weights=vote_weights)
            expected_days = normalize_proposal_holding_period(proposal, topic)
            committee_decision.update({
                'ticker': ticker,
                'thesis': thesis,
                'cio_vote': round3_results[0][:200],
                'review_depth': review_depth,
                'priority_score': priority_score,
                'target_price': proposal.get('take_profit', 15.0),
                'stop_loss': proposal.get('stop_loss', 5.0),
                'take_profit': proposal.get('take_profit', 15.0),
                'expected_days': expected_days,
                'holding_period_reason': proposal.get('holding_period_reason', ''),
                'sector': sector,
            })
            final_decisions.append(committee_decision)

            # 记录到决策追踪器
            try:
                if committee_decision.get("direction") in ("long", "short"):
                    recorder = DecisionRecorder()
                    await recorder.record_decision(
                        ticker=ticker,
                        decision=committee_decision.get('direction'),
                        confidence=committee_decision.get('confidence', 0.5),
                        target_price=float(proposal.get('take_profit', 15.0)),
                        stop_loss=float(proposal.get('stop_loss', 5.0)),
                        discussion_context=(
                            f"{thesis[:500]}\n"
                            f"验证窗口: {expected_days}天。{proposal.get('holding_period_reason', '')[:200]}\n"
                            f"审议深度: {review_depth}; priority_score={priority_score:.2f}; "
                            f"vote_margin={committee_decision.get('vote_margin', 0):.2f}; "
                            f"vote_summary={committee_decision.get('vote_summary')}; "
                            f"risk_flags={committee_decision.get('risk_flags', [])}"
                        ),
                        expected_days=expected_days,
                    )
                    print(f"      📊 决策已记录，{expected_days}天后验证")
                else:
                    print("      📊 投委会观望，跳过可验证价格预测记录")
            except Exception as e:
                logger.warning(f"记录决策失败: {e}")

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

请输出简洁的结构化结论。不要复述讨论过程，只写最终可执行判断：
## 核心判断
[买/卖/观望] [标的] | 置信度: XX%

## 关键逻辑（3条）
1. 已验证事实：
2. 核心推断：
3. 触发/否决条件：

## 操作建议
仓位: XX% | 止损: XX% | 止盈: XX%

## 风险提示
（1-2条）

若证据不足，请明确给出“观望”，仓位填0%。
""",
                temperature=0.5,
                max_tokens=5000
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


async def run_committee_approved_simulation(simulation, market_data, llm, decisions: List[Dict]):
    """Run the daily simulated portfolio step after committee decisions exist."""
    should_trade = (
        not simulation.last_trade_date or
        (datetime.now() - simulation.last_trade_date).days >= 1
    )
    if not should_trade:
        return

    if not await market_data.is_trading_day():
        print("\n💰 当前非交易日，跳过每日投资模拟交易")
        reflection = await simulation.daily_reflection(llm)
        if reflection:
            print(f"\n📝 每日投资反思:")
            print(reflection[:500] + "...")
        await simulation.save_snapshot(reflection)
        return

    print(f"\n💰 根据投委会裁决执行每日投资模拟...")
    history_reflection = await simulation.get_recent_reflection(limit=2)
    assets = await simulation.calculate_assets()
    heuristic_context = load_latest_heuristic_context()
    print(f"   当前资产: {assets['total_assets']:.2f}元 | 现金: {assets['cash']:.2f}元 | 持仓: {assets['positions_value']:.2f}元")
    if heuristic_context.available:
        print(
            f"   Heuristic风控: {heuristic_context.policy_name} "
            f"score={heuristic_context.score if heuristic_context.score is not None else 'N/A'} | "
            f"单标的上限{heuristic_context.max_position:.0%} | {heuristic_context.warning}"
        )

    current_positions = assets.get('positions', {})
    current_tickers = set(current_positions.keys())
    max_daily_trades = 5
    trade_count = 0

    trade_candidates = [
        decision for decision in decisions
        if decision.get("direction") in ("long", "short")
    ]

    if trade_candidates:
        for decision in trade_candidates[:5]:
            ticker = decision.get('ticker')
            if not ticker:
                continue
            if trade_count >= max_daily_trades:
                print(f"   ⏹️ 今日已达最大交易次数 ({max_daily_trades}次)，停止交易")
                break
            if simulation.is_in_cooldown(ticker):
                print(f"   ⏳ {ticker} 在冷却期内，跳过交易")
                continue

            direction = decision.get('direction', 'hold')
            confidence = float(decision.get('confidence', 0.5))
            target_position = float(decision.get('target_position', 0.0))
            current_price = await market_data.get_current_price(ticker)
            if current_price is None:
                print(f"   ⏭️ {ticker}: 无法获取真实价格，跳过交易")
                continue

            has_position = ticker in current_tickers
            if "卖出" in history_reflection and has_position and direction == "long":
                trade_position = target_position * 0.3
                trade_reason = "反思建议谨慎，小幅建仓"
            elif has_position and direction == "long":
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
                trade_reason = f"投委会置信度{confidence:.0%}，按裁决执行"

            signal_count = recent_prediction_observation_count(ticker)
            position_values, total_assets_for_cap = simulation._estimate_trade_assets(ticker, current_price)
            current_position_value = position_values.get(ticker, 0.0)
            current_gross_exposure = (
                sum(position_values.values()) / total_assets_for_cap
                if total_assets_for_cap > 0
                else 0.0
            )
            current_position_pct = (
                current_position_value / total_assets_for_cap
                if total_assets_for_cap > 0
                else 0.0
            )
            capped_position, cap_reason = apply_heuristic_risk_cap(
                ticker,
                trade_position,
                confidence,
                signal_count=signal_count,
                current_position=current_position_pct,
                current_gross_exposure=current_gross_exposure,
                context=heuristic_context,
            )
            if cap_reason:
                trade_reason = f"{trade_reason}；{cap_reason}"
            trade_position = capped_position

            result = await simulation.execute_trade(
                ticker=ticker,
                direction=direction,
                target_position=trade_position,
                current_price=current_price,
                llm=llm,
                reason=trade_reason,
                confidence=confidence,
                signal_count=signal_count,
                risk_cap_already_applied=True,
            )

            if result.get('success') is False:
                print(f"   ⏭️ {ticker}: {result.get('reason', '交易失败')}")
            elif result.get('action') == 'buy':
                print(f"   📈 买入 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                trade_count += 1
            elif result.get('action') == 'sell':
                print(f"   📉 卖出 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                trade_count += 1
            elif result.get('action') == 'hold' and result.get('reason'):
                print(f"   ➖ 持有 {ticker}: {result['reason']}")
    elif current_positions:
        print(f"   💤 投委会无新交易裁决，保持当前持仓不动")
        for ticker, pos in current_positions.items():
            days_held = 0
            if ticker in simulation.last_trade_records:
                try:
                    last_date = datetime.fromisoformat(simulation.last_trade_records[ticker])
                    days_held = (datetime.now() - last_date).days
                except Exception as exc:
                    logger.debug("解析持仓日期失败 %s=%r: %s", ticker, simulation.last_trade_records[ticker], exc)
            print(f"      - {ticker}: {pos['shares']}股 @ 成本{pos['avg_cost']:.2f} (持有{days_held}天)")
    else:
        print(f"   💤 投委会无新交易裁决且空仓，保持观望")

    final_assets = await simulation.calculate_assets()
    print(f"   📊 交易后: 现金 {final_assets['cash']:.2f}元 | 持仓 {final_assets['positions_value']:.2f}元")

    reflection = await simulation.daily_reflection(llm)
    if reflection:
        print(f"\n📝 每日投资反思:")
        print(reflection[:500] + "...")
    await simulation.save_snapshot(reflection)


# ============================================================================
# 主循环
# ============================================================================
async def main():
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.decision_tracker import DecisionRecorder
    from sovereign_hall.services.learning_engine import LearningEngine
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.market_data import get_market_data
    from sovereign_hall.services.spider_service import SearchQueryGenerator, SpiderSwarm

    args = parse_args()

    print("\n" + "="*60)
    print("🔥 Sovereign Hall - 无限 Token 焚化炉")
    print("="*60)
    print("设计目标：")
    print("  - 预设议题池，循环研究")
    print("  - 高并发搜索 + 多轮辩论")
    print("  - 结构化存储结论")
    print("  - 0.1秒间隔，持续燃烧Token")
    print("="*60 + "\n")

    # ========== 加载历史统计 ==========
    from sovereign_hall.services.persistence import get_persistence
    persistence = get_persistence()
    prev_stats = persistence.load_previous_stats()
    if prev_stats and prev_stats.get('total_tokens', 0) > 0:
        print(f"📊 历史累计统计:")
        print(f"   - 累计Token: {prev_stats.get('total_tokens', 0):,}")
        print(f"   - 累计成本: ${prev_stats.get('total_cost_usd', 0):.2f}")
        print(f"   - 请求次数: {prev_stats.get('total_requests', 0):,}")
        print(f"   - 已讨论话题: {len(prev_stats.get('topics_discussed', []))}个")
        print(f"   - 已完成轮次: {prev_stats.get('total_rounds', 0)}轮")
        print()

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
                except Exception as exc:
                    logger.debug("删除旧日志失败 %s: %s", f, exc)

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
    vector_db = VectorDatabase(
        dimension=vector_dim,
        max_documents=vector_config.get("max_documents"),
    )
    await vector_db.initialize()
    print(f"   ✅ Vector DB 已初始化 (当前: {len(vector_db.documents)} 条)")

    print("✅ 自检完成\n")

    db_path = project_root / "data" / "sovereign_hall.db"

    config = get_config()
    llm_config = config.get_llm_config()
    system_config = config.get("system", {})
    daily_budget = DailyTokenBudget(
        TOKEN_BUDGET_FILE,
        budget=system_config.get("daily_token_budget"),
    )
    daily_budget_pause = int(system_config.get("daily_budget_pause_seconds", 3600) or 3600)
    validation_batch_size = int(system_config.get("validation_batch_size", 100) or 100)
    topic_cooldown_hours = int(system_config.get("topic_cooldown_hours", DEFAULT_TOPIC_COOLDOWN_HOURS) or 0)

    llm = LLMClient(
        max_concurrent=16,  # 高并发
        model=llm_config.get('model'),
        provider=llm_config.get('provider'),
    )
    # 从配置中读取 Spider 并发数（已降低防止被封）
    spider_config = config.get_spider_config()
    spiders = SpiderSwarm(max_concurrent=spider_config.get('max_concurrent', 10))

    if not args.skip_preflight:
        preflight_ok = await run_startup_preflight(llm, spiders)
        if not preflight_ok:
            await spiders.close()
            await llm.close()
            raise RuntimeError("启动前联通性检查未通过")
    else:
        print("⚠️ 已跳过启动前 LLM/Embedding/搜索联通性检查")

    db_service = DatabaseService(str(db_path))
    await db_service._init_db()
    await db_service.init_report_tables()
    vector_db.set_database_service(db_service)
    market_data = get_market_data()

    # 初始化投资模拟
    from sovereign_hall.services.investment_simulation import InvestmentSimulation
    simulation = InvestmentSimulation(db_service)
    await simulation.initialize()
    await simulation.init_tables()
    print(f"   ✅ 投资模拟已初始化 (初始资金: {simulation.initial_capital}元)")

    query_gen = SearchQueryGenerator(llm)

    # 加载已完成议题
    completed_topics = load_completed_topics()

    # 从持久化加载历史轮次
    prev_stats = persistence.load_previous_stats()
    iteration = prev_stats.get('total_rounds', 0) if prev_stats else 0
    start_time = datetime.now()

    try:
        # 连续无结果计数
        empty_rounds = 0
        docs = []
        proposals = []

        # 初始化验证（处理之前的待验证决策）
        try:
            recorder = DecisionRecorder(str(db_path))
            validation_result = await recorder.validate_pending(max_count=20)
            if validation_result.get('validated', 0) > 0:
                logger.info(f"启动时验证了 {validation_result['validated']} 条历史决策")
        except Exception as e:
            logger.debug(f"初始验证失败: {e}")

        while True:
            if args.max_rounds and (iteration - (prev_stats.get('total_rounds', 0) if prev_stats else 0)) >= args.max_rounds:
                print(f"\n✅ 已达到 --max-rounds={args.max_rounds}，退出")
                break

            current_tokens = llm.get_stats().get("total_tokens", 0)
            if daily_budget.exceeded(current_tokens):
                used = daily_budget.used_today(current_tokens)
                logger.warning(
                    f"今日Token预算已用尽: {used:,}/{daily_budget.budget:,}，暂停{daily_budget_pause}秒"
                )
                await asyncio.sleep(daily_budget_pause)
                continue

            # 连续无结果时增加延迟，防止空转
            if empty_rounds >= 3:
                wait_seconds = min(60, 10 * (empty_rounds - 2))  # 最多等60秒
                logger.warning(f"连续{empty_rounds}轮无结果，等待{wait_seconds}秒...")
                await asyncio.sleep(wait_seconds)

            # 选择议题
            recent_topics = load_recent_topics(db_path, topic_cooldown_hours)
            topic = select_next_topic(completed_topics, recent_topics=recent_topics)
            if topic is None:
                wait_seconds = min(3600, max(300, topic_cooldown_hours * 60))
                print(f"\n💤 所有议题都在 {topic_cooldown_hours} 小时冷却期内，休息 {wait_seconds} 秒")
                await asyncio.sleep(wait_seconds)
                continue

            iteration += 1
            round_start = datetime.now()
            logger.info(f"🔥 第 {iteration} 轮开始 | 议题: {topic}")
            print(f"\n{'='*60}")
            print(f"🔥 第 {iteration} 轮 | 议题: {topic}")
            print(f"{'='*60}")

            # 加载历史教训并显示
            try:
                learning_engine = LearningEngine(str(db_path))
                lessons_prompt = await learning_engine.generate_lessons_prompt()
                stats = await learning_engine.get_accuracy_stats()
                if stats['total'] > 0:
                    print(f"\n📈 历史预测胜率: {stats['accuracy']:.1%} ({stats['correct']}/{stats['total']})")
                if lessons_prompt:
                    print(f"📜 加载了 {lessons_prompt.count('教训') - 1} 条历史教训")

                # 验证待验证决策
                recorder = DecisionRecorder(str(db_path))
                validation_result = await recorder.validate_pending(max_count=validation_batch_size)
                if validation_result.get('validated', 0) > 0:
                    print(f"🔄 本轮验证了 {validation_result['validated']} 条决策")

                # 更新playbook
                await learning_engine.update_playbook()

            except Exception as e:
                logger.debug(f"加载历史教训/验证失败: {e}")
                lessons_prompt = ""

            try:
                # 阶段1：按需搜索（先检查本地数据是否足够）
                # 检查 VectorDB 中是否有相关数据
                existing_docs = []
                try:
                    # 简单搜索已有数据
                    existing_docs = await vector_db.search(topic, top_k=20, llm_client=llm)
                except Exception as e:
                    logger.warning(f"向量检索失败: {e}")

                # 强制定期搜索新数据（每5轮或议题变化时）
                # 避免一直用旧缓存导致空转
                should_force_search = (
                    not existing_docs or
                    len(existing_docs) < 10 or
                    iteration % 5 == 0  # 每5轮强制搜索一次
                )

                if should_force_search and not existing_docs:
                    print(f"\n📚 阶段1：本地数据不足，进行搜索补充...")
                    docs = await stage1_mass_search(llm, spiders, topic, query_count=6)
                elif existing_docs and len(existing_docs) >= 10 and iteration % 5 != 0:
                    print(f"\n📚 阶段1：使用本地数据 ({len(existing_docs)} 条相关文档)")
                    docs = existing_docs
                else:
                    # 每5轮强制搜索更新数据
                    print(f"\n📚 阶段1：定期更新数据 (每5轮强制搜索)")
                    docs = await stage1_mass_search(llm, spiders, topic, query_count=6)

                # 保存文档
                if docs:
                    external_docs = [
                        doc for doc in docs
                        if getattr(doc, "source", "") != "obsidian_wiki"
                        and not str(getattr(doc, "id", "") or getattr(doc, "doc_id", "")).startswith("wiki:")
                    ]
                    skipped_docs = len(docs) - len(external_docs)
                    print(f"\n💾 保存 {len(external_docs)} 篇新外部文档...")
                    saved_docs = 0
                    # 先保存到数据库
                    for doc in external_docs:
                        try:
                            if await db_service.add_document(doc):
                                saved_docs += 1
                        except Exception as e:
                            logger.warning(f"保存文档失败: {e}")

                    # 批量添加到 VectorDB（带 embedding）
                    vector_saved = await vector_db.add_documents_batch(external_docs, llm_client=llm)

                    print(f"   ✅ 文档已保存 (DB: {saved_docs}, Wiki: {vector_saved}, 跳过本地派生: {skipped_docs})")

                prompt_lessons = build_lessons_with_heuristic_context(lessons_prompt)

                # 阶段2：深度研报 → 提案
                proposals = await stage2_deep_research(llm, docs, topic, db_service, lessons_prompt=prompt_lessons)
                proposals = dedupe_proposals(proposals)
                for proposal in proposals:
                    try:
                        await db_service.add_proposal(proposal)
                    except Exception as e:
                        logger.warning(f"保存提案失败: {e}")

                # 阶段3：投委会讨论
                logger.info(f"开始阶段3投委会审议，提案数: {len(proposals)}")
                try:
                    discussions, decisions = await stage3_ic_discussion(llm, spiders, proposals, topic, lessons_prompt=prompt_lessons)
                    logger.info(f"阶段3完成，讨论长度: {len(discussions)}, 决策数: {len(decisions)}")
                except Exception as e:
                    logger.error(f"阶段3失败: {e}", exc_info=True)
                    raise

                # 阶段4：综合结论
                conclusion_data = await stage4_final_conclusion(llm, discussions, decisions, topic)

                # 保存结论（包含结构化数据）
                await db_service.save_report_conclusion(
                    question=topic,
                    conclusion=conclusion_data.get('conclusion', ''),
                    ticker=conclusion_data.get('key_ticker', ''),
                    confidence=conclusion_data.get('confidence', 0.5)
                )

                # 💰 每日投资模拟：只消费投委会裁决后的结构化决策
                await run_committee_approved_simulation(simulation, market_data, llm, decisions)

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

            # 更新持久化统计
            persistence.increment_rounds()
            persistence.add_topic(topic)
            persistence.add_time(round_time)
            persistence.increment_proposals(len(proposals))

            stats_msg = f"⏱️  本轮用时: {round_time:.1f}秒 | 累计Token: {llm_stats.get('total_tokens', 0):,} | 成本: {llm_stats.get('total_cost_usd', '$0')}"
            logger.info(stats_msg)
            db_fd_count = count_open_file_handles(db_path)
            if db_fd_count > 5:
                logger.warning("SQLite fd count is high: %s handles open for %s", db_fd_count, db_path)
            elif db_fd_count >= 0:
                logger.debug("SQLite fd count: %s", db_fd_count)
            print(f"\n⏱️  本轮用时: {round_time:.1f}秒")
            print(f"🔥  累计Token: {llm_stats.get('total_tokens', 0):,}")
            print(f"🚀  峰值Token速率: {llm_stats.get('peak_token_rate', '0/s')}")
            print(f"📊  平均Token速率: {llm_stats.get('avg_token_rate', '0/s')}")
            print(f"💰  累计成本: {llm_stats.get('total_cost_usd', '$0')}")
            if daily_budget.budget:
                used_today = daily_budget.used_today(llm_stats.get('total_tokens', 0))
                print(f"🧯 今日Token预算: {used_today:,}/{daily_budget.budget:,}")

            # 根据是否有结果决定休息时间
            if args.once:
                print("\n✅ --once 模式：本轮完成后退出")
                break

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
        await market_data.close()
        await llm.close()
        await vector_db.close()
        await db_service.close()
        print("🔒 资源已释放")


if __name__ == "__main__":
    try:
        with SingleInstanceLock(project_root / "data" / "run_discussion.lock"):
            asyncio.run(main())
    except RuntimeError as exc:
        logger.error(str(exc))
        print(f"❌ {exc}")
        sys.exit(2)
