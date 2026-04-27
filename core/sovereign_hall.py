"""
🏛️ Sovereign Hall - Main System
君临殿主控系统
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core.config import get_config
from ..core import (
    Document,
    InvestmentProposal,
    ICMeetingMinutes,
    PlaybookEntry,
    SystemStats,
    DATA_DIR,
)
from ..services.llm_client import LLMClient
from ..services.spider_service import SpiderSwarm, SearchQueryGenerator
from ..services.vector_db import VectorDatabase
from ..services.investment_committee import InvestmentCommittee
from ..services.database import DatabaseService
from ..agents.agent import Agent, AnalystTeam
from ..agents import AgentRole
from ..utils import ensure_dir, setup_logging

logger = logging.getLogger(__name__)


class SovereignHall:
    """君临殿主控系统"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化君临殿

        Args:
            config: 配置字典（可选，默认从配置文件读取）
        """
        self.config = config or {}
        self.config_obj = get_config()

        # 初始化日志
        self._init_logging()

        # 初始化状态
        self.running = False
        self.iteration = 0
        self.system_stats = SystemStats()

        # 初始化核心组件
        self._init_components()

        logger.info("🏛️ Sovereign Hall initialized successfully")

    def _init_logging(self):
        """初始化日志"""
        log_config = self.config_obj.get('system', {})
        log_level = self.config.get('log_level', log_config.get('log_level', 'INFO'))
        log_format = self.config.get('log_format', log_config.get('log_format', 'json'))
        log_dir = self.config.get('log_dir', log_config.get('log_dir', str(DATA_DIR / 'logs')))

        setup_logging(
            name="sovereign_hall",
            level=log_level,
            log_format=log_format,
            log_dir=log_dir,
        )

    def _init_components(self):
        """初始化组件"""
        # LLM客户端
        llm_config = self.config_obj.get_llm_config()
        self.llm = LLMClient(
            max_concurrent=self.config.get('max_concurrent_llm', llm_config.get('max_concurrent', 16)),
            model=self.config.get('model', llm_config.get('model')),
            provider=self.config.get('provider', llm_config.get('provider')),
        )

        # 向量数据库
        vector_config = self.config_obj.get_vector_db_config()
        self.vector_db = VectorDatabase(
            dimension=self.config.get('embedding_dim', vector_config.get('dimension', 1536)),
            index_type=self.config.get('index_type', vector_config.get('index_type', 'IVF')),
            storage_path=self.config.get('vector_db_path', str(DATA_DIR / 'vector_db')),
        )

        # 爬虫集群
        self.spiders = SpiderSwarm(
            max_concurrent=self.config.get('max_concurrent_spiders', 50),
        )

        # 分析师团队
        enabled_roles = self.config.get('enabled_roles', self.config_obj.get_analyst_roles())
        self.analysts = AnalystTeam(self.llm, enabled_roles)

        # 投资委员会
        ic_config = self.config_obj.get_ic_config()
        self.ic = InvestmentCommittee(
            self.llm,
            max_rounds=self.config.get('max_rounds', ic_config.get('max_rounds', 3)),
        )

        # 机构记忆
        self.playbook: List[PlaybookEntry] = []
        self.blacklist: set = set()

        # 数据库服务
        db_config = self.config_obj.get('database', {})
        self.db = DatabaseService(db_config.get('sqlite_path', str(DATA_DIR / 'sovereign_hall.db')))

    # =========================================================================
    # 主流程
    # =========================================================================

    async def run_single_iteration(self):
        """运行单次完整流程"""
        self.iteration += 1
        self.system_stats.iteration = self.iteration

        print(f"\n{'='*80}")
        print(f"🔄 开始第 {self.iteration} 轮研究")
        print(f"{'='*80}")

        try:
            await self.db._init_db()
            await self.db.init_report_tables()
            if self.vector_db._db is None:
                await self.vector_db.initialize(self.llm)

            # 阶段1：信息收集
            docs = await self.stage1_information_harvest()

            # 阶段2：深度研报
            proposals = await self.stage2_deep_research(docs)

            # 阶段3：投委会审议
            if proposals:
                await self.stage3_ic_deliberation(proposals)

            # 阶段4：复盘进化
            await self.stage4_evolution()

            # 更新统计
            self._update_stats()

            print(f"\n✅ 第 {self.iteration} 轮完成")
            return True

        except Exception as e:
            logger.error(f"Iteration failed: {e}", exc_info=True)
            return False

    async def run_continuous(self, iterations: int = 0, interval: int = None):
        """
        持续运行

        Args:
            iterations: 运行轮数（0表示无限）
            interval: 每轮间隔（秒）
        """
        self.running = True
        self.system_stats.start_time = datetime.now()

        # 确定运行参数
        sys_config = self.config_obj.get('system', {})
        sleep_interval = interval or sys_config.get('iteration_interval', 1800)

        print(f"\n{'='*80}")
        print(f"🚀 君临殿启动 | 运行模式: {'无限' if iterations == 0 else f'{iterations}轮'}")
        print(f"{'='*80}\n")

        try:
            while self.running:
                # 0 表示无限运行
                if iterations > 0 and self.iteration >= iterations:
                    logger.info("达到最大运行轮数，停止")
                    break

                success = await self.run_single_iteration()

                if success:
                    # 保存检查点
                    await self._save_checkpoint()

                # 等待间隔
                if self.running:
                    logger.info(f"💤 休眠 {sleep_interval} 秒后进行下一轮...")
                    await asyncio.sleep(sleep_interval)

        except KeyboardInterrupt:
            logger.info("用户中断，停止运行")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """关闭系统"""
        print(f"\n{'='*80}")
        print("🛑 正在关闭君临殿...")
        print(f"{'='*80}")

        self.running = False

        # 保存最终状态
        await self._save_checkpoint()

        # 关闭组件
        await self.spiders.close()

        # 打印最终统计
        db_stats = await self.db.get_stats_summary()
        self._print_final_stats(db_stats)

    def _print_final_stats(self, db_stats: Dict = None):
        """打印最终统计"""
        print(f"\n{'='*60}")
        print("📊 最终统计")
        print(f"{'='*60}")
        print(f"  运行轮数: {self.iteration}")

        if db_stats:
            print(f"  文档数量: {db_stats.get('documents', 0)}")
            print(f"  提案数量: {db_stats.get('proposals', 0)}")
            print(f"  会议记录: {db_stats.get('meetings', 0)}")
            print(f"  经验条目: {db_stats.get('playbook', 0)}")
            print(f"  黑名单: {db_stats.get('blacklist', 0)}")

        llm_stats = self.llm.get_stats()
        spider_stats = self.spiders.get_stats()

        print(f"\n  Token消耗: {llm_stats.get('total_tokens', 0):,}")
        print(f"  预估成本: {llm_stats.get('total_cost_usd', '$0.0')}")
        print(f"\n  爬虫成功: {spider_stats['success']}")
        print(f"  爬虫失败: {spider_stats['fail']}")
        print(f"  成功率: {spider_stats.get('success_rate', 'N/A')}")
        print(f"{'='*60}")

    # =========================================================================
    # 阶段1：信息收集
    # =========================================================================

    async def stage1_information_harvest(self) -> List[Document]:
        """阶段1：广域信息收割"""
        print(f"\n{'='*60}")
        print("📡 阶段1：广域信息收割")
        print(f"{'='*60}")

        # 生成搜索词
        query_gen = SearchQueryGenerator(self.llm)
        queries = await query_gen.generate_queries(count=50)

        print(f"\n生成了 {len(queries)} 个搜索词")
        print(f"示例: {queries[:5]}")

        # 并发搜索和抓取
        raw_docs = await self.spiders.aggressive_search(
            queries,
            max_results_per_query=20,
        )

        print(f"\n抓取了 {len(raw_docs)} 篇文档")

        # 过滤和清洗
        cleaned_docs = await self._clean_documents(raw_docs)

        # 存入向量库和数据库
        for doc in cleaned_docs:
            await self.vector_db.add_document(doc, llm_client=self.llm)
            await self.db.add_document(doc)

        doc_count = await self.db.count_documents()
        self.system_stats.documents_indexed = doc_count
        print(f"✅ 数据库中共有 {doc_count} 篇文档\n")

        return cleaned_docs

    async def _clean_documents(self, docs: List[Document]) -> List[Document]:
        """清洗文档"""
        if not docs:
            return []

        # 过滤太短的文档
        cleaned = [d for d in docs if len(d.content) >= 200]

        # 去除重复URL
        seen_urls = set()
        unique_docs = []
        for doc in cleaned:
            if doc.url not in seen_urls:
                seen_urls.add(doc.url)
                unique_docs.append(doc)

        return unique_docs

    # =========================================================================
    # 阶段2：深度研报
    # =========================================================================

    async def stage2_deep_research(self, docs: List[Document]) -> List[InvestmentProposal]:
        """阶段2：深度研报生成"""
        if not docs:
            print("\n⚠️ 没有文档，跳过深度研究阶段")
            return []

        print(f"\n{'='*60}")
        print("📖 阶段2：深度研报生成")
        print(f"{'='*60}")

        # 识别热点赛道
        sector_counts = self._count_sectors(docs)
        hot_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:3]

        print(f"\n热点赛道：{[s[0] for s in hot_sectors]}")

        proposals = []

        for sector, count in hot_sectors:
            print(f"\n📊 研究赛道：{sector} (相关文档: {count})")

            # 检索该赛道的文档
            sector_docs = await self.vector_db.search(
                query=sector,
                top_k=30,
                filter_sector=sector,
                llm_client=self.llm,
            )

            if not sector_docs:
                continue

            # 选择专业分析师
            analyst = self.analysts.get_specialist(sector)

            # 生成深度报告
            report = await analyst.research_sector(sector, sector_docs, max_output_tokens=8000)

            print(f"   生成了 {len(report)} 字的深度报告")

            # 从报告中提取提案（简化处理）
            proposal = await self._extract_proposal_from_report(sector, report, analyst.role)
            if proposal:
                proposals.append(proposal)
                print(f"   ✅ 生成提案: {proposal.ticker} ({proposal.direction})")

        print(f"\n✅ 阶段2完成，生成 {len(proposals)} 个投资提案\n")
        return proposals

    def _count_sectors(self, docs: List[Document]) -> dict:
        """统计行业分布"""
        counts = {}
        for doc in docs:
            sector = doc.sector or "未知"
            counts[sector] = counts.get(sector, 0) + 1
        return counts

    async def _extract_proposal_from_report(
        self,
        sector: str,
        report: str,
        analyst_role: AgentRole,
    ) -> Optional[InvestmentProposal]:
        """从报告中提取提案"""
        # 使用LLM解析
        extract_prompt = f"""
从以下行业报告中提取投资提案信息：

{report[:3000]}...

请提取以下信息（如果没有明确的标的，返回None）：
1. 推荐的股票/ETF代码
2. 投资方向（long/short）
3. 建议仓位（0-1之间）
4. 入场价
5. 止损价
6. 止盈价
7. 持有天数
8. 置信度（0-1之间）

输出JSON格式：
{{
    "ticker": "代码",
    "direction": "long",
    "target_position": 0.1,
    "entry_price": 100.0,
    "stop_loss": 90.0,
    "take_profit": 130.0,
    "holding_period": 90,
    "confidence": 0.7
}}
如果无法提取，返回{{"ticker": null}}
"""

        try:
            response = await self.llm.chat(
                system="你是投资提案提取专家",
                user=extract_prompt,
                temperature=0.3,
                max_tokens=500,
            )

            try:
                result = json.loads(response)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse LLM response as JSON: {e}, response: {response[:200]}...")
                return None

            if not result.get('ticker'):
                return None

            # 创建提案
            return InvestmentProposal.create(
                ticker=result['ticker'],
                analyst_role=analyst_role,
                direction=result.get('direction', 'long'),
                target_position=result.get('target_position', 0.1),
                entry_price=result.get('entry_price', 100.0),
                stop_loss=result.get('stop_loss', 90.0),
                take_profit=result.get('take_profit', 130.0),
                holding_period=result.get('holding_period', 90),
                thesis=report[:5000],  # 限制长度
                supporting_evidence=[],
                risks=[],
                catalysts=[],
                confidence=result.get('confidence', 0.6),
            )

        except Exception as e:
            logger.error(f"Failed to extract proposal: {e}")
            return None

    # =========================================================================
    # 阶段3：投委会审议
    # =========================================================================

    async def stage3_ic_deliberation(self, proposals: List[InvestmentProposal]):
        """阶段3：投委会审议"""
        if not proposals:
            return

        print(f"\n{'='*60}")
        print("🔥 阶段3：投委会审议 (Token焚烧炉)")
        print(f"{'='*60}")

        for i, proposal in enumerate(proposals):
            print(f"\n### 提案 {i+1}/{len(proposals)}: {proposal.ticker} ({proposal.direction})")

            # 获取提案人
            proposer = self.analysts.get_agent(proposal.analyst_role)
            if not proposer:
                proposer = self.analysts.get_specialist(proposal.sector)

            # 检索历史教训
            lessons = await self._get_relevant_lessons(proposal)
            lessons_context = "\n".join([f"- {l.lesson}" for l in lessons[:5]]) if lessons else "无相关历史记录"

            # 召开会议
            minutes = await self.ic.hold_meeting(proposal, proposer, lessons_context)

            # 更新统计
            self.system_stats.total_meetings += 1

            # 处理裁决结果
            self._process_verdict(minutes)

            # 保存会议纪要到数据库
            await self.db.add_meeting(minutes)

        # 统计会议数量
        meeting_count = await self.db.count_meetings()
        print(f"\n✅ 阶段3完成，数据库共有 {meeting_count} 条会议记录\n")

    async def _get_relevant_lessons(self, proposal: InvestmentProposal) -> List[PlaybookEntry]:
        """检索相关历史教训"""
        query = f"{proposal.ticker} {proposal.direction}"
        results = await self.vector_db.search(query, top_k=5, llm_client=self.llm)

        relevant = []
        for entry in self.playbook:
            if entry.ticker == proposal.ticker or proposal.ticker in entry.situation:
                relevant.append(entry)

        return relevant[:5]

    def _process_verdict(self, minutes: ICMeetingMinutes):
        """处理裁决结果"""
        decision = minutes.decision

        self.system_stats.total_proposals += 1

        if decision == 'approve':
            self.system_stats.approved_proposals += 1
            print(f"✅ 提案通过")
        elif decision == 'reject':
            self.system_stats.rejected_proposals += 1
            print(f"❌ 提案拒绝")
        else:
            print(f"⏸️ 提案延期")

        # 更新爬虫统计
        spider_stats = self.spiders.get_stats()
        self.system_stats.spider_success = spider_stats['success']
        self.system_stats.spider_failed = spider_stats['fail']

    # =========================================================================
    # 阶段4：复盘进化
    # =========================================================================

    async def stage4_evolution(self):
        """阶段4：复盘与进化"""
        print(f"\n{'='*60}")
        print("🧬 阶段4：复盘与进化")
        print(f"{'='*60}")

        # 统计信息
        llm_stats = self.llm.get_stats()
        spider_stats = self.spiders.get_stats()

        print(f"\n【本轮统计】")
        print(f"  LLM调用: {llm_stats['total_requests']} 次")
        print(f"  Token消耗: {llm_stats['total_tokens']:,}")
        print(f"  预估成本: {llm_stats['total_cost_usd']}")
        print(f"  缓存命中: {llm_stats['cache_size']}")
        print(f"  爬虫成功: {spider_stats['success']}")
        print(f"  爬虫失败: {spider_stats['fail']}")

        # 保存机构记忆
        await self._save_playbook()

        # 更新Token统计
        self.system_stats.token_stats.total_tokens = int(llm_stats.get('total_tokens', 0))
        self.system_stats.token_stats.total_cost_usd = float(llm_stats.get('total_cost_usd', '0.0').replace('$', ''))

    # =========================================================================
    # 持久化
    # =========================================================================

    async def _save_playbook(self):
        """保存投资手册到数据库"""
        # 将playbook条目保存到数据库
        for entry in self.playbook:
            await self.db.add_playbook_entry(entry)

        # 更新黑名单
        for ticker in self.blacklist:
            await self.db.add_to_blacklist(ticker, "多次失败")

        # 获取统计
        playbook_count = await self.db.count_playbook()
        blacklist_count = len(self.blacklist)

        self.system_stats.playbook_entries = playbook_count
        self.system_stats.blacklisted_tickers = blacklist_count

        print(f"\n📖 数据库中共有 {playbook_count} 条经验，黑名单 {blacklist_count} 个")

    async def _save_checkpoint(self):
        """保存检查点到数据库"""
        blacklist = await self.db.get_blacklist()
        await self.db.save_checkpoint(
            iteration=self.iteration,
            stats=self.system_stats.to_dict(),
            blacklist=blacklist
        )
        # 清理旧检查点
        await self.db.clear_old_checkpoints(keep=10)
        logger.info(f"💾 检查点已保存 (第{self.iteration}轮)")

    # =========================================================================
    # 统计和信息
    # =========================================================================

    def _update_stats(self):
        """更新系统统计"""
        llm_stats = self.llm.get_stats()
        spider_stats = self.spiders.get_stats()

        self.system_stats.token_stats.total_tokens = int(llm_stats.get('total_tokens', 0))
        self.system_stats.token_stats.total_requests = int(llm_stats.get('total_requests', 0))
        self.system_stats.token_stats.successful_requests = int(llm_stats.get('successful_requests', 0))

        self.system_stats.spider_success = spider_stats['success']
        self.system_stats.spider_failed = spider_stats['fail']
        self.system_stats.documents_indexed = len(self.vector_db)

    def get_status(self) -> Dict:
        """获取系统状态"""
        return {
            'running': self.running,
            'iteration': self.iteration,
            'stats': self.system_stats.to_dict(),
            'components': {
                'llm': self.llm.get_stats(),
                'spider': self.spiders.get_stats(),
                'vector_db': self.vector_db.get_stats(),
                'analysts': len(self.analysts),
                'playbook_entries': len(self.playbook),
                'blacklist_size': len(self.blacklist),
            }
        }

    async def get_full_stats(self) -> Dict:
        """获取完整统计（从数据库）"""
        db_stats = await self.db.get_stats_summary()
        llm_stats = self.llm.get_stats()
        spider_stats = self.spiders.get_stats()

        return {
            'iteration': self.iteration,
            'database': db_stats,
            'llm': llm_stats,
            'spider': spider_stats,
        }

    def __repr__(self):
        return f"SovereignHall(iteration={self.iteration}, running={self.running})"
