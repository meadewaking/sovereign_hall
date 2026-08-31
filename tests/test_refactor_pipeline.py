import asyncio
import csv
import sqlite3
import json
import inspect
import importlib.util
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.core import AgentRole, Document, PlaybookEntry
from sovereign_hall.core.config import get_config
from sovereign_hall.agents import get_persona
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.decision_tracker import DecisionRecorder
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.portfolio_policy import (
    deployment_position_floor,
    deployment_status,
    review_position,
)
from sovereign_hall.services.reward_policy import (
    MAX_DAILY_TRADES,
    capital_reward_breakdown,
    idle_cash_exposure_penalty,
    limit_rebalance_actions,
)
from sovereign_hall.services.simulation_performance import (
    PERFORMANCE_STANDARD,
    build_simulation_performance,
)
from sovereign_hall.services.heuristic_policy import (
    HeuristicRiskContext,
    apply_heuristic_risk_cap,
    build_price_readiness_stall_report,
    derive_simulation_risk_memory,
    failure_ticker_constraints,
    format_heuristic_prompt_context,
    format_heuristic_status,
    format_price_readiness_backfill_plan,
    format_price_readiness_backfill_queue,
    format_price_readiness_stall_note,
    format_policy_checklist,
    prepare_candidate_rejection_feedback,
    recent_prediction_observation_count,
    sanitize_candidate_rejection_reason,
)
from sovereign_hall.services.market_data import (
    MarketDataService,
    collect_realtime_quote_batch,
)
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.spider_service import SearchQueryGenerator, SpiderSwarm
from sovereign_hall.services.learning_engine import LearningEngine
from sovereign_hall.services.research_discussion import ResearchDiscussionSystem
from sovereign_hall.services.prediction_tracker import PredictionTracker
from sovereign_hall.services.backtest_engine import get_backtest_engine
from sovereign_hall.services.prediction_store import ensure_prediction_tables
from sovereign_hall.utils import format_cost_breakdown, format_token, format_token_breakdown
from sovereign_hall.run_discussion import (
    TOPIC_POOL,
    aggregate_committee_decision,
    build_balanced_vote_context,
    choose_review_depth,
    build_proposal_thesis,
    build_lessons_with_heuristic_context,
    bound_round_source_lineage,
    bounded_sync_index_batch,
    cli_args_can_run_without_instance_lock,
    committee_decision_is_predictable,
    committee_deadlock_requires_review,
    committee_role_weight,
    committee_think_from_persisted_evidence,
    collect_committee_results,
    retry_absent_committee_results,
    build_deployment_evidence_queries,
    filter_repeated_rejection_proposals,
    extract_stage2_candidate_windows,
    extract_stage2_proposal_array,
    format_stage2_diagnostic_context,
    kill_existing_run_discussion_instances,
    merge_committee_deadlock_review,
    parse_committee_vote,
    parse_args,
    preflight_committee_decisions,
    merge_documents_prefer_richer,
    normalize_stage2_evidence,
    proposal_priority_score,
    prioritize_deployment_research,
    rank_stage2_documents,
    round_has_operational_result,
    select_stage2_candidate_source_excerpts,
    select_simulation_terminal,
    select_next_topic,
    load_recent_topics,
    stage2_deep_research,
    stage3_ic_discussion,
    run_committee_approved_simulation,
    run_with_graceful_shutdown,
)
from sovereign_hall.services.persistence import PersistenceManager
from sovereign_hall.domain.research import ResearchRound, ResearchRoundStatus
import sovereign_hall.services.persistence as persistence_module


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_script_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_entry_imports():
    import sovereign_hall.check_db  # noqa: F401
    import sovereign_hall.research_interactive  # noqa: F401
    import sovereign_hall.run_discussion  # noqa: F401


def test_check_db_help_exits_before_database_or_realtime_reads(monkeypatch, capsys):
    import sovereign_hall.check_db as check_db_module

    monkeypatch.setattr(
        check_db_module,
        "show_stats",
        lambda *_args, **_kwargs: pytest.fail("--help must not read production state"),
    )

    with pytest.raises(SystemExit) as exc_info:
        check_db_module.main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "usage:" in output
    assert "realtime" in output
    assert "simulated-account score" in output


def test_simulation_account_return_is_the_only_authoritative_score():
    metrics = build_simulation_performance(
        initial_capital=10000.0,
        assets={
            "valuation_complete": True,
            "total_assets": 9727.22,
            "cash": 9727.22,
            "positions_value": 0.0,
            "invested_ratio": 0.0,
            "deployment_gap": 9727.22,
        },
        trade_count=28,
        recorded_fees=12.34,
        latest_trade_at="2026-07-14T09:57:44",
        now=datetime.fromisoformat("2026-07-29T15:00:00"),
    )

    assert metrics["performance_standard"] == PERFORMANCE_STANDARD
    assert metrics["score"] == pytest.approx(-0.027278)
    assert metrics["net_total_return"] == metrics["score"]
    assert metrics["recorded_fees"] == pytest.approx(12.34)
    assert metrics["recorded_cost_ratio"] == pytest.approx(0.001234)
    assert metrics["gross_total_return_before_recorded_cost"] == pytest.approx(
        -0.026044
    )
    assert metrics["offline_backtest_promotion_allowed"] is False
    assert metrics["health_status"] == "system_failure_no_live_deployment"


def test_incomplete_realtime_valuation_never_falls_back_to_offline_return():
    metrics = build_simulation_performance(
        initial_capital=10000.0,
        assets={
            "valuation_complete": False,
            "cash": 5000.0,
            "missing_price_tickers": ["600519"],
        },
        trade_count=1,
        recorded_fees=1.0,
        latest_trade_at="2026-07-29T09:45:00",
    )

    assert metrics["score"] is None
    assert metrics["net_total_return"] is None
    assert metrics["gross_total_return_before_recorded_cost"] is None
    assert metrics["health_status"] == "valuation_incomplete"
    assert metrics["missing_price_tickers"] == ["600519"]


@pytest.mark.asyncio
async def test_spider_local_only_hard_gate_blocks_network(monkeypatch):
    spider = SpiderSwarm(network_enabled=False)
    network_call = AsyncMock(side_effect=AssertionError("network search must not run"))
    monkeypatch.setattr(spider, "_search_single_query", network_call)

    docs = await spider.aggressive_search(["A股 最新消息"])

    assert docs == []
    network_call.assert_not_awaited()
    await spider.close()


@pytest.mark.asyncio
async def test_committee_retry_recovers_only_absent_roles():
    initial = [
        '{"direction":"long","confidence":0.8,"position":0.1}',
        "[committee_task_absent] role=risk",
        "[committee_task_absent] role=quant",
    ]
    audit = {
        "stage": "round4_vote",
        "task_count": 3,
        "completed_count": 1,
        "timeout_count": 2,
        "error_count": 0,
        "absent_labels": ["risk", "quant"],
        "tasks": [
            {"label": "cio", "status": "completed"},
            {"label": "risk", "status": "timeout"},
            {"label": "quant", "status": "timeout"},
        ],
    }
    factories = [
        ("cio", lambda: AsyncMock(return_value="must not run")()),
        (
            "risk",
            lambda: AsyncMock(
                return_value='{"direction":"long","confidence":0.7,"position":0.08}'
            )(),
        ),
        (
            "quant",
            lambda: AsyncMock(
                return_value='{"direction":"hold","confidence":0.6,"position":0}'
            )(),
        ),
    ]

    results, final_audit = await retry_absent_committee_results(
        initial,
        audit,
        factories,
        timeout_seconds=1,
        stage="round4_vote",
    )

    assert results[0] == initial[0]
    assert '"direction":"long"' in results[1]
    assert '"direction":"hold"' in results[2]
    assert final_audit["completed_count"] == 3
    assert final_audit["retry_attempted_count"] == 2
    assert final_audit["retry_recovered_count"] == 2


def test_stage2_parser_recovers_json_after_verbose_reasoning():
    response = """
让我先分析材料。
[1] 政策边际改善，但需要区分事实与推断。
最终结果：
[
  {
    "ticker": "600048",
    "direction": "long",
    "target_position": 0.1,
    "confidence": 0.7,
    "thesis": "事实: 销售改善；推断: 现金流修复"
  }
]
以上是结论。
"""

    proposals, mode = extract_stage2_proposal_array(response)

    assert mode == "embedded_array"
    assert [proposal["ticker"] for proposal in proposals] == ["600048"]


def test_stage2_parser_never_synthesizes_from_prose_only():
    proposals, mode = extract_stage2_proposal_array(
        "建议关注600048，但证据不足，本次不输出结构化提案。"
    )

    assert proposals == []
    assert mode == "unparsed"


def test_stage2_parser_marks_model_empty_array_as_auditable_empty_result():
    proposals, mode = extract_stage2_proposal_array("[]")

    assert proposals == []
    assert mode == "explicit_empty"


def test_stage2_document_ranking_prioritizes_code_and_auditable_operating_fact():
    generic = Document(
        title="行业展望",
        content="消费电子复苏趋势讨论。" * 30,
        source="unit",
    )
    concrete = Document(
        title="公司公告",
        content="证券代码301387，公告显示净利润同比增长25%，经营现金流改善。",
        source="unit",
    )

    ranked = rank_stage2_documents([generic, concrete])

    assert ranked[0] is concrete


def test_deployment_followup_queries_only_use_tickers_observed_in_documents():
    docs = [
        Document(
            title="行业异动",
            content="光大同创（301387）上涨，公告显示净利润同比增长25%。",
            source="unit",
        ),
        Document(
            title="无代码观点",
            content="建议关注某龙头，但资料没有证券代码。",
            source="unit",
        ),
    ]

    queries = build_deployment_evidence_queries(docs)

    assert queries == [
        "301387 公告 财报 现金流",
        "301387 机构调研 订单 业绩",
    ]
    assert all("推荐标的" not in query for query in queries)


def test_document_merge_replaces_snippet_with_same_url_full_text():
    snippet = Document(
        title="公司公告摘要",
        content="证券代码600519，现金流改善。",
        url="https://example.com/notice",
        source="duckduckgo",
    )
    full = Document(
        title="公司公告全文",
        content="证券代码600519，经营活动现金流同比改善，公告列示原因。" * 20,
        url="https://example.com/notice",
        source="example.com",
    )

    merged = merge_documents_prefer_richer([snippet], [full])

    assert merged == [full]


def test_search_query_generator_rejects_punctuation_only_placeholder():
    generator = SearchQueryGenerator(AsyncMock())

    assert generator._is_valid_query("...", topic="消费电子复苏前景") is False
    assert generator._is_valid_query("query1", topic="消费电子复苏前景") is False
    assert generator._is_valid_query("search_query_02", topic="消费电子复苏前景") is False
    assert generator._is_valid_query("词1", topic="消费电子复苏前景") is False
    assert generator._is_valid_query("搜索词2", topic="消费电子复苏前景") is False
    assert generator._is_valid_query("消费电子 财报", topic="消费电子复苏前景") is True


@pytest.mark.asyncio
async def test_search_query_generator_rejects_format_repair_instruction_leak():
    leaked_instruction = (
        "将下面原回答中已经明确写出的搜索词整理为合法JSON字符串数组。"
        "只保留与餐饮连锁扩张逻辑直接相关、可检索的词；只输出JSON数组。"
    )
    llm = AsyncMock()
    llm.chat.return_value = json.dumps(
        [leaked_instruction, "餐饮连锁门店同店收入"],
        ensure_ascii=False,
    )
    generator = SearchQueryGenerator(llm)

    queries = await generator.generate_queries(
        count=5,
        seeds={"macro": [], "sector": ["餐饮连锁"], "stocks": []},
        topic="餐饮连锁扩张逻辑",
    )

    assert queries == ["餐饮连锁门店同店收入"]
    assert generator.last_validation_report["rejection_counts"] == {
        "meta_instruction_leak": 1,
    }
    assert generator.last_validation_report["accepted_count"] == 1


@pytest.mark.asyncio
async def test_spider_network_boundary_rejects_meta_instruction_defense_in_depth():
    spider = SpiderSwarm(max_concurrent=1, timeout=1, retry_times=1)
    spider._search_single_query = AsyncMock(return_value=[])
    leaked_instruction = "将下面原回答中的搜索词整理为合法JSON；只输出JSON数组。"
    try:
        docs = await spider.aggressive_search(
            [leaked_instruction, "云计算 财报"],
            max_results_per_query=1,
        )
    finally:
        await spider.close()

    assert docs == []
    spider._search_single_query.assert_awaited_once()
    assert spider._search_single_query.await_args.args[0] == "云计算 财报"
    assert spider.last_query_gate_report["rejection_counts"] == {
        "meta_instruction_leak": 1,
    }


@pytest.mark.asyncio
async def test_spider_isolates_failing_provider_with_circuit_breaker(monkeypatch):
    spider = SpiderSwarm(max_concurrent=10, timeout=1, retry_times=1)
    spider.search_interval = 0
    spider.provider_failure_threshold = 2
    spider.provider_cooldown_seconds = 300
    healthy_doc = Document(
        id="doc_ddg",
        title="可追溯研究资料",
        content="这是一条足够长且可用于验证 provider 隔离行为的研究摘要。",
        url="https://example.com/research",
        source="duckduckgo",
        publish_time=datetime.now(),
        sector="TMT",
        keywords=["云计算"],
    )
    ddg = AsyncMock(return_value=[healthy_doc])
    bing = AsyncMock(return_value=[])
    monkeypatch.setattr(spider, "_ddg_search", ddg)
    monkeypatch.setattr(spider, "_bing_search", bing)

    try:
        for query in ("云计算 财报", "云计算 订单", "云计算 现金流"):
            await spider._do_search(
                query,
                max_results=1,
                sources=["ddg", "bing"],
            )
    finally:
        await spider.close()

    assert ddg.await_count == 3
    assert bing.await_count == 2
    provider_report = spider.get_provider_health_report()
    assert provider_report["circuit_open_sources"] == ["bing"]
    assert provider_report["skipped_open_circuit_counts"] == {"bing": 1}
    assert provider_report["states"]["ddg"]["circuit_state"] == "closed"
    assert provider_report["states"]["bing"]["consecutive_failures"] == 2


@pytest.mark.asyncio
async def test_spider_circuit_bounds_failures_inside_one_concurrent_batch(monkeypatch):
    spider = SpiderSwarm(max_concurrent=20, timeout=1, retry_times=1)
    spider.provider_failure_threshold = 2
    spider.provider_cooldown_seconds = 300
    healthy_doc = Document(
        id="doc_batch_ddg",
        title="批量查询可追溯资料",
        content="健康搜索源持续返回足够长的摘要，失败搜索源应在同一批次内被隔离。",
        url="https://example.com/batch-research",
        source="duckduckgo",
        publish_time=datetime.now(),
        sector="TMT",
        keywords=["算力"],
    )
    ddg = AsyncMock(return_value=[healthy_doc])
    bing = AsyncMock(return_value=[])
    monkeypatch.setattr(spider, "_ddg_search", ddg)
    monkeypatch.setattr(spider, "_bing_search", bing)
    queries = [f"算力 证据 {index}" for index in range(8)]

    try:
        await asyncio.gather(
            *(
                spider._do_search(
                    query,
                    max_results=1,
                    sources=["ddg", "bing"],
                )
                for query in queries
            )
        )
    finally:
        await spider.close()

    report = spider.get_provider_health_report()
    assert ddg.await_count == len(queries)
    assert 2 <= bing.await_count <= 3
    assert report["skipped_open_circuit_counts"]["bing"] == (
        len(queries) - bing.await_count
    )
    assert report["circuit_open_sources"] == ["bing"]


@pytest.mark.asyncio
async def test_search_query_generator_drops_numbered_placeholders_before_search():
    llm = AsyncMock()
    llm.chat.return_value = json.dumps(
        ["query1", "词1", "搜索词2", "固态电池设备订单"],
        ensure_ascii=False,
    )
    generator = SearchQueryGenerator(llm)

    queries = await generator.generate_queries(
        count=5,
        seeds={"macro": [], "sector": ["固态电池"], "stocks": []},
        topic="固态电池技术路线",
    )

    assert queries == ["固态电池设备订单"]
    assert llm.chat.await_count == 1


def test_rejected_candidate_is_not_classified_as_no_evidence():
    rejection = {
        "ticker": "605338",
        "code": "proposal_lot_infeasible",
        "reason": "fresh whole-lot quote exceeds executable ceiling",
    }

    assert select_simulation_terminal(
        round_fill_count=0,
        pending_count=0,
        trade_candidates=[],
        decisions=[],
        rejections=[rejection],
    ) == "execution_rejected"
    assert select_simulation_terminal(
        round_fill_count=0,
        pending_count=0,
        trade_candidates=[],
        decisions=[],
        rejections=[],
    ) == "no_evidence"
    assert select_simulation_terminal(
        round_fill_count=0,
        pending_count=0,
        trade_candidates=[],
        decisions=[],
        rejections=[{"code": "system_failure_no_live_deployment"}],
    ) == "no_evidence"


def test_source_persisted_round_accepts_rejected_candidate_terminal():
    round_state = ResearchRound(
        base_topic="candidate screening",
        research_objective="retain exact rejection terminal",
        status=ResearchRoundStatus.SOURCES_PERSISTED,
        current_stage=ResearchRoundStatus.SOURCES_PERSISTED.value,
    )

    rejected = round_state.transition(
        ResearchRoundStatus.EXECUTION_REJECTED,
        terminal_code="execution_rejected",
        terminal_reason="all candidates rejected by auditable hard gates",
    )

    assert rejected.status == ResearchRoundStatus.EXECUTION_REJECTED
    assert rejected.terminal_code == "execution_rejected"


@pytest.mark.asyncio
async def test_search_query_generator_repairs_reasoning_without_inventing_ticker():
    class ReasoningQueryLLM:
        def __init__(self):
            self.calls = []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return (
                    "可检索对象包括中国中免601888和海南机场600515。"
                    "建议查询中国中免601888财报、海南机场600515现金流。"
                )
            return json.dumps(
                ["中国中免601888财报", "海南机场600515现金流"],
                ensure_ascii=False,
            )

    llm = ReasoningQueryLLM()
    generator = SearchQueryGenerator(llm)

    queries = await generator.generate_queries(
        count=5,
        seeds={"macro": [], "sector": ["免税店"], "stocks": []},
        topic="免税店竞争格局",
    )

    assert queries == ["中国中免601888财报", "海南机场600515现金流"]
    assert len(llm.calls) == 2
    assert "银行 股息率 2025" not in llm.calls[0]["user"]
    assert "免税店竞争格局 政策进展" in llm.calls[0]["user"]
    assert llm.calls[1]["temperature"] == 0.0
    assert llm.calls[1]["use_cache"] is False


def test_stage2_parser_does_not_let_trailing_empty_array_erase_candidate_text():
    proposals, mode = extract_stage2_proposal_array(
        "资料支持德赛西威002920作为多头提案，订单证据充分。\n最终输出：[]"
    )

    assert proposals == []
    assert mode == "ambiguous_empty_with_candidate_text"


def test_stage2_candidate_windows_only_capture_tickers_already_in_response():
    windows = extract_stage2_candidate_windows(
        "先讨论行业，再评估600515的订单证据；没有提及其他标的。"
    )

    assert [item["ticker"] for item in windows] == ["600515"]
    assert "600515" in windows[0]["excerpt"]


def test_stage2_candidate_windows_keep_late_explicit_etf_before_first_n_cut():
    response = "\n".join(
        [
            f"候选公司 {ticker} 有经营数据，但价格或整手可执行性尚未确认。"
            for ticker in (
                "600031", "603338", "601100", "000680",
                "000425", "600761", "000528", "000157",
            )
        ]
        + [
            "工程机械ETF 560280 是资料明确比较的可执行多头投资提案，"
            "规模同比增长且资金净流入。"
        ]
    )

    windows = extract_stage2_candidate_windows(response, radius=120, limit=8)

    assert "560280" in [item["ticker"] for item in windows]
    assert len(windows) == 8


def test_stage2_candidate_sources_cover_each_ticker_before_second_excerpt():
    docs = [
        "【A1】600031 订单同比增长\n来源: local://a1",
        "【A2】600031 现金流改善\n来源: local://a2",
        "【ETF1】560280 规模增长\n来源: local://etf1",
        "【ETF2】560280 资金净流入\n来源: local://etf2",
    ]

    selected, coverage = select_stage2_candidate_source_excerpts(
        docs,
        ["600031", "560280"],
        limit=2,
    )

    assert any("600031" in item for item in selected)
    assert any("560280" in item for item in selected)
    assert coverage == {"600031": 2, "560280": 2}


def test_stage2_evidence_rejects_scalar_and_character_arrays():
    assert normalize_stage2_evidence("金融周报推荐关注") == (
        [],
        "invalid_evidence_container",
    )
    assert normalize_stage2_evidence(["金", "融", "周", "报"]) == (
        [],
        "invalid_evidence_item",
    )
    assert normalize_stage2_evidence(
        ["公司公告：净息差环比企稳", "公司季报：不良率下降"]
    ) == (
        ["公司公告：净息差环比企稳", "公司季报：不良率下降"],
        "",
    )


@pytest.mark.asyncio
async def test_stage2_scalar_evidence_cannot_reach_committee_or_storage():
    class ScalarEvidenceLLM:
        async def chat(self, **_kwargs):
            return json.dumps([
                {
                    "ticker": "601916",
                    "direction": "long",
                    "target_position": 0.08,
                    "stop_loss": 5.0,
                    "take_profit": 10.0,
                    "holding_period": 30,
                    "holding_period_reason": "等待季报验证",
                    "confidence": 0.65,
                    "thesis": "事实: 周报推荐关注；推断: 股息较稳定",
                    "sector": "银行",
                    "evidence": "金融周报推荐关注",
                    "resolved_rejection": "",
                    "evidence_delta": "",
                    "reject_if": "股息率低于行业中位数",
                }
            ], ensure_ascii=False)

    db = AsyncMock()
    db.get_blacklist.return_value = []
    doc = Document(
        title="银行周报",
        content="金融周报推荐关注601916，但没有给出结构化财务证据。" * 4,
        url="https://example.com/601916",
        source="unit",
    )

    proposals = await stage2_deep_research(
        ScalarEvidenceLLM(),
        [doc],
        "银行股高股息价值",
        db_service=db,
    )

    assert proposals == []
    diagnostic = db.record_research_stage_diagnostic.await_args.kwargs
    assert diagnostic["status"] == "empty_after_adjudication"
    assert '"invalid_evidence_container": 1' in diagnostic["reason"]


@pytest.mark.asyncio
async def test_stage2_repairs_reasoning_only_response_without_fallback_ticker():
    class ReasoningThenRepairLLM:
        def __init__(self):
            self.calls = []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return (
                    "分析过程：资料明确支持贵州茅台600519的现金流改善，"
                    "并把它作为多头提案；事实来自本地财报摘要。"
                )
            return json.dumps([
                {
                    "ticker": "600519",
                    "direction": "long",
                    "target_position": 0.08,
                    "stop_loss": 6.0,
                    "take_profit": 12.0,
                    "holding_period": 30,
                    "holding_period_reason": "等待下一次月度经营数据验证",
                    "confidence": 0.72,
                    "thesis": "事实: 本地财报摘要显示现金流改善；推断: 估值有修复空间",
                    "sector": "食品饮料",
                    "evidence": ["本地财报摘要：经营现金流改善"],
                    "resolved_rejection": "",
                    "evidence_delta": "",
                    "reject_if": "经营现金流重新恶化"
                }
            ], ensure_ascii=False)

    doc = Document(
        title="贵州茅台本地财报摘要",
        content="本地财报摘要显示经营现金流改善，且资料明确标注股票代码600519。" * 3,
        url="local://evidence/600519",
        source="unit",
    )
    llm = ReasoningThenRepairLLM()

    proposals = await stage2_deep_research(llm, [doc], "现金流验证")

    assert [proposal["ticker"] for proposal in proposals] == ["600519"]
    assert len(llm.calls) == 2
    assert llm.calls[1]["temperature"] == 0.0
    assert llm.calls[1]["use_cache"] is False


@pytest.mark.asyncio
async def test_stage2_repairs_candidate_text_even_when_response_ends_with_empty_array():
    class CandidateThenEmptyLLM:
        def __init__(self):
            self.calls = []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return (
                    "资料明确支持德赛西威002920作为多头提案，"
                    "事实为本地订单摘要显示新增订单，方向为long。\n[]"
                )
            return json.dumps([
                {
                    "ticker": "002920",
                    "direction": "long",
                    "target_position": 0.1,
                    "stop_loss": 6.0,
                    "take_profit": 12.0,
                    "holding_period": 30,
                    "holding_period_reason": "等待月度订单验证",
                    "confidence": 0.7,
                    "thesis": "事实: 本地订单摘要显示新增订单；推断: 收入有望改善",
                    "sector": "汽车电子",
                    "evidence": ["本地订单摘要：新增订单"],
                    "resolved_rejection": "",
                    "evidence_delta": "",
                    "reject_if": "新增订单未转化为收入"
                }
            ], ensure_ascii=False)

    doc = Document(
        title="德赛西威本地订单摘要",
        content="本地订单摘要显示新增订单，资料明确标注股票代码002920。" * 3,
        url="local://evidence/002920",
        source="unit",
    )
    llm = CandidateThenEmptyLLM()

    proposals = await stage2_deep_research(llm, [doc], "订单验证")

    assert [proposal["ticker"] for proposal in proposals] == ["002920"]
    assert len(llm.calls) == 2


@pytest.mark.asyncio
async def test_stage2_adjudicates_candidate_after_format_repair_stays_empty():
    class CandidateRepairAdjudicationLLM:
        def __init__(self):
            self.calls = []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return (
                    "原资料中的600515具有订单增长与现金流改善两条证据，"
                    "可作为long候选，但最终格式错误。\n[]"
                )
            if len(self.calls) == 2:
                return "[]"
            return json.dumps([
                {
                    "ticker": "600515",
                    "direction": "long",
                    "target_position": 0.08,
                    "stop_loss": 6.0,
                    "take_profit": 12.0,
                    "holding_period": 30,
                    "holding_period_reason": "等待下一月订单验证",
                    "confidence": 0.7,
                    "thesis": "事实: 订单增长且现金流改善；推断: 盈利质量提升",
                    "sector": "交通服务",
                    "evidence": ["公司公告：订单增长", "公司公告：现金流改善"],
                    "resolved_rejection": "",
                    "evidence_delta": "本轮公司公告补充两条经营证据",
                    "reject_if": "订单取消或现金流重新恶化",
                }
            ], ensure_ascii=False)

    db = AsyncMock()
    db.get_blacklist.return_value = []
    doc = Document(
        title="600515公司公告",
        content=(
            "证券代码600515，公司公告披露新增订单增长；"
            "经营活动现金流同比改善，数据可追溯。"
        ) * 3,
        url="https://example.com/600515",
        source="unit",
    )
    llm = CandidateRepairAdjudicationLLM()

    proposals = await stage2_deep_research(
        llm,
        [doc],
        "空仓资金部署候选证据比较",
        db_service=db,
    )

    assert [proposal["ticker"] for proposal in proposals] == ["600515"]
    assert len(llm.calls) == 3
    diagnostic = db.record_research_stage_diagnostic.await_args.kwargs
    assert diagnostic["status"] == "proposals_recovered"
    assert diagnostic["detected_tickers"] == ["600515"]
    assert diagnostic["repair_modes"] == [
        "format:explicit_empty",
        "candidate_adjudication:generic_parser",
    ]


@pytest.mark.asyncio
async def test_stage2_repairs_candidate_adjudicator_format_loss_without_new_ticker():
    class VerboseAdjudicationThenRepairLLM:
        def __init__(self):
            self.calls = []

        async def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return (
                    "原资料中的600515具有订单增长与现金流改善两条证据，"
                    "可作为long候选，但最终格式错误。\n[]"
                )
            if len(self.calls) == 2:
                return "[]"
            if len(self.calls) == 3:
                return (
                    "审计结论：600515的两条公司公告证据可独立回链，明确通过long提案审计；"
                    "601888不在允许集合，不得使用。\n[]"
                )
            return json.dumps([
                {
                    "ticker": "600515",
                    "direction": "long",
                    "target_position": 0.08,
                    "stop_loss": 6.0,
                    "take_profit": 12.0,
                    "holding_period": 30,
                    "holding_period_reason": "等待下一月订单验证",
                    "confidence": 0.7,
                    "thesis": "事实: 订单增长且现金流改善；推断: 盈利质量提升",
                    "sector": "交通服务",
                    "evidence": ["公司公告：订单增长", "公司公告：现金流改善"],
                    "resolved_rejection": "",
                    "evidence_delta": "本轮公司公告补充两条经营证据",
                    "reject_if": "订单取消或现金流重新恶化",
                },
                {
                    "ticker": "601888",
                    "direction": "long",
                    "target_position": 0.08,
                    "stop_loss": 6.0,
                    "take_profit": 12.0,
                    "holding_period": 30,
                    "holding_period_reason": "不允许的新标的",
                    "confidence": 0.7,
                    "thesis": "不得采用",
                    "sector": "消费",
                    "evidence": ["不得采用"],
                    "resolved_rejection": "",
                    "evidence_delta": "",
                    "reject_if": "",
                },
            ], ensure_ascii=False)

    db = AsyncMock()
    db.get_blacklist.return_value = []
    doc = Document(
        title="600515公司公告",
        content=(
            "证券代码600515，公司公告披露新增订单增长；"
            "经营活动现金流同比改善，数据可追溯。"
        ) * 3,
        url="https://example.com/600515",
        source="unit",
    )
    llm = VerboseAdjudicationThenRepairLLM()

    proposals = await stage2_deep_research(
        llm,
        [doc],
        "空仓资金部署候选证据比较",
        db_service=db,
    )

    assert [proposal["ticker"] for proposal in proposals] == ["600515"]
    assert len(llm.calls) == 4
    assert llm.calls[3]["temperature"] == 0.0
    assert llm.calls[3]["use_cache"] is False
    assert '"600515"' in llm.calls[3]["user"]
    diagnostic = db.record_research_stage_diagnostic.await_args.kwargs
    assert diagnostic["status"] == "proposals_recovered"
    assert diagnostic["repair_modes"] == [
        "format:explicit_empty",
        "candidate_adjudication:ambiguous_empty_with_candidate_text",
        "candidate_adjudication_format:generic_parser",
    ]


@pytest.mark.asyncio
async def test_stage2_format_repair_cannot_introduce_unseen_ticker():
    class InventingRepairLLM:
        def __init__(self):
            self.calls = 0

        async def chat(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return "资料不足，只有泛行业分析，结构化输出缺失。"
            return json.dumps([{
                "ticker": "600519",
                "direction": "long",
                "target_position": 0.1,
                "stop_loss": 5,
                "take_profit": 10,
                "holding_period": 30,
                "confidence": 0.8,
                "thesis": "格式修复器自行新增的标的",
                "evidence": ["不存在的证据"],
            }], ensure_ascii=False)

    doc = Document(
        title="无标的行业摘要",
        content="这是一段没有证券代码、没有具体公司事实的泛行业资料。" * 4,
        url="https://example.com/industry",
        source="unit",
    )
    llm = InventingRepairLLM()

    proposals = await stage2_deep_research(llm, [doc], "行业分析")

    assert proposals == []
    assert llm.calls == 2


@pytest.mark.asyncio
async def test_stage2_persists_candidate_bearing_empty_for_next_round(tmp_path):
    class StillEmptyLLM:
        def __init__(self):
            self.calls = 0

        async def chat(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return "600515只是待核实候选，资料不足，不能形成提案。\n[]"
            return "[]"

    db = DatabaseService(str(tmp_path / "stage2.db"))
    await db._init_db()
    doc = Document(
        title="600515待核实摘要",
        content="证券代码600515出现在摘要中，但没有两条独立经营证据。" * 4,
        url="https://example.com/unverified-600515",
        source="unit",
    )

    proposals = await stage2_deep_research(
        StillEmptyLLM(),
        [doc],
        "空仓资金部署候选证据比较",
        db_service=db,
    )
    diagnostics = await db.get_recent_research_stage_diagnostics(limit=1)
    await db.close()

    assert proposals == []
    assert diagnostics[0]["status"] == "empty_after_adjudication"
    assert json.loads(diagnostics[0]["detected_tickers"]) == ["600515"]
    context = format_stage2_diagnostic_context(diagnostics)
    assert "不是当前市场事实" in context
    assert "600515" in context


@pytest.mark.asyncio
async def test_check_db_reports_persisted_stage2_candidate_loss(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "stage2_audit.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.record_research_stage_diagnostic(
        topic="空仓资金部署候选证据比较",
        stage="stage2",
        status="empty_after_adjudication",
        parse_mode="ambiguous_empty_with_candidate_text",
        repair_modes=["format:explicit_empty"],
        detected_tickers=["600515"],
        reason="format repair stayed empty",
    )
    await db.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    diagnostics = check_db.research_stage_diagnostics(conn)
    conn.close()

    assert diagnostics["available"] is True
    assert diagnostics["total"] == 1
    assert diagnostics["candidate_bearing_empty_count"] == 1
    assert json.loads(diagnostics["recent"][0]["detected_tickers"]) == ["600515"]


def test_run_discussion_defaults_to_network_research():
    args = parse_args([])

    assert args.local_only is False
    assert args.skip_preflight is False
    assert parse_args(["--local-only"]).local_only is True


def test_run_discussion_help_does_not_need_instance_lock():
    assert cli_args_can_run_without_instance_lock(["--help"]) is True
    assert cli_args_can_run_without_instance_lock(["--once"]) is False


def test_runner_cleanup_protects_own_screen_ancestor(monkeypatch):
    import sovereign_hall.run_discussion as runner

    ps_output = "\n".join([
        "700 1 SCREEN -L -dmS sovereign_hall_prod zsh -lc python /repo/run_discussion.py",
        "600 700 zsh -lc python /repo/run_discussion.py",
        "500 600 python /repo/run_discussion.py",
        "400 1 python /old/run_discussion.py",
    ])
    subprocess_calls = []
    signals = []

    class Result:
        stdout = ps_output

    def fake_run(command, **kwargs):
        subprocess_calls.append((command, kwargs))
        return Result()

    def fake_kill(pid, signal):
        signals.append((pid, signal))
        if pid == 400 and signal == 0:
            raise ProcessLookupError

    monkeypatch.setattr(runner.subprocess if hasattr(runner, "subprocess") else __import__("subprocess"), "run", fake_run)
    monkeypatch.setattr(runner.os, "getpid", lambda: 500)
    monkeypatch.setattr(runner.os, "kill", fake_kill)

    killed = kill_existing_run_discussion_instances()

    assert killed == [400]
    assert (400, 15) in signals
    assert not any(pid in {500, 600, 700} for pid, _signal in signals)
    assert subprocess_calls[0][0] == ["ps", "-eo", "pid=,ppid=,args="]


def test_runner_cleanup_refuses_sigkill_after_full_grace_period(monkeypatch):
    import sovereign_hall.run_discussion as runner

    class Result:
        stdout = "400 1 python /old/run_discussion.py"

    signals = []
    sleeps = []

    monkeypatch.setattr(__import__("subprocess"), "run", lambda *args, **kwargs: Result())
    monkeypatch.setattr(runner.os, "getpid", lambda: 500)
    monkeypatch.setattr(runner.time, "sleep", sleeps.append)

    def fake_kill(pid, sig):
        signals.append((pid, sig))

    monkeypatch.setattr(runner.os, "kill", fake_kill)

    assert kill_existing_run_discussion_instances() == [400]
    assert sleeps == [runner.RUNNER_SHUTDOWN_POLL_SECONDS] * runner.RUNNER_SHUTDOWN_POLL_COUNT
    assert signals[0] == (400, 15)
    assert (400, 9) not in signals


def test_runner_cleanup_signals_python_before_reaping_screen_wrapper(monkeypatch):
    import sovereign_hall.run_discussion as runner

    ps_output = "\n".join([
        "800 1 SCREEN -dmS old_prod zsh -lc python -m sovereign_hall.run_discussion",
        "801 800 python -m sovereign_hall.run_discussion",
    ])
    subprocess_calls = []
    signals = []

    class Result:
        stdout = ps_output

    def fake_run(command, **kwargs):
        subprocess_calls.append(command)
        return Result()

    def fake_kill(pid, sig):
        signals.append((pid, sig))
        if pid == 801 and sig == 0:
            raise ProcessLookupError

    monkeypatch.setattr(__import__("subprocess"), "run", fake_run)
    monkeypatch.setattr(runner.os, "getpid", lambda: 500)
    monkeypatch.setattr(runner.os, "kill", fake_kill)
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    assert kill_existing_run_discussion_instances() == [801]
    assert (801, 15) in signals
    assert not any(pid == 800 for pid, _sig in signals)
    assert ["screen", "-S", "old_prod", "-X", "quit"] in subprocess_calls


@pytest.mark.asyncio
async def test_sigterm_cancellation_reaches_round_cleanup():
    callback_holder = {}
    runner_started = asyncio.Event()
    runner_cleaned = asyncio.Event()
    handler_cleaned = asyncio.Event()

    def fake_install(_loop, callback):
        callback_holder["callback"] = callback

        def cleanup():
            handler_cleaned.set()

        return cleanup

    async def fake_main():
        runner_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            runner_cleaned.set()

    wrapper = asyncio.create_task(
        run_with_graceful_shutdown(fake_main, install_handler=fake_install)
    )
    await runner_started.wait()
    callback_holder["callback"]()
    await wrapper

    assert runner_cleaned.is_set()
    assert handler_cleaned.is_set()


@pytest.mark.asyncio
async def test_sigterm_terminal_is_single_idempotent_and_carries_exact_signal(tmp_path):
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.run_discussion import persist_sigterm_round_terminal

    db = DatabaseService(str(tmp_path / "sigterm-terminal.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="graceful replacement",
        research_objective="retain exact shutdown cause",
    )

    await persist_sigterm_round_terminal(coordinator, round_state.id)
    await persist_sigterm_round_terminal(coordinator, round_state.id)

    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT payload_json
        FROM round_events
        WHERE round_id = ? AND event_type = 'RoundFailed'
        """,
        (round_state.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert len(events) == 1
    assert json.loads(events[0][0])["shutdown_signal"] == "SIGTERM"
    terminal = await coordinator.get(round_state.id)
    assert terminal.terminal_code == "failed"
    assert "终止信号" in terminal.terminal_reason
    await db.close()


@pytest.mark.asyncio
async def test_sigterm_preserves_business_terminal_and_records_finalization_interrupt(
    tmp_path,
):
    from sovereign_hall.application.get_system_status import get_system_status
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.run_discussion import persist_sigterm_round_terminal

    db = DatabaseService(str(tmp_path / "sigterm-after-terminal.db"))
    await db._init_db()
    coordinator = ResearchRoundCoordinator(db)
    round_state = await coordinator.start(
        base_topic="no evidence before replacement",
        research_objective="preserve the business outcome",
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    await coordinator.advance(
        round_state.id,
        ResearchRoundStatus.NO_EVIDENCE,
        event_type="NoEvidenceTerminal",
        terminal_code="no_evidence",
        terminal_reason="no qualified proposal",
    )

    await persist_sigterm_round_terminal(coordinator, round_state.id)
    await persist_sigterm_round_terminal(coordinator, round_state.id)

    terminal = await coordinator.get(round_state.id)
    assert terminal.status == ResearchRoundStatus.NO_EVIDENCE
    assert terminal.terminal_code == "no_evidence"
    assert terminal.terminal_reason == "no qualified proposal"
    conn = await db._get_connection()
    async with conn.execute(
        """
        SELECT event_type, payload_json
        FROM round_events
        WHERE round_id = ?
          AND event_type IN ('RoundFailed', 'RoundFinalizationInterrupted')
        ORDER BY sequence
        """,
        (round_state.id,),
    ) as cursor:
        events = await cursor.fetchall()
    assert [row[0] for row in events] == ["RoundFinalizationInterrupted"]
    payload = json.loads(events[0][1])
    assert payload["shutdown_signal"] == "SIGTERM"
    assert payload["business_terminal_code"] == "no_evidence"

    status = await get_system_status(db)
    assert status["pipeline_health"] == "completed_no_evidence"
    assert status["round_finalization_pending"] is False
    assert status["finalization_interruption"]["shutdown_signal"] == "SIGTERM"

    filled_round = await coordinator.start(
        base_topic="fill before replacement",
        research_objective="preserve a stronger execution terminal",
    )
    await coordinator.advance(
        filled_round.id,
        ResearchRoundStatus.MEMORY_LOADED,
    )
    await coordinator.advance(
        filled_round.id,
        ResearchRoundStatus.SOURCES_PERSISTED,
    )
    await coordinator.advance(
        filled_round.id,
        ResearchRoundStatus.FILLED,
        event_type="SimulationPipelineTerminal",
        terminal_code="filled",
        terminal_reason="atomic fill committed",
    )
    await persist_sigterm_round_terminal(coordinator, filled_round.id)
    filled_status = await get_system_status(db)
    assert filled_status["pipeline_health"] == "completed_filled"
    assert filled_status["round_finalization_pending"] is False
    assert filled_status["finalization_interruption"][
        "business_terminal_code"
    ] == "filled"
    await db.close()


def test_vote_context_balances_all_deliberation_stages():
    context = build_balanced_vote_context(
        {
            "ticker": "600519",
            "direction": "long",
            "confidence": 0.7,
            "target_position": 0.1,
            "thesis": "事实: 可验证",
        },
        [(f"risk-{index}", "R" * 500) for index in range(12)]
        + [("opportunity-tail", "OPPORTUNITY_EVIDENCE")],
        [("cross-examination-tail", "COUNTERARGUMENT")],
        [("counterfactual-tail", "REVISION_EVIDENCE")],
        2400,
    )

    assert len(context) <= 2400
    assert "opportunity-tail" in context
    assert "cross-examination-tail" in context
    assert "counterfactual-tail" in context
    assert "OPPORTUNITY_EVIDENCE" in context


def test_stage2_prompt_has_no_preset_ticker_fallback():
    source = inspect.getsource(stage2_deep_research)

    assert "不得用预设ticker、模板ETF" in source
    assert "159995(科技)" not in source


def test_capital_reward_prioritizes_net_return_and_penalizes_long_idle_cash():
    invested = {
        "total_return": 0.08,
        "max_drawdown": -0.05,
        "cost_paid": 0.01,
        "idle_cash_penalty": idle_cash_exposure_penalty([0.03] * 20),
    }
    lower_return = {**invested, "total_return": 0.05}
    idle = {
        **invested,
        "idle_cash_penalty": idle_cash_exposure_penalty([0.80] * 20),
    }

    assert capital_reward_breakdown(invested)["score"] > capital_reward_breakdown(lower_return)["score"]
    assert capital_reward_breakdown(invested)["score"] > capital_reward_breakdown(idle)["score"]
    assert idle_cash_exposure_penalty([0.80] * 20) > idle_cash_exposure_penalty([0.80, 0.03] * 10)


def test_rebalance_daily_limit_prioritizes_exits_before_buys():
    current = {f"old{i}": 0.10 for i in range(6)}
    target = {f"new{i}": 0.10 for i in range(6)}

    limited, deferred = limit_rebalance_actions(current, target, MAX_DAILY_TRADES)

    assert len(set(current) - set(limited)) == MAX_DAILY_TRADES
    assert not (set(limited) & set(target))
    assert deferred == 7


def test_backtest_never_exceeds_five_transactions_per_day():
    module = load_script_module(
        "run_heuristic_cycle_daily_limit_module",
        "scripts/run_heuristic_cycle.py",
    )
    rows = []
    for day_index, day in enumerate(("2026-01-01", "2026-01-02", "2026-01-03")):
        for ticker_index in range(10):
            rows.append({
                "date": day,
                "ticker": f"{ticker_index:06d}",
                "price": 10.0 + day_index * 0.1,
                "confidence": 0.9,
                "risk_reward": 2.0,
                "close_observations": 3,
                "stop_gap": 0.05,
                "return_1d": 0.01,
                "signal_strength": 1.0 - ticker_index * 0.01,
                "price_source": "daily_prices",
            })
    result = module.run_backtest(
        module.pd.DataFrame(rows),
        module.PolicyConfig(
            name="daily_limit",
            max_names=10,
            max_position=0.10,
            max_gross=1.0,
            min_confidence=0.65,
            min_risk_reward=0.8,
        ),
        module.CostConfig(),
    )

    assert int(result["curve"]["trade_count"].max()) <= MAX_DAILY_TRADES
    assert result["metrics"]["max_daily_trade_count"] <= MAX_DAILY_TRADES
    assert result["metrics"]["trade_count"] == int(result["curve"]["trade_count"].sum())


def test_backtest_marks_held_ticker_from_price_history_without_new_signal():
    module = load_script_module(
        "run_heuristic_cycle_independent_marks_module",
        "scripts/run_heuristic_cycle.py",
    )
    daily = module.pd.DataFrame(
        [
            {"date": "2026-01-01", "ticker": "600519", "price": 10.0},
            {"date": "2026-01-02", "ticker": "510300", "price": 4.0},
        ]
    )
    history = module.pd.DataFrame(
        [
            {"date": "2026-01-01", "ticker": "600519", "close": 10.0},
            {"date": "2026-01-02", "ticker": "600519", "close": 10.2},
        ]
    )

    marks = module.build_mark_prices_by_date(daily, history)

    assert marks["2026-01-02"]["600519"] == pytest.approx(10.2)


def test_price_readiness_accepts_near_complete_history_when_latest_date_is_covered():
    module = load_script_module(
        "heuristic_cycle_near_complete_readiness_module",
        "scripts/run_heuristic_cycle.py",
    )
    rows = []
    for index in range(20):
        rows.append(
            {
                "ticker": f"{index:06d}",
                "date": "2026-01-02",
                "price_source": "daily_prices",
                "close_observations": 1,
            }
        )
    rows.append(
        {
            "ticker": "999999",
            "date": "2026-01-01",
            "price_source": "prediction_current_price",
            "close_observations": 1,
        }
    )

    readiness = module.build_price_readiness_report(module.pd.DataFrame(rows), module.pd.DataFrame())

    assert readiness["status"] == "ready_with_historical_provider_gaps"
    assert readiness["latest_missing_tickers"] == []


def test_committee_preflight_records_every_non_executable_decision():
    executable, rejected = preflight_committee_decisions(
        [
            {
                "ticker": "600519.SH",
                "direction": "hold",
                "risk_flags": ["证据不足"],
                "evidence_gaps": ["缺少可核验订单增速"],
                "reconsider_if": ["订单增速连续两期为正"],
            },
            {"ticker": "", "direction": "long", "target_position": 0.1},
            {"ticker": "600050", "direction": "short"},
            {"ticker": "推荐标的代码", "direction": "hold"},
            {"ticker": "510300", "direction": "long", "target_position": 0.1},
        ],
        current_tickers=set(),
        normalize_ticker=lambda ticker: ticker.replace(".SH", ""),
    )

    assert [row["ticker"] for row in executable] == ["510300"]
    assert {row["code"] for row in rejected} == {
        "committee_hold",
        "missing_ticker",
        "invalid_ticker",
        "short_without_position",
    }
    assert "证据不足" in rejected[0]["reason"]
    assert "evidence_gaps=缺少可核验订单增速" in rejected[0]["reason"]
    assert "reconsider_if=订单增速连续两期为正" in rejected[0]["reason"]


def test_rejection_feedback_keeps_audit_but_removes_obsolete_price_claims():
    raw_reason = (
        "投委会证据未形成多头/退出裁决；risk_flags="
        "标的与逻辑错配未纠正,连续9轮partial daily_prices覆盖无进展,"
        "止损物理性失效：98%缺价交易日导致退出无法执行"
    )

    active, superseded = sanitize_candidate_rejection_reason(raw_reason)
    prepared = prepare_candidate_rejection_feedback([{"last_reason": raw_reason}])[0]

    assert "标的与逻辑错配" in active
    assert "daily_prices" not in active
    assert "98%" not in active
    assert len(superseded) == 2
    assert prepared["last_reason"] == raw_reason
    assert prepared["feedback_reason"] == active
    assert prepared["feedback_usable"] is True


def test_redeployment_state_preserves_raw_blocker_but_returns_sanitized_view(tmp_path):
    async def run():
        db = DatabaseService(str(tmp_path / "test.db"))
        await db._init_db()
        sim = InvestmentSimulation(db)
        await sim.init_tables()
        await sim._write_redeployment_state(
            status="blocked_no_approved_candidates",
            deployment_gap=1000.0,
            blocker_code="missing_approved_candidates",
            blocker_reason=(
                "投委会未批准；risk_flags=止损物理性失效：98%缺价交易日导致平仓指令无法执行,"
                "标的与逻辑错配"
            ),
            next_action="重新研究",
            source="test",
        )
        state = await sim.get_redeployment_state()
        raw = (
            await db._connection.execute_fetchall(
                "SELECT blocker_reason FROM simulation_redeployment_state WHERE id=1"
            )
        )[0][0]
        await db.close()
        return state, raw

    state, raw = __import__("asyncio").run(run())
    assert "98%" in raw
    assert "98%" in state["blocker_reason_audit"]
    assert "98%" not in state["blocker_reason"]
    assert "标的与逻辑错配" in state["blocker_reason"]


def test_repeated_candidate_requires_traceable_evidence_delta_during_cooldown():
    now = datetime(2026, 7, 22, 14, 0, 0)
    memory = [{
        "ticker": "159995",
        "code": "committee_hold",
        "rejection_count": 48,
        "last_seen_at": "2026-07-22T10:00:00",
        "feedback_usable": True,
        "feedback_reason": "标的与逻辑错配未纠正",
    }]
    unchanged = {"ticker": "159995", "direction": "long", "confidence": 0.8}
    traceable = unchanged | {
        "resolved_rejection": "标的与逻辑错配未纠正",
        "evidence_delta": "本地文档doc-42确认该ETF成分与提案主题一致",
        "evidence": ["doc-42 成分核验"],
    }

    eligible, rejected = filter_repeated_rejection_proposals(
        [unchanged], memory, now=now
    )
    traceable_eligible, traceable_rejected = filter_repeated_rejection_proposals(
        [traceable], memory, now=now
    )

    assert eligible == []
    assert rejected[0]["code"] == "repeated_candidate_cooldown"
    assert traceable_eligible == [traceable]
    assert traceable_rejected == []


def test_cycle_comparison_uses_retained_best_not_diagnostic_max(tmp_path):
    module = load_script_module(
        "run_heuristic_cycle_retained_best_module",
        "scripts/run_heuristic_cycle.py",
    )
    run_dir = tmp_path / "20260715_000000"
    run_dir.mkdir()
    (run_dir / "best_metrics.json").write_text(
        json.dumps({"score": -0.284204, "reward_version": "capital_return_v2"}),
        encoding="utf-8",
    )
    (run_dir / "summary.csv").write_text(
        "trial_name,score\nretained,-0.284204\ndiagnostic_only,0.022448\n",
        encoding="utf-8",
    )

    assert module.completed_run_best_score(run_dir) == pytest.approx(-0.284204)
    score, path = module.previous_best_score(tmp_path)
    assert score == pytest.approx(-0.284204)
    assert path == run_dir / "best_metrics.json"


def test_token_format_uses_short_units():
    assert format_token(999) == "999"
    assert format_token(1_234) == "1.23k"
    assert format_token(1_234_567) == "1.23m"
    assert format_token(1_234_567_890) == "1.23g"


def test_token_and_cost_breakdowns_include_input_output():
    stats = {
        "total_tokens": 1_234_567,
        "prompt_tokens": 1_000_000,
        "completion_tokens": 234_567,
        "total_cost": 0.123456,
        "input_cost_usd": 0.023456,
        "output_cost_usd": 0.1,
    }

    assert format_token_breakdown(stats) == "1.23m (输入 1.00m / 输出 234.6k)"
    assert format_cost_breakdown(stats) == "$0.1235 (输入 $0.0235 / 输出 $0.1000)"


def test_check_db_safe_input_handles_closed_stdin(monkeypatch):
    import sovereign_hall.check_db as check_db

    def raise_eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)

    assert check_db.safe_input("choice: ") is None


def test_check_db_blank_choice_exits_safely(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "sovereign_hall.db").write_bytes(b"")
    monkeypatch.setattr(check_db, "project_root", tmp_path)
    monkeypatch.setattr(check_db, "show_stats", lambda _db_path: [])
    monkeypatch.setattr(check_db, "safe_input", lambda _prompt: "")

    check_db.main()
    output = capsys.readouterr().out

    assert "空输入，安全退出" in output
    assert "无效选择" not in output


def test_research_interactive_safe_input_handles_closed_stdin(monkeypatch):
    import sovereign_hall.research_interactive as research_interactive

    def raise_eof(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", raise_eof)

    assert research_interactive.safe_input("question: ") is None


def test_research_interactive_help_is_cli_only():
    import sovereign_hall.research_interactive as research_interactive

    with pytest.raises(SystemExit) as exc:
        research_interactive.parse_args(["--help"])

    assert exc.value.code == 0


def test_check_db_realtime_quotes_are_on_by_default(monkeypatch):
    import sovereign_hall.check_db as check_db

    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)
    assert check_db.realtime_quotes_enabled() is True

    monkeypatch.setenv("SOVEREIGN_HALL_REALTIME_QUOTES", "0")
    assert check_db.realtime_quotes_enabled() is False


def test_per_round_sqlite_reads_explicitly_close_connections(tmp_path, monkeypatch):
    """The autonomous loop must not retain sqlite handles across rounds."""
    import sovereign_hall.services.heuristic_policy as heuristic_policy

    db_path = tmp_path / "round_reads.db"
    setup = sqlite3.connect(db_path)
    try:
        setup.executescript(
            """
            CREATE TABLE report_conclusions (
                question TEXT,
                created_at TEXT
            );
            CREATE TABLE price_predictions (
                ticker TEXT,
                predicted_at TEXT
            );
            CREATE TABLE simulation_risk_memory (
                ticker TEXT,
                source TEXT,
                failure_count INTEGER,
                last_loss_pct REAL,
                worst_loss_pct REAL,
                last_trade_id INTEGER,
                last_updated TEXT,
                expires_at TEXT,
                reason TEXT
            );
            INSERT INTO report_conclusions VALUES ('topic', datetime('now'));
            INSERT INTO price_predictions VALUES ('600000', datetime('now'));
            """
        )
        setup.commit()
    finally:
        setup.close()

    real_connect = sqlite3.connect
    opened = []

    class TrackingConnection(sqlite3.Connection):
        explicitly_closed = False

        def close(self):
            self.explicitly_closed = True
            return super().close()

    def tracked_connect(*args, **kwargs):
        kwargs["factory"] = TrackingConnection
        connection = real_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)

    assert set(load_recent_topics(db_path, hours=24)) == {"topic"}
    heuristic_policy.refresh_tape_update_from_local_db(
        {"current_prediction_rows": 0},
        db_path=db_path,
    )
    heuristic_policy.recent_prediction_observation_count(
        "600000",
        db_path=db_path,
    )
    heuristic_policy.load_active_simulation_risk_memory(db_path=db_path)

    assert len(opened) == 4
    assert all(connection.explicitly_closed for connection in opened)


def test_live_iteration_attribution_requires_fill_and_uses_prior_realtime_score(
    tmp_path,
):
    module = load_script_module(
        "heuristic_cycle_stdlib_attribution_test_module",
        "scripts/run_heuristic_cycle_stdlib.py",
    )
    previous_run = tmp_path / "20260730_151700"
    previous_run.mkdir()
    (previous_run / "simulation_account_metrics.json").write_text(
        json.dumps(
            {
                "measured_at": "2026-07-30T15:18:34",
                "score": -0.027278097,
            }
        ),
        encoding="utf-8",
    )

    window_start, baseline_score = module.live_iteration_baseline(
        previous_run,
        "fallback",
    )
    no_fill = module.attach_live_iteration_attribution(
        {"score": -0.02, "trades_since_window_start": 0},
        baseline_score=baseline_score,
    )
    with_fill = module.attach_live_iteration_attribution(
        {"score": -0.02775, "trades_since_window_start": 3},
        baseline_score=baseline_score,
    )

    assert window_start == "2026-07-30T15:18:34"
    assert no_fill["iteration_performance_improvement"] is None
    assert with_fill["iteration_performance_improvement"] == pytest.approx(
        -0.000471903
    )
    assert "低于80%" in module.live_failure_suspected_reason(
        {
            "trades_since_window_start": 1,
            "current_invested_ratio": 0.79,
        }
    )
    healthy_reason = module.live_failure_suspected_reason(
        {
            "trades_since_window_start": 3,
            "current_invested_ratio": 0.8101,
        }
    )
    assert "已恢复到80%以上" in healthy_reason
    assert "低于80%" not in healthy_reason


def test_check_db_filters_placeholder_candidate_rejections():
    import sovereign_hall.check_db as check_db

    filtered = check_db.filter_supported_candidate_rejections([
        {"ticker": "600519", "code": "committee_hold"},
        {"ticker": "推荐标的代码", "code": "committee_hold"},
        {"ticker": "06862", "code": "committee_hold"},
    ])

    assert filtered == [{"ticker": "600519", "code": "committee_hold"}]


def test_check_db_requires_realtime_quote_without_local_fallback(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats (key, value) VALUES ('simulation_cash', '9000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute("INSERT INTO simulation_positions VALUES ('600519', 100, 10.0)")
    conn.execute(
        "CREATE TABLE simulation_trades (ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE price_predictions (ticker TEXT, current_price REAL, predicted_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO price_predictions VALUES (?, ?, ?)",
        [
            ("600519", 10.5, "2026-06-12T10:00:00"),
            ("600519.SH", 12.3, "2026-06-15T10:00:00"),
        ],
    )
    conn.commit()
    conn.close()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)
    monkeypatch.setattr(check_db, "get_realtime_prices", lambda tickers: {})

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "当前资产: N/A" in output
    assert "实时现价不可用" in output
    assert "不使用本地估值/预测价/成本价兜底" in output
    assert "本地最近预测价" not in output


def test_check_db_values_positions_from_realtime_quote(tmp_path, monkeypatch, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '9000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute("INSERT INTO simulation_positions VALUES ('600519', 100, 10.0)")
    conn.execute(
        "CREATE TABLE simulation_trades (ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        check_db,
        "get_realtime_prices",
        lambda tickers: {
            "600519": {
                "price": 12.3,
                "source": "test_realtime_quote",
                "fetched_at": "2026-07-13T15:00:00",
            }
        },
    )

    performance = check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "当前资产: 10230.00 元（实时现价）" in output
    assert "实时现价12.300" in output
    assert "test_realtime_quote" in output
    assert performance["valuation_complete"] is True
    assert performance["score"] == pytest.approx(0.023)


def test_check_db_one_lot_boundary_includes_minimum_commission(
    tmp_path,
    monkeypatch,
    capsys,
):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "one_lot_boundary.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '57.14928')")
    conn.execute(
        "CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)"
    )
    conn.execute("INSERT INTO simulation_positions VALUES ('510300', 100, 97.72)")
    conn.execute(
        "CREATE TABLE simulation_trades "
        "(ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        check_db,
        "get_realtime_prices",
        lambda tickers: {
            "510300": {
                "price": 97.72,
                "source": "test_realtime_quote",
                "fetched_at": datetime.now().isoformat(),
            }
        },
    )

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "新建仓一手要求交易时实时价<=0.5212元" in output
    assert "已计最低佣金5.00元与滑点" in output
    assert "<=0.5710元" not in output


def test_check_db_lifecycle_review_surfaces_complete_position_evidence(
    tmp_path,
    monkeypatch,
    capsys,
):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "lifecycle.db"
    now = datetime.now().isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '9000')")
    conn.execute(
        """
        CREATE TABLE simulation_positions (
            ticker TEXT, shares INTEGER, avg_cost REAL, opened_at TEXT,
            peak_price REAL, last_mark_price REAL, last_mark_at TEXT,
            last_mark_source TEXT, last_reviewed_at TEXT,
            review_status TEXT, review_reason TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO simulation_positions VALUES (
            '600519', 100, 10.0, ?, 12.0, 11.0, ?,
            'test_realtime_quote', ?, 'hold', 'durable hold reason'
        )
        """,
        (now, now, now),
    )
    conn.execute(
        """
        CREATE TABLE simulation_trades (
            ticker TEXT, direction TEXT, shares INTEGER, price REAL,
            fee REAL, reason TEXT, traded_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        check_db,
        "get_realtime_prices",
        lambda tickers: {
            "600519": {
                "price": 11.0,
                "source": "test_realtime_quote",
                "fetched_at": now,
            }
        },
    )

    performance = check_db.show_investment_status(db_path)
    output = capsys.readouterr().out
    review = performance["position_lifecycle_reviews"][0]

    assert "unrealized=+10.00%" in output
    assert "stop=9.2000(-8.0%)" in output
    assert "take_profit=11.5000(15.0%)" in output
    assert "peak_drawdown=-8.33%" in output
    assert "last_review=" in output
    assert "durable hold reason" in output
    assert review["max_holding_days"] == 30
    assert review["peak_drawdown"] == pytest.approx(-1 / 12)


def test_check_db_reports_pending_decision_terminal_counts(tmp_path, capsys):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "pending_status.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '10000')")
    conn.execute("CREATE TABLE simulation_positions (ticker TEXT, shares INTEGER, avg_cost REAL)")
    conn.execute(
        "CREATE TABLE simulation_trades "
        "(ticker TEXT, direction TEXT, shares INTEGER, price REAL, reason TEXT, traded_at TEXT)"
    )
    conn.execute(
        """
        CREATE TABLE simulation_pending_decisions (
            id INTEGER PRIMARY KEY, ticker TEXT, direction TEXT,
            target_position REAL, defer_code TEXT, status TEXT,
            created_at TEXT, updated_at TEXT, resolved_at TEXT,
            resolution TEXT, replay_count INTEGER
        )
        """
    )
    rows = [
        (1, "600519", "long", 0.1, "market_closed", "executed", "2026-07-18T10:00:00", "2026-07-19T10:01:00", "2026-07-19T10:01:00", "buy:filled", 1),
        (2, "000001", "long", 0.1, "market_closed", "rejected", "2026-07-18T10:02:00", "2026-07-19T10:03:00", "2026-07-19T10:03:00", "hold:heuristic veto", 1),
        (3, "159915", "sell", 0.0, "market_closed", "expired", "2026-07-10T10:00:00", "2026-07-19T10:04:00", "2026-07-19T10:04:00", "expired_without_open-session_replay", 0),
        (4, "512880", "long", 0.1, "daily_trade_limit", "pending_next_trading_session", "2026-07-19T10:05:00", "2026-07-19T10:05:00", None, None, 0),
    ]
    conn.executemany(
        "INSERT INTO simulation_pending_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    check_db.show_investment_status(db_path)
    output = capsys.readouterr().out

    assert "待执行裁决: 1 条" in output
    assert "executed=1, rejected=1, expired=1, pending=1" in output
    assert "最近裁决结果: expired | 159915 sell" in output
    assert "expired_without_open-session_replay" in output


def test_check_db_reports_live_daily_price_backfill_progress(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
    conn.execute("INSERT INTO daily_prices VALUES ('600519', '2026-06-18', 10.5)")
    conn.commit()
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,45,2026-05-01,2026-06-20,1585,2026-06-20,True,1,"
        "backfill this ticker's latest local daily_prices row first\n"
        "2,512880,44,2026-05-02,2026-06-10,1197,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-19,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n"
        "2026-06-10,512880,prediction_current_price\n",
        encoding="utf-8",
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "latest_missing_tickers": ["600519", "688256"],
            "unblock_tickers": ["600519", "688256"],
            "minimum_next_rows": 2,
            "missing_tickers_top10": [
                {"ticker": "600519", "signal_days": 45, "last_signal_date": "2026-06-20"},
                {"ticker": "512880", "signal_days": 44, "last_signal_date": "2026-06-10"},
            ],
            "backfill_plan_path": str(plan_path),
            "backfill_plan": {
                "total_missing_tickers": 2,
                "minimum_next_rows": 1,
                "top_priority_tickers": ["600519", "512880"],
            },
        },
    )

    text = check_db.format_daily_price_backfill_progress(conn, context=context)
    conn.close()

    assert "优先队列任意本地价格(非解锁口径): 1/2 tickers" in text
    assert "计划日期覆盖: 2/3 signal dates；缺口=1，补齐后重跑验证" in text
    assert "600519(missing 2026-05-01..2026-06-20, 45d, 1585obs, plan_covered=2/2" in text
    assert "512880(missing 2026-05-02..2026-06-10, 44d, 1197obs, plan_covered=0/1)" in text
    assert "下一步本地补齐: 512880 2026-05-02..2026-06-10 (44 signal days)" in text
    assert "最小解锁批次: 600519, 688256 (2 signal rows)" in text
    assert f"机器可读补齐计划: {tmp_path / 'daily_price_backfill_plan.csv'}" in text
    assert "计划优先级Top: 600519, 512880" in text
    assert "本地DB覆盖检查: python scripts/backfill_daily_prices.py --status --limit 5 --plan" in text
    assert "不联网计划查看: python scripts/backfill_daily_prices.py --dry-run --limit 5 --plan" in text
    assert "本地CSV精确日期校验: python scripts/backfill_daily_prices.py --import-csv data/local_daily_prices.csv" in text
    assert "本地CSV模板生成: python scripts/backfill_daily_prices.py --status --limit 5 --export-template" in text
    assert "local_daily_prices_template.csv" in text
    assert "MarketDataService fetch 默认关闭" in text
    assert f"--plan {plan_path}" in text
    assert "旧历史artifact复用仓位上限 <= 0.5%" in text
    assert "旧历史artifact复用仓位上限" in text
    assert "不受该历史缺口停机帽约束" in text


def test_check_db_exports_stable_local_daily_price_template(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
    conn.execute("INSERT INTO daily_prices VALUES ('600519', '2026-06-18', 10.5)")
    conn.commit()
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,45,2026-05-01,2026-06-20,1585,2026-06-20,True,1,"
        "backfill this ticker's latest local daily_prices row first\n"
        "2,512880,44,2026-05-02,2026-06-10,1197,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-19,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n"
        "2026-06-10,512880,prediction_current_price\n",
        encoding="utf-8",
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "missing_tickers_top10": [
                {"ticker": "600519", "signal_days": 45, "last_signal_date": "2026-06-20"},
                {"ticker": "512880", "signal_days": 44, "last_signal_date": "2026-06-10"},
            ],
            "backfill_plan_path": str(plan_path),
            "backfill_plan": {
                "total_missing_tickers": 2,
                "minimum_next_rows": 1,
                "top_priority_tickers": ["600519", "512880"],
            },
        },
    )

    progress = check_db.daily_price_backfill_progress(conn, context=context)
    output_path = tmp_path / "data" / "local_daily_prices_template.csv"
    written = check_db.export_daily_price_template_from_progress(progress, output_path)
    progress["template_written_rows"] = written
    progress["stable_template_path"] = str(output_path)
    text = check_db.format_daily_price_backfill_progress(conn, progress=progress)
    conn.close()

    rows = list(csv.DictReader(output_path.open("r", encoding="utf-8")))
    assert written == 1
    assert rows[0]["ticker"] == "512880"
    assert rows[0]["date"] == "2026-06-10"
    assert rows[0]["close"] == ""
    assert "入口已生成待填写模板" in text
    assert f"{output_path} (1 rows)" in text
    assert "模板填完后校验" in text
    assert "模板填完后严格校验" in text
    assert "严格校验通过后导入" in text
    assert "--require-plan-coverage" in text
    assert "--coverage-limit 5" in text

    progress["template_csv_status"] = check_db.inspect_local_daily_price_csv(output_path)
    text_with_status = check_db.format_daily_price_backfill_progress(conn, progress=progress)
    assert "模板当前状态: rows=1, valid_ohlc=0, blank=1, invalid=0" in text_with_status
    assert "模板尚未填入独立OHLC" in text_with_status


def test_backfill_daily_prices_imports_local_csv_without_network(tmp_path):
    module = load_script_module("backfill_daily_prices_test_module", "scripts/backfill_daily_prices.py")
    csv_path = tmp_path / "daily_prices.csv"
    csv_path.write_text(
        "ticker,date,open,high,low,close,volume\n"
        "600519,2026-06-20,10,11,9,10.5,1000\n"
        "512880,2026-06-20,,,,1.234,\n"
        "BAD,2026-06-20,1,1,1,0,0\n",
        encoding="utf-8",
    )

    rows, invalid = module.rows_from_csv(csv_path)

    assert len(rows) == 2
    assert rows[0][:2] == ("600519", "2026-06-20")
    assert rows[1][2:6] == (1.234, 1.234, 1.234, 1.234)
    assert invalid and invalid[0]["ticker"] == "BAD"

    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE daily_prices (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                source TEXT,
                PRIMARY KEY (ticker, date)
            )
            """
        )
    written = module.upsert_csv_rows(db_path, rows, "unit_csv")

    with sqlite3.connect(db_path) as conn:
        stored = conn.execute(
            "SELECT ticker, date, close, source FROM daily_prices ORDER BY ticker"
        ).fetchall()

    assert written == 2
    assert stored == [
        ("512880", "2026-06-20", 1.234, "unit_csv"),
        ("600519", "2026-06-20", 10.5, "unit_csv"),
    ]


def test_backfill_daily_prices_validates_exact_plan_dates(tmp_path):
    module = load_script_module("backfill_daily_prices_exact_plan_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    requests = module.requests_from_plan(plan_path, module.parse_date("2026-06-20"))
    rows = [("600519", "2026-06-20", 10.0, 10.0, 10.0, 10.0, 100.0)]

    summary = module.summarize_plan_coverage(rows, requests, plan_path, max_age_days=7)

    assert "csv_exact_ticker_coverage=0/1" in summary
    assert "signal_dates=1/2" in summary
    assert "missing_top=600519" in summary


def test_backfill_daily_prices_import_csv_defaults_to_latest_plan(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_latest_plan_module", "scripts/backfill_daily_prices.py")
    runs_root = tmp_path / "runs" / "heuristic_cycle"
    run_dir = runs_root / "20260703_000000"
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (run_dir / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    csv_path = tmp_path / "daily_prices.csv"
    csv_path.write_text(
        "ticker,date,close\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--runs-root",
            str(runs_root),
            "--import-csv",
            str(csv_path),
            "--source",
            "local_csv",
            "--dry-run",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 0
    assert f"Plan: {plan_path.resolve()}" in output
    assert "Plan coverage: plan_requests=1" in output
    assert "signal_dates=1/2" in output


def test_backfill_daily_prices_strict_plan_coverage_gate(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_strict_plan_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600519,2,2026-06-05,2026-06-20,2,2026-06-20,True,1,backfill latest\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price_source\n"
        "2026-06-05,600519,prediction_current_price\n"
        "2026-06-20,600519,prediction_current_price\n",
        encoding="utf-8",
    )
    partial_csv = tmp_path / "partial_daily_prices.csv"
    partial_csv.write_text(
        "ticker,date,close\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--plan",
            str(plan_path),
            "--import-csv",
            str(partial_csv),
            "--source",
            "local_csv",
            "--dry-run",
            "--coverage-limit",
            "1",
            "--require-plan-coverage",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 4
    assert "STRICT plan coverage failed" in output
    assert "signal_dates=1/2" in output

    full_csv = tmp_path / "full_daily_prices.csv"
    full_csv.write_text(
        "ticker,date,close\n"
        "600519,2026-06-05,10.1\n"
        "600519,2026-06-20,10.5\n",
        encoding="utf-8",
    )
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--plan",
            str(plan_path),
            "--import-csv",
            str(full_csv),
            "--source",
            "local_csv",
            "--dry-run",
            "--coverage-limit",
            "1",
            "--require-plan-coverage",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 0
    assert "STRICT plan coverage passed" in output
    assert "signal_dates=2/2" in output


def test_backfill_daily_prices_blocks_market_fetch_by_default(tmp_path, capsys):
    module = load_script_module("backfill_daily_prices_local_guard_module", "scripts/backfill_daily_prices.py")
    args = module.build_parser().parse_args(
        [
            "--db",
            str(tmp_path / "test.db"),
            "--ticker",
            "600519",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-02",
        ]
    )

    result = __import__("asyncio").run(module.run(args))
    output = capsys.readouterr().out

    assert result == 3
    assert "MarketDataService fetch disabled by default" in output
    assert "--import-csv data/local_daily_prices.csv" in output


def test_backfill_plan_uses_missing_date_range_and_csv_plan_coverage(tmp_path):
    module = load_script_module("backfill_daily_prices_plan_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,159990,43,2026-04-29,2026-06-10,445,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n"
        "2,600690,7,2026-05-28,2026-06-09,8,2026-06-20,False,0,"
        "backfill historical local daily_prices before using scores to widen exposure\n",
        encoding="utf-8",
    )

    requests = module.requests_from_plan(plan_path, datetime(2026, 6, 27).date())

    assert requests[0].ticker == "159990"
    assert requests[0].start.isoformat() == "2026-04-29"
    assert requests[0].end.isoformat() == "2026-06-10"
    assert requests[1].end.isoformat() == "2026-06-09"
    coverage = module.summarize_plan_coverage(
        [("159990", "2026-06-10", 1.0, 1.0, 1.0, 1.0, 0.0)],
        requests,
    )
    assert "csv_exact_ticker_coverage=1/2" in coverage
    assert "signal_dates=1/2" in coverage
    assert "missing_top=600690" in coverage


def test_backfill_market_request_extends_before_weekend_signal(tmp_path):
    module = load_script_module("backfill_daily_prices_weekend_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,600141,1,2026-05-30,2026-05-30,1,2026-05-30,True,1,backfill\n",
        encoding="utf-8",
    )

    exact = module.requests_from_plan(plan_path, datetime(2026, 6, 1).date())
    fetch = module.requests_from_plan(
        plan_path,
        datetime(2026, 6, 1).date(),
        lookback_days=7,
    )

    assert exact[0].start.isoformat() == "2026-05-30"
    assert fetch[0].start.isoformat() == "2026-05-23"
    assert fetch[0].end.isoformat() == "2026-05-30"


def test_backfill_plan_status_uses_exact_signal_tape_dates(tmp_path):
    module = load_script_module("backfill_daily_prices_status_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,688256,3,2026-05-09,2026-06-26,6,2026-06-26,True,1,"
        "backfill this ticker's latest local daily_prices row first\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price,close_observations,price_source\n"
        "2026-05-09,688256,1182.53,2,prediction_current_price\n"
        "2026-05-10,688256,1182.53,3,prediction_current_price\n"
        "2026-06-26,688256,1455.69,1,prediction_current_price\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE daily_prices (
                ticker TEXT,
                date TEXT,
                close REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_prices VALUES (?, ?, ?)",
            [
                ("688256", "2026-05-08", 1000.0),
                ("688256", "2026-06-18", 1200.0),
            ],
        )

    summary, details = module.summarize_db_plan_coverage(db_path, plan_path, limit=5, max_age_days=7)
    assert summary["status"] == "needs_local_daily_prices"
    assert summary["checked_signal_dates"] == 3
    assert summary["covered_signal_dates"] == 2
    assert details[0]["missing_signal_dates"] == ["2026-06-26"]
    rendered = module.format_db_plan_coverage(summary, details)
    assert "signal_dates=2/3" in rendered
    assert "missing_dates=2026-06-26" in rendered


def test_backfill_daily_prices_exports_missing_template_only(tmp_path):
    module = load_script_module("backfill_daily_prices_template_test_module", "scripts/backfill_daily_prices.py")
    plan_path = tmp_path / "daily_price_backfill_plan.csv"
    plan_path.write_text(
        "priority_rank,ticker,missing_signal_days,first_missing_signal_date,last_missing_signal_date,"
        "total_signal_observations,latest_signal_date,missing_latest_signal_date,"
        "minimum_rows_to_unblock_latest,plan_action\n"
        "1,688256,3,2026-05-09,2026-06-26,6,2026-06-26,True,1,"
        "backfill this ticker's latest local daily_prices row first\n",
        encoding="utf-8",
    )
    (tmp_path / "daily_signal_tape.csv").write_text(
        "date,ticker,price,close_observations,price_source\n"
        "2026-05-09,688256,1182.53,2,prediction_current_price\n"
        "2026-05-10,688256,1182.53,3,prediction_current_price\n"
        "2026-06-26,688256,1455.69,1,prediction_current_price\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)")
        conn.executemany(
            "INSERT INTO daily_prices VALUES (?, ?, ?)",
            [
                ("688256", "2026-05-08", 1000.0),
                ("688256", "2026-06-18", 1200.0),
            ],
        )

    summary, details = module.summarize_db_plan_coverage(db_path, plan_path, limit=5, max_age_days=7)
    template_path = tmp_path / "local_daily_prices_template.csv"
    written = module.export_missing_price_template(template_path, summary, details)

    assert written == 1
    rows = list(csv.DictReader(template_path.open("r", encoding="utf-8")))
    assert rows == [
        {
            "ticker": "688256",
            "date": "2026-06-26",
            "open": "",
            "high": "",
            "low": "",
            "close": "",
            "volume": "",
            "source_note": "fill_from_independent_local_ohlc_before_import",
        }
    ]


def test_price_readiness_uses_signal_date_price_source_not_ticker_level_history(tmp_path):
    module = load_script_module("heuristic_cycle_stdlib_readiness_test_module", "scripts/run_heuristic_cycle_stdlib.py")
    daily = [
        {
            "ticker": "688256",
            "date": "2026-06-20",
            "price_source": "daily_prices",
            "close_observations": 3,
        },
        {
            "ticker": "688256",
            "date": "2026-06-26",
            "price_source": "prediction_current_price",
            "close_observations": 1,
        },
        {
            "ticker": "159928",
            "date": "2026-06-26",
            "price_source": "daily_prices",
            "close_observations": 2,
        },
        {
            "ticker": "159990",
            "date": "2026-05-01",
            "price_source": "prediction_current_price",
            "close_observations": 100,
        },
        {
            "ticker": "159990",
            "date": "2026-05-02",
            "price_source": "prediction_current_price",
            "close_observations": 100,
        },
    ]
    price_history = {("2026-06-20", "688256"): 10.0, ("2026-06-26", "159928"): 1.0}

    readiness = module.build_price_readiness_report(daily, price_history)

    assert readiness["status"] == "partial_daily_price_backfill_needed"
    assert readiness["total_signal_ticker_count"] == 3
    assert readiness["priced_signal_ticker_count"] == 1
    assert readiness["missing_signal_ticker_count"] == 2
    assert readiness["latest_missing_tickers"] == ["688256"]
    assert readiness["minimum_next_rows"] == 1
    assert readiness["missing_tickers_top10"][0]["ticker"] == "688256"
    assert readiness["missing_tickers_top10"][0]["last_signal_date"] == "2026-06-26"
    plan_rows, plan_summary = module.build_daily_price_backfill_plan(daily, price_history, tmp_path)
    assert plan_summary["top_priority_tickers"][0] == "688256"
    assert plan_rows[0]["ticker"] == "688256"


def test_market_data_ticker_mapping():
    svc = MarketDataService()
    assert svc.is_supported_ticker("600519.SH") is True
    assert svc.is_supported_ticker("推荐标的代码") is False
    assert svc.infer_market("600519") == "sh"
    assert svc.infer_market("159995") == "sz"
    assert svc.eastmoney_secid("512880") == "1.512880"


@pytest.mark.asyncio
async def test_realtime_quote_batch_retries_only_missing_after_full_pass():
    calls = []
    attempts = {"516510": 0, "562950": 0}

    async def fetch_quote(ticker):
        calls.append(ticker)
        attempts[ticker] += 1
        if ticker == "516510" and attempts[ticker] == 1:
            return None
        return {
            "ticker": ticker,
            "price": 1.5,
            "source": "controlled_realtime_quote",
            "fetched_at": datetime.now().isoformat(),
        }

    quotes = await collect_realtime_quote_batch(
        ["516510", "562950", "516510"],
        fetch_quote,
    )

    assert calls == ["516510", "562950", "516510"]
    assert set(quotes) == {"516510", "562950"}


@pytest.mark.asyncio
async def test_calculate_assets_recovers_transient_quote_without_caller_fallback(
    monkeypatch,
):
    sim = InvestmentSimulation()
    sim.cash = 100.0
    sim.positions = {
        "516510": {"shares": 100, "avg_cost": 1.0},
        "562950": {"shares": 100, "avg_cost": 1.0},
    }
    attempts = {"516510": 0, "562950": 0}

    async def fetch_quote(ticker):
        attempts[ticker] += 1
        if ticker == "516510" and attempts[ticker] == 1:
            return None
        return {
            "ticker": ticker,
            "price": 2.0,
            "source": "controlled_realtime_quote",
            "fetched_at": datetime.now().isoformat(),
        }

    monkeypatch.setattr(sim, "get_current_quote", fetch_quote)

    assets = await sim.calculate_assets(prices={"516510": 999.0, "562950": 999.0})

    assert assets["valuation_complete"] is True
    assert assets["total_assets"] == pytest.approx(500.0)
    assert assets["position_prices"] == {"516510": 2.0, "562950": 2.0}
    assert attempts == {"516510": 2, "562950": 1}


def test_llm_client_uses_configured_defaults():
    client = LLMClient(provider="local")
    llm_config = get_config().get_llm_config()

    assert client.timeout == llm_config.get("timeout")
    assert client.max_retries == llm_config.get("max_retries", 3)
    assert client.retry_delay == llm_config.get("retry_delay", 2.0)
    assert client.max_concurrent == llm_config.get("max_concurrent")


@pytest.mark.asyncio
async def test_spider_uses_nested_rate_limit_config():
    config = get_config()
    original = dict(config.get_spider_config())
    config.set("spider.max_concurrent", 4)
    config.set("spider.rate_limit", {"requests_per_minute": 6, "burst": 2})

    spider = None
    try:
        spider = SpiderSwarm()
        assert spider.max_concurrent == 4
        assert spider.rate_limiter.rate == pytest.approx(0.1)
        assert spider.rate_limiter.burst == 2
    finally:
        if spider:
            await spider.close()
        config._config["spider"] = original


@pytest.mark.asyncio
async def test_market_data_cools_down_eastmoney_after_repeated_ohlc_failures(monkeypatch):
    class FailingClient:
        is_closed = False

        def __init__(self):
            self.calls = 0

        async def get(self, url, params=None):
            self.calls += 1
            request = httpx.Request("GET", url, params=params)
            response = httpx.Response(502, request=request)
            raise httpx.HTTPStatusError("bad gateway", request=request, response=response)

        async def aclose(self):
            pass

    svc = MarketDataService()
    await svc._client.aclose()
    failing_client = FailingClient()
    svc._client = failing_client

    async def fake_tencent(*_args, **_kwargs):
        return []

    async def fake_akshare(*_args, **_kwargs):
        return [{"date": "2026-06-10", "open": 1.0, "close": 1.0, "high": 1.0, "low": 1.0, "volume": 0}]

    monkeypatch.setattr(svc, "_fetch_tencent_ohlc", fake_tencent)
    monkeypatch.setattr(svc, "_fetch_akshare_ohlc", fake_akshare)

    for _ in range(4):
        bars = await svc.get_ohlc("600519", "2026-06-01", "2026-06-10")
        assert bars

    assert failing_client.calls == 3
    await svc.close()


@pytest.mark.asyncio
async def test_document_can_be_stored(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    doc = Document(
        title="测试标题",
        content="这是一段有效的测试文档内容，足够长，可以被写入数据库。",
        url="https://example.com/a",
        source="unit",
        sector="测试",
        keywords=["测试"],
    )

    await db.add_document(doc)

    assert await db.count_documents() == 1
    stored = await db.get_document(doc.id)
    assert stored["title"] == "测试标题"
    await db.close()


@pytest.mark.asyncio
async def test_dict_proposal_can_be_stored(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()

    long_thesis = "测试提案" + "完整论证" * 2000

    proposal_id = await db.add_proposal({
        "ticker": "512880",
        "direction": "long",
        "target_position": 0.1,
        "entry_price": 1.0,
        "stop_loss": 0.95,
        "take_profit": 1.15,
        "holding_period": 30,
        "confidence": 0.6,
        "thesis": long_thesis,
        "sector": "半导体",
        "holding_period_reason": "财报验证窗口",
        "evidence": ["公告订单", "行业价格"],
        "reject_if": "订单取消",
    })

    proposals = await db.get_proposals(limit=5)
    assert len(proposals) == 1
    assert proposals[0]["ticker"] == "512880"
    assert proposals[0]["thesis"] == long_thesis
    assert proposals[0]["created_at"]
    assert json.loads(proposals[0]["evidence"]) == ["公告订单", "行业价格"]
    assert proposals[0]["holding_period_reason"] == "财报验证窗口"
    assert proposals[0]["reject_if"] == "订单取消"

    await db.add_meeting_record(
        meeting_id="meeting-test",
        proposal_id=proposal_id,
        ticker="512880",
        decision="long",
        discussion="四轮讨论摘要",
        vote_details={"long": 4, "hold": 3},
        action_items=["30天后验证"],
    )
    meetings = await db.get_meetings(limit=5)
    assert meetings[0]["proposal_id"] == proposal_id
    assert meetings[0]["discussion"] == "四轮讨论摘要"
    await db.close()


@pytest.mark.asyncio
async def test_database_rejects_scalar_proposal_evidence(tmp_path):
    db = DatabaseService(str(tmp_path / "test.db"))
    await db._init_db()

    with pytest.raises(ValueError, match="JSON array of meaningful strings"):
        await db.add_proposal({
            "ticker": "601916",
            "direction": "long",
            "evidence": "金融周报推荐关注",
        })

    with pytest.raises(ValueError, match="character-split evidence"):
        await db.add_proposal({
            "ticker": "601916",
            "direction": "long",
            "evidence": ["金", "融", "周", "报"],
        })

    assert await db.get_proposals(limit=5) == []
    await db.close()


@pytest.mark.asyncio
async def test_proposal_timestamp_repair_preserves_legacy_null_and_orders_new_first(tmp_path):
    db_path = tmp_path / "legacy_proposals.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await db._connection.execute(
        """
        INSERT INTO proposals (proposal_id, ticker, direction, created_at)
        VALUES ('legacy-null', '600000', 'long', NULL)
        """
    )
    await db._connection.commit()

    await db.add_proposal({
        "proposal_id": "new-timestamped",
        "ticker": "510300",
        "direction": "long",
        "created_at": "2026-07-23T15:00:00",
    })

    proposals = await db.get_proposals(limit=5)
    assert [row["proposal_id"] for row in proposals] == [
        "new-timestamped",
        "legacy-null",
    ]
    assert proposals[1]["created_at"] is None
    await db.close()


@pytest.mark.asyncio
async def test_decision_records_absolute_prices(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    decision_id = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=15.0,
        stop_loss=5.0,
        entry_price=10.0,
        expected_days=30,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT current_price, target_price, stop_loss FROM price_predictions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row == (10.0, 11.5, 9.5)


@pytest.mark.asyncio
async def test_validate_pending_waits_for_expected_window(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=30,
    )

    result = await recorder.validate_pending(max_count=10)
    assert result["validated"] == 0


@pytest.mark.asyncio
async def test_simulation_refuses_trade_without_real_price(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.get_current_quote = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=999.0,
    )

    assert result["success"] is False
    assert "实时现价" in result["reason"]


@pytest.mark.asyncio
async def test_simulation_blocks_on_non_trading_day(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=False)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.get_current_price = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=None,
    )

    assert result["success"] is False
    assert "非交易日" in result["reason"]


@pytest.mark.asyncio
async def test_simulation_does_not_buy_for_short_without_position(monkeypatch):
    sim = InvestmentSimulation()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="short",
        target_position=0.1,
        current_price=10.0,
    )

    assert result["success"] is False
    assert result["action"] == "hold"
    assert sim.positions == {}


def test_portfolio_policy_targets_full_deployment_without_strategic_cash(tmp_path):
    status = deployment_status(cash=7200.0, total_assets=10000.0, target_invested_ratio=1.0)

    assert status["target_invested_ratio"] == 1.0
    assert status["invested_ratio"] == pytest.approx(0.28)
    assert status["deployment_gap"] == pytest.approx(7200.0)
    assert deployment_position_floor(7200.0, 10000.0, 4) == pytest.approx(0.18)

    evaluator = load_script_module("run_heuristic_cycle_full_deployment_module", "scripts/run_heuristic_cycle.py")
    weights = evaluator.capped_proportional_allocation(
        {"A": 9.0, "B": 1.0, "C": 1.0, "D": 1.0},
        total_weight=1.0,
        max_weight=0.25,
    )
    assert sum(weights.values()) == pytest.approx(1.0)
    assert max(weights.values()) <= 0.25 + 1e-12

    db_path = tmp_path / "simulation.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE system_stats (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO system_stats VALUES ('simulation_cash', '7200')")
        conn.execute(
            """
            CREATE TABLE simulation_positions (
                ticker TEXT, shares REAL, avg_cost REAL, opened_at TEXT,
                last_mark_price REAL, last_mark_at TEXT, last_mark_source TEXT,
                last_reviewed_at TEXT, review_status TEXT, review_reason TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO simulation_positions VALUES
            ('600050', 100, 4.5, '2026-05-01', 4.5, '2026-05-02',
             'stale local price', '2026-07-13', 'blocked_stale_price', 'stale')
            """
        )
        conn.execute(
            """
            CREATE TABLE simulation_candidate_rejections (
                ticker TEXT, code TEXT, rejection_count INTEGER, last_reason TEXT,
                source TEXT, first_seen_at TEXT, last_seen_at TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO simulation_candidate_rejections VALUES (?, 'committee_hold', 1, 'x', 'test', '2026-07-21', '2026-07-21')",
            [("600519",), ("推荐标的代码",), ("06862",)],
        )
    report = evaluator.build_portfolio_lifecycle_report(db_path)
    assert report["cash"] == pytest.approx(7200.0)
    assert report["status"] == "realtime_valuation_required"
    assert report["invested_ratio"] is None
    assert report["deployment_gap"] is None
    assert [row["ticker"] for row in report["candidate_rejection_memory"]] == ["600519"]


def test_position_review_blocks_non_realtime_price_instead_of_fabricating_exit():
    review = review_position(
        ticker="512660",
        avg_cost=1.483,
        opened_at="2026-07-08T06:02:08",
        price=1.278,
        price_at="2026-06-05",
        price_source="stale local prediction current_price",
        now=datetime.fromisoformat("2026-07-13T09:00:00"),
        max_price_age_days=3,
    )

    assert review.action == "blocked_non_realtime_price"
    assert review.holding_days == 5
    assert review.price_age_days == 38


def test_position_review_persists_price_free_max_duration_exit_without_a_quote():
    review = review_position(
        ticker="512660",
        avg_cost=1.483,
        opened_at="2026-05-12T06:02:08",
        price=None,
        price_at="",
        price_source="realtime_quote_unavailable",
        now=datetime.fromisoformat("2026-07-13T09:00:00"),
        max_holding_days=30,
    )

    assert review.action == "exit"
    assert review.holding_days == 62
    assert review.pnl_pct is None
    assert "最大持有期触发" in review.reason
    assert "price-free退出裁决" in review.reason


def test_position_review_exits_fresh_stop_or_max_holding_breach():
    stopped = review_position(
        ticker="512660",
        avg_cost=1.483,
        opened_at="2026-07-01T09:00:00",
        price=1.30,
        price_at="2026-07-13T09:00:00",
        price_source="test realtime quote",
        now=datetime.fromisoformat("2026-07-13T10:00:00"),
        max_price_age_days=3,
        stop_loss_pct=-0.08,
    )
    expired = review_position(
        ticker="600050",
        avg_cost=4.52,
        opened_at="2026-05-11T09:00:00",
        price=4.60,
        price_at="2026-07-13T09:00:00",
        price_source="test realtime quote",
        now=datetime.fromisoformat("2026-07-13T10:00:00"),
        max_holding_days=30,
    )

    assert stopped.action == "exit"
    assert "止损" in stopped.reason
    assert expired.action == "exit"
    assert "最大持有期" in expired.reason


@pytest.mark.asyncio
async def test_simulation_reviews_every_position_and_only_executes_fresh_exit():
    sim = InvestmentSimulation()
    sim.positions = {
        "512660": {"shares": 300, "avg_cost": 1.483, "opened_at": "2026-05-12T06:02:08"},
        "600050": {"shares": 100, "avg_cost": 4.52, "opened_at": "2026-05-11T02:22:01"},
    }
    sim.resolve_trade_price_detail = AsyncMock(side_effect=[
        {
            "price": None,
            "source": "realtime_quote_unavailable",
            "price_at": "",
        },
        {
            "price": 4.10,
            "source": "test realtime quote",
            "price_at": datetime.now().isoformat(),
        },
    ])
    sim.execute_trade = AsyncMock(return_value={"success": True, "action": "sell"})

    reviews = await sim.review_open_positions()

    assert [row["action"] for row in reviews] == ["exit_pending_execution", "exit"]
    assert reviews[0]["execution"]["blocker_code"] == "realtime_quote_unavailable"
    assert "禁止使用旧价成交" in reviews[0]["execution"]["reason"]
    sim.execute_trade.assert_awaited_once()
    assert sim.execute_trade.await_args.kwargs["ticker"] == "600050"


@pytest.mark.asyncio
async def test_lifecycle_exit_reuses_one_intent_across_research_rounds(
    tmp_path,
    monkeypatch,
):
    db = DatabaseService(str(tmp_path / "lifecycle_exit_idempotency.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    await db._connection.executemany(
        """
        INSERT INTO research_rounds (
            id, base_topic, research_objective, status, current_stage,
            engine_version, started_at, updated_at
        ) VALUES (?, 'test', 'test', 'running', 'created', 'canonical_v1', ?, ?)
        """,
        [
            ("round_first", datetime.now().isoformat(), datetime.now().isoformat()),
            ("round_second", datetime.now().isoformat(), datetime.now().isoformat()),
        ],
    )
    await db._connection.commit()
    opened_at = "2026-08-19T09:30:00"
    sim.positions = {
        "588860": {
            "shares": 1500,
            "avg_cost": 0.63,
            "opened_at": opened_at,
        }
    }
    sim.resolve_trade_price_detail = AsyncMock(return_value={
        "price": 0.73,
        "source": "test_realtime_quote",
        "price_at": datetime.now().isoformat(),
    })
    fake_market = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=False),
        },
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )

    first = await sim.review_open_positions(round_id="round_first")
    second = await sim.review_open_positions(round_id="round_second")
    intents = await db._connection.execute_fetchall(
        """
        SELECT id, round_id, idempotency_key, status
        FROM execution_intents
        WHERE ticker = '588860' AND direction = 'sell'
        """
    )
    pending = await db._connection.execute_fetchall(
        """
        SELECT id, round_id, intent_id, status
        FROM simulation_pending_decisions
        WHERE ticker = '588860' AND direction = 'sell'
        """
    )
    await db.close()

    assert first[0]["action"] == "exit_pending_execution"
    assert second[0]["action"] == "exit_pending_execution"
    assert len(intents) == 1
    assert tuple(intents[0][1:]) == (
        "round_first",
        f"lifecycle:588860:exit:{opened_at}",
        "deferred",
    )
    assert len(pending) == 1
    assert tuple(pending[0][1:]) == (
        "round_first",
        intents[0][0],
        "pending_next_trading_session",
    )
    assert first[0]["execution"]["intent_id"] == intents[0][0]
    assert second[0]["execution"]["intent_id"] == intents[0][0]


@pytest.mark.asyncio
async def test_committee_redeployment_awaits_complete_realtime_asset_estimate(monkeypatch):
    """A lifecycle exit must be able to flow into same-cycle candidate sizing."""
    import sovereign_hall.run_discussion as discussion_module

    context = HeuristicRiskContext(None, "", None, 0.10, True, "test", [])
    monkeypatch.setattr(discussion_module, "load_latest_heuristic_context", lambda: context)
    monkeypatch.setattr(discussion_module, "recent_prediction_observation_count", lambda ticker: 1)
    monkeypatch.setattr(
        discussion_module,
        "apply_heuristic_risk_cap",
        lambda ticker, target, confidence, **kwargs: (target, ""),
    )

    simulation = type("FakeSimulation", (), {})()
    # Same-day fills must not bypass the mandatory lifecycle review.
    simulation.last_trade_date = datetime.now()
    simulation.last_trade_records = {}
    simulation.positions = {}
    simulation.calculate_assets = AsyncMock(return_value={
        "valuation_complete": True,
        "total_assets": 10_000.0,
        "known_total_assets": 10_000.0,
        "cash": 10_000.0,
        "positions_value": 0.0,
        "positions": {},
        "position_values": {},
        "invested_ratio": 0.0,
        "deployment_gap": 10_000.0,
        "target_invested_ratio": 1.0,
        "missing_price_tickers": [],
    })
    simulation.get_recent_reflection = AsyncMock(return_value="")
    simulation.review_open_positions = AsyncMock(return_value=[])
    simulation.daily_reflection = AsyncMock(return_value="")
    simulation.save_snapshot = AsyncMock()
    simulation.is_in_cooldown = lambda ticker: False
    simulation._normalize_ticker = lambda ticker: ticker
    simulation.resolve_trade_price = AsyncMock(return_value=(10.0, "test_realtime_quote"))
    simulation._estimate_trade_assets = AsyncMock(return_value=({}, 10_000.0, []))
    simulation.execute_trade = AsyncMock(return_value={"success": True, "action": "hold"})
    simulation.count_trades_on_date = AsyncMock(return_value=1)
    simulation.record_redeployment_attempt = AsyncMock(return_value={
        "status": "blocked_candidate_execution",
        "deployment_gap": 10_000.0,
        "blocker_code": "candidate_execution_blocked",
    })
    market_data = type(
        "FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)}
    )()

    await run_committee_approved_simulation(
        simulation,
        market_data,
        None,
        [{
            "ticker": "600519",
            "direction": "long",
            "confidence": 0.8,
            "target_position": 0.1,
        }],
    )

    simulation._estimate_trade_assets.assert_awaited_once_with("600519", 10.0)
    simulation.execute_trade.assert_awaited_once()
    simulation.review_open_positions.assert_awaited_once()
    simulation.record_redeployment_attempt.assert_awaited_once()


@pytest.mark.asyncio
async def test_pending_replay_fills_count_in_cycle_without_reassigning_current_round(monkeypatch):
    """Deferred fills are cycle activity but retain their originating rounds."""
    import sovereign_hall.run_discussion as discussion_module

    monkeypatch.setattr(
        discussion_module,
        "load_latest_heuristic_context",
        lambda: HeuristicRiskContext(None, "", None, 0.10, True, "test", []),
    )
    assets = {
        "valuation_complete": True,
        "total_assets": 9_700.0,
        "cash": 4_300.0,
        "positions_value": 5_400.0,
        "positions": {},
        "position_values": {},
        "invested_ratio": 5_400.0 / 9_700.0,
        "deployment_gap": 4_300.0,
        "target_invested_ratio": 1.0,
        "missing_price_tickers": [],
    }
    simulation = type("FakeSimulation", (), {})()
    simulation.calculate_assets = AsyncMock(return_value=assets)
    simulation.get_performance_metrics = AsyncMock(return_value={
        "net_total_return": -0.03,
        "health_status": "degraded_underdeployed",
    })
    simulation.get_recent_reflection = AsyncMock(return_value="")
    simulation.review_open_positions = AsyncMock(return_value=[])
    simulation.replay_pending_decisions = AsyncMock(return_value={
        "status": "processed",
        "pending_before": 3,
        "attempted": 3,
        "executed": 3,
        "rejected": 0,
        "expired": 0,
        "remaining": 0,
        "results": [],
    })
    simulation.record_committee_outcomes = AsyncMock()
    simulation.count_trades_on_date = AsyncMock(return_value=3)
    simulation.record_redeployment_attempt = AsyncMock(return_value={
        "status": "partially_redeployed",
        "deployment_gap": 4_300.0,
        "blocker_code": "residual_operational_cash",
    })
    simulation.daily_reflection = AsyncMock(return_value="")
    simulation.save_snapshot = AsyncMock()
    simulation.positions = {}
    simulation.last_trade_records = {}
    simulation.max_daily_trades = MAX_DAILY_TRADES
    simulation._normalize_ticker = lambda ticker: ticker
    market_data = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=True),
        },
    )()

    result = await run_committee_approved_simulation(
        simulation,
        market_data,
        None,
        [],
        round_id="current_research_round",
        pre_reviewed_positions=[],
    )

    # No intent from current_research_round filled. The three replay fills keep
    # their original round_id, but the operational cycle must not report zero.
    assert result["terminal"] == "no_evidence"
    assert result["fills"] == 0
    assert result["replay_fills"] == 3
    assert result["cycle_fills"] == 3
    attempt = simulation.record_redeployment_attempt.await_args.kwargs
    assert attempt["trade_count"] == 3


def test_pending_replay_fill_resets_empty_backoff_without_reassigning_round_terminal():
    current_round = ResearchRound(
        base_topic="current empty research",
        research_objective="research without inventing evidence",
        status=ResearchRoundStatus.COMPLETED,
        current_stage=ResearchRoundStatus.COMPLETED.value,
        terminal_code="no_evidence",
        terminal_reason="stage2 returned no proposal",
    )

    assert round_has_operational_result(
        current_round,
        {"fills": 0, "replay_fills": 1, "cycle_fills": 1},
    )
    assert current_round.terminal_code == "no_evidence"
    assert not round_has_operational_result(
        current_round,
        {"fills": 0, "replay_fills": 0, "cycle_fills": 0},
    )


def test_incomplete_round_cannot_reuse_previous_cycle_fill_activity():
    active_round = ResearchRound(
        base_topic="interrupted research",
        research_objective="must reach a durable terminal",
        status=ResearchRoundStatus.SOURCES_PERSISTED,
        current_stage=ResearchRoundStatus.SOURCES_PERSISTED.value,
    )

    assert not round_has_operational_result(
        active_round,
        {"fills": 0, "replay_fills": 1, "cycle_fills": 1},
    )


def test_weaker_research_terminal_cannot_overwrite_atomic_fill_terminal():
    filled_during_lifecycle = ResearchRound(
        base_topic="lifecycle exit",
        research_objective="review before research",
        status=ResearchRoundStatus.SOURCES_PERSISTED,
        current_stage=ResearchRoundStatus.SOURCES_PERSISTED.value,
        terminal_code="filled",
        terminal_reason="atomic simulated sell committed",
    )

    no_proposal_after_fill = filled_during_lifecycle.transition(
        ResearchRoundStatus.NO_EVIDENCE,
        terminal_code="no_evidence",
        terminal_reason="stage2 returned no proposal",
    )

    assert no_proposal_after_fill.status == ResearchRoundStatus.NO_EVIDENCE
    assert no_proposal_after_fill.terminal_code == "filled"
    assert no_proposal_after_fill.terminal_reason == "atomic simulated sell committed"


@pytest.mark.asyncio
async def test_closed_market_keeps_lifecycle_and_committee_audit_then_queues_without_fill(monkeypatch):
    import sovereign_hall.run_discussion as discussion_module

    context = HeuristicRiskContext(
        run_dir=PROJECT_ROOT,
        policy_name="closed_session_preflight",
        score=0.01,
        max_position=0.10,
        min_confidence=0.65,
        min_signal_count=1,
        overfit_risk=True,
        warning="test",
        failure_cases=[],
        tape_update={"validation_status": "stale_tape"},
    )
    monkeypatch.setattr(discussion_module, "load_latest_heuristic_context", lambda: context)
    monkeypatch.setattr(
        discussion_module,
        "recent_prediction_observation_count",
        lambda ticker: 1,
    )
    simulation = type("FakeSimulation", (), {})()
    assets = {
        "valuation_complete": True,
        "total_assets": 10_000.0,
        "cash": 10_000.0,
        "positions_value": 0.0,
        "positions": {},
        "position_values": {},
        "invested_ratio": 0.0,
        "deployment_gap": 10_000.0,
        "target_invested_ratio": 1.0,
        "missing_price_tickers": [],
    }
    simulation.calculate_assets = AsyncMock(return_value=assets)
    simulation.get_recent_reflection = AsyncMock(return_value="")
    simulation.review_open_positions = AsyncMock(return_value=[])
    simulation.replay_pending_decisions = AsyncMock(
        return_value={"status": "waiting_market_open", "pending_before": 0}
    )
    simulation.record_committee_outcomes = AsyncMock()
    simulation.count_trades_on_date = AsyncMock(return_value=0)
    simulation.record_pending_decision = AsyncMock(return_value=41)
    simulation.record_redeployment_attempt = AsyncMock(return_value={
        "status": "pending_market_session",
        "deployment_gap": 10_000.0,
        "blocker_code": "market_session_pending",
    })
    simulation.daily_reflection = AsyncMock(return_value="")
    simulation.save_snapshot = AsyncMock()
    simulation.execute_trade = AsyncMock()
    simulation.resolve_trade_price = AsyncMock()
    simulation.positions = {}
    simulation.last_trade_records = {}
    simulation.max_daily_trades = MAX_DAILY_TRADES
    simulation._normalize_ticker = lambda ticker: ticker
    market_data = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=False),
        },
    )()
    decisions = [{
        "ticker": "600519",
        "direction": "long",
        "confidence": 0.8,
        "target_position": 0.1,
        "vote_summary": {"long": 6.0, "hold": 2.5, "short": 0.0},
        "vote_quorum_met": True,
    }]

    await run_committee_approved_simulation(simulation, market_data, None, decisions)

    simulation.review_open_positions.assert_awaited_once()
    simulation.record_committee_outcomes.assert_awaited_once_with(
        decisions, source="run_discussion"
    )
    simulation.record_pending_decision.assert_awaited_once()
    assert simulation.record_pending_decision.await_args.kwargs["target_position"] == pytest.approx(0.10)
    simulation.execute_trade.assert_not_awaited()
    simulation.resolve_trade_price.assert_not_awaited()
    simulation.record_redeployment_attempt.assert_awaited_once()
    assert simulation.record_redeployment_attempt.await_args.kwargs["pending_count"] == 1

    # A low-confidence directional forecast must not become a deferred ruling
    # merely because the market is closed. It remains auditable as an explicit
    # heuristic veto and can never bypass the open-session entry gate.
    simulation.record_pending_decision.reset_mock()
    simulation.record_redeployment_attempt.reset_mock()
    decisions[0]["confidence"] = 0.40

    await run_committee_approved_simulation(simulation, market_data, None, decisions)

    simulation.record_pending_decision.assert_not_awaited()
    attempt = simulation.record_redeployment_attempt.await_args.kwargs
    assert attempt["pending_count"] == 0
    assert any(
        rejection["code"] == "heuristic_entry_veto"
        for rejection in attempt["rejections"]
    )


@pytest.mark.asyncio
async def test_redeployment_queue_recovers_and_persists_attempts(tmp_path, capsys):
    db = DatabaseService(str(tmp_path / "test.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.cash = 9_727.22
    sim.positions = {}
    await sim.save_state()

    await sim._bootstrap_redeployment_state()
    recovered = await sim.get_redeployment_state()
    assert recovered["status"] == "pending_approved_candidates"
    assert recovered["deployment_gap"] == pytest.approx(9_727.22)
    assert recovered["source"] == "account_state_recovery"

    await sim.record_redeployment_attempt(
        {
            "valuation_complete": True,
            "deployment_gap": 9_727.22,
        },
        candidate_count=0,
        trade_count=0,
        blockers=["投委会无批准的可执行多头候选"],
        rejections=[
            {"code": "committee_hold", "ticker": "600519", "reason": "证据不足"},
            {"code": "committee_hold", "ticker": "510300", "reason": "证据不足"},
        ],
    )
    attempted = await sim.get_redeployment_state()
    assert attempted["status"] == "blocked_no_approved_candidates"
    assert attempted["attempt_count"] == 1
    assert attempted["last_candidate_count"] == 0
    assert "投委会" in attempted["blocker_reason"]
    assert attempted["last_rejection_counts"] == {"committee_hold": 2}
    assert attempted["rejection_counts_total"] == {"committee_hold": 2}
    assert "committee_hold=2" in attempted["next_action"]
    rejection_memory = await sim.get_candidate_rejection_memory()
    assert {(row["ticker"], row["code"], row["rejection_count"]) for row in rejection_memory} == {
        ("600519", "committee_hold", 1),
        ("510300", "committee_hold", 1),
    }
    feedback = await sim.format_redeployment_learning_context()
    assert "模拟再配置逐标的拒绝记忆" in feedback
    assert "600519 / committee_hold" in feedback
    assert "不得原样重提" in feedback
    await sim._record_candidate_rejections(
        [{"code": "market_closed", "ticker": "600519", "reason": "等待开市"}],
        source="test",
    )
    evidence_feedback = await sim.format_redeployment_learning_context()
    assert "market_closed" not in evidence_feedback
    assert "等待开市" not in evidence_feedback
    combined_prompt = build_lessons_with_heuristic_context(
        "历史教训",
        redeployment_context=evidence_feedback,
    )
    assert "历史教训" in combined_prompt
    assert "新增的本地可追溯证据" in combined_prompt

    await sim._record_candidate_rejections(
        [{"code": "committee_hold", "ticker": "推荐标的代码", "reason": "示例占位符"}],
        source="test",
    )
    async with db._connection.execute(
        "SELECT ticker FROM simulation_candidate_rejections WHERE ticker = ?",
        ("推荐标的代码",),
    ) as cursor:
        invalid_rows = await cursor.fetchall()
    assert invalid_rows == []

    await sim.record_redeployment_attempt(
        {"valuation_complete": True, "deployment_gap": 9_727.22},
        candidate_count=0,
        trade_count=0,
        blockers=["ticker缺失"],
        rejections=[{"code": "missing_ticker", "ticker": "", "reason": "ticker缺失"}],
    )
    attempted = await sim.get_redeployment_state()
    assert attempted["attempt_count"] == 2
    assert attempted["last_rejection_counts"] == {"missing_ticker": 1}
    assert attempted["rejection_counts_total"] == {
        "committee_hold": 2,
        "missing_ticker": 1,
    }

    restarted = InvestmentSimulation(db)
    await restarted.initialize()
    persisted = await restarted.get_redeployment_state()
    assert persisted["status"] == "blocked_no_approved_candidates"
    assert persisted["attempt_count"] == 2
    assert persisted["rejection_counts_total"]["committee_hold"] == 2
    await restarted.record_committee_outcomes(
        [{
            "ticker": "600519",
            "direction": "hold",
            "confidence": 0.7,
            "target_position": 0.0,
            "vote_summary": {"long": 2.0, "short": 0.0, "hold": 5.5},
            "vote_margin": 0.4118,
            "vote_count": 7,
            "parsed_vote_count": 7,
            "invalid_vote_count": 0,
            "vote_quorum_required": 5,
            "vote_quorum_met": True,
            "review_depth": "full",
            "prediction_id": "hold-prediction-1",
            "evidence_gaps": ["缺少可核验订单增速"],
            "reconsider_if": ["订单增速连续两期为正"],
            "individual_votes": [{
                "role": "CIO综合视角",
                "direction": "hold",
                "effective_weight": 2.0,
            }],
            "stage_execution_audit": [{
                "stage": "round4_vote",
                "task_count": 7,
                "completed_count": 6,
                "timeout_count": 1,
                "error_count": 0,
                "absent_labels": ["消费行业视角"],
            }],
            "deadlock_review": {
                "triggered": True,
                "adopted": False,
                "review_direction": "hold",
                "review_confidence": 0.6,
                "review_direction_support": 1.0,
            },
        }],
        source="test",
    )
    await db.close()

    import sovereign_hall.check_db as check_db

    check_db.show_investment_status(tmp_path / "test.db")
    output = capsys.readouterr().out
    assert "逐标的重复拒绝记忆" in output
    assert "600519 / committee_hold x1" in output
    assert "重提要求: 必须给出新增本地可追溯证据" in output
    assert "投委会票型审计" in output
    assert "有效票=7/7" in output
    assert "可验证反馈链接: 1/1；其中hold=1/1" in output
    assert "逐角色票型审计: 1/1" in output
    assert "阶段执行审计: 1/1；累计超时任务=1，错误任务=0" in output
    assert "空仓部署死锁复核: 触发=1，通过=0" in output
    assert "死锁复核: adopted=False" in output
    assert "HOLD补证闭环: 明确证据缺口=1/1；明确重审条件=1/1" in output
    assert "角色票: CIO综合视角=hold@2.00" in output
    assert "补证缺口: 缺少可核验订单增速" in output
    assert "重审条件: 订单增速连续两期为正" in output


def test_check_db_detects_meetings_newer_than_committee_execution_audit(tmp_path):
    import sovereign_hall.check_db as check_db

    db_path = tmp_path / "audit_gap.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE simulation_committee_outcomes (
            id INTEGER PRIMARY KEY,
            ticker TEXT,
            direction TEXT,
            vote_summary TEXT,
            vote_margin REAL,
            vote_count INTEGER,
            parsed_vote_count INTEGER,
            invalid_vote_count INTEGER,
            quorum_required INTEGER,
            quorum_met INTEGER,
            review_depth TEXT,
            prediction_id TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE TABLE meetings (id TEXT, created_at TEXT)"
    )
    conn.execute(
        """
        INSERT INTO simulation_committee_outcomes VALUES
        (1, '600519', 'hold', '{}', 1, 7, 7, 0, 5, 1, 'full', NULL, '2026-07-24T10:00:00')
        """
    )
    conn.executemany(
        "INSERT INTO meetings VALUES (?, ?)",
        [
            ("old", "2026-07-24T09:00:00"),
            ("new-1", "2026-07-25T09:00:00"),
            ("new-2", "2026-07-26T09:00:00"),
        ],
    )
    conn.commit()
    conn.row_factory = sqlite3.Row

    diagnostics = check_db.committee_outcome_diagnostics(conn)
    conn.close()

    assert diagnostics["newer_meeting_count"] == 2
    assert diagnostics["latest_meeting_at"] == "2026-07-26T09:00:00"


@pytest.mark.asyncio
async def test_simulation_position_schema_migrates_lifecycle_columns(tmp_path):
    db = DatabaseService(str(tmp_path / "test.db"))
    await db._init_db()
    conn = db._connection
    await conn.execute("DROP TABLE IF EXISTS simulation_positions")
    await conn.execute(
        "CREATE TABLE simulation_positions (ticker TEXT PRIMARY KEY, shares INTEGER, avg_cost REAL, updated_at TEXT)"
    )
    await conn.execute(
        "INSERT INTO simulation_positions VALUES ('600050', 100, 4.52, '2026-05-11T02:22:01')"
    )
    await conn.commit()

    sim = InvestmentSimulation(db)
    await sim.init_tables()
    async with conn.execute("PRAGMA table_info(simulation_positions)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    async with conn.execute("PRAGMA table_info(simulation_committee_outcomes)") as cursor:
        committee_columns = {row[1] for row in await cursor.fetchall()}
    await db.close()

    assert {
        "opened_at", "peak_price", "last_mark_price", "last_mark_at",
        "last_mark_source", "last_reviewed_at", "review_status", "review_reason",
    } <= columns
    assert {
        "evidence_gaps", "reconsider_if", "individual_votes",
        "stage_execution_audit", "deadlock_review",
        "initial_committee_decision",
    } <= committee_columns


def test_heuristic_risk_cap_uses_latest_policy_as_constraint(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[],
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.25, 0.7, context=context)

    assert capped == 0.10
    assert "限制" in reason
    assert "样本外风险" in reason


def test_heuristic_risk_cap_uses_full_investment_target_instead_of_cash_reserve(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.067,
        max_position=0.05,
        max_gross=0.15,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
    )

    capped, reason = apply_heuristic_risk_cap(
        "600519",
        0.05,
        0.8,
        current_position=0.0,
        current_gross_exposure=0.13,
        context=context,
    )
    checklist = format_policy_checklist(context)

    assert capped == pytest.approx(0.05)
    assert reason is None
    assert "组合目标投资比例=100%" in checklist


def test_heuristic_risk_cap_tightens_recent_failure_ticker(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "market_state": {"ticker": "000977"},
                "signals": {},
                "positions": {},
            }
        ],
    )

    capped, reason = apply_heuristic_risk_cap("000977.SZ", 0.10, 0.8, context=context)

    assert capped == 0.05
    assert "failure case" in reason


def test_heuristic_risk_cap_warns_on_thin_cost_stress(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.031,
        max_position=0.08,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.157,
        cost_stress_score=0.014,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.06, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.06
    assert "成本扰动余量很薄" in reason
    assert "3x滑点 0.014000" in status
    assert "样本外score=0.157000" in prompt


def test_heuristic_risk_cap_tightens_failed_etf_sleeve(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.027,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        sleeve_diagnostics={
            "allocator_status": "not_promoted",
            "sleeves": {
                "etf": {
                    "score": -0.06,
                    "cost_stress_score": -0.09,
                    "promotable": False,
                    "reason": "主样本score未转正；3x滑点余量低于0.02",
                },
                "single_stock": {
                    "score": 0.027,
                    "cost_stress_score": 0.011,
                    "promotable": False,
                    "reason": "3x滑点余量低于0.02",
                },
            },
        },
    )

    capped, reason = apply_heuristic_risk_cap("512880", 0.08, 0.8, context=context)
    single_capped, single_reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.04
    assert "ETF sleeve" in reason
    assert single_capped == 0.08
    assert single_reason is None
    assert "sleeve allocator: not_promoted" in status
    assert "etf cap/warning score=-0.060000" in prompt


def test_heuristic_risk_cap_uses_reduced_single_stock_cap(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap6",
        score=0.061,
        max_position=0.06,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        out_of_sample_score=0.073,
        cost_stress_score=0.053,
        sleeve_diagnostics={
            "allocator_status": "not_promoted",
            "sleeves": {
                "etf": {
                    "score": -0.09,
                    "cost_stress_score": -0.11,
                    "promotable": False,
                    "reason": "主样本score未转正；3x滑点余量低于0.02",
                },
                "single_stock": {
                    "score": 0.061,
                    "cost_stress_score": 0.053,
                    "promotable": True,
                    "reason": "通过主样本、样本外和3x滑点检查",
                },
            },
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)

    assert capped == pytest.approx(0.06)
    assert "限制到6.0%" in reason
    assert "single_stock pass score=0.061000" in status


def test_heuristic_context_warns_when_price_source_is_unvalidated(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap6",
        score=0.056,
        max_position=0.06,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.095,
        cost_stress_score=0.049,
        price_source="prediction current_price fallback; daily_prices table unavailable or empty",
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.06, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.015)
    assert "限制到1.5%" in reason
    assert "daily_prices缺失" in reason
    assert "禁止放大仓位" in reason
    assert "数据质量风险" in status
    assert "弱价格覆盖历史证据复用上限: 1.5%" in status
    assert "弱价格覆盖历史证据仓位<=1.5%" in prompt
    assert "current_price fallback" in prompt


def test_heuristic_context_surfaces_min_signal_count(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.056,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        out_of_sample_score=0.068,
        cost_stress_score=0.052,
        min_signal_count=2,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == 0.05
    assert "至少2条本地同日预测观察" in reason
    assert "本地信号观察门槛: >=2" in status
    assert "本地信号观察门槛=2条" in prompt


def test_heuristic_context_surfaces_evaluation_engine(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        evaluation_engine="stdlib_fallback",
        evaluation_warning="numpy/pandas import did not complete during preflight",
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert "评估引擎: stdlib_fallback" in status
    assert "评估提示: numpy/pandas import did not complete during preflight" in status
    assert "评估引擎: stdlib_fallback" in prompt
    assert "numpy/pandas import did not complete during preflight" in prompt


def test_heuristic_context_surfaces_evaluator_health(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.063373,
        max_position=0.05,
        overfit_risk=False,
        warning="主评估器已本地复核fallback结果",
        failure_cases=[],
        evaluation_engine="stdlib_fallback",
        evaluator_health={
            "validation_status": "matched",
            "baseline_engine": "stdlib_fallback",
            "validation_engine": "pandas_primary",
            "baseline_score": 0.06337303806043082,
            "validation_score": 0.06337303806043082,
            "score_abs_diff": 0.0,
            "score_tolerance": 1e-9,
        },
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert "评估器复核: matched: stdlib_fallback vs pandas_primary" in status
    assert "score差=0" in status
    assert "baseline=0.063373, validation=0.063373" in prompt


def test_heuristic_risk_cap_tightens_insufficient_signal_count(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.056,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        min_signal_count=2,
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, signal_count=1, context=context)

    assert capped == pytest.approx(0.015)
    assert "本地同日预测观察1/2不足" in reason
    assert "孤证仓位上限1.5%" in reason


def test_recent_prediction_observation_count_uses_latest_fresh_day(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE price_predictions (ticker TEXT, predicted_at TEXT)")
    conn.executemany(
        "INSERT INTO price_predictions (ticker, predicted_at) VALUES (?, ?)",
        [
            ("600519", "2026-06-09T10:00:00"),
            ("600519.SH", "2026-06-11T10:00:00"),
            ("600519", "2026-06-11T14:30:00"),
            ("000858", "2026-06-11T14:30:00"),
        ],
    )
    conn.commit()
    conn.close()

    count = recent_prediction_observation_count(
        "600519.SH",
        db_path=db_path,
        now=datetime.fromisoformat("2026-06-12T09:00:00"),
    )
    stale_count = recent_prediction_observation_count(
        "600519",
        db_path=db_path,
        max_age_days=0,
        now=datetime.fromisoformat("2026-06-12T09:00:00"),
    )

    assert count == 2
    assert stale_count == 0


def test_tape_freshness_is_recomputed_and_vetoes_new_simulated_long(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_freshness

    tape = refresh_tape_update_freshness(
        {
            "validation_status": "thin_tape_update",
            "current_latest_prediction_date": "2026-07-06",
            "latest_prediction_age_days": 3,
            "max_latest_prediction_age_days": 3,
            "new_prediction_rows_since_previous": 0,
        },
        now=datetime.fromisoformat("2026-07-11T09:00:00"),
    )
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="stale_tape_guard",
        score=0.0,
        max_position=0.04,
        overfit_risk=False,
        warning="local only",
        failure_cases=[],
        tape_update=tape,
    )

    new_cap, new_reason = apply_heuristic_risk_cap(
        "600519", 0.04, 0.8, current_position=0.0, current_gross_exposure=0.0, context=context
    )
    held_cap, held_reason = apply_heuristic_risk_cap(
        "600519", 0.04, 0.8, current_position=0.02, current_gross_exposure=0.02, context=context
    )
    status = format_heuristic_status(context)

    assert tape["latest_prediction_age_days"] == 5
    assert tape["validation_status"] == "stale_tape"
    assert new_cap == 0.0
    assert held_cap == pytest.approx(0.02)
    assert "拒绝新增或扩大模拟多头仓位" in new_reason
    assert "拒绝新增或扩大模拟多头仓位" in held_reason
    assert "陈旧tape复用否决" in status


def test_current_committee_evidence_is_not_frozen_by_historical_price_gaps(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="fresh_committee_deployment",
        score=-0.01,
        max_position=0.10,
        min_confidence=0.65,
        min_signal_count=1,
        overfit_risk=True,
        warning="historical coverage incomplete",
        failure_cases=[],
        price_source="prediction current_price fallback; daily_prices partial",
        price_coverage={"status": "partial_daily_prices_low_signal_coverage", "independent_price_row_ratio": 0.0},
        tape_update={"validation_status": "stale_tape"},
        price_readiness_stall={"status": "stalled_partial_daily_prices"},
    )

    capped, reason = apply_heuristic_risk_cap(
        "600519",
        0.10,
        0.80,
        signal_count=1,
        current_position=0.0,
        current_gross_exposure=0.0,
        fresh_local_evidence=True,
        context=context,
    )
    stale_cap, _ = apply_heuristic_risk_cap(
        "600519",
        0.10,
        0.80,
        signal_count=1,
        current_position=0.0,
        current_gross_exposure=0.0,
        context=context,
    )

    assert capped == pytest.approx(0.10)
    assert "不再强制空仓" in reason
    assert stale_cap == 0.0


def test_tape_entry_veto_clears_only_with_fresh_broad_update(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_freshness

    base = {
        "validation_status": "stale_tape",
        "current_latest_prediction_date": "2026-07-09",
        "latest_prediction_age_days": 9,
        "max_latest_prediction_age_days": 3,
        "min_new_rows_for_validation": 20,
        "min_latest_date_rows_for_validation": 5,
        "entry_veto_reason": "old artifact value",
    }
    boundary_now = datetime.fromisoformat("2026-07-12T09:00:00")
    broad = refresh_tape_update_freshness(
        {**base, "new_prediction_rows_since_previous": 20, "latest_date_prediction_rows": 5},
        now=boundary_now,
    )
    thin = refresh_tape_update_freshness(
        {**base, "new_prediction_rows_since_previous": 20, "latest_date_prediction_rows": 4},
        now=boundary_now,
    )
    expired = refresh_tape_update_freshness(
        {
            **base,
            "current_latest_prediction_date": "2026-07-08",
            "new_prediction_rows_since_previous": 20,
            "latest_date_prediction_rows": 5,
        },
        now=boundary_now,
    )

    assert broad["latest_prediction_age_days"] == 3
    assert broad["validation_status"] == "fresh_tape_update"
    assert broad["enough_for_policy_widening"] is True
    assert "entry_veto_reason" not in broad
    assert thin["validation_status"] == "thin_tape_update"
    assert thin["enough_for_policy_widening"] is False
    assert thin["freshness_recovery_pending"] is True
    assert "entry_veto_reason" not in thin
    assert expired["latest_prediction_age_days"] == 4
    assert expired["validation_status"] == "stale_tape"
    assert expired["enough_for_policy_widening"] is False


def test_entry_tape_refresh_overlays_new_local_db_rows(tmp_path):
    from sovereign_hall.services.heuristic_policy import refresh_tape_update_from_local_db

    db_path = tmp_path / "local.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE price_predictions (ticker TEXT, predicted_at TEXT)")
        conn.executemany(
            "INSERT INTO price_predictions VALUES (?, ?)",
            [
                ("600030", "2026-07-06T10:00:00"),
                ("159995", "2026-07-06T11:00:00"),
                ("159985", "2026-07-13T01:31:37"),
            ],
        )

    refreshed = refresh_tape_update_from_local_db(
        {
            "validation_status": "stale_tape",
            "current_prediction_rows": 2,
            "new_prediction_rows_since_previous": 0,
            "current_latest_prediction_date": "2026-07-06",
            "latest_date_prediction_rows": 2,
            "min_new_rows_for_validation": 20,
            "min_latest_date_rows_for_validation": 5,
            "max_latest_prediction_age_days": 3,
        },
        db_path=db_path,
        now=datetime.fromisoformat("2026-07-13T09:00:00"),
    )

    assert refreshed["current_prediction_rows"] == 3
    assert refreshed["new_prediction_rows_since_previous"] == 1
    assert refreshed["current_latest_prediction_date"] == "2026-07-13"
    assert refreshed["latest_date_prediction_rows"] == 1
    assert refreshed["latest_prediction_age_days"] == 0
    assert refreshed["validation_status"] == "thin_tape_update"
    assert refreshed["enough_for_policy_widening"] is False
    assert refreshed["freshness_recovery_pending"] is True
    assert refreshed["live_db_appended_rows_since_run"] == 1

    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="recovery_guard",
        score=0.0,
        max_position=0.04,
        overfit_risk=True,
        warning="local only",
        failure_cases=[],
        tape_update=refreshed,
    )
    assert context.stale_tape_entry_veto is True


def test_cycle_propagates_pending_stale_recovery_across_thin_runs(tmp_path):
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_recovery_module", "scripts/run_heuristic_cycle.py")
    stale_run = tmp_path / "20260712_120000"
    thin_run = tmp_path / "20260713_120000"
    stale_run.mkdir()
    thin_run.mkdir()
    (stale_run / "tape_update.json").write_text(
        json.dumps({"validation_status": "stale_tape", "current_prediction_rows": 2}),
        encoding="utf-8",
    )
    (thin_run / "tape_update.json").write_text(
        json.dumps(
            {
                "validation_status": "thin_tape_update",
                "current_prediction_rows": 3,
                "previous_run": str(stale_run),
            }
        ),
        encoding="utf-8",
    )
    now = datetime.now()
    predictions = pd.DataFrame(
        {"predicted_at": pd.to_datetime([now, now, now])}
    )

    report = module.build_tape_update_report(predictions, thin_run)

    assert report["validation_status"] == "thin_tape_update"
    assert report["freshness_recovery_pending"] is True


def test_cycle_tape_baseline_skips_same_day_reruns(tmp_path):
    module = load_script_module("run_heuristic_cycle_baseline_module", "scripts/run_heuristic_cycle.py")
    old_run = tmp_path / "20260712_120000"
    rerun_one = tmp_path / "20260713_120000"
    rerun_two = tmp_path / "20260713_121000"
    for run in (old_run, rerun_one, rerun_two):
        run.mkdir()
    (old_run / "tape_update.json").write_text(
        json.dumps({"validation_status": "stale_tape", "current_prediction_rows": 2}), encoding="utf-8"
    )
    (rerun_one / "tape_update.json").write_text(
        json.dumps({"validation_status": "thin_tape_update", "previous_run": str(old_run)}), encoding="utf-8"
    )
    (rerun_two / "tape_update.json").write_text(
        json.dumps({"validation_status": "thin_tape_update", "previous_run": str(rerun_one)}), encoding="utf-8"
    )

    baseline = module.distinct_date_tape_baseline(rerun_two, "20260713")

    assert baseline == old_run


def test_sparse_split_checks_are_inconclusive_not_robust():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_sparse_split_module", "scripts/run_heuristic_cycle.py")
    daily = pd.DataFrame(
        [
            {
                "date": f"2026-06-{day:02d}",
                "ticker": "600519",
                "price": 10.0,
                "price_source": "daily_prices",
                "confidence": 0.1,
                "risk_reward": 0.0,
                "close_observations": 1,
                "stop_gap": 0.05,
                "signal_strength": 0.1,
                "return_1d": 0.0,
                "momentum_2d": 0.0,
                "momentum_3d": 0.0,
                "momentum_5d": 0.0,
                "vol_2d": 0.0,
                "vol_3d": 0.0,
                "vol_5d": 0.0,
            }
            for day in range(1, 11)
        ]
    )
    checks = module.split_checks(daily, module.PolicyConfig(name="no_trade"), module.CostConfig())

    assert checks["insufficient_trade_evidence"] is True
    assert checks["overfit_risk"] is True
    assert "risk avoidance" in checks["inconclusive_reason"]


def test_zero_trade_failure_analysis_does_not_invent_drawdown_or_overtrading():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_zero_trade_failure_module", "scripts/run_heuristic_cycle.py")
    curve = pd.DataFrame(
        [
            {
                "date": "2026-07-01",
                "signal_date": "2026-06-30",
                "equity": 1.0,
                "net_return": 0.0,
                "turnover": 0.0,
                "gross_exposure": 0.0,
                "cost": 0.0,
                "positions": "{}",
            },
            {
                "date": "2026-07-02",
                "signal_date": "2026-07-01",
                "equity": 1.0,
                "net_return": 0.0,
                "turnover": 0.0,
                "gross_exposure": 0.0,
                "cost": 0.0,
                "positions": "{}",
            },
        ]
    )
    daily = pd.DataFrame(
        columns=["date", "ticker", "price", "confidence", "risk_reward", "close_observations"]
    )
    failures = module.analyze_failures(
        {"curve": curve, "trades": []}, daily, module.PolicyConfig(name="no_trade")
    )

    assert [row["case_type"] for row in failures] == ["insufficient_trade_evidence"]
    assert failures[0]["market_state"]["gross_exposure"] == 0.0
    assert "no positions opened" in failures[0]["result"]


def test_heuristic_context_surfaces_price_coverage(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_source="prediction current_price fallback; daily_prices table unavailable or empty",
        price_coverage={
            "status": "unvalidated_prediction_current_price_fallback",
            "independent_price_row_ratio": 0.0,
            "missing_position_price_slot_ratio": 0.3778,
            "missing_price_day_ratio": 0.2791,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.08, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.0125)
    assert "持仓缺价槽位37.8%" in reason
    assert "弱覆盖模拟买入上限1.2%" in reason
    assert "价格覆盖" in status
    assert "弱价格覆盖历史证据复用上限: 1.2%" in status
    assert "daily_prices覆盖0.0%" in prompt
    assert "弱覆盖模拟买入上限=1.2%" in prompt


def test_heuristic_price_coverage_cap_scales_with_partial_coverage(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_source="daily_prices table with fallback to prediction current_price",
        price_coverage={
            "status": "partial_daily_prices_with_missing_hold_prices",
            "independent_price_row_ratio": 0.60,
            "missing_position_price_slot_ratio": 0.18,
            "missing_price_day_ratio": 0.10,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)

    assert capped == pytest.approx(0.0175)
    assert "弱覆盖模拟买入上限1.7%" in reason


def test_pandas_daily_tape_uses_bounded_asof_daily_prices():
    import pandas as pd

    module = load_script_module("run_heuristic_cycle_test_module", "scripts/run_heuristic_cycle.py")

    predictions = pd.DataFrame(
        [
            {
                "date": "2026-06-20",
                "ticker": "159995",
                "current_price": 2.7,
                "target_price": 3.0,
                "stop_loss": 2.5,
                "direction": "long",
                "confidence": 0.8,
                "expected_days": 30,
            },
            {
                "date": "2026-06-30",
                "ticker": "159995",
                "current_price": 2.8,
                "target_price": 3.1,
                "stop_loss": 2.6,
                "direction": "long",
                "confidence": 0.8,
                "expected_days": 30,
            },
        ]
    )
    price_history = pd.DataFrame(
        [{"date": "2026-06-18", "ticker": "159995", "close": 2.55}]
    )

    daily = module.build_daily_tape(predictions, price_history)
    by_date = daily.set_index("date")

    assert by_date.loc["2026-06-20", "price"] == pytest.approx(2.55)
    assert by_date.loc["2026-06-20", "price_source"] == "daily_prices"
    assert str(by_date.loc["2026-06-20", "daily_price_date"])[:10] == "2026-06-18"
    assert by_date.loc["2026-06-30", "price"] == pytest.approx(2.8)
    assert by_date.loc["2026-06-30", "price_source"] == "prediction_current_price"


def test_stdlib_daily_tape_uses_bounded_asof_daily_prices():
    module = load_script_module(
        "run_heuristic_cycle_stdlib_test_module",
        "scripts/run_heuristic_cycle_stdlib.py",
    )

    predictions = [
        {
            "date": "2026-06-20",
            "ticker": "159995",
            "current_price": 2.7,
            "target_price": 3.0,
            "stop_loss": 2.5,
            "direction": "long",
            "confidence": 0.8,
            "expected_days": 30,
        },
        {
            "date": "2026-06-30",
            "ticker": "159995",
            "current_price": 2.8,
            "target_price": 3.1,
            "stop_loss": 2.6,
            "direction": "long",
            "confidence": 0.8,
            "expected_days": 30,
        },
    ]

    daily = module.build_daily_tape(predictions, {("2026-06-18", "159995"): 2.55})
    by_date = {row["date"]: row for row in daily}

    assert by_date["2026-06-20"]["price"] == pytest.approx(2.55)
    assert by_date["2026-06-20"]["price_source"] == "daily_prices"
    assert by_date["2026-06-30"]["price"] == pytest.approx(2.8)
    assert by_date["2026-06-30"]["price_source"] == "prediction_current_price"


def test_heuristic_context_surfaces_price_readiness(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "total_signal_ticker_count": 12,
            "priced_signal_ticker_count": 0,
            "missing_signal_ticker_count": 12,
            "latest_signal_date": "2026-06-20",
            "latest_missing_tickers": ["600519", "688256"],
            "minimum_next_rows": 2,
            "missing_tickers_top10": [
                {
                    "ticker": "600519",
                    "signal_days": 45,
                    "first_signal_date": "2026-05-01",
                    "last_signal_date": "2026-06-20",
                    "total_signal_observations": 1585,
                },
                {
                    "ticker": "512880",
                    "signal_days": 44,
                    "first_signal_date": "2026-05-02",
                    "last_signal_date": "2026-06-10",
                    "total_signal_observations": 1197,
                },
            ],
            "backfill_plan_path": str(tmp_path / "daily_price_backfill_plan.csv"),
            "backfill_plan": {
                "total_missing_tickers": 12,
                "minimum_next_rows": 2,
                "top_priority_tickers": ["600519", "512880"],
            },
            "next_action": "Backfill latest local daily_prices first.",
        },
    )

    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)
    queue = format_price_readiness_backfill_queue(context)
    plan = format_price_readiness_backfill_plan(context)

    assert "daily_prices补齐: blocked_no_daily_prices" in status
    assert "daily_prices阻塞历史证据复用上限: 0.5%" in status
    assert "daily_prices优先补齐队列: 600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20)" in status
    assert "daily_prices补齐计划: plan=" in status
    assert "top=600519, 512880" in status
    assert "缺少12/12个signal ticker" in status
    assert "最新缺价ticker=600519, 688256" in prompt
    assert "daily_prices优先补齐队列: 600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20)" in prompt
    assert "daily_prices补齐计划: plan=" in prompt
    assert "daily_prices阻塞模拟买入上限=0.5%" in prompt
    assert "本地数据质量任务" in prompt
    assert queue.startswith("600519(missing_days=45d, obs=1585, missing_range=2026-05-01..2026-06-20), 512880")
    assert "missing_tickers=12" in plan
    assert "latest_rows_to_unblock=2" in plan


def test_heuristic_risk_cap_tightens_blocked_price_readiness(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.055,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness={
            "status": "blocked_no_daily_prices",
            "total_signal_ticker_count": 12,
            "priced_signal_ticker_count": 0,
            "missing_signal_ticker_count": 12,
            "latest_missing_tickers": ["159995"],
            "minimum_next_rows": 1,
        },
    )

    capped, reason = apply_heuristic_risk_cap(
        "159995",
        0.10,
        confidence=0.8,
        context=context,
    )

    assert capped == pytest.approx(0.005)
    assert "daily_prices补齐blocked_no_daily_prices" in reason
    assert "补齐前模拟买入上限0.5%" in reason


def test_price_readiness_stall_report_counts_consecutive_blocked_runs(tmp_path):
    readiness_payload = {
        "status": "blocked_no_daily_prices",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 0,
        "missing_signal_ticker_count": 307,
        "latest_missing_tickers": ["159995"],
        "missing_tickers_top10": [{"ticker": "159995", "signal_days": 45}],
    }
    for run_id in ("20260622_123523", "20260623_123529", "20260624_123500"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)

    assert report["status"] == "stalled_no_daily_prices"
    assert report["consecutive_blocked_runs"] == 3
    assert report["first_blocked_run"] == "20260622_123523"
    assert report["latest_blocked_run"] == "20260624_123500"
    assert report["next_ticker"] == "159995"
    assert report["same_next_ticker_runs"] == 3


def test_price_readiness_stall_report_counts_partial_no_progress(tmp_path):
    readiness_payload = {
        "status": "partial_daily_price_backfill_needed",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 30,
        "missing_signal_ticker_count": 277,
        "latest_missing_tickers": ["002221", "600030"],
        "minimum_next_rows": 2,
        "missing_tickers_top10": [
            {"ticker": "002221", "signal_days": 2},
            {"ticker": "159990", "signal_days": 43},
        ],
    }
    for run_id in ("20260628_123812", "20260629_123708", "20260630_123538"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap4_min2obs",
        score=0.0,
        max_position=0.04,
        overfit_risk=False,
        warning="partial daily_prices stalled",
        failure_cases=[],
        price_readiness_stall=report,
    )

    capped, reason = apply_heuristic_risk_cap("002221", 0.04, 0.8, context=context)
    note = format_price_readiness_stall_note(context)

    assert report["status"] == "stalled_partial_daily_prices"
    assert report["stall_kind"] == "partial_daily_price_backfill_needed"
    assert report["consecutive_blocked_runs"] == 3
    assert report["priced_signal_ticker_count"] == 30
    assert report["missing_signal_ticker_count"] == 277
    assert report["next_ticker"] == "002221"
    assert report["unblock_tickers"] == ["002221", "600030"]
    assert report["same_unblock_batch_runs"] == 3
    assert capped == pytest.approx(0.002)
    assert "partial daily_prices覆盖无进展" in note
    assert "最小解锁批次=002221, 600030(2行)，同一解锁批次连续3轮" in note
    assert "数据补齐未推进仓位上限0.20%" in reason


def test_price_readiness_stall_report_dedupes_same_day_reruns(tmp_path):
    readiness_payload = {
        "status": "partial_daily_price_backfill_needed",
        "total_signal_ticker_count": 307,
        "priced_signal_ticker_count": 28,
        "missing_signal_ticker_count": 279,
        "latest_missing_tickers": ["159995", "600030"],
        "minimum_next_rows": 2,
        "missing_tickers_top10": [{"ticker": "159995", "signal_days": 1}],
    }
    for run_id in ("20260707_123540", "20260708_150542", "20260708_150718"):
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        (run_dir / "README.md").write_text("# run\n", encoding="utf-8")
        (run_dir / "price_readiness.json").write_text(
            json.dumps(readiness_payload),
            encoding="utf-8",
        )

    report = build_price_readiness_stall_report(tmp_path)

    assert report["status"] == "partial_daily_price_backfill_needed"
    assert report["consecutive_blocked_runs"] == 2
    assert report["blocked_run_ids"] == ["20260707_123540", "20260708_150718"]
    assert report["raw_lookback_runs"] == 3
    assert report["deduped_by_run_date"] is True
    assert report["unblock_tickers"] == ["159995", "600030"]


def test_heuristic_context_surfaces_price_readiness_stall(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.061,
        max_position=0.05,
        overfit_risk=False,
        warning="daily_prices缺失",
        failure_cases=[],
        price_readiness_stall={
            "status": "stalled_no_daily_prices",
            "consecutive_blocked_runs": 3,
            "minimum_blocked_runs": 3,
            "blocked_run_ids": ["20260622_123523", "20260623_123529", "20260624_123500"],
            "first_blocked_run": "20260622_123523",
            "latest_blocked_run": "20260624_123500",
            "next_ticker": "159995",
            "same_next_ticker_runs": 3,
            "next_action": "Backfill independently validated local daily_prices for 159995, then rerun the cycle.",
        },
    )

    capped, reason = apply_heuristic_risk_cap("159995", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)
    checklist = format_policy_checklist(context)
    note = format_price_readiness_stall_note(context)

    assert capped == pytest.approx(0.0025)
    assert "连续3/3轮daily_prices为0" in reason
    assert "数据补齐未推进仓位上限0.25%" in reason
    assert "daily_prices连续阻塞" in status
    assert "连续阻塞模拟买入上限=0.25%" in prompt
    assert "不得新增leaderboard分支" in prompt
    assert "daily_prices连续阻塞历史证据仓位<=0.25%" in checklist
    assert "下一步ticker=159995" in note


def test_heuristic_risk_cap_tightens_thin_tape_update(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        tape_update={
            "validation_status": "thin_tape_update",
            "new_prediction_rows_since_previous": 1,
            "current_latest_prediction_date": "2026-06-14",
            "latest_date_prediction_rows": 1,
            "latest_prediction_age_days": 1,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.01)
    assert "薄样本验证模拟买入上限1.0%" in reason
    assert "较上轮新增1行" in status
    assert "薄tape历史证据仓位<=1.0%" in prompt


def test_heuristic_risk_cap_tightens_zero_new_tape_update(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs_anomaly12",
        score=0.065,
        max_position=0.05,
        overfit_risk=False,
        warning="通过本轮基础样本外与成本扰动检查",
        failure_cases=[],
        tape_update={
            "validation_status": "thin_tape_update",
            "new_prediction_rows_since_previous": 0,
            "current_latest_prediction_date": "2026-06-14",
            "latest_date_prediction_rows": 1,
            "latest_prediction_age_days": 2,
        },
    )

    capped, reason = apply_heuristic_risk_cap("600519", 0.05, 0.8, context=context)
    status = format_heuristic_status(context)
    prompt = format_heuristic_prompt_context(context)

    assert capped == pytest.approx(0.005)
    assert "零新增样本" in reason
    assert "薄样本验证模拟买入上限0.5%" in reason
    assert "薄样本历史证据复用上限: 0.5%" in status
    assert "薄tape历史证据仓位<=0.5%" in prompt


def test_simulation_trade_losses_derive_risk_memory():
    failures = derive_simulation_risk_memory([
        {
            "id": 1,
            "ticker": "512880",
            "direction": "buy",
            "shares": 1000,
            "price": 1.0,
            "fee": 0.3,
            "traded_at": "2026-06-01T09:30:00",
        },
        {
            "id": 2,
            "ticker": "512880",
            "direction": "sell",
            "shares": 1000,
            "price": 0.95,
            "fee": 1.235,
            "traded_at": "2026-06-02T09:30:00",
        },
    ])

    assert len(failures) == 1
    assert failures[0]["ticker"] == "512880"
    assert failures[0]["last_loss_pct"] < -0.03
    assert failures[0]["expires_at"].startswith("2026-06-10")


@pytest.mark.asyncio
async def test_simulation_refreshes_closed_loss_risk_memory(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    conn = db._connection
    await conn.executemany(
        """
        INSERT INTO simulation_trades (ticker, direction, shares, price, fee, reason, traded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("512880", "buy", 1000, 1.0, 0.3, "entry", "2026-06-01T09:30:00"),
            ("512880", "sell", 1000, 0.95, 1.235, "exit", "2026-06-02T09:30:00"),
        ],
    )
    await conn.commit()

    failures = await sim.refresh_simulation_risk_memory()
    async with conn.execute("SELECT ticker, last_loss_pct FROM simulation_risk_memory") as cursor:
        rows = await cursor.fetchall()
    await db.close()

    assert failures[0]["ticker"] == "512880"
    assert rows[0][0] == "512880"
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.03,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        simulation_failures=failures,
    )
    capped, reason = apply_heuristic_risk_cap("512880", 0.08, 0.8, context=context)

    assert capped == 0.04
    assert "模拟账户近期已实现亏损风险记忆" in reason


def test_format_heuristic_status_includes_failure_cases(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="cost_robust_hold4",
        score=0.29,
        max_position=0.10,
        overfit_risk=True,
        warning="sample split weak",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-10..2026-05-15",
                "suspected_reason": "entry reversed quickly",
            }
        ],
    )

    status = format_heuristic_status(context)

    assert "cost_robust_hold4" in status
    assert "worst_trade" in status


def test_format_heuristic_status_prefers_injected_realtime_account_view(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="simulation_live_policy_v1",
        score=None,
        max_position=0.10,
        overfit_risk=False,
        warning="模拟账户实时估值不完整",
        failure_cases=[],
        simulation_performance={
            "net_total_return": None,
            "score": None,
            "health_status": "valuation_incomplete",
            "current_invested_ratio": None,
            "trade_count": 31,
        },
    )
    live_performance = {
        "net_total_return": -0.02775,
        "score": -0.02775,
        "health_status": "degraded_underdeployed",
        "current_invested_ratio": 0.272,
        "trade_count": 31,
        "latest_trade_at": "2026-07-31T10:08:29",
        "failure_reasons": ["模拟账户投入率27.2%低于80%健康线"],
    }

    status = format_heuristic_status(
        context,
        live_performance=live_performance,
    )

    assert "score: -0.027750" in status
    assert "系统健康: degraded_underdeployed" in status
    assert "投入率: 27.2%" in status
    assert "模拟账户投入率27.2%低于80%健康线" in status
    assert "实时估值不完整" not in status


def test_format_heuristic_prompt_context_marks_failure_tickers(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_cost_guard",
        score=0.067,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-27..2026-05-30",
                "market_state": {"ticker": "688256"},
                "suspected_reason": "entry reversed quickly",
            }
        ],
    )

    prompt = format_heuristic_prompt_context(context)

    assert "本地Heuristic风控约束" in prompt
    assert "688256" in prompt
    assert "不得编造成外部市场事实" in prompt
    assert "限制到4.0%或观望" in prompt


def test_heuristic_policy_checklist_surfaces_promoted_gates(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="single_stock_hold6_cap5_min2obs",
        score=0.05,
        max_position=0.05,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[],
        min_signal_count=2,
        min_confidence=0.66,
        min_risk_reward=0.9,
        min_holding_days=6,
        max_gross=0.2,
        universe="single_stock",
    )

    checklist = format_policy_checklist(context)
    prompt = format_heuristic_prompt_context(context)

    assert "置信度>=66%" in checklist
    assert "风险收益比>=0.90" in checklist
    assert "最短持有>=6天" in checklist
    assert "组合目标投资比例=100%" in checklist
    assert "禁止战略现金" in prompt
    assert "Heuristic入场校验" in prompt


def test_failure_ticker_constraints_explain_exact_cap(tmp_path):
    context = HeuristicRiskContext(
        run_dir=tmp_path,
        policy_name="no_lookahead_failure_half_size",
        score=0.08,
        max_position=0.08,
        overfit_risk=False,
        warning="split/cost passed",
        failure_cases=[
            {
                "case_type": "worst_trade",
                "time_range": "2026-05-27..2026-05-30",
                "market_state": {"ticker": "688256"},
                "suspected_reason": "entry reversed quickly",
            }
        ],
        failure_ticker_scale=0.5,
    )

    constraints = failure_ticker_constraints(context)

    assert constraints == [
        {
            "ticker": "688256",
            "max_simulated_position": 0.04,
            "action": "cap_to_failure_scale_and_require_new_evidence",
            "reason": "worst_trade",
        }
    ]


def test_run_discussion_appends_heuristic_context(monkeypatch):
    monkeypatch.setattr(
        "sovereign_hall.run_discussion.format_heuristic_prompt_context",
        lambda: "【本地Heuristic风控约束】failure tickers: 688256",
    )

    prompt = build_lessons_with_heuristic_context("【历史教训】控制换手")

    assert "【历史教训】控制换手" in prompt
    assert "failure tickers: 688256" in prompt


def test_interactive_research_extracts_general_investment_keywords():
    system = ResearchDiscussionSystem.__new__(ResearchDiscussionSystem)

    keywords = system._generate_search_keywords("选择一只三个月左右适合持有的矿业股票", AgentRole.CYCLE_ANALYST)

    assert "选择一只三个月左右适合持有的矿业股票" in keywords
    assert "持有期三个月" in keywords
    assert "股票" in keywords
    assert "周期" in keywords


@pytest.mark.asyncio
async def test_simulation_applies_heuristic_position_cap(monkeypatch):
    sim = InvestmentSimulation()
    sim.get_current_quote = AsyncMock(return_value={
        "price": 9.0,
        "source": "test_realtime_quote",
        "fetched_at": datetime.now().isoformat(),
    })
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.setattr(
        "sovereign_hall.services.investment_simulation.apply_heuristic_risk_cap",
        lambda ticker, target_position, confidence, **kwargs: (0.10, "heuristic cap"),
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.25,
        current_price=9.0,
        reason="committee",
        confidence=0.7,
        signal_count=1,
    )

    assert result["action"] == "buy"
    assert result["price"] == pytest.approx(9.0)
    assert sim.positions["600519"]["shares"] == 100


@pytest.mark.asyncio
async def test_simulation_daily_trade_limit_cannot_be_bypassed_by_direct_call(monkeypatch):
    sim = InvestmentSimulation()
    sim.max_daily_trades = MAX_DAILY_TRADES
    sim.count_trades_on_date = AsyncMock(return_value=MAX_DAILY_TRADES)
    sim.resolve_trade_price = AsyncMock(return_value=(10.0, "test_realtime_quote"))
    fake_market = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=True),
        },
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.10,
        current_price=999.0,
    )

    assert result["success"] is False
    assert result["action"] == "pending"
    assert "硬上限 5 笔" in result["reason"]
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_limit_persists_price_free_pending_decision(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.max_daily_trades = MAX_DAILY_TRADES
    sim.count_trades_on_date = AsyncMock(return_value=MAX_DAILY_TRADES)
    sim.resolve_trade_price = AsyncMock(return_value=(10.0, "should_not_be_used"))
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=999.0,
        reason="committee ruling",
        confidence=0.7,
    )

    row = await (await db._connection.execute(
        "SELECT ticker, direction, target_position, defer_code, status FROM simulation_pending_decisions"
    )).fetchone()
    columns = {item[1] for item in await (await db._connection.execute(
        "PRAGMA table_info(simulation_pending_decisions)"
    )).fetchall()}
    await db.close()

    assert result["action"] == "pending"
    assert tuple(row) == ("600519", "long", 0.1, "daily_trade_limit", "pending_next_trading_session")
    assert "price" not in columns
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_decision_refreshes_same_ticker_direction_without_duplicate(tmp_path):
    db = DatabaseService(str(tmp_path / "pending_dedupe.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()

    first_id = await sim.record_pending_decision(
        ticker="600519",
        direction="long",
        target_position=0.10,
        reason="first committee ruling",
        defer_code="market_closed",
    )
    second_id = await sim.record_pending_decision(
        ticker="600519",
        direction="long",
        target_position=0.08,
        reason="newer committee ruling",
        defer_code="non_trading_day",
    )
    rows = await (await db._connection.execute(
        "SELECT id, target_position, reason, defer_code FROM simulation_pending_decisions"
    )).fetchall()
    await db.close()

    assert second_id == first_id
    assert len(rows) == 1
    assert tuple(rows[0]) == (
        first_id,
        0.08,
        "newer committee ruling",
        "non_trading_day",
    )


@pytest.mark.asyncio
async def test_market_closed_persists_exit_without_filling(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "closed.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.resolve_trade_price = AsyncMock(return_value=(12.0, "should_not_be_used"))
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=False)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.execute_trade(
        ticker="600519",
        direction="sell",
        target_position=0.0,
        current_price=999.0,
        reason="stop loss",
    )
    pending = await sim.pending_decision_count()
    trade_count = (await (await db._connection.execute("SELECT COUNT(*) FROM simulation_trades")).fetchone())[0]
    await db.close()

    assert result["action"] == "pending"
    assert pending == 1
    assert trade_count == 0
    assert sim.positions["600519"]["shares"] == 100
    sim.resolve_trade_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_replay_waits_for_open_market_without_fetching_quote(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_closed.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    await sim.record_pending_decision(
        ticker="600519",
        direction="sell",
        target_position=0.0,
        reason="closed-market stop",
        defer_code="market_closed",
    )
    sim.execute_trade = AsyncMock()
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=False)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)

    result = await sim.replay_pending_decisions()
    row = await (await db._connection.execute(
        "SELECT status, replay_count FROM simulation_pending_decisions"
    )).fetchone()
    await db.close()

    assert result["status"] == "waiting_market_open"
    assert result["remaining"] == 1
    assert tuple(row) == ("pending_next_trading_session", 0)
    sim.execute_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_replay_is_exit_first_and_resolves_each_row_once(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_replay.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    buy_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="deferred buy", defer_code="daily_trade_limit",
    )
    sell_id = await sim.record_pending_decision(
        ticker="000001", direction="sell", target_position=0.0,
        reason="deferred exit", defer_code="market_closed",
    )
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.count_trades_on_date = AsyncMock(return_value=0)

    async def execute_with_atomic_pending_projection(**kwargs):
        if kwargs["ticker"] == "000001":
            now = datetime.now().isoformat()
            await db._connection.execute(
                """
                UPDATE simulation_pending_decisions
                SET status = 'executed', resolved_at = ?, updated_at = ?,
                    resolution = 'simulation_trade:test', defer_code = ''
                WHERE id = ?
                """,
                (now, now, kwargs["pending_decision_id"]),
            )
            await db._connection.commit()
            return {"success": True, "action": "sell", "ticker": "000001"}
        return {
            "success": False,
            "action": "hold",
            "ticker": "600519",
            "reason": "heuristic veto",
        }

    sim.execute_trade = AsyncMock(side_effect=execute_with_atomic_pending_projection)

    result = await sim.replay_pending_decisions()
    second = await sim.replay_pending_decisions()
    rows = await (await db._connection.execute(
        "SELECT id, status, replay_count FROM simulation_pending_decisions ORDER BY id"
    )).fetchall()
    await db.close()

    assert result["executed"] == 1
    assert result["rejected"] == 1
    assert result["remaining"] == 0
    assert second["status"] == "empty"
    assert [call.kwargs["ticker"] for call in sim.execute_trade.await_args_list] == ["000001", "600519"]
    assert [call.kwargs["pending_decision_id"] for call in sim.execute_trade.await_args_list] == [sell_id, buy_id]
    assert all(call.kwargs["current_price"] == 0.0 for call in sim.execute_trade.await_args_list)
    assert [tuple(row) for row in rows] == [(buy_id, "rejected", 1), (sell_id, "executed", 1)]


@pytest.mark.asyncio
async def test_pending_replay_daily_limit_reuses_row_without_duplicate(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_no_duplicate.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    pending_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="deferred buy", defer_code="market_closed",
    )
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    # Capacity exists when the queue is selected, then another caller consumes it.
    sim.count_trades_on_date = AsyncMock(side_effect=[0, MAX_DAILY_TRADES])

    result = await sim.replay_pending_decisions()
    rows = await (await db._connection.execute(
        "SELECT id, status, defer_code, replay_count FROM simulation_pending_decisions"
    )).fetchall()
    await db.close()

    assert result["attempted"] == 1
    assert result["remaining"] == 1
    assert [tuple(row) for row in rows] == [
        (pending_id, "pending_next_trading_session", "daily_trade_limit", 1)
    ]


@pytest.mark.asyncio
async def test_pending_replay_expires_stale_ruling_without_trade(tmp_path, monkeypatch):
    db = DatabaseService(str(tmp_path / "pending_expired.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    pending_id = await sim.record_pending_decision(
        ticker="600519", direction="long", target_position=0.1,
        reason="stale ruling", defer_code="market_closed",
    )
    await db._connection.execute(
        "UPDATE simulation_pending_decisions SET expires_at = ? WHERE id = ?",
        ((datetime.now() - timedelta(days=1)).isoformat(), pending_id),
    )
    await db._connection.commit()
    fake_market = type(
        "FakeMarket",
        (),
        {"is_trading_day": AsyncMock(return_value=True), "is_market_open": AsyncMock(return_value=True)},
    )()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    sim.execute_trade = AsyncMock()

    result = await sim.replay_pending_decisions()
    row = await (await db._connection.execute(
        "SELECT status, resolution FROM simulation_pending_decisions WHERE id = ?", (pending_id,)
    )).fetchone()
    await db.close()

    assert result["expired"] == 1
    assert tuple(row) == ("expired", "expired_without_open-session_replay")
    sim.execute_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_linked_exit_pending_ignores_legacy_expiry_and_stays_retryable(
    tmp_path,
    monkeypatch,
):
    db = DatabaseService(str(tmp_path / "pending_exit_legacy_expiry.db"))
    await db._init_db()
    sim = InvestmentSimulation(db)
    await sim.init_tables()
    intent_id = await sim.create_execution_intent(
        ticker="588860",
        direction="sell",
        target_position=0.0,
        reason="mandatory max-duration exit",
        idempotency_key="lifecycle:588860:exit:legacy-expiry",
    )
    pending_id = await sim.record_pending_decision(
        ticker="588860",
        direction="sell",
        target_position=0.0,
        reason="wait for fresh quote",
        defer_code="realtime_quote_unavailable",
        intent_id=intent_id,
    )
    # Simulate a pre-migration database row.  The defensive replay check must
    # preserve the durable exit even before startup reconciliation runs.
    await db._connection.executescript(
        """
        DROP TRIGGER simulation_pending_exit_expiry_guard_insert;
        DROP TRIGGER simulation_pending_exit_expiry_guard_update;
        """
    )
    await db._connection.execute(
        "UPDATE simulation_pending_decisions SET expires_at = ? WHERE id = ?",
        ((datetime.now() - timedelta(days=1)).isoformat(), pending_id),
    )
    await db._connection.commit()
    fake_market = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=True),
        },
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )
    sim.count_trades_on_date = AsyncMock(return_value=0)
    sim.execute_intent = AsyncMock(
        return_value={
            "success": False,
            "action": "error",
            "reason": "injected atomic failure",
        }
    )

    result = await sim.replay_pending_decisions()
    row = await (await db._connection.execute(
        "SELECT status, replay_count FROM simulation_pending_decisions WHERE id = ?",
        (pending_id,),
    )).fetchone()
    await db.close()

    assert result["attempted"] == 1
    assert result["expired"] == 0
    assert result["remaining"] == 1
    assert tuple(row) == ("pending_next_trading_session", 1)
    sim.execute_intent.assert_awaited_once_with(
        intent_id,
        pending_decision_id=pending_id,
    )


@pytest.mark.asyncio
async def test_market_data_realtime_execution_window():
    market = MarketDataService()
    market.is_trading_day = AsyncMock(return_value=True)
    try:
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T10:00:00")) is True
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T12:30:00")) is False
        assert await market.is_market_open(datetime.fromisoformat("2026-07-13T17:00:00")) is False
    finally:
        await market.close()


@pytest.mark.asyncio
async def test_simulation_passes_portfolio_gross_to_heuristic_cap(monkeypatch):
    sim = InvestmentSimulation()
    sim.cash = 8000.0
    sim.positions = {"000001": {"shares": 100, "avg_cost": 20.0}}
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    seen = {}
    quotes = {
        "600519": 4.0,
        "000001": 20.0,
    }

    async def realtime_quote(ticker):
        return {
            "price": quotes[ticker],
            "source": "test_realtime_quote",
            "fetched_at": datetime.now().isoformat(),
        }

    sim.get_current_quote = AsyncMock(side_effect=realtime_quote)

    def fake_cap(ticker, target_position, confidence, **kwargs):
        seen.update(kwargs)
        return 0.05, "gross cap"

    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.setattr(
        "sovereign_hall.services.investment_simulation.apply_heuristic_risk_cap",
        fake_cap,
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.25,
        current_price=4.0,
        reason="committee",
        confidence=0.7,
        signal_count=2,
    )

    assert result["action"] == "buy"
    assert seen["current_position"] == pytest.approx(0.0)
    assert seen["current_gross_exposure"] == pytest.approx(0.20)


@pytest.mark.asyncio
async def test_simulation_assets_are_na_when_realtime_quote_missing(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            ("p1", "600519.SH", 12.3, datetime.now().isoformat(), "long", 0.7),
    )
    await conn.commit()

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value=None)

    assets = await sim.calculate_assets(prices={"600519": 999.0})
    await db.close()

    assert assets["valuation_complete"] is False
    assert assets["total_assets"] is None
    assert assets["positions_value"] is None
    assert assets["missing_price_tickers"] == ["600519"]


@pytest.mark.asyncio
async def test_simulation_assets_use_realtime_quote_not_local_or_prediction(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 15.0, "2026-06-15T10:00:00", "long", 0.7),
    )
    await conn.execute(
        """
        INSERT INTO daily_prices (ticker, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("600519", "2026-07-06", 11.8, 12.4, 11.6, 12.0, 1000, "local_test"),
    )
    await conn.commit()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value={
        "price": 99.0,
        "source": "test_realtime_quote",
        "fetched_at": datetime.now().isoformat(),
    })

    assets = await sim.calculate_assets()
    await db.close()

    assert assets["valuation_complete"] is True
    assert assets["total_assets"] == pytest.approx(18900.0)
    assert assets["positions_value"] == pytest.approx(9900.0)
    sim.get_current_quote.assert_awaited_once_with("600519")


@pytest.mark.asyncio
async def test_simulation_assets_do_not_fallback_to_stale_local_price(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 15.0, "2026-07-06T10:00:00", "long", 0.7),
    )
    await conn.execute(
        """
        INSERT INTO daily_prices (ticker, date, open, high, low, close, volume, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("600519", "2026-06-18", 11.8, 12.4, 11.6, 12.0, 1000, "stale_local_test"),
    )
    await conn.commit()
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.positions = {"600519": {"shares": 100, "avg_cost": 10.0}}
    sim.get_current_quote = AsyncMock(return_value=None)

    assets = await sim.calculate_assets()
    await db.close()

    assert assets["valuation_complete"] is False
    assert assets["total_assets"] is None
    assert assets["positions_value"] is None


@pytest.mark.asyncio
async def test_simulation_trade_refuses_local_prediction_without_realtime_quote(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await ensure_prediction_tables(str(db_path))
    conn = db._connection
    await conn.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, current_price, predicted_at, direction, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("p1", "600519.SH", 12.3, datetime.now().isoformat(), "long", 0.7),
    )
    await conn.commit()
    fake_market = type("FakeMarket", (), {"is_trading_day": AsyncMock(return_value=True)})()
    monkeypatch.setattr("sovereign_hall.services.market_data.get_market_data", lambda: fake_market)
    monkeypatch.delenv("SOVEREIGN_HALL_REALTIME_QUOTES", raising=False)

    sim = InvestmentSimulation(db)
    sim.cash = 9000.0
    sim.get_current_quote = AsyncMock(return_value=None)

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.2,
        current_price=None,
        reason="prediction-only simulation",
        confidence=0.8,
        signal_count=2,
        risk_cap_already_applied=True,
    )
    await db.close()

    assert result["success"] is False
    assert result["action"] == "hold"
    assert "实时现价" in result["reason"]
    assert sim.positions == {}


@pytest.mark.asyncio
async def test_simulation_incomplete_portfolio_valuation_blocks_new_buy(monkeypatch):
    sim = InvestmentSimulation()
    sim.cash = 9_000.0
    sim.positions = {"000001": {"shares": 100, "avg_cost": 10.0}}
    sim.resolve_trade_price = AsyncMock(return_value=(20.0, "test_realtime_quote"))
    sim._estimate_trade_assets = AsyncMock(
        return_value=({}, 9_000.0, ["000001"])
    )
    fake_market = type(
        "FakeMarket",
        (),
        {
            "is_trading_day": AsyncMock(return_value=True),
            "is_market_open": AsyncMock(return_value=True),
        },
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )

    result = await sim.execute_trade(
        ticker="600519",
        direction="long",
        target_position=0.1,
        current_price=999.0,
        confidence=0.8,
        risk_cap_already_applied=True,
    )

    assert result["success"] is False
    assert result["action"] == "hold"
    assert "组合实时估值不完整" in result["reason"]
    assert "000001" in result["reason"]
    assert "600519" not in sim.positions
    sim.resolve_trade_price.assert_awaited_once_with("600519")


@pytest.mark.asyncio
async def test_prediction_tracker_waits_for_window(tmp_path):
    db_path = tmp_path / "test.db"
    tracker = PredictionTracker(str(db_path))
    await tracker.create_prediction(
        conclusion_id="",
        ticker="600519",
        current_price=10.0,
        target_price=11.0,
        stop_loss=9.5,
        direction="long",
        confidence=0.7,
        expected_days=30,
    )

    assert await tracker.validate_predictions() == 0


def test_backtest_singleton_returns_instance():
    assert get_backtest_engine() is not None


def test_expected_days_are_normalized():
    assert DecisionRecorder.normalize_expected_days(1) == 3
    assert DecisionRecorder.normalize_expected_days(365) == 180
    assert DecisionRecorder.normalize_expected_days(None, "短线事件驱动") == 14
    assert DecisionRecorder.normalize_expected_days(None, "半年产业趋势") == 120


@pytest.mark.asyncio
async def test_decision_records_dynamic_expected_days(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    decision_id = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT expected_days FROM price_predictions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row == (7,)


@pytest.mark.asyncio
async def test_recent_duplicate_decision_reuses_existing_id(tmp_path):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    first = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.7,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )
    second = await recorder.record_decision(
        ticker="600519",
        decision="long",
        confidence=0.72,
        target_price=0.1,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM price_predictions").fetchone()[0]
    conn.close()

    assert second == first
    assert count == 1


@pytest.mark.asyncio
async def test_prediction_schema_migrates_existing_table(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE price_predictions (id TEXT PRIMARY KEY, ticker TEXT NOT NULL)")
    conn.commit()
    conn.close()

    await ensure_prediction_tables(str(db_path))

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(price_predictions)")}
    daily_prices_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_prices'"
    ).fetchone()
    conn.close()

    assert {
        "entry_date",
        "discussion_context",
        "expected_days",
        "quote_source",
        "quote_fetched_at",
        "actual_return",
    }.issubset(columns)
    assert daily_prices_exists is not None


@pytest.mark.asyncio
async def test_learning_engine_generates_error_profiles(tmp_path):
    db_path = tmp_path / "test.db"
    await ensure_prediction_tables(str(db_path))
    conn = sqlite3.connect(db_path)
    rows = [
        ("p1", "600519", "long", 0.82, 30, "wrong", 0.0, "事实: 估值修复 审议深度: full; vote_margin=0.10"),
        ("p2", "000858", "long", 0.78, 30, "partial", 0.3, "事实: 消费修复 审议深度: focused; vote_margin=0.05"),
        ("p3", "512880", "short", 0.45, 7, "correct", 1.0, "事实: 交易拥挤"),
    ]
    for row in rows:
        conn.execute(
            """
            INSERT INTO price_predictions (
                id, ticker, direction, confidence, expected_days, status, result,
                accuracy_score, discussion_context, predicted_at, validated_at
            ) VALUES (?, ?, ?, ?, ?, 'validated', ?, ?, ?, datetime('now'), datetime('now'))
            """,
            row,
        )
    conn.commit()
    conn.close()

    engine = LearningEngine(str(db_path))
    profiles = await engine.analyze_error_profiles()
    prompt = await engine.generate_lessons_prompt()

    assert profiles
    assert profiles[0]["direction"] == "long"
    assert "错误画像" in prompt
    assert "600519" in prompt


@pytest.mark.asyncio
async def test_learning_engine_reinjects_same_topic_conclusions_and_prediction_results(tmp_path):
    db_path = tmp_path / "test.db"
    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.init_report_tables()
    await db.save_report_conclusion(
        "半导体景气验证",
        "旧结论：维持观望，等待库存改善。",
        ticker="512880",
        holding_period="30",
        confidence=0.6,
    )
    await db._connection.execute(
        """
        INSERT INTO price_predictions (
            id, ticker, direction, confidence, expected_days, status, result,
            accuracy_score, predicted_at, validated_at
        ) VALUES (
            'memory-p1', '512880', 'long', 0.6, 30, 'validated', 'wrong',
            0.0, '2026-06-01T10:00:00', '2026-07-01T10:00:00'
        )
        """
    )
    await db._connection.commit()

    engine = LearningEngine(str(db_path))
    memory = await engine.generate_research_memory_prompt("半导体景气验证")
    stats = await engine.get_accuracy_stats()

    assert "旧结论：维持观望" in memory
    assert "512880 long" in memory
    assert "30天" in memory
    assert "validated/wrong" in memory
    assert stats["total"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_learning_stats_exclude_legacy_validated_unknown(tmp_path):
    db_path = tmp_path / "test.db"
    await ensure_prediction_tables(str(db_path))
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """
        INSERT INTO price_predictions (
            id, ticker, direction, status, result, accuracy_score, predicted_at
        ) VALUES (?, ?, 'long', 'validated', ?, ?, datetime('now'))
        """,
        [
            ("known", "600519", "correct", 1.0),
            ("legacy-unknown", "000001", "unknown", 0.0),
        ],
    )
    conn.commit()
    conn.close()

    stats = await LearningEngine(str(db_path)).get_accuracy_stats()

    assert stats["total"] == 1
    assert stats["correct"] == 1
    assert stats["accuracy"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_stage2_empty_model_output_does_not_inject_canned_ticker():
    class EmptyProposalLLM:
        async def chat(self, **kwargs):
            return "[]"

    doc = Document(
        title="半导体行业库存跟踪",
        content="这是足够长的本地测试资料，用于确认模型没有输出提案时系统不会注入预设ETF。" * 3,
        url="https://example.com/evidence",
        source="unit",
    )

    proposals = await stage2_deep_research(
        EmptyProposalLLM(),
        [doc],
        "半导体景气验证",
    )

    assert proposals == []


def test_committee_votes_can_defer_to_hold():
    decision = aggregate_committee_decision(
        {"confidence": 0.8, "target_position": 0.2},
        ["【投票】观望 | 置信度: 70% | 仓位: 0%"] * 7,
    )

    assert decision["direction"] == "hold"
    assert decision["target_position"] == 0.0
    assert committee_decision_is_predictable(decision) is True


def test_deployment_deadlock_review_can_adopt_only_strong_quorate_direction():
    proposal = {
        "ticker": "600515",
        "direction": "long",
        "target_position": 0.08,
        "confidence": 0.7,
        "thesis": "事实: 订单增长且现金流改善；推断: 盈利质量提升",
        "evidence": ["公告订单数据", "公告现金流数据"],
    }
    original = aggregate_committee_decision(
        proposal,
        [
            '{"direction":"hold","confidence":0.4,"position":0}',
            '{"direction":"hold","confidence":0.4,"position":0}',
            '{"direction":"hold","confidence":0.4,"position":0}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )
    review = aggregate_committee_decision(
        proposal,
        [
            '{"direction":"long","confidence":0.72,"position":0.08}',
            '{"direction":"long","confidence":0.68,"position":0.06}',
            '{"direction":"long","confidence":0.70,"position":0.07}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert committee_deadlock_requires_review(
        original,
        proposal,
        "空仓资金部署候选证据比较",
    )
    merged = merge_committee_deadlock_review(original, review)

    assert merged["direction"] == "long"
    assert merged["deadlock_review"]["adopted"] is True
    assert merged["initial_committee_decision"]["direction"] == "hold"


def test_deployment_deadlock_review_preserves_hold_when_confidence_is_low():
    original = {
        "direction": "hold",
        "confidence": 0.4,
        "target_position": 0.0,
        "vote_summary": {"hold": 4.5},
    }
    weak_review = {
        "direction": "long",
        "confidence": 0.64,
        "target_position": 0.08,
        "direction_support": 1.0,
        "vote_quorum_met": True,
    }

    merged = merge_committee_deadlock_review(original, weak_review)

    assert merged["direction"] == "hold"
    assert merged["deadlock_review"]["adopted"] is False


def test_quorum_failure_hold_is_not_a_prediction():
    assert committee_decision_is_predictable({
        "direction": "hold",
        "vote_quorum_met": False,
    }) is False


@pytest.mark.asyncio
async def test_hold_prediction_records_quote_lineage_and_missed_upside(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    recorder = DecisionRecorder(str(db_path))
    decision_id = await recorder.record_decision(
        ticker="600519",
        decision="hold",
        confidence=0.7,
        target_price=0.15,
        stop_loss=0.05,
        entry_price=10.0,
        expected_days=7,
    )

    fake_market = type(
        "FakeMarket",
        (),
        {
            "get_current_price": AsyncMock(return_value=10.7),
            "get_ohlc": AsyncMock(return_value=[
                {
                    "date": "2026-07-25",
                    "open": 10.0,
                    "high": 10.6,
                    "low": 9.9,
                    "close": 10.5,
                },
            ]),
        },
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )

    result = await recorder.validate_single(decision_id)
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        """
        SELECT target_price, stop_loss, quote_source, quote_fetched_at,
               result, actual_hit_type, actual_return
        FROM price_predictions WHERE id = ?
        """,
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row[0] == pytest.approx(10.5)
    assert row[1] == pytest.approx(9.5)
    assert row[2] == "provided_prediction_anchor"
    assert row[3]
    assert row[4] == "wrong"
    assert row[5] == "missed_upside"
    assert row[6] == pytest.approx(0.05)
    assert result["actual_return"] == pytest.approx(0.05)


@pytest.mark.asyncio
async def test_prediction_fetches_and_persists_realtime_quote_metadata(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_market = type(
        "FakeMarket",
        (),
        {
            "get_current_quote": AsyncMock(return_value={
                "ticker": "600519",
                "price": 10.0,
                "source": "test_realtime_quote",
                "fetched_at": "2026-07-25T10:00:00",
            }),
        },
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )

    decision_id = await DecisionRecorder(str(db_path)).record_decision(
        ticker="600519",
        decision="hold",
        confidence=0.7,
        target_price=0.15,
        stop_loss=0.05,
        expected_days=7,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        """
        SELECT current_price, quote_source, quote_fetched_at
        FROM price_predictions WHERE id = ?
        """,
        (decision_id,),
    ).fetchone()
    conn.close()

    assert row == (10.0, "test_realtime_quote", "2026-07-25T10:00:00")
    fake_market.get_current_quote.assert_awaited_once_with("600519")


@pytest.mark.asyncio
async def test_committee_prediction_defers_quote_without_inventing_price(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "test.db"
    fake_market = type(
        "FakeMarket",
        (),
        {"get_current_quote": AsyncMock(return_value=None)},
    )()
    monkeypatch.setattr(
        "sovereign_hall.services.market_data.get_market_data",
        lambda: fake_market,
    )

    prediction_id = await DecisionRecorder(str(db_path)).record_decision(
        ticker="520500",
        decision="long",
        confidence=0.65,
        target_price=15.0,
        stop_loss=5.0,
        expected_days=30,
        round_id="round-closed-market",
        decision_id="decision-closed-market",
        defer_quote_until_execution=True,
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        """
        SELECT current_price, target_price, stop_loss, status, entry_date,
               quote_source, quote_fetched_at, round_id, decision_id
        FROM price_predictions WHERE id = ?
        """,
        (prediction_id,),
    ).fetchone()
    conn.close()

    assert row == (
        None,
        15.0,
        5.0,
        "awaiting_entry_quote",
        None,
        None,
        None,
        "round-closed-market",
        "decision-closed-market",
    )
    fake_market.get_current_quote.assert_not_awaited()


def test_hold_feedback_does_not_enter_offline_long_allocator():
    evaluator = load_script_module(
        "run_heuristic_cycle_hold_feedback_module",
        "scripts/run_heuristic_cycle.py",
    )
    frame = evaluator.pd.DataFrame([
        {
            "date": "2026-07-24",
            "ticker": "600519",
            "direction": "long",
            "current_price": 10.0,
            "target_price": 11.0,
            "stop_loss": 9.5,
            "confidence": 0.8,
            "expected_days": 30,
        },
        {
            "date": "2026-07-24",
            "ticker": "600519",
            "direction": "hold",
            "current_price": 10.0,
            "target_price": 10.5,
            "stop_loss": 9.5,
            "confidence": 0.1,
            "expected_days": 30,
        },
    ])

    daily = evaluator.build_daily_tape(frame)

    assert len(daily) == 1
    assert daily.iloc[0]["close_observations"] == 1
    assert daily.iloc[0]["confidence"] == pytest.approx(0.8)


def test_committee_vote_accepts_structured_json():
    vote = parse_committee_vote(
        '{"direction":"long","confidence":0.62,"position":0.08,'
        '"key_evidence":["订单增长"],"risk_flags":["估值偏高"],'
        '"invalid_if":"跌破支撑"}'
    )

    assert vote["direction"] == "long"
    assert vote["confidence"] == pytest.approx(0.62)
    assert vote["position"] == pytest.approx(0.08)
    assert vote["risk_flags"] == ["估值偏高"]
    assert vote["key_evidence"] == ["订单增长"]
    assert vote["invalid_if"] == "跌破支撑"


def test_committee_vote_accepts_auditable_abstention():
    vote = parse_committee_vote(
        '{"direction":"abstain","confidence":0.2,"position":0,'
        '"key_evidence":["超出消费分析能力圈"],'
        '"invalid_if":"取得独立消费需求证据"}'
    )

    assert vote["is_valid"] is True
    assert vote["direction"] == "abstain"
    assert vote["position"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_committee_task_timeouts_preserve_completed_results_and_audit_absence():
    async def completed_vote():
        await asyncio.sleep(0)
        return '{"direction":"long","confidence":0.8,"position":0.1}'

    async def slow_vote():
        await asyncio.sleep(0.05)
        return '{"direction":"hold","confidence":0.8,"position":0}'

    results, audit = await collect_committee_results(
        [
            ("CIO综合视角", completed_vote()),
            ("消费行业视角", slow_vote()),
        ],
        timeout_seconds=0.01,
        stage="round4_vote",
    )

    assert parse_committee_vote(results[0])["direction"] == "long"
    assert parse_committee_vote(results[0])["is_valid"] is True
    assert parse_committee_vote(results[1])["is_valid"] is False
    assert "[committee_task_absent]" in results[1]
    assert audit["completed_count"] == 1
    assert audit["timeout_count"] == 1
    assert audit["absent_labels"] == ["消费行业视角"]


@pytest.mark.asyncio
async def test_committee_analysis_uses_only_persisted_round_evidence():
    class FakeAgent:
        def __init__(self):
            self.calls = []

        async def think(self, **kwargs):
            self.calls.append(kwargs)
            return "durable evidence analysis"

        async def think_with_search(self, **_kwargs):
            raise AssertionError("committee must not launch untracked searches")

    agent = FakeAgent()
    result = await committee_think_from_persisted_evidence(
        agent,
        task="独立质疑估值与流动性",
        proposal={
            "ticker": "588170",
            "direction": "long",
            "confidence": 0.7,
            "target_position": 0.1,
            "thesis": "事实与推断分离",
            "evidence": ["来源A事实", "来源B反证"],
            "evidence_delta": "新增资金流证据",
            "reject_if": "流动性恶化",
        },
        discussion_context="只审计本轮资料",
        temperature=0.6,
        max_tokens=3000,
        round_id="round_test",
    )

    assert result == "durable evidence analysis"
    assert len(agent.calls) == 1
    call = agent.calls[0]
    assert call["use_memory"] is False
    assert call["context"] == "只审计本轮资料"
    assert "round_test" in call["task"]
    assert "来源A事实；来源B反证" in call["task"]
    assert "不得另发不可追溯搜索" in call["task"]


def test_committee_domain_weight_reduces_out_of_domain_hold_pressure():
    assert committee_role_weight(
        AgentRole.TMT_ANALYST, "半导体设备", "国产替代", 1.0
    ) == pytest.approx(1.0)
    assert committee_role_weight(
        AgentRole.CONSUMER_ANALYST, "半导体设备", "国产替代", 1.0
    ) == pytest.approx(0.25)
    assert committee_role_weight(
        AgentRole.RISK_OFFICER, "半导体设备", "国产替代", 1.5
    ) == pytest.approx(1.5)


def test_committee_abstentions_keep_quorum_without_counting_as_hold():
    decision = aggregate_committee_decision(
        {"confidence": 0.7, "target_position": 0.08},
        [
            '{"direction":"long","confidence":0.75,"position":0.08}',
            '{"direction":"long","confidence":0.70,"position":0.06}',
            '{"direction":"abstain","confidence":0.2,"position":0}',
            '{"direction":"abstain","confidence":0.2,"position":0}',
            '{"direction":"hold","confidence":0.4,"position":0}',
            '{"direction":"hold","confidence":0.5,"position":0}',
            '{"direction":"long","confidence":0.68,"position":0.05}',
        ],
        vote_weights=[2.0, 1.0, 0.25, 0.25, 1.0, 1.5, 1.0],
        vote_labels=["CIO", "TMT", "消费", "周期", "宏观", "风控", "量化"],
    )

    assert decision["direction"] == "long"
    assert decision["vote_summary"]["long"] == pytest.approx(4.0)
    assert decision["vote_summary"]["hold"] == pytest.approx(2.5)
    assert decision["vote_summary"]["abstain"] == pytest.approx(0.5)
    assert decision["parsed_vote_count"] == 7
    assert decision["directional_vote_count"] == 5
    assert decision["vote_quorum_met"] is True
    assert decision["individual_votes"][2]["role"] == "消费"
    assert decision["individual_votes"][2]["direction"] == "abstain"


def test_committee_hold_aggregation_preserves_evidence_work_queue():
    decision = aggregate_committee_decision(
        {"confidence": 0.7, "target_position": 0.1},
        [
            '{"direction":"hold","confidence":0.4,"position":0,'
            '"key_evidence":["缺少现金流与利润匹配数据"],'
            '"invalid_if":"经营现金流连续两期覆盖净利润"}',
            '{"direction":"hold","confidence":0.3,"position":0,'
            '"key_evidence":"缺少订单来源明细",'
            '"invalid_if":"订单明细可追溯且集中度下降"}',
            '{"direction":"long","confidence":0.8,"position":0.1,'
            '"key_evidence":["收入增长"],"invalid_if":"收入转负"}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert decision["direction"] == "hold"
    assert decision["evidence_gaps"] == [
        "缺少现金流与利润匹配数据",
        "缺少订单来源明细",
    ]
    assert decision["reconsider_if"] == [
        "经营现金流连续两期覆盖净利润",
        "订单明细可追溯且集中度下降",
    ]
    assert "收入增长" not in decision["evidence_gaps"]


def test_committee_aggregation_uses_custom_vote_weights():
    decision = aggregate_committee_decision(
        {"confidence": 0.5, "target_position": 0.1},
        [
            '{"direction":"hold","confidence":0.7,"position":0}',
            '{"direction":"short","confidence":0.6,"position":0.05}',
            '{"direction":"short","confidence":0.6,"position":0.05}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert decision["direction"] == "short"
    assert decision["vote_summary"]["hold"] == pytest.approx(2.0)
    assert decision["vote_margin"] > 0


def test_committee_aggregation_sizes_from_winning_direction_votes_only():
    decision = aggregate_committee_decision(
        {"confidence": 0.5, "target_position": 0.1},
        [
            '{"direction":"long","confidence":0.8,"position":0.10}',
            '{"direction":"long","confidence":0.7,"position":0.08}',
            '{"direction":"hold","confidence":0.99,"position":0}',
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert decision["direction"] == "long"
    assert decision["confidence"] == pytest.approx((0.8 * 2.0 + 0.7 * 1.5) / 3.5)
    assert decision["target_position"] == pytest.approx((0.10 * 2.0 + 0.08 * 1.5) / 3.5)
    assert decision["direction_support"] == pytest.approx(3.5 / 4.5, abs=1e-4)


def test_invalid_committee_votes_abstain_and_fail_quorum():
    decision = aggregate_committee_decision(
        {"confidence": 0.8, "target_position": 0.2},
        [
            '{"direction":"long","confidence":0.8,"position":0.1}',
            "投票失败: timeout",
            "无法解析该响应",
        ],
        vote_weights=[2.0, 1.5, 1.0],
    )

    assert decision["direction"] == "hold"
    assert decision["vote_summary"]["long"] == pytest.approx(2.0)
    assert decision["parsed_vote_count"] == 1
    assert decision["invalid_vote_count"] == 2
    assert decision["vote_quorum_required"] == 2
    assert decision["vote_quorum_met"] is False

    executable, rejected = preflight_committee_decisions(
        [{"ticker": "600519", **decision}],
        current_tickers=set(),
        normalize_ticker=lambda ticker: ticker,
    )
    assert executable == []
    assert rejected[0]["code"] == "committee_vote_quorum_failed"
    assert "parsed_votes=1/3" in rejected[0]["reason"]


def test_proposal_review_depth_tracks_priority():
    weak = {"ticker": "159995", "confidence": 0.42, "target_position": 0.03, "thesis": "推断: 主题轮动"}
    strong = {
        "ticker": "600519",
        "confidence": 0.76,
        "target_position": 0.18,
        "thesis": "事实: 业绩改善；证据: 财报；否决条件: 需求回落",
    }

    assert proposal_priority_score(strong) > proposal_priority_score(weak)
    assert choose_review_depth(weak) == "light"
    assert choose_review_depth(strong) == "full"


def test_topic_pool_resets_after_full_cycle_and_skips_recent(monkeypatch):
    monkeypatch.setattr("sovereign_hall.run_discussion.save_completed_topics", lambda topics: None)
    completed = set(TOPIC_POOL)
    topic = select_next_topic(completed, recent_topics={TOPIC_POOL[0]})

    assert completed == set()
    assert topic == TOPIC_POOL[1]


def test_topic_selection_falls_back_to_oldest_recent_when_pool_saturated(monkeypatch):
    monkeypatch.setattr("sovereign_hall.run_discussion.save_completed_topics", lambda topics: None)
    recent_topics = {
        topic: f"2026-05-27T{hour:02d}:00:00"
        for hour, topic in enumerate(TOPIC_POOL)
    }
    completed = set(TOPIC_POOL[1:])

    topic = select_next_topic(completed, recent_topics=recent_topics)

    assert topic == TOPIC_POOL[0]
    assert completed == set()


def test_empty_book_prioritizes_candidate_comparison_without_preset_ticker():
    base_topic = "中药配方颗粒集采"
    prioritized = prioritize_deployment_research(
        base_topic,
        {
            "valuation_complete": True,
            "total_assets": 9727.22,
            "invested_ratio": 0.0,
            "deployment_gap": 9727.22,
            "positions": {},
        },
        {
            "status": "blocked_no_approved_candidates",
            "blocker_code": "missing_approved_candidates",
        },
    )

    assert prioritized.startswith(base_topic)
    assert "空仓资金部署候选证据比较" in prioritized
    assert not re.search(r"\b[03615]\d{5}\b", prioritized)


def test_sync_wiki_index_batch_is_bounded_while_source_batch_is_preserved():
    source_documents = [{"id": index} for index in range(227)]

    sync_documents = bounded_sync_index_batch(source_documents, 30)

    assert len(source_documents) == 227
    assert len(sync_documents) == 30
    assert sync_documents == source_documents[:30]


def test_round_source_lineage_excludes_whole_documents_at_total_cap():
    documents = ["direct-a", "aggregate", "direct-b", "overlap"]
    selected, linked_ids, audit = bound_round_source_lineage(
        documents,
        [
            ["doc-a"],
            ["doc-b", "doc-c", "doc-d"],
            ["doc-e"],
            ["doc-a"],
        ],
        max_links=2,
    )

    assert selected == ["direct-a", "direct-b", "overlap"]
    assert linked_ids == ["doc-a", "doc-e"]
    assert audit == {
        "presented_document_count": 4,
        "resolved_document_count": 4,
        "traceable_document_count": 3,
        "untraceable_document_count": 0,
        "capacity_excluded_document_count": 1,
        "resolved_source_count_before_limit": 5,
        "lineage_limit": 2,
    }


def test_round_source_lineage_rejects_alignment_mismatch():
    with pytest.raises(ValueError, match="alignment mismatch"):
        bound_round_source_lineage(["one", "two"], [["doc-one"]], 3)


def test_materially_invested_book_keeps_normal_research_topic():
    base_topic = "AI算力产业链投资机会"

    prioritized = prioritize_deployment_research(
        base_topic,
        {
            "valuation_complete": True,
            "total_assets": 10_000.0,
            "invested_ratio": 0.85,
            "deployment_gap": 1_500.0,
            "positions": {"600519": {"shares": 100}},
        },
        {},
    )

    assert prioritized == base_topic


def test_actionable_residual_cash_queue_prioritizes_candidate_evidence_above_health_floor():
    base_topic = "地产政策效果评估"

    prioritized = prioritize_deployment_research(
        base_topic,
        {
            "valuation_complete": True,
            "total_assets": 9_718.45,
            "invested_ratio": 0.9376,
            "deployment_gap": 606.85,
            "positions": {"512800": {"shares": 1100}},
        },
        {
            "status": "blocked_no_approved_candidates",
            "blocker_code": "missing_approved_candidates",
        },
    )

    assert prioritized.startswith(base_topic)
    assert "待部署606.85元（已投入94%）资金部署候选证据比较" in prioritized
    assert not re.search(r"\b[03615]\d{5}\b", prioritized)


def test_completed_residual_cash_queue_does_not_force_candidate_research():
    base_topic = "家电以旧换新政策效果"

    prioritized = prioritize_deployment_research(
        base_topic,
        {
            "valuation_complete": True,
            "total_assets": 10_000.0,
            "invested_ratio": 0.99999,
            "deployment_gap": 0.01,
            "positions": {"600690": {"shares": 100}},
        },
        {
            "status": "completed",
            "blocker_code": "",
        },
    )

    assert prioritized == base_topic


def test_incomplete_realtime_valuation_does_not_claim_deployment_priority():
    base_topic = "家电以旧换新政策效果"

    prioritized = prioritize_deployment_research(
        base_topic,
        {
            "valuation_complete": False,
            "total_assets": None,
            "invested_ratio": None,
            "deployment_gap": None,
            "positions": {"600690": {"shares": 100}},
        },
        {"status": "blocked_valuation_incomplete"},
    )

    assert prioritized == base_topic


def test_persistence_preserves_token_breakdown(tmp_path, monkeypatch):
    stats_file = tmp_path / "session_stats.json"
    history_dir = tmp_path / "history"
    stats_file.write_text(json.dumps({
        "start_time": "2026-01-01T00:00:00",
        "total_rounds": 2,
        "total_time_seconds": 12.5,
        "topics_discussed": [],
        "proposals_generated": 0,
        "winning_proposals": 0,
        "token_stats": {
            "total_tokens": 100,
            "total_cost_usd": 0.2,
            "total_requests": 3,
            "prompt_tokens": 40,
            "completion_tokens": 50,
            "unattributed_tokens": 10,
        },
        "last_updated": "2026-01-01T00:00:00",
    }), encoding="utf-8")
    monkeypatch.setattr(persistence_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(persistence_module, "STATS_FILE", stats_file)
    monkeypatch.setattr(persistence_module, "HISTORY_DIR", history_dir)

    manager = PersistenceManager()
    loaded = manager.load_previous_stats()
    manager.add_time(7.5)

    saved = json.loads(stats_file.read_text(encoding="utf-8"))
    assert loaded["prompt_tokens"] == 40
    assert loaded["completion_tokens"] == 50
    assert loaded["unattributed_tokens"] == 10
    assert saved["total_time_seconds"] == 20.0


def test_agent_system_prompt_discourages_repetition_and_requires_evidence():
    prompt = get_persona(AgentRole.CIO).get_system_prompt()

    assert "不复述题目" in prompt
    assert "已验证事实" in prompt
    assert "证据不足" in prompt
    assert "不要为了节省token删减" in prompt


def test_core_discussion_prompts_are_evidence_rich_and_machine_readable():
    stage2_source = inspect.getsource(stage2_deep_research)
    stage3_source = inspect.getsource(stage3_ic_discussion)

    assert "只输出合法JSON" in stage2_source
    assert "证据不足时输出空数组" in stage2_source
    assert "max_tokens=8000" in stage2_source
    assert "build_structured_vote_prompt" in stage3_source
    assert "committee_think_from_persisted_evidence" in stage3_source
    assert ".think_with_search(" not in stage3_source
    assert "review_depth" in stage3_source
    assert "二次修正与反事实复盘" in stage3_source
    assert "vote_max_tokens" in stage3_source


def test_proposal_thesis_preserves_evidence_and_reject_conditions():
    thesis = build_proposal_thesis({
        "thesis": "事实: 订单增长；推断: 盈利弹性提升",
        "evidence": ["公告披露新订单", "行业价格回暖"],
        "reject_if": "订单取消或毛利率继续下滑",
    })

    assert "事实: 订单增长" in thesis
    assert "证据: 公告披露新订单；行业价格回暖" in thesis
    assert "否决条件: 订单取消或毛利率继续下滑" in thesis


@pytest.mark.asyncio
async def test_database_migrates_legacy_blacklist_schema(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE blacklist (ticker TEXT PRIMARY KEY, reason TEXT, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO blacklist (ticker, reason, created_at) VALUES (?, ?, ?)",
        ("600519", "legacy", "2026-01-01T00:00:00"),
    )
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.add_to_blacklist("600519", "again")
    await db.close()

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(blacklist)")}
    row = conn.execute(
        "SELECT failure_count, added_at FROM blacklist WHERE ticker = ?",
        ("600519",),
    ).fetchone()
    conn.close()

    assert {"failure_count", "added_at", "expires_at"}.issubset(columns)
    assert row[0] == 2
    assert row[1] is not None


@pytest.mark.asyncio
async def test_playbook_insert_supports_legacy_schema(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE playbook (
            entry_id TEXT PRIMARY KEY,
            category TEXT,
            situation TEXT,
            action_taken TEXT,
            outcome TEXT,
            lesson TEXT,
            confidence_delta REAL,
            ticker TEXT,
            refs TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db._init_db()
    await db.add_playbook_entry(PlaybookEntry(
        ticker="600519",
        situation="高估值回撤",
        lesson="等待确认信号",
        outcome="avoided_loss",
        confidence_delta=0.2,
        pattern="risk",
        action="hold",
        examples=["case-1"],
    ))
    await db.close()

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT category, action_taken, lesson, confidence_delta, ticker, refs FROM playbook"
    ).fetchone()
    conn.close()

    assert row[0] == "risk"
    assert row[1] == "hold"
    assert row[2] == "等待确认信号"
    assert row[3] == 0.2
    assert row[4] == "600519"
    assert json.loads(row[5]) == ["case-1"]


@pytest.mark.asyncio
async def test_report_conclusion_ids_are_backfilled_and_new_rows_get_id(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE report_conclusions (
            id INT,
            question TEXT,
            conclusion TEXT,
            ticker TEXT,
            position REAL,
            stop_loss REAL,
            take_profit REAL,
            holding_period TEXT,
            confidence REAL,
            key_points TEXT,
            risks TEXT,
            created_at TEXT,
            learned_at TEXT
        )
    """)
    conn.execute("INSERT INTO report_conclusions (question, conclusion) VALUES (?, ?)", ("q1", "c1"))
    conn.execute("INSERT INTO report_conclusions (id, question, conclusion) VALUES (?, ?, ?)", (10, "q2", "c2"))
    conn.commit()
    conn.close()

    db = DatabaseService(str(db_path))
    await db.init_report_tables()
    await db.save_report_conclusion("q3", "c3", ticker="600519")
    await db.close()

    conn = sqlite3.connect(db_path)
    null_ids, total, max_id = conn.execute(
        "SELECT SUM(id IS NULL), COUNT(*), MAX(id) FROM report_conclusions"
    ).fetchone()
    ids = [row[0] for row in conn.execute("SELECT id FROM report_conclusions ORDER BY id")]
    conn.close()

    assert null_ids == 0
    assert total == 3
    assert max_id == 12
    assert ids == [10, 11, 12]
