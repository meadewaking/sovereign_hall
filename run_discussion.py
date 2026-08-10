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
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, List, Dict, Optional
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
from sovereign_hall.utils import (
    format_cost_breakdown,
    format_token,
    format_token_breakdown,
)
from sovereign_hall.services.heuristic_policy import (
    apply_heuristic_risk_cap,
    format_heuristic_prompt_context,
    load_latest_heuristic_context,
    recent_prediction_observation_count,
)
from sovereign_hall.domain.common.ids import new_id

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


def _is_literal_empty_stage2_response(text: str) -> bool:
    """Return true only when the model answer itself is an empty JSON array."""
    stripped = str(text or "").strip()
    fenced = re.fullmatch(
        r"```(?:json)?\s*(\[\s*\])\s*```",
        stripped,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if fenced:
        stripped = fenced.group(1)
    return bool(re.fullmatch(r"\[\s*\]", stripped, flags=re.DOTALL))


def extract_stage2_proposal_array(text: str) -> tuple[List[Dict[str, Any]], str]:
    """Recover a proposal array embedded after verbose model reasoning.

    Some reasoning models ignore ``JSON only`` and prepend numbered analysis.
    The generic parser then greedily starts at an earlier ``[1]`` marker and
    discards an otherwise valid proposal array.  Scan JSON boundaries with the
    standard decoder and accept only dictionaries carrying a ticker; never
    synthesize a proposal from prose.
    """
    if not text or not isinstance(text, str):
        return [], "empty"

    saw_empty_array = False
    direct = _safe_parse_json(text, None)
    if isinstance(direct, list):
        proposals = [item for item in direct if isinstance(item, dict) and item.get("ticker")]
        if proposals:
            return proposals, "generic_parser"
        saw_empty_array = True
        if _is_literal_empty_stage2_response(text):
            return [], "explicit_empty"

    decoder = json.JSONDecoder()
    object_candidates: List[Dict[str, Any]] = []
    seen_objects = set()
    for index, char in enumerate(text):
        if char not in "[{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, list):
            proposals = [item for item in value if isinstance(item, dict) and item.get("ticker")]
            if proposals:
                return proposals, "embedded_array"
            if not value:
                saw_empty_array = True
        elif isinstance(value, dict) and value.get("ticker"):
            identity = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
            if identity not in seen_objects:
                seen_objects.add(identity)
                object_candidates.append(value)

    if object_candidates:
        return object_candidates, "embedded_objects"
    if saw_empty_array and re.search(
        r"(?<!\d)(?:[013568]\d{5})(?:\.(?:SH|SZ|BJ))?(?!\d)",
        text,
        flags=re.IGNORECASE,
    ):
        # A trailing ``[]`` must not erase an evidence-bearing candidate in the
        # model's own prose.  Mark it ambiguous so stage 2 gets one bounded,
        # evidence-preserving format repair pass.  The repair prompt may still
        # return [] and is forbidden from introducing a new ticker or fact.
        return [], "ambiguous_empty_with_candidate_text"
    return [], "unparsed"


STAGE2_TICKER_RE = re.compile(
    r"(?<!\d)(?:"
    r"(?:000|001|002|003|300|301)\d{3}|"
    r"(?:600|601|603|605|688)\d{3}|"
    r"(?:159|510|512|513|515|516|517|518|520|560|561|562|563|588)\d{3}"
    r")(?:\.(?:SH|SZ))?(?!\d)",
    flags=re.IGNORECASE,
)


def extract_stage2_candidate_windows(
    text: str,
    *,
    radius: int = 600,
    limit: int = 8,
) -> List[Dict[str, str]]:
    """Return auditable prose windows around tickers already present.

    These windows are diagnostic inputs for a bounded adjudication pass.  They
    are deliberately not converted into proposals: a ticker mention alone is
    not evidence and must never become a simulated-trade candidate.
    """
    source = str(text or "")
    ranked_windows: List[tuple[int, int, Dict[str, str]]] = []
    seen = set()
    for match in STAGE2_TICKER_RE.finditer(source):
        ticker = match.group(0).split(".", 1)[0].upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        start = max(0, match.start() - max(0, int(radius)))
        end = min(len(source), match.end() + max(0, int(radius)))
        excerpt = source[start:end].strip()
        nearby = source[
            max(0, match.start() - 100):min(len(source), match.end() + 100)
        ]
        priority = 0
        # Reasoning answers often enumerate high-price individual companies
        # before comparing an executable ETF.  A first-N cut silently dropped
        # those later ETF candidates even though the model and source material
        # both named them.  Ranking changes only which already-mentioned
        # windows receive the bounded audit; it never creates a ticker or
        # bypasses evidence, committee, quote, or execution gates.
        if re.search(r"ETF|交易型开放式指数基金|场内基金", nearby, re.IGNORECASE):
            priority += 8
        if re.search(
            r"\b(?:long|short)\b|多头|空头|做多|做空|推荐|投资提案",
            excerpt,
            re.IGNORECASE,
        ):
            priority += 4
        if re.search(r"\d+(?:\.\d+)?%|同比|环比|亿元|订单|现金流", excerpt):
            priority += 2
        ranked_windows.append((priority, match.start(), {
            "ticker": ticker,
            "excerpt": excerpt,
        }))
    ranked_windows.sort(key=lambda item: (-item[0], item[1], item[2]["ticker"]))
    return [
        item[2]
        for item in ranked_windows[:max(0, int(limit))]
    ]


def select_stage2_candidate_source_excerpts(
    doc_contents: List[str],
    candidate_tickers: List[str],
    *,
    limit: int = 12,
) -> tuple[List[str], Dict[str, int]]:
    """Select source excerpts with fair, auditable candidate coverage.

    The old global first-12 slice could be consumed by documents for the first
    company, leaving later candidates with no source text in the adjudication
    prompt.  This selector takes one matching source per candidate before a
    second pass.  It only returns existing excerpts that literally contain the
    ticker and reports coverage for durable stage diagnostics.
    """
    ordered_tickers = list(dict.fromkeys(
        str(ticker or "").split(".", 1)[0].upper()
        for ticker in candidate_tickers
        if str(ticker or "").strip()
    ))
    matches = {
        ticker: [item for item in doc_contents if ticker in item]
        for ticker in ordered_tickers
    }
    coverage = {ticker: len(items) for ticker, items in matches.items()}
    selected: List[str] = []
    selected_values = set()
    depth = 0
    bounded_limit = max(0, int(limit))
    while len(selected) < bounded_limit:
        added = False
        for ticker in ordered_tickers:
            ticker_matches = matches[ticker]
            if depth >= len(ticker_matches):
                continue
            item = ticker_matches[depth]
            if item not in selected_values:
                selected.append(item)
                selected_values.add(item)
                added = True
                if len(selected) >= bounded_limit:
                    break
        if not added:
            break
        depth += 1
    return selected, coverage


def format_stage2_diagnostic_context(rows: List[Dict[str, Any]]) -> str:
    """Turn recent stage-2 failures into falsifiable next-round memory."""
    if not rows:
        return ""
    lines = [
        "【最近阶段2结构化失败/恢复记忆】",
        "以下是管线审计记录，不是当前市场事实；只能在本轮新资料独立支持时重提标的。",
    ]
    for row in rows[:5]:
        tickers = row.get("detected_tickers") or []
        if isinstance(tickers, str):
            try:
                tickers = json.loads(tickers)
            except (TypeError, ValueError, json.JSONDecodeError):
                tickers = [tickers]
        repair_modes = row.get("repair_modes") or []
        if isinstance(repair_modes, str):
            try:
                repair_modes = json.loads(repair_modes)
            except (TypeError, ValueError, json.JSONDecodeError):
                repair_modes = [repair_modes]
        lines.append(
            f"- {row.get('created_at') or 'unknown'} status={row.get('status') or 'unknown'} "
            f"parse={row.get('parse_mode') or 'unknown'} "
            f"repair={','.join(str(item) for item in repair_modes) or 'none'} "
            f"tickers={','.join(str(item) for item in tickers) or 'none'}；"
            f"{str(row.get('reason') or '')[:300]}"
        )
    lines.append(
        "本轮若资料重新出现上述标的，必须输出合法JSON并保留证据/否决条件；"
        "若证据仍不足则保持[]，不得把这段审计记忆当作推荐。"
    )
    return "\n".join(lines)


def stage2_document_evidence_score(doc: Any) -> float:
    """Rank existing documents for concrete, auditable proposal extraction."""
    title = str(getattr(doc, "title", "") or "")
    content = str(getattr(doc, "content", "") or "")
    text = f"{title}\n{content}"
    score = min(len(content), 1200) / 1200.0
    ticker_count = len({match.group(0).split(".", 1)[0] for match in STAGE2_TICKER_RE.finditer(text)})
    score += min(ticker_count, 3) * 4.0
    if re.search(r"\d+(?:\.\d+)?%|同比|环比|亿元|订单|毛利率|净利润|现金流", text):
        score += 2.0
    if re.search(r"公告|财报|年报|季报|业绩预告|机构调研|中标|集采", text):
        score += 2.0
    if re.search(r"telegram|full animation|免费下载|钓鱼链接|配资", text, flags=re.IGNORECASE):
        score -= 8.0
    return score


def rank_stage2_documents(docs: list) -> list:
    """Stable evidence-first ordering; no document or candidate is invented."""
    return [
        doc
        for _, doc in sorted(
            enumerate(docs),
            key=lambda pair: (-stage2_document_evidence_score(pair[1]), pair[0]),
        )
    ]


def build_deployment_evidence_queries(docs: list, ticker_limit: int = 3) -> List[str]:
    """Derive follow-up queries only from ticker codes already present in sources."""
    ticker_scores: Dict[str, float] = {}
    ticker_order: Dict[str, int] = {}
    for doc_index, doc in enumerate(docs):
        text = (
            f"{getattr(doc, 'title', '') or ''}\n"
            f"{getattr(doc, 'content', '') or ''}"
        )
        for match in STAGE2_TICKER_RE.finditer(text):
            ticker = match.group(0).split(".", 1)[0]
            ticker_order.setdefault(ticker, doc_index)
            ticker_scores[ticker] = ticker_scores.get(ticker, 0.0) + stage2_document_evidence_score(doc)
    ranked = sorted(
        ticker_scores,
        key=lambda ticker: (-ticker_scores[ticker], ticker_order[ticker], ticker),
    )[:max(0, int(ticker_limit))]
    queries: List[str] = []
    for ticker in ranked:
        queries.extend([
            f"{ticker} 公告 财报 现金流",
            f"{ticker} 机构调研 订单 业绩",
        ])
    return queries


def merge_documents_prefer_richer(primary: list, additions: list) -> list:
    """Merge by URL/id and retain the longer fetched body for the same source."""
    merged = list(primary)
    key_to_index: Dict[str, int] = {}
    for index, doc in enumerate(merged):
        key = getattr(doc, "url", "") or getattr(doc, "id", "") or getattr(doc, "doc_id", "")
        if key:
            key_to_index[key] = index
    for doc in additions:
        key = getattr(doc, "url", "") or getattr(doc, "id", "") or getattr(doc, "doc_id", "")
        if not key:
            continue
        existing_index = key_to_index.get(key)
        if existing_index is None:
            key_to_index[key] = len(merged)
            merged.append(doc)
            continue
        existing_content = str(getattr(merged[existing_index], "content", "") or "")
        new_content = str(getattr(doc, "content", "") or "")
        if len(new_content) > len(existing_content):
            merged[existing_index] = doc
    return merged


def _numeric_stat(stats: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        if key in stats:
            value = stats.get(key, 0)
            if isinstance(value, str):
                value = value.replace("$", "").replace(",", "")
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _llm_stats_delta(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total_tokens": max(0, int(_numeric_stat(current, "total_tokens") - _numeric_stat(previous, "total_tokens"))),
        "prompt_tokens": max(0, int(_numeric_stat(current, "prompt_tokens") - _numeric_stat(previous, "prompt_tokens"))),
        "completion_tokens": max(0, int(_numeric_stat(current, "completion_tokens") - _numeric_stat(previous, "completion_tokens"))),
        "unattributed_tokens": max(0, int(_numeric_stat(current, "unattributed_tokens") - _numeric_stat(previous, "unattributed_tokens"))),
        "total_requests": max(0, int(_numeric_stat(current, "total_requests") - _numeric_stat(previous, "total_requests"))),
        "total_cost": max(0.0, _numeric_stat(current, "total_cost", "total_cost_usd") - _numeric_stat(previous, "total_cost", "total_cost_usd")),
        "input_cost_usd": max(0.0, _numeric_stat(current, "input_cost_usd", "input_cost") - _numeric_stat(previous, "input_cost_usd", "input_cost")),
        "output_cost_usd": max(0.0, _numeric_stat(current, "output_cost_usd", "output_cost") - _numeric_stat(previous, "output_cost_usd", "output_cost")),
    }


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


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Sovereign Hall continuous discussion runner")
    parser.add_argument("--once", action="store_true", help="只运行一轮后退出")
    parser.add_argument("--max-rounds", type=int, default=0, help="最多运行轮数，0 表示无限")
    parser.add_argument("--skip-preflight", action="store_true", help="跳过 LLM/Embedding/搜索联通性检查")
    parser.add_argument(
        "--local-only",
        action="store_true",
        default=False,
        help="仅使用本地资料并禁止网络搜索（可选）；省略时允许系统使用既有联网研究能力",
    )
    return parser.parse_args(argv)


def cli_args_can_run_without_instance_lock(argv: list[str] | None = None) -> bool:
    """Allow pure CLI help to work while a long discussion runner is active."""
    args = sys.argv[1:] if argv is None else argv
    return any(arg in {"-h", "--help"} for arg in args)


def kill_existing_run_discussion_instances() -> list[int]:
    """Stop any other ``python -m sovereign_hall.run_discussion`` processes.

    Returns the list of PIDs that were signalled. The current process and its
    ancestor chain are always excluded.  The latter matters when the runner is
    launched under ``screen``: its wrapper command contains ``run_discussion``
    too, but killing that wrapper tears down the new process before it can
    acquire the instance lock.  Unrelated old screen wrappers are still torn
    down via ``screen -X quit`` so their child Python is reaped too.
    """
    import subprocess

    own_pid = os.getpid()
    signalled: list[int] = []
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,args="],
            check=False,
            text=True,
            capture_output=True,
            timeout=10,
        )
    except Exception as exc:
        logger.warning("启动前进程扫描失败: %s", exc)
        return signalled

    processes: List[tuple[int, int, str]] = []
    parent_by_pid: Dict[int, int] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, ppid_str, args = line.split(None, 2)
            pid = int(pid_str)
            ppid = int(ppid_str)
        except ValueError:
            continue
        processes.append((pid, ppid, args))
        parent_by_pid[pid] = ppid

    protected_pids = {own_pid}
    cursor = own_pid
    while cursor in parent_by_pid:
        parent = parent_by_pid[cursor]
        if parent <= 0 or parent in protected_pids:
            break
        protected_pids.add(parent)
        cursor = parent

    screen_sessions: set[str] = set()
    for pid, _ppid, args in processes:
        if pid in protected_pids:
            continue
        if "run_discussion" not in args:
            continue
        tokens = args.split()
        first_token = tokens[0] if tokens else ""
        if first_token.endswith("grep") or first_token.endswith("/ps"):
            continue
        is_real_runner = any(
            token == "run_discussion.py"
            or token.endswith("/run_discussion.py")
            or token == "sovereign_hall.run_discussion"
            for token in tokens
        )
        if not is_real_runner:
            continue
        match = re.search(r"SCREEN\s+-\S*\s*-?\S*\s+(\S+)", args)
        if match:
            screen_sessions.add(match.group(1))
        try:
            os.kill(pid, 15)
            signalled.append(pid)
            print(f"   🛑 已向旧 run_discussion 进程发送 SIGTERM: pid={pid}")
        except ProcessLookupError:
            continue
        except PermissionError:
            print(f"   ⚠️ 无权限停止 pid={pid}（可能属于其他用户）")

    for session in screen_sessions:
        try:
            subprocess.run(
                ["screen", "-S", session, "-X", "quit"],
                check=False,
                timeout=5,
            )
            print(f"   🛑 已关闭 screen 会话: {session}")
        except Exception as exc:
            logger.debug("screen 会话 %s 关闭失败: %s", session, exc)

    if signalled:
        for _ in range(10):
            time.sleep(0.3)
            alive = []
            for pid in signalled:
                try:
                    os.kill(pid, 0)
                    alive.append(pid)
                except ProcessLookupError:
                    continue
                except PermissionError:
                    continue
            if not alive:
                break
            for pid in alive:
                try:
                    os.kill(pid, 9)
                    print(f"   💀 旧进程未退出，已 SIGKILL: pid={pid}")
                except ProcessLookupError:
                    continue
                except PermissionError:
                    continue

    return signalled


async def run_startup_preflight(llm, spiders, *, check_search: bool = True) -> bool:
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
            spiders.aggressive_search(["A股 最新消息"], max_results_per_query=1),
            timeout=90,
        )
        if not docs:
            raise RuntimeError("搜索返回空结果")

    configured_checks = [
        ("LLM", _check_llm, True),
        ("Embedding", _check_embedding, True),
    ]
    if check_search:
        configured_checks.append(("搜索", _check_search, False))
    else:
        print("   🚫 搜索: local-only 硬门禁用资料联网")

    for name, check, required in configured_checks:
        try:
            await check()
            checks.append((name, True, "OK", required))
            print(f"   ✅ {name}: OK")
        except Exception as exc:
            detail = str(exc)[:300] or exc.__class__.__name__
            checks.append((name, False, detail, required))
            status = "❌" if required else "⚠️"
            print(f"   {status} {name}: {detail}")

    failed = [item for item in checks if not item[1] and item[3]]
    if failed:
        print("\n❌ 联通性检查未通过，本次不启动 run_discussion。")
        for name, _, detail, _ in failed:
            print(f"   - {name}: {detail}")
        return False

    optional_failed = [item for item in checks if not item[1] and not item[3]]
    if optional_failed:
        print("\n⚠️ 搜索联通性暂时不可用，将依赖本地知识库并在后续轮次继续重试。")

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
        # sqlite3.Connection.__exit__ commits/rolls back but does not close the
        # connection.  This path runs once per production round, so relying on
        # ``with sqlite3.connect(...)`` leaked one database handle per round in
        # the long-running process.
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
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


def prioritize_deployment_research(
    topic: str,
    assets: Optional[Dict[str, Any]] = None,
    redeployment_state: Optional[Dict[str, Any]] = None,
) -> str:
    """Turn a sector topic into a candidate-comparison task when cash is stranded.

    This changes only the research objective.  It never inserts a ticker,
    relaxes the evidence/committee/realtime-price gates, or treats operational
    cash as a risk position.
    """
    assets = assets or {}
    if not assets.get("valuation_complete"):
        return topic

    total_assets = float(assets.get("total_assets") or 0.0)
    deployment_gap = float(assets.get("deployment_gap") or 0.0)
    invested_ratio = float(assets.get("invested_ratio") or 0.0)
    positions = assets.get("positions") or {}
    if total_assets <= 0:
        return topic

    material_gap = max(1.0, total_assets * 0.20)
    if deployment_gap < material_gap or invested_ratio >= 0.80:
        return topic

    state = redeployment_state or {}
    status = str(state.get("status") or "")
    if status == "blocked_valuation_incomplete":
        return topic

    book_state = "空仓" if not positions else f"低投入{invested_ratio:.0%}"
    return f"{topic}｜{book_state}资金部署候选证据比较"


def bounded_sync_index_batch(documents: List[Any], limit: int) -> List[Any]:
    """Bound synchronous wiki indexing without dropping SQLite persistence.

    The complete search result is persisted to ``documents`` first.  The wiki
    can lazily migrate the rest from SQLite on later searches, so indexing an
    evidence-sized batch here keeps the research-to-committee path responsive.
    """
    bounded_limit = max(int(limit or 0), 0)
    return list(documents or [])[:bounded_limit] if bounded_limit else []


def dedupe_proposals(proposals: List[Dict]) -> List[Dict]:
    """同一轮内按标的和方向去重，保留置信度最高的提案。"""
    from sovereign_hall.services.market_data import MarketDataService

    by_key = {}
    for proposal in proposals:
        ticker = str(proposal.get("ticker", "")).strip().upper()
        direction = str(proposal.get("direction", "long")).strip().lower()
        if not MarketDataService.is_supported_ticker(ticker):
            continue
        key = (ticker, direction)
        previous = by_key.get(key)
        if previous is None or float(proposal.get("confidence", 0)) > float(previous.get("confidence", 0)):
            by_key[key] = proposal | {"ticker": ticker, "direction": direction}
    return list(by_key.values())


def filter_repeated_rejection_proposals(
    proposals: List[Dict],
    rejection_memory: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
    rejection_threshold: int = 3,
    cooldown_days: int = 3,
) -> tuple[List[Dict], List[Dict[str, str]]]:
    """Hard-stop unchanged, repeatedly rejected research candidates.

    Prompt reminders alone did not stop the same ETF from being reconsidered
    dozens of times.  A candidate under cooldown may re-enter only with an
    explicit rejection point, a new local evidence delta, and traceable evidence
    labels.  This is a research/committee gate, not a trading blacklist.
    """
    now_value = now or datetime.now()
    memory_by_ticker: Dict[str, Dict[str, Any]] = {}
    for row in rejection_memory:
        if row.get("code") not in {"committee_hold", "heuristic_entry_veto"}:
            continue
        if not row.get("feedback_usable", True):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        previous = memory_by_ticker.get(ticker)
        if previous is None or int(row.get("rejection_count") or 0) > int(
            previous.get("rejection_count") or 0
        ):
            memory_by_ticker[ticker] = row

    eligible: List[Dict] = []
    suppressed: List[Dict[str, str]] = []
    for proposal in proposals:
        ticker = str(proposal.get("ticker") or "").strip().upper()
        memory = memory_by_ticker.get(ticker)
        if not memory or int(memory.get("rejection_count") or 0) < rejection_threshold:
            eligible.append(proposal)
            continue
        try:
            last_seen = datetime.fromisoformat(str(memory.get("last_seen_at") or ""))
            age_days = max(0, (now_value.date() - last_seen.date()).days)
        except (TypeError, ValueError):
            age_days = cooldown_days
        if age_days >= cooldown_days:
            eligible.append(proposal)
            continue

        resolved_rejection = str(proposal.get("resolved_rejection") or "").strip()
        evidence_delta = str(proposal.get("evidence_delta") or "").strip()
        evidence = proposal.get("evidence") or []
        has_traceable_delta = (
            len(resolved_rejection) >= 4
            and len(evidence_delta) >= 12
            and isinstance(evidence, list)
            and any(str(item).strip() for item in evidence)
        )
        if has_traceable_delta:
            eligible.append(proposal)
            continue

        suppressed.append({
            "code": "repeated_candidate_cooldown",
            "ticker": ticker,
            "reason": (
                f"近{cooldown_days}天已有{int(memory.get('rejection_count') or 0)}次证据型拒绝；"
                "本提案未同时给出resolved_rejection、evidence_delta和可追溯evidence，"
                "不进入投委会，避免无变化重复消耗"
            ),
        })
    return eligible, suppressed


def build_lessons_with_heuristic_context(
    lessons_prompt: str = "",
    redeployment_context: str = "",
) -> str:
    """Append local heuristic and simulated-redeployment memory to prompts."""
    parts = []
    if lessons_prompt and lessons_prompt.strip():
        parts.append(lessons_prompt.strip())
    heuristic_prompt = format_heuristic_prompt_context()
    if heuristic_prompt:
        parts.append(heuristic_prompt)
    if redeployment_context and redeployment_context.strip():
        parts.append(redeployment_context.strip())
    return "\n\n".join(parts)


async def record_proposal_lot_screening_event(
    round_coordinator,
    round_id: str | None,
    rejections: List[Dict[str, Any]],
) -> None:
    """Attach fresh quote lineage for proposal lot rejections to the round.

    This is a research-screening audit event, not an execution quote snapshot.
    Missing/stale quotes never enter ``rejections`` and therefore cannot create
    a durable price record or eliminate a committee candidate.
    """
    if not round_coordinator or not round_id or not rejections:
        return
    quote_rejections = []
    for item in rejections:
        quote_rejections.append({
            "ticker": str(item.get("ticker") or ""),
            "code": str(item.get("code") or "proposal_lot_infeasible"),
            "price": float(item.get("reference_price") or 0.0),
            "provider": str(item.get("quote_source") or ""),
            "fetched_at": str(item.get("quote_fetched_at") or ""),
            "purpose": str(
                item.get("quote_purpose")
                or "proposal_lot_feasibility_screening"
            ),
            "max_executable_quote": float(
                item.get("max_executable_quote") or 0.0
            ),
        })
    await round_coordinator.record_event(
        round_id,
        "ProposalLotFeasibilityRejected",
        {
            "rejection_count": len(quote_rejections),
            "rejections": quote_rejections,
        },
    )


# ============================================================================
# 阶段1：海量信息搜索（高并发）
# ============================================================================
async def stage1_mass_search(llm, spiders, topic: str, query_count: int = 30) -> list:
    """阶段1：海量信息搜索"""
    from sovereign_hall.core.config import get_config

    research_config = get_config().get("research", {})
    max_results_per_query = int(research_config.get("search_results_per_query", 10) or 10)
    deployment_deep_fetch_max_docs = int(
        research_config.get("deployment_deep_fetch_max_docs", 6) or 6
    )

    logger.info(f"========== 阶段1：海量信息搜索 - 议题: {topic} ==========")
    print("\n" + "="*60)
    print(f"📡 阶段1：海量信息搜索 - 议题: {topic}")
    print("="*60)

    # 清理议题关键词
    topic_keyword = (
        topic.split("｜", 1)[0]
        .replace("分析", "")
        .replace("研究", "")
        .replace("投资机会", "")
        .replace("行业", "")
        .strip()
    )
    deployment_research = "资金部署候选证据比较" in topic

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
    if deployment_research:
        extra_queries = [
            f"{topic_keyword} 集采结果 中标企业",
            f"{topic_keyword} 上市公司 营收 毛利率",
            f"{topic_keyword} 龙头 财报 现金流",
            f"{topic_keyword} 候选公司 估值对比",
            f"{topic_keyword} 场内ETF 基金代码 规模 成交额",
            f"{topic_keyword} ETF 跟踪指数 流动性 费率",
            f"{topic_keyword} 机构调研 订单份额",
            f"{topic_keyword} 业绩催化 验证时间",
            f"{topic_keyword} 风险 失效条件",
            f"{topic_keyword} 资金流向",
            *extra_queries,
        ]

    # 生成搜索词
    from sovereign_hall.services.spider_service import SearchQueryGenerator

    query_gen = SearchQueryGenerator(llm)
    queries = await query_gen.generate_queries(
        count=query_count,
        seeds=seeds,
        topic=topic_keyword,
    )

    print(f"\n生成 {len(queries)} 个搜索词")
    print(f"示例: {queries[:5]}")

    # 合并额外查询词并去重（保序）
    seen = set()
    all_queries = []
    # Deployment recovery queries carry required company/ETF evidence fields
    # and must not be crowded out when the query generator already returns the
    # full count.  Other research keeps the generated-query-first order.
    query_candidates = (
        extra_queries + queries if deployment_research else queries + extra_queries
    )
    for q in query_candidates:
        key = str(q).strip().lower()
        if key and key not in seen:
            seen.add(key)
            all_queries.append(q)
    all_queries = all_queries[:query_count]

    raw_docs = await spiders.aggressive_search(
        all_queries,
        max_results_per_query=max_results_per_query,
    )
    if deployment_research and raw_docs:
        evidence_queries = build_deployment_evidence_queries(raw_docs)
        if evidence_queries:
            logger.info(
                "阶段1：从已抓取资料中的明确代码派生 %s 个证据补强查询（无预设ticker）",
                len(evidence_queries),
            )
            evidence_docs = await spiders.aggressive_search(
                evidence_queries,
                max_results_per_query=max_results_per_query,
            )
            raw_docs = merge_documents_prefer_richer(raw_docs, evidence_docs)
            deep_candidates = [
                doc
                for doc in rank_stage2_documents(evidence_docs)
                if str(getattr(doc, "url", "") or "").startswith(("http://", "https://"))
            ][:max(0, deployment_deep_fetch_max_docs)]
            if deep_candidates:
                deep_docs = await spiders.parallel_fetch(
                    [getattr(doc, "url", "") for doc in deep_candidates],
                    extract_full_text=True,
                    max_concurrent=3,
                )
                raw_docs = merge_documents_prefer_richer(raw_docs, deep_docs)
                logger.info(
                    "阶段1：证据补强全文抓取 %s/%s 篇；失败保留原始摘要，不伪造正文",
                    len(deep_docs),
                    len(deep_candidates),
                )

    print(f"\n抓取 {len(raw_docs)} 篇文档")
    return raw_docs


# ============================================================================
# 阶段2：深度研报生成
# ============================================================================
async def stage2_deep_research(
    llm,
    docs: list,
    topic: str,
    db_service=None,
    lessons_prompt: str = "",
    round_id: str | None = None,
) -> list:
    """阶段2：从文档中提取投资提案"""
    from sovereign_hall.core.config import get_config

    research_config = get_config().get("research", {})
    stage2_max_docs = int(research_config.get("stage2_max_docs", 30) or 30)
    stage2_doc_chars = int(research_config.get("stage2_doc_chars", 1200) or 1200)
    stage2_context_chars = int(research_config.get("stage2_context_chars", 24000) or 24000)
    diagnostic_repair_modes: List[str] = []
    detected_candidate_windows: List[Dict[str, str]] = []
    candidate_source_coverage: Dict[str, int] = {}

    async def record_stage2_diagnostic(
        status: str,
        *,
        parse_mode: str = "",
        raw_excerpt: str = "",
        reason: str = "",
    ) -> None:
        if not db_service or not hasattr(db_service, "record_research_stage_diagnostic"):
            return
        try:
            await db_service.record_research_stage_diagnostic(
                topic=topic,
                stage="stage2",
                status=status,
                parse_mode=parse_mode,
                repair_modes=diagnostic_repair_modes,
                detected_tickers=[
                    item["ticker"] for item in detected_candidate_windows
                ],
                raw_excerpt=raw_excerpt,
                reason=reason,
                source="run_discussion.stage2_deep_research",
                round_id=round_id,
            )
        except Exception as diagnostic_error:
            logger.warning("[diag] stage2 diagnostic persistence failed: %s", diagnostic_error)

    if not docs:
        print("\n⚠️ 没有文档，跳过深度研究")
        logger.warning("阶段2：没有文档，跳过深度研究")
        await record_stage2_diagnostic(
            "empty_no_documents",
            reason="阶段1没有返回任何文档；没有候选可进入投委会",
        )
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

    logger.info(f"[diag] stage2 valid_docs={len(valid_docs)}/{len(docs)}")
    if not valid_docs:
        # 打印第一个 doc 的属性，帮助诊断
        sample = docs[0] if docs else None
        if sample is not None:
            attrs = {k: type(getattr(sample, k, None)).__name__ for k in ('content', 'title', 'url', 'doc_id')}
            logger.warning(f"[diag] stage2 docs have no content. sample attrs={attrs}")
            logger.warning(f"[diag] sample doc repr: {repr(sample)[:300]}")
        await record_stage2_diagnostic(
            "empty_no_valid_documents",
            reason=f"阶段1返回{len(docs)}篇文档，但正文超过50字符的有效文档为0",
        )
        return []

    AgentCls = _get_agent()

    # 构建文档摘要。联网返回顺序受查询完成顺序影响；先按“明确代码 +
    # 可核查经营数据”排序，避免有限上下文被泛行业摘要或垃圾页占满。
    valid_docs = rank_stage2_documents(valid_docs)
    doc_contents = []
    for doc in valid_docs[:stage2_max_docs]:
        content = getattr(doc, 'content', '') or ''
        title = getattr(doc, 'title', '') or ''
        url = getattr(doc, 'url', '') or ''
        if len(content) > 50:
            doc_contents.append(f"【{title}】\n{content[:stage2_doc_chars]}\n来源: {url}")

    content_text = "\n\n".join(doc_contents)
    logger.info(f"[diag] stage2 content_text len={len(content_text)}, doc_contents={len(doc_contents)}")

    # 一次性生成多个提案
    prompt = f"""
作为资深行业投资分析师，基于以下新闻/研报资料，提取3-5个具体的投资提案。

研究议题：{topic}
{blacklist_prompt}
{lessons_prompt}

资料：
{content_text[:stage2_context_chars]}

筛选规则：
1. 只推荐资料中有明确新增证据支持的标的；证据不足时宁可少输出
2. 不要重复同一行业逻辑，不要为了凑数输出相似提案
3. 必须把“已验证事实”和“推断”分开写入thesis
4. ETF是空仓部署的一等候选，不是低置信度替代品；若资料能验证其跟踪指数、
   规模、流动性、折溢价和行业暴露，可按与个股相同的证据门给出confidence
5. 若上文“当前整手可执行边界”存在，多头候选必须优先满足该参考价边界；
   明显无法买入一手的个股不得进入投委会，应比较资料中已有代码的可执行ETF或低价候选
6. 黑名单标的一律排除

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
        "resolved_rejection": "若该标的近期被拒绝，逐字指出本次消除的拒绝点；否则留空",
        "evidence_delta": "新增本地资料标题/文档ID，以及它如何消除上述拒绝点；否则留空",
        "reject_if": "若出现什么情况应否决该提案"
    }}
]

重要：必须排除黑名单中的标的！
重要：holding_period 必须根据投资逻辑动态决定，范围3-180天，不要一律填30。
重要：同一标的近期重复被拒绝时，缺少resolved_rejection、evidence_delta和evidence任一项都不得重提。
重要：无法从资料确定具体标的时必须少输出或输出空数组，不得用预设ticker、模板ETF或常识猜测补位。
"""

    try:
        logger.info(f"[diag] stage2 LLM call begin")
        response = await asyncio.wait_for(
            llm.chat(
                system="你是严谨的投资提案抽取器。只输出合法JSON；不编造资料中没有的事实；证据不足时输出空数组。",
                user=prompt,
                temperature=0.3,
                max_tokens=8000
            ),
            timeout=600
        )
        logger.info(f"[diag] stage2 LLM response len={len(response or '')}, first 300: {(response or '')[:300]}")

        # 解析JSON
        proposals, parse_mode = extract_stage2_proposal_array(response)
        detected_candidate_windows = extract_stage2_candidate_windows(response)
        if (
            not proposals
            and str(response or "").strip()
            and (
                parse_mode != "explicit_empty"
                or bool(detected_candidate_windows)
            )
        ):
            # Some reasoning models put a long analysis in ``reasoning_content``
            # but omit the requested final JSON.  The LLM client necessarily
            # returns that reasoning when normal content is empty.  Give the
            # model one bounded format-repair pass over its own answer; never
            # supply a fallback ticker or ask it to create new evidence.
            logger.warning(
                "[diag] stage2 primary response was %s; requesting evidence-preserving JSON repair",
                parse_mode,
            )
            repair_prompt = f"""
把下面“原始回答”中已经明确提出、且带有具体六位A股/ETF代码和证据的投资提案转换为合法JSON数组。

硬约束：
1. 只能转换原始回答已经明确提出的标的、方向、论点和证据；不得新增ticker、ETF或事实。
2. 原始回答只有分析过程、举例、候选名单但没有明确提案，或证据不足时，输出 []。
3. 每项必须包含 ticker、direction、target_position、stop_loss、take_profit、
   holding_period、holding_period_reason、confidence、thesis、sector、evidence、
   resolved_rejection、evidence_delta、reject_if。
4. 只输出JSON数组，不要Markdown、解释或思考过程。

原始回答：
{str(response)[:20000]}
"""
            try:
                repaired_response = await asyncio.wait_for(
                    llm.chat(
                        system=(
                            "你是格式修复器，只能结构化已有提案，不能生成新提案或补造证据。"
                            "没有明确且有证据的提案时只输出[]。"
                        ),
                        user=repair_prompt,
                        temperature=0.0,
                        max_tokens=5000,
                        use_cache=False,
                    ),
                    timeout=300,
                )
                repaired, repair_mode = extract_stage2_proposal_array(repaired_response)
                diagnostic_repair_modes.append(f"format:{repair_mode}")
                primary_tickers = {
                    item["ticker"] for item in detected_candidate_windows
                }
                repaired = [
                    item
                    for item in repaired
                    if str(item.get("ticker") or "").split(".", 1)[0].upper()
                    in primary_tickers
                ]
                logger.info(
                    "[diag] stage2 repair response len=%s, parsed=%s, mode=%s",
                    len(repaired_response or ""),
                    len(repaired),
                    repair_mode,
                )
                if repaired:
                    proposals = repaired
                    parse_mode = f"repair:{repair_mode}"
            except asyncio.TimeoutError:
                logger.warning("[diag] stage2 JSON repair timed out")
            except Exception as repair_error:
                logger.warning("[diag] stage2 JSON repair failed: %s", repair_error)

        if not proposals and detected_candidate_windows:
            # A second, narrowly scoped pass adjudicates only ticker mentions
            # already present in the primary answer against the original
            # source excerpts.  It cannot introduce a ticker, source or fact.
            candidate_ticker_order = [
                item["ticker"] for item in detected_candidate_windows
            ]
            candidate_tickers = set(candidate_ticker_order)
            source_excerpts, candidate_source_coverage = (
                select_stage2_candidate_source_excerpts(
                    doc_contents,
                    candidate_ticker_order,
                    limit=12,
                )
            )
            adjudication_prompt = f"""
审计下面的“候选窗口”，判断它们是否已经被“原始资料”支持为明确、可证伪的投资提案。

允许的ticker（只可从此集合选择）：
{json.dumps(sorted(candidate_tickers), ensure_ascii=False)}

硬约束：
1. 不得新增ticker、ETF、来源或事实；不允许仅凭候选名单/举例生成提案。
2. 每个保留项必须在原始资料中找到至少两条具体证据，并给出明确long或short方向。
3. 证据不足、只有行业常识、无法区分事实与推断时必须舍弃该项。
4. 每项必须包含 ticker、direction、target_position、stop_loss、take_profit、
   holding_period、holding_period_reason、confidence、thesis、sector、evidence、
   resolved_rejection、evidence_delta、reject_if。
5. 只输出合法JSON数组；没有合格项输出[]。

候选窗口：
{json.dumps(detected_candidate_windows, ensure_ascii=False)[:12000]}

原始资料摘录：
{chr(10).join(source_excerpts)[:14000] if source_excerpts else "没有包含候选ticker的原始资料，因此必须输出[]"}
"""
            try:
                adjudicated_response = await asyncio.wait_for(
                    llm.chat(
                        system=(
                            "你是证据审计器。只允许结构化原回答和原资料共同支持的提案；"
                            "不得补造候选或事实。"
                        ),
                        user=adjudication_prompt,
                        temperature=0.0,
                        max_tokens=5000,
                        use_cache=False,
                    ),
                    timeout=300,
                )
                adjudicated, adjudication_mode = extract_stage2_proposal_array(
                    adjudicated_response
                )
                diagnostic_repair_modes.append(
                    f"candidate_adjudication:{adjudication_mode}"
                )
                adjudicated = [
                    item
                    for item in adjudicated
                    if str(item.get("ticker") or "").split(".", 1)[0].upper()
                    in candidate_tickers
                ]
                logger.info(
                    "[diag] stage2 candidate adjudication len=%s, parsed=%s, mode=%s",
                    len(adjudicated_response or ""),
                    len(adjudicated),
                    adjudication_mode,
                )
                if adjudicated:
                    proposals = adjudicated
                    parse_mode = f"candidate_adjudication:{adjudication_mode}"
            except asyncio.TimeoutError:
                diagnostic_repair_modes.append("candidate_adjudication:timeout")
                logger.warning("[diag] stage2 candidate adjudication timed out")
            except Exception as adjudication_error:
                diagnostic_repair_modes.append(
                    f"candidate_adjudication:error:{type(adjudication_error).__name__}"
                )
                logger.warning(
                    "[diag] stage2 candidate adjudication failed: %s",
                    adjudication_error,
                )
        logger.info(
            "[diag] stage2 parsed type=%s, len=%s, mode=%s",
            type(proposals).__name__,
            len(proposals),
            parse_mode,
        )

        # 清洗数据（同时过滤黑名单）
        cleaned = []
        from sovereign_hall.services.market_data import MarketDataService

        cleaning_rejections: Dict[str, int] = {}

        def reject_cleaning(code: str) -> None:
            cleaning_rejections[code] = cleaning_rejections.get(code, 0) + 1

        for p in proposals:
            if not isinstance(p, dict):
                reject_cleaning("not_object")
                continue
            ticker = str(p.get('ticker', '')).strip().upper()
            direction = str(p.get("direction") or "").strip().lower()
            evidence = [
                str(item)[:240]
                for item in (p.get("evidence") or [])[:8]
                if str(item).strip()
            ]
            thesis = build_proposal_thesis(p)
            if direction not in {"long", "short"}:
                reject_cleaning("missing_explicit_direction")
                continue
            if not evidence or not str(thesis).strip():
                reject_cleaning("insufficient_structured_evidence")
                continue
            if MarketDataService.is_supported_ticker(ticker):
                # 过滤黑名单中的标的
                if blacklist and ticker in blacklist:
                    logger.warning(f"Filtered blacklisted ticker: {ticker}")
                    reject_cleaning("blacklist")
                    continue
                try:
                    cleaned_proposal = {
                        'ticker': ticker,
                        'direction': direction,
                        'target_position': float(p.get('target_position', 0.1)),
                        'stop_loss': float(p.get('stop_loss', 5.0)),
                        'take_profit': float(p.get('take_profit', 15.0)),
                        'confidence': float(p.get('confidence', 0.6)),
                        'thesis': thesis,
                        'sector': p.get('sector', '未知'),
                        'holding_period_reason': str(p.get('holding_period_reason') or '')[:200],
                        'evidence': evidence,
                        'resolved_rejection': str(p.get('resolved_rejection') or '')[:300],
                        'evidence_delta': str(p.get('evidence_delta') or '')[:500],
                        'reject_if': str(p.get('reject_if') or '')[:500],
                    }
                except (TypeError, ValueError):
                    reject_cleaning("invalid_numeric_field")
                    continue
                cleaned_proposal['holding_period'] = normalize_proposal_holding_period(cleaned_proposal | {'holding_period': p.get('holding_period')}, topic)
                cleaned.append(cleaned_proposal)
            else:
                reject_cleaning("unsupported_ticker")

        logger.info(f"[diag] stage2 cleaned={len(cleaned)} (after blacklist filter)")
        if not cleaned:
            logger.warning(f"[diag] stage2 produced 0 proposals. Raw response (first 500): {(response or '')[:500]}")
        for p in cleaned:
            print(f"      {p['ticker']} | {p['direction']} | {p['holding_period']}天 | 置信度: {p['confidence']:.0%} | {p['thesis'][:30]}")

        if not cleaned:
            print("   ⚠️ 本轮没有得到有证据支持的提案；不注入预设标的")

        await record_stage2_diagnostic(
            (
                "proposals_recovered"
                if cleaned and diagnostic_repair_modes
                else ("proposals_ready" if cleaned else "empty_after_adjudication")
            ),
            parse_mode=parse_mode,
            raw_excerpt=str(response or "")[:8000],
            reason=(
                f"cleaned={len(cleaned)}; "
                f"cleaning_rejections={json.dumps(cleaning_rejections, ensure_ascii=False)}; "
                "candidate_source_coverage="
                f"{json.dumps(candidate_source_coverage, ensure_ascii=False, sort_keys=True)}"
            ),
        )
        return cleaned

    except asyncio.TimeoutError:
        print(f"   ⏰ 超时")
        await record_stage2_diagnostic(
            "timeout",
            reason="阶段2主LLM调用超过600秒",
        )
        return []
    except Exception as e:
        print(f"   ❌ 错误: {str(e)[:80]}")
        await record_stage2_diagnostic(
            "error",
            reason=f"{type(e).__name__}: {str(e)[:1000]}",
        )
        return []


def parse_committee_vote(text: str) -> Dict:
    """Parse a loose committee vote into a small structured signal."""
    parsed_json = _safe_parse_json(str(text or ""), None)
    if isinstance(parsed_json, dict):
        raw_direction = (
            parsed_json.get("direction")
            or parsed_json.get("vote")
            or parsed_json.get("action")
            or parsed_json.get("decision")
        )
        direction_text = str(raw_direction or "").strip().lower()
        is_valid = any(
            word in direction_text
            for word in (
                "long", "buy", "买入", "看多", "做多",
                "short", "sell", "卖出", "看空", "做空",
                "hold", "defer", "neutral", "观望", "暂缓", "不建议", "反对", "拒绝",
                "abstain", "弃权", "超出能力圈",
            )
        )
        direction = normalize_vote_direction(
            raw_direction
        )
        confidence = parse_ratio_value(parsed_json.get("confidence"))
        position_value = parsed_json.get("position")
        if position_value in (None, ""):
            position_value = parsed_json.get("target_position")
        position = parse_ratio_value(position_value)
        risk_flags = parsed_json.get("risk_flags") or parsed_json.get("risks") or []
        if isinstance(risk_flags, str):
            risk_flags = [risk_flags]
        key_evidence = parsed_json.get("key_evidence") or parsed_json.get("evidence") or []
        if isinstance(key_evidence, str):
            key_evidence = [key_evidence]
        return {
            "direction": direction,
            "confidence": confidence,
            "position": position,
            "risk_flags": [str(flag)[:80] for flag in risk_flags[:5]] if isinstance(risk_flags, list) else [],
            "invalid_if": str(parsed_json.get("invalid_if") or parsed_json.get("reject_if") or "")[:240],
            "key_evidence": (
                [str(item)[:240] for item in key_evidence[:5] if str(item).strip()]
                if isinstance(key_evidence, list)
                else []
            ),
            "is_valid": is_valid,
            "parse_mode": "structured_json" if is_valid else "invalid_json_direction",
        }

    value = str(text or "").lower()
    if not value:
        return {
            "direction": "hold",
            "confidence": None,
            "position": None,
            "risk_flags": [],
            "invalid_if": "",
            "key_evidence": [],
            "is_valid": False,
            "parse_mode": "empty",
        }

    has_abstain = any(word in value for word in ("abstain", "弃权", "超出能力圈"))
    has_sell = any(word in value for word in ("卖出", "看空", "做空", "short", "sell"))
    has_hold = any(word in value for word in ("观望", "暂缓", "不建议", "反对", "拒绝", "hold", "defer"))
    has_buy = any(word in value for word in ("买入", "看多", "做多", "long", "buy"))

    if has_abstain:
        direction = "abstain"
    elif has_sell:
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

    return {
        "direction": direction,
        "confidence": confidence,
        "position": position,
        "risk_flags": [],
        "invalid_if": "",
        "key_evidence": [],
        "is_valid": bool(has_abstain or has_sell or has_hold or has_buy),
        "parse_mode": (
            "natural_language"
            if (has_abstain or has_sell or has_hold or has_buy)
            else "unparsed"
        ),
    }


def normalize_vote_direction(value: Any) -> str:
    """Normalize structured or natural-language vote direction."""
    text = str(value or "").strip().lower()
    if any(word in text for word in ("abstain", "弃权", "超出能力圈")):
        return "abstain"
    if any(word in text for word in ("short", "sell", "卖出", "看空", "做空")):
        return "short"
    if any(word in text for word in ("long", "buy", "买入", "看多", "做多")):
        return "long"
    return "hold"


def committee_role_weight(
    role: Any,
    sector: str,
    topic: str,
    base_weight: float,
) -> float:
    """Reduce out-of-domain analyst votes without silencing their critique."""
    role_value = str(getattr(role, "value", role) or "").lower()
    if role_value not in {"tmt_analyst", "consumer_analyst", "cycle_analyst"}:
        return float(base_weight)

    context = f"{sector or ''} {topic or ''}".lower()
    domain_keywords = {
        "tmt_analyst": (
            "ai", "tmt", "科技", "半导体", "芯片", "软件", "云计算",
            "计算机", "通信", "电子", "互联网", "智能化", "机器人",
        ),
        "consumer_analyst": (
            "消费", "食品", "饮料", "白酒", "乳制品", "餐饮", "家电",
            "免税", "零售", "医药", "医疗", "器械", "服务", "旅游",
        ),
        "cycle_analyst": (
            "周期", "有色", "金属", "化工", "煤炭", "钢铁", "地产",
            "工程机械", "汽车", "电池", "新能源", "光伏", "制造", "材料",
        ),
    }
    matched_roles = {
        domain_role
        for domain_role, keywords in domain_keywords.items()
        if any(keyword in context for keyword in keywords)
    }
    if role_value in matched_roles:
        return float(base_weight)
    if matched_roles:
        return round(float(base_weight) * 0.25, 4)
    return round(float(base_weight) * 0.50, 4)


COMMITTEE_DECISION_BOUNDARIES = """
【委员会口径硬约束】
- daily_prices、历史prediction价格、artifact价格只用于历史评估；当前模拟成交会在执行时重新取实时行情。因此历史日线覆盖不足不能单独构成当前买入/卖出的否决理由。
- 历史同类预测胜率只作为置信度先验，不能单独把有当前可追溯证据、可证伪逻辑、明确止损和期限的提案否决为HOLD。
- 当议题标记“空仓资金部署”时，当前组合没有行业相关性或已有科技仓位；不得虚构存量暴露作为否决理由。
- 必须区分“事实反证”与“尚未取得数据”。前者可投HOLD/SHORT；后者写入补证队列，但不能伪称事实已经证伪。
- 非本角色能力圈且没有独立新增证据时投abstain，不得用HOLD代替弃权。abstain参与出席法定人数，但不计入方向票。
- 风险控制优先使用标的筛选、分散、仓位上限、止损止盈和期限，不得把长期现金当默认风险资产。
""".strip()


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


def build_balanced_vote_context(
    proposal: Dict[str, Any],
    round1_items: List[tuple[str, str]],
    debate_items: List[tuple[str, str]],
    revision_items: List[tuple[str, str]],
    max_chars: int,
) -> str:
    """Keep every deliberation stage visible inside the bounded vote context."""
    total_budget = max(1200, int(max_chars or 0))
    thesis = str(proposal.get("thesis") or "")
    proposal_text = (
        f"【原提案】{proposal.get('ticker', '')} {proposal.get('direction', '')} | "
        f"置信度={float(proposal.get('confidence') or 0.0):.0%} | "
        f"目标仓位={float(proposal.get('target_position') or 0.0):.1%}\n"
        f"{thesis}"
    )
    proposal_budget = min(max(400, total_budget // 8), 1000)
    remaining = max(600, total_budget - proposal_budget - 12)
    stage_specs = (
        ("第一轮独立分析", round1_items, 0.40),
        ("第二轮交叉质疑", debate_items, 0.30),
        ("第三轮反事实修正", revision_items, 0.30),
    )
    sections = [proposal_text[:proposal_budget]]
    for label, items, share in stage_specs:
        if not items:
            continue
        stage_budget = max(200, int(remaining * share))
        label_cost = len(label) + 4
        item_label_cost = sum(len(str(name)) + 4 for name, _ in items)
        per_item = max(
            1,
            (stage_budget - label_cost - item_label_cost - len(items)) // len(items),
        )
        rendered = [
            f"[{name}] {str(result or '')[:per_item]}"
            for name, result in items
        ]
        sections.append((f"【{label}】\n" + "\n".join(rendered))[:stage_budget])
    return "\n\n".join(sections)[:total_budget]


def build_structured_vote_prompt(ticker: str, role_view: str, context: str, learned_context: str) -> str:
    """Ask each committee role for a machine-readable vote."""
    return f"""
基于以下讨论，对 {ticker} 从{role_view}给出最终投票。

讨论摘要：
{context}
{learned_context}

只输出JSON对象，不要Markdown，不要解释。字段：
{{
  "direction": "long/short/hold/abstain",
  "confidence": 0.0,
  "position": 0.0,
  "key_evidence": ["最关键证据1", "最关键证据2"],
  "risk_flags": ["主要风险1", "主要风险2"],
  "invalid_if": "什么情况会推翻该判断"
}}

约束：
- 证据不足或反证更强时 direction 必须是 hold，position 必须是 0。
- 若该标的超出本角色能力圈且没有独立新增证据，direction 必须是 abstain，position 必须是 0；不得用hold代替弃权。
- 模拟组合目标投资比例是100%，hold不是默认的“无风险”选项；必须同时衡量继续闲置资金的机会成本。
- 若已有可追溯事实、可证伪逻辑、明确止损/期限且预期收益风险比不低于0.8，应在风险预算内投long/short，而不是因存在一般性风险自动hold。
- 若投hold，key_evidence必须写清仍缺少的决定性证据或尚未消除的反证，invalid_if必须写下一次可转为long/short的具体条件。
- confidence 用0到1小数，position 用0到1小数。
{COMMITTEE_DECISION_BOUNDARIES}
""".strip()


def build_persisted_committee_evidence_context(
    proposal: Dict[str, Any],
    *,
    round_id: str | None = None,
) -> str:
    """Render only the evidence already persisted by the research stage.

    Committee analysis must challenge the durable research record. Launching
    fresh per-role searches here both loses round lineage and makes the role
    deadline include a shared Spider queue.
    """

    def render_items(value: Any) -> str:
        if isinstance(value, str):
            parsed = _safe_parse_json(value, default=None)
            if isinstance(parsed, list):
                value = parsed
        if isinstance(value, (list, tuple)):
            items = [str(item).strip() for item in value if str(item).strip()]
            return "；".join(items) if items else "未提供"
        return str(value or "").strip() or "未提供"

    return (
        "【本轮持久化提案证据（只能审计、交叉质疑，不得补造事实）】\n"
        f"- round_id: {round_id or proposal.get('round_id') or 'N/A'}\n"
        f"- ticker: {proposal.get('ticker') or 'N/A'}\n"
        f"- direction/confidence/target: "
        f"{proposal.get('direction') or 'N/A'} / "
        f"{proposal.get('confidence') if proposal.get('confidence') is not None else 'N/A'} / "
        f"{proposal.get('target_position') if proposal.get('target_position') is not None else 'N/A'}\n"
        f"- thesis: {render_items(proposal.get('thesis'))}\n"
        f"- evidence: {render_items(proposal.get('evidence'))}\n"
        f"- evidence_delta: {render_items(proposal.get('evidence_delta'))}\n"
        f"- resolved_rejection: {render_items(proposal.get('resolved_rejection'))}\n"
        f"- reject_if: {render_items(proposal.get('reject_if'))}\n"
        "资料已由本轮主研究阶段联网取得并按 round_id 持久化。"
        "此处不得另发不可追溯搜索；证据不足必须明确指出缺口并保持HOLD/弃权。"
    )


class CommitteeDecisionPersistenceError(RuntimeError):
    """A completed vote could not be durably linked to its research round."""


async def committee_think_from_persisted_evidence(
    agent: Any,
    *,
    task: str,
    proposal: Dict[str, Any],
    discussion_context: str,
    temperature: float,
    max_tokens: int,
    round_id: str | None = None,
) -> str:
    """Run one independent committee analysis on durable round evidence."""
    evidence_context = build_persisted_committee_evidence_context(
        proposal,
        round_id=round_id,
    )
    return await agent.think(
        task=f"{task}\n\n{evidence_context}",
        context=discussion_context,
        temperature=temperature,
        max_tokens=max_tokens,
        use_memory=False,
    )


async def collect_committee_results(
    tasks: List[tuple[str, Awaitable[str]]],
    *,
    timeout_seconds: float,
    stage: str,
) -> tuple[List[str], Dict[str, Any]]:
    """Collect concurrent committee work with an independent task deadline.

    Completed results survive a slow peer. Timed-out/error roles become
    explicit, non-directional absences and therefore cannot count as HOLD.
    """
    timeout = max(0.001, float(timeout_seconds))

    async def run_one(label: str, awaitable: Awaitable[str]) -> tuple[str, Dict[str, Any]]:
        started = asyncio.get_running_loop().time()
        status = "completed"
        error = ""
        try:
            value = await asyncio.wait_for(awaitable, timeout=timeout)
            result = str(value or "")
        except asyncio.TimeoutError:
            status = "timeout"
            error = f"timeout_after_{timeout:g}s"
            result = (
                f"[committee_task_absent] stage={stage} role={label} "
                f"reason=timeout timeout_seconds={timeout:g}"
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            status = "error"
            error = f"{type(exc).__name__}: {str(exc)[:160]}"
            result = (
                f"[committee_task_absent] stage={stage} role={label} "
                f"reason=error error_type={type(exc).__name__}"
            )
        elapsed = asyncio.get_running_loop().time() - started
        return result, {
            "label": str(label),
            "status": status,
            "elapsed_seconds": round(elapsed, 3),
            "error": error,
        }

    collected = await asyncio.gather(*[
        run_one(label, awaitable) for label, awaitable in tasks
    ])
    results = [item[0] for item in collected]
    task_audit = [item[1] for item in collected]
    audit = {
        "stage": str(stage),
        "timeout_seconds": timeout,
        "task_count": len(task_audit),
        "completed_count": sum(item["status"] == "completed" for item in task_audit),
        "timeout_count": sum(item["status"] == "timeout" for item in task_audit),
        "error_count": sum(item["status"] == "error" for item in task_audit),
        "absent_labels": [
            item["label"] for item in task_audit if item["status"] != "completed"
        ],
        "tasks": task_audit,
    }
    if audit["timeout_count"] or audit["error_count"]:
        logger.warning(
            "Committee stage %s preserved %s/%s results; timeout=%s error=%s absent=%s",
            stage,
            audit["completed_count"],
            audit["task_count"],
            audit["timeout_count"],
            audit["error_count"],
            audit["absent_labels"],
        )
    return results, audit


async def retry_absent_committee_results(
    results: List[str],
    audit: Dict[str, Any],
    retry_factories: List[tuple[str, Callable[[], Awaitable[str]]]],
    *,
    timeout_seconds: float,
    stage: str,
) -> tuple[List[str], Dict[str, Any]]:
    """Retry only absent roles once while preserving every completed result."""
    absent = {
        str(item.get("label") or "")
        for item in (audit.get("tasks") or [])
        if item.get("status") != "completed"
    }
    retry_specs = [
        (label, factory)
        for label, factory in retry_factories
        if label in absent
    ]
    if not retry_specs:
        return results, audit

    retry_results, retry_audit = await collect_committee_results(
        [(label, factory()) for label, factory in retry_specs],
        timeout_seconds=timeout_seconds,
        stage=f"{stage}_retry",
    )
    merged_results = list(results)
    merged_tasks = [dict(item) for item in (audit.get("tasks") or [])]
    index_by_label = {
        str(item.get("label") or ""): index
        for index, item in enumerate(merged_tasks)
    }
    recovered = 0
    for (label, _factory), retry_result, retry_task in zip(
        retry_specs,
        retry_results,
        retry_audit.get("tasks") or [],
    ):
        original_index = index_by_label.get(label)
        if original_index is None:
            continue
        if retry_task.get("status") == "completed":
            merged_results[original_index] = retry_result
            merged_tasks[original_index] = {
                **retry_task,
                "stage": f"{stage}_retry",
                "recovered_after_retry": True,
            }
            recovered += 1

    final_audit = {
        **audit,
        "tasks": merged_tasks,
        "completed_count": sum(
            item.get("status") == "completed" for item in merged_tasks
        ),
        "timeout_count": sum(
            item.get("status") == "timeout" for item in merged_tasks
        ),
        "error_count": sum(
            item.get("status") == "error" for item in merged_tasks
        ),
        "absent_labels": [
            str(item.get("label") or "")
            for item in merged_tasks
            if item.get("status") != "completed"
        ],
        "retry_attempted_count": len(retry_specs),
        "retry_recovered_count": recovered,
        "initial_attempt": audit,
        "retry_attempt": retry_audit,
    }
    logger.info(
        "Committee stage %s retry recovered %s/%s absent roles; final=%s/%s",
        stage,
        recovered,
        len(retry_specs),
        final_audit["completed_count"],
        final_audit["task_count"],
    )
    return merged_results, final_audit


def aggregate_committee_decision(
    proposal: Dict,
    vote_results: List[str],
    vote_weights: Optional[List[float]] = None,
    vote_labels: Optional[List[str]] = None,
) -> Dict:
    """Aggregate loose text votes into the decision used by downstream systems."""
    parsed = [parse_committee_vote(vote) for vote in vote_results]
    weights = vote_weights or [2.0, 1.0, 1.0, 1.0, 1.0, 1.5, 1.0]
    labels = vote_labels or [f"vote_{index + 1}" for index in range(len(parsed))]
    scores = {"long": 0.0, "short": 0.0, "hold": 0.0, "abstain": 0.0}
    for index, vote in enumerate(parsed):
        if not vote.get("is_valid"):
            continue
        scores[vote["direction"]] += weights[index] if index < len(weights) else 1.0

    parsed_vote_count = sum(bool(vote.get("is_valid")) for vote in parsed)
    quorum_required = max(2, (len(parsed) * 3 + 4) // 5) if parsed else 2
    participation_quorum_met = parsed_vote_count >= quorum_required
    directional_vote_count = sum(
        bool(vote.get("is_valid")) and vote.get("direction") != "abstain"
        for vote in parsed
    )
    directional_quorum_required = min(3, quorum_required)
    directional_quorum_met = directional_vote_count >= directional_quorum_required
    quorum_met = participation_quorum_met and directional_quorum_met
    if quorum_met and scores["long"] > scores["short"] and scores["long"] > scores["hold"]:
        direction = "long"
    elif quorum_met and scores["short"] > scores["long"] and scores["short"] > scores["hold"]:
        direction = "short"
    else:
        direction = "hold"

    total_weight = sum(
        weights[index] if index < len(weights) else 1.0
        for index, vote in enumerate(parsed)
        if vote.get("is_valid") and vote.get("direction") != "abstain"
    )
    selected_votes = [
        (
            vote,
            weights[index] if index < len(weights) else 1.0,
        )
        for index, vote in enumerate(parsed)
        if vote.get("is_valid") and vote.get("direction") == direction
    ]
    confidence_weight = sum(
        weight for vote, weight in selected_votes
        if vote.get("confidence") is not None
    )
    confidence = (
        sum(float(vote["confidence"]) * weight for vote, weight in selected_votes
            if vote.get("confidence") is not None)
        / confidence_weight
        if confidence_weight
        else float(proposal.get("confidence", 0.5))
    )
    position_weight = sum(
        weight for vote, weight in selected_votes
        if vote.get("position") is not None
    )
    target_position = (
        sum(float(vote["position"]) * weight for vote, weight in selected_votes
            if vote.get("position") is not None)
        / position_weight
        if position_weight
        else float(proposal.get("target_position", 0.1))
    )
    if direction == "hold":
        target_position = 0.0
    sorted_scores = sorted(scores.values(), reverse=True)
    margin = (sorted_scores[0] - sorted_scores[1]) / total_weight if total_weight and len(sorted_scores) > 1 else 0.0
    direction_support = scores.get(direction, 0.0) / total_weight if total_weight else 0.0
    risk_flags = []
    for vote in parsed:
        if not vote.get("is_valid"):
            continue
        risk_flags.extend(vote.get("risk_flags") or [])
    selected_key_evidence = list(dict.fromkeys(
        str(item).strip()
        for vote, _weight in selected_votes
        for item in (vote.get("key_evidence") or [])
        if str(item).strip()
    ))[:8]
    reconsider_if = list(dict.fromkeys(
        str(vote.get("invalid_if") or "").strip()
        for vote, _weight in selected_votes
        if str(vote.get("invalid_if") or "").strip()
    ))[:8]

    return {
        "direction": direction,
        "confidence": max(0.0, min(1.0, confidence)),
        "target_position": max(0.0, min(1.0, target_position)),
        "vote_summary": scores,
        "vote_margin": round(margin, 4),
        "direction_support": round(direction_support, 4),
        "vote_count": len(parsed),
        "parsed_vote_count": parsed_vote_count,
        "invalid_vote_count": len(parsed) - parsed_vote_count,
        "vote_quorum_required": quorum_required,
        "vote_quorum_met": quorum_met,
        "participation_quorum_met": participation_quorum_met,
        "directional_vote_count": directional_vote_count,
        "directional_quorum_required": directional_quorum_required,
        "directional_quorum_met": directional_quorum_met,
        "vote_parse_modes": [
            str(vote.get("parse_mode") or "unknown") for vote in parsed
        ],
        "individual_votes": [
            {
                "role": labels[index] if index < len(labels) else f"vote_{index + 1}",
                "direction": vote.get("direction"),
                "confidence": vote.get("confidence"),
                "position": vote.get("position"),
                "effective_weight": (
                    weights[index] if index < len(weights) else 1.0
                ),
                "is_valid": bool(vote.get("is_valid")),
                "parse_mode": str(vote.get("parse_mode") or "unknown"),
                "key_evidence": vote.get("key_evidence") or [],
                "invalid_if": vote.get("invalid_if") or "",
            }
            for index, vote in enumerate(parsed)
        ],
        "risk_flags": list(dict.fromkeys(risk_flags))[:8],
        "decision_evidence": selected_key_evidence,
        # The vote prompt defines HOLD key_evidence as the exact missing
        # decisive evidence. Preserve it so the next research round can target
        # the gap instead of replaying generic risk prose.
        "evidence_gaps": selected_key_evidence if direction == "hold" else [],
        "reconsider_if": reconsider_if,
    }


def committee_deadlock_requires_review(
    decision: Dict[str, Any],
    proposal: Dict[str, Any],
    topic: str,
) -> bool:
    """Identify an auditable deployment deadlock without approving a trade."""
    evidence = [
        item for item in (proposal.get("evidence") or [])
        if str(item).strip()
    ]
    return bool(
        "空仓资金部署" in str(topic or "")
        and str(decision.get("direction") or "").lower() == "hold"
        and bool(decision.get("vote_quorum_met"))
        and str(proposal.get("direction") or "").lower() in {"long", "short"}
        and len(evidence) >= 2
        and str(proposal.get("thesis") or "").strip()
    )


def merge_committee_deadlock_review(
    original: Dict[str, Any],
    review: Dict[str, Any],
    *,
    min_confidence: float = 0.65,
    min_direction_support: float = 0.60,
) -> Dict[str, Any]:
    """Adopt only a strong, quorate core-committee directional re-review."""
    original_copy = dict(original)
    direction = str(review.get("direction") or "hold").lower()
    qualifies = bool(
        direction in {"long", "short"}
        and review.get("vote_quorum_met")
        and float(review.get("confidence") or 0.0) >= float(min_confidence)
        and float(review.get("direction_support") or 0.0)
        >= float(min_direction_support)
        and float(review.get("target_position") or 0.0) > 0
    )
    audit = {
        "triggered": True,
        "adopted": qualifies,
        "min_confidence": float(min_confidence),
        "min_direction_support": float(min_direction_support),
        "original_direction": str(original.get("direction") or "hold"),
        "original_vote_summary": original.get("vote_summary") or {},
        "review_direction": direction,
        "review_confidence": float(review.get("confidence") or 0.0),
        "review_direction_support": float(
            review.get("direction_support") or 0.0
        ),
        "review_vote_summary": review.get("vote_summary") or {},
        "review_quorum_met": bool(review.get("vote_quorum_met")),
        "review_individual_votes": review.get("individual_votes") or [],
    }
    if not qualifies:
        original_copy["deadlock_review"] = audit
        return original_copy

    adopted = dict(review)
    adopted["deadlock_review"] = audit
    adopted["initial_committee_decision"] = {
        key: original.get(key)
        for key in (
            "direction",
            "confidence",
            "target_position",
            "vote_summary",
            "vote_margin",
            "direction_support",
            "individual_votes",
            "evidence_gaps",
            "reconsider_if",
        )
    }
    adopted["risk_flags"] = list(dict.fromkeys(
        list(original.get("risk_flags") or [])
        + list(review.get("risk_flags") or [])
    ))[:8]
    return adopted


def build_deployment_deadlock_review_prompt(
    ticker: str,
    role_view: str,
    proposal: Dict[str, Any],
    original_decision: Dict[str, Any],
    context: str,
) -> str:
    """Build a narrow second review that cannot lower the evidence bar."""
    return f"""
空仓资金部署议题中，第一轮投委会对 {ticker} 形成HOLD。请从{role_view}独立复核：
这是“事实反证成立”，还是“把一般风险/待补数据误当成否决”造成的部署死锁。

原提案（不得添加新事实）：
{json.dumps(proposal, ensure_ascii=False, default=str)[:7000]}

第一轮裁决：
{json.dumps(original_decision, ensure_ascii=False, default=str)[:5000]}

已有讨论摘要：
{context[:5000]}

只输出JSON对象：
{{
  "direction": "long/short/hold/abstain",
  "confidence": 0.0,
  "position": 0.0,
  "key_evidence": ["最关键证据1", "最关键证据2"],
  "risk_flags": ["主要风险1", "主要风险2"],
  "invalid_if": "推翻判断的具体条件"
}}

硬约束：
- 不降低证据门槛，不因为“必须部署”而买入；事实证据不足仍投HOLD。
- 不得把缺少历史日线、一般波动风险或尚未取得的数据写成已经发生的事实反证。
- 只有原提案已有至少两条可追溯证据、明确止损/期限且风险收益比可接受时，才可在仓位纪律内投long/short。
- HOLD必须指出决定性证据缺口；超出能力圈必须abstain。
- 这是模拟投资裁决，禁止真实下单。
{COMMITTEE_DECISION_BOUNDARIES}
""".strip()


def committee_decision_is_predictable(decision: Dict) -> bool:
    """Return whether a committee outcome can enter the validation loop."""
    direction = str(decision.get("direction") or "hold").strip().lower()
    if direction in {"long", "short"}:
        return True
    return direction == "hold" and bool(decision.get("vote_quorum_met"))


def preflight_committee_decisions(
    decisions: List[Dict],
    current_tickers: set[str],
    normalize_ticker: Callable[[str], str],
    *,
    min_long_confidence: float | None = None,
) -> tuple[List[Dict], List[Dict[str, str]]]:
    """Separate executable committee decisions from deterministic rejections.

    This runs before quote lookup or simulated execution.  Previously non-trade
    decisions disappeared from the deployment path, leaving repeated empty-book
    rounds with only the unhelpful ``missing_approved_candidates`` label.
    """
    executable: List[Dict] = []
    rejections: List[Dict[str, str]] = []
    from sovereign_hall.services.market_data import MarketDataService
    if min_long_confidence is None:
        from sovereign_hall.core.config import get_config

        min_long_confidence = float(
            get_config().get("simulation", {}).get(
                "min_committee_confidence",
                0.65,
            )
            or 0.65
        )

    normalized_positions = {normalize_ticker(ticker) for ticker in current_tickers}

    for index, decision in enumerate(decisions):
        raw_ticker = str(decision.get("ticker") or "").strip()
        ticker = normalize_ticker(raw_ticker) if raw_ticker else ""
        direction = str(decision.get("direction") or "hold").strip().lower()
        risk_flags = [str(item) for item in (decision.get("risk_flags") or []) if str(item).strip()]
        suffix = f"；risk_flags={','.join(risk_flags[:3])}" if risk_flags else ""
        evidence_gaps = [
            str(item).strip()
            for item in (decision.get("evidence_gaps") or [])
            if str(item).strip()
        ]
        reconsider_if = [
            str(item).strip()
            for item in (decision.get("reconsider_if") or [])
            if str(item).strip()
        ]
        if evidence_gaps:
            suffix += f"；evidence_gaps={','.join(evidence_gaps[:3])}"
        if reconsider_if:
            suffix += f"；reconsider_if={','.join(reconsider_if[:3])}"
        vote_summary = decision.get("vote_summary")
        vote_count = int(decision.get("vote_count") or 0)
        parsed_vote_count = int(decision.get("parsed_vote_count") or vote_count)
        invalid_vote_count = int(decision.get("invalid_vote_count") or 0)
        directional_vote_count = int(
            decision.get("directional_vote_count") or parsed_vote_count
        )
        directional_quorum_required = int(
            decision.get("directional_quorum_required")
            or decision.get("vote_quorum_required")
            or 0
        )
        vote_margin = float(decision.get("vote_margin") or 0.0)
        vote_audit = (
            f"；vote_summary={vote_summary}；vote_margin={vote_margin:.4f}；"
            f"parsed_votes={parsed_vote_count}/{vote_count}；"
            f"directional_votes={directional_vote_count}/{directional_quorum_required}；"
            f"invalid_votes={invalid_vote_count}"
            if vote_summary is not None or vote_count
            else ""
        )
        suffix += vote_audit

        if not ticker:
            rejections.append({
                "code": "missing_ticker",
                "ticker": f"decision_{index + 1}",
                "reason": "投委会裁决缺少可识别ticker" + suffix,
            })
            continue
        if not MarketDataService.is_supported_ticker(ticker):
            rejections.append({
                "code": "invalid_ticker",
                "ticker": f"decision_{index + 1}",
                "reason": f"投委会裁决ticker不是可执行的A股/ETF六位代码: {raw_ticker!r}" + suffix,
            })
            continue
        if direction in {"hold", "neutral", "watch", "观望"}:
            quorum_met = bool(decision.get("vote_quorum_met", True))
            rejections.append({
                "code": "committee_hold" if quorum_met else "committee_vote_quorum_failed",
                "ticker": ticker,
                "reason": (
                    "投委会证据未形成多头/退出裁决"
                    if quorum_met
                    else (
                        "投委会有效票不足法定人数"
                        f"(出席{parsed_vote_count}/"
                        f"{int(decision.get('vote_quorum_required') or 0)}，"
                        f"方向票{directional_vote_count}/{directional_quorum_required})"
                    )
                ) + suffix,
            })
            continue
        if direction not in {"long", "short", "sell"}:
            rejections.append({
                "code": "unsupported_direction",
                "ticker": ticker,
                "reason": f"不支持的裁决方向={direction}" + suffix,
            })
            continue
        if direction in {"short", "sell"} and ticker not in normalized_positions:
            rejections.append({
                "code": "short_without_position",
                "ticker": ticker,
                "reason": "模拟账户无该持仓且禁止裸做空",
            })
            continue
        if direction == "long" and ticker in normalized_positions:
            rejections.append({
                "code": "already_held_long",
                "ticker": ticker,
                "reason": "已有持仓；新增提案不能替代独立生命周期复核",
            })
            continue
        raw_confidence = decision.get("confidence")
        try:
            confidence = (
                float(raw_confidence) if raw_confidence is not None else None
            )
        except (TypeError, ValueError):
            confidence = None
        if (
            direction == "long"
            and confidence is not None
            and confidence < float(min_long_confidence)
        ):
            rejections.append({
                "code": "heuristic_entry_veto",
                "ticker": ticker,
                "reason": (
                    f"投委会置信度{confidence:.1%}低于统一入场硬门"
                    f"{float(min_long_confidence):.1%}；不得进入闭市队列或"
                    "在开市路径绕过同一门槛"
                ) + suffix,
            })
            continue
        try:
            target_position = float(decision.get("target_position") or 0.0)
        except (TypeError, ValueError):
            target_position = 0.0
        if direction == "long" and target_position <= 0:
            rejections.append({
                "code": "zero_target_position",
                "ticker": ticker,
                "reason": "多头裁决目标仓位为0，不能进入模拟成交",
            })
            continue

        normalized = dict(decision)
        normalized["ticker"] = ticker
        normalized["direction"] = "short" if direction == "sell" else direction
        executable.append(normalized)

    return executable, rejections


# ============================================================================
# 阶段3：投委会审议（多轮辩论）
# ============================================================================
async def stage3_ic_discussion(
    llm,
    spiders,
    proposals: list,
    topic: str,
    lessons_prompt: str = "",
    round_id: str | None = None,
    decision_callback: Callable[[Dict[str, Any]], Awaitable[None]] | None = None,
):
    """阶段3：投委会审议"""
    if not proposals:
        logger.warning("阶段3：无提案，跳过审议")
        return "", []

    from sovereign_hall.core import AgentRole
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.decision_tracker import DecisionRecorder

    research_config = get_config().get("research", {})
    committee_proposal_limit = int(research_config.get("committee_proposal_limit", 5) or 5)
    committee_full_discussion = bool(research_config.get("committee_full_discussion", True))
    committee_min_review_depth = str(research_config.get("committee_min_review_depth", "full") or "full")
    round1_max_tokens = int(research_config.get("committee_round1_max_tokens", 12000) or 12000)
    round2_max_tokens = int(research_config.get("committee_round2_max_tokens", 10000) or 10000)
    revision_max_tokens = int(research_config.get("committee_revision_max_tokens", 8000) or 8000)
    vote_max_tokens = int(research_config.get("committee_vote_max_tokens", 5000) or 5000)
    summary_chars = int(research_config.get("committee_summary_chars", 1200) or 1200)
    vote_context_chars = int(research_config.get("committee_vote_context_chars", 6000) or 6000)
    round1_role_timeout = float(
        research_config.get("committee_round1_role_timeout_seconds", 240) or 240
    )
    round2_role_timeout = float(
        research_config.get("committee_round2_role_timeout_seconds", 180) or 180
    )
    revision_role_timeout = float(
        research_config.get("committee_revision_role_timeout_seconds", 120) or 120
    )
    vote_role_timeout = float(
        research_config.get("committee_vote_role_timeout_seconds", 90) or 90
    )
    vote_retry_timeout = float(
        research_config.get("committee_vote_retry_timeout_seconds", 60) or 60
    )
    vote_retry_max_tokens = int(
        research_config.get("committee_vote_retry_max_tokens", 1800) or 1800
    )
    deadlock_review_enabled = bool(
        research_config.get("committee_deadlock_review_enabled", True)
    )
    deadlock_review_min_confidence = float(
        research_config.get("committee_deadlock_min_confidence", 0.65) or 0.65
    )
    deadlock_review_min_support = float(
        research_config.get("committee_deadlock_min_direction_support", 0.60)
        or 0.60
    )
    deadlock_review_limit = int(
        research_config.get("committee_deadlock_max_per_round", 1) or 1
    )
    deadlock_review_timeout = float(
        research_config.get("committee_deadlock_role_timeout_seconds", 90) or 90
    )

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
    deadlock_reviews_used = 0

    # 每轮优先讨论最高价值提案，但默认扩大审议面并保持深度辩论。
    committee_proposals = select_committee_proposals(proposals, limit=committee_proposal_limit)
    for i, proposal in enumerate(committee_proposals):
        ticker = proposal.get('ticker', '')
        thesis = proposal.get('thesis', '')
        sector = proposal.get('sector', '')
        review_depth = choose_review_depth(proposal)
        if committee_min_review_depth == "full":
            review_depth = "full"
        elif committee_min_review_depth == "focused" and review_depth == "light":
            review_depth = "focused"
        if not committee_full_discussion and review_depth == "full":
            review_depth = "focused"
        priority_score = proposal_priority_score(proposal)
        learned_context = f"\n\n{lessons_prompt}" if lessons_prompt else ""
        analysis_format = (
            "\n\n输出要求：只讲新增判断，不复述提案；"
            "按【证据】【风险/机会】【反证/压力测试】【结论】输出；"
            "不限制必要展开，但每条都必须承担不同验证角度；"
            "必须给出正反两面推理、至少两个可证伪条件、仓位纪律、观察指标；"
            "结论必须含买入/卖出/观望、置信度和否决条件。"
            f"\n\n{COMMITTEE_DECISION_BOUNDARIES}"
        )

        print(f"\n### 提案 {i+1}: {ticker} ({proposal.get('direction')}) | 置信度: {proposal.get('confidence', 0):.0%} | 深度: {review_depth} | score={priority_score:.2f}")
        stage_execution_audit: List[Dict[str, Any]] = []

        # ============================================================
        # 第一轮：按审议深度并发分析
        # ============================================================
        round1_tasks = [
            (agents[AgentRole.RISK_OFFICER], "风控-财务风险", f"作为风控官，分析{ticker}的财务造假风险。核心观点：{thesis}。请找出潜在风险。{learned_context}{analysis_format}", [f"{ticker} 财务", f"{ticker} 风险"]),
            (agents[AgentRole.RISK_OFFICER], "风控-最坏情况", f"作为风控官，分析{ticker}最坏情况可能跌多少。{learned_context}{analysis_format}", [f"{ticker} 历史跌幅"]),
            (agents[AgentRole.RISK_OFFICER], "风控-仓位纪律", f"作为风控官，给出{ticker}仓位上限、止损纪律、风险预算和触发减仓的量化条件。{learned_context}{analysis_format}", [f"{ticker} 风险预算", f"{ticker} 止损"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-技术面", f"作为量化分析师，分析{ticker}的技术走势。{learned_context}{analysis_format}", [f"{ticker} K线", f"{ticker} 技术分析"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-估值", f"作为量化分析师，分析{ticker}的估值水平PE/PB。{learned_context}{analysis_format}", [f"{ticker} 估值", f"{ticker} PE"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-胜率赔率", f"作为量化分析师，拆解{ticker}胜率、赔率、回撤、拥挤度和交易信号有效性。{learned_context}{analysis_format}", [f"{ticker} 胜率", f"{ticker} 拥挤度"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-政策风险", f"作为宏观策略师，分析{ticker}面临的政策风险。{learned_context}{analysis_format}", [f"{ticker} 政策", f"{sector} 政策"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-时机", f"作为宏观策略师，分析当前是否是买入{ticker}的时机。{learned_context}{analysis_format}", ["A股 买入时机", "2025 投资"]),
            (agents[AgentRole.MACRO_STRATEGIST], "宏观-流动性", f"作为宏观策略师，分析流动性、利率、汇率和风险偏好对{ticker}的影响路径。{learned_context}{analysis_format}", ["A股 流动性", "利率 汇率 风险偏好"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-行业", f"作为TMT分析师，从行业角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 行业", f"{ticker} 动态"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-行业", f"作为消费分析师，从行业角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 消费", f"{ticker} 消费"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-行业", f"作为周期分析师，从行业周期角度点评{ticker}。{learned_context}{analysis_format}", [f"{sector} 周期"]),
            (agents[AgentRole.CIO], "CIO-综合", f"作为CIO，综合分析{ticker}的投资价值。{learned_context}{analysis_format}", [f"{ticker} 机构观点", f"{ticker} 评级"]),
            (agents[AgentRole.CIO], "CIO-组合适配", f"作为CIO，分析{ticker}在组合中的角色、与现有持仓相关性、替代标的和执行优先级。{learned_context}{analysis_format}", [f"{ticker} 组合配置", f"{ticker} 替代标的"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-机会", f"作为TMT分析师，分析{ticker}的增长机会。{learned_context}{analysis_format}", [f"{ticker} 增长", f"{ticker} 前景"]),
            (agents[AgentRole.TMT_ANALYST], "TMT-竞争格局", f"作为TMT分析师，分析{ticker}的竞争格局、技术替代和产业链议价能力。{learned_context}{analysis_format}", [f"{ticker} 竞争格局", f"{sector} 产业链"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-机会", f"作为消费分析师，分析{ticker}的增长机会。{learned_context}{analysis_format}", [f"{ticker} 业绩", f"{ticker} 增长"]),
            (agents[AgentRole.CONSUMER_ANALYST], "消费-需求验证", f"作为消费分析师，分析{ticker}需求端恢复是否真实、库存和渠道反馈是否支持提案。{learned_context}{analysis_format}", [f"{ticker} 需求", f"{ticker} 库存"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-机会", f"作为周期分析师，分析{ticker}的周期位置。{learned_context}{analysis_format}", [f"{sector} 供需"]),
            (agents[AgentRole.CYCLE_ANALYST], "周期-价格传导", f"作为周期分析师，分析{ticker}上游成本、产品价格、库存周期和利润传导。{learned_context}{analysis_format}", [f"{ticker} 价格", f"{sector} 库存"]),
            (agents[AgentRole.QUANT_RESEARCHER], "量化-资金", f"作为量化分析师，分析{ticker}的资金流向。{learned_context}{analysis_format}", [f"{ticker} 主力资金"]),
        ]
        if not committee_full_discussion and review_depth == "focused":
            focused_names = {"风控-财务风险", "风控-最坏情况", "量化-技术面", "量化-估值", "宏观-时机", "CIO-综合", "TMT-机会", "消费-机会", "周期-机会"}
            round1_tasks = [task for task in round1_tasks if task[1] in focused_names]
        elif not committee_full_discussion and review_depth == "light":
            light_names = {"风控-最坏情况", "量化-技术面", "宏观-时机", "CIO-综合"}
            round1_tasks = [task for task in round1_tasks if task[1] in light_names]
        print(f"   📝 第一轮：{len(round1_tasks)}路并发分析...")

        try:
            round1_results, round1_audit = await collect_committee_results(
                [
                    (
                        name,
                        committee_think_from_persisted_evidence(
                            agent,
                            task=task,
                            proposal=proposal,
                            discussion_context=thesis,
                            temperature=0.8,
                            max_tokens=round1_max_tokens,
                            round_id=round_id,
                        ),
                    )
                    for agent, name, task, _queries in round1_tasks
                ],
                timeout_seconds=round1_role_timeout,
                stage="round1_independent_analysis",
            )
            round1_audit.update({
                "input_lineage": "persisted_round_proposal",
                "round_id": round_id,
                "committee_network_search_requests": 0,
            })
            stage_execution_audit.append(round1_audit)

            task_names = [name for _, name, _, _ in round1_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第一轮分析\n{'='*50}")
            for name, result in zip(task_names, round1_results):
                all_discussions.append(f"\n[{name}]\n{result[:summary_chars]}")

            print(
                f"      ✅ 第一轮完成 "
                f"({round1_audit['completed_count']}/{round1_audit['task_count']}完成，"
                f"超时{round1_audit['timeout_count']}，错误{round1_audit['error_count']})"
            )

            round1_summary = "\n".join([f"{name}: {r[:summary_chars]}" for name, r in zip(task_names, round1_results)])

            debate_tasks = [
                (agents[AgentRole.RISK_OFFICER], "质疑", f"基于以下分析提出最尖锐的质疑，并指出哪些论证最可能错：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{ticker} 风险", f"{ticker} 问题"]),
                (agents[AgentRole.QUANT_RESEARCHER], "数据质疑", f"基于以下分析指出数据问题、样本偏差、估值错配和交易拥挤风险：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{ticker} 数据", f"{ticker} 估值"]),
                (agents[AgentRole.MACRO_STRATEGIST], "宏观质疑", f"基于以下分析指出宏观风险、政策反身性和市场风格切换风险：\n{round1_summary[:vote_context_chars]}{analysis_format}", ["宏观经济 风险", "市场风格 切换"]),
                (agents[AgentRole.TMT_ANALYST], "行业反驳", f"从行业角度反驳其他观点，尤其要找出被低估的产业催化和被高估的叙事：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{sector} 趋势", f"{ticker} 催化"]),
                (agents[AgentRole.CONSUMER_ANALYST], "消费反驳", f"从消费/需求角度反驳其他观点，并检验终端需求与价格弹性：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{sector} 消费", f"{ticker} 需求"]),
                (agents[AgentRole.CYCLE_ANALYST], "周期反驳", f"从周期角度反驳其他观点，并检验库存、价格、产能和利润传导：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{sector} 周期", f"{sector} 价格"]),
                (agents[AgentRole.CIO], "CIO回应", f"回应各方质疑，给出组合层面的最终倾向、仓位和执行节奏：\n{round1_summary[:vote_context_chars]}{analysis_format}", [f"{ticker} 机构", f"{ticker} 评级"]),
            ]
            if not committee_full_discussion and review_depth == "focused":
                focused_debate_names = {"质疑", "数据质疑", "宏观质疑", "CIO回应"}
                debate_tasks = [task for task in debate_tasks if task[1] in focused_debate_names]
            elif not committee_full_discussion and review_depth == "light":
                debate_tasks = []

            # ============================================================
            # 第二轮：按需深度辩论
            # ============================================================
            debate_names = []
            round2_results = []
            if debate_tasks:
                print(f"   📝 第二轮：深度辩论 ({len(debate_tasks)}路)...")
                round2_results, round2_audit = await collect_committee_results(
                    [
                        (
                            name,
                            committee_think_from_persisted_evidence(
                                agent,
                                task=task,
                                proposal=proposal,
                                discussion_context=round1_summary[
                                    :vote_context_chars
                                ],
                                temperature=0.7,
                                max_tokens=round2_max_tokens,
                                round_id=round_id,
                            ),
                        )
                        for agent, name, task, _queries in debate_tasks
                    ],
                    timeout_seconds=round2_role_timeout,
                    stage="round2_cross_challenge",
                )
                round2_audit.update({
                    "input_lineage": "persisted_round_proposal",
                    "round_id": round_id,
                    "committee_network_search_requests": 0,
                })
                stage_execution_audit.append(round2_audit)

                debate_names = [name for _, name, _, _ in debate_tasks]
                all_discussions.append(f"\n{'='*50}\n【{ticker}】第二轮辩论\n{'='*50}")
                for name, result in zip(debate_names, round2_results):
                    all_discussions.append(f"\n[{name}]\n{result[:summary_chars]}")

                print(
                    f"      ✅ 第二轮完成 "
                    f"({round2_audit['completed_count']}/{round2_audit['task_count']}完成，"
                    f"超时{round2_audit['timeout_count']}，错误{round2_audit['error_count']})"
                )
            else:
                print("   📝 第二轮：轻量提案，跳过深度辩论")

            # ============================================================
            # 第三轮：二次修正与反事实复盘
            # ============================================================
            print("   🧠 第三轮：二次修正与反事实复盘...")
            revision_context = (
                f"【第一轮分析】\n{round1_summary[:vote_context_chars]}\n\n"
                f"【第二轮辩论】\n"
                + "\n".join([f"{n}: {r[:summary_chars]}" for n, r in zip(debate_names, round2_results)])
            )
            revision_tasks = [
                (agents[AgentRole.CIO], "CIO-最终修正", "整合全部争议，重写最终投资备忘录，必须说明是否推翻原提案、仓位如何调整、最重要的三条跟踪指标。"),
                (agents[AgentRole.RISK_OFFICER], "风控-否决清单", "列出可以一票否决该提案的证据、价格行为、财务信号和宏观触发条件，并给出监控频率。"),
                (agents[AgentRole.QUANT_RESEARCHER], "量化-执行计划", "给出入场、加仓、止损、止盈、回撤控制和失败样本复盘框架。"),
                (agents[AgentRole.MACRO_STRATEGIST], "宏观-情景矩阵", "构造乐观/基准/悲观三种宏观情景，分别判断该提案的胜率、赔率和仓位。"),
                (agents[AgentRole.TMT_ANALYST], "行业-催化复核", "复核行业催化、技术路线、竞争格局与供应链证据，指出最可能误判的地方。"),
                (agents[AgentRole.CONSUMER_ANALYST], "需求-验证框架", "复核需求侧证据、渠道库存、价格敏感性和消费场景变化，给出验证路径。"),
                (agents[AgentRole.CYCLE_ANALYST], "周期-拐点复核", "复核周期拐点、库存、产能、价格传导和盈利弹性，给出反转/失败条件。"),
            ]
            if not committee_full_discussion and review_depth == "focused":
                focused_revision_names = {
                    "CIO-最终修正",
                    "风控-否决清单",
                    "量化-执行计划",
                    "宏观-情景矩阵",
                }
                revision_tasks = [
                    task for task in revision_tasks
                    if task[1] in focused_revision_names
                ]
            elif not committee_full_discussion and review_depth == "light":
                revision_tasks = [
                    task for task in revision_tasks
                    if task[1] in {"CIO-最终修正", "风控-否决清单"}
                ]
            revision_results, revision_audit = await collect_committee_results(
                [
                    (
                        name,
                        agent.think(
                            task=f"{task}\n\n提案：{ticker}\n核心观点：{thesis}\n\n讨论材料：\n{revision_context[:vote_context_chars]}{analysis_format}",
                            temperature=0.6,
                            max_tokens=revision_max_tokens,
                        ),
                    )
                    for agent, name, task in revision_tasks
                ],
                timeout_seconds=revision_role_timeout,
                stage="round3_counterfactual_revision",
            )
            stage_execution_audit.append(revision_audit)
            revision_names = [name for _, name, _ in revision_tasks]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第三轮修正复盘\n{'='*50}")
            for name, result in zip(revision_names, revision_results):
                all_discussions.append(f"\n[{name}]\n{result[:summary_chars]}")
            print(
                f"      ✅ 第三轮完成 "
                f"({revision_audit['completed_count']}/{revision_audit['task_count']}完成，"
                f"超时{revision_audit['timeout_count']}，错误{revision_audit['error_count']})"
            )

            # ============================================================
            # 第四轮：投票裁决
            # ============================================================
            print("   📊 第四轮：投票...")

            full_context = build_balanced_vote_context(
                proposal,
                list(zip(task_names, round1_results)),
                list(zip(debate_names, round2_results)),
                list(zip(revision_names, revision_results)),
                vote_context_chars,
            )

            vote_tasks = [
                (agents[AgentRole.CIO], AgentRole.CIO, "CIO综合视角", 2.0),
                (agents[AgentRole.TMT_ANALYST], AgentRole.TMT_ANALYST, "TMT行业视角", 1.0),
                (agents[AgentRole.CONSUMER_ANALYST], AgentRole.CONSUMER_ANALYST, "消费行业视角", 1.0),
                (agents[AgentRole.CYCLE_ANALYST], AgentRole.CYCLE_ANALYST, "周期行业视角", 1.0),
                (agents[AgentRole.MACRO_STRATEGIST], AgentRole.MACRO_STRATEGIST, "宏观策略视角", 1.0),
                (agents[AgentRole.RISK_OFFICER], AgentRole.RISK_OFFICER, "风控视角", 1.5),
                (agents[AgentRole.QUANT_RESEARCHER], AgentRole.QUANT_RESEARCHER, "量化视角", 1.0),
            ]
            if not committee_full_discussion and review_depth == "focused":
                vote_tasks = [task for task in vote_tasks if task[2] in {"CIO综合视角", "宏观策略视角", "风控视角", "量化视角"}]
            elif not committee_full_discussion and review_depth == "light":
                vote_tasks = [task for task in vote_tasks if task[2] in {"CIO综合视角", "风控视角", "量化视角"}]

            vote_prompts = [
                build_structured_vote_prompt(ticker, role_view, full_context, learned_context)
                for _, _, role_view, _ in vote_tasks
            ]

            # 第四轮投票：逐角色超时，缺席不伪装成HOLD。
            print("      🔄 等待投票结果...")
            vote_names = [name for _, _, name, _ in vote_tasks]
            vote_results, vote_audit = await collect_committee_results(
                [
                    (
                        name,
                        agent.think(
                            task=prompt,
                            temperature=0.4,
                            max_tokens=vote_max_tokens,
                        ),
                    )
                    for (agent, _, name, _), prompt in zip(vote_tasks, vote_prompts)
                ],
                timeout_seconds=vote_role_timeout,
                stage="round4_vote",
            )
            vote_retry_factories: List[
                tuple[str, Callable[[], Awaitable[str]]]
            ] = []
            for (agent, _role, name, _weight), prompt in zip(
                vote_tasks,
                vote_prompts,
            ):
                vote_retry_factories.append(
                    (
                        name,
                        lambda agent=agent, prompt=prompt: agent.think(
                            task=prompt,
                            temperature=0.1,
                            max_tokens=vote_retry_max_tokens,
                        ),
                    )
                )
            vote_results, vote_audit = await retry_absent_committee_results(
                vote_results,
                vote_audit,
                vote_retry_factories,
                timeout_seconds=vote_retry_timeout,
                stage="round4_vote",
            )
            stage_execution_audit.append(vote_audit)
            vote_weights = [
                committee_role_weight(role, sector, topic, weight)
                for _, role, _, weight in vote_tasks
            ]
            all_discussions.append(f"\n{'='*50}\n【{ticker}】第四轮投票\n{'='*50}")
            for name, result in zip(vote_names, vote_results):
                all_discussions.append(f"\n[{name}]\n{result[:summary_chars]}")

            committee_decision = aggregate_committee_decision(
                proposal,
                vote_results,
                vote_weights=vote_weights,
                vote_labels=vote_names,
            )
            if (
                deadlock_review_enabled
                and deadlock_reviews_used < max(0, deadlock_review_limit)
                and committee_deadlock_requires_review(
                    committee_decision,
                    proposal,
                    topic,
                )
            ):
                deadlock_reviews_used += 1
                print(
                    "      🔁 触发空仓部署死锁复核：核心三角色独立判断，"
                    "不降低证据门槛"
                )
                review_specs = [
                    (
                        agents[AgentRole.CIO],
                        "CIO部署与机会成本视角",
                        2.0,
                    ),
                    (
                        agents[AgentRole.RISK_OFFICER],
                        "风控事实反证视角",
                        1.5,
                    ),
                    (
                        agents[AgentRole.QUANT_RESEARCHER],
                        "量化可执行性视角",
                        1.0,
                    ),
                ]
                review_names = [item[1] for item in review_specs]
                review_results, review_audit = await collect_committee_results(
                    [
                        (
                            role_view,
                            agent.think(
                                task=build_deployment_deadlock_review_prompt(
                                    ticker,
                                    role_view,
                                    proposal,
                                    committee_decision,
                                    full_context,
                                ),
                                temperature=0.2,
                                max_tokens=vote_max_tokens,
                            ),
                        )
                        for agent, role_view, _weight in review_specs
                    ],
                    timeout_seconds=deadlock_review_timeout,
                    stage="deployment_deadlock_review",
                )
                review_retry_factories: List[
                    tuple[str, Callable[[], Awaitable[str]]]
                ] = []
                for (agent, role_view, _weight) in review_specs:
                    review_retry_factories.append(
                        (
                            role_view,
                            lambda agent=agent, role_view=role_view: agent.think(
                                task=build_deployment_deadlock_review_prompt(
                                    ticker,
                                    role_view,
                                    proposal,
                                    committee_decision,
                                    full_context,
                                ),
                                temperature=0.1,
                                max_tokens=vote_retry_max_tokens,
                            ),
                        )
                    )
                review_results, review_audit = (
                    await retry_absent_committee_results(
                        review_results,
                        review_audit,
                        review_retry_factories,
                        timeout_seconds=vote_retry_timeout,
                        stage="deployment_deadlock_review",
                    )
                )
                stage_execution_audit.append(review_audit)
                review_decision = aggregate_committee_decision(
                    proposal,
                    review_results,
                    vote_weights=[item[2] for item in review_specs],
                    vote_labels=review_names,
                )
                committee_decision = merge_committee_deadlock_review(
                    committee_decision,
                    review_decision,
                    min_confidence=deadlock_review_min_confidence,
                    min_direction_support=deadlock_review_min_support,
                )
                all_discussions.append(
                    f"\n{'='*50}\n【{ticker}】空仓部署死锁复核\n{'='*50}"
                )
                for name, result in zip(review_names, review_results):
                    all_discussions.append(f"\n[{name}]\n{result[:summary_chars]}")
                deadlock_audit = committee_decision.get("deadlock_review") or {}
                logger.info(
                    "Deployment deadlock review ticker=%s adopted=%s "
                    "direction=%s confidence=%.3f support=%.3f",
                    ticker,
                    deadlock_audit.get("adopted"),
                    deadlock_audit.get("review_direction"),
                    float(deadlock_audit.get("review_confidence") or 0.0),
                    float(deadlock_audit.get("review_direction_support") or 0.0),
                )
                if deadlock_audit.get("adopted"):
                    print(
                        f"      ✅ 死锁复核通过: {committee_decision.get('direction')} "
                        f"| confidence={float(committee_decision.get('confidence') or 0):.0%}"
                    )
                else:
                    print("      ➖ 死锁复核未通过，保留HOLD并记录具体缺口")
            expected_days = normalize_proposal_holding_period(proposal, topic)
            committee_decision.update({
                'decision_id': new_id("decision"),
                'round_id': round_id,
                'ticker': ticker,
                'thesis': thesis,
                'cio_vote': vote_results[0][:200],
                'review_depth': review_depth,
                'priority_score': priority_score,
                'target_price': proposal.get('take_profit', 15.0),
                'stop_loss': proposal.get('stop_loss', 5.0),
                'take_profit': proposal.get('take_profit', 15.0),
                'expected_days': expected_days,
                'holding_period_reason': proposal.get('holding_period_reason', ''),
                'sector': sector,
                'discussion_excerpt': full_context[:24000],
                'stage_execution_audit': stage_execution_audit,
            })
            # 记录到决策追踪器
            try:
                if committee_decision_is_predictable(committee_decision):
                    recorder = DecisionRecorder()
                    prediction_id = await recorder.record_decision(
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
                            f"individual_votes={committee_decision.get('individual_votes', [])}; "
                            f"risk_flags={committee_decision.get('risk_flags', [])}; "
                            f"evidence_gaps={committee_decision.get('evidence_gaps', [])}; "
                            f"reconsider_if={committee_decision.get('reconsider_if', [])}"
                        ),
                        expected_days=expected_days,
                        round_id=round_id,
                        decision_id=committee_decision["decision_id"],
                    )
                    committee_decision["prediction_id"] = prediction_id
                    if committee_decision.get("direction") == "hold":
                        print(
                            f"      📊 观望预测已记录（±5%中性带），"
                            f"{expected_days}天后验证错失机会/避损"
                        )
                    else:
                        print(f"      📊 决策已记录，{expected_days}天后验证")
                else:
                    print("      📊 投票未达法定人数，不写入可验证预测")
            except Exception as e:
                logger.warning(f"记录决策失败: {e}")

            if decision_callback is not None:
                try:
                    await decision_callback(committee_decision)
                except Exception as exc:
                    raise CommitteeDecisionPersistenceError(
                        f"committee decision persistence failed for {ticker}: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
            final_decisions.append(committee_decision)

            print(
                f"      ✅ 投票完成 "
                f"({vote_audit['completed_count']}/{vote_audit['task_count']}完成，"
                f"超时{vote_audit['timeout_count']}，错误{vote_audit['error_count']}；"
                f"有效票{committee_decision['parsed_vote_count']}/"
                f"{committee_decision['vote_count']})"
            )

        except asyncio.TimeoutError:
            print(f"      ⏰ 讨论超时，跳过")
            all_discussions.append(f"\n【{ticker}】讨论超时")
        except asyncio.CancelledError:
            print(f"      ⚠️ 讨论被取消")
            all_discussions.append(f"\n【{ticker}】讨论被取消")
            raise  # 重新抛出 CancelledError，让上层处理
        except CommitteeDecisionPersistenceError:
            raise
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
async def stage4_final_conclusion(
    llm,
    discussions: str,
    decisions: List[Dict],
    topic: str,
    memory_prompt: str = "",
) -> Dict:
    """阶段4：生成综合结论并结构化"""
    from sovereign_hall.core.config import get_config

    research_config = get_config().get("research", {})
    conclusion_context_chars = int(research_config.get("conclusion_discussion_context_chars", 24000) or 24000)

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

历史记忆与已验证结果（旧结论不是当前事实）：
{memory_prompt[:8000] if memory_prompt else "无"}

讨论内容：
{discussions[:conclusion_context_chars]}

请输出简洁的结构化结论。不要复述讨论过程，只写最终可执行判断：
## 核心判断
[买/卖/观望] [标的] | 置信度: XX%

## 关键逻辑（3条）
1. 已验证事实：
2. 核心推断：
3. 相对历史结论是维持、修正还是推翻，以及触发/否决条件：

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


async def run_committee_approved_simulation(
    simulation,
    market_data,
    llm,
    decisions: List[Dict],
    initial_rejections: Optional[List[Dict[str, str]]] = None,
    round_id: str | None = None,
    round_coordinator=None,
    pre_reviewed_positions: Optional[List[Dict[str, Any]]] = None,
):
    """Run the daily simulated portfolio step after committee decisions exist."""
    from sovereign_hall.services.portfolio_policy import (
        deployment_position_floor as calculate_deployment_position_floor,
    )
    from sovereign_hall.services.reward_policy import MAX_DAILY_TRADES
    trading_day = await market_data.is_trading_day()
    market_open = bool(trading_day)
    if trading_day and hasattr(market_data, "is_market_open"):
        market_open = bool(await market_data.is_market_open())
    market_session_open = bool(trading_day and market_open)

    if market_session_open:
        print(f"\n💰 根据投委会裁决执行每日投资模拟...")
    else:
        print("\n💰 当前非交易时段：继续逐仓复核、持久化投委会结果并形成待执行裁决，不模拟成交")
    history_reflection = await simulation.get_recent_reflection(limit=2)
    assets = await simulation.calculate_assets()
    performance = (
        await simulation.get_performance_metrics()
        if hasattr(simulation, "get_performance_metrics")
        else {}
    )
    logger.info(
        "SIMULATION_PIPELINE_BEGIN decisions=%s trading_day=%s market_open=%s "
        "market_session_open=%s valuation_complete=%s invested_ratio=%s "
        "net_total_return=%s health=%s",
        len(decisions),
        trading_day,
        market_open,
        market_session_open,
        assets.get("valuation_complete"),
        assets.get("invested_ratio"),
        performance.get("net_total_return"),
        performance.get("health_status"),
    )
    heuristic_context = load_latest_heuristic_context()
    if assets.get("valuation_complete"):
        print(f"   当前资产: {assets['total_assets']:.2f}元 | 现金: {assets['cash']:.2f}元 | 持仓: {assets['positions_value']:.2f}元（实时现价）")
    else:
        print(
            "   当前资产: N/A | 现金: "
            f"{assets['cash']:.2f}元 | 缺少实时现价: "
            f"{', '.join(assets.get('missing_price_tickers', []))}"
        )
    if heuristic_context.available:
        live_return = performance.get("net_total_return")
        live_return_text = (
            "N/A" if live_return is None else f"{float(live_return):+.2%}"
        )
        print(
            f"   模拟账户唯一绩效: 累计净收益={live_return_text} | "
            f"health={performance.get('health_status', 'valuation_incomplete')} | "
            f"当前执行安全策略={heuristic_context.policy_name} | "
            f"单标的上限{heuristic_context.max_position:.0%}"
        )
        print("   离线回测仅作诊断，不产生best/score，也不能替代模拟成交")

    print("   🔎 先执行全部现有持仓的强制生命周期复核...")
    position_reviews = (
        list(pre_reviewed_positions)
        if pre_reviewed_positions is not None
        else await simulation.review_open_positions()
    )
    logger.info(
        "SIMULATION_LIFECYCLE_REVIEW positions=%s outcomes=%s",
        len(position_reviews),
        {
            action: sum(
                str(item.get("action") or "unknown") == action
                for item in position_reviews
            )
            for action in sorted({
                str(item.get("action") or "unknown")
                for item in position_reviews
            })
        },
    )
    for review in position_reviews:
        action = review.get("action", "unknown")
        ticker = review.get("ticker", "")
        reason = review.get("reason", "")
        if action == "exit":
            execution = review.get("execution") or {}
            if execution.get("action") == "sell":
                print(f"   📉 风控退出 {ticker}: {reason}")
            else:
                print(f"   ⚠️ {ticker} 触发退出但未成交: {execution.get('reason', reason)}")
        elif action.startswith("blocked_"):
            print(f"   ⛔ {ticker} 复核阻塞: {reason}")
        else:
            pnl = review.get("pnl_pct")
            pnl_text = "N/A" if pnl is None else f"{float(pnl):.1%}"
            print(f"   ➖ {ticker} 复核持有: PnL={pnl_text}；{reason}")

    # Lifecycle exits belong to the active round because their price-free
    # intents were created during its mandatory pre-research review.  Count
    # only newly committed fills; a duplicate idempotent replay is not a fill.
    lifecycle_fill_count = sum(
        1
        for review in position_reviews
        if str((review.get("execution") or {}).get("action") or "")
        in {"buy", "sell"}
        and (review.get("execution") or {}).get("success") is not False
    )

    pending_replay = (
        await simulation.replay_pending_decisions()
        if hasattr(simulation, "replay_pending_decisions")
        else {"status": "not_supported", "pending_before": 0}
    )
    if pending_replay.get("pending_before"):
        print(
            "   🗂️ 待执行裁决: "
            f"状态={pending_replay.get('status')}；"
            f"尝试={pending_replay.get('attempted', 0)}，"
            f"成交={pending_replay.get('executed', 0)}，"
            f"拒绝={pending_replay.get('rejected', 0)}，"
            f"过期={pending_replay.get('expired', 0)}，"
            f"剩余={pending_replay.get('remaining', 0)}"
        )
        for replayed in pending_replay.get("results", []):
            print(
                f"      - #{replayed.get('id')} {replayed.get('ticker', '')}: "
                f"{replayed.get('action', 'unknown')}；{replayed.get('reason', '')}"
            )
    replay_fill_count = int(pending_replay.get("executed") or 0)
    if lifecycle_fill_count or replay_fill_count:
        logger.info(
            "SIMULATION_CYCLE_EXISTING_INTENT_FILLS lifecycle_fills=%s "
            "replay_fills=%s cycle_fills_before_new_candidates=%s",
            lifecycle_fill_count,
            replay_fill_count,
            lifecycle_fill_count + replay_fill_count,
        )

    if round_coordinator and round_id:
        from sovereign_hall.domain.research import ResearchRoundStatus

        current_round = await round_coordinator.get(round_id)
        if (
            current_round
            and current_round.status == ResearchRoundStatus.PREDICTIONS_RECORDED
        ):
            await round_coordinator.advance(
                round_id,
                ResearchRoundStatus.PORTFOLIO_REVIEWED,
                event_type="PortfolioLifecycleReviewed",
                payload={"position_count": len(position_reviews)},
            )

    assets = await simulation.calculate_assets()
    if assets.get("valuation_complete"):
        print(
            f"   资金部署: 已投资{assets['invested_ratio']:.1%} / "
            f"目标{assets.get('target_invested_ratio', 1.0):.1%}，"
            f"待部署{assets['deployment_gap']:.2f}元"
        )
    else:
        print("   资金部署: N/A；组合实时估值不完整，禁止新增或扩大仓位")
    if assets.get('deployment_gap') is not None and assets['deployment_gap'] > 0:
        print("   规则: 待部署现金不是风险储备；仅因缺少合格标的、新鲜价格、手续费或整手约束暂时未成交")

    current_positions = assets.get('positions', {})
    current_tickers = set(current_positions.keys())
    current_ticker_codes = {simulation._normalize_ticker(ticker) for ticker in current_tickers}
    if hasattr(simulation, "record_committee_outcomes"):
        outcome_kwargs = {"source": "run_discussion"}
        if round_id is not None:
            outcome_kwargs["round_id"] = round_id
        await simulation.record_committee_outcomes(decisions, **outcome_kwargs)
    max_daily_trades = int(getattr(simulation, "max_daily_trades", MAX_DAILY_TRADES))
    prior_trade_count = (
        await simulation.count_trades_on_date()
        if hasattr(simulation, "count_trades_on_date")
        else 0
    )
    trade_count = 0
    print(f"   交易频率纪律: 今日已成交{prior_trade_count}/{max_daily_trades}笔；硬上限不允许绕过")
    redeployment_blockers: List[str] = []
    trade_candidates, redeployment_rejections = preflight_committee_decisions(
        decisions,
        current_tickers,
        simulation._normalize_ticker,
    )
    if initial_rejections:
        redeployment_rejections = list(initial_rejections) + redeployment_rejections
    intent_stage_recorded = False

    async def note_execution_intent(intent_id: str | None) -> None:
        nonlocal intent_stage_recorded
        if (
            not intent_id
            or intent_stage_recorded
            or not round_coordinator
            or not round_id
        ):
            return
        from sovereign_hall.domain.research import ResearchRoundStatus

        current_round = await round_coordinator.get(round_id)
        if (
            current_round
            and current_round.status == ResearchRoundStatus.PORTFOLIO_REVIEWED
        ):
            await round_coordinator.advance(
                round_id,
                ResearchRoundStatus.EXECUTION_INTENTS_CREATED,
                event_type="FirstExecutionIntentPersisted",
                payload={"intent_id": intent_id},
            )
        intent_stage_recorded = True

    async def persist_rejected_intent(
        decision: Dict[str, Any],
        *,
        ticker: str,
        direction: str,
        target_position: float,
        confidence: float,
        code: str,
        reason: str,
    ) -> None:
        if not hasattr(simulation, "create_execution_intent"):
            return
        intent_id = await simulation.create_execution_intent(
            ticker=ticker,
            direction=direction,
            target_position=target_position,
            confidence=confidence,
            reason=reason,
            round_id=round_id,
            decision_id=decision.get("decision_id"),
            priority=50 if direction in {"sell", "short"} else 100,
            idempotency_key=(
                f"{round_id or 'standalone'}:"
                f"{decision.get('decision_id') or ticker}:"
                f"{direction}"
            ),
        )
        await note_execution_intent(intent_id)
        if intent_id and hasattr(simulation, "reject_execution_intent"):
            await simulation.reject_execution_intent(
                intent_id,
                code=code,
                reason=reason,
            )

    def reject(code: str, reason: str, ticker: str = "") -> None:
        item = {"code": code, "ticker": ticker, "reason": reason}
        redeployment_rejections.append(item)
        label = f"{ticker}: " if ticker else ""
        redeployment_blockers.append(f"[{code}] {label}{reason}")
        logger.warning(
            "SIMULATION_TERMINAL_REJECTION code=%s ticker=%s reason=%s",
            code,
            ticker or "-",
            str(reason or "")[:1000],
        )

    if redeployment_rejections:
        counts: Dict[str, int] = {}
        for item in redeployment_rejections:
            code = item["code"]
            counts[code] = counts.get(code, 0) + 1
            label = f"{item.get('ticker')}: " if item.get("ticker") else ""
            redeployment_blockers.append(f"[{code}] {label}{item.get('reason', '')}")
        print(
            "   🧪 投委会裁决预检否决: "
            + ", ".join(f"{code}={count}" for code, count in sorted(counts.items()))
        )
        logger.warning(
            "SIMULATION_PREFLIGHT_REJECTIONS candidates=%s counts=%s",
            len(trade_candidates),
            json.dumps(counts, ensure_ascii=False, sort_keys=True),
        )
    else:
        logger.info(
            "SIMULATION_PREFLIGHT_REJECTIONS candidates=%s counts={}",
            len(trade_candidates),
        )
    deployable_new_longs = [
        decision for decision in trade_candidates
        if decision.get("direction") == "long"
        and decision.get("ticker")
        and simulation._normalize_ticker(decision.get("ticker")) not in current_ticker_codes
    ]
    deployment_position_floor = 0.0
    if deployable_new_longs and assets.get('total_assets') is not None and assets['total_assets'] > 0:
        deployment_position_floor = calculate_deployment_position_floor(
            assets.get('deployment_gap', 0.0),
            assets['total_assets'],
            len(deployable_new_longs),
        )

    if not market_session_open:
        pending_count = 0
        for decision in trade_candidates[:5]:
            ticker = simulation._normalize_ticker(decision.get("ticker"))
            direction = str(decision.get("direction") or "hold").lower()
            target_position = float(decision.get("target_position") or 0.0)
            confidence = float(decision.get("confidence") or 0.0)
            if direction == "long" and not assets.get("valuation_complete"):
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=direction,
                    target_position=target_position,
                    confidence=confidence,
                    code="valuation_incomplete",
                    reason="组合实时估值不完整，禁止形成新增/扩仓待执行裁决",
                )
                reject(
                    "valuation_incomplete",
                    "组合实时估值不完整，禁止形成新增/扩仓待执行裁决",
                    ticker,
                )
                continue
            if (
                direction == "long"
                and ticker not in current_ticker_codes
                and deployment_position_floor > target_position
            ):
                target_position = deployment_position_floor

            # A closed-session row is a deferred ruling, not a bypass around
            # the same price-independent heuristic gates used while the market
            # is open. Replay still fetches a fresh quote and re-runs every
            # execution gate before it can create a simulated fill.
            cap_reason = ""
            if direction == "long":
                total_assets_for_cap = float(assets.get("total_assets") or 0.0)
                position_values = assets.get("position_values") or {}
                current_position_value = float(position_values.get(ticker, 0.0) or 0.0)
                current_position_pct = (
                    current_position_value / total_assets_for_cap
                    if total_assets_for_cap > 0
                    else 0.0
                )
                current_gross_exposure = (
                    sum(float(value or 0.0) for value in position_values.values())
                    / total_assets_for_cap
                    if total_assets_for_cap > 0
                    else 0.0
                )
                signal_count = recent_prediction_observation_count(ticker)
                target_position, cap_reason = apply_heuristic_risk_cap(
                    ticker,
                    target_position,
                    confidence,
                    signal_count=signal_count,
                    current_position=current_position_pct,
                    current_gross_exposure=current_gross_exposure,
                    fresh_local_evidence=True,
                    context=heuristic_context,
                )
                if target_position <= current_position_pct:
                    await persist_rejected_intent(
                        decision,
                        ticker=ticker,
                        direction=direction,
                        target_position=target_position,
                        confidence=confidence,
                        code="heuristic_entry_veto",
                        reason=(
                            cap_reason
                            or "heuristic风控未允许形成新增或扩仓待执行裁决"
                        ),
                    )
                    reject(
                        "heuristic_entry_veto",
                        cap_reason or "heuristic风控未允许形成新增或扩仓待执行裁决",
                        ticker,
                    )
                    print(
                        f"   ⛔ {ticker}: heuristic预检否决，不形成待执行裁决；"
                        f"{cap_reason or '目标仓位未增加'}"
                    )
                    continue

            pending_reason = (
                "投委会已通过heuristic预检；非交易时段不成交，"
                "下一交易时段重新取实时行情并重过全部风控"
            )
            if cap_reason:
                pending_reason += f"；{cap_reason}"
            if hasattr(simulation, "create_execution_intent"):
                intent_id = await simulation.create_execution_intent(
                    ticker=ticker,
                    direction=direction,
                    target_position=target_position,
                    confidence=confidence,
                    reason=pending_reason,
                    round_id=round_id,
                    decision_id=decision.get("decision_id"),
                    priority=50 if direction in {"sell", "short"} else 100,
                    idempotency_key=(
                        f"{round_id or 'standalone'}:"
                        f"{decision.get('decision_id') or ticker}:"
                        f"{direction}"
                    ),
                )
                pending_result = (
                    await simulation.execute_intent(intent_id, llm=llm)
                    if intent_id
                    else {
                        "success": False,
                        "action": "error",
                        "reason": "执行裁决持久化失败",
                    }
                )
                await note_execution_intent(intent_id)
            else:
                # Compatibility seam for tests/third-party adapters. Production
                # InvestmentSimulation always uses the durable intent path.
                pending_id = await simulation.record_pending_decision(
                    ticker=ticker,
                    direction=direction,
                    target_position=target_position,
                    confidence=confidence,
                    reason=pending_reason,
                    defer_code=(
                        "non_trading_day" if not trading_day else "market_closed"
                    ),
                )
                pending_result = {
                    "success": False,
                    "action": "pending",
                    "pending_decision_id": pending_id,
                }
            pending_id = pending_result.get("pending_decision_id")
            if pending_id is not None:
                pending_count += 1
                print(f"   🗂️ {ticker}: 裁决 #{pending_id} 已排队，未记录成交价")
                logger.info(
                    "SIMULATION_PENDING_DECISION id=%s ticker=%s direction=%s "
                    "target_position=%.6f confidence=%.6f defer_code=%s",
                    pending_id,
                    ticker,
                    direction,
                    target_position,
                    confidence,
                    "non_trading_day" if not trading_day else "market_closed",
                )

        closed_reason = (
            "当前非交易日；投委会结果与逐仓复核已持久化，可执行裁决仅排队"
            if not trading_day
            else "当前不在A股交易时段；投委会结果与逐仓复核已持久化，可执行裁决仅排队"
        )
        redeployment_blockers.append(closed_reason)
        final_assets = await simulation.calculate_assets()
        if hasattr(simulation, "record_redeployment_attempt"):
            state = await simulation.record_redeployment_attempt(
                final_assets,
                candidate_count=len(deployable_new_longs),
                trade_count=0,
                pending_count=pending_count,
                blockers=redeployment_blockers,
                rejections=redeployment_rejections,
            )
            if state and final_assets.get("deployment_gap"):
                print(
                    f"   🧾 再配置队列: {state.get('status')} | "
                    f"gap={state.get('deployment_gap')} | blocker={state.get('blocker_code')}"
                )
        reflection = await simulation.daily_reflection(llm)
        if reflection:
            print(f"\n📝 每日投资反思:")
            print(reflection[:500] + "...")
        await simulation.save_snapshot(reflection, round_id=round_id)
        logger.info(
            "SIMULATION_PIPELINE_END fills=0 pending=%s candidates=%s "
            "valuation_complete=%s invested_ratio=%s terminal=market_closed",
            pending_count,
            len(trade_candidates),
            final_assets.get("valuation_complete"),
            final_assets.get("invested_ratio"),
        )
        closed_terminal = select_simulation_terminal(
            round_fill_count=0,
            pending_count=pending_count,
            trade_candidates=trade_candidates,
            decisions=decisions,
            rejections=redeployment_rejections,
        )
        return {
            "terminal": closed_terminal,
            "fills": 0,
            "replay_fills": 0,
            "cycle_fills": 0,
            "pending": pending_count,
            "candidate_count": len(trade_candidates),
            "rejections": redeployment_rejections,
            "assets": final_assets,
            "performance": performance,
        }

    if trade_candidates:
        bounded_candidates = trade_candidates[:5]
        for decision_index, decision in enumerate(bounded_candidates):
            ticker = simulation._normalize_ticker(decision.get('ticker'))
            if not ticker:
                continue
            if prior_trade_count + trade_count >= max_daily_trades:
                blocker = f"今日已达持久化最大交易次数 ({max_daily_trades}次)"
                reject("daily_trade_limit", blocker)
                for deferred in bounded_candidates[decision_index:]:
                    deferred_ticker = simulation._normalize_ticker(deferred.get("ticker"))
                    if not deferred_ticker:
                        continue
                    if hasattr(simulation, "create_execution_intent"):
                        deferred_intent_id = await simulation.create_execution_intent(
                            ticker=deferred_ticker,
                            direction=deferred.get("direction", "hold"),
                            target_position=float(deferred.get("target_position", 0.0)),
                            confidence=float(deferred.get("confidence", 0.5)),
                            reason="投委会裁决超过当日5笔共享硬门",
                            round_id=round_id,
                            decision_id=deferred.get("decision_id"),
                            priority=(
                                50
                                if str(deferred.get("direction") or "").lower()
                                in {"sell", "short"}
                                else 100
                            ),
                            idempotency_key=(
                                f"{round_id or 'standalone'}:"
                                f"{deferred.get('decision_id') or deferred_ticker}:"
                                f"{str(deferred.get('direction') or 'hold').lower()}"
                            ),
                        )
                        if deferred_intent_id:
                            await note_execution_intent(deferred_intent_id)
                            await simulation.execute_intent(deferred_intent_id, llm=llm)
                    elif hasattr(simulation, "record_pending_decision"):
                        await simulation.record_pending_decision(
                            ticker=deferred_ticker,
                            direction=deferred.get("direction", "hold"),
                            target_position=float(
                                deferred.get("target_position", 0.0)
                            ),
                            confidence=float(deferred.get("confidence", 0.5)),
                            reason="投委会裁决超过当日5笔共享硬门",
                            defer_code="daily_trade_limit",
                        )
                print(f"   ⏹️ {blocker}，剩余裁决已记录到下一交易时段")
                break
            if simulation.is_in_cooldown(ticker):
                direction = str(decision.get("direction") or "hold").lower()
                confidence = float(decision.get("confidence") or 0.0)
                target_position = float(decision.get("target_position") or 0.0)
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=direction,
                    target_position=target_position,
                    confidence=confidence,
                    code="cooldown",
                    reason="同一标的仍在交易冷却期",
                )
                reject("cooldown", "同一标的仍在交易冷却期", ticker)
                print(f"   ⏳ {ticker} 在冷却期内，跳过交易")
                continue

            direction = decision.get('direction', 'hold')
            confidence = float(decision.get('confidence', 0.5))
            target_position = float(decision.get('target_position', 0.0))
            current_price, price_source = await simulation.resolve_trade_price(ticker)
            if current_price is None:
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=str(direction).lower(),
                    target_position=target_position,
                    confidence=confidence,
                    code="realtime_quote_unavailable",
                    reason="实时行情不可用",
                )
                reject("realtime_quote_unavailable", "实时行情不可用", ticker)
                print(f"   ⏭️ {ticker}: 无法获取实时现价，跳过模拟交易")
                continue

            has_position = ticker in current_ticker_codes
            if direction == "long" and not has_position and deployment_position_floor > target_position:
                target_position = deployment_position_floor
                print(
                    f"   🎯 {ticker}: 为完成100%资金部署，将候选目标仓位提高到"
                    f"{target_position:.1%}，随后仍接受单标的/证据风控约束"
                )
            if "卖出" in history_reflection and has_position and direction == "long":
                trade_position = target_position * 0.3
                trade_reason = "反思建议谨慎，小幅建仓"
            elif has_position and direction == "long":
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=str(direction).lower(),
                    target_position=target_position,
                    confidence=confidence,
                    code="no_position_change",
                    reason="已有持仓，重复买入裁决没有形成仓位变化",
                )
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
            if price_source:
                trade_reason = f"{trade_reason}；价格来源={price_source}"

            signal_count = recent_prediction_observation_count(ticker)
            # Revalue the whole account from realtime quotes before sizing.  This
            # helper is async and also returns the set of missing quotes; ignoring
            # either detail used to crash the post-exit redeployment path before a
            # committee-approved candidate could reach execute_trade().
            (
                position_values,
                total_assets_for_cap,
                missing_price_tickers,
            ) = await simulation._estimate_trade_assets(ticker, current_price)
            if missing_price_tickers and direction == "long":
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=str(direction).lower(),
                    target_position=trade_position,
                    confidence=confidence,
                    code="valuation_incomplete",
                    reason=(
                        "组合实时估值不完整("
                        + ",".join(missing_price_tickers)
                        + ")"
                    ),
                )
                reject(
                    "valuation_incomplete",
                    f"组合实时估值不完整({','.join(missing_price_tickers)})",
                    ticker,
                )
                print(
                    f"   ⏭️ {ticker}: 组合实时估值不完整，拒绝新增/扩大模拟仓位；"
                    f"缺少实时现价: {', '.join(missing_price_tickers)}"
                )
                continue
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
                fresh_local_evidence=True,
                context=heuristic_context,
            )
            if cap_reason:
                trade_reason = f"{trade_reason}；{cap_reason}"
            trade_position = capped_position
            if direction == "long" and trade_position <= current_position_pct:
                await persist_rejected_intent(
                    decision,
                    ticker=ticker,
                    direction=str(direction).lower(),
                    target_position=trade_position,
                    confidence=confidence,
                    code="heuristic_entry_veto",
                    reason=cap_reason or "heuristic风控未允许新增或扩大仓位",
                )
                reject(
                    "heuristic_entry_veto",
                    cap_reason or "heuristic风控未允许新增或扩大仓位",
                    ticker,
                )
                print(f"   ⛔ {ticker}: heuristic入场否决；{cap_reason or '目标仓位未增加'}")
                continue

            if hasattr(simulation, "create_execution_intent"):
                intent_id = await simulation.create_execution_intent(
                    ticker=ticker,
                    direction=direction,
                    target_position=trade_position,
                    confidence=confidence,
                    reason=trade_reason,
                    round_id=round_id,
                    decision_id=decision.get("decision_id"),
                    priority=50 if str(direction).lower() in {"sell", "short"} else 100,
                    idempotency_key=(
                        f"{round_id or 'standalone'}:"
                        f"{decision.get('decision_id') or ticker}:"
                        f"{str(direction).lower()}"
                    ),
                )
                result = (
                    await simulation.execute_intent(intent_id, llm=llm)
                    if intent_id
                    else {
                        "success": False,
                        "action": "error",
                        "reason": "执行裁决持久化失败",
                    }
                )
                await note_execution_intent(intent_id)
            else:
                result = await simulation.execute_trade(
                    ticker=ticker,
                    direction=direction,
                    target_position=trade_position,
                    current_price=current_price,
                    llm=llm,
                    reason=trade_reason,
                )

            if result.get('success') is False:
                reason = result.get('reason', '交易失败')
                result_blocker_code = str(result.get("blocker_code") or "")
                if result_blocker_code:
                    code = result_blocker_code
                elif "硬上限" in reason:
                    code = "daily_trade_limit"
                elif "交易时段" in reason or "非交易日" in reason:
                    code = "market_closed"
                elif "实时现价" in reason:
                    code = "realtime_quote_unavailable"
                elif "估值不完整" in reason:
                    code = "valuation_incomplete"
                elif "冷却期" in reason:
                    code = "cooldown"
                else:
                    code = "execution_failed"
                reject(code, reason, ticker)
                print(f"   ⏭️ {ticker}: {result.get('reason', '交易失败')}")
            elif result.get('action') == 'buy':
                print(f"   📈 买入 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                trade_count += 1
                logger.info(
                    "SIMULATION_FILL action=buy ticker=%s shares=%s price=%.6f "
                    "source=%s reason=%s",
                    ticker,
                    result.get("shares"),
                    float(result.get("price") or 0.0),
                    result.get("price_source") or price_source or "realtime_quote",
                    str(trade_reason)[:1000],
                )
            elif result.get('action') == 'sell':
                print(f"   📉 卖出 {ticker} {result['shares']}股 @ {result['price']:.2f} ({trade_reason})")
                trade_count += 1
                logger.info(
                    "SIMULATION_FILL action=sell ticker=%s shares=%s price=%.6f "
                    "source=%s reason=%s",
                    ticker,
                    result.get("shares"),
                    float(result.get("price") or 0.0),
                    result.get("price_source") or price_source or "realtime_quote",
                    str(trade_reason)[:1000],
                )
            elif result.get('action') == 'hold' and result.get('reason'):
                reason = result.get('reason')
                code = str(result.get("blocker_code") or "")
                if not code:
                    code = "lot_size_or_cash" if any(
                        marker in reason for marker in ("一手", "资金不足", "数量不足")
                    ) else "no_position_change"
                reject(code, reason, ticker)
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
        if not redeployment_rejections:
            reject(
                "system_failure_no_live_deployment",
                "模拟账户空仓且投委会没有返回任何结构化裁决；研究到成交链路未闭合",
            )
        print(
            "   ❌ 系统异常：模拟账户空仓且无可执行裁决；"
            "本轮未产生成交，不能解释为正常观望或用离线收益覆盖"
        )
        logger.error(
            "SIMULATION_PIPELINE_FAILURE code=system_failure_no_live_deployment "
            "decisions=%s candidates=%s rejections=%s",
            len(decisions),
            len(trade_candidates),
            json.dumps(redeployment_rejections, ensure_ascii=False)[:4000],
        )

    final_assets = await simulation.calculate_assets()
    round_fill_count = lifecycle_fill_count + trade_count
    cycle_fill_count = round_fill_count + replay_fill_count
    if hasattr(simulation, "record_redeployment_attempt"):
        state = await simulation.record_redeployment_attempt(
            final_assets,
            candidate_count=len(deployable_new_longs),
            trade_count=cycle_fill_count,
            blockers=redeployment_blockers,
            rejections=redeployment_rejections,
        )
        if state and final_assets.get("deployment_gap"):
            print(
                f"   🧾 再配置队列: {state.get('status')} | "
                f"gap={state.get('deployment_gap')} | blocker={state.get('blocker_code')}"
            )
    if final_assets.get("valuation_complete"):
        print(f"   📊 交易后: 现金 {final_assets['cash']:.2f}元 | 持仓 {final_assets['positions_value']:.2f}元（实时现价）")
    else:
        print(
            f"   📊 交易后: 现金 {final_assets['cash']:.2f}元 | 当前资产N/A；"
            f"缺少实时现价: {', '.join(final_assets.get('missing_price_tickers', []))}"
        )
    final_performance = (
        await simulation.get_performance_metrics()
        if hasattr(simulation, "get_performance_metrics")
        else {}
    )
    final_return = final_performance.get("net_total_return")
    final_return_text = (
        "N/A" if final_return is None else f"{float(final_return):+.2%}"
    )
    print(
        "   唯一绩效结算: "
        f"模拟账户累计净收益={final_return_text} | "
        f"本轮新增成交={cycle_fill_count} "
        f"(当前round={round_fill_count}, pending replay={replay_fill_count}) | "
        f"health={final_performance.get('health_status', 'valuation_incomplete')}"
    )
    if cycle_fill_count == 0:
        print("   本轮绩效改进=N/A（没有新增模拟成交，不允许声明策略收益改进）")
    else:
        print(
            "   本轮有可审计新增成交；改进值必须由本次实时score"
            "相对前次实时计量计算，不由成交数、代码或离线结果证明"
        )
    logger.info(
        "SIMULATION_PIPELINE_END fills=%s replay_fills=%s cycle_fills=%s "
        "candidates=%s rejections=%s "
        "valuation_complete=%s invested_ratio=%s net_total_return=%s health=%s",
        round_fill_count,
        replay_fill_count,
        cycle_fill_count,
        len(trade_candidates),
        len(redeployment_rejections),
        final_assets.get("valuation_complete"),
        final_assets.get("invested_ratio"),
        final_performance.get("net_total_return"),
        final_performance.get("health_status"),
    )

    reflection = await simulation.daily_reflection(llm)
    if reflection:
        print(f"\n📝 每日投资反思:")
        print(reflection[:500] + "...")
    await simulation.save_snapshot(reflection, round_id=round_id)
    terminal = select_simulation_terminal(
        round_fill_count=round_fill_count,
        pending_count=0,
        trade_candidates=trade_candidates,
        decisions=decisions,
        rejections=redeployment_rejections,
    )
    return {
        "terminal": terminal,
        # ``fills`` remains the active round's fill count so a replay keeps
        # its originating round_id. ``cycle_fills`` is the operational count
        # for this invocation and includes fills from deferred-intent replay.
        "fills": round_fill_count,
        "replay_fills": replay_fill_count,
        "cycle_fills": cycle_fill_count,
        "pending": 0,
        "candidate_count": len(trade_candidates),
        "rejections": redeployment_rejections,
        "assets": final_assets,
        "performance": final_performance,
    }


def select_simulation_terminal(
    *,
    round_fill_count: int,
    pending_count: int,
    trade_candidates: List[Dict[str, Any]],
    decisions: List[Dict[str, Any]],
    rejections: List[Dict[str, Any]],
) -> str:
    """Choose one canonical terminal from durable pipeline facts.

    Candidate evidence that reached a repeated-candidate, lot-feasibility or
    execution hard gate must remain distinguishable from a stage-2 response
    that contained no usable evidence at all.
    """
    if round_fill_count > 0:
        return "filled"
    if pending_count > 0:
        return "market_closed_pending"
    candidate_screen_rejection_codes = {
        "proposal_lot_infeasible",
        "repeated_candidate_cooldown",
    }
    has_candidate_screen_rejection = any(
        str(item.get("code") or "") in candidate_screen_rejection_codes
        for item in rejections
    )
    if trade_candidates or has_candidate_screen_rejection:
        return "execution_rejected"
    if any(
        str(item.get("direction") or "hold").lower()
        in {"long", "buy", "short", "sell"}
        for item in decisions
    ):
        return "execution_rejected"
    if decisions:
        return "committee_hold"
    return "no_evidence"


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
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.domain.research import ResearchRoundStatus

    args = parse_args()

    print("\n" + "="*60)
    print("🔥 Sovereign Hall - 无限 Token 焚化炉")
    print("="*60)
    print("设计目标：")
    print("  - 预设议题池，循环研究")
    print("  - 本地知识库 + 多轮辩论" if args.local_only else "  - 高并发搜索 + 多轮辩论")
    print("  - 结构化存储结论")
    print("  - 0.1秒间隔，持续燃烧Token")
    print("="*60 + "\n")

    # ========== 加载历史统计 ==========
    from sovereign_hall.services.persistence import get_persistence
    persistence = get_persistence()
    prev_stats = persistence.load_previous_stats()
    if prev_stats and prev_stats.get('total_tokens', 0) > 0:
        print(f"📊 历史累计统计:")
        print(f"   - 累计Token: {format_token_breakdown(prev_stats)}")
        print(f"   - 累计成本: {format_cost_breakdown(prev_stats)}")
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
    research_config = config.get("research", {})
    daily_budget = DailyTokenBudget(
        TOKEN_BUDGET_FILE,
        budget=system_config.get("daily_token_budget"),
    )
    daily_budget_pause = int(system_config.get("daily_budget_pause_seconds", 3600) or 3600)
    validation_batch_size = int(system_config.get("validation_batch_size", 100) or 100)
    topic_cooldown_hours = int(system_config.get("topic_cooldown_hours", DEFAULT_TOPIC_COOLDOWN_HOURS) or 0)
    search_query_count = int(research_config.get("search_query_count", 30) or 30)
    wiki_ingest_max_docs = int(
        research_config.get(
            "wiki_ingest_max_docs_per_round",
            research_config.get("stage2_max_docs", 30),
        )
        or 0
    )
    force_search_interval = int(research_config.get("force_search_interval", 1) or 0)
    if args.local_only:
        force_search_interval = 0

    llm = LLMClient(
        max_concurrent=int(llm_config.get('max_concurrent', 12)),
        model=llm_config.get('model'),
        provider=llm_config.get('provider'),
    )
    # 从配置中读取 Spider 并发数（已降低防止被封）
    spider_config = config.get_spider_config()
    spiders = SpiderSwarm(
        max_concurrent=spider_config.get('max_concurrent', 10),
        network_enabled=not args.local_only,
    )

    if not args.skip_preflight:
        preflight_ok = await run_startup_preflight(
            llm,
            spiders,
            check_search=not args.local_only,
        )
        if not preflight_ok:
            await spiders.close()
            await llm.close()
            raise RuntimeError("启动前联通性检查未通过")
    else:
        print("⚠️ 已跳过启动前 LLM/Embedding/搜索联通性检查")
    if args.local_only:
        print("🛡️ local-only 已生效：资料网络搜索被 SpiderSwarm 共享硬门禁止")

    db_service = DatabaseService(str(db_path))
    await db_service._init_db()
    await db_service.init_report_tables()
    round_coordinator = ResearchRoundCoordinator(db_service)
    recovered_rounds = await round_coordinator.recover_abandoned_rounds()
    if recovered_rounds:
        logger.warning(
            "Recovered %s abandoned research round(s) before production resumed: %s",
            len(recovered_rounds),
            [item["round_id"] for item in recovered_rounds],
        )
        print(
            "   ♻️ 已原子收敛 "
            f"{len(recovered_rounds)} 个上一进程遗留的无终态研究轮"
        )
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
                    f"今日Token预算已用尽: {format_token(used)}/{format_token(daily_budget.budget)}，暂停{daily_budget_pause}秒"
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
            base_topic = select_next_topic(completed_topics, recent_topics=recent_topics)
            if base_topic is None:
                wait_seconds = min(3600, max(300, topic_cooldown_hours * 60))
                print(f"\n💤 所有议题都在 {topic_cooldown_hours} 小时冷却期内，休息 {wait_seconds} 秒")
                await asyncio.sleep(wait_seconds)
                continue
            try:
                research_assets = await simulation.calculate_assets()
                redeployment_state = await simulation.get_redeployment_state()
                topic = prioritize_deployment_research(
                    base_topic,
                    research_assets,
                    redeployment_state,
                )
            except Exception as exc:
                logger.warning("读取资金部署状态失败，保持原研究议题: %s", exc)
                topic = base_topic

            iteration += 1
            docs = []
            proposals = []
            round_start = datetime.now()
            round_start_stats = llm.get_stats()
            round_record = await round_coordinator.start(
                base_topic=base_topic,
                research_objective=topic,
                prompt_version="run_discussion_canonical_v1",
            )
            active_round_id = round_record.id
            logger.info(f"🔥 第 {iteration} 轮开始 | 议题: {topic}")
            logger.info("RESEARCH_ROUND_STARTED round_id=%s", active_round_id)
            print(f"\n{'='*60}")
            print(f"🔥 第 {iteration} 轮 | 议题: {topic}")
            print(f"{'='*60}")
            if topic != base_topic:
                print(
                    "🎯 资金部署优先：当前存在重大操作性现金缺口，"
                    "本轮先比较可部署候选；不降低证据、投票、实时行情或交易时段门槛"
                )

            # 先验证到期预测，再把最新结果和旧结论注入本轮。
            try:
                # Existing holdings are reviewed before any new-research work.
                # This keeps lifecycle exits and redeployment ahead of candidate
                # discovery while still requiring a fresh quote at actual fill time.
                pre_round_position_reviews = await simulation.review_open_positions(
                    round_id=active_round_id,
                )
                await round_coordinator.record_event(
                    active_round_id,
                    "PreResearchPortfolioLifecycleReviewed",
                    {
                        "position_count": len(pre_round_position_reviews),
                        "outcomes": [
                            {
                                "ticker": item.get("ticker"),
                                "action": item.get("action"),
                                "reason": item.get("reason"),
                            }
                            for item in pre_round_position_reviews
                        ],
                    },
                )
                t0 = datetime.now()
                learning_engine = LearningEngine(str(db_path))
                logger.info("[diag] validate_pending begin")
                recorder = DecisionRecorder(str(db_path))
                validation_result = await asyncio.wait_for(
                    recorder.validate_pending(max_count=validation_batch_size), timeout=180
                )
                logger.info(f"[diag] validate_pending done in {(datetime.now()-t0).total_seconds():.1f}s")
                if validation_result.get('validated', 0) > 0:
                    print(f"🔄 本轮验证了 {validation_result['validated']} 条决策")

                # 更新playbook
                t0 = datetime.now()
                logger.info("[diag] update_playbook begin")
                await asyncio.wait_for(learning_engine.update_playbook(), timeout=180)
                logger.info(f"[diag] update_playbook done in {(datetime.now()-t0).total_seconds():.1f}s")

                t0 = datetime.now()
                logger.info("[diag] generate_lessons_prompt begin")
                lessons_prompt = await asyncio.wait_for(
                    learning_engine.generate_lessons_prompt(), timeout=120
                )
                research_memory_prompt = await asyncio.wait_for(
                    learning_engine.generate_research_memory_prompt(base_topic), timeout=120
                )
                logger.info(f"[diag] generate_lessons_prompt done in {(datetime.now()-t0).total_seconds():.1f}s")

                t0 = datetime.now()
                logger.info("[diag] get_accuracy_stats begin")
                stats = await asyncio.wait_for(
                    learning_engine.get_accuracy_stats(), timeout=60
                )
                logger.info(f"[diag] get_accuracy_stats done in {(datetime.now()-t0).total_seconds():.1f}s")
                if stats['total'] > 0:
                    print(f"\n📈 可判定历史预测胜率: {stats['accuracy']:.1%} ({stats['correct']}/{stats['total']})")
                if lessons_prompt:
                    print("📜 已加载历史预测教训与错误画像")
                if research_memory_prompt:
                    print("🧠 已加载同议题旧结论、预测期限和验证结果")

            except asyncio.TimeoutError as e:
                logger.error(f"加载历史教训/验证超时: {e}")
                lessons_prompt = ""
                research_memory_prompt = ""
            except Exception as e:
                logger.exception(f"加载历史教训/验证失败: {e}")
                lessons_prompt = ""
                research_memory_prompt = ""
            await round_coordinator.advance(
                active_round_id,
                ResearchRoundStatus.MEMORY_LOADED,
                event_type="HistoricalMemoryLoaded",
                payload={
                    "lessons_loaded": bool(lessons_prompt),
                    "topic_memory_loaded": bool(research_memory_prompt),
                },
            )

            try:
                # 阶段1：按需搜索（先检查本地数据是否足够）
                # 检查 VectorDB 中是否有相关数据
                existing_docs = []
                try:
                    t0 = datetime.now()
                    logger.info(f"[diag] vector_db.search begin topic={topic!r}")
                    existing_docs = await asyncio.wait_for(
                        vector_db.search(topic, top_k=20, llm_client=llm), timeout=120
                    )
                    logger.info(
                        f"[diag] vector_db.search done in {(datetime.now()-t0).total_seconds():.1f}s, "
                        f"got {len(existing_docs)} docs"
                    )
                except asyncio.TimeoutError:
                    logger.error(f"向量检索超时 (120s): topic={topic!r}")
                    existing_docs = []
                except Exception as e:
                    logger.warning(f"向量检索失败: {e}")
                    existing_docs = []

                if args.local_only:
                    print(
                        f"\n📚 阶段1：local-only 使用本地数据 "
                        f"({len(existing_docs)} 条相关文档)；禁止资料联网"
                    )
                    docs = existing_docs
                # 强制定期搜索新数据（默认每轮刷新，避免本地缓存让每轮 token 过低）
                # 避免一直用旧缓存导致空转
                force_search_due = bool(force_search_interval and iteration % force_search_interval == 0)
                should_force_search = (
                    not existing_docs or
                    len(existing_docs) < 10 or
                    force_search_due
                )

                if args.local_only:
                    pass
                elif should_force_search and not existing_docs:
                    print(f"\n📚 阶段1：本地数据不足，进行搜索补充...")
                    docs = await stage1_mass_search(llm, spiders, topic, query_count=search_query_count)
                elif existing_docs and len(existing_docs) >= 10 and not force_search_due:
                    print(f"\n📚 阶段1：使用本地数据 ({len(existing_docs)} 条相关文档)")
                    docs = existing_docs
                else:
                    print(f"\n📚 阶段1：定期更新数据 (每 {force_search_interval or 'N'} 轮强制搜索)")
                    docs = await stage1_mass_search(llm, spiders, topic, query_count=search_query_count)
                    if not docs and existing_docs:
                        logger.warning(
                            "阶段1：外部搜索返回0篇，回退使用本地数据 %s 条，避免后续阶段空转",
                            len(existing_docs),
                        )
                        print(f"⚠️ 外部搜索返回0篇，回退使用本地数据 ({len(existing_docs)} 条)")
                        docs = existing_docs
                    elif docs and existing_docs:
                        seen_doc_keys = {
                            (getattr(doc, "url", "") or getattr(doc, "id", "") or getattr(doc, "doc_id", ""))
                            for doc in docs
                        }
                        added_local_docs = 0
                        for doc in existing_docs:
                            doc_key = getattr(doc, "url", "") or getattr(doc, "id", "") or getattr(doc, "doc_id", "")
                            if doc_key and doc_key not in seen_doc_keys:
                                docs.append(doc)
                                seen_doc_keys.add(doc_key)
                                added_local_docs += 1
                            if len(docs) >= max(10, search_query_count):
                                break
                        if added_local_docs:
                            logger.info("阶段1：搜索结果较少时追加本地数据 %s 条", added_local_docs)

                # 保存文档
                if docs:
                    external_docs = [
                        doc for doc in docs
                        if getattr(doc, "source", "") != "obsidian_wiki"
                        and not str(getattr(doc, "id", "") or getattr(doc, "doc_id", "")).startswith("wiki:")
                    ]
                    skipped_docs = len(docs) - len(external_docs)
                    t0 = datetime.now()
                    logger.info(f"[diag] save_docs begin: external={len(external_docs)} skipped={skipped_docs}")
                    sys.stdout.flush()
                    saved_docs = 0
                    # 先保存到数据库
                    for i, doc in enumerate(external_docs):
                        try:
                            if await asyncio.wait_for(
                                # Raw documents may be globally durable before
                                # the research phase commits, but round links
                                # must only be written by ``persist_sources``
                                # together with the stage/event transition.
                                db_service.add_document(doc),
                                timeout=30,
                            ):
                                saved_docs += 1
                        except asyncio.TimeoutError:
                            logger.warning(f"保存文档超时 (30s): doc #{i} {getattr(doc, 'title', '')[:50]}")
                        except Exception as e:
                            logger.warning(f"保存文档失败: {e}")

                    # 批量添加到 VectorDB（带 embedding）
                    wiki_docs = bounded_sync_index_batch(
                        external_docs,
                        wiki_ingest_max_docs,
                    )
                    deferred_wiki_docs = len(external_docs) - len(wiki_docs)
                    logger.info(
                        "[diag] add_documents_batch begin: sync=%s deferred_to_sqlite_lazy_migration=%s",
                        len(wiki_docs),
                        deferred_wiki_docs,
                    )
                    try:
                        vector_saved = await asyncio.wait_for(
                            vector_db.add_documents_batch(wiki_docs, llm_client=llm),
                            timeout=600,
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"[diag] add_documents_batch timeout (600s), vector_saved=0")
                        vector_saved = 0
                    except Exception as e:
                        logger.error(f"[diag] add_documents_batch failed: {e}")
                        vector_saved = 0
                    logger.info(f"[diag] save_docs done in {(datetime.now()-t0).total_seconds():.1f}s, "
                                f"DB={saved_docs}, Wiki={vector_saved}, WikiDeferred={deferred_wiki_docs}")

                    print(
                        f"   ✅ 文档已保存 (DB新增: {saved_docs}, Wiki同步: {vector_saved}, "
                        f"Wiki延后懒迁移: {deferred_wiki_docs}, 跳过本地派生: {skipped_docs})"
                    )
                presented_document_count = len(docs)
                resolved_lineage = await db_service.resolve_document_lineage(docs)
                traceable_docs = [
                    doc
                    for doc, document_ids in zip(docs, resolved_lineage)
                    if document_ids
                ]
                linked_document_ids = sorted(
                    {
                        document_id
                        for document_ids in resolved_lineage
                        for document_id in document_ids
                    }
                )
                untraceable_document_count = (
                    presented_document_count - len(traceable_docs)
                )
                if untraceable_document_count:
                    logger.warning(
                        "阶段1：排除 %s/%s 条无法回链 SQLite 原始资料的派生文档",
                        untraceable_document_count,
                        presented_document_count,
                    )
                docs = traceable_docs
                await round_coordinator.persist_sources(
                    active_round_id,
                    document_ids=linked_document_ids,
                    presented_document_count=presented_document_count,
                    traceable_document_count=len(traceable_docs),
                    untraceable_document_count=untraceable_document_count,
                )

                redeployment_context = (
                    await simulation.format_redeployment_learning_context()
                    if hasattr(simulation, "format_redeployment_learning_context")
                    else ""
                )
                historical_learning_context = "\n\n".join(
                    part for part in (lessons_prompt, research_memory_prompt) if part
                )
                stage2_diagnostic_context = ""
                if hasattr(db_service, "get_recent_research_stage_diagnostics"):
                    try:
                        stage2_diagnostic_context = format_stage2_diagnostic_context(
                            await db_service.get_recent_research_stage_diagnostics(
                                stage="stage2",
                                limit=5,
                            )
                        )
                    except Exception as diagnostic_error:
                        logger.warning(
                            "读取阶段2诊断记忆失败，不阻塞联网研究: %s",
                            diagnostic_error,
                        )
                if stage2_diagnostic_context:
                    historical_learning_context = "\n\n".join(
                        part
                        for part in (
                            historical_learning_context,
                            stage2_diagnostic_context,
                        )
                        if part
                    )
                prompt_lessons = build_lessons_with_heuristic_context(
                    historical_learning_context,
                    redeployment_context=redeployment_context,
                )

                # 阶段2：深度研报 → 提案
                proposals = await stage2_deep_research(
                    llm,
                    docs,
                    topic,
                    db_service,
                    lessons_prompt=prompt_lessons,
                    round_id=active_round_id,
                )
                proposals = dedupe_proposals(proposals)
                rejection_memory = (
                    await simulation.get_candidate_rejection_memory(limit=100)
                    if hasattr(simulation, "get_candidate_rejection_memory")
                    else []
                )
                proposals, repeated_candidate_rejections = filter_repeated_rejection_proposals(
                    proposals,
                    rejection_memory,
                )
                lot_rejections: List[Dict[str, Any]] = []
                if proposals and hasattr(simulation, "screen_proposal_lot_feasibility"):
                    proposals, lot_rejections = (
                        await simulation.screen_proposal_lot_feasibility(proposals)
                    )
                    await record_proposal_lot_screening_event(
                        round_coordinator,
                        active_round_id,
                        lot_rejections,
                    )
                    repeated_candidate_rejections.extend(lot_rejections)
                    if lot_rejections:
                        logger.warning(
                            "投委会前整手可行性筛选移除%s个候选: %s",
                            len(lot_rejections),
                            ", ".join(
                                str(item.get("ticker") or "")
                                for item in lot_rejections
                            ),
                        )
                        print(
                            "   🧮 整手可行性硬门: "
                            + ", ".join(
                                f"{item['ticker']}(参考价>"
                                f"{float(item.get('max_executable_quote') or 0):.4f})"
                                for item in lot_rejections
                            )
                        )
                if repeated_candidate_rejections:
                    print(
                        "   🧱 重复候选硬门: "
                        + ", ".join(
                            f"{item['ticker']}({item['code']})"
                            for item in repeated_candidate_rejections
                        )
                    )
                for proposal in proposals:
                    try:
                        proposal["round_id"] = active_round_id
                        proposal["proposal_id"] = await db_service.add_proposal(
                            proposal,
                            round_id=active_round_id,
                        )
                    except Exception as e:
                        logger.warning(f"保存提案失败: {e}")

                # 阶段3：投委会讨论
                logger.info(f"开始阶段3投委会审议，提案数: {len(proposals)}")
                if proposals:
                    await round_coordinator.advance(
                        active_round_id,
                        ResearchRoundStatus.PROPOSALS_EXTRACTED,
                        event_type="ProposalsExtracted",
                        payload={"proposal_count": len(proposals)},
                    )
                    try:
                        async def persist_committee_decision(
                            decision: Dict[str, Any],
                        ) -> None:
                            await simulation.record_committee_outcomes(
                                [decision],
                                source="run_discussion",
                                round_id=active_round_id,
                                append_round_events=True,
                            )

                        discussions, decisions = await stage3_ic_discussion(
                            llm,
                            spiders,
                            proposals,
                            topic,
                            lessons_prompt=prompt_lessons,
                            round_id=active_round_id,
                            decision_callback=persist_committee_decision,
                        )
                        logger.info(f"阶段3完成，讨论长度: {len(discussions)}, 决策数: {len(decisions)}")
                    except Exception as e:
                        logger.error(f"阶段3失败: {e}", exc_info=True)
                        raise
                    await round_coordinator.advance(
                        active_round_id,
                        ResearchRoundStatus.COMMITTEE_DECIDED,
                        event_type="CommitteeDecisionsRecorded",
                        payload={"decision_count": len(decisions)},
                    )
                    await round_coordinator.advance(
                        active_round_id,
                        ResearchRoundStatus.PREDICTIONS_RECORDED,
                        event_type="PredictionsRecorded",
                        payload={
                            "prediction_count": sum(
                                bool(item.get("prediction_id")) for item in decisions
                            )
                        },
                    )
                else:
                    discussions, decisions = "", []
                    if repeated_candidate_rejections:
                        # Stage 2 did produce one or more evidence candidates,
                        # but every candidate was removed by an auditable hard
                        # gate.  Keep the round non-terminal until the
                        # simulation read model records the exact rejection;
                        # writing no_evidence here used to create a canonical
                        # terminal that contradicted RoundCompleted.
                        await round_coordinator.record_event(
                            active_round_id,
                            "CandidatePipelineRejectedAll",
                            {
                                "rejection_count": len(
                                    repeated_candidate_rejections
                                ),
                                "codes": sorted(
                                    {
                                        str(item.get("code") or "unknown")
                                        for item in repeated_candidate_rejections
                                    }
                                ),
                                "tickers": sorted(
                                    {
                                        str(item.get("ticker") or "")
                                        for item in repeated_candidate_rejections
                                        if str(item.get("ticker") or "")
                                    }
                                ),
                            },
                        )
                    else:
                        await round_coordinator.advance(
                            active_round_id,
                            ResearchRoundStatus.NO_EVIDENCE,
                            event_type="NoEvidenceTerminal",
                            payload={"source_count": len(docs), "proposal_count": 0},
                            terminal_code="no_evidence",
                            terminal_reason="阶段2没有形成满足证据约束的结构化提案",
                        )

                # 投委会原始讨论是独立记忆，不依赖后续综合结论是否成功。
                proposal_by_ticker = {
                    str(item.get("ticker") or "").strip().upper(): item
                    for item in proposals
                }
                for decision in decisions:
                    ticker_key = str(decision.get("ticker") or "").strip().upper()
                    proposal = proposal_by_ticker.get(ticker_key, {})
                    try:
                        from sovereign_hall.utils import generate_id
                        await db_service.add_meeting_record(
                            meeting_id=generate_id("meeting"),
                            proposal_id=str(proposal.get("proposal_id") or ""),
                            ticker=ticker_key,
                            decision=str(decision.get("direction") or "hold"),
                            discussion=str(decision.get("discussion_excerpt") or "")[:24000],
                            vote_details={
                                "summary": decision.get("vote_summary"),
                                "margin": decision.get("vote_margin"),
                                "vote_count": decision.get("vote_count"),
                                "valid_vote_count": decision.get("parsed_vote_count"),
                                "quorum_required": decision.get("vote_quorum_required"),
                                "quorum_met": decision.get("vote_quorum_met"),
                                "review_depth": decision.get("review_depth"),
                                "individual_votes": decision.get("individual_votes") or [],
                                "directional_vote_count": decision.get("directional_vote_count"),
                                "directional_quorum_required": decision.get("directional_quorum_required"),
                                "directional_quorum_met": decision.get("directional_quorum_met"),
                                "evidence_gaps": decision.get("evidence_gaps") or [],
                                "reconsider_if": decision.get("reconsider_if") or [],
                                "deadlock_review": decision.get("deadlock_review") or {},
                                "initial_committee_decision": (
                                    decision.get("initial_committee_decision") or {}
                                ),
                            },
                            action_items=[
                                f"验证窗口: {int(decision.get('expected_days') or 30)}天",
                                f"止损: {decision.get('stop_loss')}",
                                f"止盈: {decision.get('take_profit')}",
                                *(
                                    ["补证: " + "；".join(decision.get("evidence_gaps") or [])]
                                    if decision.get("evidence_gaps")
                                    else []
                                ),
                                *(
                                    ["重审条件: " + "；".join(decision.get("reconsider_if") or [])]
                                    if decision.get("reconsider_if")
                                    else []
                                ),
                            ],
                            round_id=active_round_id,
                        )
                    except Exception as e:
                        logger.warning("保存投委会会议记忆失败 %s: %s", ticker_key, e)

                # 阶段4：综合结论
                conclusion_data = await stage4_final_conclusion(
                    llm,
                    discussions,
                    decisions,
                    topic,
                    memory_prompt=prompt_lessons,
                )

                # 保存结论（包含结构化数据）
                primary_decision = decisions[0] if decisions else {}
                await db_service.save_report_conclusion(
                    question=base_topic,
                    conclusion=conclusion_data.get('conclusion', ''),
                    ticker=conclusion_data.get('key_ticker', ''),
                    position=float(primary_decision.get("target_position") or 0.0),
                    stop_loss=float(primary_decision.get("stop_loss") or 0.0),
                    take_profit=float(primary_decision.get("take_profit") or 0.0),
                    holding_period=str(primary_decision.get("expected_days") or ""),
                    confidence=conclusion_data.get('confidence', 0.5),
                    key_points=json.dumps(
                        {
                            "direction": primary_decision.get("direction"),
                            "thesis": primary_decision.get("thesis"),
                            "vote_summary": primary_decision.get("vote_summary"),
                            "vote_margin": primary_decision.get("vote_margin"),
                            "research_objective": topic,
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                    risks=json.dumps(
                        primary_decision.get("risk_flags") or [],
                        ensure_ascii=False,
                        default=str,
                    ),
                    round_id=active_round_id,
                )
                await db_service.save_reflection_summary(
                    question=base_topic,
                    previous_conclusions=research_memory_prompt[:8000],
                    reflection_text=(
                        "本轮使用新检索资料经过独立分析、交叉质疑、反事实修正和投票后形成结论。"
                    ),
                    verification_results=json.dumps(
                        {
                            "decision_count": len(decisions),
                            "proposal_count": len(proposals),
                            "vote_quorum": primary_decision.get("vote_quorum_met"),
                        },
                        ensure_ascii=False,
                    ),
                    adjusted_conclusion=conclusion_data.get('conclusion', '')[:12000],
                    lessons_learned=lessons_prompt[:8000],
                    round_id=active_round_id,
                )

                # 💰 每日投资模拟：只消费投委会裁决后的结构化决策
                simulation_result = await run_committee_approved_simulation(
                    simulation,
                    market_data,
                    llm,
                    decisions,
                    initial_rejections=repeated_candidate_rejections,
                    round_id=active_round_id,
                    round_coordinator=round_coordinator,
                    pre_reviewed_positions=pre_round_position_reviews,
                )
                current_round = await round_coordinator.get(active_round_id)
                terminal_map = {
                    "market_closed_pending": ResearchRoundStatus.MARKET_CLOSED_PENDING,
                    "execution_rejected": ResearchRoundStatus.EXECUTION_REJECTED,
                    "committee_hold": ResearchRoundStatus.COMMITTEE_HOLD,
                    "filled": ResearchRoundStatus.FILLED,
                    "no_evidence": ResearchRoundStatus.NO_EVIDENCE,
                }
                terminal_name = str(simulation_result.get("terminal") or "failed")
                target_terminal_status = terminal_map.get(
                    terminal_name,
                    ResearchRoundStatus.FAILED,
                )
                if (
                    current_round
                    and current_round.status != ResearchRoundStatus.NO_EVIDENCE
                    and current_round.status != target_terminal_status
                ):
                    await round_coordinator.advance(
                        active_round_id,
                        target_terminal_status,
                        event_type="SimulationPipelineTerminal",
                        payload={
                            "terminal": terminal_name,
                            "fills": simulation_result.get("fills", 0),
                            "replay_fills": simulation_result.get("replay_fills", 0),
                            "cycle_fills": simulation_result.get("cycle_fills", 0),
                            "pending": simulation_result.get("pending", 0),
                            "candidate_count": simulation_result.get("candidate_count", 0),
                        },
                        terminal_code=terminal_name,
                        terminal_reason=(
                            "模拟投资管线已持久化明确终态；"
                            f"fills={simulation_result.get('fills', 0)}"
                        ),
                    )
                await round_coordinator.advance(
                    active_round_id,
                    ResearchRoundStatus.REFLECTED,
                    event_type="RoundReflectionPersisted",
                    payload={"reflection_saved": True},
                )
                await round_coordinator.advance(
                    active_round_id,
                    ResearchRoundStatus.COMPLETED,
                    event_type="RoundCompleted",
                    payload={
                        "simulation_terminal": terminal_name,
                        "fills": simulation_result.get("fills", 0),
                        "replay_fills": simulation_result.get("replay_fills", 0),
                        "cycle_fills": simulation_result.get("cycle_fills", 0),
                    },
                )

                # 更新已完成议题
                completed_topics.add(base_topic)
                save_completed_topics(completed_topics)

                # 打印结论
                print("\n" + "="*60)
                print("📋 综合结论")
                print("="*60)
                print(conclusion_data.get('conclusion', '')[:1500])
                print("="*60)

            except KeyboardInterrupt:
                await round_coordinator.fail(
                    active_round_id,
                    code="failed",
                    reason="研究轮失败：用户中断",
                )
                raise
            except asyncio.CancelledError:
                print(f"\n⚠️ 任务被取消，保存进度...")
                await round_coordinator.fail(
                    active_round_id,
                    code="failed",
                    reason="研究轮失败：任务取消",
                )
                raise
            except Exception as e:
                print(f"\n❌ 本轮错误: {e}")
                logger.exception(
                    "RESEARCH_ROUND_FAILED round_id=%s",
                    active_round_id,
                )
                await round_coordinator.fail(
                    active_round_id,
                    code="failed",
                    reason=f"研究轮失败：管线异常：{e}",
                )

            # 统计
            round_time = (datetime.now() - round_start).total_seconds()
            total_time = (datetime.now() - start_time).total_seconds()
            llm_stats = llm.get_stats()
            round_llm_stats = _llm_stats_delta(llm_stats, round_start_stats)

            final_round = await round_coordinator.get(active_round_id)
            round_completed = bool(
                final_round
                and final_round.status == ResearchRoundStatus.COMPLETED
            )
            has_valid_result = bool(
                round_completed
                and final_round.terminal_code not in {"no_evidence", "failed"}
            )

            if has_valid_result:
                if empty_rounds > 0:
                    logger.info(f"恢复搜索，成功获取{len(docs)}篇文档和{len(proposals)}个提案")
                empty_rounds = 0
            else:
                empty_rounds += 1
                logger.warning(f"第{iteration}轮无有效结果 (连续{empty_rounds}轮)")

            # 更新持久化统计
            if round_completed:
                persistence.increment_rounds()
                persistence.add_topic(base_topic)
                persistence.add_time(round_time)
                persistence.increment_proposals(len(proposals))
            else:
                logger.error(
                    "研究轮未完成，不计入完成轮次: round_id=%s status=%s",
                    active_round_id,
                    final_round.status.value if final_round else "missing",
                )

            stats_msg = (
                f"⏱️  本轮用时: {round_time:.1f}秒 | "
                f"本轮Token: {format_token_breakdown(round_llm_stats)} | "
                f"累计Token: {format_token_breakdown(llm_stats)} | "
                f"本轮成本: {format_cost_breakdown(round_llm_stats)} | "
                f"累计成本: {format_cost_breakdown(llm_stats)}"
            )
            logger.info(stats_msg)
            db_fd_count = count_open_file_handles(db_path)
            if db_fd_count > 5:
                logger.warning("SQLite fd count is high: %s handles open for %s", db_fd_count, db_path)
            elif db_fd_count >= 0:
                logger.debug("SQLite fd count: %s", db_fd_count)
            print(f"\n⏱️  本轮用时: {round_time:.1f}秒")
            print(f"🔥  本轮Token: {format_token_breakdown(round_llm_stats)}")
            print(f"🔥  累计Token: {format_token_breakdown(llm_stats)}")
            print(f"🚀  峰值Token速率: {llm_stats.get('peak_token_rate', '0/s')}")
            print(f"📊  平均Token速率: {llm_stats.get('avg_token_rate', '0/s')}")
            print(f"💰  本轮成本: {format_cost_breakdown(round_llm_stats)}")
            print(f"💰  累计成本: {format_cost_breakdown(llm_stats)}")
            if daily_budget.budget:
                used_today = daily_budget.used_today(llm_stats.get('total_tokens', 0))
                print(f"🧯 今日Token预算: {format_token(used_today)}/{format_token(daily_budget.budget)}")

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
        print(f"   累计Token: {format_token_breakdown(llm_stats)}")
        print(f"   峰值Token速率: {llm_stats.get('peak_token_rate', '0/s')}")
        print(f"   平均Token速率: {llm_stats.get('avg_token_rate', '0/s')}")
        print(f"   累计成本: {format_cost_breakdown(llm_stats)}")

    finally:
        await spiders.close()
        await market_data.close()
        await llm.close()
        await vector_db.close()
        await db_service.close()
        print("🔒 资源已释放")


if __name__ == "__main__":
    if cli_args_can_run_without_instance_lock():
        parse_args()
        sys.exit(0)
    try:
        killed = kill_existing_run_discussion_instances()
        if killed:
            print(f"   ✅ 已停掉 {len(killed)} 个旧 run_discussion 进程，继续启动新实例")
        with SingleInstanceLock(project_root / "data" / "run_discussion.lock"):
            asyncio.run(main())
    except RuntimeError as exc:
        logger.error(str(exc))
        print(f"❌ {exc}")
        sys.exit(2)
