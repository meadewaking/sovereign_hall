"""Opt-in, non-executing evidence council for issuer dividend research.

This is an early HOLD/referral gate, not an investment approval or a substitute
for portfolio risk checks. Model prose is retained in experiment transcripts,
never promoted into factual decision reasons. Only validated references and
arithmetic predicates can enter the compiled result. The income-only adapter
cannot resolve the decisive gaps: sufficient-evidence promotion is untested.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, date
import hashlib
import json
import math
import re

from .issuer_dossier import FIELDS, VERSION as DOSSIER_VERSION

VERSION = "evidence_council_v1"
GAPS = {
    "original_filing": "发行人原公告及转录核验",
    "net_interest_margin": "净息差及口径、期间；利息净收入不能替代",
    "asset_quality": "不良率、拨备及变动",
    "capital_adequacy": "资本充足及约束",
    "dividend_plan": "已披露分红方案及支付能力",
    "valuation": "时点一致的估值及股息率；执行报价另由交易门检查",
}
PREDICATES = {"reported_value", "yoy_increase", "yoy_decrease", "yoy_flat"}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def packet_from_dossiers(documents: list[dict], *, cutoff: datetime) -> dict:
    """Adapt already captured public income dossiers; reject corrupted lineage.

    Hash checks verify internal artifact consistency, not independent source
    authenticity. The secondary-source caveat is carried to every fact.
    """
    if cutoff.tzinfo is None:
        raise ValueError("timezone required")
    facts, issuers = {}, {}
    for doc in documents:
        meta = doc["metadata"]
        match = re.fullmatch(r"issuer_dossier_v1_((?:0|3|6)\d{5})_([0-9a-f]{16})", doc["id"])
        if not match or meta.get("dossier_version") != DOSSIER_VERSION:
            raise ValueError("unsupported dossier")
        ticker, digest = match.groups()
        if ticker in issuers:
            raise ValueError("duplicate issuer dossier")
        if hashlib.sha256(doc["content"].encode()).hexdigest()[:16] != digest:
            raise ValueError("dossier content hash mismatch")
        captured = datetime.fromisoformat(meta["crawled_at"])
        if captured.tzinfo is None or captured > cutoff:
            raise ValueError("unavailable capture")
        if meta.get("source_kind") != "secondary_distributor" or not meta.get("url"):
            raise ValueError("missing source lineage")
        # Metadata and the hash-bound public excerpt must describe identical facts.
        body_facts = json.loads(doc["content"].splitlines()[3])
        if body_facts != meta["facts"]:
            raise ValueError("metadata/excerpt mismatch")
        issuers[ticker] = {"missing": list(GAPS), "fact_ids": []}
        for raw in meta["facts"]:
            key = raw["field"]
            if key not in FIELDS or raw["label"] != FIELDS[key] or not _number(raw["value"]):
                raise ValueError("unsupported metric or value")
            period, notice = date.fromisoformat(raw["report_period"]), date.fromisoformat(raw["notice_date"])
            if not period <= notice < cutoff.date():
                raise ValueError("unavailable report")
            fact_id = f"{ticker}:{period}:{key}"
            if fact_id in facts:
                raise ValueError("duplicate metric")
            fact = {"id": fact_id, "ticker": ticker, "metric": key,
                    "label": FIELDS[key], "value": raw["value"], "unit": "CNY",
                    "period_kind": "cumulative", "report_period": str(period),
                    "notice_date": str(notice), "source_document_id": doc["id"],
                    "source_url": meta["url"], "captured_at": captured.isoformat(),
                    "source_kind": "secondary_distributor", "independently_verified": False}
            if "prior_value" in raw:
                prior, prior_notice = date.fromisoformat(raw["prior_period"]), date.fromisoformat(raw["prior_notice_date"])
                if (not _number(raw["prior_value"]) or
                        (prior.year, prior.month, prior.day) != (period.year - 1, period.month, period.day) or
                        not prior <= prior_notice < cutoff.date()):
                    raise ValueError("invalid prior period")
                fact.update(prior_value=raw["prior_value"], prior_period=str(prior), prior_notice_date=str(prior_notice))
                if raw["prior_value"] > 0:
                    fact["derived_yoy_pct"] = round((raw["value"] / raw["prior_value"] - 1) * 100, 4)
            if raw.get("derived_yoy_pct") != fact.get("derived_yoy_pct"):
                raise ValueError("growth arithmetic mismatch")
            facts[fact_id] = fact
            issuers[ticker]["fact_ids"].append(fact_id)
    if not facts:
        raise ValueError("no facts")
    return {"version": VERSION, "cutoff": cutoff.isoformat(), "facts": facts,
            "issuers": issuers, "gap_catalog": GAPS.copy()}


def validate_ballot(raw: str, packet: dict) -> tuple[dict | None, list[str]]:
    """Strict JSON and closed vocabulary; invalid output is an explicit abstention."""
    def unique_object(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError("duplicate JSON key")
            obj[key] = value
        return obj

    try:
        ballot = json.loads(raw, object_pairs_hook=unique_object)
        if not isinstance(ballot, dict) or set(ballot) != {"assessments"}:
            raise ValueError("top-level schema")
        assessments = ballot["assessments"]
        if not isinstance(assessments, list) or len(assessments) != len(packet["issuers"]):
            raise ValueError("issuer coverage")
        seen = set()
        for row in assessments:
            if not isinstance(row, dict) or set(row) != {"ticker", "vote", "evidence", "priority_gaps"}:
                raise ValueError("assessment schema")
            ticker = row["ticker"]
            if ticker not in packet["issuers"] or ticker in seen:
                raise ValueError("wrong or duplicate issuer")
            seen.add(ticker)
            if row["vote"] not in {"HOLD", "REFER"}:
                raise ValueError("unsupported vote")
            evidence = row["evidence"]
            if not isinstance(evidence, list) or not 1 <= len(evidence) <= 8:
                raise ValueError("missing factual reasons")
            references = set()
            for claim in evidence:
                if not isinstance(claim, dict) or set(claim) != {"fact_id", "predicate"}:
                    raise ValueError("claim schema; free text is not evidence")
                fid, pred = claim["fact_id"], claim["predicate"]
                fact = packet["facts"].get(fid)
                if fact is None or fact["ticker"] != ticker or pred not in PREDICATES:
                    raise ValueError("unknown or wrong-issuer reference")
                if (fid, pred) in references:
                    raise ValueError("duplicate reference")
                references.add((fid, pred))
                if pred != "reported_value":
                    growth = fact.get("derived_yoy_pct")
                    if growth is None or not {"yoy_increase": growth > 0,
                                              "yoy_decrease": growth < 0,
                                              "yoy_flat": growth == 0}[pred]:
                        raise ValueError("unsupported growth predicate")
            gaps = row["priority_gaps"]
            if (not isinstance(gaps, list) or len(gaps) != len(set(gaps)) or
                    any(g not in packet["issuers"][ticker]["missing"] for g in gaps)):
                raise ValueError("invalid gap references")
            if packet["issuers"][ticker]["missing"] and not gaps:
                raise ValueError("missing research priority")
        return ballot, []
    except (ValueError, TypeError, KeyError) as exc:
        return None, [str(exc)]


def compile_decisions(packet: dict, proposal: dict | None, ballots: list[dict | None]) -> list[dict]:
    """Unanimous two-seat referral is still subject to existing risk/committee gates."""
    results = []
    quorum = proposal is not None and len(ballots) == 2 and all(b is not None for b in ballots)
    for ticker, issuer in packet["issuers"].items():
        rows = [next(r for r in b["assessments"] if r["ticker"] == ticker)
                for b in ballots if b is not None]
        missing = issuer["missing"]
        refer = quorum and not missing and all(r["vote"] == "REFER" for r in rows)
        # Evidence comes from canonical records; all known facts remain visible,
        # even if the model omitted counterevidence from its selected references.
        facts = [packet["facts"][fid] for fid in issuer["fact_ids"]]
        priorities = sorted(missing, key=lambda g: (
            sum(r["priority_gaps"].index(g) if g in r["priority_gaps"] else len(GAPS) for r in rows), g))
        results.append({"ticker": ticker, "action": "REFER" if refer else "HOLD",
                        "executable": False, "quorum_met": quorum,
                        "reason": "refer_existing_committee" if refer else
                                  "invalid_model_output" if not quorum else
                                  "missing_evidence" if missing else "committee_hold",
                        "votes": [r["vote"] for r in rows],
                        "facts": facts, "selected_evidence": [r["evidence"] for r in rows],
                        "missing": list(missing), "research_queue": priorities})
    return results


SCHEMA = """只输出JSON对象，不要Markdown或额外字段：
{"assessments":[{"ticker":"证券代码","vote":"HOLD或REFER",
"evidence":[{"fact_id":"packet中的完整事实ID","predicate":"reported_value或yoy_increase或yoy_decrease或yoy_flat"}],
"priority_gaps":["按补证价值排序的缺口ID"]}]}
逐一覆盖packet中的每个证券。至少引用一个事实，不得杜撰ID、增减符号或自由文本。
REFER仅请求既有投委会继续进行估值/组合/执行风险审议，不是批准投资。
本轮核心是银行分红可持续性；信息不足应HOLD。利润增长不证明分红安全；
利息净收入为金额，不是净息差，不能证明LPR因果；累计期不等于单季度。
priority_gaps只能选择本证券missing中的项目。事实不能从模型记忆补充。
"""


async def run_evidence_council(client, packet: dict, *, topic: str, record) -> dict:
    """Three real model calls, no retrieval/database/account/price dependencies.

    Caller supplies an async durable recorder and enforces total cost/time.
    Both committee seats see the same validated proposal, never each other's
    votes. Raw responses and validation errors stay separate from final facts.
    """
    public = json.dumps({"topic": topic, "packet": packet}, ensure_ascii=False)

    async def ask(role, proposal=None):
        request = {"system": role + "\n" + SCHEMA,
                   "user": public + ("\n提案=" + json.dumps(proposal, ensure_ascii=False) if proposal is not None else ""),
                   "temperature": 0, "max_tokens": 2400, "json_output": True, "use_cache": False}
        await record(role, "request", request)
        raw = await client.chat(**request)
        parsed, errors = validate_ballot(raw, packet)
        await record(role, "response", {"raw": raw, "validated": parsed, "errors": errors})
        return parsed

    proposal = await ask("提案研究员：选择有信息量的经营事实，提出HOLD或继续审议及下一步补证顺序。")
    if proposal is None:
        return {"version": VERSION, "status": "invalid_proposal", "decisions": compile_decisions(packet, None, [])}
    ballots = await asyncio.gather(
        ask("证据审议席：独立核对提案的事实、符号、口径及分红逻辑是否受现有资料支持。", proposal),
        ask("风险审议席：独立审查分红、资产质量、资本和估值缺口，优先补最影响判断的信息。", proposal))
    return {"version": VERSION, "status": "completed" if all(b is not None for b in ballots) else "invalid_vote",
            "proposal": proposal, "ballots": ballots, "decisions": compile_decisions(packet, proposal, ballots)}
