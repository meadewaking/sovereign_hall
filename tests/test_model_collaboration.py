"""Collaboration must preserve provenance, bounded spend, and durable decisions."""
import asyncio
import copy
from datetime import datetime
from importlib import import_module
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.core import Document, TokenStats
from sovereign_hall.core.config import Config
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.services.model_collaboration import (
    BudgetedReviewer, CollaborationState, CollaborativeLLM, ReviewBudgetExceeded,
    parse_review, review_committee_decision,
)
from sovereign_hall.run_discussion import execute_durable_committee_stage, supplement_model_evidence


SOURCE = {"id": "source-one", "title": "600519 audited filing", "url": "https://example.org/filing",
          "content": "600519公司本期营业收入为100亿元，同比增长10%。本期归母净利润为30亿元，同比增长5%。"}
PROPOSAL = {"ticker": "600519", "direction": "long", "confidence": 0.6,
            "target_position": 0.1, "thesis": "两项已披露经营指标增长",
            "evidence": ["营业收入同比增长10%", "归母净利润同比增长5%"],
            "sector": "消费", "stop_loss": 8, "take_profit": 15, "holding_period": 21}
RESEARCH = {"direction": "hold", "confidence": 0.6, "target_position": 0.0,
            "vote_quorum_met": True, "vote_count": 7, "parsed_vote_count": 7,
            "individual_votes": [{"role": "risk", "direction": "hold", "key_evidence": ["需核对原文"]}],
            "risk_flags": ["research_disagreement"], "evidence_gaps": ["需核对原文"]}


def verdict(**updates):
    result = {"action": "approve", "direction": "long", "confidence": 0.7,
              "position": 0.1, "key_evidence": ["营收增长", "净利润增长"],
              "risk_flags": [], "invalid_if": "原始披露出现更正", "questions": [],
              "citations": [{"source_id": "source-one", "quote": "本期营业收入为100亿元，同比增长10%"},
                            {"source_id": "source-one", "quote": "本期归母净利润为30亿元，同比增长5%"}]}
    return {**result, **updates}


def client(tmp_path, responses, *, daily_limit=10, round_limit=4):
    state = CollaborationState(tmp_path / "collaboration.json", daily_limit=daily_limit,
                               round_limit=round_limit)
    research = SimpleNamespace(model="self-hosted/deepseek-v4.1-flash-1017", profile="self-hosted/deepseek-v4.1-flash-1017", provider="openai",
        base_url="http://offline.invalid/v1", chat=AsyncMock(), close=AsyncMock())
    reviewer = SimpleNamespace(model="kimi-k3.1-pro", profile="kimi-k3.1-pro", provider="openai",
        base_url="https://offline.invalid/v1", chat=AsyncMock(side_effect=responses), close=AsyncMock())
    # Preserve coverage of the legacy wire contract during the protocol change.
    result = CollaborativeLLM(research, reviewer, settings={"review_timeout_seconds": 1,
                              "review_citation_wire": False}, state=state)
    result.remember_sources([SOURCE], "round-one")
    return result


async def run_review(llm, **kwargs):
    return await review_committee_decision(llm, PROPOSAL, RESEARCH, "不同角色的支持与反对意见",
        round_id="round-one", execute_stage=execute_durable_committee_stage, **kwargs)


@pytest.mark.parametrize("changes,error", [
    ({"confidence": float("nan")}, "invalid_confidence"),
    ({"position": True}, "invalid_position"),
    ({"citations": [{"source_id": "invented", "quote": "这段引文没有原文支持"}]}, "unknown_source"),
    ({"citations": [{"source_id": "source-one", "quote": "虚构的公司收入与利润"}]}, "untraceable_quote"),
    ({"citations": [verdict()["citations"][0]] * 2}, "insufficient_quoted_evidence"),
    ({"action": "reject"}, "action_direction_mismatch"),
    ({"action": "request_evidence", "direction": "hold", "position": 0}, "missing_bounded_questions"),
])
def test_review_requires_finite_fields_and_traceable_nonduplicate_quotes(changes, error):
    parsed = parse_review(json.dumps(verdict(**changes)), [SOURCE])
    assert parsed["schema_valid"] is False and parsed["error"] == error


def test_duplicate_fields_and_unfinished_outputs_are_invalid():
    assert not parse_review('{"action":"approve","action":"reject"}', [SOURCE])["schema_valid"]
    assert not parse_review('[committee_task_absent]\n' + json.dumps(verdict()), [SOURCE])["schema_valid"]
    assert parse_review(json.dumps(verdict()), [SOURCE])["schema_valid"]


@pytest.mark.parametrize("opening,newline", [("```json", "\n"), ("```", "\n"),
                                          ("```JSON \t", "\r\n")])
def test_complete_final_json_fence_preserves_the_strict_verdict(opening, newline):
    raw = f" \n{opening}{newline}{json.dumps(verdict())}{newline}```\n "
    parsed = parse_review(raw, [SOURCE])
    assert parsed == {**verdict(), "schema_valid": True,
                      "parse_mode": "collaboration_review_fenced_json_v1"}


@pytest.mark.parametrize("wrapper", [
    "Analysis before final:\n```json\n%s\n```",
    "```json\n%s\n```\nExtra conclusion",
    "```python\n%s\n```",
    "```json\n%s",
    "```json\n%s\n```\n```json\n{}\n```",
    "[committee_task_absent]\n```json\n%s\n```",
    "```json\n<think>possibly approve</think>\n%s\n```",
])
def test_review_does_not_extract_verdicts_from_prose_partial_or_multiple_fences(wrapper):
    assert not parse_review(wrapper % json.dumps(verdict()), [SOURCE])["schema_valid"]


@pytest.mark.parametrize("content,error", [
    ('{"action":"approve","action":"reject"}', "duplicate_field"),
    (json.dumps(verdict(citations=[])), "insufficient_quoted_evidence"),
    (json.dumps(verdict(confidence=float("nan"))), "invalid_confidence"),
    (json.dumps(verdict(citations=[{"source_id": "invented", "quote": "虚构的资料原文无法支持"}])), "unknown_source"),
    (json.dumps(verdict(citations=[{"source_id": "source-one", "quote": "虚构的公司收入与利润"}])), "untraceable_quote"),
    (json.dumps(verdict(risk_flags=["账户净值穿透-10%全局止损线，禁止开仓"])), "unconfigured_execution_gate"),
])
def test_json_fence_never_bypasses_existing_safety_checks(content, error):
    parsed = parse_review("```json\n" + content + "\n```", [SOURCE])
    assert not parsed["schema_valid"] and parsed["error"] == error


def test_call_budget_survives_reconstruction_and_refuses_corruption(tmp_path):
    path = tmp_path / "state.json"
    state = CollaborationState(path, daily_limit=3, round_limit=2)
    state.reserve("one")
    CollaborationState(path, daily_limit=3, round_limit=2).reserve("one")
    with pytest.raises(ReviewBudgetExceeded, match="round"):
        state.reserve("one")
    state.reserve("two")
    with pytest.raises(ReviewBudgetExceeded, match="daily"):
        state.reserve("three")
    path.write_text("corrupted")
    with pytest.raises(ValueError):
        state.reserve("four")


def test_midnight_resets_daily_limit_but_not_inflight_round_limit(tmp_path):
    state = CollaborationState(tmp_path / "state.json", daily_limit=1, round_limit=1)
    state.reserve("same-round")
    saved = json.loads(state.path.read_text())
    saved["date"] = "2000-01-01"
    state.path.write_text(json.dumps(saved))
    with pytest.raises(ReviewBudgetExceeded, match="round"):
        state.reserve("same-round")
    state.reserve("new-round")


@pytest.mark.asyncio
async def test_failed_review_consumes_allowance_without_research_fallback(tmp_path):
    llm = client(tmp_path, [RuntimeError("provider down")], daily_limit=1)
    reviewer = llm.reviewer.for_round("one")
    with pytest.raises(RuntimeError, match="provider down"):
        await reviewer.chat(system="check", user="check")
    with pytest.raises(ReviewBudgetExceeded):
        await reviewer.chat(system="check", user="check")
    assert llm.reviewer.client_impl.chat.await_count == 1
    llm.research.chat.assert_not_awaited()


@pytest.mark.asyncio
async def test_final_reviewer_can_resolve_research_disagreement_without_forging_votes(tmp_path):
    llm = client(tmp_path, [json.dumps(verdict())])
    result, audits, _ = await run_review(llm)
    assert result["direction"] == "long" and result["final_review_model"] == "kimi-k3.1-pro"
    assert result["individual_votes"] == RESEARCH["individual_votes"]
    assert result["research_committee_decision"] == RESEARCH
    request = llm.reviewer.client_impl.chat.call_args.kwargs
    packet = json.loads(request["user"])
    assert packet["sources"][0]["content"] == SOURCE["content"]
    assert packet["research_committee"]["individual_votes"][0]["direction"] == "hold"
    assert request["json_output"] and request["use_cache"] is False
    assert len(audits) == 1


@pytest.mark.asyncio
async def test_fenced_reject_retains_evidence_and_reconsider_conditions_without_extra_call(tmp_path):
    reject = verdict(action="reject", direction="hold", position=0,
                     key_evidence=["个股证据缺失", "行业与标的赛道不符"],
                     invalid_if="取得标的最新财务数据与赛道需求证据后重审")
    llm = client(tmp_path, ["```json\n" + json.dumps(reject) + "\n```"])
    result, audits, _ = await run_review(llm)
    assert result["direction"] == "hold" and result["target_position"] == 0
    assert result["vote_quorum_met"] and "final_review_absent" not in result["risk_flags"]
    assert result["evidence_gaps"] == reject["key_evidence"]
    assert result["reconsider_if"] == [reject["invalid_if"]]
    assert result["individual_votes"] == RESEARCH["individual_votes"]
    assert llm.reviewer.client_impl.chat.await_count == len(audits) == 1
    assert audits[0]["tasks"][0]["parse_mode"] == "collaboration_review_fenced_json_v1"


@pytest.mark.asyncio
@pytest.mark.parametrize("response", ["reasoning only", json.dumps(verdict(citations=[])), RuntimeError("down")])
async def test_absent_or_invalid_final_review_never_executes_a_research_approval(tmp_path, response):
    llm = client(tmp_path, [response])
    original = {**RESEARCH, "direction": "long", "target_position": 0.1}
    result, _, _ = await review_committee_decision(llm, PROPOSAL, original, "views",
        round_id="round-one", execute_stage=execute_durable_committee_stage)
    assert result["direction"] == "hold" and result["target_position"] == 0
    assert result["vote_quorum_met"] is False
    assert "final_review_absent" in result["risk_flags"]


@pytest.mark.asyncio
async def test_k3_does_not_supply_missing_research_participation(tmp_path):
    llm = client(tmp_path, [json.dumps(verdict())])
    result, _, _ = await review_committee_decision(llm, PROPOSAL,
        {**RESEARCH, "vote_quorum_met": False}, "views", round_id="round-one",
        execute_stage=execute_durable_committee_stage)
    assert not result["vote_quorum_met"] and result["target_position"] == 0


@pytest.mark.asyncio
async def test_only_novel_raw_evidence_triggers_one_additional_k3_call(tmp_path):
    request = verdict(action="request_evidence", direction="hold", position=0,
                      questions=["核对现金流是否有改善"], citations=[])
    llm = client(tmp_path, [json.dumps(request), json.dumps(verdict())])
    supplement = AsyncMock(return_value={"sources": [{"id": "source-two",
        "content": "600519本期经营活动现金流为35亿元，收入与利润已得到现金流支持。"}]})
    result, audits, _ = await run_review(llm, supplement=supplement)
    assert result["direction"] == "long" and len(audits) == 2
    assert llm.reviewer.client_impl.chat.await_count == 2
    second = json.loads(llm.reviewer.client_impl.chat.call_args.kwargs["user"])
    assert second["sources"][0]["id"] == "source-two"
    assert supplement.await_count == 1


@pytest.mark.asyncio
async def test_empty_or_repeated_supplement_does_not_spend_another_review(tmp_path):
    request = verdict(action="request_evidence", direction="hold", position=0,
                      questions=["需要新的独立证据"], citations=[])
    llm = client(tmp_path, [json.dumps(request)])
    result, audits, _ = await run_review(llm, supplement=AsyncMock(return_value={"sources": [SOURCE]}))
    assert result["direction"] == "hold" and result["vote_quorum_met"]
    assert len(audits) == llm.reviewer.client_impl.chat.await_count == 1
    assert result["final_review"]["action"] == "request_evidence"


@pytest.mark.asyncio
async def test_repeated_request_for_evidence_stops_after_one_supplement(tmp_path):
    request = verdict(action="request_evidence", direction="hold", position=0,
                      questions=["需要补证"], citations=[])
    llm = client(tmp_path, [json.dumps(request), json.dumps(request)])
    supplement = AsyncMock(return_value={"sources": [{"id": "new", "content": "新的证据尚不能解决全部问题"}]})
    result, audits, _ = await run_review(llm, supplement=supplement)
    assert result["direction"] == "hold" and len(audits) == 2
    assert supplement.await_count == 1


@pytest.mark.asyncio
async def test_durable_review_resume_does_not_make_a_second_paid_call(tmp_path):
    llm = client(tmp_path, [json.dumps(verdict())])
    db = DatabaseService(str(tmp_path / "committee.db"))
    await db._init_db()
    try:
        scope = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="review")
        store = CommitteeMeetingStore(db)
        meeting = await store.start_or_resume(round_id=scope.id, proposal=PROPOSAL, review_depth="full")
        first, _, _ = await run_review(llm, meeting_store=store, meeting_id=meeting)
        second, audits, _ = await run_review(llm, meeting_store=store, meeting_id=meeting)
        assert first["direction"] == second["direction"] == "long"
        assert audits[0]["checkpoint_resumed_count"] == 1
        assert llm.reviewer.client_impl.chat.await_count == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_recheck_resume_reuses_both_review_checkpoints_and_bounded_source_packet(tmp_path):
    request = verdict(action="request_evidence", direction="hold", position=0,
                      questions=["补充现金流证据"], citations=[])
    llm = client(tmp_path, [json.dumps(request), json.dumps(verdict())])
    new_source = {"id": "new", "content": "600519本期现金流改善，相关经营事实如下。" * 2000}
    supplement = AsyncMock(return_value={"status": "sources_persisted", "sources": [new_source]})
    db = DatabaseService(str(tmp_path / "committee.db"))
    await db._init_db()
    try:
        scope = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="recheck")
        store = CommitteeMeetingStore(db)
        meeting = await store.start_or_resume(round_id=scope.id, proposal=PROPOSAL, review_depth="full")
        options = dict(meeting_store=store, meeting_id=meeting, supplement=supplement)
        first, _, _ = await run_review(llm, **options)
        second, audits, _ = await run_review(llm, **options)
        assert first["direction"] == second["direction"] == "long"
        assert all(audit["checkpoint_resumed_count"] == 1 for audit in audits)
        assert llm.reviewer.client_impl.chat.await_count == 2
        second_prompt = llm.reviewer.client_impl.chat.call_args.kwargs["user"]
        assert len(second_prompt) < 40000
        assert "content" not in first["final_review_turns"][1]["targeted_evidence"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_checkpoint_changes_when_deployment_or_prompt_changes(tmp_path):
    db = DatabaseService(str(tmp_path / "committee.db"))
    await db._init_db()
    try:
        scope = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="identity")
        store = CommitteeMeetingStore(db)
        meeting = await store.start_or_resume(round_id=scope.id, proposal=PROPOSAL, review_depth="full")
        call = AsyncMock(return_value="valid")
        spec = {"label": "same", "model": "same-model", "provider": "openai",
                "base_url": "https://one.invalid", "input_material": "same", "factory": call}
        options = dict(timeout_seconds=1, stage="test", max_concurrency=1,
                       meeting_store=store, meeting_id=meeting)
        await execute_durable_committee_stage([spec], **options)
        await execute_durable_committee_stage([spec], **options)
        await execute_durable_committee_stage([{**spec, "base_url": "https://two.invalid"}], **options)
        await execute_durable_committee_stage([spec], **options, prompt_version="new-prompt")
        assert call.await_count == 3
    finally:
        await db.close()


class EphemeralStats(TokenStats):
    def _try_load_from_disk(self):
        pass

    def _save_to_disk(self):
        pass


@pytest.mark.asyncio
async def test_two_transports_share_lifetime_totals_and_attribute_each_request_once(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "offline-fixture")
    state = CollaborationState(tmp_path / "usage.json", daily_limit=5, round_limit=2)
    totals = EphemeralStats(prompt_tokens=100, completion_tokens=50, total_tokens=150)
    # Transport accounting should not depend on a retired deployed model name.
    for name in ("usage-test-research", "usage-test-review"):
        monkeypatch.setitem(Config()._config["llm_profiles"], name, {
            "provider": "openai", "model": name, "base_url": "https://offline.invalid/v1",
            "api_key": "not_need",
        })
    clients = [LLMClient(profile=name, token_stats=totals, usage_observer=state.observe)
               for name in ("usage-test-research", "usage-test-review")]
    seen = []

    def handler(request):
        seen.append((json.loads(request.content)["model"], request.headers["Authorization"]))
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"ok":true}'},
                                                      "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}})
    try:
        for llm in clients:
            await llm._http_client.aclose()
            llm._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        await asyncio.gather(*(llm.chat(system="test", user="test", json_output=True) for llm in clients))
        assert totals.total_tokens == 210
        assert clients[0].get_stats()["total_tokens"] == clients[1].get_stats()["total_tokens"] == 210
        with state.locked() as saved:
            assert len(saved["usage"]) == 2
            assert sum(row["prompt_tokens"] for row in saved["usage"].values()) == 20
            assert sum(row["completion_tokens"] for row in saved["usage"].values()) == 40
        assert seen[0][1] == "Bearer offline-fixture"
    finally:
        await asyncio.gather(*(llm.close() for llm in clients))


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_queries", ['["600519 现金流 公告"]',
                                         '["600519 现金流 公告"]\n以上为搜索词。'])
async def test_targeted_sources_are_durable_and_linked_before_reviewer_sees_them(tmp_path, raw_queries):
    db = DatabaseService(str(tmp_path / "sources.db"))
    await db._init_db()
    try:
        scope = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="supplement")
        doc = Document(id="source-new", title="600519公司经营公告", content=SOURCE["content"] * 3,
                       url="https://example.org/2026-10-25/filing", source="issuer_filing",
                       publish_time=datetime(2026, 10, 25), metadata={"publish_time_source": "page_publication_metadata"})
        llm = SimpleNamespace(chat=AsyncMock(return_value=raw_queries),
                              reviewer=SimpleNamespace(model="kimi-k3.1-pro"))
        spiders = SimpleNamespace(aggressive_search=AsyncMock(return_value=[doc]),
                                  parallel_fetch=AsyncMock(return_value=[]))
        result = await supplement_model_evidence(llm, spiders, PROPOSAL, ["核对现金流"],
                                                 round_id=scope.id, db_service=db)
        assert result["status"] == "sources_persisted"
        assert result["sources"][0]["id"] == "source-new"
        conn = await db._get_connection()
        async with conn.execute("SELECT document_id FROM round_documents WHERE round_id=?", (scope.id,)) as cursor:
            assert "source-new" in [row[0] for row in await cursor.fetchall()]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_empty_candidate_audit_is_sampled_and_bounded(tmp_path):
    llm = client(tmp_path, ["[]"])
    assert await llm.audit_empty_proposals("evidence", "one") is None
    assert await llm.audit_empty_proposals("evidence", "two") is None
    assert await llm.audit_empty_proposals("evidence", "three") == "[]"
    assert await llm.audit_empty_proposals("evidence", "three") is None
    assert llm.reviewer.client_impl.chat.await_count == 1


@pytest.mark.asyncio
async def test_full_committee_uses_ds_roles_then_k3_1_and_persists_both_models(tmp_path, monkeypatch):
    rd = import_module("sovereign_hall.run_discussion")
    config_module = import_module("sovereign_hall.core.config")
    config = object.__new__(Config)
    config.load_defaults()
    config.set("research", {"committee_full_discussion": False,
                            "committee_min_review_depth": "light",
                            "committee_deadlock_review_enabled": False})
    monkeypatch.setattr(config_module, "get_config", lambda: config)
    llm = client(tmp_path, [json.dumps(verdict())])

    async def research_chat(**request):
        if request.get("json_output"):
            return json.dumps({"direction": "hold", "confidence": 0.6, "position": 0,
                               "key_evidence": ["需核对原文"], "risk_flags": [],
                               "invalid_if": "完成原文核验"})
        return "已有财报提供支持，但应由终审直接核对原文，并考虑估值和风险。"

    llm.research.chat.side_effect = research_chat
    recorder = import_module("sovereign_hall.services.decision_tracker").DecisionRecorder
    monkeypatch.setattr(recorder, "record_decision", AsyncMock(return_value="test-prediction"))
    db = DatabaseService(str(tmp_path / "full.db"))
    await db._init_db()
    try:
        scope = await ResearchRoundCoordinator(db).start(base_topic="test", research_objective="full")
        doc = Document(id=SOURCE["id"], title=SOURCE["title"], content=SOURCE["content"] * 3,
                       url=SOURCE["url"], source="issuer_filing", publish_time=datetime(2026, 10, 25),
                       metadata={"publish_time_source": "page_publication_metadata"})
        await db.add_documents_batch([doc], round_id=scope.id)
        _, decisions = await rd.stage3_ic_discussion(llm, None, [PROPOSAL], "test",
            round_id=scope.id, committee_store=CommitteeMeetingStore(db))
        assert len(decisions) == 1
        final = decisions[0]
        assert final["direction"] == "long" and final["final_review_model"] == "kimi-k3.1-pro"
        assert final["research_committee_decision"]["direction"] == "hold"
        assert "K3.1终审 model=kimi-k3.1-pro" in final["discussion_excerpt"]
        assert "final_review_model=kimi-k3.1-pro" in recorder.record_decision.call_args.kwargs["discussion_context"]
        assert llm.research.chat.await_count >= 7
        assert llm.reviewer.client_impl.chat.await_count == 1
        conn = await db._get_connection()
        async with conn.execute("SELECT DISTINCT model FROM committee_tasks") as cursor:
            assert {row[0] for row in await cursor.fetchall()} == {"self-hosted/deepseek-v4.1-flash-1017", "kimi-k3.1-pro"}
        packet = json.loads(llm.reviewer.client_impl.chat.call_args.kwargs["user"])
        assert packet["sources"][0]["content"] == SOURCE["content"] * 3
    finally:
        await db.close()
