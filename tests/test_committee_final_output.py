"""Offline regressions for final votes, transport and durable recovery."""
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from sovereign_hall.agents.agent import Agent
from sovereign_hall.agents import get_all_personas
from sovereign_hall.core import AgentRole
from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
from sovereign_hall.services.llm_client import LLMClient
from sovereign_hall.run_discussion import (
    aggregate_committee_decision,
    enforce_strict_committee_vote_results,
    execute_durable_committee_stage,
    merge_committee_retry_results,
    parse_strict_committee_vote,
    parse_committee_vote,
)


VOTE = json.dumps({
    "direction": "hold", "confidence": 0.4, "position": 0.0,
    "key_evidence": ["missing current revenue filing"],
    "risk_flags": [], "invalid_if": "obtain independently verified filing",
})


@pytest.mark.parametrize("direction", ["long", "short", "hold"])
def test_absent_envelope_never_counts_in_public_aggregation(direction):
    vote = json.dumps({**json.loads(VOTE), "direction": direction,
                       "position": 0.1, "confidence": 0.9})
    absent = "  [committee_task_absent] reason=structured_output_incomplete\n" + vote
    assert not parse_committee_vote(absent)["is_valid"]
    assert parse_strict_committee_vote(absent)["schema_error"] == "committee_task_absent"
    decision = aggregate_committee_decision({}, [vote, absent], vote_labels=["valid", "absent"])
    assert decision["parsed_vote_count"] == 1
    assert not decision["vote_quorum_met"]
    assert decision["individual_votes"][1]["effective_weight"] == 0
    # A later completed response remains eligible under the original rules.
    recovered = aggregate_committee_decision({}, [vote, vote])
    assert recovered["parsed_vote_count"] == 2 and recovered["vote_quorum_met"]


@pytest.mark.asyncio
@pytest.mark.parametrize("content,reasoning,finish,accepted", [
    (VOTE, "intermediate deliberation", "stop", True),
    ("", VOTE, "length", False),
    ("", VOTE, "stop", False),
    (VOTE, "", "length", False),
    (VOTE, "", None, False),
])
async def test_structured_vote_requires_completed_final_content(
    content, reasoning, finish, accepted,
):
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        response = {
            "choices": [{
                "message": {
                    "content": content,
                    "reasoning_content": reasoning,
                },
                "finish_reason": finish,
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
            },
        }
        return httpx.Response(200, json=response)

    client = LLMClient.__new__(LLMClient)
    client.base_url = "https://offline.invalid/v1"
    client.model = "offline-fixture"
    client.model_uuid = None
    client.api_key = "offline-fixture"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._http_client = transport
        call = client._openai_chat([], 0.1, 100, json_output=True)
        if accepted:
            result, _ = await call
            assert result == VOTE
        else:
            result, _ = await call
            assert result.startswith("[committee_task_absent]")
            assert "structured_output_incomplete" in result
            assert (content or reasoning) in result
            assert parse_strict_committee_vote(result)["schema_valid"] is False
    assert requests[0]["response_format"] == {"type": "json_object"}
    assert requests[0]["stream"] is False
    assert "stream_options" not in requests[0]


@pytest.mark.asyncio
async def test_unstructured_chat_keeps_stream_transport():
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        chunk = {
            "choices": [{
                "delta": {"content": "plain response"},
                "finish_reason": "stop",
            }],
        }
        return httpx.Response(
            200,
            text="data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n",
        )

    client = LLMClient.__new__(LLMClient)
    client.base_url = "https://offline.invalid/v1"
    client.model = "offline-fixture"
    client.model_uuid = None
    client.api_key = "offline-fixture"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._http_client = transport
        result, _ = await client._openai_chat([], 0.1, 100)
    assert result == "plain response"
    assert requests[0]["stream"] is True
    assert requests[0]["stream_options"] == {"include_usage": True}


@pytest.mark.asyncio
async def test_structured_vote_disables_duplicate_model_reasoning_only():
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={
            "choices": [{
                "message": {"content": VOTE, "reasoning": None},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
            },
        })

    client = LLMClient.__new__(LLMClient)
    client.base_url = "https://offline.invalid/v1"
    client.model = "offline-fixture"
    client.model_uuid = None
    client.api_key = "offline-fixture"
    client.structured_output_reasoning_effort = "none"
    client.structured_output_use_response_format = False
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._http_client = transport
        result, _ = await client._openai_chat([], 0.1, 100, json_output=True)

    assert result == VOTE
    assert requests[0]["reasoning_effort"] == "none"
    assert "response_format" not in requests[0]


@pytest.mark.asyncio
async def test_reasoning_effort_never_changes_unstructured_analysis_transport():
    requests = []

    async def handler(request):
        requests.append(json.loads(request.content))
        chunk = {
            "choices": [{
                "delta": {"content": "analysis result"},
                "finish_reason": "stop",
            }],
        }
        return httpx.Response(
            200,
            text="data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n",
        )

    client = LLMClient.__new__(LLMClient)
    client.base_url = "https://offline.invalid/v1"
    client.model = "offline-fixture"
    client.model_uuid = None
    client.api_key = "offline-fixture"
    client.structured_output_reasoning_effort = "none"
    client.structured_output_use_response_format = False
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._http_client = transport
        result, _ = await client._openai_chat([], 0.1, 100)

    assert result == "analysis result"
    assert "reasoning_effort" not in requests[0]


@pytest.mark.asyncio
async def test_json_vote_bypasses_unconstrained_llm_cache():
    client = LLMClient(provider="local")
    client._get_cached = lambda key: "old unstructured response"
    client._openai_chat = AsyncMock(return_value=(VOTE, {}))
    try:
        result = await client.chat("JSON only", "vote", json_output=True)
        assert result == VOTE
        assert client._openai_chat.call_args.kwargs == {"json_output": True}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_agent_forwards_final_vote_contract_without_changing_analysis():
    llm = AsyncMock()
    llm.chat.return_value = VOTE
    agent = Agent(AgentRole.CIO, llm)
    await agent.think("vote", use_memory=False, json_output=True)
    assert llm.chat.call_args.kwargs["json_output"] is True
    system = llm.chat.call_args.kwargs["system"]
    assert "单个完整JSON对象" in system
    assert "输出使用Markdown" not in system
    assert "直接回答“无新增证据”" not in system
    assert "不得补造事实" in system
    await agent.think("analyze", use_memory=False)
    assert "json_output" not in llm.chat.call_args.kwargs
    assert "输出使用Markdown" in llm.chat.call_args.kwargs["system"]


@pytest.mark.parametrize("role", list(get_all_personas()))
def test_every_persona_preserves_identity_with_exclusive_json_contract(role):
    from sovereign_hall.agents import get_persona

    persona = get_persona(role)
    if persona is None:
        pytest.fail(f"Missing persona for {role}")
    prompt = persona.get_system_prompt(json_output=True)
    assert persona.prompt_prefix in prompt
    assert "【最终JSON裁决要求】" in prompt
    assert "【输出要求】" not in prompt
    assert "hold/abstain" in prompt


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [
    "unfinished analysis",
    "[committee_task_absent] reason=structured_output_incomplete\n" + VOTE,
])
async def test_invalid_vote_checkpoint_retries_then_resumes_valid_turn(tmp_path, invalid):
    db = DatabaseService(str(tmp_path / "committee.db"))
    await db._init_db()
    try:
        round_state = await ResearchRoundCoordinator(db).start(
            base_topic="offline regression", research_objective="recover invalid final vote",
        )
        store = CommitteeMeetingStore(db)
        meeting_id = await store.start_or_resume(
            round_id=round_state.id,
            proposal={"proposal_id": "offline-proposal", "ticker": "TEST"},
            review_depth="focused",
        )
        factory = AsyncMock(side_effect=[invalid, VOTE])
        specs = [{"label": "CIO", "factory": factory, "input_material": "JSON vote"}]
        options = dict(timeout_seconds=1, stage="round4_vote", max_concurrency=1,
                       meeting_store=store, meeting_id=meeting_id,
                       parse_output=parse_strict_committee_vote)
        _, first = await execute_durable_committee_stage(specs, **options)
        second_result, second = await execute_durable_committee_stage(specs, **options)
        third_result, third = await execute_durable_committee_stage(specs, **options)
        assert first["tasks"][0]["parse_mode"] == "schema_invalid"
        assert factory.await_count == 2
        assert second["checkpoint_resumed_count"] == 0
        assert third["checkpoint_resumed_count"] == 1
        assert second_result == third_result == [VOTE]
        conn = await db._get_connection()
        async with conn.execute("SELECT raw_output FROM committee_turns ORDER BY id") as cur:
            assert [r[0] for r in await cur.fetchall()] == [invalid, VOTE]
        async with conn.execute("SELECT COUNT(*) FROM execution_intents") as cur:
            assert (await cur.fetchone())[0] == 0
    finally:
        await db.close()


@pytest.mark.parametrize("stage", ["round4_vote", "deployment_deadlock_review"])
def test_retry_recovery_counts_only_schema_valid_votes(stage):
    labels = ["CIO", "quant"]
    tasks = [{"label": name, "status": "completed"} for name in labels]
    results, audit = merge_committee_retry_results(
        ["first invalid", "second invalid"], {"tasks": tasks}, labels,
        ["still deliberating", VOTE], {"tasks": tasks}, stage=stage,
    )
    assert audit["retry_recovered_count"] == 1
    assert results == ["first invalid", VOTE]
    strict, final = enforce_strict_committee_vote_results(results, audit, labels, stage=stage)
    decision = aggregate_committee_decision({}, strict, vote_labels=labels)
    assert final["schema_invalid_count"] == 1
    assert decision["parsed_vote_count"] == 1
    assert decision["vote_quorum_met"] is False
    assert decision["individual_votes"][0]["effective_weight"] == 0
    assert audit["retry_attempt"]["tasks"] == tasks
