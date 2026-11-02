"""Safety/semantic regressions; synthetic cases are not investment evidence."""
import copy
from datetime import datetime, timezone
import hashlib
import json

import pytest

from sovereign_hall.services.issuer_dossier import build_income_dossier
from sovereign_hall.services.evidence_council import (
    packet_from_dossiers, validate_ballot, compile_decisions, run_evidence_council,
)

NOW = datetime(2026, 9, 29, 6, tzinfo=timezone.utc)


def docs():
    result = []
    for ticker, profit in [("000001", 110), ("600015", 80)]:
        payload = {"success": True, "result": {"data": [
            {"SECURITY_CODE": ticker, "REPORT_DATE": "2026-06-30", "NOTICE_DATE": "2026-08-20",
             "PARENT_NETPROFIT": profit, "INTEREST_NI": 90},
            {"SECURITY_CODE": ticker, "REPORT_DATE": "2025-06-30", "NOTICE_DATE": "2025-08-20",
             "PARENT_NETPROFIT": 100, "INTEREST_NI": 100},
        ]}}
        ds, _ = build_income_dossier(payload, ticker=ticker, captured_at=NOW, cutoff=NOW)
        result.extend(d.to_dict() for d in ds)
    return result


def packet():
    return packet_from_dossiers(docs(), cutoff=NOW)


def ballot(p):
    return {"assessments": [
        {"ticker": t, "vote": "HOLD", "evidence": [
            {"fact_id": f"{t}:2026-06-30:PARENT_NETPROFIT",
             "predicate": "yoy_increase" if t == "000001" else "yoy_decrease"}],
         "priority_gaps": ["dividend_plan", "original_filing"]}
        for t in p["issuers"]]}


def test_fact_lineage_units_and_negative_evidence_survive_hold():
    p = packet(); b = ballot(p)
    parsed, errors = validate_ballot(json.dumps(b), p)
    assert not errors
    decisions = compile_decisions(p, parsed, [parsed, parsed])
    assert all(d["action"] == "HOLD" and d["quorum_met"] and not d["executable"] for d in decisions)
    assert all(d["research_queue"][0] == "dividend_plan" for d in decisions)
    negative = next(f for f in decisions[1]["facts"] if f["metric"] == "PARENT_NETPROFIT")
    assert negative["derived_yoy_pct"] == -20
    assert negative["period_kind"] == "cumulative" and negative["unit"] == "CNY"
    interest = next(f for f in decisions[0]["facts"] if f["metric"] == "INTEREST_NI")
    assert "不是净息差" in interest["label"] and not interest["independently_verified"]


@pytest.mark.parametrize("mutation", [
    lambda b: b["assessments"][0].update(thesis="息差收窄已由财报确认"),
    lambda b: b["assessments"][0]["evidence"][0].update(predicate="NIM_compression"),
    lambda b: b["assessments"][0]["evidence"][0].update(predicate="yoy_decrease"),
    lambda b: b["assessments"][0]["evidence"][0].update(fact_id="600015:2026-06-30:PARENT_NETPROFIT"),
    lambda b: b["assessments"][0]["evidence"][0].update(value=999),
    lambda b: b["assessments"][0].update(vote="BUY"),
    lambda b: b["assessments"][0].update(evidence=[]),
    lambda b: b["assessments"][0].update(priority_gaps=[]),
    lambda b: b["assessments"][0].update(priority_gaps=["synthetic_price"]),
    lambda b: b["assessments"][0].update(priority_gaps=[{}]),
    lambda b: b["assessments"].pop(),
])
def test_unsupported_claims_and_invalid_ballots_fail_explicitly(mutation):
    p = packet(); b = ballot(p); mutation(b)
    parsed, errors = validate_ballot(json.dumps(b), p)
    assert parsed is None and errors
    result = compile_decisions(p, ballot(p), [parsed, ballot(p)])
    assert all(d["reason"] == "invalid_model_output" and not d["quorum_met"] for d in result)


def test_duplicate_keys_and_non_json_not_sanitized():
    for raw in ['{"assessments": [], "assessments": []}', '```json\n{}\n```', '{}']:
        assert validate_ballot(raw, packet())[0] is None


def test_model_cannot_vote_away_missing_fields():
    p = packet(); b = ballot(p)
    for r in b["assessments"]: r["vote"] = "REFER"
    validated, errors = validate_ballot(json.dumps(b), p)
    assert not errors
    assert all(d["action"] == "HOLD" for d in compile_decisions(p, validated, [validated, validated]))


def test_referral_needs_two_seats_and_remains_non_executable():
    # Synthetic interface test only: income adapter cannot supply these gaps.
    p = packet(); b = ballot(p)
    for i in p["issuers"].values(): i["missing"] = []
    for r in b["assessments"]: r.update(vote="REFER", priority_gaps=[])
    validated, errors = validate_ballot(json.dumps(b), p)
    assert not errors
    assert all(d["action"] == "REFER" and not d["executable"] for d in compile_decisions(p, validated, [validated, validated]))
    assert all(d["action"] == "HOLD" for d in compile_decisions(p, validated, [validated]))


@pytest.mark.parametrize("kind", ["future", "naive", "content", "metadata", "duplicate"])
def test_corrupt_or_unavailable_dossier_rejected(kind):
    d = docs()
    if kind == "future": d[0]["metadata"]["crawled_at"] = "2026-09-30T06:00:00+00:00"
    if kind == "naive": d[0]["metadata"]["crawled_at"] = "2026-09-29T06:00:00"
    if kind == "content": d[0]["content"] += "伪造事实"
    if kind == "metadata": d[0]["metadata"]["facts"][0]["value"] = 999
    if kind == "duplicate": d.append(copy.deepcopy(d[0]))
    with pytest.raises(ValueError): packet_from_dossiers(d, cutoff=NOW)


def test_adapter_recalculates_growth_even_when_content_hash_is_consistent():
    d = docs(); old = d[0]["metadata"]["facts"][0]["derived_yoy_pct"]
    d[0]["metadata"]["facts"][0]["derived_yoy_pct"] = -999
    d[0]["content"] = d[0]["content"].replace(f'"derived_yoy_pct": {old}', '"derived_yoy_pct": -999', 1)
    d[0]["id"] = "issuer_dossier_v1_000001_" + hashlib.sha256(d[0]["content"].encode()).hexdigest()[:16]
    with pytest.raises(ValueError, match="arithmetic"): packet_from_dossiers(d, cutoff=NOW)


@pytest.mark.asyncio
async def test_calls_are_independent_and_only_public_packet_is_sent():
    p = packet(); requests = []; recorded = []
    class Client:
        async def chat(self, **kw):
            requests.append(kw)
            return json.dumps(ballot(p))
    async def record(*args): recorded.append(args)
    result = await run_evidence_council(Client(), p, topic="synthetic research", record=record)
    assert result["status"] == "completed" and len(requests) == 3
    assert requests[1]["user"] == requests[2]["user"]
    assert len(recorded) == 6 and all(r["json_output"] and not r["use_cache"] for r in requests)


@pytest.mark.asyncio
async def test_invalid_proposal_stops_before_committee():
    calls = []
    class Client:
        async def chat(self, **kw): calls.append(kw); return '{"fabricated": "nim"}'
    async def record(*args): pass
    result = await run_evidence_council(Client(), packet(), topic="synthetic", record=record)
    assert result["status"] == "invalid_proposal" and len(calls) == 1
    assert all(not d["quorum_met"] for d in result["decisions"])
