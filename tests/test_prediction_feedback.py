"""Keep action-conditioned feedback faithful at the research-memory boundary."""
import json

import pytest

from sovereign_hall.services.prediction_feedback import (
    format_prediction_feedback, prediction_feedback,
)
from sovereign_hall.services.learning_engine import LearningEngine
from sovereign_hall.services.research_memory import retrieve_memory
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.storage_archive import content_hash


def observation(**changes):
    return dict(id="hold-missed", round_id="source-round", ticker="300026", direction="hold",
                status="validated", result="wrong", current_price=3.18,
                actual_hit_price=3.339, actual_return=.05, actual_hit_type="missed_upside",
                actual_hit_date="2026-10-12", predicted_at="2026-09-27T08:27:47",
                validated_at="2026-10-27T08:33:26", **changes)


def changed(**changes):
    return {**observation(), **changes}


def test_real_hold_missed_upside_is_not_a_failed_bullish_thesis_or_account_loss():
    row = observation()
    original = dict(row)
    card = prediction_feedback(row, as_of="2026-10-27T09:00:00+08:00")
    assert card["outcome"] == "missed_upside"
    assert card["underlying_price_return"] == .05
    assert card["thesis_verdict"] == "not_evaluated"
    assert card["realized_net_account_return"] is None
    assert "错失" in card["interpretation"] and "不是账户净收益" in format_prediction_feedback(row)
    assert row == original


@pytest.mark.parametrize("direction,result,hit,change", [
    ("hold", "correct", "avoided_downside", -.05),
    ("watch", "correct", "neutral_band_held", 0),
    ("neutral", "partial", "neutral_band_both_sides", .01),
    ("long", "wrong", "stop_loss", -.05),
    ("short", "correct", "target", -.05),
])
def test_other_price_path_types_preserve_action_and_underlying_price_sign(direction, result, hit, change):
    card = prediction_feedback(changed(direction=direction, result=result, actual_hit_type=hit,
        actual_return=change, actual_hit_price=3.18 * (1 + change)))
    assert card["outcome"] == hit
    assert card["underlying_price_return"] == change
    assert card["realized_net_account_return"] is None


@pytest.mark.parametrize("changes", [
    {"result": "correct"}, {"actual_return": -.05}, {"actual_hit_price": 1.0},
    {"direction": "long"},
])
def test_conflicting_fields_cannot_become_a_settled_outcome(changes):
    card = prediction_feedback(changed(**changes))
    assert card["outcome"] in {"inconsistent_record", "legacy_outcome_only"}
    assert card["underlying_price_return"] is None


def test_unknown_legacy_result_never_infers_missed_upside_from_wrong_alone():
    card = prediction_feedback(changed(actual_hit_type=None))
    assert card["outcome"] == "legacy_outcome_only"
    assert card["underlying_price_return"] is None


@pytest.mark.parametrize("changes,cutoff", [
    ({"status": "pending", "result": "unknown"}, "2026-10-28T09:00:00+08:00"),
    ({}, "2026-10-27T08:00:00+08:00"),
    ({}, "invalid-cutoff"),
])
def test_pending_future_or_invalid_cutoff_cannot_publish_observed_price_return(changes, cutoff):
    card = prediction_feedback(changed(**changes), as_of=cutoff)
    assert card["outcome"] == "unresolved"
    assert card["underlying_price_return"] is None


async def test_outcome_cards_survive_narrative_budget_and_archived_availability(tmp_path):
    db = DatabaseService(str(tmp_path / "memory.db"))
    await db._init_db()
    try:
        prediction = observation()
        memory = dict(schema_version=1, round_id="source-round", topic="中药配方颗粒集采",
            completed_at="2026-09-27T08:28:10", status="completed", failure_reason=None,
            conclusions=[dict(id=1, conclusion="历史声称HOLD/wrong证明看多逻辑错误。" * 300)],
            reflections=[], predictions=[prediction], decisions=[], evidence=[])
        await db._connection.execute("INSERT INTO research_memories VALUES (?,?,?,?,?,?,?)",
            (memory["round_id"], content_hash(memory), memory["topic"], "2026-10-27T01:00:00+00:00",
             memory["completed_at"], memory["status"], json.dumps(memory, ensure_ascii=False)))
        await db._connection.commit()
        prompt = await retrieve_memory(db.db_path, memory["topic"], as_of="2026-10-27T10:00:00+08:00", max_chars=700)
        assert len(prompt) <= 700
        assert "missed_upside" in prompt and "+5.00%" in prompt
        assert "source-round" in prompt
        assert "历史声称" not in prompt
        assert await retrieve_memory(db.db_path, memory["topic"], as_of="2026-10-27T08:30:00+08:00") == ""
    finally:
        await db.close()


async def test_global_lessons_distinguish_missed_opportunities_and_do_not_command_smaller_hold(tmp_path):
    from sovereign_hall.services.decision_tracker import ensure_prediction_tables
    path = str(tmp_path / "predictions.db")
    await ensure_prediction_tables(path)
    import sqlite3
    row = observation()
    row.update(confidence=.6, expected_days=30, discussion_context="原看多提案改为观望")
    with sqlite3.connect(path) as conn:
        fields = list(row)
        conn.execute(f"INSERT INTO price_predictions({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                     tuple(row.values()))
    prompt = await LearningEngine(path).generate_lessons_prompt()
    assert "missed_upside" in prompt and "错失上涨" in prompt
    assert "必须降低仓位" not in prompt
    assert "not_evaluated" not in prompt  # The user-facing form uses plain Chinese.
