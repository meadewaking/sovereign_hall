"""Describe prediction-path feedback without turning it into thesis or PnL proof.

The validator's result is conditional on its recorded action. In particular,
HOLD/wrong/missed_upside is a missed price move, not a failed bullish thesis.
This module only renders existing observations; it never regrades a prediction.
"""
from __future__ import annotations

from datetime import datetime
import math
from zoneinfo import ZoneInfo

VERSION = "action_conditioned_feedback_v1"
ZONE = ZoneInfo("Asia/Shanghai")
MEANINGS = {
    "missed_upside": "观望后触及上涨边界，记录为错失价格上涨",
    "avoided_downside": "观望后触及下跌边界，记录为避开价格下跌",
    "neutral_band_held": "观望窗口内价格保持在中性带",
    "neutral_band_both_sides": "同根日线穿越中性带两侧，路径先后不明",
    "target": "该方向的价格目标被触及",
    "stop_loss": "该方向的价格止损边界被触及",
}


def _time(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=ZONE) if result.tzinfo is None else result
    except (TypeError, ValueError):
        return None


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def prediction_feedback(row, *, as_of=None):
    """Return a conservative, action-specific interpretation of one source row.

    An explicit cutoff also hides outcomes validated later. Archived memory's
    availability gate remains necessary: mutable live rows are not replay data.
    Missing or conflicting legacy fields remain ambiguous rather than inferred.
    """
    cutoff = _time(as_of) if as_of is not None else datetime.now(ZONE)
    validated = _time(row.get("validated_at"))
    action = str(row.get("direction") or "unknown").lower()
    normalized = "hold" if action in {"hold", "neutral", "watch"} else (
        "long" if action == "buy" else "short" if action == "sell" else action)
    card = dict(version=VERSION, prediction_id=row.get("id"), round_id=row.get("round_id"),
                ticker=row.get("ticker"), action=action,
                status=row.get("status"), result=row.get("result"),
                predicted_at=row.get("predicted_at"), validated_at=row.get("validated_at"),
                outcome="unresolved", interpretation="尚无可用的已验证价格路径结果",
                underlying_price_return=None, thesis_verdict="not_evaluated",
                realized_net_account_return=None)
    if row.get("status") != "validated" or row.get("result") not in {"correct", "wrong", "partial"}:
        return card
    if validated is None:
        card.update(outcome="legacy_outcome_only", interpretation="旧记录缺少验证时间；保留原标签，不认定已观察到价格结果")
        return card
    if cutoff is None or validated > cutoff:
        card.update(status="not_yet_available", result="unknown", validated_at=None)
        return card
    hit = str(row.get("actual_hit_type") or "")
    expected = ({"missed_upside": "wrong", "avoided_downside": "correct",
                 "neutral_band_held": "correct", "neutral_band_both_sides": "partial"}
                if normalized == "hold" else {"target": "correct", "stop_loss": "wrong"}
                if normalized in {"long", "short"} else {})
    if hit not in expected:
        card.update(outcome="legacy_outcome_only", interpretation="仅有旧结果标签，缺少可解释的动作与触发类型")
        return card
    entry, end = _number(row.get("current_price")), _number(row.get("actual_hit_price"))
    change = _number(row.get("actual_return"))
    derived = end / entry - 1 if entry is not None and entry > 0 and end is not None and end > 0 else None
    if (expected[hit] != row.get("result")
            or (change is not None and derived is not None and not math.isclose(change, derived, abs_tol=1e-7))
            or (change is not None and hit == "missed_upside" and change <= 0)
            or (change is not None and hit == "avoided_downside" and change >= 0)):
        card.update(outcome="inconsistent_record", interpretation="结果、动作或价格记录冲突，须核查原记录")
        return card
    card.update(outcome=hit, interpretation=MEANINGS[hit],
                underlying_price_return=change if change is not None else derived,
                endpoint_date=row.get("actual_hit_date"))
    return card


def format_prediction_feedback(row, *, as_of=None):
    card = prediction_feedback(row, as_of=as_of)
    change = card["underlying_price_return"]
    price = f"{change:+.2%}" if change is not None else "N/A"
    return (
        f"预测[{card['prediction_id']}]: {card['ticker']} {card['action']} | "
        f"状态={card['status']} 结果={card['result']} | "
        f"触发={card['outcome']}（{card['interpretation']}） | "
        f"标的价格变动={price}，不是账户净收益 | "
        f"预测时间={card['predicted_at']} 验证时间={card['validated_at']} | "
        f"来源轮次={card['round_id']} | 基本面因果与投资论点真伪=未评估"
    )
