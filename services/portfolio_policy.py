"""Deterministic portfolio lifecycle rules for realtime-quoted simulated investing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PositionReview:
    ticker: str
    action: str
    reason: str
    pnl_pct: float | None
    holding_days: int | None
    price_age_days: int | None
    price: float | None
    price_at: str
    price_source: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_date(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value)[:10])
        except (TypeError, ValueError):
            return None


def review_position(
    ticker: str,
    avg_cost: float,
    opened_at: Any,
    price: float | None,
    price_at: Any,
    price_source: str,
    *,
    now: datetime | None = None,
    max_price_age_days: int = 3,
    stop_loss_pct: float = -0.08,
    take_profit_pct: float = 0.15,
    max_holding_days: int = 30,
) -> PositionReview:
    """Review one simulated holding without accepting fabricated or stale marks."""
    now_value = now or datetime.now()
    opened = _parse_date(opened_at)
    holding_days = max(0, (now_value.date() - opened.date()).days) if opened else None
    mark_time = _parse_date(price_at)
    price_age_days = max(0, (now_value.date() - mark_time.date()).days) if mark_time else None
    realtime_source = "realtime" in str(price_source).lower()
    max_duration_breached = (
        holding_days is not None and holding_days >= max_holding_days
    )

    if not realtime_source:
        if max_duration_breached:
            return PositionReview(
                ticker=ticker,
                action="exit",
                reason=(
                    f"最大持有期触发: {holding_days}d >= {max_holding_days}d；"
                    "当前报价非实时，仅形成price-free退出裁决，成交时必须重新取得实时行情"
                ),
                pnl_pct=None,
                holding_days=holding_days,
                price_age_days=price_age_days,
                price=price,
                price_at=str(price_at or ""),
                price_source=price_source,
            )
        return PositionReview(
            ticker=ticker,
            action="blocked_non_realtime_price",
            reason="报价不是实时行情；禁止使用本地估值、历史预测价或成本价进行复核/成交",
            pnl_pct=None,
            holding_days=holding_days,
            price_age_days=price_age_days,
            price=price,
            price_at=str(price_at or ""),
            price_source=price_source,
        )

    if price is None or price <= 0 or price_age_days is None or price_age_days > max_price_age_days:
        age_text = "unknown" if price_age_days is None else str(price_age_days)
        realtime_missing = price is None or price <= 0
        if max_duration_breached:
            quote_blocker = (
                "实时行情缺失"
                if realtime_missing
                else f"实时行情已过期（age={age_text}d, limit={max_price_age_days}d）"
            )
            return PositionReview(
                ticker=ticker,
                action="exit",
                reason=(
                    f"最大持有期触发: {holding_days}d >= {max_holding_days}d；"
                    f"{quote_blocker}，仅形成price-free退出裁决，成交时必须重新取得实时行情"
                ),
                pnl_pct=None,
                holding_days=holding_days,
                price_age_days=price_age_days,
                price=price,
                price_at=str(price_at or ""),
                price_source=price_source,
            )
        return PositionReview(
            ticker=ticker,
            action="blocked_realtime_price" if realtime_missing else "blocked_stale_price",
            reason=(
                "无法获取实时现价；禁止使用本地估值、历史预测价或成本价替代"
                if realtime_missing
                else f"价格已过期或时间不可验证（age={age_text}d, limit={max_price_age_days}d）"
            ),
            pnl_pct=None,
            holding_days=holding_days,
            price_age_days=price_age_days,
            price=price,
            price_at=str(price_at or ""),
            price_source=price_source,
        )

    pnl_pct = price / avg_cost - 1.0 if avg_cost > 0 else None
    if pnl_pct is not None and pnl_pct <= stop_loss_pct:
        action = "exit"
        reason = f"止损触发: {pnl_pct:.1%} <= {stop_loss_pct:.1%}"
    elif pnl_pct is not None and pnl_pct >= take_profit_pct:
        action = "exit"
        reason = f"止盈触发: {pnl_pct:.1%} >= {take_profit_pct:.1%}"
    elif holding_days is not None and holding_days >= max_holding_days:
        action = "exit"
        reason = f"最大持有期触发: {holding_days}d >= {max_holding_days}d"
    else:
        action = "hold"
        reason = "持仓仍在止损/止盈/最大持有期边界内"

    return PositionReview(
        ticker=ticker,
        action=action,
        reason=reason,
        pnl_pct=pnl_pct,
        holding_days=holding_days,
        price_age_days=price_age_days,
        price=price,
        price_at=str(price_at or ""),
        price_source=price_source,
    )


def deployment_status(cash: float, total_assets: float, target_invested_ratio: float = 1.0) -> dict[str, float]:
    """Describe capital deployment; residual cash is operational, never strategic risk reserve."""
    total = max(float(total_assets or 0.0), 0.0)
    cash_value = max(float(cash or 0.0), 0.0)
    target = min(max(float(target_invested_ratio), 0.0), 1.0)
    invested = max(total - cash_value, 0.0)
    invested_ratio = invested / total if total > 0 else 0.0
    target_value = total * target
    return {
        "target_invested_ratio": target,
        "invested_ratio": invested_ratio,
        "deployment_gap": max(target_value - invested, 0.0),
        "cash": cash_value,
        "total_assets": total,
    }


def deployment_position_floor(deployment_gap: float, total_assets: float, candidate_count: int) -> float:
    """Allocate the undeployed ratio evenly across approved new long candidates."""
    if total_assets <= 0 or candidate_count <= 0:
        return 0.0
    return max(float(deployment_gap), 0.0) / float(total_assets) / int(candidate_count)
