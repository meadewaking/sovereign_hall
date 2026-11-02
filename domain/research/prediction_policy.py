"""Pure policy helpers for durable, price-free prediction creation."""

from __future__ import annotations


HOLD_NEUTRAL_BAND = 0.05


def normalize_prediction_price_targets(
    *,
    decision: str,
    entry_price: float,
    target_price: float,
    stop_loss: float,
) -> tuple[float, float]:
    """Resolve percentage-like proposal targets against a real quote."""
    direction = (decision or "").lower()
    if direction in ("hold", "neutral", "watch", "观望"):
        return (
            round(entry_price * (1 + HOLD_NEUTRAL_BAND), 4),
            round(entry_price * (1 - HOLD_NEUTRAL_BAND), 4),
        )

    if (
        target_price > 1
        and stop_loss > 1
        and target_price <= 100
        and stop_loss <= 100
        and (
            (
                direction in ("sell", "short")
                and target_price < entry_price < stop_loss
            )
            or (
                direction not in ("sell", "short")
                and stop_loss < entry_price < target_price
            )
        )
        and (
            abs(target_price - entry_price) / entry_price > 0.3
            or abs(entry_price - stop_loss) / entry_price > 0.3
        )
    ):
        if direction in ("sell", "short"):
            return (
                round(entry_price * (1 - target_price / 100), 4),
                round(entry_price * (1 + stop_loss / 100), 4),
            )
        return (
            round(entry_price * (1 + target_price / 100), 4),
            round(entry_price * (1 - stop_loss / 100), 4),
        )

    def as_take_profit(value: float) -> float:
        if value <= 0:
            return entry_price * (
                0.92 if direction in ("sell", "short") else 1.08
            )
        if value <= 1:
            return entry_price * (
                1 - value if direction in ("sell", "short") else 1 + value
            )
        if direction in ("sell", "short") and value >= entry_price:
            return entry_price * (1 - value / 100)
        if direction not in ("sell", "short") and value <= entry_price:
            return entry_price * (1 + value / 100)
        if entry_price < 10 and value > entry_price * 3 and value <= 100:
            return entry_price * (
                1 - value / 100
                if direction in ("sell", "short")
                else 1 + value / 100
            )
        return value

    def as_stop(value: float) -> float:
        if value <= 0:
            return entry_price * (
                1.05 if direction in ("sell", "short") else 0.95
            )
        if value <= 1:
            return entry_price * (
                1 + value if direction in ("sell", "short") else 1 - value
            )
        if direction in ("sell", "short") and value <= entry_price:
            return entry_price * (1 + value / 100)
        if direction not in ("sell", "short") and value >= entry_price:
            return entry_price * (1 - value / 100)
        if entry_price < 10 and value > entry_price:
            return entry_price * (
                1.05 if direction in ("sell", "short") else 0.95
            )
        return value

    return round(as_take_profit(target_price), 4), round(as_stop(stop_loss), 4)
