"""One fail-closed event-time boundary for paper execution and valuation."""
from datetime import datetime
import math
from zoneinfo import ZoneInfo

MARKET_ZONE = ZoneInfo('Asia/Shanghai')


def retrieved_after(quote, not_before, *, now=None):
    """Retrieval must follow a durable intent/request, with no future tolerance.

    Provider event time may precede an intent while still being fresh. It is
    retrieval time that establishes the new observation; neither a cache hit
    nor replacing its timestamp establishes that observation.
    """
    try:
        def instant(value):
            result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return result.replace(tzinfo=MARKET_ZONE) if result.tzinfo is None else result
        current = now or datetime.now(MARKET_ZONE)
        if current.tzinfo is None:
            current = current.replace(tzinfo=MARKET_ZONE)
        return instant(not_before) <= instant(quote['fetched_at']) <= current
    except (TypeError, KeyError, ValueError, OverflowError):
        return False


def quote_age_seconds(fetched_at, quoted_at, *, now=None):
    """Age of the older timestamp. Retrieval cannot rejuvenate a market tick.

    Legacy local timestamps are Shanghai time, never the host's arbitrary TZ.
    Missing source times remain unknown; historical rows are not backfilled.
    """
    try:
        current = now or datetime.now(MARKET_ZONE)
        if current.tzinfo is None:
            current = current.replace(tzinfo=MARKET_ZONE)
        ages = []
        for value in (fetched_at, quoted_at):
            instant = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if instant.tzinfo is None:
                instant = instant.replace(tzinfo=MARKET_ZONE)
            age = (current - instant).total_seconds()
            if age < -60:
                return None
            ages.append(age)
        return max(0.0, *ages)
    except (TypeError, ValueError, OverflowError):
        return None


def is_fresh_quote(quote, *, max_age_seconds=120, now=None):
    if not isinstance(quote, dict) or quote.get('valuation_only') is True:
        return False
    try:
        price = float(quote.get('price'))
        age = quote_age_seconds(quote.get('fetched_at'), quote.get('quoted_at'), now=now)
        return (math.isfinite(price) and price > 0
                and bool(str(quote.get('source') or '').strip())
                and age is not None and age <= max_age_seconds)
    except (TypeError, ValueError, OverflowError):
        return False
