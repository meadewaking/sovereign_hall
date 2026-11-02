"""PR2.4 — Append-only daily-price revisions and data-fetch failure log.

Every successful write to ``daily_prices`` is mirrored as an immutable row in
``daily_price_revisions`` with the raw response hash, fetch time, provider,
adjustment method and batch coverage.  A frozen evaluation references a
specific ``batch_id`` so a provider-side revision cannot silently rewrite
history; replay reads the revision rather than the mutable latest view.

Every failed fetch is logged to ``data_fetch_failures`` with a concrete
``next_action`` (retry / switch provider / import locally / escalate) so the
next run can pick a different recovery path instead of looping on the same
provider.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import aiosqlite

from ..infrastructure.sqlite.migrations import apply_architecture_migrations


@dataclass(frozen=True)
class DailyPriceRevision:
    batch_id: str
    ticker: str
    date: str
    close: float
    provider: str
    adjustment: str
    fetched_at: str
    raw_response_hash: str
    batch_coverage_start: str
    batch_coverage_end: str
    quality_status: str
    trade_status: str


@dataclass(frozen=True)
class DataFetchFailure:
    id: int | None
    ticker: str | None
    date: str | None
    provider: str | None
    error: str
    next_action: str
    context: dict


class DailyPriceRevisionStore:
    """Append-only revision log for daily prices and fetch failures."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    @staticmethod
    def hash_raw_response(raw: str | bytes | Mapping | Sequence) -> str:
        if isinstance(raw, Mapping | Sequence) and not isinstance(raw, str | bytes):
            payload = json.dumps(
                list(raw) if isinstance(raw, Sequence) and not isinstance(raw, Mapping) else raw,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        elif isinstance(raw, str):
            payload = raw.encode("utf-8")
        elif isinstance(raw, bytes):
            payload = raw
        else:
            payload = str(raw).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    async def append_batch(
        self,
        *,
        batch_id: str,
        provider: str,
        adjustment: str,
        bars: Iterable[Mapping[str, object]],
        raw_response: str | bytes | Mapping | Sequence | None = None,
        quality_status: str = "validated",
        default_trade_status: str = "normal",
        fetched_at: str | None = None,
    ) -> tuple[int, list[DailyPriceRevision]]:
        """Persist one batch of daily bars as an immutable revision set.

        Returns ``(row_count, revisions)``.  Revisions are append-only; the
        mutable ``daily_prices`` table is also upserted so legacy callers
        keep working against the latest view.
        """
        await self.initialize()
        timestamp = fetched_at or datetime.now().isoformat()
        raw_hash = (
            self.hash_raw_response(raw_response)
            if raw_response is not None
            else ""
        )
        bar_list = [dict(bar) for bar in bars]
        if not bar_list:
            return (0, [])
        coverage_dates = [
            str(bar.get("date") or "")[:10] for bar in bar_list if bar.get("date")
        ]
        coverage_start = min(coverage_dates) if coverage_dates else None
        coverage_end = max(coverage_dates) if coverage_dates else None
        revisions: list[DailyPriceRevision] = []
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for bar in bar_list:
                    ticker = str(bar.get("ticker") or bar.get("code") or "")
                    bar_date = str(bar.get("date") or "")[:10]
                    if not ticker or not bar_date:
                        continue
                    close = float(bar.get("close") or 0.0)
                    open_ = bar.get("open")
                    high = bar.get("high")
                    low = bar.get("low")
                    volume = bar.get("volume")
                    trade_status = str(bar.get("trade_status") or default_trade_status)
                    await conn.execute(
                        """
                        INSERT INTO daily_price_revisions (
                            batch_id, ticker, date, open, high, low, close,
                            volume, source, adjustment, provider, fetched_at,
                            raw_response_hash, batch_coverage_start,
                            batch_coverage_end, quality_status, trade_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch_id,
                            ticker,
                            bar_date,
                            float(open_) if open_ is not None else None,
                            float(high) if high is not None else None,
                            float(low) if low is not None else None,
                            close,
                            float(volume) if volume is not None else None,
                            str(bar.get("source") or provider),
                            adjustment,
                            provider,
                            timestamp,
                            raw_hash,
                            coverage_start,
                            coverage_end,
                            quality_status,
                            trade_status,
                        ),
                    )
                    # Mirror into the mutable latest-view so legacy callers
                    # keep working.  This upsert does NOT lose history: the
                    # revision row above is the source of truth for replay.
                    await conn.execute(
                        """
                        INSERT INTO daily_prices (
                            ticker, date, open, high, low, close, volume,
                            source, adjustment, provider, fetched_at,
                            quality_status, trade_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(ticker, date) DO UPDATE SET
                            open = excluded.open,
                            high = excluded.high,
                            low = excluded.low,
                            close = excluded.close,
                            volume = excluded.volume,
                            source = excluded.source,
                            adjustment = excluded.adjustment,
                            provider = excluded.provider,
                            fetched_at = excluded.fetched_at,
                            quality_status = excluded.quality_status,
                            trade_status = excluded.trade_status
                        """,
                        (
                            ticker,
                            bar_date,
                            float(open_) if open_ is not None else None,
                            float(high) if high is not None else None,
                            float(low) if low is not None else None,
                            close,
                            float(volume) if volume is not None else None,
                            str(bar.get("source") or provider),
                            adjustment,
                            provider,
                            timestamp,
                            quality_status,
                            trade_status,
                        ),
                    )
                    revisions.append(
                        DailyPriceRevision(
                            batch_id=batch_id,
                            ticker=ticker,
                            date=bar_date,
                            close=close,
                            provider=provider,
                            adjustment=adjustment,
                            fetched_at=timestamp,
                            raw_response_hash=raw_hash,
                            batch_coverage_start=coverage_start or "",
                            batch_coverage_end=coverage_end or "",
                            quality_status=quality_status,
                            trade_status=trade_status,
                        )
                    )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return (len(revisions), revisions)

    async def read_revision(
        self,
        *,
        batch_id: str,
        ticker: str,
        bar_date: str,
    ) -> DailyPriceRevision | None:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT batch_id, ticker, date, close, provider, adjustment,
                       fetched_at, raw_response_hash, batch_coverage_start,
                       batch_coverage_end, quality_status, trade_status
                FROM daily_price_revisions
                WHERE batch_id = ? AND ticker = ? AND date = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (batch_id, ticker, bar_date),
            ) as cursor:
                row = await cursor.fetchone()
        if not row:
            return None
        return DailyPriceRevision(
            batch_id=str(row[0]),
            ticker=str(row[1]),
            date=str(row[2]),
            close=float(row[3]),
            provider=str(row[4]),
            adjustment=str(row[5]),
            fetched_at=str(row[6]),
            raw_response_hash=str(row[7] or ""),
            batch_coverage_start=str(row[8] or ""),
            batch_coverage_end=str(row[9] or ""),
            quality_status=str(row[10] or ""),
            trade_status=str(row[11] or ""),
        )

    async def record_failure(
        self,
        *,
        ticker: str | None,
        bar_date: str | None,
        provider: str | None,
        error: str,
        next_action: str,
        context: Mapping[str, object] | None = None,
    ) -> DataFetchFailure:
        """Log a failed fetch with a concrete next action.

        ``next_action`` must be one of: retry, switch_provider,
        import_locally, escalate.  The next run reads the most recent failure
        for a ticker/date and picks a different recovery path.
        """
        if next_action not in {
            "retry",
            "switch_provider",
            "import_locally",
            "escalate",
            "skip",
        }:
            raise ValueError(
                f"unknown next_action {next_action!r}; "
                "expected retry/switch_provider/import_locally/escalate/skip"
            )
        await self.initialize()
        payload = json.dumps(
            dict(context or {}), ensure_ascii=False, sort_keys=True, default=str
        )
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    """
                    INSERT INTO data_fetch_failures (
                        ticker, date, provider, error, next_action, context_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker,
                        bar_date,
                        provider,
                        error,
                        next_action,
                        payload,
                        datetime.now().isoformat(),
                    ),
                )
                await conn.commit()
                failure_id = cursor.lastrowid
            except BaseException:
                await conn.rollback()
                raise
        return DataFetchFailure(
            id=failure_id,
            ticker=ticker,
            date=bar_date,
            provider=provider,
            error=error,
            next_action=next_action,
            context=dict(context or {}),
        )

    async def latest_failure(
        self,
        *,
        ticker: str,
        bar_date: str | None = None,
    ) -> DataFetchFailure | None:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            if bar_date is None:
                async with conn.execute(
                    """
                    SELECT id, ticker, date, provider, error, next_action,
                           context_json
                    FROM data_fetch_failures
                    WHERE ticker = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (ticker,),
                ) as cursor:
                    row = await cursor.fetchone()
            else:
                async with conn.execute(
                    """
                    SELECT id, ticker, date, provider, error, next_action,
                           context_json
                    FROM data_fetch_failures
                    WHERE ticker = ? AND (date IS NULL OR date = ?)
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (ticker, bar_date),
                ) as cursor:
                    row = await cursor.fetchone()
        if not row:
            return None
        try:
            context = json.loads(str(row[6] or "{}"))
        except json.JSONDecodeError:
            context = {}
        return DataFetchFailure(
            id=int(row[0]) if row[0] is not None else None,
            ticker=str(row[1]) if row[1] is not None else None,
            date=str(row[2]) if row[2] is not None else None,
            provider=str(row[3]) if row[3] is not None else None,
            error=str(row[4]),
            next_action=str(row[5]),
            context=context,
        )
