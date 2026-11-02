#!/usr/bin/env python3
"""Backfill validated local daily OHLC rows into ``daily_prices``.

The heuristic cycle intentionally refuses to treat prediction ``current_price``
as independent market data. The default repair path is local-only CSV import
or status validation; fetching through MarketDataService requires an explicit
opt-in flag so automation cannot accidentally call external market services.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sovereign_hall.services.market_data import get_market_data  # noqa: E402
from sovereign_hall.services.prediction_store import ensure_prediction_tables  # noqa: E402
from sovereign_hall.domain.portfolio.instruments import (  # noqa: E402
    InstrumentProfile,
    is_etf_ticker,
    normalize_ticker,
)


@dataclass(frozen=True)
class BackfillRequest:
    ticker: str
    start: date
    end: date
    priority_rank: int = 0


def parse_date(value: object, fallback: date | None = None) -> date:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if text:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    if fallback is not None:
        return fallback
    return datetime.now().date()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def latest_backfill_plan(runs_root: Path) -> Path | None:
    plans = sorted(runs_root.glob("*/daily_price_backfill_plan.csv"))
    return plans[-1] if plans else None


def plan_items_from_csv(plan_path: Path, default_end: date) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with plan_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ticker = normalize_ticker(row.get("ticker"))
            if not ticker:
                continue
            start = parse_date(row.get("first_missing_signal_date"), default_end - timedelta(days=90))
            end = parse_date(
                row.get("last_missing_signal_date") or row.get("latest_signal_date"),
                default_end,
            )
            try:
                priority = int(row.get("priority_rank") or 0)
            except ValueError:
                priority = 0
            request = BackfillRequest(ticker=ticker, start=start, end=end, priority_rank=priority)
            items.append(
                {
                    "request": request,
                    "ticker": ticker,
                    "priority_rank": priority,
                    "missing_signal_days": _safe_int(row.get("missing_signal_days")),
                    "first_missing_signal_date": start.isoformat(),
                    "last_missing_signal_date": end.isoformat(),
                    "latest_signal_date": str(row.get("latest_signal_date", "") or "")[:10],
                    "missing_latest_signal_date": _safe_bool(row.get("missing_latest_signal_date")),
                    "minimum_rows_to_unblock_latest": _safe_int(row.get("minimum_rows_to_unblock_latest")),
                    "raw": row,
                }
            )
    return items


def requests_from_plan(
    plan_path: Path,
    default_end: date,
    lookback_days: int = 0,
) -> list[BackfillRequest]:
    """Build requests, optionally extending before the first signal date.

    Signal dates can fall on weekends or exchange holidays.  Coverage uses a
    bounded as-of match, so the fetch window must include the same preceding
    tolerance or a valid Friday bar is never requested for a Sunday signal.
    CSV coverage validation keeps the default exact-date window.
    """
    days = max(int(lookback_days), 0)
    requests = [item["request"] for item in plan_items_from_csv(plan_path, default_end)]
    if days:
        requests = [
            BackfillRequest(
                ticker=request.ticker,
                start=request.start - timedelta(days=days),
                end=request.end,
                priority_rank=request.priority_rank,
            )
            for request in requests
        ]
    return dedupe_requests(requests)


def requests_from_tickers(tickers: Iterable[str], start: date, end: date) -> list[BackfillRequest]:
    return [
        BackfillRequest(ticker=code, start=start, end=end, priority_rank=index + 1)
        for index, code in enumerate(dict.fromkeys(normalize_ticker(t) for t in tickers))
        if code
    ]


def requests_from_predictions(
    db_path: Path,
    start: date,
    end: date,
    lookback_days: int = 7,
    extend_to_end: bool = True,
) -> list[BackfillRequest]:
    """Request a lifecycle-safe price window for every local signal ticker.

    Fetching only through each ticker's last signal date leaves a held position
    unpriceable after that signal disappears.  The default therefore includes
    a bounded pre-signal lookback and extends every ticker through the common
    evaluation end date.
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT ticker, COUNT(*) AS obs, MIN(date(predicted_at)), MAX(date(predicted_at))
            FROM price_predictions
            WHERE ticker IS NOT NULL
              AND predicted_at IS NOT NULL
              AND datetime(predicted_at) >= datetime(?)
              AND datetime(predicted_at) <= datetime(?)
            GROUP BY ticker
            ORDER BY obs DESC, ticker
            """,
            (start.isoformat(), f"{end.isoformat()}T23:59:59"),
        ).fetchall()

    requests: list[BackfillRequest] = []
    for index, (ticker, _obs, first_seen, last_seen) in enumerate(rows):
        code = normalize_ticker(ticker)
        if len(code) != 6 or not code.isdigit():
            continue
        first_signal = parse_date(first_seen, start)
        req_start = max(start, first_signal - timedelta(days=max(0, lookback_days)))
        req_end = end if extend_to_end else min(end, parse_date(last_seen, end))
        requests.append(BackfillRequest(code, req_start, req_end, index + 1))
    return dedupe_requests(requests)


def requests_from_systematic_etf_universe(
    db_path: Path,
    start: date,
    end: date,
) -> list[BackfillRequest]:
    """Load the persisted ETF universe without inventing a current-survivor list."""
    tickers: list[str] = []
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "instrument_master" in tables:
            tickers.extend(
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT ticker
                    FROM instrument_master
                    WHERE instrument_type = 'etf'
                      AND (listed_at IS NULL OR date(listed_at) <= date(?))
                      AND (delisted_at IS NULL OR date(delisted_at) >= date(?))
                    ORDER BY ticker
                    """,
                    (end.isoformat(), start.isoformat()),
                ).fetchall()
            )
        if not tickers and "price_predictions" in tables:
            tickers.extend(
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT ticker FROM price_predictions WHERE ticker IS NOT NULL"
                ).fetchall()
                if is_etf_ticker(row[0])
            )
    return requests_from_tickers(tickers, start, end)


def dedupe_requests(requests: Iterable[BackfillRequest]) -> list[BackfillRequest]:
    by_ticker: dict[str, BackfillRequest] = {}
    for request in requests:
        current = by_ticker.get(request.ticker)
        if current is None:
            by_ticker[request.ticker] = request
            continue
        by_ticker[request.ticker] = BackfillRequest(
            ticker=request.ticker,
            start=min(current.start, request.start),
            end=max(current.end, request.end),
            priority_rank=min(current.priority_rank or request.priority_rank, request.priority_rank or current.priority_rank),
        )
    return sorted(by_ticker.values(), key=lambda item: (item.priority_rank or 999999, item.ticker))


async def fetch_request(request: BackfillRequest, semaphore: asyncio.Semaphore) -> tuple[BackfillRequest, list[dict]]:
    async with semaphore:
        bars = await get_market_data().get_ohlc(request.ticker, request.start, request.end)
        return request, bars


def upsert_bars(db_path: Path, ticker: str, bars: list[dict], source: str) -> int:
    if not bars:
        return 0
    rows = []
    fetched_at = datetime.now().isoformat()
    for bar in bars:
        try:
            close = float(bar["close"])
            if close <= 0:
                continue
            open_price = float(bar.get("open") or close)
            high = float(bar.get("high") or close)
            low = float(bar.get("low") or close)
            if min(open_price, high, low) <= 0:
                continue
            if high < max(open_price, close, low) or low > min(open_price, close, high):
                continue
            rows.append(
                (
                    normalize_ticker(ticker),
                    str(bar["date"])[:10],
                    open_price,
                    high,
                    low,
                    close,
                    float(bar.get("volume") or 0),
                    source,
                    str(bar.get("adjustment") or "qfq"),
                    str(bar.get("provider") or source),
                    str(bar.get("fetched_at") or fetched_at),
                    str(bar.get("quality_status") or "validated"),
                    str(bar.get("trade_status") or "normal"),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue

    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        ensure_market_truth_schema(conn)
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO daily_prices (
                ticker, date, open, high, low, close, volume, source,
                adjustment, provider, fetched_at, quality_status, trade_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            rows,
        )
        written = conn.total_changes - before
        persist_market_truth_metadata(
            conn,
            {normalize_ticker(ticker)},
            {row[1] for row in rows},
            source=source,
            fetched_at=fetched_at,
        )
        conn.commit()
        return written


def _csv_value(row: dict[str, str], *aliases: str) -> str:
    normalized = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        value = normalized.get(alias.lower())
        if value not in (None, ""):
            return str(value).strip()
    return ""


def rows_from_csv(csv_path: Path) -> tuple[list[tuple], list[dict[str, str]]]:
    """Parse local OHLC CSV rows without calling any market data service."""
    rows: list[tuple] = []
    invalid: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for line_number, row in enumerate(reader, start=2):
            ticker = normalize_ticker(_csv_value(row, "ticker", "code", "symbol", "sec_code"))
            raw_date = _csv_value(row, "date", "trade_date", "trading_date")
            raw_close = _csv_value(row, "close", "close_price", "收盘", "收盘价")
            try:
                day = parse_date(raw_date).isoformat()
                close = float(raw_close)
                if not ticker or close <= 0:
                    raise ValueError("missing ticker or non-positive close")
                open_price = float(_csv_value(row, "open", "open_price", "开盘", "开盘价") or close)
                high = float(_csv_value(row, "high", "high_price", "最高", "最高价") or close)
                low = float(_csv_value(row, "low", "low_price", "最低", "最低价") or close)
                volume = float(_csv_value(row, "volume", "vol", "成交量") or 0)
                if open_price <= 0 or high <= 0 or low <= 0:
                    raise ValueError("non-positive OHLC value")
                if high < max(open_price, close, low) or low > min(open_price, close, high):
                    raise ValueError("inconsistent OHLC range")
            except Exception as exc:
                invalid.append(
                    {
                        "line": str(line_number),
                        "ticker": ticker,
                        "date": raw_date,
                        "reason": str(exc),
                    }
                )
                continue
            rows.append((ticker, day, open_price, high, low, close, volume))
    return rows, invalid


def upsert_csv_rows(db_path: Path, rows: list[tuple], source: str) -> int:
    if not rows:
        return 0
    fetched_at = datetime.now().isoformat()
    payload = [
        (*row, source, "qfq", source, fetched_at, "validated", "normal")
        for row in rows
    ]
    with sqlite3.connect(db_path) as conn:
        ensure_market_truth_schema(conn)
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO daily_prices (
                ticker, date, open, high, low, close, volume, source,
                adjustment, provider, fetched_at, quality_status, trade_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            payload,
        )
        written = conn.total_changes - before
        persist_market_truth_metadata(
            conn,
            {normalize_ticker(row[0]) for row in rows},
            {str(row[1])[:10] for row in rows},
            source=source,
            fetched_at=fetched_at,
        )
        conn.commit()
        return written


def ensure_market_truth_schema(conn: sqlite3.Connection) -> None:
    """Make the standalone importer forward-compatible with older local DBs."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            source TEXT,
            adjustment TEXT,
            provider TEXT,
            fetched_at TEXT,
            quality_status TEXT,
            trade_status TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(ticker, date)
        )
        """
    )

    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(daily_prices)").fetchall()
    }
    for name, definition in (
        ("adjustment", "TEXT"),
        ("provider", "TEXT"),
        ("fetched_at", "TEXT"),
        ("quality_status", "TEXT"),
        ("trade_status", "TEXT"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE daily_prices ADD COLUMN {name} {definition}")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS instrument_master (
            ticker TEXT PRIMARY KEY,
            instrument_type TEXT NOT NULL,
            lot_size INTEGER NOT NULL DEFAULT 100,
            listed_at TEXT,
            delisted_at TEXT,
            source TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trading_calendar (
            trade_date TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT 'CN',
            is_open INTEGER NOT NULL,
            source TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            quality_status TEXT NOT NULL DEFAULT 'validated',
            PRIMARY KEY(trade_date, market)
        );
        """
    )
    calendar_columns = {row[1] for row in conn.execute('PRAGMA table_info(trading_calendar)')}
    for name in ('source_identity', 'source_kind', 'validity_start', 'validity_end', 'inference_rule'):
        if name not in calendar_columns:
            conn.execute(f'ALTER TABLE trading_calendar ADD COLUMN {name} TEXT')


def persist_market_truth_metadata(
    conn: sqlite3.Connection,
    tickers: set[str],
    trading_dates: set[str],
    *,
    source: str,
    fetched_at: str,
) -> None:
    for ticker in sorted(tickers):
        profile = InstrumentProfile.from_ticker(ticker, source=source)
        conn.execute(
            """
            INSERT INTO instrument_master (
                ticker, instrument_type, lot_size, listed_at, delisted_at,
                source, metadata_json, updated_at
            ) VALUES (?, ?, ?, NULL, NULL, ?, '{}', ?)
            ON CONFLICT(ticker) DO UPDATE SET
                instrument_type = excluded.instrument_type,
                lot_size = excluded.lot_size,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            (
                profile.ticker,
                profile.instrument_type.value,
                profile.lot_size,
                source,
                fetched_at,
            ),
        )
    conn.executemany(
        """
        INSERT INTO trading_calendar (
            trade_date, market, is_open, source, fetched_at, quality_status
        ) VALUES (?, 'CN', 1, ?, ?, 'validated')
        ON CONFLICT(trade_date, market) DO UPDATE SET
            is_open = 1,
            source = excluded.source,
            fetched_at = excluded.fetched_at,
            quality_status = excluded.quality_status
        WHERE coalesce(trading_calendar.source_kind, '') != 'exchange_calendar'
        """,
        [(day, source, fetched_at) for day in sorted(trading_dates)],
    )


def persist_instrument_profiles(
    db_path: Path,
    tickers: Iterable[str],
    *,
    source: str,
) -> int:
    """Persist known identifier/type/lot facts even when price providers fail.

    Listing and delisting dates intentionally remain null until supplied by a
    security-master source.  The metadata makes that limitation explicit
    instead of inventing lifecycle dates from a failed price request.
    """
    fetched_at = datetime.now().isoformat()
    profiles = [
        InstrumentProfile.from_ticker(ticker, source=source)
        for ticker in sorted({normalize_ticker(value) for value in tickers})
        if ticker
    ]
    if not profiles:
        return 0
    metadata = json.dumps(
        {
            "classification_method": "canonical_ticker_rules",
            "listing_dates": "unknown_until_security_master_source",
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    with sqlite3.connect(db_path) as conn:
        ensure_market_truth_schema(conn)
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO instrument_master (
                ticker, instrument_type, lot_size, listed_at, delisted_at,
                source, metadata_json, updated_at
            ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                instrument_type = excluded.instrument_type,
                lot_size = excluded.lot_size,
                source = CASE
                    WHEN instrument_master.source IS NULL
                      OR trim(instrument_master.source) = ''
                      OR instrument_master.source = 'prediction_signal_universe'
                    THEN excluded.source
                    ELSE instrument_master.source
                END,
                metadata_json = CASE
                    WHEN instrument_master.metadata_json IS NULL
                      OR trim(instrument_master.metadata_json) IN ('', '{}')
                    THEN excluded.metadata_json
                    ELSE instrument_master.metadata_json
                END,
                updated_at = excluded.updated_at
            """,
            [
                (
                    profile.ticker,
                    profile.instrument_type.value,
                    profile.lot_size,
                    source,
                    metadata,
                    fetched_at,
                )
                for profile in profiles
            ],
        )
        written = conn.total_changes - before
        conn.commit()
        return written


def persist_trading_calendar(db_path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with sqlite3.connect(db_path) as conn:
        ensure_market_truth_schema(conn)
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO trading_calendar (
                trade_date, market, is_open, source, fetched_at, quality_status,
                source_identity, source_kind, validity_start, validity_end, inference_rule
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, market) DO UPDATE SET
                is_open = excluded.is_open,
                source = excluded.source,
                fetched_at = excluded.fetched_at,
                quality_status = excluded.quality_status,
                source_identity = excluded.source_identity,
                source_kind = excluded.source_kind,
                validity_start = excluded.validity_start,
                validity_end = excluded.validity_end,
                inference_rule = excluded.inference_rule
            WHERE (coalesce(trading_calendar.source_kind, '') != 'exchange_calendar'
                   OR excluded.source_kind = 'exchange_calendar')
                AND (is_open, source, fetched_at, quality_status, source_identity,
                   source_kind, validity_start, validity_end, inference_rule)
                IS NOT (excluded.is_open, excluded.source, excluded.fetched_at,
                        excluded.quality_status, excluded.source_identity,
                        excluded.source_kind, excluded.validity_start,
                        excluded.validity_end, excluded.inference_rule)
            """,
            [
                (
                    str(row["trade_date"])[:10],
                    str(row.get("market") or "CN"),
                    1 if row.get("is_open", 1) else 0,
                    str(row.get("source") or "market_data_service"),
                    str(row.get("fetched_at") or datetime.now().isoformat()),
                    str(row.get("quality_status") or "validated"),
                    row.get('source_identity'), row.get('source_kind'),
                    row.get('validity_start'), row.get('validity_end'), row.get('inference_rule'),
                )
                for row in rows
            ],
        )
        written = conn.total_changes - before
        conn.commit()
        return written


def summarize_csv_rows(rows: list[tuple]) -> str:
    tickers = sorted({row[0] for row in rows})
    if not rows:
        return "valid_rows=0"
    dates = sorted({row[1] for row in rows})
    return (
        f"valid_rows={len(rows)}, tickers={len(tickers)}, "
        f"date_range={dates[0]}..{dates[-1]}, top_tickers={', '.join(tickers[:8])}"
    )


def summarize_plan_coverage(
    rows: list[tuple],
    requests: list[BackfillRequest],
    plan_path: Path | None = None,
    max_age_days: int = 7,
) -> str:
    """Summarize whether local CSV rows cover exact planned missing signal dates."""
    stats = plan_coverage_stats(rows, requests, plan_path, max_age_days)
    return format_plan_coverage_stats(stats)


def plan_coverage_stats(
    rows: list[tuple],
    requests: list[BackfillRequest],
    plan_path: Path | None = None,
    max_age_days: int = 7,
) -> dict[str, Any]:
    """Return machine-checkable coverage of planned signal dates by CSV rows."""
    if not requests:
        return {
            "plan_requests": 0,
            "covered_tickers": 0,
            "checked_signal_dates": 0,
            "covered_signal_dates": 0,
            "asof_max_age_days": max_age_days,
            "missing_top": [],
            "complete": False,
        }
    row_dates: dict[str, set[date]] = {}
    for ticker, day, *_rest in rows:
        try:
            row_dates.setdefault(normalize_ticker(ticker), set()).add(parse_date(day))
        except Exception:
            continue

    covered: list[str] = []
    missing: list[str] = []
    checked_dates_total = 0
    covered_dates_total = 0
    exact_missing_dates = missing_signal_dates_from_artifacts(plan_path) if plan_path else {}
    for request in requests:
        signal_dates = sorted(exact_missing_dates.get(request.ticker) or {request.end})
        dates = row_dates.get(request.ticker, set())
        uncovered_dates = [
            signal_day
            for signal_day in signal_dates
            if _asof_covering_price(signal_day, dates, max_age_days) is None
        ]
        checked_dates_total += len(signal_dates)
        covered_dates_total += len(signal_dates) - len(uncovered_dates)
        if not uncovered_dates:
            covered.append(request.ticker)
        else:
            missing.append(request.ticker)
    return {
        "plan_requests": len(requests),
        "covered_tickers": len(covered),
        "checked_signal_dates": checked_dates_total,
        "covered_signal_dates": covered_dates_total,
        "asof_max_age_days": max_age_days,
        "missing_top": missing[:8],
        "complete": checked_dates_total > 0 and covered_dates_total == checked_dates_total and not missing,
    }


def format_plan_coverage_stats(stats: dict[str, Any]) -> str:
    missing_top = stats.get("missing_top") or []
    plan_requests = _safe_int(stats.get("plan_requests"))
    covered_tickers = _safe_int(stats.get("covered_tickers"))
    covered_dates = _safe_int(stats.get("covered_signal_dates"))
    checked_dates = _safe_int(stats.get("checked_signal_dates"))
    max_age_days = _safe_int(stats.get("asof_max_age_days"), 7)
    return (
        f"plan_requests={plan_requests}, csv_exact_ticker_coverage={covered_tickers}/{plan_requests}, "
        f"signal_dates={covered_dates}/{checked_dates}, "
        f"asof_max_age={max_age_days}d, missing_top={', '.join(missing_top) or 'none'}"
    )


def _split_date_list(raw: object) -> list[date]:
    text = str(raw or "").strip()
    if not text:
        return []
    dates: list[date] = []
    for part in text.replace("|", ";").replace(",", ";").split(";"):
        value = part.strip()
        if not value:
            continue
        try:
            dates.append(parse_date(value))
        except Exception:
            continue
    return sorted(set(dates))


def missing_signal_dates_from_artifacts(plan_path: Path) -> dict[str, set[date]]:
    """Load exact missing signal dates from the plan or sibling signal tape."""
    missing: dict[str, set[date]] = {}
    try:
        with plan_path.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                ticker = normalize_ticker(row.get("ticker"))
                if not ticker:
                    continue
                for day in _split_date_list(row.get("missing_signal_dates")):
                    missing.setdefault(ticker, set()).add(day)
    except Exception:
        pass

    tape_path = plan_path.with_name("daily_signal_tape.csv")
    if tape_path.exists():
        try:
            with tape_path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    if str(row.get("price_source", "")).strip() == "daily_prices":
                        continue
                    ticker = normalize_ticker(row.get("ticker"))
                    if not ticker:
                        continue
                    try:
                        missing.setdefault(ticker, set()).add(parse_date(row.get("date")))
                    except Exception:
                        continue
        except Exception:
            pass
    return missing


def _asof_covering_price(signal_day: date, local_days: set[date], max_age_days: int) -> date | None:
    candidates = [day for day in local_days if day <= signal_day]
    if not candidates:
        return None
    latest = max(candidates)
    if (signal_day - latest).days <= max_age_days:
        return latest
    return None


def summarize_db_plan_coverage(
    db_path: Path,
    plan_path: Path,
    limit: int = 20,
    max_age_days: int = 7,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Compare the current SQLite ``daily_prices`` rows with a backfill plan."""
    items = plan_items_from_csv(plan_path, datetime.now().date())
    if limit:
        items = items[:limit]
    exact_missing_dates = missing_signal_dates_from_artifacts(plan_path)
    tickers = [item["ticker"] for item in items]

    local_dates: dict[str, set[date]] = {ticker: set() for ticker in tickers}
    latest_local: dict[str, str] = {}
    if tickers and db_path.exists():
        placeholders = ",".join("?" for _ in tickers)
        try:
            with sqlite3.connect(db_path) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_prices'"
                ).fetchone()
                if exists:
                    rows = conn.execute(
                        f"""
                        SELECT ticker, date
                        FROM daily_prices
                        WHERE ticker IN ({placeholders})
                          AND date IS NOT NULL
                        """,
                        tickers,
                    ).fetchall()
                    for ticker, day_text in rows:
                        ticker = normalize_ticker(ticker)
                        try:
                            day = parse_date(day_text)
                        except Exception:
                            continue
                        local_dates.setdefault(ticker, set()).add(day)
                        latest_local[ticker] = max(latest_local.get(ticker, ""), day.isoformat())
        except sqlite3.Error:
            pass

    details: list[dict[str, Any]] = []
    checked_dates_total = 0
    covered_dates_total = 0
    latest_missing_total = 0
    latest_missing_covered = 0

    for item in items:
        request: BackfillRequest = item["request"]
        ticker = item["ticker"]
        signal_dates = sorted(exact_missing_dates.get(ticker) or {request.end})
        price_days = local_dates.get(ticker, set())
        covered: list[str] = []
        missing: list[str] = []
        matched_price_dates: dict[str, str] = {}
        for signal_day in signal_dates:
            matched = _asof_covering_price(signal_day, price_days, max_age_days)
            if matched is None:
                missing.append(signal_day.isoformat())
            else:
                covered.append(signal_day.isoformat())
                matched_price_dates[signal_day.isoformat()] = matched.isoformat()

        checked_dates_total += len(signal_dates)
        covered_dates_total += len(covered)
        if item["missing_latest_signal_date"]:
            latest_missing_total += 1
            if not missing:
                latest_missing_covered += 1
        details.append(
            {
                "priority_rank": item["priority_rank"],
                "ticker": ticker,
                "status": "covered" if not missing else "needs_backfill",
                "checked_signal_dates": len(signal_dates),
                "covered_signal_dates": len(covered),
                "missing_signal_dates": missing,
                "covered_dates": covered,
                "matched_price_dates": matched_price_dates,
                "latest_local_date": latest_local.get(ticker, ""),
                "missing_latest_signal_date": item["missing_latest_signal_date"],
                "planned_range": f"{request.start.isoformat()}..{request.end.isoformat()}",
            }
        )

    missing_tickers = [item["ticker"] for item in details if item["status"] != "covered"]
    summary = {
        "status": "ready_for_cycle_rerun" if not missing_tickers else "needs_local_daily_prices",
        "plan_path": str(plan_path),
        "db_path": str(db_path),
        "checked_tickers": len(details),
        "missing_tickers": len(missing_tickers),
        "checked_signal_dates": checked_dates_total,
        "covered_signal_dates": covered_dates_total,
        "missing_signal_dates": max(0, checked_dates_total - covered_dates_total),
        "latest_missing_tickers_checked": latest_missing_total,
        "latest_missing_tickers_covered": latest_missing_covered,
        "max_age_days": max_age_days,
        "missing_top": missing_tickers[:8],
    }
    return summary, details


def format_db_plan_coverage(summary: dict[str, Any], details: list[dict[str, Any]]) -> str:
    lines = [
        f"Plan DB coverage: status={summary.get('status')}",
        (
            "Coverage: "
            f"tickers={summary.get('checked_tickers', 0) - summary.get('missing_tickers', 0)}/"
            f"{summary.get('checked_tickers', 0)}, "
            f"signal_dates={summary.get('covered_signal_dates', 0)}/"
            f"{summary.get('checked_signal_dates', 0)}, "
            f"latest_missing={summary.get('latest_missing_tickers_covered', 0)}/"
            f"{summary.get('latest_missing_tickers_checked', 0)}, "
            f"asof_max_age={summary.get('max_age_days', 7)}d"
        ),
    ]
    if summary.get("missing_top"):
        lines.append(f"Still missing top: {', '.join(summary['missing_top'])}")
    for item in details:
        missing = ", ".join(item.get("missing_signal_dates", [])[:5]) or "none"
        latest = item.get("latest_local_date") or "none"
        lines.append(
            f"{int(item.get('priority_rank') or 0):>3} {item['ticker']} "
            f"{item['status']} covered={item['covered_signal_dates']}/"
            f"{item['checked_signal_dates']} latest_local={latest} "
            f"missing_dates={missing}"
        )
    return "\n".join(lines)


def export_missing_price_template(
    output_path: Path,
    summary: dict[str, Any],
    details: list[dict[str, Any]],
) -> int:
    """Write a local OHLC template for only the currently missing plan dates."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_note",
    ]
    rows: list[dict[str, str]] = []
    for item in details:
        ticker = normalize_ticker(item.get("ticker"))
        for day in item.get("missing_signal_dates", []) or []:
            rows.append(
                {
                    "ticker": ticker,
                    "date": str(day)[:10],
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "source_note": "fill_from_independent_local_ohlc_before_import",
                }
            )

    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


async def run(args: argparse.Namespace) -> int:
    db_path = (PROJECT_ROOT / args.db).resolve()
    plan_path = Path(args.plan).resolve() if args.plan else latest_backfill_plan((PROJECT_ROOT / args.runs_root).resolve())

    if args.status or args.export_template:
        if not plan_path or not plan_path.exists():
            print("No backfill plan found.")
            return 1
        summary, details = summarize_db_plan_coverage(
            db_path,
            plan_path,
            limit=args.limit,
            max_age_days=args.max_age_days,
        )
        print(f"DB: {db_path}")
        print(f"Plan: {plan_path}")
        print(format_db_plan_coverage(summary, details))
        if args.export_template:
            output_path = Path(args.export_template).expanduser().resolve()
            written = export_missing_price_template(output_path, summary, details)
            print(f"Template: {output_path}")
            print(f"Template rows needing independent OHLC values: {written}")
            print(
                "After filling close/open/high/low, validate with: "
                f"python scripts/backfill_daily_prices.py --import-csv {output_path} "
                f"--source local_csv --dry-run --plan {plan_path}"
            )
            print(
                "Strict top-plan validation before import: "
                f"python scripts/backfill_daily_prices.py --import-csv {output_path} "
                f"--source local_csv --dry-run --coverage-limit {args.limit} "
                f"--require-plan-coverage --plan {plan_path}"
            )
        return 0

    await ensure_prediction_tables(str(db_path))

    if args.import_csv:
        csv_path = Path(args.import_csv).expanduser().resolve()
        if not csv_path.exists():
            print(f"CSV not found: {csv_path}")
            return 1
        rows, invalid = rows_from_csv(csv_path)
        print(f"DB: {db_path}")
        print(f"CSV: {csv_path}")
        print(f"Parsed: {summarize_csv_rows(rows)}, invalid_rows={len(invalid)}")
        import_plan_path = plan_path if plan_path and plan_path.exists() else None
        coverage_stats: dict[str, Any] | None = None
        if import_plan_path and import_plan_path.exists():
            requests = requests_from_plan(import_plan_path, datetime.now().date())
            if args.coverage_limit:
                requests = requests[: args.coverage_limit]
            elif args.require_plan_coverage and args.limit:
                requests = requests[: args.limit]
            coverage_stats = plan_coverage_stats(rows, requests, import_plan_path, args.max_age_days)
            print(f"Plan: {import_plan_path}")
            print(
                "Plan coverage: "
                f"{format_plan_coverage_stats(coverage_stats)}"
            )
            if args.require_plan_coverage:
                if coverage_stats.get("complete"):
                    print("STRICT plan coverage passed: selected plan dates are covered by local CSV rows.")
                else:
                    print(
                        "STRICT plan coverage failed: selected plan dates are not fully covered; "
                        "no rows will be imported with --require-plan-coverage."
                    )
        elif args.require_plan_coverage:
            print("STRICT plan coverage failed: no backfill plan was found for --require-plan-coverage.")
        if invalid:
            for item in invalid[:10]:
                print(f"INVALID line={item['line']} ticker={item['ticker']} date={item['date']}: {item['reason']}")
        if args.dry_run:
            print("DRY local CSV import: no rows written.")
            if args.require_plan_coverage and coverage_stats is None:
                return 4
            if args.require_plan_coverage and coverage_stats is not None and not coverage_stats.get("complete"):
                return 4
            return 0 if rows else 2
        if args.require_plan_coverage and coverage_stats is None:
            return 4
        if args.require_plan_coverage and coverage_stats is not None and not coverage_stats.get("complete"):
            return 4
        source = "local_csv" if args.source == "market_data_service" else args.source
        written = upsert_csv_rows(db_path, rows, source)
        print(f"Done: upserted_rows={written}, source={source}")
        return 0 if written > 0 else 2

    if args.no_akshare:
        market = get_market_data()

        async def _skip_akshare(_ticker: str, _start_s: str, _end_s: str) -> tuple[list[dict], bool]:
            return [], False

        market._fetch_akshare_ohlc = _skip_akshare  # type: ignore[method-assign]

    end = parse_date(args.end)
    default_start = (
        date(2015, 1, 1)
        if args.systematic_etf_history
        else end - timedelta(days=args.days)
    )
    start = parse_date(args.start, default_start)
    if args.ticker:
        requests = requests_from_tickers(args.ticker, start, end)
    elif args.systematic_etf_history:
        requests = requests_from_systematic_etf_universe(db_path, start, end)
    elif not args.all_prediction_tickers and plan_path and plan_path.exists():
        requests = requests_from_plan(
            plan_path,
            end,
            lookback_days=args.max_age_days,
        )
    else:
        requests = requests_from_predictions(
            db_path,
            start,
            end,
            lookback_days=args.max_age_days,
            extend_to_end=True,
        )

    if args.limit and not args.systematic_etf_history:
        requests = requests[: args.limit]
    if not requests and not args.backfill_calendar:
        print("No backfill requests found.")
        return 1

    print(f"DB: {db_path}")
    request_source = ""
    if args.ticker:
        request_source = " from explicit tickers"
    elif args.systematic_etf_history:
        request_source = " from persisted ETF universe"
    elif args.all_prediction_tickers:
        request_source = " from forward prediction signals"
    elif plan_path and plan_path.exists():
        request_source = f" from {plan_path}"
    print(f"Requests: {len(requests)}{request_source}")
    if args.dry_run:
        for request in requests:
            print(f"DRY {request.priority_rank:>3} {request.ticker} {request.start}..{request.end}")
        print("DRY local-only request preview: no fetches and no rows written.")
        if args.backfill_calendar or args.systematic_etf_history:
            print(f"DRY trading calendar {start}..{end}")
        return 0

    if args.master_only:
        written = persist_instrument_profiles(
            db_path,
            (request.ticker for request in requests),
            source="prediction_signal_universe",
        )
        print(f"Done: instrument_profiles={written}, no price rows fetched or written")
        return 0 if written else 2

    if not args.allow_market_fetch and os.environ.get("SOVEREIGN_HALL_ALLOW_MARKET_BACKFILL") != "1":
        print("MarketDataService fetch disabled by default for local-only heuristic cycles.")
        print("Use --status to inspect current SQLite coverage without network calls.")
        print("Use --import-csv data/local_daily_prices.csv --source local_csv --dry-run to validate local OHLC rows.")
        print("If external market-data fetch is intentionally allowed, rerun with --allow-market-fetch.")
        return 3

    persist_instrument_profiles(
        db_path,
        (request.ticker for request in requests),
        source="prediction_signal_universe",
    )

    # A bulk repair spans hundreds of independent symbols.  A few transient
    # transport errors must not put the primary provider into a five-minute
    # global cooldown and turn every remaining symbol into an empty result.
    market = get_market_data()
    market._eastmoney_ohlc_failure_threshold = max(3, args.provider_failure_threshold)
    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = [fetch_request(request, semaphore) for request in requests]
    total_bars = 0
    total_written = 0
    failed: list[str] = []
    for coro in asyncio.as_completed(tasks):
        request, bars = await coro
        written = upsert_bars(db_path, request.ticker, bars, args.source)
        total_bars += len(bars)
        total_written += written
        status = "ok" if bars else "empty"
        if not bars:
            failed.append(request.ticker)
        print(f"{status:5} {request.ticker} {request.start}..{request.end}: fetched={len(bars)} upserted={written}")

    calendar_written = 0
    if args.backfill_calendar or args.systematic_etf_history:
        calendar_rows = await market.get_trading_calendar(start, end)
        calendar_written = persist_trading_calendar(db_path, calendar_rows)
        print(
            f"calendar {start}..{end}: fetched={len(calendar_rows)} "
            f"upserted={calendar_written}"
        )

    await market.close()
    print(
        f"Done: fetched_bars={total_bars}, upserted_rows={total_written}, "
        f"calendar_rows={calendar_written}, empty_tickers={len(failed)}"
    )
    if failed:
        print("Empty:", ", ".join(failed[:20]) + (" ..." if len(failed) > 20 else ""))
    return 0 if total_written > 0 or calendar_written > 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill SQLite daily_prices with validated local OHLC bars.")
    parser.add_argument("--db", default="data/sovereign_hall.db", help="SQLite DB path relative to project root")
    parser.add_argument("--runs-root", default="runs/heuristic_cycle", help="Heuristic runs root")
    parser.add_argument("--plan", default=None, help="daily_price_backfill_plan.csv path; defaults to latest")
    parser.add_argument("--ticker", action="append", help="Ticker to backfill; can be repeated")
    parser.add_argument(
        "--all-prediction-tickers",
        action="store_true",
        help=(
            "Ignore the latest missing-row plan and fetch every valid local prediction ticker "
            "from its first signal/lookback through the common --end date"
        ),
    )
    parser.add_argument(
        "--systematic-etf-history",
        action="store_true",
        help=(
            "Backfill the persisted ETF instrument universe from 2015-01-01 "
            "(or --start) through --end, plus the persistent CN calendar"
        ),
    )
    parser.add_argument(
        "--backfill-calendar",
        action="store_true",
        help="Persist validated CN open sessions for the selected date range",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD; used with --ticker or DB fallback")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD; defaults to today")
    parser.add_argument("--days", type=int, default=90, help="Lookback days when --start is omitted")
    parser.add_argument("--limit", type=int, default=20, help="Limit request count; use 0 for no limit")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent market-data requests")
    parser.add_argument(
        "--provider-failure-threshold",
        type=int,
        default=50,
        help="Consecutive primary-provider transport failures before a bulk-run cooldown",
    )
    parser.add_argument("--source", default="market_data_service", help="daily_prices.source value")
    parser.add_argument("--dry-run", action="store_true", help="Show requests without fetching or writing")
    parser.add_argument(
        "--master-only",
        action="store_true",
        help=(
            "Persist canonical ticker type/lot metadata for the selected universe "
            "without fetching or writing any price rows"
        ),
    )
    parser.add_argument("--status", action="store_true", help="Check current DB coverage of the selected backfill plan without network or writes")
    parser.add_argument(
        "--export-template",
        default=None,
        help="Write a fill-in local OHLC CSV template for exact plan dates still missing from the DB",
    )
    parser.add_argument("--max-age-days", type=int, default=7, help="As-of daily_prices tolerance used by the heuristic tape")
    parser.add_argument(
        "--coverage-limit",
        type=int,
        default=0,
        help="When validating a CSV against a plan, check only the top N plan rows; 0 checks all rows unless --require-plan-coverage falls back to --limit.",
    )
    parser.add_argument(
        "--require-plan-coverage",
        action="store_true",
        help="Fail local CSV validation/import unless selected plan signal dates are fully covered.",
    )
    parser.add_argument(
        "--allow-market-fetch",
        action="store_true",
        help="Explicitly allow MarketDataService fetches; otherwise only status, dry-run, and local CSV import are allowed",
    )
    parser.add_argument(
        "--import-csv",
        default=None,
        help=(
            "Import a local OHLC CSV into daily_prices without network calls. "
            "Accepted columns include ticker/code, date/trade_date, open/high/low/close/volume."
        ),
    )
    parser.add_argument("--no-akshare", action="store_true", help="Skip AkShare fallback; useful when local NumPy/Pandas wheels are broken")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
