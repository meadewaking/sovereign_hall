#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - 数据库统计查看
功能：查看数据库统计，并可选择浏览内容或进行讨论
用法：直接运行此脚本
"""

import sys
import os
import csv
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sovereign_hall.services.portfolio_policy import deployment_status, review_position
from sovereign_hall.services.reward_policy import MAX_DAILY_TRADES, REWARD_FORMULA

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root.parent))

LOCAL_DAILY_PRICE_TEMPLATE = project_root / "data" / "local_daily_prices_template.csv"


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def get_realtime_prices(tickers: list) -> dict:
    """获取实时价格、来源和抓取时间；失败时不使用历史价格兜底。"""
    import asyncio
    from sovereign_hall.services.market_data import get_market_data

    async def fetch():
        md = get_market_data()
        prices = {}
        for ticker in tickers:
            if hasattr(md, "get_current_quote"):
                quote = await md.get_current_quote(ticker)
            else:
                price = await md.get_current_price(ticker)
                quote = {
                    "price": price,
                    "source": "realtime_quote",
                    "fetched_at": datetime.now().isoformat(),
                } if price else None
            if quote and quote.get("price"):
                prices[ticker] = quote
        return prices

    return asyncio.run(fetch())


def normalize_ticker(ticker: str) -> str:
    code = str(ticker or "").strip().upper()
    return code.split(".")[0] if "." in code else code


def realtime_quotes_enabled() -> bool:
    """Realtime valuation is the default; explicit opt-out yields N/A, never fallback."""
    value = os.environ.get("SOVEREIGN_HALL_REALTIME_QUOTES", "1").strip().lower()
    return value in {"1", "true", "yes", "on"}


def safe_input(prompt: str) -> str | None:
    """Read optional interactive input; return None when stdin is closed."""
    try:
        return input(prompt)
    except EOFError:
        return None


def format_position_pct(value: float) -> str:
    """Keep tiny observation caps readable without changing normal cap formatting."""
    return f"{value:.2%}" if abs(value) < 0.005 else f"{value:.1%}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _read_backfill_plan_rows(plan_path: str, limit: int) -> dict[str, dict[str, Any]]:
    if not plan_path:
        return {}
    path = Path(plan_path).expanduser()
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                ticker = normalize_ticker(row.get("ticker", ""))
                if not ticker or ticker in rows:
                    continue
                rows[ticker] = {
                    "first_missing_signal_date": str(row.get("first_missing_signal_date", "") or "")[:10],
                    "last_missing_signal_date": str(row.get("last_missing_signal_date", "") or "")[:10],
                    "missing_signal_days": _safe_int(row.get("missing_signal_days")),
                    "total_signal_observations": _safe_int(row.get("total_signal_observations")),
                    "missing_latest_signal_date": str(row.get("missing_latest_signal_date", "")).lower()
                    in {"1", "true", "yes"},
                    "minimum_rows_to_unblock_latest": _safe_int(row.get("minimum_rows_to_unblock_latest")),
                    "plan_action": str(row.get("plan_action", "") or ""),
                    "missing_signal_dates": str(row.get("missing_signal_dates", "") or ""),
                }
                if len(rows) >= limit:
                    break
    except Exception:
        return {}
    return rows


def _format_backfill_queue_item(item: dict[str, Any]) -> str:
    ticker = item["ticker"]
    details: list[str] = []
    first_missing = str(item.get("first_missing_signal_date", "") or "")[:10]
    last_missing = str(item.get("last_missing_signal_date", "") or "")[:10]
    if first_missing and last_missing:
        details.append(f"missing {first_missing}..{last_missing}")
    elif last_missing:
        details.append(f"missing_to {last_missing}")
    missing_days = _safe_int(item.get("missing_signal_days"))
    if missing_days:
        details.append(f"{missing_days}d")
    observations = _safe_int(item.get("total_signal_observations"))
    if observations:
        details.append(f"{observations}obs")
    checked_signal_dates = _safe_int(item.get("checked_signal_dates"))
    if checked_signal_dates:
        details.append(
            f"plan_covered={_safe_int(item.get('covered_signal_dates'))}/{checked_signal_dates}"
        )
    row_count = _safe_int(item.get("row_count"))
    latest_date = str(item.get("latest_date", "") or "")[:10]
    if row_count:
        details.append(f"local_rows={row_count}")
    if latest_date:
        details.append(f"local_latest={latest_date}")
    return f"{ticker}({', '.join(details)})" if details else ticker


def _parse_iso_date(value: Any) -> date | None:
    try:
        text = str(value or "").strip()[:10]
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


def _split_signal_dates(raw: Any) -> list[date]:
    dates: list[date] = []
    for part in str(raw or "").replace("|", ";").replace(",", ";").split(";"):
        parsed = _parse_iso_date(part)
        if parsed is not None:
            dates.append(parsed)
    return sorted(set(dates))


def _read_exact_missing_signal_dates(plan_path: str, queue: list[str]) -> dict[str, list[date]]:
    """Read exact missing signal dates from the latest run artifacts when present."""
    wanted = {normalize_ticker(ticker) for ticker in queue}
    if not plan_path or not wanted:
        return {}

    path = Path(plan_path).expanduser()
    dates_by_ticker: dict[str, set[date]] = {ticker: set() for ticker in wanted}
    if path.exists():
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    ticker = normalize_ticker(row.get("ticker", ""))
                    if ticker not in wanted:
                        continue
                    for day in _split_signal_dates(row.get("missing_signal_dates")):
                        dates_by_ticker.setdefault(ticker, set()).add(day)
        except Exception:
            pass

    tape_path = path.with_name("daily_signal_tape.csv")
    if tape_path.exists():
        try:
            with tape_path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    if str(row.get("price_source", "")).strip() == "daily_prices":
                        continue
                    ticker = normalize_ticker(row.get("ticker", ""))
                    if ticker not in wanted:
                        continue
                    day = _parse_iso_date(row.get("date"))
                    if day is not None:
                        dates_by_ticker.setdefault(ticker, set()).add(day)
        except Exception:
            pass

    return {
        ticker: sorted(days)
        for ticker, days in dates_by_ticker.items()
        if days
    }


def _asof_covered(signal_day: date, local_days: set[date], max_age_days: int = 7) -> bool:
    candidates = [day for day in local_days if day <= signal_day]
    if not candidates:
        return False
    return (signal_day - max(candidates)).days <= max_age_days


def _format_next_backfill_item(item: dict[str, Any]) -> str:
    text = item["ticker"]
    first_missing = str(item.get("first_missing_signal_date", "") or "")[:10]
    last_missing = str(item.get("last_missing_signal_date", "") or "")[:10]
    if first_missing and last_missing:
        text += f" {first_missing}..{last_missing}"
    missing_days = _safe_int(item.get("missing_signal_days"))
    if missing_days:
        text += f" ({missing_days} signal days)"
    return text


def _template_dates_for_missing_item(item: dict[str, Any]) -> list[str]:
    dates: list[str] = []
    raw_dates = item.get("missing_signal_dates") or []
    if isinstance(raw_dates, list):
        for raw in raw_dates:
            day = str(raw or "").strip()[:10]
            if day:
                dates.append(day)
    if not dates:
        fallback = str(item.get("last_missing_signal_date", "") or "").strip()[:10]
        if fallback:
            dates.append(fallback)
    return sorted(set(dates))


def export_daily_price_template_from_progress(
    progress: dict[str, Any],
    output_path: Path | None = None,
) -> int:
    """Write a stable local OHLC template for the live missing plan dates."""
    if not progress:
        return 0
    rows: list[dict[str, str]] = []
    for item in progress.get("missing_details", []) or []:
        if not isinstance(item, dict):
            continue
        ticker = normalize_ticker(str(item.get("ticker", "")))
        if not ticker:
            continue
        for day in _template_dates_for_missing_item(item):
            rows.append(
                {
                    "ticker": ticker,
                    "date": day,
                    "open": "",
                    "high": "",
                    "low": "",
                    "close": "",
                    "volume": "",
                    "source_note": "fill_from_independent_local_ohlc_before_import",
                }
            )

    if not rows:
        return 0

    output_path = output_path or Path(
        str(progress.get("stable_template_path") or LOCAL_DAILY_PRICE_TEMPLATE)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_note",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def inspect_local_daily_price_csv(csv_path: Path | str) -> dict[str, Any]:
    """Inspect a local OHLC CSV candidate without importing or fetching prices."""
    path = Path(csv_path).expanduser()
    status: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "rows": 0,
        "valid_ohlc_rows": 0,
        "blank_rows": 0,
        "invalid_rows": 0,
    }
    if not path.exists():
        return status

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                status["rows"] += 1
                ticker = normalize_ticker(str(row.get("ticker") or row.get("code") or row.get("symbol") or ""))
                day = str(row.get("date") or row.get("trade_date") or "").strip()[:10]
                close_text = str(row.get("close") or row.get("close_price") or "").strip()
                open_text = str(row.get("open") or row.get("open_price") or close_text).strip()
                high_text = str(row.get("high") or row.get("high_price") or close_text).strip()
                low_text = str(row.get("low") or row.get("low_price") or close_text).strip()
                if not any([close_text, open_text, high_text, low_text]):
                    status["blank_rows"] += 1
                    continue
                try:
                    values = [float(value) for value in (close_text, open_text, high_text, low_text)]
                    if not ticker or not day or any(value <= 0 for value in values):
                        raise ValueError("missing ticker/date or non-positive OHLC")
                except Exception:
                    status["invalid_rows"] += 1
                    continue
                status["valid_ohlc_rows"] += 1
    except Exception as exc:
        status["read_error"] = str(exc)
    return status


def daily_price_backfill_progress(
    conn: sqlite3.Connection,
    context: Any = None,
    limit: int = 5,
) -> dict[str, Any]:
    """Compare the latest heuristic backfill queue with live local daily_prices rows."""
    try:
        from sovereign_hall.services.heuristic_policy import (
            format_price_readiness_stall_note,
            load_latest_heuristic_context,
            price_readiness_position_cap,
            price_readiness_stall_position_cap,
            thin_tape_update_position_cap,
            weak_price_coverage_position_cap,
        )
    except Exception:
        return {}

    ctx = context or load_latest_heuristic_context()
    readiness = getattr(ctx, "price_readiness", {}) or {}
    if not isinstance(readiness, dict):
        return {}

    queue: list[str] = []
    top_missing = readiness.get("missing_tickers_top10") or []
    if isinstance(top_missing, list):
        for row in top_missing:
            if not isinstance(row, dict) or not row.get("ticker"):
                continue
            ticker = normalize_ticker(str(row["ticker"]))
            if ticker and ticker not in queue:
                queue.append(ticker)
            if len(queue) >= limit:
                break
    if not queue:
        latest_missing = readiness.get("latest_missing_tickers") or []
        if isinstance(latest_missing, list):
            queue = [normalize_ticker(str(ticker)) for ticker in latest_missing[:limit] if ticker]
    queue = [ticker for ticker in queue if ticker]
    if not queue:
        return {}
    raw_unblock = readiness.get("unblock_tickers") or readiness.get("latest_missing_tickers") or []
    unblock_tickers = []
    if isinstance(raw_unblock, list):
        for ticker in raw_unblock:
            code = normalize_ticker(str(ticker))
            if code and code not in unblock_tickers:
                unblock_tickers.append(code)
    minimum_next_rows = _safe_int(readiness.get("minimum_next_rows"), len(unblock_tickers))

    plan = readiness.get("backfill_plan") if isinstance(readiness, dict) else {}
    plan_path = str(readiness.get("backfill_plan_path", "") or "") if isinstance(readiness, dict) else ""
    plan_rows = _read_backfill_plan_rows(plan_path, limit)
    top_rows: dict[str, dict[str, Any]] = {}
    if isinstance(top_missing, list):
        for row in top_missing:
            if not isinstance(row, dict) or not row.get("ticker"):
                continue
            ticker = normalize_ticker(str(row["ticker"]))
            top_rows[ticker] = row

    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_prices'")
    table_exists = c.fetchone() is not None
    c.execute("SELECT COUNT(*) FROM daily_prices" if table_exists else "SELECT 0")
    total_rows = int(c.fetchone()[0])

    priced: dict[str, dict[str, Any]] = {}
    local_dates: dict[str, set[date]] = {ticker: set() for ticker in queue}
    if table_exists and total_rows > 0:
        placeholders = ",".join("?" for _ in queue)
        c.execute(
            f"""
            SELECT ticker, COUNT(*) AS row_count, MIN(date) AS first_date, MAX(date) AS latest_date
            FROM daily_prices
            WHERE ticker IN ({placeholders})
            GROUP BY ticker
            """,
            queue,
        )
        for ticker, row_count, first_date, latest_date in c.fetchall():
            priced[normalize_ticker(ticker)] = {
                "row_count": int(row_count),
                "first_date": first_date,
                "latest_date": latest_date,
            }
        c.execute(
            f"""
            SELECT ticker, date
            FROM daily_prices
            WHERE ticker IN ({placeholders})
              AND date IS NOT NULL
            """,
            queue,
        )
        for ticker, day_text in c.fetchall():
            day = _parse_iso_date(day_text)
            if day is not None:
                local_dates.setdefault(normalize_ticker(ticker), set()).add(day)

    tickers_without_any_local_rows = [
        ticker for ticker in queue if priced.get(ticker, {}).get("row_count", 0) <= 0
    ]
    exact_missing_dates = _read_exact_missing_signal_dates(plan_path, queue)
    top_plan = []
    if isinstance(plan, dict):
        raw_top = plan.get("top_priority_tickers", [])
        if isinstance(raw_top, list):
            top_plan = [normalize_ticker(str(ticker)) for ticker in raw_top if ticker]
    queue_details: list[dict[str, Any]] = []
    for ticker in queue:
        detail: dict[str, Any] = {"ticker": ticker}
        top_row = top_rows.get(ticker, {})
        if isinstance(top_row, dict):
            detail.update(
                {
                    "missing_signal_days": top_row.get("signal_days"),
                    "first_missing_signal_date": top_row.get("first_signal_date"),
                    "last_missing_signal_date": top_row.get("last_signal_date"),
                    "total_signal_observations": top_row.get("total_signal_observations"),
                }
            )
        detail.update({k: v for k, v in plan_rows.get(ticker, {}).items() if v not in ("", 0, None)})
        detail.update(priced.get(ticker, {}))
        signal_dates = exact_missing_dates.get(ticker, [])
        if not signal_dates:
            last_missing = _parse_iso_date(detail.get("last_missing_signal_date"))
            signal_dates = [last_missing] if last_missing is not None else []
        missing_signal_dates: list[str] = []
        covered_signal_dates = 0
        for signal_day in signal_dates:
            if _asof_covered(signal_day, local_dates.get(ticker, set())):
                covered_signal_dates += 1
            else:
                missing_signal_dates.append(signal_day.isoformat())
        detail["checked_signal_dates"] = len(signal_dates)
        detail["covered_signal_dates"] = covered_signal_dates
        detail["missing_signal_dates"] = missing_signal_dates
        queue_details.append(detail)
    def _needs_plan_backfill(item: dict[str, Any]) -> bool:
        if _safe_int(item.get("checked_signal_dates")) > 0:
            return bool(item.get("missing_signal_dates"))
        return (
            bool(item.get("first_missing_signal_date"))
            or bool(item.get("last_missing_signal_date"))
            or item["ticker"] in tickers_without_any_local_rows
        )

    needs_plan_backfill = [item for item in queue_details if _needs_plan_backfill(item)]
    checked_signal_dates_total = sum(_safe_int(item.get("checked_signal_dates")) for item in queue_details)
    covered_signal_dates_total = sum(_safe_int(item.get("covered_signal_dates")) for item in queue_details)
    cap_candidates = [
        price_readiness_position_cap(ctx),
        price_readiness_stall_position_cap(ctx),
        weak_price_coverage_position_cap(ctx),
        thin_tape_update_position_cap(ctx),
    ]
    active_caps = [float(cap) for cap in cap_candidates if cap is not None]
    dry_run_parts = ["python", "scripts/backfill_daily_prices.py", "--dry-run", "--limit", str(limit)]
    if plan_path:
        dry_run_parts.extend(["--plan", plan_path])
    status_parts = ["python", "scripts/backfill_daily_prices.py", "--status", "--limit", str(limit)]
    if plan_path:
        status_parts.extend(["--plan", plan_path])
    latest_status_parts = ["python", "scripts/backfill_daily_prices.py", "--status", "--limit", str(limit)]
    latest_import_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        "data/local_daily_prices.csv",
        "--source",
        "local_csv",
        "--dry-run",
    ]
    local_import_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        "data/local_daily_prices.csv",
        "--source",
        "local_csv",
        "--dry-run",
    ]
    if plan_path:
        local_import_parts.extend(["--plan", plan_path])
    local_strict_import_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        "data/local_daily_prices.csv",
        "--source",
        "local_csv",
        "--dry-run",
        "--coverage-limit",
        str(limit),
        "--require-plan-coverage",
    ]
    if plan_path:
        local_strict_import_parts.extend(["--plan", plan_path])
    stable_template_path = (
        str(Path(plan_path).expanduser().with_name("local_daily_prices_template.csv"))
        if plan_path
        else str(LOCAL_DAILY_PRICE_TEMPLATE)
    )
    stable_template_import_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        stable_template_path,
        "--source",
        "local_csv",
        "--dry-run",
    ]
    if plan_path:
        stable_template_import_parts.extend(["--plan", plan_path])
    stable_template_strict_import_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        stable_template_path,
        "--source",
        "local_csv",
        "--dry-run",
        "--coverage-limit",
        str(limit),
        "--require-plan-coverage",
    ]
    if plan_path:
        stable_template_strict_import_parts.extend(["--plan", plan_path])
    stable_template_commit_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--import-csv",
        stable_template_path,
        "--source",
        "local_csv",
        "--coverage-limit",
        str(limit),
        "--require-plan-coverage",
    ]
    if plan_path:
        stable_template_commit_parts.extend(["--plan", plan_path])
    template_parts = [
        "python",
        "scripts/backfill_daily_prices.py",
        "--status",
        "--limit",
        str(limit),
        "--export-template",
        stable_template_path,
    ]
    if plan_path:
        template_parts.extend(["--plan", plan_path])
    return {
        "status": readiness.get("status", "unknown"),
        "queue": queue,
        "queue_details": queue_details,
        "has_local_count": sum(1 for ticker in queue if priced.get(ticker, {}).get("row_count", 0) > 0),
        "total_count": len(queue),
        "checked_signal_dates": checked_signal_dates_total,
        "covered_signal_dates": covered_signal_dates_total,
        "missing_signal_dates": max(0, checked_signal_dates_total - covered_signal_dates_total),
        "missing": [item["ticker"] for item in needs_plan_backfill],
        "missing_details": needs_plan_backfill,
        "priced": priced,
        "next_ticker": needs_plan_backfill[0]["ticker"] if needs_plan_backfill else None,
        "next_detail": needs_plan_backfill[0] if needs_plan_backfill else None,
        "unblock_tickers": unblock_tickers,
        "minimum_next_rows": minimum_next_rows,
        "daily_prices_rows": total_rows,
        "backfill_plan_path": plan_path,
        "backfill_plan_top": top_plan,
        "active_cap": min(active_caps) if active_caps else None,
        "stall_note": format_price_readiness_stall_note(ctx),
        "status_command": " ".join(status_parts),
        "latest_status_command": " ".join(latest_status_parts),
        "dry_run_command": " ".join(dry_run_parts),
        "latest_import_command": " ".join(latest_import_parts),
        "local_import_command": " ".join(local_import_parts),
        "local_strict_import_command": " ".join(local_strict_import_parts),
        "template_command": " ".join(template_parts),
        "stable_template_path": stable_template_path,
        "stable_template_import_command": " ".join(stable_template_import_parts),
        "stable_template_strict_import_command": " ".join(stable_template_strict_import_parts),
        "stable_template_commit_command": " ".join(stable_template_commit_parts),
        "market_fetch_note": "MarketDataService fetch 默认关闭；本入口只建议 status 与本地CSV精确日期校验",
    }


def format_daily_price_backfill_progress(
    conn: sqlite3.Connection,
    context: Any = None,
    limit: int = 5,
    progress: dict[str, Any] | None = None,
) -> str:
    """Format live local daily_prices progress for check_db."""
    progress = progress or daily_price_backfill_progress(conn, context=context, limit=limit)
    if not progress:
        return ""

    queue_text = ", ".join(
        _format_backfill_queue_item(item)
        for item in progress.get("queue_details", [])
    ) or ", ".join(progress["queue"])
    missing_text = ", ".join(
        _format_backfill_queue_item(item)
        for item in progress.get("missing_details", [])
    )
    lines = [
        "\n🧱 daily_prices 本地补齐进度",
        "=" * 60,
        f"   状态: {progress['status']}",
        (
            f"   优先队列任意本地价格(非解锁口径): {progress['has_local_count']}/"
            f"{progress['total_count']} tickers"
        ),
        (
            f"   计划日期覆盖: {progress.get('covered_signal_dates', 0)}/"
            f"{progress.get('checked_signal_dates', 0)} signal dates；"
            f"缺口={progress.get('missing_signal_dates', 0)}，补齐后重跑验证"
        ),
        f"   优先队列: {queue_text}",
        f"   daily_prices 当前总行数: {progress['daily_prices_rows']:,}",
    ]
    if missing_text:
        lines.append(f"   优先队列仍需补齐/验证: {missing_text}")
    if progress.get("next_ticker"):
        next_detail = progress.get("next_detail") or {"ticker": progress["next_ticker"]}
        lines.append(f"   下一步本地补齐: {_format_next_backfill_item(next_detail)}")
    else:
        lines.append("   下一步本地补齐: 优先队列已覆盖，需重新运行 heuristic cycle 验证")
    if progress.get("unblock_tickers"):
        lines.append(
            "   最小解锁批次: "
            f"{', '.join(progress['unblock_tickers'][:8])} "
            f"({progress.get('minimum_next_rows', len(progress['unblock_tickers']))} signal rows)"
        )
    if progress.get("backfill_plan_path"):
        lines.append(f"   机器可读补齐计划: {progress['backfill_plan_path']}")
    if progress.get("backfill_plan_top"):
        lines.append(f"   计划优先级Top: {', '.join(progress['backfill_plan_top'][:5])}")
    if progress.get("status_command"):
        lines.append(f"   本地DB覆盖检查: {progress['status_command']}")
    if progress.get("latest_status_command"):
        lines.append(f"   本地DB覆盖检查(最新计划短命令): {progress['latest_status_command']}")
    if progress.get("dry_run_command"):
        lines.append(f"   不联网计划查看: {progress['dry_run_command']}")
    if progress.get("local_import_command"):
        lines.append(f"   本地CSV精确日期校验: {progress['local_import_command']}")
    if progress.get("local_strict_import_command"):
        lines.append(f"   本地CSV严格覆盖校验(Top计划): {progress['local_strict_import_command']}")
    if progress.get("latest_import_command"):
        lines.append(f"   本地CSV精确日期校验(最新计划短命令): {progress['latest_import_command']}")
    if progress.get("template_command"):
        lines.append(f"   本地CSV模板生成: {progress['template_command']}")
    template_status = progress.get("template_csv_status")
    if isinstance(template_status, dict) and template_status.get("exists"):
        lines.append(
            "   模板当前状态: "
            f"rows={_safe_int(template_status.get('rows'))}, "
            f"valid_ohlc={_safe_int(template_status.get('valid_ohlc_rows'))}, "
            f"blank={_safe_int(template_status.get('blank_rows'))}, "
            f"invalid={_safe_int(template_status.get('invalid_rows'))}"
        )
        if _safe_int(template_status.get("valid_ohlc_rows")) <= 0:
            lines.append("   模板尚未填入独立OHLC；严格覆盖校验会失败，不能导入或解除数据阻塞")
    elif isinstance(template_status, dict):
        lines.append(f"   模板当前状态: 未找到 {template_status.get('path')}")
    if progress.get("template_written_rows") is not None:
        rows = _safe_int(progress.get("template_written_rows"))
        if rows:
            lines.append(
                f"   入口已生成待填写模板: {progress.get('stable_template_path')} ({rows} rows)"
            )
            lines.append(
                f"   模板填完后校验: {progress.get('stable_template_import_command')}"
            )
            lines.append(
                f"   模板填完后严格校验: {progress.get('stable_template_strict_import_command')}"
            )
            lines.append(
                f"   严格校验通过后导入: {progress.get('stable_template_commit_command')}"
            )
        else:
            lines.append("   入口模板生成: 当前优先队列没有可写入的缺口日期")
    if progress.get("template_write_error"):
        lines.append(f"   入口模板生成失败: {progress['template_write_error']}")
    if progress.get("market_fetch_note"):
        lines.append(f"   数据安全门: {progress['market_fetch_note']}")
    if progress.get("stall_note"):
        lines.append(f"   连续阻塞: {progress['stall_note']}")
    if progress.get("active_cap") is not None:
        lines.append(
            "   系统动作: 补齐并重新评估前，模拟买入上限维持 <= "
            f"{format_position_pct(progress['active_cap'])}，不得扩仓"
        )
    else:
        lines.append("   系统动作: 仅作为本地数据质量提示，不触发扩仓")
    return "\n".join(lines) + "\n"


def pending_decision_diagnostics(conn):
    """Return durable deferred-ruling lifecycle diagnostics for user entries."""
    diagnostics = {
        "unresolved_count": 0,
        "status_counts": {},
        "pending_rows": [],
        "last_resolution": None,
    }
    try:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='simulation_pending_decisions'"
        ).fetchone()
        if not table:
            return diagnostics
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(simulation_pending_decisions)")
        }
        status_rows = conn.execute(
            "SELECT status, COUNT(*) FROM simulation_pending_decisions GROUP BY status"
        ).fetchall()
        diagnostics["status_counts"] = {
            str(status): int(count) for status, count in status_rows
        }
        diagnostics["unresolved_count"] = sum(
            diagnostics["status_counts"].get(status, 0)
            for status in ("pending_next_trading_session", "replaying")
        )
        diagnostics["pending_rows"] = conn.execute(
            """
            SELECT ticker, direction, target_position, defer_code, created_at
            FROM simulation_pending_decisions
            WHERE status IN ('pending_next_trading_session', 'replaying')
            ORDER BY datetime(created_at), id
            LIMIT 10
            """
        ).fetchall()
        terminal_columns = {"resolved_at", "resolution", "replay_count"}
        if terminal_columns.issubset(columns):
            row = conn.execute(
                """
                SELECT ticker, direction, status, resolution, resolved_at,
                       replay_count, defer_code
                FROM simulation_pending_decisions
                WHERE status IN ('executed', 'rejected', 'expired')
                ORDER BY datetime(COALESCE(resolved_at, updated_at)) DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            diagnostics["last_resolution"] = dict(row) if row else None
    except sqlite3.Error:
        return diagnostics
    return diagnostics


def show_investment_status(db_path):
    """显示投资模拟状态"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 检查表是否存在
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='simulation_positions'")
    if not c.fetchone():
        print("\n" + "="*60)
        print("📊 投资模拟状态")
        print("="*60)
        print("   初始资金: 10,000.00 元")
        print("   当前资产: 10,000.00 元")
        print("   📈 盈亏: +0.00 元 (+0.00%)")
        print("   现金: 10,000.00 元")
        print("\n   📦 当前持仓:")
        print("   (空仓)")
        print("\n   📜 最近交易:")
        print("   (无交易记录)")
        conn.close()
        return

    # 获取初始资金
    c.execute("SELECT value FROM system_stats WHERE key = 'simulation_cash'")
    cash_row = c.fetchone()
    initial_capital = 10000
    cash = float(cash_row[0]) if cash_row else initial_capital

    # 获取持仓
    try:
        position_columns = {row[1] for row in c.execute("PRAGMA table_info(simulation_positions)")}
        metadata_columns = [
            column for column in (
                "opened_at", "last_reviewed_at", "review_status", "review_reason"
            ) if column in position_columns
        ]
        select_columns = "ticker, shares, avg_cost"
        if metadata_columns:
            select_columns += ", " + ", ".join(metadata_columns)
        c.execute(f"SELECT {select_columns} FROM simulation_positions")
        positions = c.fetchall()
    except:
        positions = []
        metadata_columns = []

    last_buys = {}
    try:
        c.execute("""
            SELECT replace(replace(ticker, '.SH', ''), '.SZ', ''), MAX(traded_at)
            FROM simulation_trades
            WHERE direction = 'buy'
            GROUP BY replace(replace(ticker, '.SH', ''), '.SZ', '')
        """)
        last_buys = {row[0]: row[1] for row in c.fetchall()}
    except Exception:
        last_buys = {}

    # 获取最近交易
    try:
        c.execute("""
            SELECT ticker, direction, shares, price, reason, traded_at
            FROM simulation_trades
            ORDER BY traded_at DESC LIMIT 10
        """)
        trades = c.fetchall()
    except:
        trades = []

    try:
        c.execute(
            "SELECT COUNT(*) FROM simulation_trades "
            "WHERE date(traded_at) = date('now', 'localtime')"
        )
        trades_today = int(c.fetchone()[0])
    except sqlite3.Error:
        trades_today = 0
    try:
        from sovereign_hall.core.config import get_config

        max_daily_trades = int(
            get_config().get("simulation", {}).get("max_daily_trades", MAX_DAILY_TRADES)
        )
    except Exception:
        max_daily_trades = MAX_DAILY_TRADES

    redeployment_state = None
    try:
        state_columns = {
            row[1] for row in c.execute("PRAGMA table_info(simulation_redeployment_state)")
        }
        rejection_columns = [
            name for name in ("last_rejection_counts", "rejection_counts_total")
            if name in state_columns
        ]
        rejection_select = ", " + ", ".join(rejection_columns) if rejection_columns else ""
        c.execute(
            f"""
            SELECT status, deployment_gap, blocker_code, blocker_reason,
                   next_action, source, attempt_count, last_attempt_at,
                   last_candidate_count, last_trade_count, updated_at
                   {rejection_select}
            FROM simulation_redeployment_state WHERE id = 1
            """
        )
        row = c.fetchone()
        redeployment_state = dict(row) if row else None
    except sqlite3.Error:
        redeployment_state = None

    candidate_rejection_memory = []
    candidate_rejection_memory_available = False
    try:
        candidate_rejection_memory_available = bool(
            c.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='simulation_candidate_rejections'"
            ).fetchone()
        )
        c.execute(
            """
            SELECT ticker, code, rejection_count, last_reason, last_seen_at
            FROM simulation_candidate_rejections
            ORDER BY rejection_count DESC, datetime(last_seen_at) DESC, ticker
            LIMIT 5
            """
        )
        candidate_rejection_memory = [dict(row) for row in c.fetchall()]
    except sqlite3.Error:
        candidate_rejection_memory = []
        candidate_rejection_memory_available = False

    pending_diagnostics = pending_decision_diagnostics(conn)
    pending_decisions = pending_diagnostics["pending_rows"]
    pending_decision_total = pending_diagnostics["unresolved_count"]

    tickers = [pos[0] for pos in positions]
    conn.close()

    # 投资状态只接受实时行情；关闭或失败时显示不可估值，绝不回退旧价/成本价。
    use_realtime_quotes = realtime_quotes_enabled()
    realtime_prices = get_realtime_prices(tickers) if tickers and use_realtime_quotes else {}

    # 计算当前资产
    known_position_value = 0.0
    missing_realtime_tickers = []
    position_details = []
    lifecycle_details = []
    for pos in positions:
        ticker = pos[0]
        shares = pos[1]
        cost = pos[2]
        quote = realtime_prices.get(ticker)
        if quote:
            current_price = float(quote["price"])
            price_at = str(quote.get("fetched_at") or datetime.now().isoformat())
            price_source = str(quote.get("source") or "realtime_quote")
            position_value = shares * current_price
            known_position_value += position_value
            change = (current_price - cost) / cost * 100 if cost > 0 else 0
            sign = '+' if change >= 0 else ''
            position_details.append(
                f"  {ticker}: {shares}股 @ 实时现价{current_price:.3f} 成本{cost:.3f} "
                f"({sign}{change:.1f}%) | {price_source} @ {price_at}"
            )
        else:
            current_price = None
            price_at = ""
            price_source = "realtime_quote_unavailable"
            missing_realtime_tickers.append(ticker)
            position_details.append(
                f"  {ticker}: {shares}股 @ 实时现价不可用；不使用本地估值/预测价/成本价兜底"
            )
        opened_at = None
        if "opened_at" in metadata_columns:
            opened_at = pos["opened_at"]
        opened_at = opened_at or last_buys.get(normalize_ticker(ticker))
        lifecycle = review_position(
            ticker=ticker,
            avg_cost=float(cost),
            opened_at=opened_at,
            price=float(current_price) if current_price is not None else None,
            price_at=price_at,
            price_source=price_source,
            now=datetime.now(),
            max_price_age_days=3,
            stop_loss_pct=-0.08,
            take_profit_pct=0.15,
            max_holding_days=30,
        )
        lifecycle_details.append(
            f"  {ticker}: action={lifecycle.action}, held={lifecycle.holding_days if lifecycle.holding_days is not None else 'unknown'}d, "
            f"quote_age={lifecycle.price_age_days if lifecycle.price_age_days is not None else 'unknown'}d, {lifecycle.reason}"
        )

    valuation_complete = not missing_realtime_tickers
    total_value = cash + known_position_value if valuation_complete else None
    profit = total_value - initial_capital if total_value is not None else None
    profit_pct = (profit / initial_capital) * 100 if profit is not None else None

    print("\n" + "="*60)
    print("📊 投资模拟状态")
    print("="*60)
    print(f"   初始资金: {initial_capital:.2f} 元")
    if total_value is None:
        print(f"   当前资产: N/A（缺少实时现价: {', '.join(missing_realtime_tickers)}）")
        print("   盈亏: N/A（拒绝使用本地估值、历史预测价或成本价推算）")
    else:
        print(f"   当前资产: {total_value:.2f} 元（实时现价）")
    if profit is not None and profit >= 0:
        print(f"   📈 盈亏: +{profit:.2f} 元 ({profit_pct:+.2f}%)")
    elif profit is not None:
        print(f"   📉 盈亏: {profit:.2f} 元 ({profit_pct:+.2f}%)")
    print(f"   现金: {cash:.2f} 元")
    print(f"   今日模拟成交: {trades_today}/{max_daily_trades} 笔（每日硬上限）")
    print(f"   待执行裁决: {pending_decision_total} 条（成交前必须重新取实时行情并重过全部风控）")
    for pending in pending_decisions[:5]:
        print(
            f"      - {pending['ticker']} {pending['direction']} -> "
            f"{float(pending['target_position']):.1%} | {pending['defer_code']} | {pending['created_at']}"
        )
    pending_counts = pending_diagnostics["status_counts"]
    print(
        "   裁决生命周期累计: "
        f"executed={pending_counts.get('executed', 0)}, "
        f"rejected={pending_counts.get('rejected', 0)}, "
        f"expired={pending_counts.get('expired', 0)}, "
        f"pending={pending_decision_total}"
    )
    last_resolution = pending_diagnostics["last_resolution"]
    if last_resolution:
        print(
            "   最近裁决结果: "
            f"{last_resolution.get('status')} | {last_resolution.get('ticker')} "
            f"{last_resolution.get('direction')} | {last_resolution.get('resolved_at')} | "
            f"replay={int(last_resolution.get('replay_count') or 0)} | "
            f"{last_resolution.get('resolution') or last_resolution.get('defer_code') or ''}"
        )
    else:
        print("   最近裁决结果: 尚无已解决裁决")
    print(f"   Reward: {REWARD_FORMULA}")
    if total_value is None:
        print("   资金部署: N/A / 目标100.0%（实时估值不完整，禁止据此调仓）")
    else:
        deployment = deployment_status(cash, total_value, 1.0)
        print(
            f"   资金部署: {deployment['invested_ratio']:.1%} / 目标100.0%；"
            f"待部署 {deployment['deployment_gap']:.2f} 元"
        )
        if deployment['deployment_gap'] > 0:
            print("   说明: 现金不是风险储备；只允许因缺少合格标的、实时报价、手续费或整手约束暂时留存")

    print("\n   🧾 资金再配置队列:")
    if redeployment_state:
        gap = redeployment_state.get("deployment_gap")
        gap_text = "N/A" if gap is None else f"{float(gap):.2f} 元"
        print(
            f"   状态: {redeployment_state.get('status')} | gap={gap_text} | "
            f"尝试={int(redeployment_state.get('attempt_count') or 0)}次"
        )
        print(
            f"   最近尝试: {redeployment_state.get('last_attempt_at') or '尚未执行'} | "
            f"候选={int(redeployment_state.get('last_candidate_count') or 0)} | "
            f"成交={int(redeployment_state.get('last_trade_count') or 0)}"
        )
        if redeployment_state.get("blocker_code"):
            print(
                f"   操作性阻塞: {redeployment_state.get('blocker_code')}；"
                f"{redeployment_state.get('blocker_reason') or ''}"
            )
        for label, key in (
            ("本轮裁决否决", "last_rejection_counts"),
            ("累计裁决否决", "rejection_counts_total"),
        ):
            raw_counts = redeployment_state.get(key)
            if not raw_counts:
                continue
            try:
                counts = json.loads(raw_counts) if isinstance(raw_counts, str) else raw_counts
            except (TypeError, ValueError, json.JSONDecodeError):
                counts = {}
            if counts:
                summary = ", ".join(
                    f"{code}={int(count)}" for code, count in sorted(counts.items())
                )
                print(f"   {label}: {summary}")
        if candidate_rejection_memory:
            print("   逐标的重复拒绝记忆（仅统计迁移后真实尝试）:")
            for item in candidate_rejection_memory:
                reason = str(item.get("last_reason") or "未记录具体原因").replace("\n", " ")
                print(
                    f"      - {item.get('ticker')} / {item.get('code')} "
                    f"x{int(item.get('rejection_count') or 0)} | {reason[:220]}"
                )
            print("   重提要求: 必须给出新增本地可追溯证据，并明确消除哪条最近拒绝原因；否则继续hold")
        elif candidate_rejection_memory_available:
            print("   逐标的重复拒绝记忆: 0 条（不反推旧记录；下一次真实拒绝开始累计并反馈给投委会prompt）")
        print(f"   下一动作: {redeployment_state.get('next_action') or '下一轮继续评估'}")
        print(f"   状态来源: {redeployment_state.get('source') or 'unknown'}")
    elif total_value is not None and cash > 0:
        print("   状态尚未持久化；下一次 run_discussion 初始化时将从空仓现金恢复待部署队列")
    else:
        print("   (无待处理再配置状态)")

    print(f"\n   📦 当前持仓:")
    if position_details:
        for pd in position_details:
            print(pd)
        if not use_realtime_quotes:
            print("   提示: 实时行情被显式关闭，因此资产/盈亏为N/A；系统不会回退本地估值")
    else:
        print("   (空仓)")

    print("\n   🩺 强制持仓复核:")
    if lifecycle_details:
        for detail in lifecycle_details:
            print(detail)
    else:
        print("   (无持仓需要复核)")

    print(f"\n   📜 最近交易:")
    if trades:
        for trade in trades:
            print(f"   {trade[5][:10]} {trade[1]} {trade[0]} {trade[2]}股 @ {trade[3]:.2f}")
    else:
        print("   (无交易记录)")


def show_stats(db_path):
    """显示数据库统计"""
    print("\n" + "="*60)
    print("📊 Sovereign Hall - 数据库统计")
    print("="*60)

    # 先显示投资状态
    show_investment_status(db_path)
    try:
        from sovereign_hall.services.heuristic_policy import (
            format_heuristic_status,
            sync_simulation_risk_memory_sqlite,
        )

        with sqlite3.connect(str(db_path)) as conn:
            sync_simulation_risk_memory_sqlite(conn)
            progress = daily_price_backfill_progress(conn)
            if progress and _safe_int(progress.get("missing_signal_dates")) > 0:
                try:
                    progress["template_written_rows"] = export_daily_price_template_from_progress(progress)
                    progress["template_csv_status"] = inspect_local_daily_price_csv(
                        progress.get("stable_template_path") or LOCAL_DAILY_PRICE_TEMPLATE
                    )
                except Exception as template_exc:
                    progress["template_write_error"] = str(template_exc)
            backfill_progress = format_daily_price_backfill_progress(conn, progress=progress)
        print(format_heuristic_status())
        if backfill_progress:
            print(backfill_progress)
    except Exception as exc:
        print(f"\n🧭 Heuristic 学习状态: 无法读取 ({exc})")

    print(f"\n   数据库: {db_path.name}")
    print(f"   大小: {format_size(os.path.getsize(db_path))}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in c.fetchall()]

    print(f"\n   📋 数据表:")
    for i, table in enumerate(tables, 1):
        try:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            print(f"      {i}. {table}: {count:,} 条")
        except:
            print(f"      {i}. {table}: (无法读取)")

    c.execute("SELECT COUNT(*) FROM report_conclusions")
    rc_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM reflection_summary")
    rs_count = c.fetchone()[0]

    print(f"\n   📈 研究讨论统计:")
    print(f"      - 讨论结论: {rc_count} 条")
    print(f"      - 反思总结: {rs_count} 条")

    if "price_predictions" in tables:
        try:
            c.execute("""
                SELECT status, result, COUNT(*)
                FROM price_predictions
                GROUP BY status, result
                ORDER BY status, result
            """)
            prediction_rows = c.fetchall()
            total_predictions = sum(row[2] for row in prediction_rows)
            print(f"\n   🎯 预测验证统计:")
            print(f"      - 预测记录: {total_predictions:,} 条")
            if prediction_rows:
                for status, result, count in prediction_rows:
                    print(f"      - {status}/{result}: {count:,} 条")
                c.execute("""
                    SELECT COUNT(*)
                    FROM price_predictions
                    WHERE status = 'pending'
                    AND datetime(predicted_at, '+' || COALESCE(expected_days, 30) || ' days') <= datetime('now', 'localtime')
                """)
                due_count = c.fetchone()[0]
                c.execute("""
                    SELECT MIN(datetime(predicted_at, '+' || COALESCE(expected_days, 30) || ' days'))
                    FROM price_predictions
                    WHERE status = 'pending'
                """)
                next_due = c.fetchone()[0]
                print(f"      - 当前到期可验证: {due_count:,} 条")
                print(f"      - 下一批到期时间: {next_due or 'N/A'}")
            else:
                print("      - 暂无可信预测记录")
        except Exception as e:
            print(f"\n   🎯 预测验证统计: 无法读取 ({e})")

    conn.close()
    return tables


def browse_table(db_path, table_name):
    """浏览表内容"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = c.fetchone()[0]

    print(f"\n   📄 {table_name} (共 {total:,} 条)")

    limit = 3
    offset = 0

    while True:
        c.execute(f"SELECT * FROM {table_name} ORDER BY ROWID LIMIT ? OFFSET ?", (limit, offset))
        rows = c.fetchall()

        if not rows:
            break

        for row in rows:
            print(f"\n   {'-'*40}")
            row_dict = dict(row)
            for key, val in row_dict.items():
                if val is None:
                    continue
                val_str = str(val)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                print(f"   {key}: {val_str}")

        offset += limit
        more_raw = safe_input(f"\n   显示更多 {limit} 条? (y/n): ")
        if more_raw is None:
            print("\n   非交互输入结束，停止浏览")
            break
        more = more_raw.strip().lower()
        if more != 'y':
            break

    conn.close()


def show_recent_conclusions(db_path, limit=5):
    """显示最近的讨论结论"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""SELECT * FROM report_conclusions ORDER BY created_at DESC LIMIT ?""", (limit,))
    rows = c.fetchall()

    if not rows:
        print("\n   暂无讨论结论")
        conn.close()
        return

    print(f"\n   📋 最近 {len(rows)} 条讨论结论:")
    for i, row in enumerate(rows, 1):
        print(f"\n   【{i}】{row['ticker'] or 'N/A'} | {row['direction'] or 'N/A'} | 置信度: {row['confidence']:.0%}")
        print(f"   时间: {row['created_at'][:19]}")
        conclusion = row['conclusion'][:200] + "..." if row['conclusion'] and len(row['conclusion']) > 200 else row['conclusion'] or ""
        print(f"   结论: {conclusion}")

    conn.close()


def generate_topic_from_db(db_path) -> str:
    """从数据库动态生成研究议题"""
    import random

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. 获取最近的提案
    c.execute("SELECT ticker, direction, thesis FROM proposals ORDER BY created_at DESC LIMIT 20")
    proposals = c.fetchall()

    # 2. 获取最近的文档
    c.execute("SELECT title, sector FROM documents ORDER BY crawled_at DESC LIMIT 10")
    docs = c.fetchall()

    # 3. 获取最近的讨论结论
    c.execute("SELECT conclusion, ticker FROM report_conclusions ORDER BY created_at DESC LIMIT 5")
    conclusions = c.fetchall()

    conn.close()

    # 从提案中随机选择一个有投资价值的
    if proposals:
        # 选择有明确方向的提案
        valid_proposals = [p for p in proposals if p['direction'] and p['ticker']]
        if valid_proposals:
            prop = random.choice(valid_proposals)
            topic = f"分析 {prop['ticker']} 的投资价值，当前方向: {prop['direction']}"
            thesis_preview = prop['thesis'][:100] if prop['thesis'] else ""
            if thesis_preview:
                topic += f"，参考逻辑: {thesis_preview}..."
            return topic

    # 从文档中选择
    if docs:
        doc = random.choice(docs)
        if doc['sector']:
            return f"{doc['sector']}行业近期动态分析"
        return f"{doc['title'][:30]}相关投资机会"

    # 默认议题
    return "A股市场近期走势与投资机会"


def run_discussion_once(db_path):
    """运行一次讨论"""
    import asyncio

    from sovereign_hall.services.research_discussion import ResearchDiscussionSystem

    # 生成议题
    topic = generate_topic_from_db(db_path)
    print(f"\n   🎯 生成议题: {topic}")

    async def do_research():
        system = ResearchDiscussionSystem(
            enable_search=False,
            enable_web=False
        )
        context = await system.research(topic)
        return context

    try:
        context = asyncio.run(do_research())
        print(f"\n✅ 讨论完成！结论已保存到数据库")
        return True
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        return False


def validate_pending_predictions():
    """验证已到期的待验证预测"""
    import asyncio
    from sovereign_hall.services.decision_tracker import DecisionRecorder

    async def _run():
        recorder = DecisionRecorder()
        return await recorder.validate_pending(max_count=50)

    try:
        result = asyncio.run(_run())
        print("\n" + "="*60)
        print("🎯 到期预测验证")
        print("="*60)
        print(f"   本次处理: {result.get('validated', 0)} 条")
        print(f"   正确数量: {result.get('correct', 0)} 条")
        if result.get("results"):
            for item in result["results"][:10]:
                if "error" in item:
                    print(f"   - {item['error']}")
                else:
                    print(f"   - {item.get('result')} | accuracy={item.get('accuracy', 0):.2f} | price={item.get('current_price')}")
        return True
    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        return False


def clean_database(db_path):
    """清洗数据库 - 删除无实际内容的文档和提案"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print("\n" + "="*60)
    print("🧹 数据库清洗")
    print("="*60)

    # 1. 统计当前情况
    c.execute("SELECT COUNT(*) FROM documents")
    total_docs = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM proposals")
    total_proposals = c.fetchone()[0]

    print(f"清洗前:")
    print(f"  文档: {total_docs} 条")
    print(f"  提案: {total_proposals} 条")

    # 2. 定义需要删除的内容模式
    delete_patterns = [
        "Detailed content",
        "测试文档",
        "Mock",
        "Example domain",
        "占位符",
    ]

    placeholders = [
        "Detailed content about",
        "This is a test",
        "测试标题",
    ]

    # 3. 删除文档
    conditions = []
    for pattern in delete_patterns:
        conditions.append(f"content LIKE '%{pattern}%'")
    for ph in placeholders:
        conditions.append(f"title LIKE '%{ph}%'")
        conditions.append(f"content LIKE '%{ph}%'")

    # 内容过短
    conditions.append("length(content) < 50")
    # URL 为空
    conditions.append("url IS NULL OR url = ''")
    # 来源为 mock
    conditions.append("source LIKE '%mock%' OR source = 'MockSource'")

    where_clause = " OR ".join(conditions)
    where_clause = f"({where_clause})"

    # 获取要删除的文档ID
    c.execute(f"SELECT id, title FROM documents WHERE {where_clause}")
    to_delete_docs = c.fetchall()
    doc_ids_to_delete = [row[0] for row in to_delete_docs]

    if doc_ids_to_delete:
        print(f"\n将删除 {len(doc_ids_to_delete)} 条无效文档:")
        for row in to_delete_docs[:10]:
            print(f"  - {row[1][:50]}...")
        if len(doc_ids_to_delete) > 10:
            print(f"  ... 共 {len(doc_ids_to_delete)} 条")

        c.execute(f"DELETE FROM documents WHERE id IN ({','.join(['?' for _ in doc_ids_to_delete])})", doc_ids_to_delete)
        deleted_docs = c.rowcount
    else:
        deleted_docs = 0
        print("\n✅ 没有需要删除的文档")

    # 4. 删除无效提案 (proposals表主键是 proposal_id)
    c.execute("SELECT proposal_id, ticker FROM proposals WHERE ticker IS NULL OR ticker = '' OR ticker LIKE '%test%' OR ticker LIKE '%mock%' OR ticker LIKE '%BASKET'")
    to_delete_proposals = c.fetchall()
    proposal_ids_to_delete = [row[0] for row in to_delete_proposals]

    if proposal_ids_to_delete:
        c.execute(f"DELETE FROM proposals WHERE proposal_id IN ({','.join(['?' for _ in proposal_ids_to_delete])})", proposal_ids_to_delete)
        deleted_proposals = c.rowcount
    else:
        deleted_proposals = 0

    # 5. 删除没有有效ticker的提案
    c.execute("SELECT proposal_id FROM proposals WHERE ticker IS NULL OR TRIM(ticker) = '' OR length(ticker) < 2 OR ticker = 'NULL'")
    empty_ticker = [row[0] for row in c.fetchall()]
    if empty_ticker:
        c.execute(f"DELETE FROM proposals WHERE proposal_id IN ({','.join(['?' for _ in empty_ticker])})", empty_ticker)
        deleted_proposals += c.rowcount

    conn.commit()

    # 6. 显示清洗后结果
    c.execute("SELECT COUNT(*) FROM documents")
    after_docs = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM proposals")
    after_proposals = c.fetchone()[0]

    print(f"\n清洗后:")
    print(f"  文档: {after_docs} 条 (删除 {deleted_docs} 条)")
    print(f"  提案: {after_proposals} 条 (删除 {deleted_proposals} 条)")

    conn.close()
    return deleted_docs, deleted_proposals


def main():
    db_path = project_root / "data" / "sovereign_hall.db"

    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        return

    tables = show_stats(db_path)

    print("\n" + "="*60)
    print("请选择操作:")
    print("   1. 查看统计数据")
    print("   2. 浏览数据表内容")
    print("   3. 查看最近讨论结论")
    print("   4. 运行一次讨论（动态生成议题）")
    print("   5. 🧹 清洗数据库（删除无效数据）")
    print("   6. 🎯 验证到期预测")
    print("   q. 退出")
    print("="*60)

    while True:
        choice_raw = safe_input("\n👉 请选择 (1/2/3/4/5/6/q): ")
        if choice_raw is None:
            print("\n👋 非交互输入结束，安全退出")
            break
        choice = choice_raw.strip().lower()

        if not choice:
            print("👋 空输入，安全退出")
            break
        if choice == '1':
            show_stats(db_path)
        elif choice == '2':
            print(f"\n   可浏览的表: {', '.join(tables)}")
            table_raw = safe_input("   输入表名: ")
            if table_raw is None:
                print("\n   非交互输入结束，取消浏览")
                break
            table = table_raw.strip()
            if table in tables:
                browse_table(db_path, table)
            else:
                print(f"   表 '{table}' 不存在")
        elif choice == '3':
            show_recent_conclusions(db_path)
        elif choice == '4':
            run_discussion_once(db_path)
        elif choice == '5':
            clean_database(db_path)
        elif choice == '6':
            validate_pending_predictions()
        elif choice == 'q':
            print("👋 再见！")
            break
        else:
            print("   无效选择")

        print("\n" + "-"*60)


if __name__ == "__main__":
    main()
