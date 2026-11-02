"""Tests for PR2.3 / PR2.4 — requirements union, daily-price revisions and
data-fetch failure log."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from sovereign_hall.domain.portfolio.instruments import CSI_300_INDEX
from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.services.daily_price_revision_store import (
    DailyPriceRevisionStore,
)
from sovereign_hall.services.market_data_requirements import build_requirements


def test_build_requirements_unions_holdings_experiments_and_benchmark():
    requirements = build_requirements(
        as_of="2026-09-13",
        current_holdings=["600519", "000858"],
        active_experiments=[
            {
                "experiment_id": "exp_alpha",
                "arm_tickers": ["600519", "002714"],
                "frozen_universe": ["600519", "000858", "002714", "300750"],
                "warmup_start": "2026-06-01",
                "required_through": "2026-09-13",
            }
        ],
        frozen_candidates=["000333"],
        benchmark_identities=[CSI_300_INDEX],
    )
    # The union must include every frozen-universe ticker even if the
    # experiment arm no longer holds it; a backtest cannot drop losers.
    assert set(requirements.tickers) == {
        "600519", "000858", "002714", "300750", "000333", "000300"
    }
    assert requirements.covers("600519")
    assert requirements.covers("300750")
    by_exp = [r for r in requirements.by_experiment if r.experiment_id == "exp_alpha"]
    assert {r.ticker for r in by_exp} == {"600519", "000858", "002714", "300750"}
    # The benchmark is in the union too.
    assert "000300" in requirements.tickers


@pytest.mark.asyncio
async def test_append_batch_writes_revisions_and_latest_view(tmp_path: Path):
    db_path = tmp_path / "prices.db"
    store = DailyPriceRevisionStore(db_path)
    raw_payload = '[{"date":"2026-09-13","close":10.0,"ticker":"600519"}]'
    count, revisions = await store.append_batch(
        batch_id="batch_2026_09_13_a",
        provider="eastmoney_ohlc_qfq",
        adjustment="qfq",
        bars=[
            {"ticker": "600519", "date": "2026-09-13", "open": 9.8,
             "high": 10.2, "low": 9.7, "close": 10.0, "volume": 1000},
            {"ticker": "000858", "date": "2026-09-13", "open": 168.0,
             "high": 170.0, "low": 167.5, "close": 169.5, "volume": 5000},
        ],
        raw_response=raw_payload,
    )
    assert count == 2
    assert {r.ticker for r in revisions} == {"600519", "000858"}
    assert revisions[0].raw_response_hash
    assert revisions[0].batch_coverage_start == "2026-09-13"
    assert revisions[0].batch_coverage_end == "2026-09-13"

    # The mutable latest view is also populated for legacy callers.
    async with aiosqlite.connect(db_path) as conn:
        async with conn.execute(
            "SELECT close FROM daily_prices WHERE ticker = ? AND date = ?",
            ("600519", "2026-09-13"),
        ) as cursor:
            row = await cursor.fetchone()
    assert row is not None and float(row[0]) == 10.0


@pytest.mark.asyncio
async def test_read_revision_returns_immutable_history(tmp_path: Path):
    db_path = tmp_path / "prices.db"
    store = DailyPriceRevisionStore(db_path)
    # First provider wrote close=10.0.
    await store.append_batch(
        batch_id="batch_a",
        provider="eastmoney",
        adjustment="qfq",
        bars=[{"ticker": "600519", "date": "2026-09-13", "close": 10.0}],
    )
    # Provider revised the same bar to close=10.5 in a new batch.
    await store.append_batch(
        batch_id="batch_b",
        provider="eastmoney",
        adjustment="qfq",
        bars=[{"ticker": "600519", "date": "2026-09-13", "close": 10.5}],
    )
    # A frozen evaluation that referenced batch_a must still see close=10.0.
    revision = await store.read_revision(
        batch_id="batch_a", ticker="600519", bar_date="2026-09-13"
    )
    assert revision is not None
    assert revision.close == 10.0
    later = await store.read_revision(
        batch_id="batch_b", ticker="600519", bar_date="2026-09-13"
    )
    assert later is not None and later.close == 10.5


@pytest.mark.asyncio
async def test_record_failure_logs_concrete_next_action(tmp_path: Path):
    db_path = tmp_path / "prices.db"
    store = DailyPriceRevisionStore(db_path)
    failure = await store.record_failure(
        ticker="600519",
        bar_date="2026-09-13",
        provider="eastmoney",
        error="HTTP 502 from upstream",
        next_action="switch_provider",
        context={"attempt": 1, "elapsed_seconds": 4.2},
    )
    assert failure.next_action == "switch_provider"
    latest = await store.latest_failure(ticker="600519", bar_date="2026-09-13")
    assert latest is not None
    assert latest.error == "HTTP 502 from upstream"
    assert latest.context.get("attempt") == 1


@pytest.mark.asyncio
async def test_record_failure_rejects_unknown_next_action(tmp_path: Path):
    db_path = tmp_path / "prices.db"
    store = DailyPriceRevisionStore(db_path)
    with pytest.raises(ValueError, match="unknown next_action"):
        await store.record_failure(
            ticker="600519",
            bar_date="2026-09-13",
            provider="eastmoney",
            error="oops",
            next_action="hope_for_the_best",
        )


@pytest.mark.asyncio
async def test_apply_architecture_migrations_creates_new_tables(tmp_path: Path):
    db_path = tmp_path / "prices.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        async with conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('daily_price_revisions', 'data_fetch_failures')"
        ) as cursor:
            names = {str(row[0]) for row in await cursor.fetchall()}
    assert {"daily_price_revisions", "data_fetch_failures"} <= names
