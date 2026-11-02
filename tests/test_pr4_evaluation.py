"""Tests for PR4 — dated trial returns, statistical validation and benchmark attribution."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.services.benchmark_attribution import (
    BenchmarkBar,
    build_attribution_report,
    compute_buy_and_hold,
)
from sovereign_hall.services.experiment_store import (
    ExperimentStore,
    TrialReturnsResult,
    TrialReturnSeries,
)
from sovereign_hall.services.statistical_validation import (
    validate_dated_trial_family,
    validate_experiment_family,
)


@pytest.mark.asyncio
async def test_unique_trial_returns_dated_rejects_same_length_different_dates(
    tmp_path: Path,
):
    """PR4.2: two series with the same length but different dates must
    surface as a misalignment warning, not silent alignment."""
    db_path = tmp_path / "trials.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        # Strategy A: dates 2026-09-10..2026-09-14
        await conn.execute(
            """
            INSERT INTO evaluation_runs (
                id, sleeve, strategy_version, strategy_hash, engine_version,
                data_cutoff, data_hash, universe_hash, universe_json,
                cost_model_version, cost_schedule_json, initial_capital,
                trial_count, status, config_json, metrics_json, result_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, '{}', 10000, 1, ?,
                      '{}', '{}', ?, ?)
            """,
            (
                "run_a", "llm_research", "strategy_a", "hash_a", "canonical_v1",
                "2026-09-14", "data_hash_a", "uni_hash_a",
                "fee_v1", "available",
                '{"curve": ['
                '{"date": "2026-09-10", "net_return": 0.01},'
                '{"date": "2026-09-11", "net_return": 0.02},'
                '{"date": "2026-09-12", "net_return": 0.015},'
                '{"date": "2026-09-13", "net_return": -0.005},'
                '{"date": "2026-09-14", "net_return": 0.012}'
                ']}',
                "2026-09-14T16:00:00",
            ),
        )
        # Strategy B: same length (5 rows) but different dates
        await conn.execute(
            """
            INSERT INTO evaluation_runs (
                id, sleeve, strategy_version, strategy_hash, engine_version,
                data_cutoff, data_hash, universe_hash, universe_json,
                cost_model_version, cost_schedule_json, initial_capital,
                trial_count, status, config_json, metrics_json, result_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, '{}', 10000, 1, ?,
                      '{}', '{}', ?, ?)
            """,
            (
                "run_b", "llm_research", "strategy_b", "hash_b", "canonical_v1",
                "2026-09-14", "data_hash_b", "uni_hash_b",
                "fee_v1", "available",
                '{"curve": ['
                '{"date": "2026-09-15", "net_return": 0.02},'
                '{"date": "2026-09-16", "net_return": 0.018},'
                '{"date": "2026-09-17", "net_return": -0.01},'
                '{"date": "2026-09-18", "net_return": 0.005},'
                '{"date": "2026-09-19", "net_return": 0.021}'
                ']}',
                "2026-09-19T16:00:00",
            ),
        )
        await conn.commit()

    store = ExperimentStore(db_path)
    result = await store.unique_trial_returns_dated("llm_research")
    assert isinstance(result, TrialReturnsResult)
    assert len(result.by_strategy) == 2
    # No common dates — must surface as misaligned, not as aligned empty.
    assert result.family_status == "misaligned_dates"
    assert result.common_dates == ()
    assert any("no common dates" in w for w in result.quality_warnings)


@pytest.mark.asyncio
async def test_unique_trial_returns_dated_aligns_on_common_dates(tmp_path: Path):
    db_path = tmp_path / "trials.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        for run_id, strategy, date_offset, payload in (
            (
                "run_a", "strategy_a", 0,
                '{"curve": ['
                '{"date": "2026-09-10", "net_return": 0.01},'
                '{"date": "2026-09-11", "net_return": 0.02}'
                ']}',
            ),
            (
                "run_b", "strategy_b", 0,
                '{"curve": ['
                '{"date": "2026-09-10", "net_return": 0.005},'
                '{"date": "2026-09-11", "net_return": 0.015}'
                ']}',
            ),
        ):
            await conn.execute(
                """
                INSERT INTO evaluation_runs (
                    id, sleeve, strategy_version, strategy_hash, engine_version,
                    data_cutoff, data_hash, universe_hash, universe_json,
                    cost_model_version, cost_schedule_json, initial_capital,
                    trial_count, status, config_json, metrics_json, result_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, '{}', 10000, 1, ?,
                          '{}', '{}', ?, ?)
                """,
                (
                    run_id, "llm_research", strategy, f"hash_{strategy}",
                    "canonical_v1", "2026-09-11", f"data_{strategy}",
                    f"uni_{strategy}", "fee_v1", "available", payload,
                    "2026-09-11T16:00:00",
                ),
            )
        await conn.commit()

    store = ExperimentStore(db_path)
    result = await store.unique_trial_returns_dated("llm_research")
    assert result.family_status == "aligned"
    assert result.common_dates == ("2026-09-10", "2026-09-11")
    aligned = result.aligned_returns()
    assert aligned["strategy_a"] == [0.01, 0.02]
    assert aligned["strategy_b"] == [0.005, 0.015]


def test_validate_dated_trial_family_blocks_misaligned_family():
    misaligned = TrialReturnsResult(
        by_strategy={
            "a": TrialReturnSeries(
                strategy_version="a",
                strategy_hash="h",
                dated_returns={"2026-09-10": 0.01},
            ),
            "b": TrialReturnSeries(
                strategy_version="b",
                strategy_hash="h",
                dated_returns={"2026-09-11": 0.02},
            ),
        },
        common_dates=(),
        quality_warnings=("no common dates across series; cannot align",),
        family_status="misaligned_dates",
    )
    result = validate_dated_trial_family([0.01], misaligned)
    assert result.status == "insufficient_evidence"
    assert result.dsr_probability is None
    assert result.pbo_probability is None
    assert any("misaligned" in reason for reason in result.reasons)


def test_compute_buy_and_hold_charges_fees_and_respects_lot_size():
    bars = [
        BenchmarkBar(date="2026-09-10", close=10.0),
        BenchmarkBar(date="2026-09-11", close=10.5),
        BenchmarkBar(date="2026-09-12", close=10.2),
    ]
    result = compute_buy_and_hold(
        ticker="510300",
        bars=bars,
        initial_capital=10000.0,
        lot_size=100,
        commission_bps=2.5,
        stamp_duty_bps=10.0,
    )
    assert result is not None
    # 10000 - commission (5 min) = 9995; 9995 / 10 = 999.5 -> 900 shares
    assert result.shares == 900
    assert result.entry_price == 10.0
    # Final value = cash + 900 * 10.2; cash = 10000 - 9000 - 5 = 995
    assert result.final_value == pytest.approx(995 + 900 * 10.2)
    assert result.net_return < 0.02  # roughly flat after fees


def test_build_attribution_report_discloses_b_minus_a_and_benchmark_lag():
    benchmark = compute_buy_and_hold(
        ticker="510300",
        bars=[
            BenchmarkBar(date="2026-09-10", close=10.0),
            BenchmarkBar(date="2026-09-12", close=11.0),
        ],
        initial_capital=10000.0,
        lot_size=100,
        commission_bps=2.5,
        stamp_duty_bps=10.0,
    )
    assert benchmark is not None
    report = build_attribution_report(
        experiment_id="exp_alpha",
        control_net_return=0.03,
        candidate_net_return=0.05,
        control_fees=15.0,
        candidate_fees=18.0,
        control_turnover=0.4,
        candidate_turnover=0.6,
        control_holding_period_days=10.0,
        candidate_holding_period_days=8.0,
        control_sample_count=20,
        candidate_sample_count=20,
        benchmark=benchmark,
        notes=["B beats A but both lag benchmark"],
    )
    assert report.b_minus_a == pytest.approx(0.02)
    # Benchmark roughly: 900 shares at 10 -> 9000 + 995 cash; final 900*11 + 995 = 10895
    # benchmark return ~ 0.0895
    assert report.control.excess_vs_benchmark == pytest.approx(0.03 - benchmark.net_return)
    assert report.candidate.excess_vs_benchmark == pytest.approx(0.05 - benchmark.net_return)
    assert report.notes[0] == "B beats A but both lag benchmark"
