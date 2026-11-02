"""PR2.3 — Compute the daily market-data target set per active experiment.

The target set for any given session is the union of:

* current holdings in the simulated account,
* holdings in each active shadow experiment (control and candidate arms),
* tickers in frozen candidate universes,
* fixed benchmarks (e.g. CSI 300 buy-and-hold).

Each experiment also declares a per-ticker warmup window from its first
signal date (or registered warmup) to the latest completed trading day.  The
union is materialised here so the closing-time data job knows exactly what to
fetch and can refuse to widen the universe to "every historical ticker".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Iterable, Mapping, Sequence

from ..domain.portfolio.instruments import (
    CSI_300_INDEX,
    InstrumentIdentity,
)


@dataclass(frozen=True)
class ExperimentTickerRequirement:
    """One ticker required by one experiment over a dated window."""

    experiment_id: str
    ticker: str
    warmup_start: str
    required_through: str
    reason: str


@dataclass(frozen=True)
class MarketDataRequirements:
    """The union of tickers the closing-time data job must fetch.

    Frozen investable universes cannot be downsampled to only the survivors;
    the requirement preserves every ticker the experiment was registered
    against so a backtest cannot quietly drop losers.
    """

    as_of: str
    tickers: tuple[str, ...]
    by_experiment: tuple[ExperimentTickerRequirement, ...] = field(default_factory=tuple)
    benchmark_identities: tuple[InstrumentIdentity, ...] = (
        CSI_300_INDEX,
    )

    def covers(self, ticker: str) -> bool:
        cleaned = str(ticker or "").strip().upper().split(".", 1)[0]
        return any(cleaned == t.upper() for t in self.tickers)


def build_requirements(
    *,
    as_of: date | datetime | str | None = None,
    current_holdings: Iterable[str] = (),
    active_experiments: Sequence[Mapping[str, object]] = (),
    frozen_candidates: Iterable[str] = (),
    benchmark_identities: Sequence[InstrumentIdentity] = (CSI_300_INDEX,),
) -> MarketDataRequirements:
    """Materialise the union target set for the closing-time data job.

    ``active_experiments`` is a sequence of mappings, each shaped like::

        {
            "experiment_id": "exp_...",
            "arm_tickers": ["600519", "000858"],   # control + candidate arms
            "frozen_universe": ["600519", "000858", "002714", ...],
            "warmup_start": "2026-06-01",
            "required_through": "2026-09-13",
        }

    The frozen universe is preserved verbatim; no ticker is dropped because
    its price tape is hard to fetch or its realised return was poor.
    """
    as_of_value = (
        as_of.isoformat()[:10]
        if isinstance(as_of, (date, datetime))
        else str(as_of or datetime.now().isoformat())[:10]
    )
    requirements: list[ExperimentTickerRequirement] = []
    tickers: set[str] = set()

    for holding in current_holdings:
        cleaned = str(holding or "").strip().upper().split(".", 1)[0]
        if cleaned:
            tickers.add(cleaned)

    for ticker in frozen_candidates:
        cleaned = str(ticker or "").strip().upper().split(".", 1)[0]
        if cleaned:
            tickers.add(cleaned)

    for experiment in active_experiments:
        experiment_id = str(experiment.get("experiment_id") or "")
        if not experiment_id:
            continue
        warmup_start = str(experiment.get("warmup_start") or as_of_value)
        required_through = str(
            experiment.get("required_through") or as_of_value
        )
        arm_tickers = list(experiment.get("arm_tickers") or [])
        frozen_universe = list(experiment.get("frozen_universe") or [])
        # Both the arm holdings AND the frozen universe are required.  A
        # candidate that left the arm still has to be priced for the
        # experiment's realised return.
        for ticker in {*arm_tickers, *frozen_universe}:
            cleaned = str(ticker or "").strip().upper().split(".", 1)[0]
            if not cleaned:
                continue
            tickers.add(cleaned)
            requirements.append(
                ExperimentTickerRequirement(
                    experiment_id=experiment_id,
                    ticker=cleaned,
                    warmup_start=warmup_start,
                    required_through=required_through,
                    reason="arm_holding" if ticker in arm_tickers else "frozen_universe",
                )
            )

    for identity in benchmark_identities:
        tickers.add(str(identity.code))

    return MarketDataRequirements(
        as_of=as_of_value,
        tickers=tuple(sorted(tickers)),
        by_experiment=tuple(requirements),
        benchmark_identities=tuple(benchmark_identities),
    )
