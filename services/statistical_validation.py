"""Deterministic multiple-testing diagnostics for strategy qualification.

These diagnostics are deliberately gates, not strategy optimizers.  They only
become available when enough observations and a genuine family of recorded
trials exist; otherwise callers must keep the user-facing result ``未证明``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import combinations
import math
from statistics import NormalDist, mean, pstdev
from typing import TYPE_CHECKING, Mapping, Sequence

if TYPE_CHECKING:
    from .experiment_store import TrialReturnsResult


@dataclass(frozen=True)
class StatisticalValidationResult:
    dsr_probability: float | None
    pbo_probability: float | None
    trial_count: int
    observation_count: int
    status: str
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def annualized_sharpe(returns: Sequence[float]) -> float | None:
    values = _finite(returns)
    if len(values) < 2:
        return None
    volatility = pstdev(values)
    if volatility <= 0:
        return None
    return mean(values) / volatility * math.sqrt(252.0)


def deflated_sharpe_probability(
    returns: Sequence[float],
    *,
    trial_sharpes: Sequence[float],
) -> float | None:
    """Estimate the probability that Sharpe exceeds selection bias.

    Implements the Deflated Sharpe Ratio structure from Bailey and
    López de Prado using an expected-maximum Sharpe benchmark and a
    skew/kurtosis-adjusted sampling variance.  No value is emitted for short
    samples or a single trial because that would overstate certainty.
    """
    values = _finite(returns)
    trials = _finite(trial_sharpes)
    if len(values) < 30 or len(trials) < 2:
        return None
    daily_volatility = pstdev(values)
    if daily_volatility <= 0:
        return None
    daily_sr = mean(values) / daily_volatility
    centered = [(value - mean(values)) / daily_volatility for value in values]
    skew = mean([value**3 for value in centered])
    kurtosis = mean([value**4 for value in centered])
    variance_daily_sr = (
        1.0 - skew * daily_sr + ((kurtosis - 1.0) / 4.0) * daily_sr**2
    ) / max(len(values) - 1, 1)
    if variance_daily_sr <= 0:
        return None

    trial_sigma = pstdev(trials)
    if trial_sigma <= 0:
        expected_maximum = max(trials)
    else:
        trial_count = len(trials)
        normal = NormalDist()
        euler_gamma = 0.5772156649015329
        upper_one = normal.inv_cdf(1.0 - 1.0 / trial_count)
        upper_e = normal.inv_cdf(1.0 - 1.0 / (trial_count * math.e))
        expected_maximum = mean(trials) + trial_sigma * (
            (1.0 - euler_gamma) * upper_one + euler_gamma * upper_e
        )

    observed_annual_sr = daily_sr * math.sqrt(252.0)
    annual_standard_error = math.sqrt(variance_daily_sr * 252.0)
    z_score = (observed_annual_sr - expected_maximum) / annual_standard_error
    return min(max(NormalDist().cdf(z_score), 0.0), 1.0)


def probability_of_backtest_overfitting(
    trial_returns: Mapping[str, Sequence[float]],
    *,
    partitions: int = 8,
) -> float | None:
    """Return the CSCV probability that the in-sample winner underperforms OOS."""
    if len(trial_returns) < 2:
        return None
    names = sorted(trial_returns)
    lengths = {len(trial_returns[name]) for name in names}
    if len(lengths) != 1:
        return None
    observation_count = lengths.pop()
    partitions = min(max(int(partitions), 4), observation_count)
    if partitions % 2:
        partitions -= 1
    if partitions < 4:
        return None

    values = {name: _finite(trial_returns[name]) for name in names}
    if any(len(series) != observation_count for series in values.values()):
        return None
    blocks = _contiguous_blocks(observation_count, partitions)
    logits: list[float] = []
    half = partitions // 2
    for selected_blocks in combinations(range(partitions), half):
        selected = set(selected_blocks)
        # Symmetric complements carry the same information in reverse.  Keep
        # one orientation for stable runtime and no double-counting.
        if 0 not in selected:
            continue
        train_indices = [index for block in selected for index in blocks[block]]
        test_indices = [
            index for block in range(partitions) if block not in selected for index in blocks[block]
        ]
        train_scores = {
            name: _score([values[name][index] for index in train_indices])
            for name in names
        }
        winner = max(names, key=lambda name: (train_scores[name], name))
        test_scores = {
            name: _score([values[name][index] for index in test_indices])
            for name in names
        }
        ordered = sorted(names, key=lambda name: (test_scores[name], name))
        rank = ordered.index(winner) + 1
        percentile = rank / (len(names) + 1.0)
        logits.append(math.log(percentile / (1.0 - percentile)))
    if not logits:
        return None
    return sum(1 for value in logits if value <= 0.0) / len(logits)


def validate_experiment_family(
    candidate_returns: Sequence[float],
    trial_returns: Mapping[str, Sequence[float]],
) -> StatisticalValidationResult:
    values = _finite(candidate_returns)
    trial_sharpes = [
        value
        for value in (annualized_sharpe(series) for series in trial_returns.values())
        if value is not None
    ]
    dsr = deflated_sharpe_probability(values, trial_sharpes=trial_sharpes)
    pbo = probability_of_backtest_overfitting(trial_returns)
    reasons: list[str] = []
    if dsr is None:
        reasons.append("dsr_requires_at_least_two_trials_and_30_returns")
    if pbo is None:
        reasons.append("pbo_requires_aligned_returns_for_at_least_two_trials")
    return StatisticalValidationResult(
        dsr_probability=dsr,
        pbo_probability=pbo,
        trial_count=len(trial_returns),
        observation_count=len(values),
        status="available" if not reasons else "insufficient_evidence",
        reasons=tuple(reasons),
    )


def validate_dated_trial_family(
    candidate_returns: Sequence[float],
    dated_result: "TrialReturnsResult",
) -> StatisticalValidationResult:
    """Validate a dated trial family.

    PR4.2: statistical validation must reject a family whose series share
    a length but not the same dates.  The dated result carries the
    alignment decision and quality warnings; this function turns them into
    the same ``StatisticalValidationResult`` shape callers already use.
    """
    reasons: list[str] = list(dated_result.quality_warnings)
    if dated_result.family_status != "aligned":
        reasons.append(f"family_status={dated_result.family_status}")
        return StatisticalValidationResult(
            dsr_probability=None,
            pbo_probability=None,
            trial_count=len(dated_result.by_strategy),
            observation_count=0,
            status="insufficient_evidence",
            reasons=tuple(reasons),
        )
    aligned = dated_result.aligned_returns()
    if len(aligned) < 2:
        reasons.append("requires_at_least_two_aligned_series")
        return StatisticalValidationResult(
            dsr_probability=None,
            pbo_probability=None,
            trial_count=len(dated_result.by_strategy),
            observation_count=0,
            status="insufficient_evidence",
            reasons=tuple(reasons),
        )
    result = validate_experiment_family(candidate_returns, aligned)
    return StatisticalValidationResult(
        dsr_probability=result.dsr_probability,
        pbo_probability=result.pbo_probability,
        trial_count=result.trial_count,
        observation_count=result.observation_count,
        status=result.status,
        reasons=tuple(reasons) + list(result.reasons),
    )


def _score(values: Sequence[float]) -> float:
    finite = _finite(values)
    if not finite:
        return float("-inf")
    volatility = pstdev(finite) if len(finite) > 1 else 0.0
    if volatility <= 0:
        return mean(finite)
    return mean(finite) / volatility


def _finite(values: Sequence[float]) -> list[float]:
    result: list[float] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            result.append(number)
    return result


def _contiguous_blocks(length: int, partitions: int) -> list[list[int]]:
    base, remainder = divmod(length, partitions)
    blocks: list[list[int]] = []
    cursor = 0
    for block_index in range(partitions):
        width = base + (1 if block_index < remainder else 0)
        blocks.append(list(range(cursor, cursor + width)))
        cursor += width
    return blocks

