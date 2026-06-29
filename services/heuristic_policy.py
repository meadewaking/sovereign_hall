"""Shared access to the latest local heuristic-cycle artifacts.

The helper is intentionally read-only except for applying conservative risk
caps. It never calls market data services and never places orders.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs" / "heuristic_cycle"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "sovereign_hall.db"
SIMULATION_RISK_LOSS_THRESHOLD = -0.03
SIMULATION_RISK_MEMORY_DAYS = 8
WEAK_PRICE_COVERAGE_POSITION_SCALE = 0.5
UNVALIDATED_PRICE_COVERAGE_POSITION_SCALE = 0.25
PARTIAL_PRICE_COVERAGE_POSITION_SCALE = 0.35
INSUFFICIENT_SIGNAL_POSITION_SCALE = 0.3
THIN_TAPE_UPDATE_POSITION_SCALE = 0.2
ZERO_NEW_TAPE_UPDATE_POSITION_SCALE = 0.1
PRICE_READINESS_BLOCKED_POSITION_SCALE = 0.1
PRICE_READINESS_PARTIAL_POSITION_SCALE = 0.25
PRICE_READINESS_STALLED_POSITION_SCALE = 0.05
PRICE_READINESS_STALLED_MIN_RUNS = 3


@dataclass(frozen=True)
class HeuristicRiskContext:
    run_dir: Path | None
    policy_name: str
    score: float | None
    max_position: float
    overfit_risk: bool
    warning: str
    failure_cases: list[dict[str, Any]]
    min_signal_count: int = 1
    min_confidence: float | None = None
    min_risk_reward: float | None = None
    min_holding_days: int | None = None
    max_gross: float | None = None
    universe: str = ""
    failure_ticker_scale: float = 0.5
    simulation_failures: list[dict[str, Any]] = field(default_factory=list)
    out_of_sample_score: float | None = None
    cost_stress_score: float | None = None
    sleeve_diagnostics: dict[str, Any] = field(default_factory=dict)
    price_source: str = ""
    price_coverage: dict[str, Any] = field(default_factory=dict)
    tape_update: dict[str, Any] = field(default_factory=dict)
    price_readiness: dict[str, Any] = field(default_factory=dict)
    price_readiness_stall: dict[str, Any] = field(default_factory=dict)
    evaluation_engine: str = ""
    evaluation_warning: str = ""
    evaluator_health: dict[str, Any] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return self.run_dir is not None and bool(self.policy_name)

    @property
    def thin_cost_stress_margin(self) -> bool:
        return self.cost_stress_score is not None and self.cost_stress_score < 0.02

    @property
    def price_source_unvalidated(self) -> bool:
        return "prediction current_price fallback" in self.price_source

    @property
    def weak_price_coverage(self) -> bool:
        if self.price_source_unvalidated:
            return True
        status = str(self.price_coverage.get("status", ""))
        independent_raw = self.price_coverage.get("independent_price_row_ratio")
        independent_ratio = float(independent_raw) if independent_raw is not None else 1.0
        missing_slot_ratio = float(self.price_coverage.get("missing_position_price_slot_ratio", 0.0) or 0.0)
        return (
            status.startswith("unvalidated")
            or status.startswith("partial")
            or independent_ratio < 0.80
            or missing_slot_ratio > 0.10
        )

    @property
    def thin_tape_update(self) -> bool:
        status = str(self.tape_update.get("validation_status", ""))
        return status in {"thin_tape_update", "stale_tape", "empty_prediction_tape"}

    @property
    def zero_new_tape_update(self) -> bool:
        if not self.thin_tape_update:
            return False
        try:
            return int(self.tape_update.get("new_prediction_rows_since_previous", 0) or 0) <= 0
        except (TypeError, ValueError):
            return False


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _recent_run_dirs(runs_root: Path) -> list[Path]:
    if not runs_root.exists():
        return []
    candidates = [path.parent for path in runs_root.glob("*/README.md") if path.parent.is_dir()]
    return sorted(candidates, key=lambda path: path.name)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_blocked_no_daily_prices(readiness: dict[str, Any]) -> bool:
    if not isinstance(readiness, dict):
        return False
    return (
        str(readiness.get("status", "")) == "blocked_no_daily_prices"
        and _safe_int(readiness.get("priced_signal_ticker_count")) == 0
        and _safe_int(readiness.get("missing_signal_ticker_count")) > 0
    )


def _readiness_next_ticker(readiness: dict[str, Any]) -> str:
    latest_missing = readiness.get("latest_missing_tickers") if isinstance(readiness, dict) else None
    if isinstance(latest_missing, list) and latest_missing:
        return normalize_ticker(str(latest_missing[0]))
    top_missing = readiness.get("missing_tickers_top10") if isinstance(readiness, dict) else None
    if isinstance(top_missing, list):
        for row in top_missing:
            if isinstance(row, dict) and row.get("ticker"):
                return normalize_ticker(str(row["ticker"]))
    return ""


def build_price_readiness_stall_report(
    runs_root: Path = RUNS_ROOT,
    pending_run_dir: Path | None = None,
    pending_price_readiness: dict[str, Any] | None = None,
    min_blocked_runs: int = PRICE_READINESS_STALLED_MIN_RUNS,
) -> dict[str, Any]:
    """Report whether local daily_prices readiness has stalled across cycles."""
    entries: list[dict[str, Any]] = []
    for run_dir in _recent_run_dirs(runs_root):
        if pending_run_dir is not None and run_dir.resolve() == pending_run_dir.resolve():
            continue
        readiness = _read_json(run_dir / "price_readiness.json")
        if readiness:
            entries.append({"run_dir": str(run_dir), "run_id": run_dir.name, "readiness": readiness})

    if pending_run_dir is not None and pending_price_readiness is not None:
        entries.append(
            {
                "run_dir": str(pending_run_dir),
                "run_id": pending_run_dir.name,
                "readiness": pending_price_readiness,
            }
        )

    if not entries:
        return {
            "status": "no_price_readiness_history",
            "consecutive_blocked_runs": 0,
            "minimum_blocked_runs": min_blocked_runs,
            "blocked_run_ids": [],
            "next_ticker": "",
            "rule": (
                "After repeated blocked_no_daily_prices cycles, stop adding leaderboard branches "
                "and prioritize local validated daily_prices backfill or tooling."
            ),
            "next_action": "Run a heuristic cycle after local prediction and price artifacts exist.",
        }

    blocked_streak: list[dict[str, Any]] = []
    for entry in reversed(entries):
        if not _is_blocked_no_daily_prices(entry["readiness"]):
            break
        blocked_streak.append(entry)

    latest_readiness = entries[-1]["readiness"]
    next_ticker = _readiness_next_ticker(latest_readiness)
    same_next_ticker_runs = 0
    if next_ticker:
        for entry in blocked_streak:
            if _readiness_next_ticker(entry["readiness"]) != next_ticker:
                break
            same_next_ticker_runs += 1

    blocked_count = len(blocked_streak)
    status = "not_stalled"
    if blocked_count:
        status = "stalled_no_daily_prices" if blocked_count >= min_blocked_runs else "blocked_no_daily_prices"
    blocked_run_ids = [entry["run_id"] for entry in reversed(blocked_streak)]
    return {
        "status": status,
        "consecutive_blocked_runs": blocked_count,
        "minimum_blocked_runs": min_blocked_runs,
        "blocked_run_ids": blocked_run_ids,
        "lookback_runs": len(entries),
        "first_blocked_run": blocked_run_ids[0] if blocked_run_ids else "",
        "latest_blocked_run": blocked_run_ids[-1] if blocked_run_ids else "",
        "next_ticker": next_ticker,
        "same_next_ticker_runs": same_next_ticker_runs,
        "rule": (
            "If daily_prices remains empty for repeated cycles, do not add new leaderboard branches "
            "or widen exposure; focus on validated local daily_prices backfill/tooling."
        ),
        "next_action": (
            f"Backfill independently validated local daily_prices for {next_ticker}, then rerun the cycle."
            if next_ticker
            else "Backfill independently validated local daily_prices for the priority queue, then rerun the cycle."
        ),
    }


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _coverage_pct(value: Any) -> str:
    try:
        return f"{float(value):.1%}"
    except (TypeError, ValueError):
        return "N/A"


def format_price_coverage_note(context: HeuristicRiskContext) -> str:
    coverage = context.price_coverage or {}
    if not isinstance(coverage, dict) or not coverage:
        return ""
    status = coverage.get("status", "unknown")
    independent = _coverage_pct(coverage.get("independent_price_row_ratio"))
    missing_slots = _coverage_pct(coverage.get("missing_position_price_slot_ratio"))
    missing_days = _coverage_pct(coverage.get("missing_price_day_ratio"))
    return (
        f"{status}: daily_prices覆盖{independent}, "
        f"持仓缺价槽位{missing_slots}, 缺价交易日{missing_days}"
    )


def format_tape_update_note(context: HeuristicRiskContext) -> str:
    tape = context.tape_update or {}
    if not isinstance(tape, dict) or not tape:
        return ""
    status = tape.get("validation_status", "unknown")
    new_rows = tape.get("new_prediction_rows_since_previous")
    new_rows_text = "unknown" if new_rows is None else str(new_rows)
    zero_new_text = "，零新增样本" if context.zero_new_tape_update else ""
    latest_date = tape.get("current_latest_prediction_date", "unknown")
    latest_rows = tape.get("latest_date_prediction_rows", "unknown")
    age_days = tape.get("latest_prediction_age_days", "unknown")
    return (
        f"{status}: 较上轮新增{new_rows_text}行{zero_new_text}, "
        f"最新日{latest_date}有{latest_rows}行, 最新样本年龄{age_days}天"
    )


def format_price_readiness_note(context: HeuristicRiskContext) -> str:
    """Summarize the local backfill task required before trusting daily prices."""
    readiness = context.price_readiness or {}
    if not isinstance(readiness, dict) or not readiness:
        return ""

    status = str(readiness.get("status", "unknown"))
    missing = readiness.get("missing_signal_ticker_count")
    total = readiness.get("total_signal_ticker_count")
    latest_missing = readiness.get("latest_missing_tickers") or []
    if isinstance(latest_missing, list):
        latest_text = ", ".join(str(ticker) for ticker in latest_missing[:5])
    else:
        latest_text = ""

    parts = [status]
    if missing is not None and total is not None:
        parts.append(f"缺少{int(missing)}/{int(total)}个signal ticker的daily_prices")
    min_rows = readiness.get("minimum_next_rows")
    if min_rows is not None:
        min_rows_int = int(min_rows)
        if min_rows_int > 0:
            parts.append(f"下一步至少补齐{min_rows_int}行最新本地日线")
        else:
            parts.append("最新日缺价已清零")
    if latest_text:
        parts.append(f"最新缺价ticker={latest_text}")
    next_action = str(readiness.get("next_action", "") or "")
    if next_action:
        parts.append(next_action)
    return "；".join(parts)


def format_price_readiness_backfill_queue(
    context: HeuristicRiskContext,
    limit: int = 5,
) -> str:
    """Return a prioritized local daily_prices backfill queue.

    ``last_signal_date`` in price_readiness means the last signal date still
    missing independent daily_prices coverage, not the latest local price date.
    The rendered labels keep that distinction explicit for user entries and
    agent prompts.
    """
    readiness = context.price_readiness or {}
    if not isinstance(readiness, dict):
        return ""

    top_missing = readiness.get("missing_tickers_top10") or []
    if not isinstance(top_missing, list):
        return ""

    parts: list[str] = []
    for row in top_missing:
        if not isinstance(row, dict) or not row.get("ticker"):
            continue
        ticker = normalize_ticker(str(row["ticker"]))
        details: list[str] = []
        signal_days = row.get("signal_days")
        observations = row.get("total_signal_observations")
        first_signal = str(row.get("first_signal_date", "") or "")[:10]
        last_signal = str(row.get("last_signal_date", "") or "")[:10]
        try:
            if signal_days is not None:
                details.append(f"missing_days={int(signal_days)}d")
        except (TypeError, ValueError):
            pass
        try:
            if observations is not None:
                details.append(f"obs={int(observations)}")
        except (TypeError, ValueError):
            pass
        if first_signal and last_signal:
            details.append(f"missing_range={first_signal}..{last_signal}")
        elif first_signal:
            details.append(f"missing_from={first_signal}")
        elif last_signal:
            details.append(f"missing_to={last_signal}")
        parts.append(f"{ticker}({', '.join(details)})" if details else ticker)
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def format_price_readiness_backfill_plan(context: HeuristicRiskContext) -> str:
    """Return the latest machine-readable local daily_prices backfill plan."""
    readiness = context.price_readiness or {}
    if not isinstance(readiness, dict):
        return ""

    plan = readiness.get("backfill_plan")
    plan_path = str(readiness.get("backfill_plan_path", "") or "")
    if not isinstance(plan, dict) and not plan_path:
        return ""

    top = []
    if isinstance(plan, dict):
        raw_top = plan.get("top_priority_tickers", [])
        if isinstance(raw_top, list):
            top = [normalize_ticker(str(ticker)) for ticker in raw_top if ticker]
        total = _safe_int(plan.get("total_missing_tickers"))
        min_rows = _safe_int(plan.get("minimum_next_rows"))
    else:
        total = 0
        min_rows = 0

    details = []
    if plan_path:
        details.append(f"plan={plan_path}")
    if top:
        details.append(f"top={', '.join(top[:5])}")
    if total:
        details.append(f"missing_tickers={total}")
    if min_rows:
        details.append(f"latest_rows_to_unblock={min_rows}")
    return "；".join(details)


def price_readiness_missing_tickers(context: HeuristicRiskContext) -> set[str]:
    """Return tickers that still need independent local daily_prices rows."""
    readiness = context.price_readiness or {}
    if not isinstance(readiness, dict):
        return set()

    tickers: set[str] = set()
    latest_missing = readiness.get("latest_missing_tickers") or []
    if isinstance(latest_missing, list):
        tickers.update(normalize_ticker(str(ticker)) for ticker in latest_missing if ticker)

    top_missing = readiness.get("missing_tickers_top10") or []
    if isinstance(top_missing, list):
        for row in top_missing:
            if isinstance(row, dict) and row.get("ticker"):
                tickers.add(normalize_ticker(str(row["ticker"])))
    return {ticker for ticker in tickers if ticker}


def price_readiness_position_cap(
    context: HeuristicRiskContext,
    ticker: str | None = None,
) -> float | None:
    """Return a conservative cap while independent daily_prices are not ready."""
    readiness = context.price_readiness or {}
    if not isinstance(readiness, dict) or not readiness:
        return None

    status = str(readiness.get("status", ""))
    if status == "blocked_no_daily_prices":
        return context.max_position * PRICE_READINESS_BLOCKED_POSITION_SCALE
    if status != "partial_daily_price_backfill_needed":
        return None

    if ticker is None:
        return None
    if normalize_ticker(ticker) in price_readiness_missing_tickers(context):
        return context.max_position * PRICE_READINESS_PARTIAL_POSITION_SCALE
    return None


def format_price_readiness_stall_note(context: HeuristicRiskContext) -> str:
    """Summarize repeated local daily_prices backfill stalls across cycles."""
    stall = context.price_readiness_stall or {}
    if not isinstance(stall, dict) or not stall:
        return ""

    status = str(stall.get("status", "unknown"))
    blocked_runs = _safe_int(stall.get("consecutive_blocked_runs"))
    min_runs = _safe_int(stall.get("minimum_blocked_runs"), PRICE_READINESS_STALLED_MIN_RUNS)
    if blocked_runs <= 0:
        return ""

    parts = [status, f"连续{blocked_runs}/{min_runs}轮daily_prices为0且补齐阻塞"]
    next_ticker = str(stall.get("next_ticker", "") or "")
    if next_ticker:
        same_next = _safe_int(stall.get("same_next_ticker_runs"))
        repeat_text = f"，同一下一步ticker连续{same_next}轮" if same_next > 1 else ""
        parts.append(f"下一步ticker={next_ticker}{repeat_text}")
    first_run = str(stall.get("first_blocked_run", "") or "")
    latest_run = str(stall.get("latest_blocked_run", "") or "")
    if first_run and latest_run:
        parts.append(f"阻塞run={first_run}..{latest_run}")
    next_action = str(stall.get("next_action", "") or "")
    if next_action:
        parts.append(next_action)
    return "；".join(parts)


def price_readiness_stall_position_cap(context: HeuristicRiskContext) -> float | None:
    """Return an extra-small cap when daily_prices readiness is repeatedly stuck."""
    stall = context.price_readiness_stall or {}
    if not isinstance(stall, dict):
        return None
    if str(stall.get("status", "")) != "stalled_no_daily_prices":
        return None
    return context.max_position * PRICE_READINESS_STALLED_POSITION_SCALE


def format_evaluator_health_note(context: HeuristicRiskContext) -> str:
    """Summarize whether the fallback evaluator was cross-checked locally."""
    health = context.evaluator_health or {}
    if not isinstance(health, dict) or not health:
        return ""

    status = str(health.get("validation_status", "unknown"))
    baseline = health.get("baseline_engine", context.evaluation_engine or "baseline")
    validation = health.get("validation_engine", "validation")
    score_diff = health.get("score_abs_diff")
    primary_score = health.get("validation_score")
    fallback_score = health.get("baseline_score")
    threshold = health.get("score_tolerance")

    parts = [f"{status}: {baseline} vs {validation}"]
    if score_diff is not None:
        parts.append(f"score差={float(score_diff):.6g}")
    if fallback_score is not None and primary_score is not None:
        parts.append(f"baseline={float(fallback_score):.6f}, validation={float(primary_score):.6f}")
    if threshold is not None:
        parts.append(f"容忍={float(threshold):.6g}")
    warning = str(health.get("environment_warning", "") or "")
    if warning:
        parts.append(warning)
    return "；".join(parts)


def weak_price_coverage_position_cap(context: HeuristicRiskContext) -> float | None:
    """Return the conservative simulated-position cap when local prices are weak."""
    if not context.weak_price_coverage:
        return None
    coverage = context.price_coverage if isinstance(context.price_coverage, dict) else {}
    try:
        independent_ratio = float(coverage.get("independent_price_row_ratio", 0.0) or 0.0)
    except (TypeError, ValueError):
        independent_ratio = 0.0
    try:
        missing_slot_ratio = float(coverage.get("missing_position_price_slot_ratio", 0.0) or 0.0)
    except (TypeError, ValueError):
        missing_slot_ratio = 1.0

    if context.price_source_unvalidated or independent_ratio <= 0.0:
        scale = UNVALIDATED_PRICE_COVERAGE_POSITION_SCALE
    elif independent_ratio < 0.75 or missing_slot_ratio > 0.25:
        scale = PARTIAL_PRICE_COVERAGE_POSITION_SCALE
    else:
        scale = WEAK_PRICE_COVERAGE_POSITION_SCALE
    return context.max_position * scale


def thin_tape_update_position_cap(context: HeuristicRiskContext) -> float | None:
    """Return an observational cap when the latest cycle has too little fresh tape."""
    if not context.thin_tape_update:
        return None
    scale = ZERO_NEW_TAPE_UPDATE_POSITION_SCALE if context.zero_new_tape_update else THIN_TAPE_UPDATE_POSITION_SCALE
    return context.max_position * scale


def insufficient_signal_position_cap(
    context: HeuristicRiskContext,
    signal_count: int | None,
) -> float | None:
    """Return a small-position cap when the latest policy needs more local evidence."""
    if context.min_signal_count <= 1 or signal_count is None:
        return None
    if signal_count >= context.min_signal_count:
        return None
    return context.max_position * INSUFFICIENT_SIGNAL_POSITION_SCALE


def gross_exposure_position_cap(
    context: HeuristicRiskContext,
    current_position: float | None,
    current_gross_exposure: float | None,
) -> float | None:
    """Return the largest target position that keeps simulated gross under max_gross."""
    if context.max_gross is None or current_position is None or current_gross_exposure is None:
        return None
    other_gross = max(0.0, float(current_gross_exposure) - max(float(current_position), 0.0))
    return max(0.0, float(context.max_gross) - other_gross)


def recent_prediction_observation_count(
    ticker: str,
    db_path: Path = DEFAULT_DB_PATH,
    max_age_days: int = 2,
    now: datetime | None = None,
) -> int:
    """Count same-day local prediction observations for a ticker on its latest fresh date."""
    normalized = normalize_ticker(ticker)
    if not normalized or not db_path.exists():
        return 0

    try:
        with sqlite3.connect(db_path) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='price_predictions'"
            ).fetchone()
            if not exists:
                return 0
            rows = conn.execute(
                """
                SELECT predicted_at
                FROM price_predictions
                WHERE predicted_at IS NOT NULL
                  AND (ticker = ? OR ticker LIKE ?)
                ORDER BY datetime(predicted_at) DESC
                LIMIT 200
                """,
                (normalized, f"{normalized}.%"),
            ).fetchall()
    except Exception:
        return 0

    timestamps = [
        parsed
        for (predicted_at,) in rows
        if (parsed := _parse_datetime(predicted_at)) is not None
    ]
    if not timestamps:
        return 0

    latest_date = max(ts.date() for ts in timestamps)
    current = now or datetime.now()
    if (current.date() - latest_date).days > max_age_days:
        return 0
    return sum(1 for ts in timestamps if ts.date() == latest_date)


def derive_simulation_risk_memory(
    trade_rows: list[Any],
    loss_threshold: float = SIMULATION_RISK_LOSS_THRESHOLD,
    memory_days: int = SIMULATION_RISK_MEMORY_DAYS,
) -> list[dict[str, Any]]:
    """Replay local simulated trades and extract no-lookahead closed-loss memory."""
    lots: dict[str, list[dict[str, float]]] = {}
    failures: dict[str, dict[str, Any]] = {}

    for row in trade_rows:
        ticker = normalize_ticker(str(_row_value(row, "ticker", "")))
        if not ticker:
            continue
        direction = str(_row_value(row, "direction", "") or "").lower()
        try:
            shares = int(float(_row_value(row, "shares", 0) or 0))
            price = float(_row_value(row, "price", 0.0) or 0.0)
            fee = float(_row_value(row, "fee", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if shares <= 0 or price <= 0:
            continue

        if direction == "buy":
            cost_per_share = (shares * price + fee) / shares
            lots.setdefault(ticker, []).append({"shares": float(shares), "cost_per_share": cost_per_share})
            continue

        if direction != "sell":
            continue

        remaining = shares
        matched = 0
        cost_basis = 0.0
        ticker_lots = lots.setdefault(ticker, [])
        while remaining > 0 and ticker_lots:
            lot = ticker_lots[0]
            take = min(remaining, int(lot["shares"]))
            cost_basis += take * lot["cost_per_share"]
            lot["shares"] -= take
            remaining -= take
            matched += take
            if lot["shares"] <= 0:
                ticker_lots.pop(0)

        if matched <= 0 or cost_basis <= 0:
            continue

        fee_for_matched = fee * (matched / shares)
        proceeds = matched * price - fee_for_matched
        pnl_pct = (proceeds - cost_basis) / cost_basis
        if pnl_pct > loss_threshold:
            continue

        traded_at = str(_row_value(row, "traded_at", "") or "")
        traded_dt = _parse_datetime(traded_at) or datetime.now()
        previous = failures.get(ticker)
        failure_count = int(previous.get("failure_count", 0)) + 1 if previous else 1
        worst_loss = min(float(previous.get("worst_loss_pct", pnl_pct)), pnl_pct) if previous else pnl_pct
        failures[ticker] = {
            "ticker": ticker,
            "source": "closed_simulation_trade",
            "failure_count": failure_count,
            "last_loss_pct": float(pnl_pct),
            "worst_loss_pct": float(worst_loss),
            "last_trade_id": _row_value(row, "id"),
            "last_updated": traded_dt.isoformat(),
            "expires_at": (traded_dt + timedelta(days=memory_days)).isoformat(),
            "reason": (
                f"closed simulated sell realized {pnl_pct:.2%}, "
                f"below {loss_threshold:.2%} risk-memory threshold"
            ),
        }

    return sorted(failures.values(), key=lambda item: item["ticker"])


def ensure_simulation_risk_memory_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS simulation_risk_memory (
            ticker TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            failure_count INTEGER NOT NULL,
            last_loss_pct REAL NOT NULL,
            worst_loss_pct REAL NOT NULL,
            last_trade_id INTEGER,
            last_updated TEXT NOT NULL,
            expires_at TEXT,
            reason TEXT
        )
        """
    )


def sync_simulation_risk_memory_sqlite(
    conn: sqlite3.Connection,
    loss_threshold: float = SIMULATION_RISK_LOSS_THRESHOLD,
    memory_days: int = SIMULATION_RISK_MEMORY_DAYS,
) -> list[dict[str, Any]]:
    """Refresh durable risk memory from local simulated closed trades."""
    ensure_simulation_risk_memory_table(conn)
    has_trades = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='simulation_trades'"
    ).fetchone()
    if not has_trades:
        conn.commit()
        return []

    rows = conn.execute(
        """
        SELECT id, ticker, direction, shares, price, fee, reason, traded_at
        FROM simulation_trades
        ORDER BY datetime(traded_at), id
        """
    ).fetchall()
    failures = derive_simulation_risk_memory(
        list(rows),
        loss_threshold=loss_threshold,
        memory_days=memory_days,
    )
    for failure in failures:
        conn.execute(
            """
            INSERT INTO simulation_risk_memory (
                ticker, source, failure_count, last_loss_pct, worst_loss_pct,
                last_trade_id, last_updated, expires_at, reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                source = excluded.source,
                failure_count = excluded.failure_count,
                last_loss_pct = excluded.last_loss_pct,
                worst_loss_pct = excluded.worst_loss_pct,
                last_trade_id = excluded.last_trade_id,
                last_updated = excluded.last_updated,
                expires_at = excluded.expires_at,
                reason = excluded.reason
            """,
            (
                failure["ticker"],
                failure["source"],
                failure["failure_count"],
                failure["last_loss_pct"],
                failure["worst_loss_pct"],
                failure["last_trade_id"],
                failure["last_updated"],
                failure["expires_at"],
                failure["reason"],
            ),
        )
    conn.commit()
    return failures


def load_active_simulation_risk_memory(
    db_path: Path = DEFAULT_DB_PATH,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    now_iso = (now or datetime.now()).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='simulation_risk_memory'"
            ).fetchone()
            if not exists:
                return []
            rows = conn.execute(
                """
                SELECT ticker, source, failure_count, last_loss_pct, worst_loss_pct,
                       last_trade_id, last_updated, expires_at, reason
                FROM simulation_risk_memory
                WHERE expires_at IS NULL OR expires_at >= ?
                ORDER BY last_updated DESC, ticker
                """,
                (now_iso,),
            ).fetchall()
            return [dict(row) for row in rows]
    except Exception:
        return []


def latest_heuristic_run(runs_root: Path = RUNS_ROOT) -> Path | None:
    latest = runs_root / "LATEST"
    if latest.exists():
        candidate = Path(latest.read_text(encoding="utf-8").strip())
        if candidate.exists():
            return candidate
    candidates = [path for path in runs_root.glob("*/README.md") if path.parent.is_dir()]
    if not candidates:
        return None
    return sorted(candidates)[-1].parent


def load_latest_heuristic_context(runs_root: Path = RUNS_ROOT) -> HeuristicRiskContext:
    run_dir = latest_heuristic_run(runs_root)
    if run_dir is None:
        return HeuristicRiskContext(None, "", None, 0.10, True, "暂无本地 heuristic cycle 结果", [])

    metrics = _read_json(run_dir / "best_metrics.json")
    checks = _read_json(run_dir / "overfit_checks.json")
    policy_text = (run_dir / "policy_snapshot.py").read_text(encoding="utf-8") if (run_dir / "policy_snapshot.py").exists() else ""
    name_match = re.search(r"'name': '([^']+)'", policy_text)
    max_pos_match = re.search(r"'max_position': ([0-9.]+)", policy_text)
    max_gross_match = re.search(r"'max_gross': ([0-9.]+)", policy_text)
    min_confidence_match = re.search(r"'min_confidence': ([0-9.]+)", policy_text)
    min_risk_reward_match = re.search(r"'min_risk_reward': ([0-9.]+)", policy_text)
    min_holding_days_match = re.search(r"'min_holding_days': ([0-9]+)", policy_text)
    min_signal_count_match = re.search(r"'min_signal_count': ([0-9]+)", policy_text)
    universe_match = re.search(r"'universe': '([^']+)'", policy_text)
    failure_mode_match = re.search(r"'failure_memory_mode': '([^']+)'", policy_text)
    failure_scale_match = re.search(r"'failure_memory_scale': ([0-9.]+)", policy_text)
    policy_name = name_match.group(1) if name_match else run_dir.name
    max_position = float(max_pos_match.group(1)) if max_pos_match else 0.10
    max_gross = float(max_gross_match.group(1)) if max_gross_match else None
    min_confidence = float(min_confidence_match.group(1)) if min_confidence_match else None
    min_risk_reward = float(min_risk_reward_match.group(1)) if min_risk_reward_match else None
    min_holding_days = int(min_holding_days_match.group(1)) if min_holding_days_match else None
    min_signal_count = int(min_signal_count_match.group(1)) if min_signal_count_match else 1
    universe = universe_match.group(1) if universe_match else ""
    failure_memory_mode = failure_mode_match.group(1) if failure_mode_match else "none"
    if failure_memory_mode == "scale" and failure_scale_match:
        failure_ticker_scale = float(failure_scale_match.group(1))
    elif failure_memory_mode == "veto":
        failure_ticker_scale = 0.0
    else:
        failure_ticker_scale = 0.5

    failures: list[dict[str, Any]] = []
    failure_path = run_dir / "failure_cases.jsonl"
    if failure_path.exists():
        for line in failure_path.read_text(encoding="utf-8").splitlines()[-10:]:
            try:
                failures.append(json.loads(line))
            except Exception:
                continue

    overfit_risk = bool(checks.get("overfit_risk", True))
    out_of_sample = checks.get("out_of_sample") if isinstance(checks, dict) else {}
    cost_stress = checks.get("cost_stress_3x_slippage") if isinstance(checks, dict) else {}
    out_of_sample_score = (
        float(out_of_sample["score"])
        if isinstance(out_of_sample, dict) and "score" in out_of_sample
        else None
    )
    cost_stress_score = (
        float(cost_stress["score"])
        if isinstance(cost_stress, dict) and "score" in cost_stress
        else None
    )
    warning = (
        "样本外或成本扰动仍有风险，仅作为风控上限/提示使用"
        if overfit_risk
        else "通过本轮基础样本外与成本扰动检查"
    )
    if cost_stress_score is not None and cost_stress_score < 0.02:
        warning += "；3x滑点成本扰动余量很薄，禁止据此放大仓位"
    project_context = _read_json(run_dir / "project_context.json")
    price_source = str(project_context.get("price_source", ""))
    evaluation_engine = str(project_context.get("evaluation_engine", ""))
    evaluation_warning = str(project_context.get("evaluation_warning", ""))
    price_coverage = _read_json(run_dir / "price_coverage.json") or project_context.get("price_coverage", {})
    tape_update = _read_json(run_dir / "tape_update.json") or project_context.get("tape_update", {})
    price_readiness = _read_json(run_dir / "price_readiness.json") or project_context.get("price_readiness", {})
    price_readiness_stall = (
        _read_json(run_dir / "price_readiness_stall.json")
        or project_context.get("price_readiness_stall", {})
        or build_price_readiness_stall_report()
    )
    evaluator_health = _read_json(run_dir / "evaluator_health.json") or project_context.get("evaluator_health", {})
    if evaluation_warning:
        warning += f"；评估引擎提示: {evaluation_warning}"
    if isinstance(evaluator_health, dict) and evaluator_health:
        health_status = str(evaluator_health.get("validation_status", "unknown"))
        score_abs_diff = evaluator_health.get("score_abs_diff")
        if health_status == "matched":
            warning += "；主评估器已本地复核fallback结果"
        elif score_abs_diff is not None:
            warning += f"；评估器复核未通过，score差={float(score_abs_diff):.6g}"
        else:
            warning += "；评估器复核状态不明"
    if "prediction current_price fallback" in price_source:
        warning += "；daily_prices缺失，收益评估仍依赖预测current_price fallback"
    if isinstance(price_coverage, dict) and price_coverage:
        coverage_context = HeuristicRiskContext(
            run_dir=run_dir,
            policy_name=policy_name,
            score=None,
            max_position=max_position,
            overfit_risk=overfit_risk,
            warning="",
            failure_cases=[],
            min_confidence=min_confidence,
            min_risk_reward=min_risk_reward,
            min_holding_days=min_holding_days,
            max_gross=max_gross,
            universe=universe,
            price_source=price_source,
            price_coverage=price_coverage,
            tape_update=tape_update if isinstance(tape_update, dict) else {},
        )
        coverage_note = format_price_coverage_note(coverage_context)
        if coverage_note:
            coverage_cap = weak_price_coverage_position_cap(coverage_context)
            if coverage_cap is not None:
                warning += f"；价格覆盖{coverage_note}，模拟买入上限降至{coverage_cap:.1%}且禁止据此扩仓"
            else:
                warning += f"；价格覆盖{coverage_note}"
    if isinstance(tape_update, dict) and tape_update:
        tape_context = HeuristicRiskContext(
            run_dir=run_dir,
            policy_name=policy_name,
            score=None,
            max_position=max_position,
            overfit_risk=overfit_risk,
            warning="",
            failure_cases=[],
            min_confidence=min_confidence,
            min_risk_reward=min_risk_reward,
            min_holding_days=min_holding_days,
            max_gross=max_gross,
            universe=universe,
            tape_update=tape_update,
        )
        tape_note = format_tape_update_note(tape_context)
        tape_cap = thin_tape_update_position_cap(tape_context)
        if tape_note:
            if tape_cap is not None:
                warning += f"；本轮tape验证{tape_note}，模拟买入上限降至{tape_cap:.1%}"
            else:
                warning += f"；本轮tape验证{tape_note}"
    if isinstance(price_readiness, dict) and price_readiness:
        readiness_context = HeuristicRiskContext(
            run_dir=run_dir,
            policy_name=policy_name,
            score=None,
            max_position=max_position,
            overfit_risk=overfit_risk,
            warning="",
            failure_cases=[],
            price_readiness=price_readiness,
        )
        readiness_note = format_price_readiness_note(readiness_context)
        if readiness_note:
            warning += f"；daily_prices补齐状态: {readiness_note}"
    if isinstance(price_readiness_stall, dict) and price_readiness_stall:
        stall_context = HeuristicRiskContext(
            run_dir=run_dir,
            policy_name=policy_name,
            score=None,
            max_position=max_position,
            overfit_risk=overfit_risk,
            warning="",
            failure_cases=[],
            price_readiness_stall=price_readiness_stall,
        )
        stall_note = format_price_readiness_stall_note(stall_context)
        stall_cap = price_readiness_stall_position_cap(stall_context)
        if stall_note:
            warning += f"；daily_prices连续阻塞: {stall_note}"
            if stall_cap is not None:
                warning += f"，模拟买入上限降至{stall_cap:.2%}"
    if min_signal_count > 1:
        warning += f"；latest policy要求至少{min_signal_count}条本地同日预测观察，单条孤证不得扩仓"
    return HeuristicRiskContext(
        run_dir=run_dir,
        policy_name=policy_name,
        score=float(metrics["score"]) if "score" in metrics else None,
        max_position=max_position,
        min_signal_count=min_signal_count,
        min_confidence=min_confidence,
        min_risk_reward=min_risk_reward,
        min_holding_days=min_holding_days,
        max_gross=max_gross,
        universe=universe,
        overfit_risk=overfit_risk,
        warning=warning,
        failure_cases=failures,
        failure_ticker_scale=failure_ticker_scale,
        simulation_failures=load_active_simulation_risk_memory(),
        out_of_sample_score=out_of_sample_score,
        cost_stress_score=cost_stress_score,
        sleeve_diagnostics=_read_json(run_dir / "sleeve_diagnostics.json"),
        price_source=price_source,
        price_coverage=price_coverage if isinstance(price_coverage, dict) else {},
        tape_update=tape_update if isinstance(tape_update, dict) else {},
        price_readiness=price_readiness if isinstance(price_readiness, dict) else {},
        price_readiness_stall=price_readiness_stall if isinstance(price_readiness_stall, dict) else {},
        evaluation_engine=evaluation_engine,
        evaluation_warning=evaluation_warning,
        evaluator_health=evaluator_health if isinstance(evaluator_health, dict) else {},
    )


def apply_heuristic_risk_cap(
    ticker: str,
    target_position: float,
    confidence: float | None = None,
    signal_count: int | None = None,
    current_position: float | None = None,
    current_gross_exposure: float | None = None,
    context: HeuristicRiskContext | None = None,
) -> tuple[float, str | None]:
    """Cap a proposed simulation position using the latest robust local policy."""
    ctx = context or load_latest_heuristic_context()
    if not ctx.available:
        return target_position, None

    capped = min(max(target_position, 0.0), ctx.max_position)
    sleeve_reason = sleeve_constraint_reason(ctx, ticker)
    if sleeve_reason:
        capped = min(capped, ctx.max_position * 0.5)
    price_coverage_cap = weak_price_coverage_position_cap(ctx)
    if price_coverage_cap is not None:
        capped = min(capped, price_coverage_cap)
    readiness_cap = price_readiness_position_cap(ctx, ticker)
    if readiness_cap is not None:
        capped = min(capped, readiness_cap)
    readiness_stall_cap = price_readiness_stall_position_cap(ctx)
    if readiness_stall_cap is not None:
        capped = min(capped, readiness_stall_cap)
    tape_update_cap = thin_tape_update_position_cap(ctx)
    if tape_update_cap is not None:
        capped = min(capped, tape_update_cap)
    signal_count_cap = insufficient_signal_position_cap(ctx, signal_count)
    if signal_count_cap is not None:
        capped = min(capped, signal_count_cap)
    gross_cap = gross_exposure_position_cap(ctx, current_position, current_gross_exposure)
    if gross_cap is not None:
        capped = min(capped, gross_cap)
    failure_tickers = recent_failure_tickers(ctx)
    simulation_memory = simulation_memory_tickers(ctx)
    failure_cases = failure_case_tickers(ctx)
    if normalize_ticker(ticker) in failure_tickers:
        capped = min(capped, ctx.max_position * ctx.failure_ticker_scale)
    if confidence is not None and confidence < 0.4:
        capped = min(capped, ctx.max_position * 0.3)
    elif confidence is not None and confidence < 0.6:
        capped = min(capped, ctx.max_position * 0.5)

    if capped < target_position:
        reason = f"heuristic风控将{ticker}目标仓位从{target_position:.1%}限制到{capped:.1%}"
        normalized = normalize_ticker(ticker)
        coverage_note = format_price_coverage_note(ctx)
        readiness_note = format_price_readiness_note(ctx)
        readiness_stall_note = format_price_readiness_stall_note(ctx)
        tape_note = format_tape_update_note(ctx)
        if normalized in simulation_memory:
            reason += "；该标的在模拟账户近期已实现亏损风险记忆中"
        if normalized in failure_cases:
            reason += "；该标的出现在最近failure case中"
        if sleeve_reason:
            reason += f"；{sleeve_reason}"
        if ctx.overfit_risk:
            reason += "；latest policy存在样本外风险，未放大仓位"
        if ctx.thin_cost_stress_margin:
            reason += "；3x滑点成本扰动余量很薄"
        if ctx.price_source_unvalidated:
            reason += "；daily_prices缺失，收益评估依赖预测current_price fallback，禁止放大仓位"
        if coverage_note:
            reason += f"；价格覆盖{coverage_note}"
            if price_coverage_cap is not None:
                reason += f"，弱覆盖模拟买入上限{price_coverage_cap:.1%}"
        if readiness_note:
            reason += f"；daily_prices补齐{readiness_note}"
            if readiness_cap is not None:
                reason += f"，补齐前模拟买入上限{readiness_cap:.1%}"
        if readiness_stall_note:
            reason += f"；daily_prices连续阻塞{readiness_stall_note}"
            if readiness_stall_cap is not None:
                reason += f"，数据补齐未推进仓位上限{readiness_stall_cap:.2%}"
        if tape_note:
            reason += f"；本地tape验证{tape_note}"
            if tape_update_cap is not None:
                reason += f"，薄样本验证模拟买入上限{tape_update_cap:.1%}"
        if signal_count_cap is not None:
            observed = 0 if signal_count is None else signal_count
            reason += (
                f"；本地同日预测观察{observed}/{ctx.min_signal_count}不足，"
                f"孤证仓位上限{signal_count_cap:.1%}"
            )
        elif ctx.min_signal_count > 1:
            reason += f"；latest policy要求至少{ctx.min_signal_count}条本地同日预测观察支持"
        if gross_cap is not None and gross_cap < target_position:
            reason += f"；组合总模拟仓位上限{ctx.max_gross:.1%}，该标的当前可用仓位{gross_cap:.1%}"
        return capped, reason
    if sleeve_reason:
        return capped, sleeve_reason
    risk_notes: list[str] = []
    if ctx.overfit_risk:
        risk_notes.append("latest heuristic policy仅作风险提示，未提高默认仓位")
    if ctx.thin_cost_stress_margin:
        risk_notes.append("latest heuristic policy通过基础检查但3x滑点成本扰动余量很薄，禁止放大仓位")
    if ctx.price_source_unvalidated:
        risk_notes.append("latest heuristic policy通过基础检查但daily_prices缺失，仅作本地风控约束，禁止放大仓位")
    coverage_note = format_price_coverage_note(ctx)
    if coverage_note:
        if price_coverage_cap is not None:
            risk_notes.append(
                f"价格覆盖{coverage_note}，弱覆盖模拟买入上限{price_coverage_cap:.1%}，仅作本地风控约束"
            )
        else:
            risk_notes.append(f"价格覆盖{coverage_note}，仅作本地风控约束")
    readiness_note = format_price_readiness_note(ctx)
    if readiness_note:
        if readiness_cap is not None:
            risk_notes.append(
                f"daily_prices补齐{readiness_note}，补齐前模拟买入上限{readiness_cap:.1%}，不得扩仓"
            )
        else:
            risk_notes.append(f"daily_prices补齐{readiness_note}，仅作本地数据质量约束")
    readiness_stall_note = format_price_readiness_stall_note(ctx)
    if readiness_stall_note:
        if readiness_stall_cap is not None:
            risk_notes.append(
                f"daily_prices连续阻塞{readiness_stall_note}，"
                f"数据补齐未推进仓位上限{readiness_stall_cap:.2%}，停止新增leaderboard分支"
            )
        else:
            risk_notes.append(f"daily_prices连续阻塞{readiness_stall_note}，优先补齐本地价格")
    if signal_count_cap is not None:
        observed = 0 if signal_count is None else signal_count
        risk_notes.append(
            f"本地同日预测观察{observed}/{ctx.min_signal_count}不足，"
            f"孤证仓位上限{signal_count_cap:.1%}，仅允许观察/小仓"
        )
    if ctx.min_signal_count > 1:
        risk_notes.append(
            f"latest policy要求至少{ctx.min_signal_count}条本地同日预测观察支持，单条孤证只允许观察/小仓"
        )
    if gross_cap is not None and gross_cap <= max(0.0, target_position) + 1e-12:
        risk_notes.append(f"组合总模拟仓位上限{ctx.max_gross:.1%}已纳入交易约束")
    tape_note = format_tape_update_note(ctx)
    if tape_note:
        if tape_update_cap is not None:
            risk_notes.append(
                f"本地tape验证{tape_note}，薄样本验证模拟买入上限{tape_update_cap:.1%}，不能作为扩仓验证"
            )
        else:
            risk_notes.append(f"本地tape验证{tape_note}，仅作本地风控约束")
    if risk_notes:
        return capped, "；".join(risk_notes)
    return capped, None


def normalize_ticker(ticker: str) -> str:
    code = str(ticker or "").strip().upper()
    return code.split(".")[0] if "." in code else code


def is_etf_ticker(ticker: str) -> bool:
    code = normalize_ticker(ticker)
    return code.startswith(("15", "51", "56", "58"))


def sleeve_constraint_reason(context: HeuristicRiskContext, ticker: str) -> str | None:
    """Return a conservative sleeve warning for tickers outside robust local sleeves."""
    diagnostics = context.sleeve_diagnostics or {}
    sleeves = diagnostics.get("sleeves") if isinstance(diagnostics, dict) else {}
    if not isinstance(sleeves, dict):
        return None

    sleeve_key = "etf" if is_etf_ticker(ticker) else "single_stock"
    if sleeve_key != "etf":
        return None
    sleeve = sleeves.get(sleeve_key)
    if not isinstance(sleeve, dict):
        return None
    if sleeve.get("promotable"):
        return None

    reason = sleeve.get("reason") or "sleeve未通过本地稳健性检查"
    label = "ETF sleeve" if sleeve_key == "etf" else "单股 sleeve"
    return f"{label}{reason}，仅允许小仓观察/风险提示"


def failure_case_tickers(context: HeuristicRiskContext) -> set[str]:
    tickers: set[str] = set()
    for case in context.failure_cases:
        market_state = case.get("market_state") if isinstance(case, dict) else {}
        signals = case.get("signals") if isinstance(case, dict) else {}
        positions = case.get("positions") if isinstance(case, dict) else {}
        for source in (market_state, signals, positions):
            if isinstance(source, dict):
                ticker = source.get("ticker")
                if ticker:
                    tickers.add(normalize_ticker(str(ticker)))
    return tickers


def simulation_memory_tickers(context: HeuristicRiskContext) -> set[str]:
    return {
        normalize_ticker(str(row.get("ticker")))
        for row in context.simulation_failures
        if isinstance(row, dict) and row.get("ticker")
    }


def recent_failure_tickers(context: HeuristicRiskContext) -> set[str]:
    return failure_case_tickers(context) | simulation_memory_tickers(context)


def failure_ticker_constraints(context: HeuristicRiskContext | None = None) -> list[dict[str, Any]]:
    """Describe exact local-only constraints applied to recent failure tickers."""
    ctx = context or load_latest_heuristic_context()
    if not ctx.available:
        return []

    cap = ctx.max_position * ctx.failure_ticker_scale
    rows: list[dict[str, Any]] = []
    simulation_memory = simulation_memory_tickers(ctx)
    for ticker in sorted(recent_failure_tickers(ctx)):
        matching_cases = [
            case.get("case_type", "case")
            for case in ctx.failure_cases
            if ticker in {
                normalize_ticker(str(value.get("ticker")))
                for value in (
                    case.get("market_state"),
                    case.get("signals"),
                    case.get("positions"),
                )
                if isinstance(value, dict) and value.get("ticker")
            }
            or (
                isinstance(case.get("positions"), dict)
                and ticker in {normalize_ticker(key) for key in case["positions"]}
            )
        ]
        if ticker in simulation_memory:
            matching_cases.append("closed_simulation_trade_loss")
        rows.append(
            {
                "ticker": ticker,
                "max_simulated_position": cap,
                "action": "cap_to_failure_scale_and_require_new_evidence",
                "reason": ", ".join(sorted(set(matching_cases))) or "recent_failure_case",
            }
        )
    return rows


def format_heuristic_prompt_context(context: HeuristicRiskContext | None = None) -> str:
    """Return a compact local-only risk block for research and committee prompts."""
    ctx = context or load_latest_heuristic_context()
    if not ctx.available:
        return ""

    failure_tickers = sorted(recent_failure_tickers(ctx))
    score = "N/A" if ctx.score is None else f"{ctx.score:.6f}"
    lines = [
        "【本地Heuristic风控约束】",
        (
            f"- 最新policy: {ctx.policy_name}; score={score}; "
            f"单标的模拟仓位上限={ctx.max_position:.1%}; "
            f"本地信号观察门槛={ctx.min_signal_count}条"
        ),
        f"- 稳健性: {ctx.warning}",
        "- 用法: 只能作为本地风控约束/解释，不得编造成外部市场事实；禁止因此放大仓位。",
    ]
    if ctx.evaluation_engine:
        engine_line = f"- 评估引擎: {ctx.evaluation_engine}"
        if ctx.evaluation_warning:
            engine_line += f"；{ctx.evaluation_warning}"
        lines.append(engine_line)
    evaluator_health_note = format_evaluator_health_note(ctx)
    if evaluator_health_note:
        lines.append(f"- 评估器复核: {evaluator_health_note}。")
    checklist = format_policy_checklist(ctx)
    if checklist:
        lines.append(checklist)
    sleeve_text = format_sleeve_diagnostics(ctx)
    if sleeve_text:
        lines.append(sleeve_text)
    if ctx.price_source_unvalidated:
        lines.append("- 数据质量: daily_prices 仍为空，本轮评估依赖预测记录里的 current_price fallback；禁止据此扩大仓位。")
    coverage_note = format_price_coverage_note(ctx)
    if coverage_note:
        coverage_cap = weak_price_coverage_position_cap(ctx)
        cap_text = f"；弱覆盖模拟买入上限={coverage_cap:.1%}" if coverage_cap is not None else ""
        lines.append(f"- 价格覆盖: {coverage_note}{cap_text}；只能作为风险约束，不能作为扩仓依据。")
    tape_note = format_tape_update_note(ctx)
    if tape_note:
        tape_cap = thin_tape_update_position_cap(ctx)
        cap_text = f"；薄样本验证模拟买入上限={tape_cap:.1%}" if tape_cap is not None else ""
        lines.append(f"- 本地tape验证: {tape_note}{cap_text}；不能作为扩仓验证。")
    readiness_note = format_price_readiness_note(ctx)
    if readiness_note:
        readiness_cap = price_readiness_position_cap(ctx)
        cap_text = f"；daily_prices阻塞模拟买入上限={readiness_cap:.1%}" if readiness_cap is not None else ""
        lines.append(f"- daily_prices补齐: {readiness_note}{cap_text}；这是本地数据质量任务，不得当作市场事实。")
    readiness_stall_note = format_price_readiness_stall_note(ctx)
    if readiness_stall_note:
        readiness_stall_cap = price_readiness_stall_position_cap(ctx)
        cap_text = (
            f"；连续阻塞模拟买入上限={readiness_stall_cap:.2%}"
            if readiness_stall_cap is not None
            else ""
        )
        lines.append(
            f"- daily_prices连续阻塞: {readiness_stall_note}{cap_text}；"
            "本轮不得新增leaderboard分支或扩大模拟仓位。"
        )
    readiness_queue = format_price_readiness_backfill_queue(ctx)
    if readiness_queue:
        lines.append(f"- daily_prices优先补齐队列: {readiness_queue}；先补这些本地价格再评估是否放松弱覆盖/薄tape仓位。")
    readiness_plan = format_price_readiness_backfill_plan(ctx)
    if readiness_plan:
        lines.append(f"- daily_prices补齐计划: {readiness_plan}；先完成本地计划再新增收益型规则。")
    if ctx.out_of_sample_score is not None or ctx.cost_stress_score is not None:
        oos = "N/A" if ctx.out_of_sample_score is None else f"{ctx.out_of_sample_score:.6f}"
        stress = "N/A" if ctx.cost_stress_score is None else f"{ctx.cost_stress_score:.6f}"
        lines.append(f"- 验证分数: 样本外score={oos}; 3x滑点score={stress}。")
    if ctx.min_signal_count > 1:
        lines.append(
            f"- 入场证据门槛: 至少{ctx.min_signal_count}条本地同日预测观察；不足时不得把该policy解释为扩仓依据。"
        )
    if failure_tickers:
        lines.append(
            f"- 近期failure-case标的: {', '.join(failure_tickers)}；若再次推荐，必须给出新增证据、反证失效条件，并将模拟仓位限制到{ctx.max_position * ctx.failure_ticker_scale:.1%}或观望。"
        )
    if ctx.simulation_failures:
        tickers = ", ".join(row["ticker"] for row in ctx.simulation_failures[:6])
        lines.append(
            f"- 模拟账户已实现亏损风险记忆: {tickers}；该记忆来自本地simulation_trades平仓结果，过期前仅允许减仓/小仓观察。"
        )
    if ctx.failure_cases:
        lines.append("- 近期失败模式:")
        for case in ctx.failure_cases[-3:]:
            lines.append(
                f"  * {case.get('case_type', 'case')} {case.get('time_range', '')}: "
                f"{case.get('suspected_reason', '')[:90]}"
            )
    return "\n".join(lines)


def format_heuristic_status(context: HeuristicRiskContext | None = None) -> str:
    ctx = context or load_latest_heuristic_context()
    if not ctx.available:
        return "\n🧭 Heuristic 学习状态: 暂无本地运行结果\n"

    score = "N/A" if ctx.score is None else f"{ctx.score:.6f}"
    lines = [
        "\n🧭 Heuristic 学习状态",
        "=" * 60,
        f"   最新run: {ctx.run_dir}",
        f"   best policy: {ctx.policy_name} | score: {score}",
        f"   单标的模拟仓位上限: {ctx.max_position:.1%}",
        f"   本地信号观察门槛: >={ctx.min_signal_count} 条同日预测观察",
        f"   风险标记: {ctx.warning}",
    ]
    if ctx.evaluation_engine:
        lines.append(f"   评估引擎: {ctx.evaluation_engine}")
    if ctx.evaluation_warning:
        lines.append(f"   评估提示: {ctx.evaluation_warning}")
    evaluator_health_note = format_evaluator_health_note(ctx)
    if evaluator_health_note:
        lines.append(f"   评估器复核: {evaluator_health_note}")
    checklist = format_policy_checklist(ctx)
    if checklist:
        lines.append(f"   {checklist}")
    if ctx.price_source:
        lines.append(f"   价格数据: {ctx.price_source}")
    if ctx.price_source_unvalidated:
        lines.append("   数据质量风险: daily_prices缺失，收益评估只作本地风控约束，禁止据此扩大仓位")
    coverage_note = format_price_coverage_note(ctx)
    if coverage_note:
        lines.append(f"   价格覆盖: {coverage_note}")
    coverage_cap = weak_price_coverage_position_cap(ctx)
    if coverage_cap is not None:
        lines.append(f"   弱价格覆盖模拟买入上限: {coverage_cap:.1%}")
    tape_note = format_tape_update_note(ctx)
    if tape_note:
        lines.append(f"   本地tape验证: {tape_note}")
    tape_cap = thin_tape_update_position_cap(ctx)
    if tape_cap is not None:
        lines.append(f"   薄样本验证模拟买入上限: {tape_cap:.1%}")
    readiness_note = format_price_readiness_note(ctx)
    if readiness_note:
        lines.append(f"   daily_prices补齐: {readiness_note}")
    readiness_queue = format_price_readiness_backfill_queue(ctx)
    if readiness_queue:
        lines.append(f"   daily_prices优先补齐队列: {readiness_queue}")
    readiness_plan = format_price_readiness_backfill_plan(ctx)
    if readiness_plan:
        lines.append(f"   daily_prices补齐计划: {readiness_plan}")
    readiness_cap = price_readiness_position_cap(ctx)
    if readiness_cap is not None:
        lines.append(f"   daily_prices阻塞模拟买入上限: {readiness_cap:.1%}")
    readiness_stall_note = format_price_readiness_stall_note(ctx)
    if readiness_stall_note:
        lines.append(f"   daily_prices连续阻塞: {readiness_stall_note}")
    readiness_stall_cap = price_readiness_stall_position_cap(ctx)
    if readiness_stall_cap is not None:
        lines.append(f"   daily_prices连续阻塞模拟买入上限: {readiness_stall_cap:.2%}")
    sleeve_text = format_sleeve_diagnostics(ctx)
    if sleeve_text:
        lines.append(f"   {sleeve_text}")
    if ctx.out_of_sample_score is not None or ctx.cost_stress_score is not None:
        oos = "N/A" if ctx.out_of_sample_score is None else f"{ctx.out_of_sample_score:.6f}"
        stress = "N/A" if ctx.cost_stress_score is None else f"{ctx.cost_stress_score:.6f}"
        lines.append(f"   验证分数: 样本外 {oos} | 3x滑点 {stress}")
    if ctx.failure_cases:
        lines.append("   最近 failure cases:")
        for case in ctx.failure_cases[-3:]:
            lines.append(
                f"      - {case.get('case_type', 'case')} {case.get('time_range', '')}: "
                f"{case.get('suspected_reason', '')[:70]}"
            )
    constraints = failure_ticker_constraints(ctx)
    if constraints:
        lines.append("   failure ticker 当前约束:")
        for row in constraints[:8]:
            lines.append(
                f"      - {row['ticker']}: simulated cap {row['max_simulated_position']:.1%}; "
                "需要新增证据/反证失效条件"
            )
    if ctx.simulation_failures:
        lines.append("   模拟账户风险记忆:")
        for row in ctx.simulation_failures[:6]:
            lines.append(
                f"      - {row['ticker']}: last {row['last_loss_pct']:.1%}, "
                f"worst {row['worst_loss_pct']:.1%}, expires {str(row.get('expires_at', ''))[:10]}"
            )
    return "\n".join(lines) + "\n"


def format_policy_checklist(context: HeuristicRiskContext) -> str:
    """Human-readable local policy gates for research prompts and status output."""
    if not context.available:
        return ""

    gates = []
    if context.universe:
        gates.append(f"适用范围={context.universe}")
    if context.min_confidence is not None:
        gates.append(f"置信度>={context.min_confidence:.0%}")
    if context.min_risk_reward is not None:
        gates.append(f"风险收益比>={context.min_risk_reward:.2f}")
    if context.min_holding_days:
        gates.append(f"最短持有>={context.min_holding_days}天")
    if context.max_gross is not None:
        gates.append(f"组合总模拟仓位<={context.max_gross:.0%}")
    if context.min_signal_count > 1:
        gates.append(f"同日观察>={context.min_signal_count}条")
    coverage_cap = weak_price_coverage_position_cap(context)
    if coverage_cap is not None:
        gates.append(f"弱价格覆盖仓位<={coverage_cap:.1%}")
    tape_cap = thin_tape_update_position_cap(context)
    if tape_cap is not None:
        gates.append(f"薄tape验证仓位<={tape_cap:.1%}")
    readiness_cap = price_readiness_position_cap(context)
    if readiness_cap is not None:
        gates.append(f"daily_prices阻塞仓位<={readiness_cap:.1%}")
    readiness_stall_cap = price_readiness_stall_position_cap(context)
    if readiness_stall_cap is not None:
        gates.append(f"daily_prices连续阻塞仓位<={readiness_stall_cap:.2%}")

    if not gates:
        return ""
    return "- Heuristic入场校验: " + "；".join(gates) + "；不满足则降仓或观望。"


def format_sleeve_diagnostics(context: HeuristicRiskContext) -> str:
    diagnostics = context.sleeve_diagnostics or {}
    if not isinstance(diagnostics, dict):
        return ""
    allocator_status = diagnostics.get("allocator_status")
    sleeves = diagnostics.get("sleeves")
    if not allocator_status or not isinstance(sleeves, dict):
        return ""

    parts = [f"sleeve allocator: {allocator_status}"]
    for key in ("etf", "single_stock"):
        row = sleeves.get(key)
        if not isinstance(row, dict):
            continue
        score = row.get("score")
        stress = row.get("cost_stress_score")
        state = "pass" if row.get("promotable") else ("cap/warning" if key == "etf" else "warning")
        parts.append(f"{key} {state} score={float(score):.6f} 3x={float(stress):.6f}")
    return "; ".join(parts)
