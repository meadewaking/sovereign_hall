"""Authoritative performance metrics for the simulated investment account.

Offline backtests are research diagnostics only.  They must never substitute
for, rank, or promote a policy against the realtime-valued simulated account.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable
from zoneinfo import ZoneInfo


PERFORMANCE_STANDARD = "simulation_account_realtime_v1"
PERFORMANCE_FORMULA = (
    "score = realtime_simulation_total_assets / initial_capital - 1; "
    "offline backtest returns are diagnostic-only and cannot substitute"
)

# Status dimensions introduced in PR1.1. Each dimension classifies one
# independent question; the legacy ``health_status`` field is preserved for
# backwards-compatible display only and must not drive new automation. A low
# deployment ratio alone never justifies "需要改策略/修系统".
RESEARCH_STATUSES = ("ready", "source_unavailable", "invalid_output")
EXECUTION_STATUSES = (
    "ready",
    "market_closed",
    "no_approved_intent",
    "quote_unavailable",
    "daily_limit",
)
CAPITAL_STATUSES = ("deployed", "awaiting_candidates", "lot_or_cost_blocked")
EVALUATION_STATUSES = (
    "current",
    "data_blocked",
    "insufficient_samples",
    "stale",
)
PROFIT_STATUSES = ("positive", "negative", "unavailable")


async def classify_simulation_runtime(conn, assets, market, *, max_daily_trades=5, target_ratio=1.0):
    """Classify observed pipeline states; holding cash is not an execution fault."""
    result = {"research_status": "unavailable", "evaluation_status": "insufficient_samples"}
    async with conn.execute("SELECT status,terminal_code,terminal_reason FROM research_rounds "
                            "ORDER BY started_at DESC,id DESC LIMIT 1") as cursor:
        research = await cursor.fetchone()
    if research:
        reason = str(research[2] or "")
        result["research_status"] = ("source_unavailable" if "资料源" in reason or "来源不可用" in reason
                                     else "invalid_output" if research[0] == "failed" else "ready")
    async with conn.execute("SELECT defer_code FROM execution_intents WHERE status IN "
                            "('pending','deferred','executing') ORDER BY priority,created_at LIMIT 1") as cursor:
        pending = await cursor.fetchone()
    if not pending:
        execution = "no_approved_intent"
    elif ((hasattr(market, "is_trading_day") and not await market.is_trading_day()) or
          (hasattr(market, "is_market_open") and not await market.is_market_open())):
        execution = "market_closed"
    elif assets.get("missing_price_tickers") or pending[0] == "realtime_quote_unavailable":
        execution = "quote_unavailable"
    else:
        async with conn.execute("SELECT COUNT(*) FROM simulation_trades WHERE date(traded_at)=?",
                                (datetime.now().date().isoformat(),)) as cursor:
            count = (await cursor.fetchone())[0]
        execution = "daily_limit" if count >= max_daily_trades else "ready"
    result["execution_status"] = execution
    deployed = assets.get("valuation_complete") and float(assets.get("invested_ratio") or 0) >= max(0, target_ratio - .05)
    async with conn.execute("SELECT code FROM simulation_candidate_rejections "
                            "ORDER BY last_seen_at DESC,ticker,code LIMIT 1") as cursor:
        blocker = await cursor.fetchone()
    result["capital_status"] = ("deployed" if deployed else "lot_or_cost_blocked" if blocker and
        blocker[0] in {"cash_lot_infeasible", "lot_size_cap_infeasible", "lot_or_cost_blocked"}
        else "awaiting_candidates")
    async with conn.execute("SELECT value FROM system_stats WHERE key='continuous_evaluation_status'") as cursor:
        status = await cursor.fetchone()
    if status:
        result["evaluation_status"] = json.loads(status[0]).get("evaluation_status", "data_blocked")
    else:
        async with conn.execute("SELECT status,data_cutoff FROM evaluation_runs ORDER BY created_at DESC,id DESC LIMIT 1") as cursor:
            evaluation = await cursor.fetchone()
        if evaluation:
            result["evaluation_status"] = ("data_blocked" if evaluation[0] not in {"available", "not_proven", "qualified"} else
                "stale" if str(evaluation[1])[:10] < datetime.now().date().isoformat() else "insufficient_samples")
    return result


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _maximum_drawdown(values: Iterable[float]) -> float | None:
    peak = None
    worst = 0.0
    seen = False
    for raw in values:
        value = float(raw or 0.0)
        if value <= 0:
            continue
        seen = True
        peak = value if peak is None else max(peak, value)
        if peak:
            worst = min(worst, value / peak - 1.0)
    return worst if seen else None


def build_simulation_performance(
    *,
    initial_capital: float,
    assets: dict[str, Any],
    trade_count: int,
    recorded_fees: float,
    latest_trade_at: str | None,
    snapshot_rows: Iterable[dict[str, Any]] = (),
    trades_since_window_start: int = 0,
    window_start: str | None = None,
    now: datetime | None = None,
    no_trade_failure_days: int = 3,
    minimum_healthy_invested_ratio: float = 0.80,
    # PR1.2 inputs — all optional and default to "no evidence" so callers
    # that have not been upgraded yet keep producing the legacy shape.
    prev_total_assets: float | None = None,
    external_flow: float | None = None,
    candidate_net_return: float | None = None,
    control_net_return: float | None = None,
    research_status: str | None = None,
    execution_status: str | None = None,
    capital_status: str | None = None,
    evaluation_status: str | None = None,
    profit_status: str | None = None,
    correct_count: int | None = None,
    judgeable_total: int | None = None,
    partial_count: int | None = None,
) -> dict[str, Any]:
    """Build the only valid performance score from the simulated account."""
    current_time = now or datetime.now()
    valuation_complete = bool(assets.get("valuation_complete"))
    total_assets = assets.get("total_assets") if valuation_complete else None
    invested_ratio = assets.get("invested_ratio") if valuation_complete else None
    cash = float(assets.get("cash") or 0.0)
    initial = float(initial_capital or 0.0)
    # PR1.2: a missing realtime price must surface as NULL, never as 0% return.
    # ``total_assets`` is None whenever ``valuation_complete`` is False; we
    # keep that contract and do not call ``get(..., 0.0)`` on the score path.
    net_total_return = (
        float(total_assets) / initial - 1.0
        if total_assets is not None and initial > 0
        else None
    )
    durable_cost_paid = max(float(recorded_fees or 0.0), 0.0)
    gross_total_return_before_recorded_cost = (
        (float(total_assets) + durable_cost_paid) / initial - 1.0
        if total_assets is not None and initial > 0
        else None
    )
    latest_trade_dt = _parse_datetime(latest_trade_at)
    days_since_last_trade = (
        max(0, (current_time.date() - latest_trade_dt.date()).days)
        if latest_trade_dt
        else None
    )
    snapshot_list = [dict(row) for row in snapshot_rows]
    max_drawdown = _maximum_drawdown(
        row.get("total_assets") for row in snapshot_list
    )
    snapshot_invested_ratios = [
        max(
            0.0,
            min(
                1.0,
                float(row.get("positions_value") or 0.0)
                / float(row.get("total_assets") or 1.0),
            ),
        )
        for row in snapshot_list
        if float(row.get("total_assets") or 0.0) > 0
    ]
    average_invested_ratio = (
        sum(snapshot_invested_ratios) / len(snapshot_invested_ratios)
        if snapshot_invested_ratios
        else invested_ratio
    )

    failure_reasons: list[str] = []
    if not valuation_complete:
        health_status = "valuation_incomplete"
        failure_reasons.append(
            "模拟账户实时估值不完整，绩效为N/A；禁止用离线回测或旧价补位"
        )
    else:
        low_deployment = float(invested_ratio or 0.0) < float(
            minimum_healthy_invested_ratio
        )
        prolonged_no_fill = (
            days_since_last_trade is None
            or days_since_last_trade >= max(int(no_trade_failure_days), 1)
        )
        if low_deployment and prolonged_no_fill:
            health_status = "system_failure_no_live_deployment"
            failure_reasons.append(
                f"模拟账户投入率仅{float(invested_ratio or 0.0):.1%}，"
                f"距离最近成交{days_since_last_trade if days_since_last_trade is not None else 'N/A'}天"
            )
            failure_reasons.append(
                "离线回测盈利不能抵消真实模拟账户无成交/未部署故障"
            )
        elif low_deployment:
            health_status = "degraded_underdeployed"
            failure_reasons.append(
                f"模拟账户投入率{float(invested_ratio or 0.0):.1%}低于"
                f"{float(minimum_healthy_invested_ratio):.0%}健康线"
            )
        else:
            health_status = "healthy"

    iteration_attribution = (
        "live_fills_observed"
        if int(trades_since_window_start or 0) > 0
        else "no_new_live_fills_no_performance_claim"
    )

    # PR1.2 — split account movements from strategy improvement. Each field is
    # NULL when its evidence is absent; we never substitute 0.0 to make a row
    # look complete. ``external_flow`` in particular must remain NULL unless
    # the caller passes a verified deposit/withdrawal amount.
    prev_assets = (
        float(prev_total_assets) if prev_total_assets is not None else None
    )
    if total_assets is not None and prev_assets is not None:
        account_nav_change = float(total_assets) - prev_assets
    else:
        account_nav_change = None
    if (
        total_assets is not None
        and prev_assets is not None
        and external_flow is not None
    ):
        price_pnl_change = (
            float(total_assets) - prev_assets - float(external_flow)
        )
    else:
        price_pnl_change = None
    cash_paid_cost_change = (
        float(recorded_fees or 0.0) if recorded_fees is not None else None
    )
    if candidate_net_return is not None and control_net_return is not None:
        candidate_minus_control_net_return = (
            float(candidate_net_return) - float(control_net_return)
        )
    else:
        candidate_minus_control_net_return = None

    # PR1.1 — five orthogonal status dimensions. Callers that have not been
    # upgraded pass None; we then default to ``unavailable`` rather than
    # inventing a value, so dashboards can distinguish "not classified" from
    # a real failure code.
    resolved_research_status = (
        research_status
        if research_status in RESEARCH_STATUSES
        else "unavailable"
    )
    resolved_execution_status = (
        execution_status
        if execution_status in EXECUTION_STATUSES
        else "unavailable"
    )
    resolved_capital_status = (
        capital_status
        if capital_status in CAPITAL_STATUSES
        else (
            "deployed"
            if valuation_complete
            and invested_ratio is not None
            and float(invested_ratio) >= float(minimum_healthy_invested_ratio)
            else "unavailable"
        )
    )
    resolved_evaluation_status = (
        evaluation_status
        if evaluation_status in EVALUATION_STATUSES
        else "unavailable"
    )
    if profit_status is not None and profit_status in PROFIT_STATUSES:
        resolved_profit_status = profit_status
    elif net_total_return is None:
        resolved_profit_status = "unavailable"
    else:
        resolved_profit_status = "positive" if net_total_return > 0 else "negative"

    # PR1.2 — strict correct ratio. ``correct / judgeable_total`` only; partial
    # hits are reported on a separate line with half weight, never folded into
    # the same percentage as the strict numerator.
    if correct_count is not None and judgeable_total is not None and int(judgeable_total) > 0:
        strict_correct_ratio = int(correct_count) / int(judgeable_total)
    else:
        strict_correct_ratio = None
    partial_half_weight = (
        0.5 * int(partial_count) if partial_count is not None else None
    )

    session_valued=assets.get('valuation_basis') in {'session_close','session_pause'}
    standard='simulation_account_session_valued_v2' if session_valued else PERFORMANCE_STANDARD
    return {
        "performance_standard": standard,
        "reward_version": standard,
        "reward_formula": ('score = source-validated session account assets / initial_capital - 1; valuation marks cannot execute'
                           if session_valued else PERFORMANCE_FORMULA),
        **{key:assets.get(key) for key in ('valuation_basis','valuation_label','valuation_as_of')},
        "authoritative": True,
        "offline_backtest_promotion_allowed": False,
        "valuation_complete": valuation_complete,
        "initial_capital": initial,
        "total_assets": float(total_assets) if total_assets is not None else None,
        "cash": cash,
        "positions_value": (
            float(assets.get("positions_value") or 0.0)
            if valuation_complete
            else None
        ),
        "net_total_return": net_total_return,
        "total_return": net_total_return,
        "score": net_total_return,
        "gross_total_return_before_recorded_cost": (
            gross_total_return_before_recorded_cost
        ),
        "recorded_cost_ratio": (
            durable_cost_paid / initial if initial > 0 else None
        ),
        "max_drawdown_from_simulation_snapshots": max_drawdown,
        "average_invested_ratio_from_simulation_snapshots": average_invested_ratio,
        "current_invested_ratio": invested_ratio,
        "deployment_gap": assets.get("deployment_gap"),
        "trade_count": int(trade_count or 0),
        "recorded_fees": durable_cost_paid,
        "latest_trade_at": str(latest_trade_at or "") or None,
        "days_since_last_trade": days_since_last_trade,
        "window_start": window_start,
        "trades_since_window_start": int(trades_since_window_start or 0),
        "iteration_attribution": iteration_attribution,
        "iteration_performance_improvement": None,
        "health_status": health_status,
        "health_status_role": "legacy_display_only",
        "research_status": resolved_research_status,
        "execution_status": resolved_execution_status,
        "capital_status": resolved_capital_status,
        "evaluation_status": resolved_evaluation_status,
        "profit_status": resolved_profit_status,
        "account_nav_change": account_nav_change,
        "price_pnl_change": price_pnl_change,
        "cash_paid_cost_change": cash_paid_cost_change,
        "external_flow": external_flow,
        "candidate_minus_control_net_return": candidate_minus_control_net_return,
        "strict_correct_ratio": strict_correct_ratio,
        "strict_correct_count": correct_count,
        "judgeable_total": judgeable_total,
        "partial_count": partial_count,
        "partial_half_weight_total": partial_half_weight,
        "failure_reasons": failure_reasons,
        "missing_price_tickers": list(assets.get("missing_price_tickers") or []),
        "measured_at": current_time.isoformat(),
        "price_rule": assets.get('valuation_rule','realtime quotes only; no local/prediction/cost fallback'),
    }


async def collect_simulation_performance(
    db_path: str | Path,
    *,
    window_start: str | None = None,
) -> dict[str, Any]:
    """Fetch a realtime account valuation and build authoritative metrics."""
    try:
        from sovereign_hall.services.database import DatabaseService
        from sovereign_hall.services.investment_simulation import InvestmentSimulation
    except ModuleNotFoundError:
        # The explicitly named stdlib heuristic runner must remain runnable in
        # the system Python. An empty account is exactly valued from cash; a
        # non-empty account becomes N/A because stdlib mode cannot obtain the
        # controlled realtime quotes and is forbidden to use old prices.
        return _collect_simulation_performance_sqlite_fallback(
            db_path,
            window_start=window_start,
        )

    db = DatabaseService(str(db_path))
    try:
        await db._init_db()
        simulation = InvestmentSimulation(db)
        await simulation.initialize()
        assets = await simulation.calculate_assets()
        from sovereign_hall.services.portfolio_policy import review_position

        lifecycle_reviews: list[dict[str, Any]] = []
        review_now = datetime.now()
        for ticker, position in simulation.positions.items():
            code = simulation._normalize_ticker(ticker)
            quote = dict((assets.get("quote_details") or {}).get(code) or {})
            price = quote.get("price")
            source = str(quote.get("source") or "realtime_quote_unavailable")
            fetched_at = str(quote.get("quoted_at") or "")
            review = review_position(
                ticker=code,
                avg_cost=float(position.get("avg_cost") or 0.0),
                opened_at=position.get("opened_at"),
                price=float(price) if price is not None else None,
                price_at=fetched_at,
                price_source='verified_session_valuation' if quote.get('valuation_only') else source,
                now=review_now,
                max_price_age_days=simulation.max_trade_price_age_days,
                stop_loss_pct=simulation.stop_loss_pct,
                take_profit_pct=simulation.take_profit_pct,
                max_holding_days=simulation.max_holding_days,
            )
            if quote.get('valuation_only') and review.action!='exit':
                from dataclasses import replace
                review=replace(review,action='market_closed',reason='休市期间按最近交易时报价估值；成交等待开市实时行情')
            peak_raw = position.get("peak_price")
            peak_price = (
                max(float(peak_raw or price), float(price))
                if price is not None
                else (float(peak_raw) if peak_raw is not None else None)
            )
            peak_drawdown = (
                float(price) / peak_price - 1.0
                if price is not None and peak_price and peak_price > 0
                else None
            )
            lifecycle_reviews.append(
                {
                    **review.as_dict(),
                    "shares": int(position.get("shares") or 0),
                    "avg_cost": float(position.get("avg_cost") or 0.0),
                    "opened_at": position.get("opened_at"),
                    "stop_loss_pct": simulation.stop_loss_pct,
                    "stop_price": (
                        float(position.get("avg_cost") or 0.0)
                        * (1.0 + simulation.stop_loss_pct)
                    ),
                    "take_profit_pct": simulation.take_profit_pct,
                    "take_profit_price": (
                        float(position.get("avg_cost") or 0.0)
                        * (1.0 + simulation.take_profit_pct)
                    ),
                    "max_holding_days": simulation.max_holding_days,
                    "peak_price": peak_price,
                    "peak_drawdown": peak_drawdown,
                    "last_reviewed_at": position.get("last_reviewed_at"),
                    "durable_review_status": position.get("review_status"),
                    "durable_review_reason": position.get("review_reason"),
                }
            )
        conn = db._connection

        async with conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(fee), 0), MAX(traded_at) "
            "FROM simulation_trades"
        ) as cursor:
            trade_row = await cursor.fetchone()
        if window_start:
            async with conn.execute(
                "SELECT COUNT(*) FROM simulation_trades "
                "WHERE datetime(traded_at) > datetime(?)",
                (window_start,),
            ) as cursor:
                window_row = await cursor.fetchone()
            trades_since = int(window_row[0] if window_row else 0)
        else:
            trades_since = 0
        async with conn.execute(
            """
            SELECT s.total_assets, s.cash, s.positions_value,
                   s.snapshot_date, s.created_at
            FROM simulation_snapshots s
            JOIN (
                SELECT snapshot_date, MAX(id) AS max_id
                FROM simulation_snapshots
                GROUP BY snapshot_date
            ) latest ON latest.max_id = s.id
            ORDER BY date(s.snapshot_date), s.id
            """
        ) as cursor:
            snapshot_rows = [dict(row) async for row in cursor]

        runtime = await classify_simulation_runtime(conn, assets, simulation._market_data_service(),
            max_daily_trades=simulation.max_daily_trades, target_ratio=simulation.target_invested_ratio)
        performance = build_simulation_performance(
            initial_capital=simulation.initial_capital,
            assets=assets,
            trade_count=int(trade_row[0] if trade_row else 0),
            recorded_fees=float(trade_row[1] if trade_row else 0.0),
            latest_trade_at=trade_row[2] if trade_row else None,
            snapshot_rows=snapshot_rows,
            trades_since_window_start=trades_since,
            window_start=window_start,
            **runtime,
            no_trade_failure_days=int(
                simulation.config.get("no_trade_failure_days", 3) or 3
            ),
            minimum_healthy_invested_ratio=float(
                simulation.config.get("minimum_healthy_invested_ratio", 0.80)
                or 0.80
            ),
        )
        performance["position_lifecycle_reviews"] = lifecycle_reviews
        performance["quote_details"] = assets.get("quote_details") or {}
        return performance
    finally:
        await db.close()


def _collect_simulation_performance_sqlite_fallback(
    db_path: str | Path,
    *,
    window_start: str | None = None,
) -> dict[str, Any]:
    """Dependency-free, no-price-fallback account view for stdlib diagnostics."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cash_row = conn.execute(
            "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
        ).fetchone()
        cash = float(cash_row[0]) if cash_row else 10000.0
        positions = [
            str(row[0])
            for row in conn.execute(
                "SELECT ticker FROM simulation_positions ORDER BY ticker"
            ).fetchall()
        ]
        trade_row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(fee), 0), MAX(traded_at) "
            "FROM simulation_trades"
        ).fetchone()
        trades_since = (
            int(
                conn.execute(
                    "SELECT COUNT(*) FROM simulation_trades "
                    "WHERE datetime(traded_at) > datetime(?)",
                    (window_start,),
                ).fetchone()[0]
            )
            if window_start
            else 0
        )
        snapshot_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT s.total_assets, s.cash, s.positions_value,
                       s.snapshot_date, s.created_at
                FROM simulation_snapshots s
                JOIN (
                    SELECT snapshot_date, MAX(id) AS max_id
                    FROM simulation_snapshots
                    GROUP BY snapshot_date
                ) latest ON latest.max_id = s.id
                ORDER BY date(s.snapshot_date), s.id
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    valuation_complete = not positions
    assets = {
        "valuation_complete": valuation_complete,
        "total_assets": cash if valuation_complete else None,
        "cash": cash,
        "positions_value": 0.0 if valuation_complete else None,
        "invested_ratio": 0.0 if valuation_complete else None,
        "deployment_gap": cash if valuation_complete else None,
        "missing_price_tickers": positions,
    }
    return build_simulation_performance(
        initial_capital=10000.0,
        assets=assets,
        trade_count=int(trade_row[0] if trade_row else 0),
        recorded_fees=float(trade_row[1] if trade_row else 0.0),
        latest_trade_at=trade_row[2] if trade_row else None,
        snapshot_rows=snapshot_rows,
        trades_since_window_start=trades_since,
        window_start=window_start,
        no_trade_failure_days=3,
        minimum_healthy_invested_ratio=0.80,
    )


def load_persisted_simulation_performance(
    db_path: str | Path,
    *,
    now: datetime | None = None,
    no_trade_failure_days: int = 3,
    minimum_healthy_invested_ratio: float = 0.80,
) -> dict[str, Any]:
    """Read a safe status view without fetching prices.

    An empty account can be valued exactly from cash.  A non-empty account is
    deliberately marked incomplete here: callers that need a current score
    must use ``collect_simulation_performance`` so every holding is re-quoted.
    """
    path = Path(db_path)
    if not path.exists():
        return {}
    try:
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        cash_row = conn.execute(
            "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
        ).fetchone()
        cash = float(cash_row[0]) if cash_row else 10000.0
        initial_capital = 10000.0
        position_count = int(
            conn.execute("SELECT COUNT(*) FROM simulation_positions").fetchone()[0]
        )
        trade_row = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(fee), 0), MAX(traded_at) "
            "FROM simulation_trades"
        ).fetchone()
        snapshot_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT s.total_assets, s.cash, s.positions_value,
                       s.snapshot_date, s.created_at
                FROM simulation_snapshots s
                JOIN (
                    SELECT snapshot_date, MAX(id) AS max_id
                    FROM simulation_snapshots
                    GROUP BY snapshot_date
                ) latest ON latest.max_id = s.id
                ORDER BY date(s.snapshot_date), s.id
                """
            ).fetchall()
        ]
        runtime_row = conn.execute("SELECT value FROM system_stats WHERE key='simulation_runtime_status'").fetchone()
        runtime = json.loads(runtime_row[0]) if runtime_row else {}
        conn.close()
        measured = datetime.fromisoformat(runtime["measured_at"]) if runtime.get("measured_at") else None
        current = now or datetime.now().astimezone()
        if current.tzinfo is None:
            current = current.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        if measured is not None and measured.tzinfo is None:
            measured = measured.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        if measured is None or abs((current - measured).total_seconds()) > 180:
            runtime = {}
    except (sqlite3.Error, ValueError, TypeError):
        return {}

    if position_count:
        assets = {
            "valuation_complete": False,
            "cash": cash,
            "missing_price_tickers": ["realtime_refresh_required"],
        }
    else:
        assets = {
            "valuation_complete": True,
            "total_assets": cash,
            "cash": cash,
            "positions_value": 0.0,
            "invested_ratio": 0.0,
            "deployment_gap": cash,
            "missing_price_tickers": [],
        }
    return build_simulation_performance(
        initial_capital=initial_capital,
        assets=assets,
        trade_count=int(trade_row[0] if trade_row else 0),
        recorded_fees=float(trade_row[1] if trade_row else 0.0),
        latest_trade_at=trade_row[2] if trade_row else None,
        snapshot_rows=snapshot_rows,
        now=now,
        no_trade_failure_days=no_trade_failure_days,
        minimum_healthy_invested_ratio=minimum_healthy_invested_ratio,
        **{key: runtime[key] for key in ("research_status", "execution_status", "capital_status", "evaluation_status") if key in runtime},
    )
