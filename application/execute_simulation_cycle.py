"""Atomic persistence for a simulated fill.

Realtime quote retrieval and portfolio sizing happen before this short unit of
work.  Every durable consequence of the fill is committed together.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from ..domain.common.ids import new_id
from ..domain.portfolio.quote_freshness import is_fresh_quote, quote_age_seconds, retrieved_after
from ..domain.research import normalize_prediction_price_targets


class DailyTradeLimitReached(RuntimeError):
    pass


class StaleRealtimeQuote(RuntimeError):
    pass


class InvalidAccountTransition(RuntimeError):
    """Reject stale sizing or a fill that violates the durable account state."""


async def validate_balance_checkpoint(conn, cash, positions, *, create=True):
    """Observe an explicit legacy boundary; never invent or edit old cash flows.

    All ledger entries after that immutable boundary must reconcile. The old
    unmatched-sales/cash discrepancy remains visible as historical evidence.
    The caller owns the transaction when recording a checkpoint.
    """
    row = await (await conn.execute("SELECT value FROM system_stats WHERE key='simulation_balance_checkpoint'" )).fetchone()
    if row is None:
        ledger_id = (await (await conn.execute("SELECT COALESCE(MAX(id),0) FROM simulation_ledger_entries")).fetchone())[0]
        raw_cash, shares, unmatched = 10000.0, {}, []
        for trade in await (await conn.execute("SELECT id,ticker,direction,shares,price,fee FROM simulation_trades ORDER BY traded_at,id")).fetchall():
            trade_id, ticker, side, quantity, price, fee = trade
            delta = quantity if side == 'buy' else -quantity
            raw_cash -= delta * price + fee
            if side == 'sell' and quantity > shares.get(ticker, 0):
                unmatched.append(trade_id)
            else:
                shares[ticker] = shares.get(ticker, 0) + delta
        checkpoint = {"version": "observed_balance_boundary_v1", "observed_at": datetime.now().astimezone().isoformat(),
            "ledger_id": ledger_id, "cash": cash, "positions": positions,
            "historical_cash_difference": cash - raw_cash, "historical_unmatched_sale_ids": unmatched,
            "scope": "verify subsequent ledger only; historical records retained without inferred repairs"}
        if create:
            await conn.execute("INSERT OR IGNORE INTO system_stats(key,value,updated_at) VALUES "
                "('simulation_balance_checkpoint',?,?)", (json.dumps(checkpoint), checkpoint["observed_at"]))
        return {"status": "checkpoint_recorded" if create else "historical_boundary_not_recorded", **checkpoint}
    checkpoint = json.loads(row[0])
    expected_cash, expected_positions = float(checkpoint["cash"]), dict(checkpoint["positions"])
    for ticker, share_delta, cash_delta in await (await conn.execute(
        "SELECT ticker,share_delta,cash_delta FROM simulation_ledger_entries WHERE id>? ORDER BY id", (checkpoint["ledger_id"],))).fetchall():
        expected_cash += cash_delta
        if ticker:
            expected_positions[ticker] = expected_positions.get(ticker, 0) + share_delta
    expected_positions = {t: s for t, s in expected_positions.items() if s}
    balanced = math.isclose(cash, expected_cash, abs_tol=1e-6) and expected_positions == positions
    if not balanced and create:
        raise InvalidAccountTransition("durable account does not reconcile to its checkpoint and subsequent ledger")
    return {"status": "balanced_since_checkpoint" if balanced else "mismatch", **checkpoint,
            "cash_difference_since_checkpoint": cash - expected_cash}


@dataclass(frozen=True)
class TradeCommitRequest:
    ticker: str
    direction: str
    shares: int
    price: float
    gross_amount: float
    commission: float
    stamp_duty: float
    slippage_cost: float
    total_fee: float
    reason: str
    quote_provider: str
    quote_fetched_at: str
    quote_quoted_at: str
    new_cash: float
    new_positions: Dict[str, Dict[str, Any]]
    idempotency_key: str
    deployment_gap_after: Optional[float] = None
    round_id: Optional[str] = None
    intent_id: Optional[str] = None
    pending_decision_id: Optional[int] = None
    traded_at: str = ""
    asset_type: str = ""
    cost_model_version: str = ""
    expected_cash: Optional[float] = None
    expected_shares: Optional[Dict[str, int]] = None
    position_marks: Optional[Dict[str, float]] = None
    approved_target: Optional[float] = None


@dataclass(frozen=True)
class TradeCommitResult:
    trade_id: int
    quote_snapshot_id: str
    duplicate: bool = False


@dataclass(frozen=True)
class ExecutionRejectionCommitRequest:
    ticker: str
    price: float
    quote_provider: str
    quote_fetched_at: str
    quote_quoted_at: str
    code: str
    reason: str
    intent_id: str
    idempotency_key: str
    round_id: Optional[str] = None
    pending_decision_id: Optional[int] = None
    deployment_gap: Optional[float] = None
    next_action: str = ""
    recorded_at: str = ""


@dataclass(frozen=True)
class ExecutionRejectionCommitResult:
    quote_snapshot_id: str
    duplicate: bool = False


@dataclass(frozen=True)
class UnquotedExecutionRejectionCommitRequest:
    """A terminal execution outcome that intentionally has no quote.

    Missing/stale quotes and price-independent hard gates must not create a
    quote snapshot.  They still need one atomic durable boundary for the
    intent, pending replay, prediction lineage, redeployment state, round
    terminal and append-only event.
    """

    ticker: str
    code: str
    reason: str
    intent_id: str
    round_id: Optional[str] = None
    pending_decision_id: Optional[int] = None
    deployment_gap: Optional[float] = None
    next_action: str = ""
    recorded_at: str = ""


@dataclass(frozen=True)
class UnquotedExecutionRejectionCommitResult:
    prediction_id: Optional[str] = None
    duplicate: bool = False


class AtomicSimulationExecutor:
    def __init__(
        self,
        db_service,
        *,
        max_daily_trades: int = 5,
        max_quote_age_seconds: int = 120,
    ):
        self.db_service = db_service
        self.max_daily_trades = int(max_daily_trades)
        self.max_quote_age_seconds = int(max_quote_age_seconds)

    async def commit(self, request: TradeCommitRequest) -> TradeCommitResult:
        if not self.db_service:
            raise RuntimeError("durable simulation database is required")
        traded_at = request.traded_at or datetime.now().isoformat()
        trade_date = traded_at[:10]
        quote_id = new_id("quote")
        quote_age = quote_age_seconds(request.quote_fetched_at, request.quote_quoted_at)
        if (
            not is_fresh_quote({"price": request.price, "source": request.quote_provider,
                                "fetched_at": request.quote_fetched_at,
                                "quoted_at": request.quote_quoted_at},
                               max_age_seconds=self.max_quote_age_seconds)
        ):
            raise StaleRealtimeQuote(
                "simulated fill requires a fresh realtime quote with provider metadata"
            )

        async with self.db_service.transaction(immediate=True) as conn:
            quote_age = quote_age_seconds(request.quote_fetched_at, request.quote_quoted_at)
            if quote_age is None or quote_age > self.max_quote_age_seconds:
                raise StaleRealtimeQuote("quote expired while awaiting transaction")
            async with conn.execute(
                """
                SELECT id, quote_snapshot_id
                FROM simulation_trades
                WHERE idempotency_key = ?
                """,
                (request.idempotency_key,),
            ) as cursor:
                duplicate = await cursor.fetchone()
            if duplicate:
                return TradeCommitResult(
                    trade_id=int(duplicate[0]),
                    quote_snapshot_id=str(duplicate[1] or ""),
                    duplicate=True,
                )

            async with conn.execute(
                "SELECT COUNT(*) FROM simulation_trades WHERE date(traded_at) = ?",
                (trade_date,),
            ) as cursor:
                row = await cursor.fetchone()
            if int(row[0] if row else 0) >= self.max_daily_trades:
                raise DailyTradeLimitReached(
                    f"today already reached {self.max_daily_trades} simulated fills"
                )

            await self._validate_transition(conn, request)
            await self._validate_intent_quote(conn, request)

            await conn.execute(
                """
                INSERT INTO quote_snapshots (
                    id, round_id, intent_id, ticker, price, provider, purpose,
                    fetched_at, received_at, freshness_seconds, quoted_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'execution', ?, ?, ?, ?)
                """,
                (
                    quote_id,
                    request.round_id,
                    request.intent_id,
                    request.ticker,
                    request.price,
                    request.quote_provider,
                    request.quote_fetched_at,
                    datetime.now().isoformat(),
                    quote_age,
                    request.quote_quoted_at,
                ),
            )
            await _anchor_deferred_prediction(
                conn,
                intent_id=request.intent_id,
                ticker=request.ticker,
                price=request.price,
                quote_provider=request.quote_provider,
                quote_fetched_at=request.quote_fetched_at,
                quote_snapshot_id=quote_id,
                recorded_at=traded_at,
                round_id=request.round_id,
            )
            cursor = await conn.execute(
                """
                INSERT INTO simulation_trades (
                    ticker, direction, shares, price, fee, reason, traded_at,
                    round_id, intent_id, quote_snapshot_id, quote_provider,
                    quote_fetched_at, gross_amount, commission, stamp_duty,
                    slippage_cost, idempotency_key, asset_type,
                    cost_model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.ticker,
                    request.direction,
                    request.shares,
                    request.price,
                    request.total_fee,
                    request.reason,
                    traded_at,
                    request.round_id,
                    request.intent_id,
                    quote_id,
                    request.quote_provider,
                    request.quote_fetched_at,
                    request.gross_amount,
                    request.commission,
                    request.stamp_duty,
                    request.slippage_cost,
                    request.idempotency_key,
                    request.asset_type or None,
                    request.cost_model_version or None,
                ),
            )
            trade_id = int(cursor.lastrowid)
            share_delta = (
                request.shares if request.direction == "buy" else -request.shares
            )
            cash_delta = (
                -(request.gross_amount + request.total_fee)
                if request.direction == "buy"
                else request.gross_amount - request.total_fee
            )
            await conn.execute(
                """
                INSERT INTO simulation_ledger_entries (
                    trade_id, intent_id, round_id, entry_type, ticker,
                    share_delta, cash_delta, commission, stamp_duty,
                    slippage_cost, source, created_at, asset_type,
                    cost_model_version
                ) VALUES (?, ?, ?, 'simulated_fill', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    request.intent_id,
                    request.round_id,
                    request.ticker,
                    share_delta,
                    cash_delta,
                    request.commission,
                    request.stamp_duty,
                    request.slippage_cost,
                    "atomic_simulation_executor",
                    traded_at,
                    request.asset_type or None,
                    request.cost_model_version or None,
                ),
            )

            await conn.execute(
                """
                INSERT OR REPLACE INTO system_stats(key, value, updated_at)
                VALUES ('simulation_cash', ?, ?)
                """,
                (str(request.new_cash), traded_at),
            )
            await conn.execute(
                """
                INSERT OR REPLACE INTO system_stats(key, value, updated_at)
                VALUES ('last_trade_date', ?, ?)
                """,
                (traded_at, traded_at),
            )
            for ticker, position in request.new_positions.items():
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO simulation_positions (
                        ticker, shares, avg_cost, updated_at, opened_at,
                        peak_price, last_mark_price, last_mark_at,
                        last_mark_source, last_reviewed_at, review_status,
                        review_reason, holding_period_days,
                        strategy_stop_loss_pct, strategy_take_profit_pct,
                        failure_conditions, contract_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker,
                        int(position["shares"]),
                        float(position["avg_cost"]),
                        traded_at,
                        position.get("opened_at"),
                        position.get("peak_price"),
                        position.get("last_mark_price"),
                        position.get("last_mark_at"),
                        position.get("last_mark_source"),
                        position.get("last_reviewed_at"),
                        position.get("review_status"),
                        position.get("review_reason"),
                        position.get("holding_period_days"),
                        position.get("strategy_stop_loss_pct"),
                        position.get("strategy_take_profit_pct"),
                        position.get("failure_conditions"),
                        position.get("contract_source"),
                    ),
                )
            if request.new_positions:
                placeholders = ",".join("?" for _ in request.new_positions)
                await conn.execute(
                    f"DELETE FROM simulation_positions "
                    f"WHERE ticker NOT IN ({placeholders})",
                    tuple(request.new_positions),
                )
            else:
                await conn.execute("DELETE FROM simulation_positions")

            if request.intent_id:
                await conn.execute(
                    """
                    UPDATE execution_intents
                    SET status = 'executed', executed_at = ?, updated_at = ?,
                        resolution = ?, defer_code = NULL
                    WHERE id = ? AND status IN ('pending', 'deferred', 'executing')
                    """,
                    (
                        traded_at,
                        traded_at,
                        f"simulation_trade:{trade_id}",
                        request.intent_id,
                    ),
                )
                # A fresh lifecycle review may complete the same intent before
                # its deferred-queue row is claimed by replay. Resolve that
                # exact intent's active rows in this fill transaction too.
                # Otherwise replay mislabels an already filled ruling rejected.
                await conn.execute(
                    """
                    UPDATE simulation_pending_decisions
                    SET status = 'executed', resolved_at = ?, updated_at = ?,
                        resolution = ?, defer_code = ''
                    WHERE intent_id = ?
                      AND status IN ('pending_next_trading_session', 'replaying')
                    """,
                    (traded_at, traded_at, f"simulation_trade:{trade_id}", request.intent_id),
                )
            if request.pending_decision_id is not None:
                await conn.execute(
                    """
                    UPDATE simulation_pending_decisions
                    SET status = 'executed', resolved_at = ?, updated_at = ?,
                        resolution = ?, defer_code = ''
                    WHERE id = ?
                    """,
                    (
                        traded_at,
                        traded_at,
                        f"simulation_trade:{trade_id}",
                        int(request.pending_decision_id),
                    ),
                )

            if request.direction in {"buy", "sell"}:
                is_sell = request.direction == "sell"
                redeployment_status = (
                    "pending_approved_candidates"
                    if is_sell
                    else (
                        "completed"
                        if request.deployment_gap_after is not None
                        and request.deployment_gap_after <= 0.01
                        else "partially_redeployed"
                    )
                )
                blocker_code = (
                    "released_capital_pending_redeployment"
                    if is_sell
                    else (
                        ""
                        if redeployment_status == "completed"
                        else "residual_operational_cash"
                    )
                )
                blocker_reason = (
                    (
                        f"模拟卖出释放 "
                        f"{request.gross_amount - request.total_fee:.2f} 元，"
                        "等待同一投资闭环再配置"
                    )
                    if is_sell
                    else (
                        "资金部署达到100%目标"
                        if redeployment_status == "completed"
                        else "模拟买入已原子提交，剩余操作性现金继续进入再配置队列"
                    )
                )
                next_action = (
                    "投委会提供合格标的后，以实时行情重新估值并模拟成交"
                    if is_sell
                    else (
                        "继续逐仓生命周期复核"
                        if redeployment_status == "completed"
                        else "下一轮继续消费剩余部署缺口"
                    )
                )
                await conn.execute(
                    """
                    INSERT INTO simulation_redeployment_state (
                        id, status, deployment_gap, blocker_code,
                        blocker_reason, next_action, source, attempt_count,
                        last_attempt_at, last_candidate_count, last_trade_count,
                        last_entry_trade_count, last_exit_trade_count,
                        last_rejection_counts, rejection_counts_total,
                        created_at, updated_at, completed_at
                    ) VALUES (
                        1, ?, ?, ?, ?, ?, ?, 0, ?, 0, 1, ?, ?, '{}', '{}',
                        ?, ?, ?
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        status = excluded.status,
                        deployment_gap = excluded.deployment_gap,
                        blocker_code = excluded.blocker_code,
                        blocker_reason = excluded.blocker_reason,
                        next_action = excluded.next_action,
                        source = excluded.source,
                        last_attempt_at = excluded.last_attempt_at,
                        last_candidate_count = 0,
                        last_trade_count = 1,
                        last_entry_trade_count = excluded.last_entry_trade_count,
                        last_exit_trade_count = excluded.last_exit_trade_count,
                        last_rejection_counts = '{}',
                        updated_at = excluded.updated_at,
                        completed_at = excluded.completed_at
                    """,
                    (
                        redeployment_status,
                        request.deployment_gap_after,
                        blocker_code,
                        blocker_reason,
                        next_action,
                        f"simulation_{request.direction}_trade:{trade_id}:{request.ticker}",
                        traded_at,
                        0 if is_sell else 1,
                        1 if is_sell else 0,
                        traded_at,
                        traded_at,
                        traded_at if redeployment_status == "completed" else None,
                    ),
                )

            if request.round_id:
                async with conn.execute(
                    """
                    SELECT terminal_code
                    FROM research_rounds
                    WHERE id = ?
                    """,
                    (request.round_id,),
                ) as cursor:
                    round_row = await cursor.fetchone()
                previous_terminal_code = (
                    str(round_row[0] or "") if round_row else ""
                )
                await conn.execute(
                    """
                    UPDATE research_rounds
                    SET terminal_code = 'filled',
                        terminal_reason = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND status <> 'failed'
                      AND COALESCE(terminal_code, '') NOT IN (
                          'failed', 'pipeline_exception', 'cancelled', 'interrupted'
                      )
                    """,
                    (
                        (
                            "模拟成交已原子提交；"
                            f"trade_id={trade_id}; ticker={request.ticker}"
                        ),
                        traded_at,
                        request.round_id,
                    ),
                )
                async with conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM round_events WHERE round_id = ?",
                    (request.round_id,),
                ) as cursor:
                    sequence_row = await cursor.fetchone()
                await conn.execute(
                    """
                    INSERT INTO round_events (
                        round_id, sequence, event_type, payload_json, created_at
                    ) VALUES (?, ?, 'SimulatedFillCreated', ?, ?)
                    """,
                    (
                        request.round_id,
                        int(sequence_row[0] if sequence_row else 1),
                        json.dumps(
                            {
                                "trade_id": trade_id,
                                "ticker": request.ticker,
                                "direction": request.direction,
                                "shares": request.shares,
                                "price": request.price,
                                "total_fee": request.total_fee,
                                "previous_terminal_code": (
                                    previous_terminal_code or None
                                ),
                                "terminal_code": "filled",
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        traded_at,
                    ),
                )

        return TradeCommitResult(trade_id=trade_id, quote_snapshot_id=quote_id)

    async def _validate_intent_quote(self, conn, request) -> None:
        """Check the durable boundary for fills AND quoted rejections."""
        intent = await (await conn.execute(
            'SELECT created_at,status,ticker,idempotency_key,direction FROM execution_intents WHERE id=?',
            (request.intent_id,))).fetchone()
        if (intent is None or intent[1] not in {'pending', 'deferred', 'executing'}
                or intent[2] != request.ticker or intent[3] != request.idempotency_key):
            raise InvalidAccountTransition('matching executable price-free intent required')
        side = getattr(request, 'direction', None)
        if side and intent[4] not in ({'long', 'buy'} if side == 'buy' else {'short', 'sell'}):
            raise InvalidAccountTransition('fill direction differs from durable intent')
        if not retrieved_after({'fetched_at': request.quote_fetched_at}, intent[0]):
            raise StaleRealtimeQuote('execution quote retrieval must follow durable intent')

    async def _validate_transition(self, conn, request: TradeCommitRequest) -> None:
        """Recheck money, quantities and current limits under the write lock."""
        from ..core.config import get_config
        from ..domain.portfolio.execution_policy import approved_execution_target
        policy = get_config().get("simulation", {})
        cash_row = await (await conn.execute(
            "SELECT value FROM system_stats WHERE key='simulation_cash'" )).fetchone()
        cash = float(cash_row[0]) if cash_row else float(policy.get("initial_capital", 10000))
        before = {str(row[0]): int(row[1]) for row in await (await conn.execute(
            "SELECT ticker,shares FROM simulation_positions WHERE shares>0")).fetchall()}
        await validate_balance_checkpoint(conn, cash, before)
        if request.expected_cash is not None and not math.isclose(cash, request.expected_cash, abs_tol=1e-7):
            raise InvalidAccountTransition("cash changed after sizing; reload before retry")
        if request.expected_shares is not None and before != request.expected_shares:
            raise InvalidAccountTransition("positions changed after sizing; reload before retry")
        if (request.direction not in {"buy", "sell"} or request.shares <= 0
                or int(request.shares) != request.shares
                or not all(math.isfinite(v) for v in (request.price, request.gross_amount,
                    request.total_fee, request.new_cash, request.commission,
                    request.stamp_duty, request.slippage_cost))
                or request.price <= 0 or min(request.total_fee, request.commission,
                    request.stamp_duty, request.slippage_cost) < 0
                or not math.isclose(request.gross_amount, request.price * request.shares, abs_tol=1e-7)
                or not math.isclose(request.total_fee, request.commission + request.stamp_duty + request.slippage_cost, abs_tol=1e-7)):
            raise InvalidAccountTransition("invalid fill quantity, amount or fee components")
        delta = request.shares if request.direction == "buy" else -request.shares
        after = dict(before)
        after[request.ticker] = after.get(request.ticker, 0) + delta
        if after[request.ticker] < 0:
            raise InvalidAccountTransition("sell exceeds durable position")
        after = {ticker: shares for ticker, shares in after.items() if shares > 0}
        supplied = {ticker: int(pos["shares"]) for ticker, pos in request.new_positions.items() if pos["shares"] > 0}
        cash_delta = (-request.gross_amount if delta > 0 else request.gross_amount) - request.total_fee
        if supplied != after or request.new_cash < -1e-7 or not math.isclose(
                request.new_cash, cash + cash_delta, abs_tol=1e-7):
            raise InvalidAccountTransition("cash or positions do not reconcile to the fill")
        if delta <= 0:
            return  # Risk exits must remain possible with other missing quotes.
        marks = dict(request.position_marks or {})
        marks[request.ticker] = request.price
        if any(ticker not in marks or not math.isfinite(marks[ticker]) or marks[ticker] <= 0 for ticker in before):
            raise InvalidAccountTransition("buy requires marks for the complete durable book")
        equity = cash + sum(shares * marks[ticker] for ticker, shares in before.items())
        budget = float(policy.get("max_single_position", 1.0))
        if request.approved_target is not None:
            budget = min(budget, request.approved_target)
        if request.intent_id:
            intent = await (await conn.execute(
                "SELECT target_position,round_id,decision_id,status FROM execution_intents WHERE id=?",
                (request.intent_id,))).fetchone()
            if intent is None or intent[3] not in {"pending", "deferred", "executing"}:
                raise InvalidAccountTransition("intent is no longer executable")
            budget = min(budget, float(intent[0]))
            if intent[1] and intent[2]:
                outcome = await (await conn.execute(
                    "SELECT target_position,confidence FROM simulation_committee_outcomes "
                    "WHERE round_id=? AND decision_id=? AND ticker=? ORDER BY id DESC LIMIT 1",
                    (intent[1], intent[2], request.ticker))).fetchone()
                if outcome:
                    budget = min(budget, approved_execution_target(outcome[0], outcome[1]))
        if after.get(request.ticker, 0) * request.price > equity * budget + 1e-7:
            raise InvalidAccountTransition("fill exceeds current or approved single-name budget")
        if sum(shares * marks[ticker] for ticker, shares in after.items()) > equity * float(policy.get("max_gross", 1.0)) + 1e-7:
            raise InvalidAccountTransition("fill exceeds current gross exposure budget")

    async def commit_rejection(
        self,
        request: ExecutionRejectionCommitRequest,
    ) -> ExecutionRejectionCommitResult:
        """Atomically preserve a fresh execution quote and its non-fill outcome.

        A valid quote that proves a price/lot/cash/valuation blocker is part of
        the execution audit trail. Missing or stale quotes still create neither
        a snapshot nor a fill.
        """
        if not self.db_service:
            raise RuntimeError("durable simulation database is required")
        recorded_at = request.recorded_at or datetime.now().isoformat()
        quote_age = quote_age_seconds(request.quote_fetched_at, request.quote_quoted_at)
        if (
            not is_fresh_quote({"price": request.price, "source": request.quote_provider,
                                "fetched_at": request.quote_fetched_at,
                                "quoted_at": request.quote_quoted_at},
                               max_age_seconds=self.max_quote_age_seconds)
        ):
            raise StaleRealtimeQuote(
                "execution rejection requires a fresh realtime quote with provider metadata"
            )

        quote_id = new_id("quote")
        resolution = f"{request.code}:{request.reason}"[:1000]
        next_action = (
            request.next_action.strip()
            or "下一轮研究优先形成满足当前实时价格、整手和静态仓位硬门的候选"
        )
        async with self.db_service.transaction(immediate=True) as conn:
            quote_age = quote_age_seconds(request.quote_fetched_at, request.quote_quoted_at)
            if quote_age is None or quote_age > self.max_quote_age_seconds:
                raise StaleRealtimeQuote("quote expired while awaiting transaction")
            async with conn.execute(
                """
                SELECT status, resolution
                FROM execution_intents
                WHERE id = ? AND idempotency_key = ?
                """,
                (request.intent_id, request.idempotency_key),
            ) as cursor:
                intent_row = await cursor.fetchone()
            if not intent_row:
                raise KeyError(f"execution intent not found: {request.intent_id}")
            if str(intent_row[0]) == "executed":
                raise RuntimeError("cannot reject an executed intent")

            async with conn.execute(
                """
                SELECT id
                FROM quote_snapshots
                WHERE intent_id = ? AND purpose = 'execution_rejection'
                ORDER BY rowid
                LIMIT 1
                """,
                (request.intent_id,),
            ) as cursor:
                duplicate_quote = await cursor.fetchone()
            if str(intent_row[0]) == "rejected" and duplicate_quote:
                return ExecutionRejectionCommitResult(
                    quote_snapshot_id=str(duplicate_quote[0]),
                    duplicate=True,
                )

            await self._validate_intent_quote(conn, request)

            if duplicate_quote:
                quote_id = str(duplicate_quote[0])
            else:
                await conn.execute(
                    """
                    INSERT INTO quote_snapshots (
                        id, round_id, intent_id, ticker, price, provider, purpose,
                        fetched_at, received_at, freshness_seconds, quoted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'execution_rejection', ?, ?, ?, ?)
                    """,
                    (
                        quote_id,
                        request.round_id,
                        request.intent_id,
                        request.ticker,
                        request.price,
                        request.quote_provider,
                        request.quote_fetched_at,
                        recorded_at,
                        quote_age,
                        request.quote_quoted_at,
                    ),
                )
            await _anchor_deferred_prediction(
                conn,
                intent_id=request.intent_id,
                ticker=request.ticker,
                price=request.price,
                quote_provider=request.quote_provider,
                quote_fetched_at=request.quote_fetched_at,
                quote_snapshot_id=quote_id,
                recorded_at=recorded_at,
                round_id=request.round_id,
            )

            cursor = await conn.execute(
                """
                UPDATE execution_intents
                SET status = 'rejected', defer_code = ?, resolution = ?,
                    updated_at = ?
                WHERE id = ? AND status IN ('pending', 'deferred', 'executing')
                """,
                (
                    request.code,
                    resolution,
                    recorded_at,
                    request.intent_id,
                ),
            )
            if not cursor.rowcount and str(intent_row[0]) != "rejected":
                raise RuntimeError(
                    f"execution intent changed before rejection: {request.intent_id}"
                )

            if request.pending_decision_id is not None:
                await conn.execute(
                    """
                    UPDATE simulation_pending_decisions
                    SET status = 'rejected', resolved_at = ?, updated_at = ?,
                        resolution = ?, defer_code = ?
                    WHERE id = ?
                    """,
                    (
                        recorded_at,
                        recorded_at,
                        resolution,
                        request.code,
                        int(request.pending_decision_id),
                    ),
                )

            await conn.execute(
                """
                INSERT INTO simulation_redeployment_state (
                    id, status, deployment_gap, blocker_code, blocker_reason,
                    next_action, source, attempt_count, last_candidate_count,
                    last_trade_count, last_rejection_counts,
                    rejection_counts_total, created_at, updated_at
                ) VALUES (
                    1, 'blocked_candidate_execution', ?, ?, ?, ?,
                    'atomic_execution_rejection', 0, 1, 0, ?, '{}', ?, ?
                )
                ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    deployment_gap = excluded.deployment_gap,
                    blocker_code = excluded.blocker_code,
                    blocker_reason = excluded.blocker_reason,
                    next_action = excluded.next_action,
                    source = excluded.source,
                    last_candidate_count = 1,
                    last_trade_count = 0,
                    last_rejection_counts = excluded.last_rejection_counts,
                    updated_at = excluded.updated_at,
                    completed_at = NULL
                """,
                (
                    request.deployment_gap,
                    request.code,
                    request.reason[:2000],
                    next_action[:2000],
                    json.dumps({request.code: 1}, ensure_ascii=False, sort_keys=True),
                    recorded_at,
                    recorded_at,
                ),
            )

            if request.round_id:
                async with conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM round_events WHERE round_id = ?",
                    (request.round_id,),
                ) as cursor:
                    sequence_row = await cursor.fetchone()
                await conn.execute(
                    """
                    INSERT INTO round_events (
                        round_id, sequence, event_type, payload_json, created_at
                    ) VALUES (?, ?, 'ExecutionRejectedWithQuote', ?, ?)
                    """,
                    (
                        request.round_id,
                        int(sequence_row[0] if sequence_row else 1),
                        json.dumps(
                            {
                                "blocker_code": request.code,
                                "intent_id": request.intent_id,
                                "price": request.price,
                                "provider": request.quote_provider,
                                "quote_snapshot_id": quote_id,
                                "reason": request.reason,
                                "ticker": request.ticker,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        recorded_at,
                    ),
                )

        return ExecutionRejectionCommitResult(quote_snapshot_id=quote_id)

    async def commit_unquoted_rejection(
        self,
        request: UnquotedExecutionRejectionCommitRequest,
    ) -> UnquotedExecutionRejectionCommitResult:
        """Atomically terminate an intent without inventing a quote.

        The intent id is the stable idempotency boundary.  A replay after the
        first successful transition observes the already-rejected row and does
        not append a duplicate event.  Failed writes roll the whole unit back,
        leaving the price-free intent retryable.
        """
        if not self.db_service:
            raise RuntimeError("durable simulation database is required")
        recorded_at = request.recorded_at or datetime.now().isoformat()
        resolution = f"{request.code}:{request.reason}"[:1000]
        next_action = (
            request.next_action.strip()
            or "下一轮研究形成可执行候选后，重新取得完整实时行情并重过全部硬门"
        )

        async with self.db_service.transaction(immediate=True) as conn:
            async with conn.execute(
                """
                SELECT status, round_id, decision_id, ticker
                FROM execution_intents
                WHERE id = ?
                """,
                (request.intent_id,),
            ) as cursor:
                intent_row = await cursor.fetchone()
            if not intent_row:
                raise KeyError(f"execution intent not found: {request.intent_id}")
            intent_status = str(intent_row[0] or "")
            if intent_status == "executed":
                raise RuntimeError("cannot reject an executed intent")

            effective_round_id = request.round_id or (
                str(intent_row[1]) if intent_row[1] else None
            )
            if intent_status == "rejected":
                return UnquotedExecutionRejectionCommitResult(duplicate=True)

            cursor = await conn.execute(
                """
                UPDATE execution_intents
                SET status = 'rejected', defer_code = ?, resolution = ?,
                    updated_at = ?
                WHERE id = ? AND status IN ('pending', 'deferred', 'executing')
                """,
                (
                    request.code,
                    resolution,
                    recorded_at,
                    request.intent_id,
                ),
            )
            if not cursor.rowcount:
                raise RuntimeError(
                    f"execution intent changed before rejection: {request.intent_id}"
                )

            if request.pending_decision_id is not None:
                await conn.execute(
                    """
                    UPDATE simulation_pending_decisions
                    SET status = 'rejected', resolved_at = ?, updated_at = ?,
                        resolution = ?, defer_code = ?
                    WHERE id = ?
                    """,
                    (
                        recorded_at,
                        recorded_at,
                        resolution,
                        request.code,
                        int(request.pending_decision_id),
                    ),
                )

            prediction_id = await _reject_deferred_prediction_without_quote(
                conn,
                intent_id=request.intent_id,
                recorded_at=recorded_at,
            )

            await conn.execute(
                """
                INSERT INTO simulation_redeployment_state (
                    id, status, deployment_gap, blocker_code, blocker_reason,
                    next_action, source, attempt_count, last_candidate_count,
                    last_trade_count, last_rejection_counts,
                    rejection_counts_total, created_at, updated_at
                ) VALUES (
                    1, 'blocked_candidate_execution', ?, ?, ?, ?,
                    'atomic_execution_rejection_without_quote', 0, 1, 0, ?,
                    '{}', ?, ?
                )
                ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    deployment_gap = COALESCE(
                        excluded.deployment_gap,
                        simulation_redeployment_state.deployment_gap
                    ),
                    blocker_code = excluded.blocker_code,
                    blocker_reason = excluded.blocker_reason,
                    next_action = excluded.next_action,
                    source = excluded.source,
                    last_candidate_count = 1,
                    last_trade_count = 0,
                    last_rejection_counts = excluded.last_rejection_counts,
                    updated_at = excluded.updated_at,
                    completed_at = NULL
                """,
                (
                    request.deployment_gap,
                    request.code,
                    request.reason[:2000],
                    next_action[:2000],
                    json.dumps(
                        {request.code: 1},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    recorded_at,
                    recorded_at,
                ),
            )

            previous_terminal_code = None
            if effective_round_id:
                async with conn.execute(
                    "SELECT terminal_code FROM research_rounds WHERE id = ?",
                    (effective_round_id,),
                ) as cursor:
                    round_row = await cursor.fetchone()
                previous_terminal_code = (
                    str(round_row[0] or "") if round_row else ""
                )
                await conn.execute(
                    """
                    UPDATE research_rounds
                    SET terminal_code = 'execution_rejected',
                        terminal_reason = ?, updated_at = ?
                    WHERE id = ?
                      AND status <> 'failed'
                      AND COALESCE(terminal_code, '') NOT IN (
                          'filled', 'failed', 'pipeline_exception', 'cancelled',
                          'interrupted'
                      )
                    """,
                    (
                        (
                            "执行裁决在无可用新鲜行情/price-free硬门处原子终止；"
                            f"intent_id={request.intent_id}; code={request.code}"
                        ),
                        recorded_at,
                        effective_round_id,
                    ),
                )
                async with conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 "
                    "FROM round_events WHERE round_id = ?",
                    (effective_round_id,),
                ) as cursor:
                    sequence_row = await cursor.fetchone()
                await conn.execute(
                    """
                    INSERT INTO round_events (
                        round_id, sequence, event_type, payload_json, created_at
                    ) VALUES (?, ?, 'ExecutionRejectedWithoutQuote', ?, ?)
                    """,
                    (
                        effective_round_id,
                        int(sequence_row[0] if sequence_row else 1),
                        json.dumps(
                            {
                                "blocker_code": request.code,
                                "intent_id": request.intent_id,
                                "prediction_id": prediction_id,
                                "previous_terminal_code": (
                                    previous_terminal_code or None
                                ),
                                "quote_snapshot_created": False,
                                "reason": request.reason,
                                "terminal_code": (
                                    "filled"
                                    if previous_terminal_code == "filled"
                                    else "execution_rejected"
                                ),
                                "ticker": request.ticker,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        recorded_at,
                    ),
                )

        return UnquotedExecutionRejectionCommitResult(
            prediction_id=prediction_id,
        )


async def _anchor_deferred_prediction(
    conn,
    *,
    intent_id: Optional[str],
    ticker: str,
    price: float,
    quote_provider: str,
    quote_fetched_at: str,
    quote_snapshot_id: str,
    recorded_at: str,
    round_id: Optional[str],
) -> Optional[str]:
    """Anchor a price-free committee prediction inside the execution UoW."""
    if not intent_id:
        return None
    async with conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='price_predictions'"
    ) as cursor:
        if not await cursor.fetchone():
            return None
    async with conn.execute(
        """
        SELECT prediction.id, prediction.direction,
               prediction.target_price, prediction.stop_loss,
               prediction.round_id
        FROM execution_intents intent
        JOIN price_predictions prediction
          ON prediction.decision_id = intent.decision_id
         AND prediction.ticker = intent.ticker
        WHERE intent.id = ?
          AND prediction.status = 'awaiting_entry_quote'
          AND (prediction.round_id = intent.round_id
               OR (prediction.round_id IS NULL AND intent.round_id IS NULL))
        ORDER BY prediction.rowid
        LIMIT 1
        """,
        (intent_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        return None

    prediction_id = str(row[0])
    target_price, stop_loss = normalize_prediction_price_targets(
        decision=str(row[1] or "hold"),
        entry_price=float(price),
        target_price=float(row[2] or 0.0),
        stop_loss=float(row[3] or 0.0),
    )
    cursor = await conn.execute(
        """
        UPDATE price_predictions
        SET current_price = ?, target_price = ?, stop_loss = ?,
            status = 'pending', entry_date = ?, quote_source = ?,
            quote_fetched_at = ?
        WHERE id = ? AND status = 'awaiting_entry_quote'
        """,
        (
            float(price),
            target_price,
            stop_loss,
            quote_fetched_at,
            quote_provider,
            quote_fetched_at,
            prediction_id,
        ),
    )
    if not cursor.rowcount:
        return None

    event_round_id = round_id or (str(row[4]) if row[4] else None)
    if event_round_id:
        async with conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 "
            "FROM round_events WHERE round_id = ?",
            (event_round_id,),
        ) as cursor:
            sequence_row = await cursor.fetchone()
        await conn.execute(
            """
            INSERT INTO round_events (
                round_id, sequence, event_type, payload_json, created_at
            ) VALUES (?, ?, 'PredictionQuoteAnchored', ?, ?)
            """,
            (
                event_round_id,
                int(sequence_row[0] if sequence_row else 1),
                json.dumps(
                    {
                        "intent_id": intent_id,
                        "prediction_id": prediction_id,
                        "provider": quote_provider,
                        "quote_snapshot_id": quote_snapshot_id,
                        "ticker": ticker,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                recorded_at,
            ),
        )
    return prediction_id


async def _reject_deferred_prediction_without_quote(
    conn,
    *,
    intent_id: str,
    recorded_at: str,
) -> Optional[str]:
    """Close exact deferred prediction lineage when execution cannot start."""
    async with conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='price_predictions'"
    ) as cursor:
        if not await cursor.fetchone():
            return None
    async with conn.execute(
        """
        SELECT prediction.id
        FROM execution_intents intent
        JOIN price_predictions prediction
          ON prediction.decision_id = intent.decision_id
         AND prediction.ticker = intent.ticker
        WHERE intent.id = ?
          AND prediction.status = 'awaiting_entry_quote'
          AND (prediction.round_id = intent.round_id
               OR (prediction.round_id IS NULL AND intent.round_id IS NULL))
        ORDER BY prediction.rowid
        LIMIT 1
        """,
        (intent_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        return None
    prediction_id = str(row[0])
    cursor = await conn.execute(
        """
        UPDATE price_predictions
        SET status = 'execution_rejected', result = 'unknown',
            validated_at = NULL
        WHERE id = ? AND status = 'awaiting_entry_quote'
        """,
        (prediction_id,),
    )
    return prediction_id if cursor.rowcount else None
