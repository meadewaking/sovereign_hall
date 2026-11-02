"""Durable 10,000 RMB paper broker isolated from the production simulation."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Mapping

import aiosqlite

from ..domain.common.ids import new_id
from ..domain.portfolio.costs import CostSchedule
from ..domain.portfolio.instruments import InstrumentProfile, normalize_ticker
from ..ports.broker import (
    BrokerAccount,
    BrokerFill,
    BrokerOrder,
    BrokerQuote,
    ReconciliationResult,
    assert_safe_execution_mode,
)
from .sqlite.migrations import apply_architecture_migrations


PAPER_INITIAL_CAPITAL = 10000.0
DEFAULT_PAPER_ACCOUNT_ID = "shadow_paper_10000"


class PaperBroker:
    def __init__(
        self,
        db_path: str | Path,
        *,
        execution_mode: str = "paper",
        account_id: str = DEFAULT_PAPER_ACCOUNT_ID,
        initial_capital: float = PAPER_INITIAL_CAPITAL,
        cost_schedule: CostSchedule | None = None,
    ):
        self.execution_mode = assert_safe_execution_mode(execution_mode)
        if float(initial_capital) != PAPER_INITIAL_CAPITAL:
            raise ValueError("PaperBroker is fixed to the current 10000 RMB account")
        self.db_path = Path(db_path)
        self.account_id = str(account_id)
        self.initial_capital = float(initial_capital)
        self.cost_schedule = cost_schedule or CostSchedule()

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)
            await conn.execute(
                """
                INSERT OR IGNORE INTO paper_accounts (
                    account_id, initial_capital, cash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (self.account_id, self.initial_capital, self.initial_capital, now, now),
            )
            async with conn.execute(
                "SELECT initial_capital FROM paper_accounts WHERE account_id = ?",
                (self.account_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None or float(row[0]) != self.initial_capital:
                raise ValueError("existing paper account capital does not match 10000")
            await conn.commit()

    async def next_trading_session(self, signal_at: str) -> str:
        await self.initialize()
        signal_day = str(signal_at or "")[:10]
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT trade_date
                FROM trading_calendar
                WHERE market = 'CN' AND is_open = 1
                  AND quality_status = 'validated' AND trade_date > ?
                ORDER BY trade_date LIMIT 1
                """,
                (signal_day,),
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("no validated next trading session is available")
        return str(row[0])[:10]

    async def submit_order(self, order: BrokerOrder) -> str:
        await self.initialize()
        ticker = normalize_ticker(order.ticker)
        profile = InstrumentProfile.from_ticker(ticker)
        side = str(order.side).strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("paper order side must be buy or sell")
        shares = int(order.shares)
        if shares <= 0 or shares % profile.lot_size:
            raise ValueError(f"paper order shares must be positive whole lots of {profile.lot_size}")
        signal_day = str(order.signal_at or "")[:10]
        earliest = str(order.earliest_fill_date or "")[:10]
        if not signal_day or not earliest or earliest <= signal_day:
            raise ValueError("earliest_fill_date must be a trading date after the signal date")
        order_type = str(order.order_type or "market").strip().lower()
        if order_type not in {"market", "limit"}:
            raise ValueError("paper order type must be market or limit")
        if order_type == "limit" and (order.limit_price is None or order.limit_price <= 0):
            raise ValueError("positive limit_price is required for limit orders")
        now = order.submitted_at or datetime.now().isoformat()
        order_id = new_id("paper_order")
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    "SELECT id FROM paper_orders WHERE idempotency_key = ?",
                    (order.idempotency_key,),
                ) as cursor:
                    duplicate = await cursor.fetchone()
                if duplicate:
                    await conn.rollback()
                    return str(duplicate[0])
                await conn.execute(
                    """
                    INSERT INTO paper_orders (
                        id, account_id, idempotency_key, ticker, side, shares,
                        order_type, limit_price, status, reason, signal_at,
                        earliest_fill_date, submitted_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id, self.account_id, order.idempotency_key, ticker,
                        side, shares, order_type, order.limit_price, order.reason,
                        order.signal_at, earliest, now, now,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return order_id

    async def execute_pending(
        self,
        session: str,
        opening_quotes: Mapping[str, BrokerQuote],
    ) -> tuple[BrokerFill, ...]:
        await self.initialize()
        session_day = str(session or "")[:10]
        quotes = {normalize_ticker(key): value for key, value in opening_quotes.items()}
        fills: list[BrokerFill] = []
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON")
            async with conn.execute(
                "SELECT is_open, quality_status FROM trading_calendar WHERE trade_date = ? AND market = 'CN'",
                (session_day,),
            ) as cursor:
                calendar = await cursor.fetchone()
            if calendar is None or int(calendar["is_open"]) != 1 or str(calendar["quality_status"]) != "validated":
                return ()
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    "SELECT cash FROM paper_accounts WHERE account_id = ?",
                    (self.account_id,),
                ) as cursor:
                    account_row = await cursor.fetchone()
                cash = float(account_row["cash"])
                async with conn.execute(
                    """
                    SELECT * FROM paper_orders
                    WHERE account_id = ? AND status = 'pending'
                      AND earliest_fill_date <= ? AND date(signal_at) < date(?)
                    ORDER BY submitted_at, id
                    """,
                    (self.account_id, session_day, session_day),
                ) as cursor:
                    orders = await cursor.fetchall()
                for row in orders:
                    ticker = str(row["ticker"])
                    quote = quotes.get(ticker)
                    if not self._usable_quote(quote):
                        await self._reject(conn, row["id"], "missing_or_untradable_open", session_day)
                        continue
                    assert quote is not None
                    if row["order_type"] == "limit" and not self._limit_crossed(
                        str(row["side"]), float(quote.price), float(row["limit_price"])
                    ):
                        continue
                    shares = int(row["shares"])
                    side = str(row["side"])
                    cost = self.cost_schedule.calculate(
                        ticker, side, price=float(quote.price), shares=shares
                    )
                    if side == "buy" and cost.notional + cost.total > cash + 1e-9:
                        await self._reject(conn, row["id"], "insufficient_cash", session_day)
                        continue
                    if side == "sell" and await self._available_shares(conn, ticker, session_day) < shares:
                        await self._reject(
                            conn, row["id"], "t_plus_one_or_insufficient_position", session_day
                        )
                        continue

                    fill_id = new_id("paper_fill")
                    filled_at = f"{session_day}T09:30:00"
                    cash_delta = (
                        -(cost.notional + cost.total)
                        if side == "buy" else cost.notional - cost.total
                    )
                    cash += cash_delta
                    if side == "buy":
                        await self._apply_buy(
                            conn, ticker, shares, (cost.notional + cost.total) / shares, session_day
                        )
                    else:
                        await self._apply_sell(conn, ticker, shares, session_day)
                    profile = InstrumentProfile.from_ticker(ticker)
                    await conn.execute(
                        """
                        INSERT INTO paper_fills (
                            id, account_id, order_id, ticker, side, shares,
                            price, notional, commission, stamp_duty,
                            slippage_cost, total_cost, asset_type,
                            cost_model_version, filled_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            fill_id, self.account_id, row["id"], ticker, side,
                            shares, quote.price, cost.notional, cost.commission,
                            cost.stamp_duty, cost.slippage, cost.total,
                            profile.instrument_type.value, cost.model_version, filled_at,
                        ),
                    )
                    await conn.execute(
                        """
                        INSERT INTO paper_ledger_entries (
                            id, account_id, order_id, fill_id, ticker,
                            share_delta, cash_delta, commission, stamp_duty,
                            slippage_cost, cost_model_version, entry_type, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'paper_fill', ?)
                        """,
                        (
                            new_id("paper_ledger"), self.account_id, row["id"],
                            fill_id, ticker, shares if side == "buy" else -shares,
                            cash_delta, cost.commission, cost.stamp_duty,
                            cost.slippage, cost.model_version, filled_at,
                        ),
                    )
                    await conn.execute(
                        "UPDATE paper_orders SET status = 'filled', updated_at = ? WHERE id = ?",
                        (filled_at, row["id"]),
                    )
                    fills.append(
                        BrokerFill(
                            fill_id=fill_id, order_id=str(row["id"]), ticker=ticker,
                            side=side, shares=shares, price=float(quote.price),
                            notional=cost.notional, total_cost=cost.total,
                            filled_at=filled_at, cost_model_version=cost.model_version,
                        )
                    )
                await conn.execute(
                    "UPDATE paper_accounts SET cash = ?, updated_at = ? WHERE account_id = ?",
                    (cash, datetime.now().isoformat(), self.account_id),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return tuple(fills)

    async def mark_to_market(
        self,
        session: str,
        closing_quotes: Mapping[str, BrokerQuote],
    ) -> dict[str, object]:
        await self.initialize()
        session_day = str(session or "")[:10]
        quotes = {normalize_ticker(key): value for key, value in closing_quotes.items()}
        stale: dict[str, int] = {}
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT ticker, shares, last_mark, last_mark_at FROM paper_positions WHERE account_id = ?",
                (self.account_id,),
            ) as cursor:
                positions = await cursor.fetchall()
            market_value = 0.0
            for position in positions:
                ticker = str(position["ticker"])
                quote = quotes.get(ticker)
                if self._usable_quote(quote):
                    assert quote is not None
                    mark = float(quote.price)
                    await conn.execute(
                        """
                        UPDATE paper_positions SET last_mark = ?, last_mark_at = ?, updated_at = ?
                        WHERE account_id = ? AND ticker = ?
                        """,
                        (mark, session_day, datetime.now().isoformat(), self.account_id, ticker),
                    )
                else:
                    mark = float(position["last_mark"] or 0.0)
                    last_day = str(position["last_mark_at"] or "")[:10]
                    stale[ticker] = 0 if not last_day else max(
                        0, (datetime.fromisoformat(session_day) - datetime.fromisoformat(last_day)).days
                    )
                market_value += int(position["shares"]) * mark
            async with conn.execute(
                "SELECT cash FROM paper_accounts WHERE account_id = ?",
                (self.account_id,),
            ) as cursor:
                cash_row = await cursor.fetchone()
            await conn.commit()
        cash = float(cash_row["cash"])
        return {
            "date": session_day, "cash": cash, "market_value": market_value,
            "total_assets": cash + market_value, "stale_days": stale,
            "usable": not stale,
        }

    async def account(self) -> BrokerAccount:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                "SELECT initial_capital, cash FROM paper_accounts WHERE account_id = ?",
                (self.account_id,),
            ) as cursor:
                row = await cursor.fetchone()
            async with conn.execute(
                "SELECT ticker, shares FROM paper_positions WHERE account_id = ? ORDER BY ticker",
                (self.account_id,),
            ) as cursor:
                positions = {str(item[0]): int(item[1]) for item in await cursor.fetchall()}
        return BrokerAccount(
            account_id=self.account_id, initial_capital=float(row[0]),
            cash=float(row[1]), positions=positions,
        )

    async def reconcile(self, snapshot_at: str) -> ReconciliationResult:
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    "SELECT initial_capital, cash FROM paper_accounts WHERE account_id = ?",
                    (self.account_id,),
                ) as cursor:
                    account = await cursor.fetchone()
                async with conn.execute(
                    "SELECT COALESCE(SUM(cash_delta), 0) FROM paper_ledger_entries WHERE account_id = ?",
                    (self.account_id,),
                ) as cursor:
                    ledger_cash = float((await cursor.fetchone())[0])
                expected_cash = float(account["initial_capital"]) + ledger_cash
                cash_difference = float(account["cash"]) - expected_cash
                async with conn.execute(
                    """
                    SELECT ticker, COALESCE(SUM(share_delta), 0)
                    FROM paper_ledger_entries
                    WHERE account_id = ? AND ticker IS NOT NULL GROUP BY ticker
                    """,
                    (self.account_id,),
                ) as cursor:
                    expected_positions = {str(row[0]): int(row[1]) for row in await cursor.fetchall()}
                async with conn.execute(
                    "SELECT ticker, shares, last_mark FROM paper_positions WHERE account_id = ?",
                    (self.account_id,),
                ) as cursor:
                    actual_rows = await cursor.fetchall()
                actual_positions = {str(row[0]): int(row[1]) for row in actual_rows}
                share_differences = {
                    ticker: actual_positions.get(ticker, 0) - expected_positions.get(ticker, 0)
                    for ticker in sorted(set(actual_positions) | set(expected_positions))
                    if actual_positions.get(ticker, 0) != expected_positions.get(ticker, 0)
                }
                status = "balanced" if abs(cash_difference) <= 1e-6 and not share_differences else "mismatch"
                snapshot_id = new_id("paper_snapshot")
                market_value = sum(
                    int(row["shares"]) * float(row["last_mark"] or 0.0) for row in actual_rows
                )
                details = {
                    "expected_cash": expected_cash, "actual_cash": float(account["cash"]),
                    "cash_difference": cash_difference,
                    "expected_positions": expected_positions,
                    "actual_positions": actual_positions,
                    "share_differences": share_differences,
                }
                await conn.execute(
                    """
                    INSERT INTO paper_account_snapshots (
                        id, account_id, cash, positions_json, total_assets,
                        reconciliation_status, reconciliation_json, snapshot_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id, self.account_id, account["cash"],
                        json.dumps(actual_positions, sort_keys=True),
                        float(account["cash"]) + market_value, status,
                        json.dumps(details, sort_keys=True), snapshot_at,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return ReconciliationResult(
            account_id=self.account_id, status=status,
            cash_difference=cash_difference, share_differences=share_differences,
            snapshot_id=snapshot_id,
        )

    async def _apply_buy(
        self, conn: aiosqlite.Connection, ticker: str, shares: int,
        unit_cost: float, session_day: str,
    ) -> None:
        now = datetime.now().isoformat()
        async with conn.execute(
            "SELECT shares, average_cost FROM paper_positions WHERE account_id = ? AND ticker = ?",
            (self.account_id, ticker),
        ) as cursor:
            row = await cursor.fetchone()
        old_shares = int(row[0]) if row else 0
        old_cost = float(row[1]) if row else 0.0
        new_shares = old_shares + shares
        average_cost = (old_shares * old_cost + shares * unit_cost) / new_shares
        await conn.execute(
            """
            INSERT INTO paper_positions (
                account_id, ticker, shares, average_cost, last_mark, last_mark_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, ticker) DO UPDATE SET
                shares = excluded.shares, average_cost = excluded.average_cost,
                last_mark = excluded.last_mark, last_mark_at = excluded.last_mark_at,
                updated_at = excluded.updated_at
            """,
            (self.account_id, ticker, new_shares, average_cost, unit_cost, session_day, now),
        )
        await conn.execute(
            """
            INSERT INTO paper_position_lots (
                id, account_id, ticker, acquired_at, shares, remaining_shares, unit_cost
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (new_id("paper_lot"), self.account_id, ticker, session_day, shares, shares, unit_cost),
        )

    async def _apply_sell(
        self, conn: aiosqlite.Connection, ticker: str, shares: int, session_day: str,
    ) -> None:
        remaining = shares
        async with conn.execute(
            """
            SELECT id, remaining_shares FROM paper_position_lots
            WHERE account_id = ? AND ticker = ? AND remaining_shares > 0 AND acquired_at < ?
            ORDER BY acquired_at, id
            """,
            (self.account_id, ticker, session_day),
        ) as cursor:
            lots = await cursor.fetchall()
        for lot_id, lot_shares in lots:
            used = min(remaining, int(lot_shares))
            await conn.execute(
                "UPDATE paper_position_lots SET remaining_shares = remaining_shares - ? WHERE id = ?",
                (used, lot_id),
            )
            remaining -= used
            if remaining <= 0:
                break
        async with conn.execute(
            """
            SELECT COALESCE(SUM(remaining_shares), 0),
                   COALESCE(SUM(remaining_shares * unit_cost), 0)
            FROM paper_position_lots
            WHERE account_id = ? AND ticker = ? AND remaining_shares > 0
            """,
            (self.account_id, ticker),
        ) as cursor:
            aggregate = await cursor.fetchone()
        shares_left = int(aggregate[0])
        if shares_left <= 0:
            await conn.execute(
                "DELETE FROM paper_positions WHERE account_id = ? AND ticker = ?",
                (self.account_id, ticker),
            )
        else:
            await conn.execute(
                """
                UPDATE paper_positions SET shares = ?, average_cost = ?, updated_at = ?
                WHERE account_id = ? AND ticker = ?
                """,
                (
                    shares_left, float(aggregate[1]) / shares_left,
                    datetime.now().isoformat(), self.account_id, ticker,
                ),
            )

    async def _available_shares(
        self, conn: aiosqlite.Connection, ticker: str, session_day: str,
    ) -> int:
        async with conn.execute(
            """
            SELECT COALESCE(SUM(remaining_shares), 0) FROM paper_position_lots
            WHERE account_id = ? AND ticker = ? AND remaining_shares > 0 AND acquired_at < ?
            """,
            (self.account_id, ticker, session_day),
        ) as cursor:
            row = await cursor.fetchone()
        return int(row[0] if row else 0)

    @staticmethod
    async def _reject(
        conn: aiosqlite.Connection, order_id: str, reason: str, session_day: str,
    ) -> None:
        await conn.execute(
            "UPDATE paper_orders SET status = 'rejected', reason = ?, updated_at = ? WHERE id = ?",
            (reason, f"{session_day}T09:30:00", order_id),
        )

    @staticmethod
    def _usable_quote(quote: BrokerQuote | None) -> bool:
        return bool(
            quote is not None and quote.tradable and float(quote.price) > 0
            and str(quote.provider).strip() and str(quote.fetched_at).strip()
        )

    @staticmethod
    def _limit_crossed(side: str, price: float, limit_price: float) -> bool:
        return price <= limit_price if side == "buy" else price >= limit_price

