"""
🏛️ Sovereign Hall - 决策追踪器
记录投资决策并追踪预测表现
"""
import uuid
import logging
import json
import math
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional
from dataclasses import dataclass, field

import aiosqlite

from ..domain.research import (
    HOLD_NEUTRAL_BAND as DEFAULT_HOLD_NEUTRAL_BAND,
    normalize_prediction_price_targets,
)
from .prediction_store import ensure_prediction_tables

logger = logging.getLogger(__name__)


@dataclass
class DecisionRecord:
    """决策记录"""
    id: str = ""
    ticker: str = ""
    decision: str = ""  # buy/sell/hold
    confidence: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    entry_price: float = 0.0
    entry_date: str = ""
    expected_days: int = 30
    discussion_context: str = ""
    validation_status: str = "pending"  # pending/validated/expired
    result: str = "unknown"  # correct/wrong/partial/unknown
    accuracy_score: float = 0.0
    validated_at: str = ""
    created_at: str = ""

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.entry_date:
            self.entry_date = datetime.now().isoformat()


class DecisionRecorder:
    """决策记录器 - 记录每次投票决策"""

    MIN_EXPECTED_DAYS = 3
    MAX_EXPECTED_DAYS = 180
    HOLD_NEUTRAL_BAND = DEFAULT_HOLD_NEUTRAL_BAND

    def __init__(self, db_path: str = None):
        from ..core import DATA_DIR
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")

    @classmethod
    def normalize_expected_days(cls, expected_days: int = None, context: str = "") -> int:
        """归一化预测验证窗口，允许模型动态决定但限制在可验证范围内。"""
        if expected_days is None:
            text = context or ""
            if any(word in text for word in ("半年", "6个月", "六个月", "中长期", "长线")):
                expected_days = 120
            elif any(word in text for word in ("季度", "三个月", "3个月", "中线")):
                expected_days = 90
            elif any(word in text for word in ("月内", "一个月", "1个月", "波段")):
                expected_days = 30
            elif any(word in text for word in ("短线", "催化", "事件驱动", "财报", "政策落地")):
                expected_days = 14
            else:
                expected_days = 30

        try:
            expected_days = int(float(expected_days))
        except (TypeError, ValueError):
            expected_days = 30

        return max(cls.MIN_EXPECTED_DAYS, min(cls.MAX_EXPECTED_DAYS, expected_days))

    async def _ensure_tables(self):
        """确保表结构存在"""
        await ensure_prediction_tables(self.db_path)

    async def record_decision(
        self,
        ticker: str,
        decision: str,
        confidence: float,
        target_price: float,
        stop_loss: float,
        entry_price: float = None,
        discussion_context: str = "",
        expected_days: int = 30,
        round_id: str | None = None,
        decision_id: str | None = None,
        defer_quote_until_execution: bool = False,
    ) -> str:
        """记录一次决策，必要时先保存无价格、待执行行情锚定的预测。"""
        await self._ensure_tables()

        from .market_data import get_market_data

        market = get_market_data()
        if entry_price is None and not defer_quote_until_execution:
            quote = await market.get_current_quote(ticker) or {}
            current_price = quote.get("price")
        elif entry_price is None:
            quote = {}
            current_price = None
        else:
            current_price = entry_price
            quote = {
                "ticker": ticker,
                "price": current_price,
                "source": "provided_prediction_anchor",
                "fetched_at": datetime.now().isoformat(),
            }
        if current_price is None or current_price <= 0:
            if not defer_quote_until_execution:
                raise ValueError(
                    f"无法获取 {ticker} 的真实入场价格，拒绝记录不可验证决策"
                )
            if not round_id or not decision_id:
                raise ValueError(
                    f"{ticker} 无行情预测必须带有round_id和decision_id，拒绝孤立记录"
                )
            prediction_id = await self._record_deferred_prediction(
                ticker=ticker,
                decision=decision,
                confidence=confidence,
                target_price=float(target_price or 0),
                stop_loss=float(stop_loss or 0),
                discussion_context=discussion_context,
                expected_days=expected_days,
                round_id=round_id,
                decision_id=decision_id,
            )
            logger.info(
                "决策预测已无价格持久化，等待执行时段行情锚定: %s %s %s",
                ticker,
                decision,
                prediction_id,
            )
            return prediction_id
        quote_source = str(quote.get("source") or "").strip()
        quote_fetched_at = str(quote.get("fetched_at") or "").strip()
        if not quote_source or not quote_fetched_at:
            raise ValueError(f"{ticker} 行情缺少source/fetched_at，拒绝记录不可审计决策")

        target_price, stop_loss = self._normalize_price_targets(
            decision=decision,
            entry_price=float(current_price),
            target_price=float(target_price or 0),
            stop_loss=float(stop_loss or 0),
        )
        expected_days = self.normalize_expected_days(expected_days, discussion_context)

        recent_id = None
        if not round_id:
            recent_id = await self._find_recent_duplicate(
                ticker=ticker,
                decision=decision,
                confidence=confidence,
                current_price=float(current_price),
                target_price=target_price,
                stop_loss=stop_loss,
                expected_days=expected_days,
            )
        if recent_id:
            logger.info(f"跳过重复决策: {ticker} {decision}，复用记录 {recent_id}")
            return recent_id

        record = DecisionRecord(
            ticker=ticker,
            decision=decision,
            confidence=confidence,
            target_price=target_price,
            stop_loss=stop_loss,
            entry_price=float(current_price),
            discussion_context=discussion_context[:1000],
            expected_days=expected_days,
        )

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO price_predictions (
                    id, ticker, current_price, target_price, stop_loss, direction,
                    confidence, predicted_at, expected_days,
                    discussion_context, status, result, accuracy_score,
                    created_at, entry_date, quote_source, quote_fetched_at,
                    round_id, decision_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id,
                record.ticker,
                record.entry_price,
                record.target_price,
                record.stop_loss,
                record.decision,
                record.confidence,
                record.created_at,
                record.expected_days,
                record.discussion_context,
                record.validation_status,
                record.result,
                record.accuracy_score,
                record.created_at,
                record.entry_date,
                quote_source,
                quote_fetched_at,
                round_id,
                decision_id,
            ))
            await db.commit()

        logger.info(f"决策已记录: {ticker} {decision} 置信度{confidence:.0%}")
        return record.id

    async def _find_recent_duplicate(
        self,
        ticker: str,
        decision: str,
        confidence: float,
        current_price: float,
        target_price: float,
        stop_loss: float,
        expected_days: int,
        hours: int = 24,
    ) -> Optional[str]:
        """Return a recent near-identical pending prediction id, if one exists."""
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT id
                FROM price_predictions
                WHERE ticker = ?
                  AND direction = ?
                  AND expected_days = ?
                  AND status = 'pending'
                  AND predicted_at >= ?
                  AND ABS(COALESCE(current_price, 0) - ?) <= MAX(0.01, ? * 0.005)
                  AND ABS(COALESCE(target_price, 0) - ?) <= MAX(0.01, ? * 0.01)
                  AND ABS(COALESCE(stop_loss, 0) - ?) <= MAX(0.01, ? * 0.01)
                  AND ABS(COALESCE(confidence, 0) - ?) <= 0.05
                ORDER BY predicted_at DESC
                LIMIT 1
                """,
                (
                    ticker,
                    decision,
                    expected_days,
                    cutoff,
                    current_price,
                    current_price,
                    target_price,
                    target_price,
                    stop_loss,
                    stop_loss,
                    confidence,
                ),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    async def _record_deferred_prediction(
        self,
        *,
        ticker: str,
        decision: str,
        confidence: float,
        target_price: float,
        stop_loss: float,
        discussion_context: str,
        expected_days: int,
        round_id: str,
        decision_id: str,
    ) -> str:
        """Persist prediction lineage without inventing a closed-market price."""
        expected_days = self.normalize_expected_days(
            expected_days,
            discussion_context,
        )
        now = datetime.now().isoformat()
        prediction_id = str(uuid.uuid4())
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT id
                FROM price_predictions
                WHERE round_id = ? AND decision_id = ?
                ORDER BY rowid
                LIMIT 1
                """,
                (round_id, decision_id),
            ) as cursor:
                existing = await cursor.fetchone()
            if existing:
                return str(existing[0])
            await db.execute(
                """
                INSERT INTO price_predictions (
                    id, ticker, current_price, target_price, stop_loss, direction,
                    confidence, predicted_at, expected_days,
                    discussion_context, status, result, accuracy_score,
                    created_at, entry_date, quote_source, quote_fetched_at,
                    round_id, decision_id
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?,
                          'awaiting_entry_quote', 'unknown', 0.0, ?, NULL,
                          NULL, NULL, ?, ?)
                """,
                (
                    prediction_id,
                    ticker,
                    target_price,
                    stop_loss,
                    decision,
                    confidence,
                    now,
                    expected_days,
                    discussion_context[:1000],
                    now,
                    round_id,
                    decision_id,
                ),
            )
            await db.commit()
        return prediction_id

    async def recover_deferred_prediction_lineage(
        self,
        *,
        limit: int = 100,
    ) -> Dict[str, object]:
        """Recover only exact, still-pending committee prediction lineage.

        Historical rows with ambiguous proposal or committee associations are
        intentionally untouched. The recovered row remains price-free until a
        fresh execution quote anchors it inside the atomic execution unit.
        """
        await self._ensure_tables()
        summary: Dict[str, object] = {
            "status": "completed",
            "eligible": 0,
            "recovered": 0,
            "prediction_ids": [],
        }
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            try:
                await db.execute("BEGIN IMMEDIATE")
                async with db.execute(
                    """
                    SELECT intent.id AS intent_id, intent.round_id,
                           intent.decision_id, intent.ticker,
                           outcome.id AS outcome_id,
                           outcome.direction, outcome.confidence,
                           outcome.created_at AS predicted_at,
                           proposal.take_profit, proposal.stop_loss,
                           proposal.holding_period, proposal.thesis
                    FROM execution_intents intent
                    JOIN simulation_pending_decisions pending
                      ON pending.intent_id = intent.id
                     AND pending.status = 'pending_next_trading_session'
                    JOIN simulation_committee_outcomes outcome
                      ON outcome.round_id = intent.round_id
                     AND outcome.decision_id = intent.decision_id
                     AND outcome.ticker = intent.ticker
                    JOIN proposals proposal
                      ON proposal.round_id = intent.round_id
                     AND proposal.ticker = intent.ticker
                    LEFT JOIN price_predictions prediction
                      ON prediction.round_id = intent.round_id
                     AND prediction.decision_id = intent.decision_id
                    WHERE intent.status IN ('pending', 'deferred')
                      AND intent.round_id IS NOT NULL
                      AND intent.decision_id IS NOT NULL
                      AND prediction.id IS NULL
                      AND trim(COALESCE(outcome.prediction_id, '')) = ''
                      AND (
                          SELECT COUNT(*) FROM proposals candidate
                          WHERE candidate.round_id = intent.round_id
                            AND candidate.ticker = intent.ticker
                      ) = 1
                      AND (
                          SELECT COUNT(*)
                          FROM simulation_committee_outcomes candidate_outcome
                          WHERE candidate_outcome.round_id = intent.round_id
                            AND candidate_outcome.decision_id = intent.decision_id
                            AND candidate_outcome.ticker = intent.ticker
                      ) = 1
                    ORDER BY pending.id
                    LIMIT ?
                    """,
                    (max(0, int(limit)),),
                ) as cursor:
                    rows = await cursor.fetchall()
                summary["eligible"] = len(rows)
                prediction_ids: List[str] = []
                for row in rows:
                    now = datetime.now().isoformat()
                    prediction_id = str(uuid.uuid4())
                    expected_days = self.normalize_expected_days(
                        row["holding_period"],
                        str(row["thesis"] or ""),
                    )
                    await db.execute(
                        """
                        INSERT INTO price_predictions (
                            id, ticker, current_price, target_price, stop_loss,
                            direction, confidence, predicted_at, expected_days,
                            discussion_context, status, result, accuracy_score,
                            created_at, entry_date, quote_source,
                            quote_fetched_at, round_id, decision_id
                        ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?,
                                  'awaiting_entry_quote', 'unknown', 0.0, ?,
                                  NULL, NULL, NULL, ?, ?)
                        """,
                        (
                            prediction_id,
                            row["ticker"],
                            float(row["take_profit"] or 0.0),
                            float(row["stop_loss"] or 0.0),
                            row["direction"],
                            float(row["confidence"] or 0.0),
                            row["predicted_at"] or now,
                            expected_days,
                            (
                                f"lineage_recovered_for_pending_intent="
                                f"{row['intent_id']}; "
                                f"{str(row['thesis'] or '')[:850]}"
                            ),
                            now,
                            row["round_id"],
                            row["decision_id"],
                        ),
                    )
                    await db.execute(
                        """
                        UPDATE simulation_committee_outcomes
                        SET prediction_id = ?
                        WHERE id = ?
                          AND trim(COALESCE(prediction_id, '')) = ''
                        """,
                        (prediction_id, int(row["outcome_id"])),
                    )
                    async with db.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM round_events WHERE round_id = ?",
                        (row["round_id"],),
                    ) as event_cursor:
                        sequence_row = await event_cursor.fetchone()
                    await db.execute(
                        """
                        INSERT INTO round_events (
                            round_id, sequence, event_type,
                            payload_json, created_at
                        ) VALUES (?, ?, 'PredictionLineageRecovered', ?, ?)
                        """,
                        (
                            row["round_id"],
                            int(sequence_row[0] if sequence_row else 1),
                            json.dumps(
                                {
                                    "intent_id": row["intent_id"],
                                    "prediction_id": prediction_id,
                                    "price_deferred": True,
                                    "ticker": row["ticker"],
                                },
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            now,
                        ),
                    )
                    prediction_ids.append(prediction_id)
                await db.commit()
                summary["recovered"] = len(prediction_ids)
                summary["prediction_ids"] = prediction_ids
            except aiosqlite.OperationalError as exc:
                await db.rollback()
                if "no such table" in str(exc).lower():
                    summary["status"] = "schema_unavailable"
                    return summary
                raise
            except Exception:
                await db.rollback()
                raise
        return summary

    async def reconcile_deferred_prediction_terminals(
        self,
        *,
        limit: int = 100,
    ) -> Dict[str, object]:
        """Close only exact deferred predictions whose execution is terminal.

        Two forward-only legacy shapes are safe to reconcile: an exact linked
        intent already has a terminal non-fill status, or the unique linked
        committee outcome belongs to a round already terminally marked
        ``execution_rejected`` and no intent was ever written.  The latter is
        recorded explicitly as a legacy missing-intent defect; no intent,
        quote, price, fill or historical association is fabricated.
        """
        await self._ensure_tables()
        summary: Dict[str, object] = {
            "status": "completed",
            "eligible": 0,
            "reconciled": 0,
            "prediction_ids": [],
        }
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            try:
                await db.execute("BEGIN IMMEDIATE")
                async with db.execute(
                    """
                    SELECT prediction.id AS prediction_id,
                           prediction.ticker, prediction.round_id,
                           prediction.decision_id,
                           intent.id AS intent_id,
                           intent.status AS intent_status,
                           round.terminal_code AS round_terminal_code
                    FROM price_predictions prediction
                    JOIN simulation_committee_outcomes outcome
                      ON outcome.prediction_id = prediction.id
                     AND outcome.round_id = prediction.round_id
                     AND outcome.decision_id = prediction.decision_id
                     AND outcome.ticker = prediction.ticker
                    JOIN research_rounds round
                      ON round.id = prediction.round_id
                    LEFT JOIN execution_intents intent
                      ON intent.round_id = prediction.round_id
                     AND intent.decision_id = prediction.decision_id
                     AND intent.ticker = prediction.ticker
                    WHERE prediction.status = 'awaiting_entry_quote'
                      AND (
                          intent.status IN ('rejected', 'expired', 'cancelled')
                          OR (
                              intent.id IS NULL
                              AND round.terminal_code = 'execution_rejected'
                          )
                      )
                      AND (
                          SELECT COUNT(*)
                          FROM simulation_committee_outcomes exact_outcome
                          WHERE exact_outcome.prediction_id = prediction.id
                            AND exact_outcome.round_id = prediction.round_id
                            AND exact_outcome.decision_id = prediction.decision_id
                            AND exact_outcome.ticker = prediction.ticker
                      ) = 1
                      AND (
                          SELECT COUNT(*)
                          FROM execution_intents exact_intent
                          WHERE exact_intent.round_id = prediction.round_id
                            AND exact_intent.decision_id = prediction.decision_id
                            AND exact_intent.ticker = prediction.ticker
                      ) <= 1
                    ORDER BY prediction.rowid
                    LIMIT ?
                    """,
                    (max(0, int(limit)),),
                ) as cursor:
                    rows = await cursor.fetchall()
                summary["eligible"] = len(rows)
                reconciled_ids: List[str] = []
                for row in rows:
                    now = datetime.now().isoformat()
                    cursor = await db.execute(
                        """
                        UPDATE price_predictions
                        SET status = 'execution_rejected', result = 'unknown',
                            validated_at = NULL
                        WHERE id = ? AND status = 'awaiting_entry_quote'
                        """,
                        (row["prediction_id"],),
                    )
                    if not cursor.rowcount:
                        continue
                    previous_terminal = str(row["round_terminal_code"] or "")
                    if row["intent_id"] and previous_terminal not in {
                        "filled",
                        "failed",
                        "pipeline_exception",
                        "cancelled",
                        "interrupted",
                    }:
                        await db.execute(
                            """
                            UPDATE research_rounds
                            SET terminal_code = 'execution_rejected',
                                terminal_reason = ?, updated_at = ?
                            WHERE id = ?
                              AND status <> 'failed'
                              AND COALESCE(terminal_code, '') NOT IN (
                                  'filled', 'failed', 'pipeline_exception',
                                  'cancelled', 'interrupted'
                              )
                            """,
                            (
                                (
                                    "精确关联的执行裁决已终止且没有可审计入场价；"
                                    f"intent_id={row['intent_id']}"
                                ),
                                now,
                                row["round_id"],
                            ),
                        )
                    async with db.execute(
                        "SELECT COALESCE(MAX(sequence), 0) + 1 "
                        "FROM round_events WHERE round_id = ?",
                        (row["round_id"],),
                    ) as event_cursor:
                        sequence_row = await event_cursor.fetchone()
                    await db.execute(
                        """
                        INSERT INTO round_events (
                            round_id, sequence, event_type,
                            payload_json, created_at
                        ) VALUES (?, ?, 'DeferredPredictionTerminalReconciled', ?, ?)
                        """,
                        (
                            row["round_id"],
                            int(sequence_row[0] if sequence_row else 1),
                            json.dumps(
                                {
                                    "intent_id": row["intent_id"],
                                    "intent_status": row["intent_status"],
                                    "legacy_missing_intent": not bool(
                                        row["intent_id"]
                                    ),
                                    "prediction_id": row["prediction_id"],
                                    "previous_round_terminal_code": (
                                        previous_terminal or None
                                    ),
                                    "price_fabricated": False,
                                    "ticker": row["ticker"],
                                },
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                            now,
                        ),
                    )
                    reconciled_ids.append(str(row["prediction_id"]))
                await db.commit()
                summary["reconciled"] = len(reconciled_ids)
                summary["prediction_ids"] = reconciled_ids
            except aiosqlite.OperationalError as exc:
                await db.rollback()
                if "no such table" in str(exc).lower():
                    summary["status"] = "schema_unavailable"
                    return summary
                raise
            except Exception:
                await db.rollback()
                raise
        return summary

    def _normalize_price_targets(
        self,
        decision: str,
        entry_price: float,
        target_price: float,
        stop_loss: float,
    ) -> tuple[float, float]:
        """Convert percent-style targets into absolute prices when needed."""
        return normalize_prediction_price_targets(
            decision=decision,
            entry_price=entry_price,
            target_price=target_price,
            stop_loss=stop_loss,
        )

    async def get_pending_decisions(self, limit: int = 100) -> List[Dict]:
        """获取待验证的决策列表"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE status = 'pending'
                ORDER BY predicted_at ASC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_unvalidated_decisions(self, days: int = 7) -> List[Dict]:
        """获取N天前尚未验证的决策"""
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE status = 'pending'
                AND predicted_at < ?
                ORDER BY predicted_at ASC
            """, (cutoff_date,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_decision_by_ticker(self, ticker: str, limit: int = 10) -> List[Dict]:
        """获取某股票的历史决策"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE ticker = ?
                ORDER BY predicted_at DESC
                LIMIT ?
            """, (ticker, limit)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_recent_decisions(self, limit: int = 20) -> List[Dict]:
        """获取最近的决策"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                ORDER BY predicted_at DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def count_decisions(self) -> int:
        """统计总决策数"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM price_predictions") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def calculate_accuracy_stats(self) -> Dict:
        """计算整体准确率统计"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as correct,
                    SUM(CASE WHEN result = 'partial' THEN 1 ELSE 0 END) as partial,
                    SUM(CASE WHEN result = 'wrong' THEN 1 ELSE 0 END) as wrong,
                    AVG(accuracy_score) as avg_accuracy
                FROM price_predictions
                WHERE status = 'validated'
                AND result IN ('correct', 'partial', 'wrong')
            """) as cursor:
                row = await cursor.fetchone()
                if not row or row[0] == 0:
                    return {
                        "total": 0,
                        "correct": 0,
                        "partial": 0,
                        "wrong": 0,
                        "accuracy": 0.0,
                        "avg_accuracy": 0.0,
                    }

                total, correct, partial, wrong, avg_accuracy = row
                accuracy = (correct + partial * 0.5) / total if total > 0 else 0

                return {
                    "total": total,
                    "correct": correct or 0,
                    "partial": partial or 0,
                    "wrong": wrong or 0,
                    "accuracy": accuracy,
                    "avg_accuracy": avg_accuracy or 0,
                }

    async def _fetch_price(self, ticker: str) -> Optional[float]:
        """获取当前股价（腾讯API + 东方财富备用）"""
        import requests

        # 尝试两个市场（沪市和深市）
        markets = ["sh", "sz"]

        for market in markets:
            try:
                url = f"http://qt.gtimg.cn/q={market}{ticker}"
                resp = requests.get(url, timeout=8)
                if resp.status_code == 200 and "none_match" not in resp.text:
                    # 格式: v_sh600519="1~贵州茅台~600519~1458.49~1419.00~...
                    # 价格是第4个字段（索引3）
                    parts = resp.text.split('~')
                    if len(parts) > 3 and parts[3]:
                        try:
                            return float(parts[3])
                        except ValueError as exc:
                            logger.debug("腾讯行情价格解析失败 %s: %s", ticker, exc)
            except Exception as e:
                logger.debug("腾讯行情请求失败 %s/%s: %s", market, ticker, e)
                continue

        # 东方财富备用（不稳定但偶尔可用）
        try:
            url = f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f1,f2,f12,f14&secids=1.{ticker}" if ticker.startswith("6") else f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f1,f2,f12,f14&secids=0.{ticker}"
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                diff = data.get("data", {}).get("diff", [])
                if diff and diff[0].get("f2"):
                    return float(diff[0]["f2"])
        except Exception as e:
            logger.debug(f"东方财富API获取{ticker}失败: {e}")

        logger.warning(f"获取{ticker}价格失败")
        return None

    async def validate_single(self, record_id: str) -> Dict:
        """验证单个决策"""
        await self._ensure_tables()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM price_predictions WHERE id = ?", (record_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {"error": "记录不存在"}
                record = dict(row)

        from .market_data import get_market_data

        market = get_market_data()
        if record['status'] != 'pending':
            return {"result": record['result'], "accuracy": record['accuracy_score'],
                    "actual_return": record.get('actual_return'), "unchanged": True}

        def local_time(value):
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
            return parsed

        try:
            # Execution may anchor a prediction later than its committee decision.
            window_start = max(local_time(record['predicted_at']),
                               local_time(record.get('entry_date') or record['predicted_at']))
            window_end = window_start + timedelta(days=int(record.get('expected_days') or 30))
            if window_end <= window_start:
                raise ValueError("invalid horizon")
        except (ValueError, TypeError):
            return {"error": "invalid_prediction_window"}
        if datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None) < window_end:
            return {"error": "prediction_window_not_matured"}

        decision = record['direction']
        target = record['target_price']
        stop = record['stop_loss']
        entry = record.get('current_price')
        try:
            entry, target, stop = float(entry), float(target), float(stop)
            if not all(math.isfinite(value) and value > 0 for value in (entry, target, stop)):
                raise ValueError("invalid anchor or barriers")
        except (ValueError, TypeError):
            return {"error": "invalid_prediction_anchor"}

        result = "unknown"
        accuracy = 0.0
        hit_price = None
        hit_date = None
        hit_type = None
        max_price = None
        min_price = None

        raw_bars = await market.get_ohlc(record['ticker'], window_start, window_end)
        if not raw_bars:
            return {"error": "prediction_window_data_unavailable"}
        calendar = await market.get_trading_calendar(window_start, window_end)
        if not calendar:
            return {"error": "prediction_window_calendar_unavailable"}
        # Daily bars cannot resolve which part of an intraday boundary crossed a
        # barrier. Full pre-entry/post-expiry sessions must never enter feedback.
        bars = []
        ambiguous_start = False
        ambiguous_end = False
        try:
            for bar in sorted(raw_bars, key=lambda item: item['date']):
                day = datetime.fromisoformat(str(bar['date'])[:10]).date()
                opened = datetime.combine(day, time(9, 30))
                closed = datetime.combine(day, time(15))
                if closed <= window_start or opened >= window_end:
                    continue
                if opened < window_start:
                    ambiguous_start |= (float(bar['high']) >= max(target, stop)
                                        or float(bar['low']) <= min(target, stop))
                    continue
                if closed > window_end:
                    ambiguous_end = True
                    continue
                if not all(math.isfinite(float(bar[key])) and float(bar[key]) > 0 for key in ('open', 'high', 'low', 'close')):
                    return {"error": "invalid_prediction_window_bar"}
                bars.append(bar)
        except (ValueError, TypeError, KeyError):
            return {"error": "invalid_prediction_window_bar"}
        entry_session_open = any(
            str(item['trade_date'])[:10] == window_start.date().isoformat()
            and item.get('is_open') and time(9, 30) < window_start.time() < time(15)
            for item in calendar
        )
        if entry_session_open and not any(
            str(bar['date'])[:10] == window_start.date().isoformat()
            for bar in raw_bars
        ):
            return {"error": "prediction_entry_session_incomplete"}
        if ambiguous_start:
            return {"error": "prediction_entry_session_ambiguous"}
        if not bars:
            return {"error": "prediction_window_data_unavailable"}
        current_price = float(bars[-1]['close'])  # bounded endpoint, never today's quote
        hit_price = current_price
        if bars:
            max_price = max(bar["high"] for bar in bars)
            min_price = min(bar["low"] for bar in bars)
            for bar in bars:
                if decision in ("buy", "long"):
                    if bar["low"] <= stop:
                        result = "wrong"
                        accuracy = 0.0
                        hit_price = stop
                        hit_date = bar["date"]
                        hit_type = "stop_loss"
                        break
                    if bar["high"] >= target:
                        result = "correct"
                        accuracy = 1.0
                        hit_price = target
                        hit_date = bar["date"]
                        hit_type = "target"
                        break
                elif decision in ("sell", "short"):
                    if bar["high"] >= stop:
                        result = "wrong"
                        accuracy = 0.0
                        hit_price = stop
                        hit_date = bar["date"]
                        hit_type = "stop_loss"
                        break
                    if bar["low"] <= target:
                        result = "correct"
                        accuracy = 1.0
                        hit_price = target
                        hit_date = bar["date"]
                        hit_type = "target"
                        break
                elif decision in ("hold", "neutral", "watch"):
                    crossed_up = bar["high"] >= target
                    crossed_down = bar["low"] <= stop
                    if crossed_up and crossed_down:
                        result = "partial"
                        accuracy = 0.5
                        hit_price = current_price
                        hit_date = bar["date"]
                        hit_price = float(bar["close"])
                        hit_type = "neutral_band_both_sides"
                        break
                    if crossed_up:
                        result = "wrong"
                        accuracy = 0.0
                        hit_price = target
                        hit_date = bar["date"]
                        hit_type = "missed_upside"
                        break
                    if crossed_down:
                        result = "correct"
                        accuracy = 1.0
                        hit_price = stop
                        hit_date = bar["date"]
                        hit_type = "avoided_downside"
                        break

        evidence_end = hit_date or bars[-1]['date']
        expected_dates = {
            str(item['trade_date'])[:10] for item in calendar
            if item.get('is_open') and
            datetime.combine(datetime.fromisoformat(str(item['trade_date'])[:10]).date(), time(9, 30)) >= window_start and
            datetime.combine(datetime.fromisoformat(str(item['trade_date'])[:10]).date(), time(15)) <= window_end
        }
        if not expected_dates or not {day for day in expected_dates if day <= evidence_end}.issubset(
                {str(bar['date'])[:10] for bar in bars}):
            return {"error": "prediction_window_incomplete"}
        ambiguous_end |= any(
            str(item['trade_date'])[:10] == window_end.date().isoformat()
            and item.get('is_open') and time(9, 30) < window_end.time() < time(15)
            for item in calendar
        )
        if hit_date is None and (ambiguous_end or bars[-1]['date'] != max(expected_dates)):
            return {"error": "prediction_expiry_evidence_incomplete"}
        validation_window = {
            "version": "feedback_session_window_v2",
            "start": window_start.isoformat(), "end": window_end.isoformat(),
            "endpoint_date": evidence_end,
            "bars": [bar for bar in bars if bar['date'] <= evidence_end],
            "calendar": calendar,
        }

        if result == "unknown" and decision in ("buy", "long"):
            if current_price >= target:
                result = "correct"
                accuracy = 1.0
            elif current_price <= stop:
                result = "wrong"
                accuracy = 0.0
            elif current_price > entry * 1.02:
                result = "partial"
                accuracy = 0.5
            else:
                profit_pct = (current_price - entry) / entry
                accuracy = max(0.0, min(1.0, profit_pct / 0.1))

        # 卖出/做空
        elif result == "unknown" and decision in ("sell", "short"):
            if current_price <= target:
                result = "correct"
                accuracy = 1.0
            elif current_price >= stop:
                result = "wrong"
                accuracy = 0.0
            else:
                profit_pct = (entry - current_price) / entry
                accuracy = max(0.0, min(1.0, profit_pct / 0.1))
        elif result == "unknown" and decision in ("hold", "neutral", "watch"):
            price_change = (current_price - entry) / entry
            if price_change >= self.HOLD_NEUTRAL_BAND:
                result = "wrong"
                accuracy = 0.0
                hit_type = "missed_upside"
            elif price_change <= -self.HOLD_NEUTRAL_BAND:
                result = "correct"
                accuracy = 1.0
                hit_type = "avoided_downside"
            else:
                result = "correct"
                accuracy = 1.0
                hit_type = "neutral_band_held"

        actual_return = (
            (float(hit_price) - float(entry)) / float(entry)
            if entry and hit_price is not None
            else None
        )

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA busy_timeout = 5000")
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute("""
                UPDATE price_predictions
                SET status = 'validated',
                    result = ?,
                    accuracy_score = ?,
                    validated_at = ?,
                    actual_hit_price = ?,
                    actual_hit_date = ?,
                    actual_hit_type = ?,
                    max_price_reached = ?,
                    min_price_reached = ?,
                    actual_return = ?
                WHERE id = ? AND status = 'pending'
            """, (
                result,
                accuracy,
                datetime.now().isoformat(),
                hit_price,
                hit_date or evidence_end,
                hit_type,
                max_price,
                min_price,
                actual_return,
                record_id,
            ))
            if not cursor.rowcount:
                async with db.execute("SELECT result, accuracy_score, actual_return FROM price_predictions WHERE id = ?", (record_id,)) as existing:
                    prior = await existing.fetchone()
                await db.rollback()
                return {"result": prior[0], "accuracy": prior[1], "actual_return": prior[2], "unchanged": True}
            if record.get('round_id'):
                async with db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='round_events'") as probe:
                    has_events = await probe.fetchone()
                if has_events:
                    async with db.execute("SELECT COALESCE(MAX(sequence), 0) + 1 FROM round_events WHERE round_id = ?", (record['round_id'],)) as seq:
                        sequence = (await seq.fetchone())[0]
                    await db.execute(
                        "INSERT INTO round_events(round_id, sequence, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        (record['round_id'], sequence, 'PredictionValidationWindowApplied',
                         json.dumps({"prediction_id": record_id, "result": result, **validation_window}, ensure_ascii=False),
                         datetime.now().isoformat()),
                    )
            await db.commit()

        logger.info("决策验证: %s %s window=%s", record['ticker'], result, json.dumps(validation_window, ensure_ascii=False))
        return {
            "result": result,
            "accuracy": accuracy,
            "current_price": current_price,
            "actual_return": actual_return,
            "validation_window": validation_window,
        }

    async def validate_pending(self, max_count: int = 50) -> Dict:
        """批量验证待验证的决策"""
        # 只验证已到预期窗口的 pending 记录，避免新预测被当天价格污染。
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT id, predicted_at, expected_days FROM price_predictions
                WHERE status = 'pending'
                AND datetime(MAX(predicted_at, COALESCE(entry_date, predicted_at)), '+' || COALESCE(expected_days, 30) || ' days') <= datetime('now', 'localtime')
                ORDER BY predicted_at ASC
                LIMIT ?
            """, (max_count,)) as cursor:
                candidates = await cursor.fetchall()

        results = []
        for row in candidates:
            record_id = row['id']
            result = await self.validate_single(record_id)
            results.append(result)

        terminal_results = {"correct", "partial", "wrong"}
        validated = sum(1 for r in results if r.get("result") in terminal_results)
        correct = sum(1 for r in results if r.get("result") == "correct")
        failed = sum(1 for r in results if r.get("error") or r.get("result") not in terminal_results)

        logger.info(
            "批量验证完成: 尝试%s条，成功%s条，失败/不可判定%s条",
            len(results),
            validated,
            failed,
        )
        return {
            "attempted": len(results),
            "validated": validated,
            "failed": failed,
            "correct": correct,
            "results": results,
        }


_recorder: Optional[DecisionRecorder] = None


def get_recorder(db_path: str = None) -> DecisionRecorder:
    global _recorder
    if _recorder is None:
        _recorder = DecisionRecorder(db_path)
    return _recorder
