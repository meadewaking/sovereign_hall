"""
🏛️ Sovereign Hall - 决策追踪器
记录投资决策并追踪预测表现
"""
import uuid
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, field

import aiosqlite

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

    def __init__(self, db_path: str = None):
        from ..core import DATA_DIR
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")

    async def _ensure_tables(self):
        """确保表结构存在"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS price_predictions (
                    id TEXT PRIMARY KEY,
                    conclusion_id TEXT,
                    ticker TEXT NOT NULL,
                    current_price REAL,
                    target_price REAL,
                    stop_loss REAL,
                    direction TEXT,
                    confidence REAL,
                    predicted_at TEXT,
                    expected_days INTEGER,
                    actual_hit_price REAL,
                    actual_hit_date TEXT,
                    actual_hit_type TEXT,
                    max_price_reached REAL,
                    min_price_reached REAL,
                    status TEXT DEFAULT 'pending',
                    result TEXT DEFAULT 'unknown',
                    accuracy_score REAL,
                    created_at TEXT,
                    validated_at TEXT,
                    entry_date TEXT,
                    discussion_context TEXT,
                    FOREIGN KEY (conclusion_id) REFERENCES report_conclusions(id)
                )
            """)
            await db.commit()

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
    ) -> str:
        """记录一次决策"""
        await self._ensure_tables()

        from .market_data import get_market_data

        market = get_market_data()
        current_price = entry_price or await market.get_current_price(ticker)
        if current_price is None or current_price <= 0:
            raise ValueError(f"无法获取 {ticker} 的真实入场价格，拒绝记录不可验证决策")

        target_price, stop_loss = self._normalize_price_targets(
            decision=decision,
            entry_price=float(current_price),
            target_price=float(target_price or 0),
            stop_loss=float(stop_loss or 0),
        )

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
                    created_at, entry_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ))
            await db.commit()

        logger.info(f"决策已记录: {ticker} {decision} 置信度{confidence:.0%}")
        return record.id

    def _normalize_price_targets(
        self,
        decision: str,
        entry_price: float,
        target_price: float,
        stop_loss: float,
    ) -> tuple[float, float]:
        """Convert percent-style targets into absolute prices when needed."""
        direction = (decision or "").lower()

        # Stage-2 proposals often emit take_profit=15.0 and stop_loss=5.0 to
        # mean +15% / -5%. Detect that paired shape before treating values as
        # absolute prices.
        if (
            target_price > 1
            and stop_loss > 1
            and target_price <= 100
            and stop_loss <= 100
            and (
                (direction in ("sell", "short") and target_price < entry_price < stop_loss)
                or (direction not in ("sell", "short") and stop_loss < entry_price < target_price)
            )
            and (abs(target_price - entry_price) / entry_price > 0.3 or abs(entry_price - stop_loss) / entry_price > 0.3)
        ):
            if direction in ("sell", "short"):
                return round(entry_price * (1 - target_price / 100), 4), round(entry_price * (1 + stop_loss / 100), 4)
            return round(entry_price * (1 + target_price / 100), 4), round(entry_price * (1 - stop_loss / 100), 4)

        def as_take_profit(value: float) -> float:
            if value <= 0:
                return entry_price * (0.92 if direction in ("sell", "short") else 1.08)
            if value <= 1:
                return entry_price * (1 - value if direction in ("sell", "short") else 1 + value)
            # LLM proposals often use 15.0 to mean +15%. If the target is on the
            # wrong side of entry, treat it as a percentage.
            if direction in ("sell", "short") and value >= entry_price:
                return entry_price * (1 - value / 100)
            if direction not in ("sell", "short") and value <= entry_price:
                return entry_price * (1 + value / 100)
            if entry_price < 10 and value > entry_price * 3 and value <= 100:
                return entry_price * (1 - value / 100 if direction in ("sell", "short") else 1 + value / 100)
            return value

        def as_stop(value: float) -> float:
            if value <= 0:
                return entry_price * (1.05 if direction in ("sell", "short") else 0.95)
            if value <= 1:
                return entry_price * (1 + value if direction in ("sell", "short") else 1 - value)
            if direction in ("sell", "short") and value <= entry_price:
                return entry_price * (1 + value / 100)
            if direction not in ("sell", "short") and value >= entry_price:
                return entry_price * (1 - value / 100)
            # For low-priced ETFs, values like 5.0 are almost certainly percent.
            if entry_price < 10 and value > entry_price:
                return entry_price * (1.05 if direction in ("sell", "short") else 0.95)
            return value

        return round(as_take_profit(target_price), 4), round(as_stop(stop_loss), 4)

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
                        except ValueError:
                            pass
            except Exception as e:
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
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM price_predictions WHERE id = ?", (record_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {"error": "记录不存在"}
                record = dict(row)

        from .market_data import get_market_data

        market = get_market_data()
        predicted_at = datetime.fromisoformat(record['predicted_at'])
        current_price = await market.get_current_price(record['ticker'])
        if current_price is None:
            return {"error": "无法获取当前价格"}

        decision = record['direction']
        target = record['target_price']
        stop = record['stop_loss']
        entry = record.get('entry_price', record.get('current_price', target * 0.95))

        result = "unknown"
        accuracy = 0.0
        hit_price = current_price
        hit_date = None
        hit_type = None
        max_price = None
        min_price = None

        bars = await market.get_ohlc(record['ticker'], predicted_at, datetime.now())
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

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE price_predictions
                SET status = 'validated',
                    result = ?,
                    accuracy_score = ?,
                    validated_at = ?,
                    actual_hit_price = ?,
                    actual_hit_date = ?,
                    actual_hit_type = ?,
                    max_price_reached = ?,
                    min_price_reached = ?
                WHERE id = ?
            """, (
                result,
                accuracy,
                datetime.now().isoformat(),
                hit_price,
                hit_date,
                hit_type,
                max_price,
                min_price,
                record_id,
            ))
            await db.commit()

        logger.info(f"决策验证: {record['ticker']} {result}")
        return {"result": result, "accuracy": accuracy, "current_price": current_price}

    async def validate_pending(self, max_count: int = 50) -> Dict:
        """批量验证待验证的决策"""
        # 只验证已到预期窗口的 pending 记录，避免新预测被当天价格污染。
        now = datetime.now()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT id, predicted_at, expected_days FROM price_predictions
                WHERE status = 'pending'
                ORDER BY predicted_at ASC
                LIMIT ?
            """, (max_count * 5,)) as cursor:
                candidates = await cursor.fetchall()

        all_ids = []
        for row in candidates:
            try:
                predicted_at = datetime.fromisoformat(row['predicted_at'])
                expected_days = int(row['expected_days'] or 30)
                if predicted_at + timedelta(days=expected_days) <= now:
                    all_ids.append(row['id'])
                if len(all_ids) >= max_count:
                    break
            except Exception:
                continue

        results = []
        for record_id in all_ids:
            result = await self.validate_single(record_id)
            results.append(result)

        validated = sum(1 for r in results if r.get("result") != "unknown")
        correct = sum(1 for r in results if r.get("result") == "correct")

        logger.info(f"批量验证完成: 验证{len(results)}条")
        return {"validated": len(results), "correct": correct, "results": results}


_recorder: Optional[DecisionRecorder] = None


def get_recorder(db_path: str = None) -> DecisionRecorder:
    global _recorder
    if _recorder is None:
        _recorder = DecisionRecorder(db_path)
    return _recorder
