"""
🏛️ Sovereign Hall - Prediction Tracker
预测追踪与评估系统
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import aiosqlite

from ..core import DATA_DIR
from .market_data import get_market_data

logger = logging.getLogger(__name__)


class PredictionStatus(Enum):
    """预测状态"""
    PENDING = "pending"           # 等待验证
    VALIDATED = "validated"       # 已验证
    EXPIRED = "expired"          # 过期未触发
    CANCELLED = "cancelled"       # 手动取消


class PredictionResult(Enum):
    """预测结果"""
    CORRECT = "correct"          # 预测正确
    PARTIAL = "partial"          # 部分正确
    WRONG = "wrong"             # 预测错误
    UNKNOWN = "unknown"          # 无法判断


@dataclass
class PricePrediction:
    """价格预测记录"""
    id: str
    conclusion_id: str          # 关联的研究结论ID
    ticker: str
    current_price: float        # 预测时价格
    target_price: float         # 目标价格
    stop_loss: float           # 止损价格
    direction: str             # "long" / "short"
    confidence: float          # 置信度 0-1
    predicted_at: datetime
    expected_days: int         # 预期达成天数
    
    # 验证结果
    actual_hit_price: Optional[float] = None
    actual_hit_date: Optional[datetime] = None
    actual_hit_type: Optional[str] = None  # "target" / "stop_loss" / "expired"
    max_price_reached: Optional[float] = None
    min_price_reached: Optional[float] = None
    
    status: PredictionStatus = PredictionStatus.PENDING
    result: PredictionResult = PredictionResult.UNKNOWN
    accuracy_score: Optional[float] = None  # 0-1 准确率评分
    
    # 元数据
    created_at: datetime = None
    validated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class PredictionTracker:
    """预测追踪器 - 核心组件"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")
        self._init_task = None
        
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
                    FOREIGN KEY (conclusion_id) REFERENCES report_conclusions(id)
                )
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_predictions_ticker 
                ON price_predictions(ticker)
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_predictions_status 
                ON price_predictions(status)
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at 
                ON price_predictions(predicted_at)
            """)
            
            # 预测准确率统计表
            await db.execute("""
                CREATE TABLE IF NOT EXISTS prediction_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT,
                    period_days INTEGER,
                    total_predictions INTEGER DEFAULT 0,
                    correct_predictions INTEGER DEFAULT 0,
                    partial_correct INTEGER DEFAULT 0,
                    wrong_predictions INTEGER DEFAULT 0,
                    avg_accuracy_score REAL,
                    avg_return_pct REAL,
                    win_rate REAL,
                    sharpe_ratio REAL,
                    calculated_at TEXT,
                    UNIQUE(ticker, period_days)
                )
            """)
            
            await db.commit()
    
    async def create_prediction(
        self,
        conclusion_id: str,
        ticker: str,
        current_price: float,
        target_price: float,
        stop_loss: float,
        direction: str,
        confidence: float,
        expected_days: int = 30
    ) -> PricePrediction:
        """创建新的价格预测"""
        await self._ensure_tables()

        if current_price is None:
            current_price = await get_market_data().get_current_price(ticker)
        if current_price is None or current_price <= 0:
            raise ValueError(f"无法获取 {ticker} 的真实当前价格，拒绝创建不可验证预测")
        
        prediction_id = f"pred_{datetime.now().strftime('%Y%m%d%H%M%S')}_{ticker}"
        
        prediction = PricePrediction(
            id=prediction_id,
            conclusion_id=conclusion_id,
            ticker=ticker,
            current_price=current_price,
            target_price=target_price,
            stop_loss=stop_loss,
            direction=direction,
            confidence=confidence,
            predicted_at=datetime.now(),
            expected_days=expected_days
        )
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO price_predictions 
                (id, conclusion_id, ticker, current_price, target_price, stop_loss,
                 direction, confidence, predicted_at, expected_days, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                prediction.id, prediction.conclusion_id, prediction.ticker,
                prediction.current_price, prediction.target_price, prediction.stop_loss,
                prediction.direction, prediction.confidence, 
                prediction.predicted_at.isoformat(), prediction.expected_days,
                prediction.status.value, prediction.created_at.isoformat()
            ))
            await db.commit()
        
        logger.info(f"Created prediction {prediction_id} for {ticker}: "
                   f"{current_price} -> {target_price} (SL: {stop_loss})")
        
        return prediction
    
    async def validate_predictions(self, ticker: str = None):
        """验证待验证的预测"""
        await self._ensure_tables()
        
        async with aiosqlite.connect(self.db_path) as db:
            # 获取待验证的预测
            cutoff = datetime.now()
            if ticker:
                cursor = await db.execute(
                    """
                    SELECT * FROM price_predictions
                    WHERE ticker = ? AND status = 'pending'
                    ORDER BY predicted_at ASC
                    """,
                    (ticker,),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT * FROM price_predictions
                    WHERE status = 'pending'
                    ORDER BY predicted_at ASC
                    """
                )

            rows = await cursor.fetchall()

            validated_count = 0
            for row in rows:
                prediction = self._row_to_prediction(row)
                if prediction.predicted_at + timedelta(days=prediction.expected_days) > cutoff:
                    continue
                await self._validate_single_prediction(db, prediction)
                validated_count += 1
            
            logger.info(f"Validated {validated_count} predictions")
            return validated_count
    
    async def _validate_single_prediction(self, db: aiosqlite.Connection, 
                                         prediction: PricePrediction):
        """验证单个预测"""
        # 获取实际价格和预测窗口内路径
        current_price = await self._fetch_current_price(prediction.ticker)

        if current_price is None:
            logger.warning(f"Could not fetch price for {prediction.ticker}")
            return

        days_elapsed = (datetime.now() - prediction.predicted_at).days
        bars = await get_market_data().get_ohlc(
            prediction.ticker,
            prediction.predicted_at,
            prediction.predicted_at + timedelta(days=max(prediction.expected_days, 1)),
        )
        if bars:
            prediction.max_price_reached = max(bar["high"] for bar in bars)
            prediction.min_price_reached = min(bar["low"] for bar in bars)

            for bar in bars:
                if prediction.direction == "long":
                    if bar["low"] <= prediction.stop_loss:
                        prediction.status = PredictionStatus.VALIDATED
                        prediction.result = PredictionResult.WRONG
                        prediction.actual_hit_price = prediction.stop_loss
                        prediction.actual_hit_date = datetime.fromisoformat(bar["date"])
                        prediction.actual_hit_type = "stop_loss"
                        prediction.accuracy_score = 0.0
                        break
                    if bar["high"] >= prediction.target_price:
                        prediction.status = PredictionStatus.VALIDATED
                        prediction.result = PredictionResult.CORRECT
                        prediction.actual_hit_price = prediction.target_price
                        prediction.actual_hit_date = datetime.fromisoformat(bar["date"])
                        prediction.actual_hit_type = "target"
                        prediction.accuracy_score = 1.0
                        break
                else:
                    if bar["high"] >= prediction.stop_loss:
                        prediction.status = PredictionStatus.VALIDATED
                        prediction.result = PredictionResult.WRONG
                        prediction.actual_hit_price = prediction.stop_loss
                        prediction.actual_hit_date = datetime.fromisoformat(bar["date"])
                        prediction.actual_hit_type = "stop_loss"
                        prediction.accuracy_score = 0.0
                        break
                    if bar["low"] <= prediction.target_price:
                        prediction.status = PredictionStatus.VALIDATED
                        prediction.result = PredictionResult.CORRECT
                        prediction.actual_hit_price = prediction.target_price
                        prediction.actual_hit_date = datetime.fromisoformat(bar["date"])
                        prediction.actual_hit_type = "target"
                        prediction.accuracy_score = 1.0
                        break

        is_expired = days_elapsed >= prediction.expected_days

        if prediction.status == PredictionStatus.PENDING and is_expired:
            prediction.status = PredictionStatus.EXPIRED
            # 判断是否部分正确
            if prediction.direction == "long":
                price_change = (current_price - prediction.current_price) / prediction.current_price
                if price_change > 0.05:  # 涨5%以上算部分正确
                    prediction.result = PredictionResult.PARTIAL
                    prediction.accuracy_score = 0.5
                else:
                    prediction.result = PredictionResult.WRONG
                    prediction.accuracy_score = 0.0
            else:
                price_change = (prediction.current_price - current_price) / prediction.current_price
                if price_change > 0.05:
                    prediction.result = PredictionResult.PARTIAL
                    prediction.accuracy_score = 0.5
                else:
                    prediction.result = PredictionResult.WRONG
                    prediction.accuracy_score = 0.0
        
        prediction.validated_at = datetime.now()
        
        # 更新数据库
        await db.execute("""
            UPDATE price_predictions SET
                status = ?,
                result = ?,
                actual_hit_price = ?,
                actual_hit_date = ?,
                actual_hit_type = ?,
                max_price_reached = ?,
                min_price_reached = ?,
                accuracy_score = ?,
                validated_at = ?
            WHERE id = ?
        """, (
            prediction.status.value,
            prediction.result.value,
            prediction.actual_hit_price,
            prediction.actual_hit_date.isoformat() if prediction.actual_hit_date else None,
            prediction.actual_hit_type,
            prediction.max_price_reached,
            prediction.min_price_reached,
            prediction.accuracy_score,
            prediction.validated_at.isoformat() if prediction.validated_at else None,
            prediction.id
        ))
        await db.commit()
        
        logger.info(f"Validated prediction {prediction.id}: {prediction.result.value} "
                   f"(score: {prediction.accuracy_score})")
    
    async def _fetch_current_price(self, ticker: str) -> Optional[float]:
        """获取当前价格 - 统一使用 MarketDataService。"""
        return await get_market_data().get_current_price(ticker)
    
    async def calculate_accuracy_stats(self, ticker: str = None, 
                                      period_days: int = 90) -> Dict:
        """计算预测准确率统计"""
        await self._ensure_tables()
        
        async with aiosqlite.connect(self.db_path) as db:
            since = (datetime.now() - timedelta(days=period_days)).isoformat()
            
            if ticker:
                cursor = await db.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as correct,
                        SUM(CASE WHEN result = 'partial' THEN 1 ELSE 0 END) as partial,
                        SUM(CASE WHEN result = 'wrong' THEN 1 ELSE 0 END) as wrong,
                        AVG(accuracy_score) as avg_score,
                        AVG(CASE WHEN actual_hit_price IS NOT NULL 
                            THEN (actual_hit_price - current_price) / current_price * 100 
                            ELSE NULL END) as avg_return
                    FROM price_predictions
                    WHERE ticker = ? AND predicted_at >= ? AND status IN ('validated', 'expired')
                """, (ticker, since))
            else:
                cursor = await db.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as correct,
                        SUM(CASE WHEN result = 'partial' THEN 1 ELSE 0 END) as partial,
                        SUM(CASE WHEN result = 'wrong' THEN 1 ELSE 0 END) as wrong,
                        AVG(accuracy_score) as avg_score,
                        AVG(CASE WHEN actual_hit_price IS NOT NULL 
                            THEN (actual_hit_price - current_price) / current_price * 100 
                            ELSE NULL END) as avg_return
                    FROM price_predictions
                    WHERE predicted_at >= ? AND status IN ('validated', 'expired')
                """, (since,))
            
            row = await cursor.fetchone()
            
            if not row or row[0] == 0:
                return {
                    'ticker': ticker or 'ALL',
                    'period_days': period_days,
                    'total_predictions': 0,
                    'message': 'No validated predictions in this period'
                }
            
            total, correct, partial, wrong, avg_score, avg_return = row
            
            # 计算胜率
            win_rate = (correct + partial * 0.5) / total if total > 0 else 0
            
            stats = {
                'ticker': ticker or 'ALL',
                'period_days': period_days,
                'total_predictions': total,
                'correct_predictions': correct or 0,
                'partial_correct': partial or 0,
                'wrong_predictions': wrong or 0,
                'win_rate': round(win_rate * 100, 2),
                'avg_accuracy_score': round(avg_score, 3) if avg_score else 0,
                'avg_return_pct': round(avg_return, 2) if avg_return else 0,
                'calculated_at': datetime.now().isoformat()
            }
            
            # 保存统计结果
            await db.execute("""
                INSERT OR REPLACE INTO prediction_stats
                (ticker, period_days, total_predictions, correct_predictions, 
                 partial_correct, wrong_predictions, avg_accuracy_score, 
                 avg_return_pct, win_rate, calculated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker or 'ALL', period_days, stats['total_predictions'],
                stats['correct_predictions'], stats['partial_correct'],
                stats['wrong_predictions'], stats['avg_accuracy_score'],
                stats['avg_return_pct'], stats['win_rate'],
                stats['calculated_at']
            ))
            await db.commit()
            
            return stats
    
    def _row_to_prediction(self, row) -> PricePrediction:
        """数据库行转对象"""
        return PricePrediction(
            id=row[0],
            conclusion_id=row[1],
            ticker=row[2],
            current_price=row[3],
            target_price=row[4],
            stop_loss=row[5],
            direction=row[6],
            confidence=row[7],
            predicted_at=datetime.fromisoformat(row[8]) if row[8] else None,
            expected_days=row[9],
            actual_hit_price=row[10],
            actual_hit_date=datetime.fromisoformat(row[11]) if row[11] else None,
            actual_hit_type=row[12],
            max_price_reached=row[13],
            min_price_reached=row[14],
            status=PredictionStatus(row[15]),
            result=PredictionResult(row[16]),
            accuracy_score=row[17],
            created_at=datetime.fromisoformat(row[18]) if row[18] else None,
            validated_at=datetime.fromisoformat(row[19]) if row[19] else None
        )
    
    async def get_predictions_report(self, ticker: str = None, 
                                    days: int = 30) -> str:
        """生成预测准确率报告"""
        stats = await self.calculate_accuracy_stats(ticker, days)
        
        if stats.get('total_predictions', 0) == 0:
            return f"📊 预测准确率报告\n{'='*50}\n暂无已验证的预测数据"
        
        report = f"""
📊 Sovereign Hall 市场预测准确率报告
{'='*60}
标的: {stats['ticker']}
统计周期: 最近{stats['period_days']}天
统计时间: {stats['calculated_at'][:19]}

📈 预测统计
  总预测数: {stats['total_predictions']}
  ✅ 完全正确: {stats['correct_predictions']} ({stats['correct_predictions']/stats['total_predictions']*100:.1f}%)
  ⚠️ 部分正确: {stats['partial_correct']} ({stats['partial_correct']/stats['total_predictions']*100:.1f}%)
  ❌ 预测错误: {stats['wrong_predictions']} ({stats['wrong_predictions']/stats['total_predictions']*100:.1f}%)

🎯 关键指标
  胜率 (Win Rate): {stats['win_rate']}%
  平均准确率得分: {stats['avg_accuracy_score']}
  平均收益率: {stats['avg_return_pct']}%

📊 评估等级: {'A' if stats['win_rate'] >= 60 else 'B' if stats['win_rate'] >= 50 else 'C' if stats['win_rate'] >= 40 else 'D'}

{'='*60}
"""
        return report


# 全局实例
_tracker_instance = None

def get_prediction_tracker() -> PredictionTracker:
    """获取预测追踪器单例"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = PredictionTracker()
    return _tracker_instance
