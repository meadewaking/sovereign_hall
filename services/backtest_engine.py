"""
🏛️ Sovereign Hall - Backtest Engine
回测引擎 - 验证历史策略表现
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import aiosqlite
import pandas as pd
import numpy as np

from ..core import DATA_DIR
from .market_data import get_market_data

logger = logging.getLogger(__name__)


@dataclass
class BacktestTrade:
    """回测交易记录"""
    entry_date: datetime
    exit_date: Optional[datetime]
    ticker: str
    direction: str  # "long" / "short"
    entry_price: float
    exit_price: Optional[float]
    target_price: float
    stop_loss: float
    shares: int
    pnl: float
    pnl_pct: float
    exit_reason: str  # "target", "stop_loss", "expired", "open"


@dataclass
class BacktestResult:
    """回测结果"""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_return: float
    max_drawdown: float
    sharpe_ratio: float
    profit_factor: float
    trades: List[BacktestTrade]


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")
        self.price_cache = {}  # 价格缓存
    
    async def run_backtest(
        self,
        start_date: datetime,
        end_date: datetime,
        ticker: str = None,
        min_confidence: float = 0.6
    ) -> BacktestResult:
        """
        运行回测
        
        Args:
            start_date: 回测开始日期
            end_date: 回测结束日期
            ticker: 指定标的（None则回测所有）
            min_confidence: 最小置信度筛选
        """
        logger.info(f"Starting backtest from {start_date} to {end_date}")
        
        # 获取历史预测记录
        predictions = await self._get_historical_predictions(
            start_date, end_date, ticker, min_confidence
        )
        
        logger.info(f"Found {len(predictions)} predictions to backtest")
        
        # 模拟每笔交易
        trades = []
        for pred in predictions:
            trade = await self._simulate_trade(pred)
            if trade:
                trades.append(trade)
        
        # 计算回测统计
        result = self._calculate_backtest_stats(trades)
        
        # 保存回测结果
        await self._save_backtest_result(start_date, end_date, ticker, result)
        
        return result
    
    async def _get_historical_predictions(
        self,
        start_date: datetime,
        end_date: datetime,
        ticker: Optional[str],
        min_confidence: float
    ) -> List[Dict]:
        """获取历史预测数据"""
        async with aiosqlite.connect(self.db_path) as db:
            query = """
                SELECT 
                    p.id, p.ticker, p.current_price, p.target_price, 
                    p.stop_loss, p.direction, p.confidence, p.predicted_at,
                    p.expected_days
                FROM price_predictions p
                WHERE p.predicted_at >= ? AND p.predicted_at <= ?
                AND p.confidence >= ?
            """
            params = [start_date.isoformat(), end_date.isoformat(), min_confidence]
            
            if ticker:
                query += " AND p.ticker = ?"
                params.append(ticker)
            
            query += " ORDER BY p.predicted_at"
            
            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()
            
            predictions = []
            for row in rows:
                predictions.append({
                    'id': row[0],
                    'ticker': row[1],
                    'entry_price': row[2],
                    'target_price': row[3],
                    'stop_loss': row[4],
                    'direction': row[5],
                    'confidence': row[6],
                    'predicted_at': datetime.fromisoformat(row[7]),
                    'expected_days': row[8]
                })
            
            return predictions
    
    async def _simulate_trade(self, prediction: Dict) -> Optional[BacktestTrade]:
        """模拟单笔交易"""
        ticker = prediction['ticker']
        entry_date = prediction['predicted_at']
        entry_price = prediction['entry_price']
        target_price = prediction['target_price']
        stop_loss = prediction['stop_loss']
        direction = prediction['direction']
        expected_days = prediction['expected_days']
        
        # 获取后续价格数据
        bars = await self._get_price_history(ticker, entry_date, expected_days * 2)

        if not bars or len(bars) < 2:
            logger.warning(f"No price data for {ticker} from {entry_date}")
            return None
        
        # 模拟交易过程
        exit_date = None
        exit_price = None
        exit_reason = "open"
        
        for i, bar in enumerate(bars[1:], 1):  # 从第二天开始
            date = datetime.fromisoformat(bar["date"])
            # 检查是否触发目标或止损
            if direction == "long":
                if bar["low"] <= stop_loss:
                    exit_date = date
                    exit_price = stop_loss
                    exit_reason = "stop_loss"
                    break
                if bar["high"] >= target_price:
                    exit_date = date
                    exit_price = target_price
                    exit_reason = "target"
                    break
            else:  # short
                if bar["high"] >= stop_loss:
                    exit_date = date
                    exit_price = stop_loss
                    exit_reason = "stop_loss"
                    break
                if bar["low"] <= target_price:
                    exit_date = date
                    exit_price = target_price
                    exit_reason = "target"
                    break

            # 检查是否过期
            if i >= expected_days:
                exit_date = date
                exit_price = bar["close"]
                exit_reason = "expired"
                break

        # 如果还未平仓，使用最后一个价格
        if exit_reason == "open" and bars:
            exit_date = datetime.fromisoformat(bars[-1]["date"])
            exit_price = bars[-1]["close"]
            exit_reason = "expired"
        
        # 计算盈亏
        if direction == "long":
            pnl = exit_price - entry_price
            pnl_pct = (exit_price - entry_price) / entry_price * 100
        else:
            pnl = entry_price - exit_price
            pnl_pct = (entry_price - exit_price) / entry_price * 100
        
        return BacktestTrade(
            entry_date=entry_date,
            exit_date=exit_date,
            ticker=ticker,
            direction=direction,
            entry_price=entry_price,
            exit_price=exit_price,
            target_price=target_price,
            stop_loss=stop_loss,
            shares=100,  # 假设一手
            pnl=pnl * 100,
            pnl_pct=pnl_pct,
            exit_reason=exit_reason
        )
    
    async def _get_price_history(
        self, 
        ticker: str, 
        start_date: datetime,
        max_days: int
    ) -> List[Dict]:
        """获取历史价格数据"""
        cache_key = f"{ticker}_{start_date.strftime('%Y%m%d')}"

        if cache_key in self.price_cache:
            return self.price_cache[cache_key]

        end_date = start_date + timedelta(days=max_days)
        bars = await get_market_data().get_ohlc(ticker, start_date, end_date)
        self.price_cache[cache_key] = bars
        return bars
    
    def _calculate_backtest_stats(self, trades: List[BacktestTrade]) -> BacktestResult:
        """计算回测统计"""
        if not trades:
            return BacktestResult(
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate=0,
                avg_return=0,
                max_drawdown=0,
                sharpe_ratio=0,
                profit_factor=0,
                trades=[]
            )
        
        total_trades = len(trades)
        winning_trades = sum(1 for t in trades if t.pnl > 0)
        losing_trades = total_trades - winning_trades
        win_rate = winning_trades / total_trades * 100
        
        returns = [t.pnl_pct for t in trades]
        avg_return = np.mean(returns)
        
        # 计算最大回撤
        cumulative = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = running_max - cumulative
        max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0
        
        # 计算夏普比率（简化版，假设无风险利率为0）
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)  # 年化
        else:
            sharpe_ratio = 0
        
        # 计算盈亏比
        avg_win = np.mean([t.pnl_pct for t in trades if t.pnl > 0]) if winning_trades > 0 else 0
        avg_loss = abs(np.mean([t.pnl_pct for t in trades if t.pnl < 0])) if losing_trades > 0 else 1
        profit_factor = avg_win / avg_loss if avg_loss > 0 else 0
        
        return BacktestResult(
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            avg_return=avg_return,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            profit_factor=profit_factor,
            trades=trades
        )
    
    async def _save_backtest_result(
        self,
        start_date: datetime,
        end_date: datetime,
        ticker: Optional[str],
        result: BacktestResult
    ):
        """保存回测结果"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_date TEXT,
                    end_date TEXT,
                    ticker TEXT,
                    total_trades INTEGER,
                    winning_trades INTEGER,
                    losing_trades INTEGER,
                    win_rate REAL,
                    avg_return REAL,
                    max_drawdown REAL,
                    sharpe_ratio REAL,
                    profit_factor REAL,
                    created_at TEXT
                )
            """)
            
            await db.execute("""
                INSERT INTO backtest_results
                (start_date, end_date, ticker, total_trades, winning_trades,
                 losing_trades, win_rate, avg_return, max_drawdown, 
                 sharpe_ratio, profit_factor, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                start_date.isoformat(),
                end_date.isoformat(),
                ticker or 'ALL',
                result.total_trades,
                result.winning_trades,
                result.losing_trades,
                result.win_rate,
                result.avg_return,
                result.max_drawdown,
                result.sharpe_ratio,
                result.profit_factor,
                datetime.now().isoformat()
            ))
            await db.commit()
    
    def generate_backtest_report(self, result: BacktestResult) -> str:
        """生成回测报告"""
        report = f"""
🏛️ Sovereign Hall 策略回测报告
{'='*60}

📊 整体表现
  总交易次数: {result.total_trades}
  盈利次数: {result.winning_trades} ({result.win_rate:.1f}%)
  
📈 收益指标
  平均收益率: {result.avg_return:.2f}%
  最大回撤: {result.max_drawdown:.2f}%
  夏普比率: {result.sharpe_ratio:.2f}
  盈亏比: {result.profit_factor:.2f}

📝 最近交易明细
{'='*60}
"""
        
        # 显示最近10笔交易
        recent_trades = sorted(result.trades, key=lambda x: x.entry_date, reverse=True)[:10]
        
        for trade in recent_trades:
            emoji = "🟢" if trade.pnl > 0 else "🔴" if trade.pnl < 0 else "⚪"
            report += f"\n{emoji} {trade.ticker} | {trade.entry_date.strftime('%Y-%m-%d')}"
            report += f"\n   {trade.direction.upper()} @ {trade.entry_price:.2f} -> {trade.exit_price:.2f}"
            report += f"\n   盈亏: {trade.pnl_pct:+.2f}% | 原因: {trade.exit_reason}"
        
        report += f"\n\n{'='*60}"
        return report


# 全局实例
_backtest_instance = None

def get_backtest_engine() -> BacktestEngine:
    """获取回测引擎单例"""
    global _backtest_instance
    if _backtest_instance is None:
        _backtest_instance = BacktestEngine()
    return _backtest_instance
