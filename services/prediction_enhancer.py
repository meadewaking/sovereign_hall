"""
🏛️ Sovereign Hall - Prediction Enhancer
预测增强器 - 基于历史表现优化预测
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import aiosqlite

from ..core import DATA_DIR
from .prediction_tracker import get_prediction_tracker, PredictionTracker

logger = logging.getLogger(__name__)


class PredictionEnhancer:
    """
    预测增强器
    
    功能：
    1. 分析历史预测表现，识别哪些特征导致成功/失败
    2. 动态调整置信度阈值
    3. 基于回测结果优化预测参数
    4. 为不同标的/策略分配权重
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")
        self.tracker = get_prediction_tracker()
    
    async def analyze_prediction_patterns(self, ticker: str = None) -> Dict:
        """
        分析预测成功/失败的模式
        
        返回：
        - 高置信度预测的成功率
        - 不同持有时长的表现
        - 不同标的的预测难度
        """
        async with aiosqlite.connect(self.db_path) as db:
            # 分析置信度与准确率的关系
            cursor = await db.execute("""
                SELECT 
                    CASE 
                        WHEN confidence >= 0.8 THEN 'high'
                        WHEN confidence >= 0.6 THEN 'medium'
                        ELSE 'low'
                    END as confidence_level,
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as correct,
                    SUM(CASE WHEN result = 'partial' THEN 1 ELSE 0 END) as partial,
                    AVG(accuracy_score) as avg_score
                FROM price_predictions
                WHERE status IN ('validated', 'expired')
                GROUP BY confidence_level
            """)
            
            confidence_analysis = {}
            async for row in cursor:
                level, total, correct, partial, avg_score = row
                win_rate = ((correct or 0) + (partial or 0) * 0.5) / (total or 1) * 100
                confidence_analysis[level] = {
                    'total': total,
                    'win_rate': round(win_rate, 2),
                    'avg_score': round(avg_score or 0, 3)
                }
            
            # 分析不同标的的表现
            cursor = await db.execute("""
                SELECT 
                    ticker,
                    COUNT(*) as total,
                    AVG(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) * 100 as win_rate,
                    AVG(accuracy_score) as avg_score
                FROM price_predictions
                WHERE status IN ('validated', 'expired')
                GROUP BY ticker
                HAVING COUNT(*) >= 5
                ORDER BY avg_score DESC
            """)
            
            ticker_performance = {}
            async for row in cursor:
                ticker_code, total, win_rate, avg_score = row
                ticker_performance[ticker_code] = {
                    'total_predictions': total,
                    'win_rate': round(win_rate or 0, 2),
                    'avg_score': round(avg_score or 0, 3)
                }
            
            # 分析预期天数与实际达成时间的关系
            cursor = await db.execute("""
                SELECT 
                    expected_days,
                    AVG(CASE WHEN actual_hit_date IS NOT NULL 
                        THEN julianday(actual_hit_date) - julianday(predicted_at)
                        ELSE NULL END) as avg_actual_days,
                    COUNT(*) as total
                FROM price_predictions
                WHERE status = 'validated' AND actual_hit_date IS NOT NULL
                GROUP BY expected_days
            """)
            
            time_analysis = {}
            async for row in cursor:
                expected, actual, total = row
                if actual:
                    time_analysis[str(expected)] = {
                        'expected_days': expected,
                        'avg_actual_days': round(actual, 1),
                        'sample_size': total
                    }
            
            return {
                'confidence_analysis': confidence_analysis,
                'ticker_performance': ticker_performance,
                'time_analysis': time_analysis,
                'analyzed_at': datetime.now().isoformat()
            }
    
    async def get_enhanced_prediction_params(
        self,
        ticker: str,
        base_confidence: float,
        base_target_price: float,
        base_stop_loss: float
    ) -> Dict:
        """
        获取增强的预测参数
        
        基于历史表现动态调整：
        1. 对该标的历史表现差 -> 降低置信度
        2. 整体胜率低 -> 收紧止损
        3. 平均达成时间短 -> 调整预期天数
        """
        params = {
            'original_confidence': base_confidence,
            'adjusted_confidence': base_confidence,
            'original_target': base_target_price,
            'adjusted_target': base_target_price,
            'original_stop_loss': base_stop_loss,
            'adjusted_stop_loss': base_stop_loss,
            'adjustment_reasons': []
        }
        
        async with aiosqlite.connect(self.db_path) as db:
            # 查询该标的历史表现
            cursor = await db.execute("""
                SELECT 
                    AVG(accuracy_score) as avg_score,
                    COUNT(*) as total,
                    AVG(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as win_rate
                FROM price_predictions
                WHERE ticker = ? AND status IN ('validated', 'expired')
            """, (ticker,))
            
            row = await cursor.fetchone()
            if row and row[0]:
                avg_score, total, win_rate = row
                
                if total >= 5:  # 有足够样本
                    # 如果历史表现差，降低置信度
                    if avg_score < 0.4:
                        params['adjusted_confidence'] = base_confidence * 0.8
                        params['adjustment_reasons'].append(
                            f"{ticker}历史预测准确率较低({avg_score:.2f})，置信度下调20%"
                        )
                    elif avg_score > 0.7:
                        params['adjusted_confidence'] = min(0.95, base_confidence * 1.1)
                        params['adjustment_reasons'].append(
                            f"{ticker}历史预测表现优秀({avg_score:.2f})，置信度上调"
                        )
                    
                    # 如果胜率低，收紧止损
                    if win_rate < 0.4:
                        # 将止损收紧10%
                        stop_range = base_stop_loss - base_target_price if base_stop_loss < base_target_price else base_stop_loss - base_target_price
                        params['adjusted_stop_loss'] = base_stop_loss + stop_range * 0.1
                        params['adjustment_reasons'].append(
                            f"历史胜率较低({win_rate*100:.1f}%)，止损位收紧10%"
                        )
        
        # 查询整体市场状态
        cursor = await db.execute("""
            SELECT AVG(accuracy_score) as market_avg_score
            FROM price_predictions
            WHERE status IN ('validated', 'expired')
            AND predicted_at >= date('now', '-30 days')
        """)
        
        row = await cursor.fetchone()
        if row and row[0] and row[0] < 0.4:
            # 市场整体表现差，进一步降低置信度
            params['adjusted_confidence'] = params['adjusted_confidence'] * 0.9
            params['adjustment_reasons'].append(
                f"近期市场整体预测准确率偏低({row[0]:.2f})，全局置信度下调10%"
            )
        
        return params
    
    async def generate_insights(self) -> List[str]:
        """生成可执行的洞察建议"""
        insights = []
        
        analysis = await self.analyze_prediction_patterns()
        
        # 1. 置信度分析洞察
        conf_analysis = analysis.get('confidence_analysis', {})
        if conf_analysis:
            high_conf = conf_analysis.get('high', {})
            low_conf = conf_analysis.get('low', {})
            
            if high_conf.get('win_rate', 0) > 60:
                insights.append(
                    f"✅ 高置信度(≥0.8)预测胜率{high_conf['win_rate']:.1f}%，建议优先执行"
                )
            
            if low_conf.get('win_rate', 0) < 40:
                insights.append(
                    f"⚠️ 低置信度(<0.6)预测胜率仅{low_conf['win_rate']:.1f}%，建议忽略或降低仓位"
                )
        
        # 2. 标的表现洞察
        ticker_perf = analysis.get('ticker_performance', {})
        best_tickers = sorted(ticker_perf.items(), 
                             key=lambda x: x[1]['avg_score'], 
                             reverse=True)[:3]
        worst_tickers = sorted(ticker_perf.items(), 
                              key=lambda x: x[1]['avg_score'])[:3]
        
        if best_tickers:
            insights.append(
                f"🎯 预测准确率最高的标的: {', '.join([t[0] for t in best_tickers])}"
            )
        
        if worst_tickers:
            insights.append(
                f"⚠️ 预测准确率较低的标的: {', '.join([t[0] for t in worst_tickers])} - 建议谨慎"
            )
        
        # 3. 时间分析洞察
        time_analysis = analysis.get('time_analysis', {})
        if time_analysis:
            avg_actual = sum(t['avg_actual_days'] for t in time_analysis.values()) / len(time_analysis)
            avg_expected = sum(t['expected_days'] for t in time_analysis.values()) / len(time_analysis)
            
            if avg_actual > avg_expected * 1.5:
                insights.append(
                    f"⏱️ 目标达成时间普遍比预期长{avg_actual/avg_expected:.1f}倍，建议延长预期持有期"
                )
        
        # 4. 整体建议
        all_predictions = sum(t['total_predictions'] for t in ticker_perf.values())
        total_win_rate = sum(t['win_rate'] * t['total_predictions'] for t in ticker_perf.values()) / all_predictions if all_predictions > 0 else 0
        
        if total_win_rate < 50:
            insights.append(
                f"📊 整体胜率{total_win_rate:.1f}%低于50%，建议：1)提高置信度门槛 2)收紧止损 3)减少交易频率"
            )
        elif total_win_rate > 60:
            insights.append(
                f"📊 整体胜率{total_win_rate:.1f}%表现良好，可适当增加仓位或放宽止损"
            )
        
        return insights
    
    async def get_strategy_recommendation(self) -> Dict:
        """获取策略优化建议"""
        analysis = await self.analyze_prediction_patterns()
        
        recommendations = {
            'confidence_threshold': 0.6,  # 默认
            'position_size_factor': 1.0,
            'stop_loss_tightening': 0.0,
            'focus_tickers': [],
            'avoid_tickers': [],
            'max_holding_days': 30
        }
        
        # 基于分析调整参数
        conf_analysis = analysis.get('confidence_analysis', {})
        medium_conf = conf_analysis.get('medium', {})
        
        if medium_conf.get('win_rate', 60) < 50:
            # 中等置信度表现不佳，提高门槛
            recommendations['confidence_threshold'] = 0.75
        
        ticker_perf = analysis.get('ticker_performance', {})
        for ticker, perf in ticker_perf.items():
            if perf.get('avg_score', 0) > 0.7:
                recommendations['focus_tickers'].append(ticker)
            elif perf.get('avg_score', 0) < 0.3:
                recommendations['avoid_tickers'].append(ticker)
        
        return recommendations


# 全局实例
_enhancer_instance = None

def get_prediction_enhancer() -> PredictionEnhancer:
    """获取预测增强器单例"""
    global _enhancer_instance
    if _enhancer_instance is None:
        _enhancer_instance = PredictionEnhancer()
    return _enhancer_instance