#!/usr/bin/env python3
"""
Sovereign Hall - 价格锚定系统
解决：投资建议缺乏具体价格锚点
"""

import re
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PriceAnchor:
    """价格锚点"""
    ticker: str
    current_price: float
    entry_price: float
    target_price: float
    stop_loss: float
    time_horizon: int
    
    @property
    def risk_reward_ratio(self) -> float:
        """风险收益比"""
        risk = abs(self.entry_price - self.stop_loss)
        reward = abs(self.target_price - self.entry_price)
        return reward / risk if risk > 0 else 0
    
    @property
    def upside_potential(self) -> float:
        """上涨空间"""
        return (self.target_price - self.current_price) / self.current_price


class PriceAnchorExtractor:
    """从文本中提取价格锚点"""
    
    def __init__(self):
        self.patterns = {
            'price_with_unit': r'(\d+\.?\d*)\s*[元块]',
            'price_after_label': r'(?:目标价|止损|入场|买入|卖出)[\s:：]*(\d+\.?\d*)',
            'percentage': r'([+-]?\d+\.?\d*)%',
        }
    
    def extract(self, text: str, ticker: str, current_price: float) -> Optional[PriceAnchor]:
        """从文本中提取价格信息"""
        prices = []
        
        for pattern_name, pattern in self.patterns.items():
            matches = re.findall(pattern, text)
            prices.extend([float(m) for m in matches])
        
        if len(prices) >= 3:
            prices.sort()
            return PriceAnchor(
                ticker=ticker,
                current_price=current_price,
                entry_price=prices[1],
                target_price=prices[-1],
                stop_loss=prices[0],
                time_horizon=90
            )
        
        if current_price > 0:
            return PriceAnchor(
                ticker=ticker,
                current_price=current_price,
                entry_price=current_price * 0.98,
                target_price=current_price * 1.15,
                stop_loss=current_price * 0.90,
                time_horizon=90
            )
        
        return None
