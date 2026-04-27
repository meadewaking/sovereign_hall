#!/usr/bin/env python3
"""
Sovereign Hall - 增强版讨论系统
集成：预测验证 + 价格锚定 + 深度辩论
"""

import logging
from typing import Dict, List, Optional

from .prediction_validator import PredictionValidator, PredictionStatus
from .price_anchor import PriceAnchorExtractor, AnchoredProposal
from .deep_debate import DeepDebateSystem, Stance

logger = logging.getLogger(__name__)


class EnhancedDiscussion:
    """增强版投资讨论系统"""
    
    def __init__(self, db_service=None, llm_client=None):
        self.db_service = db_service
        self.llm = llm_client
        self.validator = PredictionValidator(db_service)
        self.price_extractor = PriceAnchorExtractor()
        self.debate_system = DeepDebateSystem(max_rounds=5)
    
    async def run_enhanced_discussion(
        self,
        topic: str,
        ticker: str,
        current_price: float,
        agents: List[Dict]
    ) -> Dict:
        """
        运行增强版讨论流程：
        1. 深度辩论（多轮交叉质询）
        2. 提取价格锚点
        3. 创建可验证的预测
        4. 输出带验证机制的投资建议
        """
        logger.info(f"Starting enhanced discussion for {ticker}")
        
        # 1. 注册Agent到辩论系统
        for agent in agents:
            stance = self._map_decision_to_stance(agent.get('decision', 'hold'))
            self.debate_system.register_agent(
                agent['id'], agent['name'], stance
            )
        
        # 2. 运行深度辩论
        debate_result = self.debate_system.run_debate(
            topic=f"{ticker} 投资分析",
            context=f"当前价格: {current_price}"
        )
        
        # 3. 生成带价格锚点的提案
        # 模拟从辩论结果生成投资建议文本
        proposal_text = self._generate_proposal_text(debate_result, ticker)
        
        # 4. 提取价格锚点
        price_anchor = self.price_extractor.extract(
            proposal_text, ticker, current_price
        )
        
        if not price_anchor:
            logger.warning(f"Failed to extract price anchor for {ticker}")
            return {'error': 'Price extraction failed'}
        
        # 5. 创建可验证的预测记录
        prediction = await self.validator.create_prediction(
            ticker=ticker,
            entry=price_anchor.entry_price,
            target=price_anchor.target_price,
            stop=price_anchor.stop_loss,
            confidence=debate_result['final_consensus']
        )
        
        # 6. 组装最终结果
        result = {
            'ticker': ticker,
            'current_price': current_price,
            'debate_summary': debate_result,
            'price_anchor': {
                'entry': price_anchor.entry_price,
                'target': price_anchor.target_price,
                'stop_loss': price_anchor.stop_loss,
                'risk_reward_ratio': price_anchor.risk_reward_ratio,
                'upside_potential': price_anchor.upside_potential
            },
            'prediction_id': prediction.id,
            'confidence': debate_result['final_consensus'],
            'action_plan': self._generate_action_plan(price_anchor)
        }
        
        logger.info(f"Enhanced discussion completed for {ticker}")
        return result
    
    def _map_decision_to_stance(self, decision: str) -> Stance:
        """映射决策到立场"""
        decision_map = {
            'buy': Stance.BULLISH,
            'strong_buy': Stance.BULLISH,
            'hold': Stance.NEUTRAL,
            'reduce': Stance.BEARISH,
            'sell': Stance.BEARISH
        }
        return decision_map.get(decision.lower(), Stance.NEUTRAL)
    
    def _generate_proposal_text(self, debate_result: Dict, ticker: str) -> str:
        """从辩论结果生成提案文本"""
        # 简化的文本生成
        text = f"关于{ticker}的投资建议："
        
        stances = debate_result.get('final_stances', {})
        bullish = sum(1 for s in stances.values() if s == Stance.BULLISH)
        bearish = sum(1 for s in stances.values() if s == Stance.BEARISH)
        
        if bullish > bearish:
            text += "建议买入，目标价上涨15%，止损设10%。"
        elif bearish > bullish:
            text += "建议观望或减仓，下行风险较大。"
        else:
            text += "建议持有，设置明确止损线。"
        
        return text
    
    def _generate_action_plan(self, price_anchor) -> Dict:
        """生成行动计划"""
        return {
            'entry': f"{price_anchor.entry_price:.2f}元",
            'target': f"{price_anchor.target_price:.2f}元 ({price_anchor.upside_potential*100:.1f}%上涨空间)",
            'stop_loss': f"{price_anchor.stop_loss:.2f}元 ({abs(price_anchor.stop_loss/price_anchor.entry_price-1)*100:.1f}%止损)",
            'risk_reward': f"1:{price_anchor.risk_reward_ratio:.2f}",
            'time_horizon': "90天"
        }
    
    async def validate_predictions(self, price_data: Dict[str, float]) -> Dict:
        """批量验证预测"""
        results = {}
        for ticker, current_price in price_data.items():
            # 找到该ticker的最新预测
            for pred_id, pred in self.validator.predictions.items():
                if pred.ticker == ticker:
                    status = self.validator.validate(pred_id, current_price)
                    results[ticker] = {
                        'status': status.value,
                        'return': pred.actual_return
                    }
        return results
    
    def get_accuracy_report(self) -> str:
        """获取准确率报告"""
        stats = self.validator.get_stats()
        
        if 'message' in stats:
            return stats['message']
        
        report = f"""
📊 预测准确率报告
━━━━━━━━━━━━━━━━━━━━━━━
总预测数: {stats['total']}
成功: {stats['success']} ({stats['success_rate']*100:.1f}%)
平均收益: {stats['avg_return']*100:.2f}%
━━━━━━━━━━━━━━━━━━━━━━━
"""
        return report
