#!/usr/bin/env python3
"""
重构测试脚本 - 验证新模块功能
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.prediction_validator import PredictionValidator, PredictionStatus
from core.price_anchor import PriceAnchorExtractor
from core.deep_debate import DeepDebateSystem, Stance


def test_prediction_validator():
    """测试预测验证器"""
    print("\n" + "="*60)
    print("Testing PredictionValidator")
    print("="*60)
    
    validator = PredictionValidator()
    
    # 测试1: 创建预测
    pred = asyncio.run(validator.create_prediction(
        ticker="601888",
        entry=70.0,
        target=80.0,
        stop=65.0,
        confidence=0.75
    ))
    print(f"✅ Created prediction: {pred.id}")
    print(f"   Entry: {pred.entry_price}, Target: {pred.target_price}")
    
    # 测试2: 验证成功场景
    status = validator.validate(pred.id, 81.0)
    assert status == PredictionStatus.SUCCESS, f"Expected SUCCESS, got {status}"
    print(f"✅ Success validation passed: {status.value}")
    
    # 测试3: 验证失败场景
    pred2 = asyncio.run(validator.create_prediction(
        ticker="601888", entry=70.0, target=80.0, stop=65.0, confidence=0.7
    ))
    status2 = validator.validate(pred2.id, 64.0)
    assert status2 == PredictionStatus.FAILED, f"Expected FAILED, got {status2}"
    print(f"✅ Failed validation passed: {status2.value}")
    
    # 测试4: 统计
    stats = validator.get_stats()
    assert stats['total'] == 2, f"Expected 2 predictions, got {stats['total']}"
    assert stats['success'] == 1, f"Expected 1 success, got {stats['success']}"
    print(f"✅ Stats correct: {stats}")
    
def test_price_anchor():
    """测试价格锚定"""
    print("\n" + "="*60)
    print("Testing PriceAnchorExtractor")
    print("="*60)
    
    extractor = PriceAnchorExtractor()
    
    # 测试1: 从文本提取
    text = "建议买入，目标价80元，止损设65元，当前价格70元。"
    anchor = extractor.extract(text, "601888", 70.0)
    
    assert anchor is not None, "Failed to extract anchor"
    print(f"✅ Extracted anchor for {anchor.ticker}")
    print(f"   Entry: {anchor.entry_price}")
    print(f"   Target: {anchor.target_price} ({anchor.upside_potential*100:.1f}% upside)")
    print(f"   Stop: {anchor.stop_loss}")
    print(f"   Risk/Reward: 1:{anchor.risk_reward_ratio:.2f}")
    
    # 验证计算
    expected_rr = (80-70)/(70-65)  # 10/5 = 2
    assert abs(anchor.risk_reward_ratio - expected_rr) < 0.1, "Risk/Reward calculation error"
    print("✅ Risk/Reward calculation correct")
    
def test_deep_debate():
    """测试深度辩论"""
    print("\n" + "="*60)
    print("Testing DeepDebateSystem")
    print("="*60)
    
    debate = DeepDebateSystem(max_rounds=3, consensus_threshold=0.7)
    
    # 注册Agent
    debate.register_agent("value", "Value Investor", Stance.BULLISH)
    debate.register_agent("growth", "Growth Investor", Stance.NEUTRAL)
    debate.register_agent("contrarian", "Contrarian", Stance.BEARISH)
    
    # 运行辩论
    result = debate.run_debate(
        topic="中国中免(601888)投资分析",
        context="当前价格70元，成本72.3元，浮亏3%"
    )
    
    assert result['total_rounds'] > 0, "Debate did not run"
    print(f"✅ Debate completed in {result['total_rounds']} rounds")
    print(f"✅ Final consensus: {result['final_consensus']:.2f}")
    print(f"✅ Final stances: {result['final_stances']}")
    
    # 验证有辩论记录
    assert len(result['debate_log']) > 0, "No debate log"
    print(f"✅ Generated {len(result['debate_log'])} rounds of debate")
    
def run_all_tests():
    """运行所有测试"""
    print("\n" + "🧪 "*20)
    print("SOVEREIGN HALL REFACTOR TEST SUITE")
    print("🧪 "*20)
    
    tests = [
        ("PredictionValidator", test_prediction_validator),
        ("PriceAnchorExtractor", test_price_anchor),
        ("DeepDebateSystem", test_deep_debate),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            print(f"\n>>> Running {name}...")
            test_func()
            print(f"✅ {name} PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ {name} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*60)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
