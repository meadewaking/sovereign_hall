# 🏛️ Sovereign Hall 市场预测重构指南

## 问题诊断

### 原有系统缺陷

| 问题 | 影响 | 严重程度 |
|------|------|----------|
| ❌ **无价格预测** | 只有定性判断（买入/卖出），无法量化效果 | 🔴 高 |
| ❌ **无回测机制** | 2.3万条结论无后续追踪，无法知道历史决策对错 | 🔴 高 |
| ❌ **无准确率统计** | 不知道哪种策略有效，无法优化 | 🟠 中 |
| ❌ **无反馈闭环** | 错误无法被系统学习，重复犯同样错误 | 🟠 中 |

### 根本原因
系统只有**预测生成**，没有**预测验证**和**效果评估**闭环。

---

## 🚀 重构方案概述

我已为你创建以下核心组件：

### 1️⃣ Prediction Tracker (`services/prediction_tracker.py`)
追踪所有价格预测，自动验证结果，计算准确率

### 2️⃣ Backtest Engine (`services/backtest_engine.py`)  
历史策略回测，计算胜率、收益率、夏普比率等

### 3️⃣ Prediction Enhancer (`services/prediction_enhancer.py`)
基于历史表现优化预测参数，提供可执行洞察

### 4️⃣ CLI工具 (`cli/prediction_cli.py`)
命令行管理工具：验证、统计、回测、报告

---

## 📊 核心改进

### 预测准确率追踪
```
📊 预测准确率报告
════════════════════════════════════════════════════════════
总预测数: 150
✅ 完全正确: 60 (40.0%)
⚠️ 部分正确: 30 (20.0%)
❌ 预测错误: 60 (40.0%)

🎯 关键指标
胜率 (Win Rate): 50.0%
平均准确率得分: 0.65
平均收益率: 3.2%
```

### 回测报告
```
🏛️ 策略回测报告
════════════════════════════════════════════════════════════
总交易次数: 100
盈利次数: 55 (55.0%)

📈 收益指标
平均收益率: 2.8%
最大回撤: 12.5%
夏普比率: 1.2
盈亏比: 1.8
```

### 洞察建议
```
💡 可执行洞察
✅ 高置信度(≥0.8)预测胜率72%，建议优先执行
⚠️ 低置信度(<0.6)预测胜率仅32%，建议忽略
🎯 预测准确率最高的标的: 512880, 159995
⏱️ 目标达成时间普遍比预期长1.5倍
```

---

## 🔧 实施步骤

### 第1步：创建新表
```bash
cd /Users/wangziming/PycharmProjects/PythonProject/sovereign_hall
python3 -c "
from services.prediction_tracker import PredictionTracker
import asyncio
async def init():
    tracker = PredictionTracker()
    await tracker._ensure_tables()
asyncio.run(init())
"
```

### 第2步：集成到现有流程
修改 `services/investment_committee.py`：
```python
# 在生成结论后添加
from .prediction_tracker import get_prediction_tracker

tracker = get_prediction_tracker()
prediction = await tracker.create_prediction(
    conclusion_id=conclusion.id,
    ticker=proposal.ticker,
    current_price=current_price,
    target_price=extract_target_price(conclusion),
    stop_loss=extract_stop_loss(conclusion),
    direction="long",
    confidence=conclusion.confidence,
    expected_days=30
)
```

### 第3步：添加定时验证任务
修改 `run_discussion.py`，添加：
```python
async def daily_validation():
    tracker = get_prediction_tracker()
    await tracker.validate_predictions()
    report = await tracker.get_predictions_report()
    logger.info(report)

# 在main loop中调用
await daily_validation()
```

### 第4步：运行回测验证
```bash
python3 cli/prediction_cli.py backtest --days 180
```

---

## 🎯 成功指标

| 指标 | 当前 | 目标 | 验证方式 |
|------|------|------|----------|
| 预测准确率 | 未知 | ≥55% | prediction_stats表 |
| 胜率 | 未知 | ≥50% | backtest_results表 |
| 夏普比率 | 未知 | ≥1.0 | 回测报告 |
| 最大回撤 | 未知 | ≤15% | 回测报告 |

---

## 💡 关键设计决策

### 1. 为什么需要价格预测？
原有系统只有"买入/卖出/持有"定性判断，无法量化效果。新增价格预测（目标价、止损价）后，可以：**计算准确率**、**评估收益**、**优化策略**。

### 2. 如何验证预测？
- 自动获取实时价格（AKShare）
- 判断是否触发目标/止损
- 计算准确率得分（0-1）
- 统计胜率、收益率

### 3. 如何利用历史数据优化？
- 分析高置信度预测的真实胜率
- 识别哪些标的容易预测错误
- 动态调整置信度阈值
- 提供可执行的策略建议

---

## 📁 新增文件列表

```
sovereign_hall/
├── services/
│   ├── prediction_tracker.py      # 预测追踪
│   ├── backtest_engine.py         # 回测引擎
│   └── prediction_enhancer.py     # 预测增强
├── cli/
│   └── prediction_cli.py          # CLI工具
└── REFACTORING_GUIDE.md           # 本指南
```

---

## 🚀 下一步行动

1. **立即执行**: 运行表结构初始化
2. **本周完成**: 集成到InvestmentCommittee
3. **下周完成**: 添加定时验证任务
4. **持续优化**: 每周查看准确率报告，调整策略

**重构完成日期**: 2026-04-10
**作者**: AI Assistant
**版本**: v1.0
