# Sovereign Hall - 君临殿

全自动化多智能体投资研究与策略学习系统。

Sovereign Hall 模拟一个买方投研机构：自动选择议题、检索资料、组织多角色投委会辩论、生成投资提案，并把提案转成可验证的价格预测和模拟交易。当前版本已经从“生成研究结论”扩展为“生成 -> 执行/记录 -> 验证 -> 回测 -> 学习”的闭环。

> 本项目仅供研究学习使用，不构成任何投资建议。

## 产品目标与边界

系统的主循环是：持续联网检索新资料，结合数据库中的旧结论、预测期限和真实验证结果，由多 agent 独立分析、交叉质疑、反事实修正并投票，最后把资料、提案、会议、结论、预测、模拟交易和反思写回数据库。下一次讨论必须把这些历史结果作为“待重新验证的先验”带回讨论，而不是把旧结论当成当前事实。

Heuristic Learning 是维护系统的 coding agent 使用的非梯度优化方法。它读取可复现实验和模拟账户结果，以扣除真实模拟成本后的总资金收益为主要 reward，同时约束回撤、成本、换手和资金部署。Heuristic Learning coding agent 不得通过外部网页搜索替代本地实验；这项限制不约束交易系统本身。`run_discussion` 和 `research_interactive` 默认可以联网研究。

系统只允许模拟交易，禁止实盘和真实下单接口。

## 当前状态

- 主数据库：`data/sovereign_hall.db`
- 当前数据库规模：约 7.4 万篇文档、7,551 条研究结论、5,502 条价格预测、13,880 条投资提案
- 最新启发式学习运行：`runs/heuristic_cycle/20260724_143322`
- 当前保留策略：`full_deployment_diversified_hold10`
- 当前 `capital_return_v2` score：0.055596；净总收益 11.2538%，最大回撤 -4.2430%
- 注意：样本外结果仍为负，当前策略只作为风险约束和研究参考，不作为实盘策略。

## 快速开始

建议先创建虚拟环境并安装依赖：

```bash
cd /Users/wangziming/PycharmProjects/PythonProject/sovereign_hall
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

常用入口：

```bash
# 查看数据库、预测、持仓和交易状态
python -m sovereign_hall.check_db

# 连续联网自动投研和模拟投资；Ctrl+C 停止
python -m sovereign_hall.run_discussion

# 只运行一轮自动投研
python -m sovereign_hall.run_discussion --once

# 使用持续学习后的系统回答用户问题
python -m sovereign_hall.research_interactive

# 离线启发式学习循环，读取本地 price_predictions 并生成 run artifacts
python scripts/run_heuristic_cycle.py --db data/sovereign_hall.db
```

如需显式禁用研究资料联网，可向 `run_discussion` 添加 `--local-only`；这不是默认模式。

## 核心工作流

### 1. 自动研究与投委会

`run_discussion.py` 会从议题池中选择议题，联网拉取并保存研究材料，再从有明确证据的资料中抽取投资提案，最后让多智能体团队进行四轮分析、质疑、修正和投票。模型没有给出有证据支持的提案时，本轮保持空结果，不再注入预设 ETF 或虚构候选。

默认角色包括：

| 角色 | 关注点 |
| --- | --- |
| TMT 分析师 | 科技、AI、半导体、云计算 |
| 消费分析师 | 消费、医药、白酒、服务 |
| 周期分析师 | 有色、化工、地产、制造 |
| 宏观策略 | 利率、汇率、政策、市场风格 |
| 风控官 | 下行风险、仓位约束、反方论证 |
| 量化研究 | 数据、胜率、回测和信号质量 |
| 投资总监 | 综合投票、定案和组合取舍 |

### 2. 决策记录与价格预测

投委会结论会被记录为结构化数据：

- `proposals`：投资提案
- `meetings`：可回放的投委会讨论摘要、票型和行动项
- `report_conclusions`：研究结论
- `price_predictions`：可验证价格预测，包括入场价、目标价、止损、方向、置信度和验证窗口
- `reflection_summary` / `playbook`：历史反思和机构经验

每轮会先验证到期预测，再生成历史教训；同议题旧结论、预测期限和验证结果会作为待证伪记忆注入新一轮。当前代码会拒绝没有真实价格的数据进入关键预测和模拟交易环节，避免把不可验证的假价格写进闭环。

### 3. 市场数据与验证

`services/market_data.py` 统一处理 A 股和 ETF 行情：

- 代码标准化和市场推断
- 腾讯行情与东方财富行情
- 东方财富日线 OHLC，AkShare 作为兜底
- 交易日判断
- 短 TTL 行情缓存

`services/decision_tracker.py` 和 `services/prediction_tracker.py` 会按预测窗口验证结果，并写回命中目标、触发止损、过期、准确率等字段。

### 4. 模拟投资

`services/investment_simulation.py` 维护模拟账户：

- 初始资金：10,000 元
- 最小交易单位：100 股
- 佣金：0.03%
- 印花税：0.10%，卖出时收取
- 非交易日不交易
- 无真实价格时拒绝交易

相关表：

- `simulation_positions`
- `simulation_trades`
- `simulation_snapshots`
- `system_stats`

### 5. 离线启发式学习循环

`scripts/run_heuristic_cycle.py` 是 coding agent 的离线评估入口。它读取本地 SQLite 数据和可复现实验产物，不使用网页资料搜索，也不下单；这不改变产品运行入口默认联网研究的行为。

它会：

1. 读取 `price_predictions`
2. 构建按日聚合的信号带
3. 测试多组可解释策略
4. 计算收益、回撤、Sharpe、Sortino、胜率、换手和交易成本
5. 输出失败案例、过拟合检查、最优策略快照和图表

输出目录示例：

```text
runs/heuristic_cycle/20260724_143322/
├── README.md
├── summary.csv
├── trials.jsonl
├── baseline_metrics.json
├── best_metrics.json
├── overfit_checks.json
├── failure_cases.jsonl
├── daily_signal_tape.csv
├── equity_curve_best.csv
├── trades_best.csv
├── policy_snapshot.py
└── sample_efficiency.png
```

`runs/heuristic_cycle/LATEST` 保存最新运行目录。

## 项目结构

```text
sovereign_hall/
├── README.md
├── config.yaml
├── requirements.txt
├── main.py
├── check_db.py
├── run_discussion.py
├── research_interactive.py
├── scripts/
│   └── run_heuristic_cycle.py
├── agents/
│   └── agent.py
├── core/
│   ├── config.py
│   ├── sovereign_hall.py
│   ├── deep_debate.py
│   ├── enhanced_discussion.py
│   ├── prediction_validator.py
│   └── price_anchor.py
├── services/
│   ├── database.py
│   ├── llm_client.py
│   ├── spider_service.py
│   ├── market_data.py
│   ├── decision_tracker.py
│   ├── prediction_tracker.py
│   ├── prediction_enhancer.py
│   ├── backtest_engine.py
│   ├── investment_committee.py
│   ├── investment_simulation.py
│   ├── learning_engine.py
│   ├── vector_db.py
│   ├── db_viewer.py
│   └── db_inspector.py
├── tests/
│   └── test_refactor_pipeline.py
├── data/
│   ├── sovereign_hall.db
│   ├── logs/
│   ├── vector_db/
│   └── session_history/
└── runs/
    └── heuristic_cycle/
```

## 配置

主要配置在 `config.yaml`。

重点配置项：

```yaml
llm:
  provider: "openai"
  base_url: "http://172.18.5.19:8000/v1"
  model: "GLM-5.2-FP8"
  model_uuid: ""
  max_concurrent: 16
  max_tokens: 15000

spider:
  max_concurrent: 2
  proxy: "http://127.0.0.1:7890"
  rate_limit:
    requests_per_minute: 6
    burst: 2
  search_interval: 5

simulation:
  enabled: true
  initial_capital: 10000
  min_unit: 100
  trading_fee: 0.0003
  stamp_duty: 0.001

system:
  daily_token_budget: 100000000
  iteration_interval: 3600
  validation_batch_size: 100

investment_committee:
  max_rounds: 3
  quorum: 5
  approval_threshold: 0.6
```

根据本机环境需要调整：

- `llm.base_url` / `llm.api_key`
- `llm.embedding_base_url` / `llm.embedding_uuid`
- `spider.proxy`
- `database.path`
- `output.reports_dir`

## 数据表概览

当前主数据库包含这些关键表：

| 表 | 用途 |
| --- | --- |
| `documents` | 爬取和清洗后的研究文档 |
| `proposals` | 投资提案 |
| `report_conclusions` | 多智能体讨论结论 |
| `price_predictions` | 带目标价、止损和验证窗口的预测记录 |
| `reflection_summary` | 反思摘要 |
| `simulation_positions` | 当前模拟持仓 |
| `simulation_trades` | 模拟交易流水 |
| `simulation_snapshots` | 模拟账户快照 |
| `system_stats` | 系统状态和模拟现金等键值数据 |
| `blacklist` | 需要规避的标的或模式 |
| `playbook` | 机构经验库 |

## 测试与验证

从项目目录运行测试时，需要让 Python 能找到父级包路径：

```bash
cd /Users/wangziming/PycharmProjects/PythonProject/sovereign_hall
PYTHONPATH=.. pytest tests/test_refactor_pipeline.py
```

快速检查离线学习脚本：

```bash
python scripts/run_heuristic_cycle.py --db data/sovereign_hall.db --timestamp manual_check
```

如果 `runs/heuristic_cycle/manual_check` 已存在，换一个新的 `--timestamp`。

## 重要注意事项

- 这是研究系统，不是交易系统。
- LLM 输出会被结构化和验证，但仍可能产生错误推理。
- 离线回测基于本地预测带，不能代表未来收益。
- 当前最优启发式策略在成本压力测试下存在过拟合风险。
- 爬虫配置较保守，默认启用代理并降低频率，避免请求过密。
- 数据库和 `runs/` 产物可能很大，提交代码前应确认是否需要纳入版本管理。
