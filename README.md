# Sovereign Hall - 君临殿

全自动化多智能体投资研究与策略学习系统。

Sovereign Hall 模拟一个买方投研机构：自动选择议题、检索资料、组织多角色投委会辩论、生成投资提案，并把提案转成可验证的价格预测和模拟交易。当前版本已经从“生成研究结论”扩展为“生成 -> 执行/记录 -> 验证 -> 回测 -> 学习”的闭环。

> 本项目仅供研究学习使用，不构成任何投资建议。

## 产品目标与边界

系统的主循环是：持续联网检索新资料，结合数据库中的旧结论、预测期限和真实验证结果，由多 agent 独立分析、交叉质疑、反事实修正并投票，最后把资料、提案、会议、结论、预测、模拟交易和反思写回数据库。下一次讨论必须把这些历史结果作为“待重新验证的先验”带回讨论，而不是把旧结论当成当前事实。

Heuristic Learning 是维护系统的 coding agent 使用的非梯度优化方法。唯一有效的 reward/score 是按受控实时行情估值后的模拟账户累计净收益；佣金、卖出印花税和滑点已经通过真实模拟成交写入现金与净值。离线回测、OOS、Sharpe 和 leaderboard 只用于诊断失败模式，不能产生 best、不能晋升策略、不能替代模拟账户没有成交的事实。Heuristic Learning coding agent 不得通过外部网页搜索替代本地实验；这项限制不约束交易系统本身。`run_discussion` 和 `research_interactive` 默认可以联网研究。

系统只允许模拟交易，禁止实盘和真实下单接口。

## 架构

当前采用渐进式模块化单体，而不是大爆炸重写：

- `application/`：三个入口复用的用例与编排
- `domain/`：研究轮次状态机和纯组合执行约束
- `ports/`：事务与受控实时行情契约
- `infrastructure/sqlite/`：前向迁移与原子工作单元
- `services/`：保留并逐步收窄的研究、行情和兼容服务

每轮研究由持久化 `ResearchRound` 和有序事件描述；每次新模拟操作先
创建不含成交价的 `ExecutionIntent`，成交时重新获取受控实时行情，并在一个
事务内写入报价、成交、费用台账、现金、持仓、待执行状态和日内成交限额。
完整设计、迁移边界与剩余风险见
[`docs/architecture_refactor_20260729.md`](docs/architecture_refactor_20260729.md)。

## 当前状态

- 主数据库：`data/sovereign_hall.db`
- 当前数据库规模：约 7.4 万篇文档、7,551 条研究结论、5,502 条价格预测、13,880 条投资提案
- 唯一绩效标准：`simulation_account_realtime_v1`
- 当前模拟账户资产：9,727.22 元；累计净收益 / score：-2.73%
- 当前持仓：空仓；投入率 0%；最近模拟成交：2026-07-14
- 当前健康状态：`system_failure_no_live_deployment`。长期空仓且无近期成交是研究到模拟执行链路故障，不能用离线回测收益覆盖。
- 当前执行安全策略：`simulation_live_policy_v1`，静态边界来自 `config.yaml`；不存在由离线收益选出的生效 best policy。

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

# Heuristic Learning：唯一绩效仍来自实时模拟账户；离线结果只作诊断
python scripts/run_heuristic_cycle.py --db data/sovereign_hall.db
```

如需显式禁用研究资料联网，可向 `run_discussion` 添加 `--local-only`；这不是默认模式。

## 核心工作流

### 1. 自动研究与投委会

`run_discussion.py` 会从议题池中选择议题，联网拉取并保存研究材料，再从有明确证据的资料中抽取投资提案，最后让多智能体团队进行四轮分析、质疑、修正和投票。模型没有给出有证据支持的提案时，本轮保持空结果，不再注入预设 ETF 或虚构候选。

阶段2的每次终态会追加保存到 `research_stage_diagnostics`。若模型回答里出现
具体ticker但JSON提案丢失，系统先做一次格式修复，再只针对原回答已出现且原始
资料独立支持的ticker做一次证据审计；仍不合格就保持空数组，并把原因回灌下一轮。
空仓部署议题出现法定人数HOLD时，证据最强的一个提案可进入一次CIO/风控/量化
死锁复核，但只有高置信、高方向支持的复核才能改变HOLD，不会为了制造交易降低
证据门槛。

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
- 每个交易日买入、卖出、止损、止盈、超期退出和调仓合计最多5笔
- 非交易日或非A股实际交易时段只记录待执行裁决，不伪造成交
- 每次模拟成交前重新获取受控实时行情；调用方价格、成本价、历史日线和预测价
  均不能绕过实时重取
- 任一持仓实时行情缺失时组合估值为N/A，并禁止新增或扩大仓位

相关表：

- `simulation_positions`
- `simulation_trades`
- `simulation_snapshots`
- `system_stats`

### 5. Heuristic Learning 与离线诊断

`scripts/run_heuristic_cycle.py` 是 coding agent 的本地迭代入口。它先读取实时估值后的模拟账户作为唯一绩效，再读取本地 SQLite 数据做可复现的离线诊断；不使用网页资料搜索，也不下单。这不改变产品运行入口默认联网研究的行为。

它会：

1. 读取 `price_predictions`
2. 构建按日聚合的信号带
3. 把模拟账户实时净值收益写入 `simulation_account_metrics.json` 和兼容的 `best_metrics.json`
4. 测试少量可解释规则，但全部标记为 `offline_diagnostic_only`、`promotion_eligible=false`
5. 输出系统失败案例、离线诊断、入口影响和回归结果；没有新增模拟成交时，best 与本轮改善均为 N/A

输出目录示例：

```text
runs/heuristic_cycle/20260724_143322/
├── README.md
├── summary.csv
├── trials.jsonl
├── simulation_account_metrics.json
├── best_metrics.json
├── failure_cases.jsonl
├── offline_diagnostic_baseline_metrics.json
├── offline_diagnostic_best_metrics.json
├── offline_diagnostic_overfit_checks.json
├── offline_diagnostic_failure_cases.jsonl
├── daily_signal_tape.csv
├── offline_diagnostic_equity_curve.csv
├── offline_diagnostic_trades.csv
├── offline_diagnostic_policy_snapshot.py
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
├── application/
│   ├── run_research_round.py
│   ├── execute_simulation_cycle.py
│   ├── get_system_status.py
│   └── answer_research_question.py
├── domain/
│   ├── research/
│   └── portfolio/
├── ports/
│   ├── unit_of_work.py
│   └── quote_provider.py
├── infrastructure/
│   └── sqlite/
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
│   ├── test_refactor_pipeline.py
│   └── test_architecture_refactor.py
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
  slippage_rate: 0.0005
  max_daily_trades: 5
  max_realtime_quote_age_seconds: 120

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
| `research_rounds` / `round_events` | 研究轮次状态与有序事件 |
| `execution_intents` | 不含调用方成交价的模拟执行意图 |
| `quote_snapshots` | 成交所用实时行情及来源时间 |
| `simulation_ledger_entries` | 成交现金与费用台账 |
| `system_stats` | 系统状态和模拟现金等键值数据 |
| `blacklist` | 需要规避的标的或模式 |
| `playbook` | 机构经验库 |

## 测试与验证

项目已在 `pyproject.toml` 中固定测试包路径。从项目目录运行：

```bash
cd /Users/wangziming/PycharmProjects/PythonProject/sovereign_hall
pytest -q
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
- 当前没有可由离线诊断晋升的 best policy；本轮零新增模拟成交时改善为 N/A。
- 爬虫配置较保守，默认启用代理并降低频率，避免请求过密。
- 数据库和 `runs/` 产物可能很大，提交代码前应确认是否需要纳入版本管理。
