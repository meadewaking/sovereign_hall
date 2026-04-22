# 🏛️ Project Sovereign Hall - 君临殿

## 全自动化多智能体投资研究系统

> 一个模拟完整买方投资机构的AI系统，通过对抗性多智能体辩论机制，生成高质量投资决策。

---

## 🎬 三个一键脚本

| 脚本 | 功能 | 使用场景 |
|-----|------|---------|
| `check_db.py` | 查看数据库统计和投资状态 | 了解系统有多少数据、当前资产情况 |
| `run_discussion.py` | 持续自动讨论+模拟投资 | 让AI自动研究，积累结论并执行模拟交易 |
| `research_interactive.py` | 交互式研究 | 输入你的问题，AI给出投资建议 |

---

## 🚀 快速开始

```bash
cd sovereign_hall/

# 1. 查看当前数据库和投资状态
python -m sovereign_hall.check_db

# 2. 让AI自动研究，模拟投资（Ctrl+C停止）
python -m sovereign_hall.run_discussion

# 3. 输入你的问题，让AI给出投资建议
python -m sovereign_hall.research_interactive
```

---

## 📁 项目结构

```
sovereign_hall/
├── README.md                      # 本文档
├── check_db.py                    # 查看数据库统计和投资状态
├── run_discussion.py              # 持续自动讨论+模拟投资
├── research_interactive.py        # 交互式研究
├── config.yaml                    # 配置文件
├── requirements.txt               # 依赖
│
├── data/
│   ├── sovereign_hall.db          # SQLite数据库（异步）
│   ├── logs/                      # 日志目录（自动轮转）
│   ├── vector_db/                 # 向量数据库（LRU缓存）
│   └── reports/                   # 报告输出
│
├── core/                          # 核心模块
├── agents/                        # 7种智能体人格
├── services/                      # 核心服务
│   ├── llm_client.py              # LLM客户端（支持embedding）
│   ├── spider_service.py          # 爬虫服务（防封策略）
│   ├── database.py                # 异步数据库服务
│   ├── vector_db.py               # 向量数据库（LRU+持久化）
│   ├── investment_simulation.py   # 投资模拟服务
│   └── ...
└── utils/                         # 工具函数
```

---

## 🧠 系统特性

### 7智能体团队

| 角色 | 名字 | 风格 |
|-----|------|------|
| TMT分析师 | 张科技 | 激进乐观 |
| 消费分析师 | 李稳健 | 保守谨慎 |
| 周期分析师 | 王周期 | 周期主义 |
| 宏观策略 | 赵宏观 | 鹰派现实 |
| 风控官 | 刘挑刺 | 悲观主义 |
| 量化研究 | 钱量化 | 数据至上 |
| 投资总监 | 陈总监 | 平衡者 |

### 4阶段讨论流程

1. **阶段1：海量搜索** - 并发爬取相关文档（防封策略+缓存）
2. **阶段2：深度研报** - 从文档提取投资提案
3. **阶段3：投委会辩论** - 多智能体轮询分析（共享搜索缓存）
4. **阶段4：综合结论** - 生成最终投资建议

### 🔄 学习闭环系统

系统具备**决策→验证→学习**的闭环能力：

| 模块 | 功能 |
|------|------|
| `decision_tracker.py` | 记录每次投票决策（ticker、方向、置信度、目标/止损） |
| `learning_engine.py` | 分析错误特征，生成历史教训Prompt |
| 验证机制 | 定时验证决策是否命中目标/止损（7天/30天） |
| 胜率追踪 | 实时统计预测准确率并显示 |

**工作流程**：
- 讨论结束后自动记录决策
- 定时验证决策结果（使用AKShare获取实时价格）
- 从错误决策中提取教训，注入下次讨论
- 自动更新playbook经验库

### 💰 投资模拟

- 初始资金：10,000元
- 每日基于提案执行买入/卖出/持有
- 交易记录和资产变化存入数据库
- 每日生成投资反思
- 支持置信度分级仓位管理

---

## 🔧 技术特性

### 数据库
- **aiosqlite** 异步SQLite，避免并发锁死
- 向量数据库支持LRU淘汰和自动持久化
- 统一DatabaseService访问层

### 爬虫（已优化）
- 并发控制：10个并发
- 请求频率：30次/分钟
- 搜索间隔：0.5秒
- 告警模式：连续失败5次后进入，30秒自动恢复
- **搜索缓存**：相同查询词1小时内直接返回缓存，减少重复请求约50%

### 日志
- 自动轮转，保留最近10份
- 启动时自动清理旧日志

### 内存管理
- Agent记忆绑定到议题，不跨话题污染
- VectorDB最大10000条，LRU淘汰

---

## 📊 代码统计

- **总代码量**: ~12,000+ 行 Python
- **核心模块**:
  - `services/llm_client.py` - LLM + Embedding
  - `services/spider_service.py` - 分布式爬虫（带缓存）
  - `services/database.py` - 异步数据库
  - `services/vector_db.py` - 向量检索
  - `services/investment_simulation.py` - 投资模拟
  - `services/decision_tracker.py` - 决策追踪（学习闭环）
  - `services/learning_engine.py` - 学习引擎
  - `run_discussion.py` - 主循环

---

## ⚙️ 配置说明

主要配置在 `config.yaml`：

```yaml
# LLM配置
llm:
  provider: "openai"
  base_url: "http://172.18.1.128:30977/v1"
  model: "MiniMax/MiniMax-M2.5"
  embedding_model: "bge-large-zh-v1.5"

# 爬虫配置
spider:
  max_concurrent: 10
  requests_per_minute: 30
  search_interval: 0.5

# 投资模拟
simulation:
  enabled: true
  initial_capital: 10000
```

---

**⚠️ 风险提示**

本系统仅供研究学习使用，不构成投资建议。

---

<div align="center">

**🏛️ 君临殿 - Where AI Agents Deliberate**

</div>