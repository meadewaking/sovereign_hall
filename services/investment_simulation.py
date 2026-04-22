"""
🏛️ Sovereign Hall - 投资模拟服务
模拟每日投资操作，记录交易和资产变化
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from ..core import DATA_DIR
from ..services.llm_client import LLMClient

logger = logging.getLogger(__name__)


class InvestmentSimulation:
    """投资模拟器"""

    def __init__(self, db_service=None):
        self.db_service = db_service
        self.config = self._load_config()

        # 模拟参数
        self.initial_capital = self.config.get('initial_capital', 10000)
        self.min_unit = self.config.get('min_unit', 100)  # 一手=100股
        self.trading_fee = self.config.get('trading_fee', 0.0003)  # 佣金万三
        self.stamp_duty = self.config.get('stamp_duty', 0.001)  # 印花税千一

        # 当前持仓
        self.positions: Dict[str, Dict] = {}  # {ticker: {shares, avg_cost}}
        self.cash = self.initial_capital
        self.last_trade_date = None

        # 交易冷却期配置（天）
        self.cooldown_days = 3  # 同一只股票至少隔3天才能再次交易
        self.last_trade_records: Dict[str, str] = {}  # {ticker: last_trade_date isoformat}

    def _load_config(self) -> Dict:
        """加载配置"""
        try:
            from ..core.config import get_config
            config = get_config()
            return config.get('simulation', {})
        except:
            return {}

    async def initialize(self):
        """初始化，从数据库加载上次状态"""
        if not self.db_service:
            return

        try:
            conn = self.db_service._connection

            # 加载现金
            async with conn.execute(
                "SELECT value FROM system_stats WHERE key = 'simulation_cash'"
            ) as cursor:
                cash_row = await cursor.fetchone()
            if cash_row:
                self.cash = float(cash_row[0])

            # 加载持仓
            async with conn.execute(
                "SELECT ticker, shares, avg_cost FROM simulation_positions"
            ) as cursor:
                async for row in cursor:
                    self.positions[row[0]] = {
                        'shares': row[1],
                        'avg_cost': row[2]
                    }

            # 加载上次交易日期
            async with conn.execute(
                "SELECT value FROM system_stats WHERE key = 'last_trade_date'"
            ) as cursor:
                date_row = await cursor.fetchone()
            if date_row:
                self.last_trade_date = datetime.fromisoformat(date_row[0])

            logger.info(f"Simulation initialized: cash={self.cash}, positions={len(self.positions)}")

            # 加载交易记录用于冷却期判断
            await self._load_trade_records_for_cooldown()
        except Exception as e:
            logger.warning(f"Failed to load simulation state: {e}")

    async def _load_trade_records_for_cooldown(self):
        """加载最近的交易记录，用于冷却期判断"""
        if not self.db_service:
            return

        try:
            conn = self.db_service._connection
            # 获取每只股票的最后交易日期
            async with conn.execute("""
                SELECT ticker, MAX(traded_at) as last_trade
                FROM simulation_trades
                GROUP BY ticker
            """) as cursor:
                async for row in cursor:
                    self.last_trade_records[row[0]] = row[1]

            logger.info(f"Loaded cooldown records: {len(self.last_trade_records)} tickers")
        except Exception as e:
            logger.warning(f"Failed to load trade records: {e}")

    def is_in_cooldown(self, ticker: str) -> bool:
        """检查股票是否在冷却期内"""
        last_trade = self.last_trade_records.get(ticker)
        if not last_trade:
            return False

        try:
            last_date = datetime.fromisoformat(last_trade)
            days_since = (datetime.now() - last_date).days
            return days_since < self.cooldown_days
        except:
            return False

    async def save_state(self):
        """保存状态到数据库"""
        if not self.db_service:
            return

        try:
            conn = self.db_service._connection

            # 保存现金
            await conn.execute(
                "INSERT OR REPLACE INTO system_stats (key, value, updated_at) VALUES (?, ?, ?)",
                ('simulation_cash', str(self.cash), datetime.now().isoformat())
            )

            # 保存上次交易日期
            if self.last_trade_date:
                await conn.execute(
                    "INSERT OR REPLACE INTO system_stats (key, value, updated_at) VALUES (?, ?, ?)",
                    ('last_trade_date', self.last_trade_date.isoformat(), datetime.now().isoformat())
                )

            # 保存持仓
            for ticker, pos in self.positions.items():
                await conn.execute("""
                    INSERT OR REPLACE INTO simulation_positions (ticker, shares, avg_cost, updated_at)
                    VALUES (?, ?, ?, ?)
                """, (ticker, pos['shares'], pos['avg_cost'], datetime.now().isoformat()))

            # 删除不在当前持仓中的股票
            if self.positions:
                placeholders = ','.join(['?' for _ in self.positions])
                await conn.execute(f"""
                    DELETE FROM simulation_positions WHERE ticker NOT IN ({placeholders})
                """, list(self.positions.keys()))
            else:
                await conn.execute("DELETE FROM simulation_positions")

            await conn.commit()
        except Exception as e:
            logger.warning(f"Failed to save simulation state: {e}")

    def get_current_price(self, ticker: str) -> Optional[float]:
        """获取当前价格（这里需要接入真实行情或使用AI预测）"""
        # TODO: 接入真实行情API
        # 暂时返回模拟价格
        return None

    async def execute_trade(
        self,
        ticker: str,
        direction: str,
        target_position: float,
        current_price: float,
        llm: LLMClient = None,
        reason: str = ""
    ) -> Dict:
        """
        执行交易（支持买入、卖出、持有）

        Args:
            ticker: 股票代码
            direction: long/short
            target_position: 目标仓位比例 (0=清仓, 0.5=半仓, 1.0=满仓)
            current_price: 当前价格
            llm: LLM客户端（用于获取价格）
            reason: 交易原因

        Returns:
            交易结果
        """
        # 检查冷却期
        if self.is_in_cooldown(ticker):
            return {
                'success': False,
                'action': 'hold',
                'ticker': ticker,
                'reason': f'冷却期内，上次交易{self.last_trade_records.get(ticker, "")[:10]}'
            }

        price = current_price or 10.0

        # 计算当前持仓
        current_shares = self.positions.get(ticker, {}).get('shares', 0)
        current_position_value = current_shares * price
        total_assets = self.cash + current_position_value
        current_position_pct = current_position_value / total_assets if total_assets > 0 else 0

        target_value = total_assets * target_position
        diff_value = target_value - current_position_value

        # === 买入 ===
        if diff_value > 0:
            # 需要买入
            buy_amount = diff_value
            # 扣除手续费
            buy_amount_with_fee = buy_amount / (1 + self.trading_fee)
            shares_to_buy = int(buy_amount_with_fee / price / self.min_unit) * self.min_unit

            if shares_to_buy <= 0:
                return {'success': True, 'action': 'hold', 'reason': '金额不足一手'}

            cost = shares_to_buy * price
            fee = cost * self.trading_fee
            total_cost = cost + fee

            if total_cost > self.cash:
                # 资金不足，调整数量
                max_shares = int(self.cash / price / (1 + self.trading_fee) / self.min_unit) * self.min_unit
                if max_shares <= 0:
                    return {'success': True, 'action': 'hold', 'reason': '资金不足'}
                shares_to_buy = max_shares
                cost = shares_to_buy * price
                fee = cost * self.trading_fee
                total_cost = cost + fee

            # 执行买入
            self.cash -= total_cost

            if ticker in self.positions:
                old_shares = self.positions[ticker]['shares']
                old_cost = self.positions[ticker]['avg_cost'] * old_shares
                new_shares = old_shares + shares_to_buy
                new_cost = old_cost + cost
                self.positions[ticker] = {
                    'shares': new_shares,
                    'avg_cost': new_cost / new_shares
                }
            else:
                self.positions[ticker] = {
                    'shares': shares_to_buy,
                    'avg_cost': price
                }

            self.last_trade_date = datetime.now()
            self.last_trade_records[ticker] = datetime.now().isoformat()

            # 记录交易
            await self._record_trade(
                ticker=ticker,
                direction='buy',
                shares=shares_to_buy,
                price=price,
                fee=fee,
                reason=reason or f"加仓至{target_position*100:.0f}%"
            )

            await self.save_state()

            return {
                'success': True,
                'action': 'buy',
                'ticker': ticker,
                'shares': shares_to_buy,
                'price': price,
                'cost': total_cost,
                'remaining_cash': self.cash
            }

        # === 卖出 ===
        elif diff_value < 0 and current_shares > 0:
            # 需要卖出
            sell_value = abs(diff_value)
            shares_to_sell = int(sell_value / price / self.min_unit) * self.min_unit

            if shares_to_sell <= 0:
                return {'success': True, 'action': 'hold', 'reason': '数量不足一手'}

            if shares_to_sell > current_shares:
                shares_to_sell = current_shares

            proceeds = shares_to_sell * price
            trading_fee = proceeds * self.trading_fee
            stamp_duty = proceeds * self.stamp_duty  # 印花税
            total_fee = trading_fee + stamp_duty
            net_proceeds = proceeds - total_fee

            # 执行卖出
            self.cash += net_proceeds
            remaining_shares = current_shares - shares_to_sell

            if remaining_shares > 0:
                self.positions[ticker]['shares'] = remaining_shares
            else:
                del self.positions[ticker]

            self.last_trade_date = datetime.now()
            self.last_trade_records[ticker] = datetime.now().isoformat()

            # 记录交易
            await self._record_trade(
                ticker=ticker,
                direction='sell',
                shares=shares_to_sell,
                price=price,
                fee=total_fee,
                reason=reason or f"减仓至{target_position*100:.0f}%"
            )

            await self.save_state()

            return {
                'success': True,
                'action': 'sell',
                'ticker': ticker,
                'shares': shares_to_sell,
                'price': price,
                'proceeds': net_proceeds,
                'remaining_cash': self.cash
            }

        # === 持有 ===
        return {'success': True, 'action': 'hold', 'reason': '仓位合适'}

    async def _record_trade(
        self,
        ticker: str,
        direction: str,
        shares: int,
        price: float,
        fee: float,
        reason: str = ""
    ):
        """记录交易到数据库"""
        if not self.db_service:
            return

        try:
            conn = self.db_service._connection
            await conn.execute("""
                INSERT INTO simulation_trades (ticker, direction, shares, price, fee, reason, traded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker, direction, shares, price, fee, reason, datetime.now().isoformat()
            ))
            await conn.commit()
        except Exception as e:
            logger.warning(f"Failed to record trade: {e}")

    async def calculate_assets(self, prices: Dict[str, float] = None) -> Dict:
        """计算当前总资产"""
        total_value = self.cash

        for ticker, pos in self.positions.items():
            price = (prices or {}).get(ticker) or self.get_current_price(ticker) or pos['avg_cost']
            total_value += pos['shares'] * price

        return {
            'cash': self.cash,
            'positions_value': total_value - self.cash,
            'total_assets': total_value,
            'positions': self.positions.copy(),
            'last_trade_date': self.last_trade_date.isoformat() if self.last_trade_date else None
        }

    async def daily_reflection(self, llm: LLMClient = None) -> str:
        """
        每日反思
        读取上次记录，计算当前资产，对投资决策进行反思
        """
        if not llm:
            return ""

        # 获取历史交易记录
        trades = await self.get_trade_history(days=7)

        # 获取当前资产
        assets = await self.calculate_assets()

        # 获取持仓详情
        positions_text = ""
        for ticker, pos in assets['positions'].items():
            positions_text += f"- {ticker}: {pos['shares']}股, 成本 {pos['avg_cost']:.2f}\n"

        # 构建反思Prompt
        reflection_prompt = f"""
你是一位专业投资顾问，请对过去的投资操作进行反思总结。

【当前资产状态】
- 现金: {assets['cash']:.2f} 元
- 持仓市值: {assets['positions_value']:.2f} 元
- 总资产: {assets['total_assets']:.2f} 元
- 初始资金: {self.initial_capital} 元
- 收益率: {((assets['total_assets'] / self.initial_capital) - 1) * 100:.2f}%

【当前持仓】
{positions_text}

【最近交易记录】
"""

        for trade in trades[-10:]:
            reflection_prompt += f"- {trade['traded_at'][:10]} {trade['direction']} {trade['ticker']} {trade['shares']}股 @ {trade['price']:.2f}\n"

        reflection_prompt += """
请进行以下分析：
1. 当前资产变化分析
2. 持仓盈亏情况
3. 交易策略是否有效
4. 下一步投资建议

请用简洁的中文回答。
"""

        try:
            response = await llm.chat(
                system="你是专业投资顾问",
                user=reflection_prompt,
                temperature=0.5,
                max_tokens=1000
            )
            return response
        except Exception as e:
            logger.warning(f"Failed to generate reflection: {e}")
            return f"反思生成失败: {e}"

    async def get_recent_reflection(self, limit: int = 3) -> str:
        """获取最近N条反思摘要"""
        if not self.db_service:
            return ""

        try:
            conn = self.db_service._connection
            async with conn.execute("""
                SELECT reflection, snapshot_date
                FROM simulation_snapshots
                WHERE reflection IS NOT NULL AND reflection != ''
                ORDER BY snapshot_date DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return "暂无历史反思"

            result = "【历史投资反思】\n"
            for i, row in enumerate(rows, 1):
                result += f"\n{i}. ({row[1][:10]}): {row[0][:200]}...\n"
            return result
        except Exception as e:
            logger.warning(f"Failed to get recent reflection: {e}")
            return "暂无历史反思"

    async def get_trade_history(self, days: int = 30, limit: int = 50) -> List[Dict]:
        """获取交易历史"""
        if not self.db_service:
            return []

        try:
            conn = self.db_service._connection
            async with conn.execute("""
                SELECT id, ticker, direction, shares, price, fee, reason, traded_at
                FROM simulation_trades
                ORDER BY traded_at DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [
                    {
                        'id': row[0],
                        'ticker': row[1],
                        'direction': row[2],
                        'shares': row[3],
                        'price': row[4],
                        'fee': row[5],
                        'reason': row[6],
                        'traded_at': row[7]
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.warning(f"Failed to get trade history: {e}")
            return []

    async def init_tables(self):
        """初始化数据库表"""
        if not self.db_service:
            return

        conn = self.db_service._connection

        # 交易记录表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,
                shares INTEGER NOT NULL,
                price REAL NOT NULL,
                fee REAL NOT NULL,
                reason TEXT,
                traded_at TEXT NOT NULL
            )
        """)

        # 持仓表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_positions (
                ticker TEXT PRIMARY KEY,
                shares INTEGER NOT NULL,
                avg_cost REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # 资产快照表（每日）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_assets REAL NOT NULL,
                cash REAL NOT NULL,
                positions_value REAL NOT NULL,
                reflection TEXT,
                snapshot_date TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        await conn.commit()

    async def save_snapshot(self, reflection: str = ""):
        """保存每日资产快照"""
        if not self.db_service:
            return

        try:
            assets = await self.calculate_assets()
            conn = self.db_service._connection

            await conn.execute("""
                INSERT INTO simulation_snapshots (total_assets, cash, positions_value, reflection, snapshot_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                assets['total_assets'],
                assets['cash'],
                assets['positions_value'],
                reflection[:2000] if reflection else "",
                datetime.now().date().isoformat(),
                datetime.now().isoformat()
            ))
            await conn.commit()
            logger.info(f"Saved snapshot: total={assets['total_assets']}")
        except Exception as e:
            logger.warning(f"Failed to save snapshot: {e}")


# 辅助函数：运行每日模拟
async def run_daily_simulation(llm: LLMClient, db_service, proposals: List[Dict] = None):
    """
    运行每日模拟投资

    Args:
        llm: LLM客户端
        db_service: 数据库服务
        proposals: 提案列表（可选，用于决策）
    """
    simulation = InvestmentSimulation(db_service)
    await simulation.initialize()
    await simulation.init_tables()

    # 计算当前资产
    assets = await simulation.calculate_assets()

    # 如果有提案，可以执行交易
    if proposals:
        for proposal in proposals[:3]:  # 最多处理3个提案
            ticker = proposal.get('ticker')
            direction = proposal.get('direction', 'long')
            target_position = proposal.get('target_position', 0.1)

            # 获取当前价格（这里需要真实行情）
            current_price = 10.0  # 模拟价格

            result = await simulation.execute_trade(
                ticker=ticker,
                direction=direction,
                target_position=target_position,
                current_price=current_price,
                llm=llm
            )

            if result.get('success'):
                logger.info(f"Simulation trade: {result}")

    # 生成每日反思
    reflection = await simulation.daily_reflection(llm)

    # 保存快照
    await simulation.save_snapshot(reflection)

    return assets, reflection


async def show_investment_status(db_service) -> str:
    """
    显示投资状态（供 check_db.py 调用）
    """
    simulation = InvestmentSimulation(db_service)
    await simulation.initialize()

    assets = await simulation.calculate_assets()
    trades = await simulation.get_trade_history(days=30, limit=20)

    initial = simulation.initial_capital
    total = assets['total_assets']
    profit = total - initial
    profit_pct = (profit / initial) * 100

    status = f"""
📊 投资模拟状态
================
初始资金: {initial:.2f} 元
当前资产: {total:.2f} 元
{'📈' if profit >= 0 else '📉'} 盈亏: {profit:+.2f} 元 ({profit_pct:+.2f}%)
现金: {assets['cash']:.2f} 元
持仓市值: {assets['positions_value']:.2f} 元

📦 当前持仓:
"""
    if assets['positions']:
        for ticker, pos in assets['positions'].items():
            status += f"  {ticker}: {pos['shares']}股 @ 成本 {pos['avg_cost']:.2f}\n"
    else:
        status += "  (空仓)\n"

    status += f"\n📜 最近交易 ({len(trades)}条):\n"
    for trade in trades[:10]:
        status += f"  {trade['traded_at'][:10]} {trade['direction']} {trade['ticker']} {trade['shares']}股 @ {trade['price']:.2f}\n"

    return status