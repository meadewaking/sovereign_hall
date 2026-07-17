"""
🏛️ Sovereign Hall - 投资模拟服务
模拟每日投资操作，记录交易和资产变化
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from ..core import DATA_DIR
from ..services.llm_client import LLMClient
from ..services.heuristic_policy import (
    SIMULATION_RISK_LOSS_THRESHOLD,
    SIMULATION_RISK_MEMORY_DAYS,
    apply_heuristic_risk_cap,
    derive_simulation_risk_memory,
    recent_prediction_observation_count,
)
from ..services.portfolio_policy import deployment_status, review_position
from ..services.reward_policy import MAX_DAILY_TRADES

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
        self.target_invested_ratio = float(self.config.get('target_invested_ratio', 1.0))
        self.realtime_quotes_required = bool(self.config.get('realtime_quotes_required', True))
        self.trade_during_market_hours_only = bool(
            self.config.get('trade_during_market_hours_only', True)
        )
        self.max_trade_price_age_days = int(self.config.get('max_trade_price_age_days', 3))
        self.stop_loss_pct = float(self.config.get('stop_loss_pct', -0.08))
        self.take_profit_pct = float(self.config.get('take_profit_pct', 0.15))
        self.max_holding_days = int(self.config.get('max_holding_days', 30))
        self.max_daily_trades = int(self.config.get('max_daily_trades', MAX_DAILY_TRADES))

        # 当前持仓
        self.positions: Dict[str, Dict] = {}  # {ticker: {shares, avg_cost}}
        self.cash = self.initial_capital
        self.last_trade_date = None

        # 交易冷却期配置（天）
        self.cooldown_days = 3  # 同一只股票至少隔3天才能再次交易
        self.last_trade_records: Dict[str, str] = {}  # {ticker: last_trade_date isoformat}
        self.risk_memory_loss_threshold = SIMULATION_RISK_LOSS_THRESHOLD
        self.risk_memory_days = SIMULATION_RISK_MEMORY_DAYS

    def _load_config(self) -> Dict:
        """加载配置"""
        try:
            from ..core.config import get_config
            config = get_config()
            return config.get('simulation', {})
        except Exception as exc:
            logger.warning("加载投资模拟配置失败，使用默认参数: %s", exc)
            return {}

    async def initialize(self):
        """初始化，从数据库加载上次状态"""
        if not self.db_service:
            return

        try:
            await self.init_tables()
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
                """
                SELECT ticker, shares, avg_cost, opened_at, peak_price,
                       last_mark_price, last_mark_at, last_mark_source,
                       last_reviewed_at, review_status, review_reason
                FROM simulation_positions
                """
            ) as cursor:
                async for row in cursor:
                    self.positions[row[0]] = {
                        'shares': row[1],
                        'avg_cost': row[2],
                        'opened_at': row[3],
                        'peak_price': row[4],
                        'last_mark_price': row[5],
                        'last_mark_at': row[6],
                        'last_mark_source': row[7],
                        'last_reviewed_at': row[8],
                        'review_status': row[9],
                        'review_reason': row[10],
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
            await self._bootstrap_redeployment_state()
        except Exception as e:
            logger.warning(f"Failed to load simulation state: {e}")

    async def _bootstrap_redeployment_state(self) -> None:
        """Recover an actionable deployment queue after a process restart.

        An empty book needs no quote, so its full cash deployment gap is known
        exactly.  This recovery never invents a price or a trade.
        """
        if not self.db_service or self.positions or self.cash <= 0:
            return
        current = await self.get_redeployment_state()
        if current and current.get("status") not in {"completed", "not_required"}:
            return
        await self._write_redeployment_state(
            status="pending_approved_candidates",
            deployment_gap=self.cash,
            blocker_code="missing_approved_candidates",
            blocker_reason="空仓资金等待投委会批准且可取得实时行情的合格标的",
            next_action="下一轮投委会先消费该队列；成交前重新取得实时行情",
            source="account_state_recovery",
        )

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
        except Exception as exc:
            logger.warning("解析最近交易日期失败 %s=%r: %s", ticker, last_trade, exc)
            return False

    async def _estimate_trade_assets(
        self,
        ticker: str,
        price: float,
    ) -> tuple[Dict[str, float], float, List[str]]:
        """Value the portfolio from realtime quotes before sizing a simulated trade."""
        assets = await self.calculate_assets()
        return (
            dict(assets.get("position_values") or {}),
            float(assets.get("known_total_assets") or 0.0),
            list(assets.get("missing_price_tickers") or []),
        )

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
                    INSERT OR REPLACE INTO simulation_positions (
                        ticker, shares, avg_cost, updated_at, opened_at, peak_price,
                        last_mark_price, last_mark_at, last_mark_source,
                        last_reviewed_at, review_status, review_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker, pos['shares'], pos['avg_cost'], datetime.now().isoformat(),
                    pos.get('opened_at'), pos.get('peak_price'), pos.get('last_mark_price'),
                    pos.get('last_mark_at'), pos.get('last_mark_source'),
                    pos.get('last_reviewed_at'), pos.get('review_status'), pos.get('review_reason'),
                ))

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

    async def get_redeployment_state(self) -> Dict[str, Any]:
        """Return the durable operational-cash deployment state."""
        if not self.db_service:
            return {}
        conn = self.db_service._connection
        try:
            async with conn.execute(
                """
                SELECT status, deployment_gap, blocker_code, blocker_reason,
                       next_action, source, attempt_count, last_attempt_at,
                       last_candidate_count, last_trade_count,
                       last_rejection_counts, rejection_counts_total,
                       created_at, updated_at, completed_at
                FROM simulation_redeployment_state WHERE id = 1
                """
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return {}
            result = dict(row)
            for key in ("last_rejection_counts", "rejection_counts_total"):
                raw = result.get(key)
                try:
                    result[key] = json.loads(raw) if raw else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    result[key] = {}
            return result
        except Exception:
            return {}

    async def _write_redeployment_state(
        self,
        *,
        status: str,
        deployment_gap: float | None,
        blocker_code: str,
        blocker_reason: str,
        next_action: str,
        source: str,
        increment_attempt: bool = False,
        candidate_count: int = 0,
        trade_count: int = 0,
        rejection_counts: Dict[str, int] | None = None,
    ) -> None:
        if not self.db_service:
            return
        conn = self.db_service._connection
        now = datetime.now().isoformat()
        completed_at = now if status == "completed" else None
        normalized_rejections = {
            str(code): int(count)
            for code, count in (rejection_counts or {}).items()
            if str(code).strip() and int(count) > 0
        }
        cumulative_rejections: Dict[str, int] = {}
        try:
            async with conn.execute(
                "SELECT rejection_counts_total FROM simulation_redeployment_state WHERE id = 1"
            ) as cursor:
                existing_row = await cursor.fetchone()
            if existing_row and existing_row[0]:
                cumulative_rejections = {
                    str(code): int(count)
                    for code, count in json.loads(existing_row[0]).items()
                }
        except (TypeError, ValueError, json.JSONDecodeError, aiosqlite.Error):
            cumulative_rejections = {}
        if increment_attempt:
            for code, count in normalized_rejections.items():
                cumulative_rejections[code] = cumulative_rejections.get(code, 0) + count
        last_rejections_json = json.dumps(normalized_rejections, ensure_ascii=False, sort_keys=True)
        cumulative_rejections_json = json.dumps(cumulative_rejections, ensure_ascii=False, sort_keys=True)
        await conn.execute(
            """
            INSERT INTO simulation_redeployment_state (
                id, status, deployment_gap, blocker_code, blocker_reason,
                next_action, source, attempt_count, last_attempt_at,
                last_candidate_count, last_trade_count, last_rejection_counts,
                rejection_counts_total, created_at, updated_at, completed_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                deployment_gap = excluded.deployment_gap,
                blocker_code = excluded.blocker_code,
                blocker_reason = excluded.blocker_reason,
                next_action = excluded.next_action,
                source = excluded.source,
                attempt_count = simulation_redeployment_state.attempt_count + ?,
                last_attempt_at = CASE WHEN ? THEN excluded.last_attempt_at ELSE simulation_redeployment_state.last_attempt_at END,
                last_candidate_count = CASE WHEN ? THEN excluded.last_candidate_count ELSE simulation_redeployment_state.last_candidate_count END,
                last_trade_count = CASE WHEN ? THEN excluded.last_trade_count ELSE simulation_redeployment_state.last_trade_count END,
                last_rejection_counts = CASE WHEN ? THEN excluded.last_rejection_counts ELSE simulation_redeployment_state.last_rejection_counts END,
                rejection_counts_total = excluded.rejection_counts_total,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at
            """,
            (
                status, deployment_gap, blocker_code, blocker_reason, next_action,
                source, 1 if increment_attempt else 0,
                now if increment_attempt else None, candidate_count, trade_count,
                last_rejections_json, cumulative_rejections_json,
                now, now, completed_at,
                1 if increment_attempt else 0,
                1 if increment_attempt else 0,
                1 if increment_attempt else 0,
                1 if increment_attempt else 0,
                1 if increment_attempt else 0,
            ),
        )
        await conn.commit()

    async def mark_redeployment_required(self, proceeds: float, source: str) -> None:
        """Put released simulated-sale cash into the durable redeployment queue."""
        if proceeds <= 0:
            return
        await self._write_redeployment_state(
            status="pending_approved_candidates",
            deployment_gap=self.cash if not self.positions else None,
            blocker_code="released_capital_pending_redeployment",
            blocker_reason=f"模拟卖出释放 {proceeds:.2f} 元，等待同一投资闭环再配置",
            next_action="投委会提供合格标的后，以实时行情重新估值并模拟成交",
            source=source,
        )

    async def record_redeployment_attempt(
        self,
        assets: Dict[str, Any],
        *,
        candidate_count: int,
        trade_count: int,
        blockers: List[str] | None = None,
        rejections: List[Dict[str, Any]] | None = None,
        source: str = "run_discussion",
    ) -> Dict[str, Any]:
        """Persist one committee redeployment attempt and its exact blocker."""
        blockers = [str(item) for item in (blockers or []) if str(item).strip()]
        rejection_counts: Dict[str, int] = {}
        for item in rejections or []:
            code = str(item.get("code") or "unknown_rejection").strip()
            rejection_counts[code] = rejection_counts.get(code, 0) + 1
        rejection_summary = ", ".join(
            f"{code}={count}" for code, count in sorted(rejection_counts.items())
        )
        if not assets.get("valuation_complete"):
            status = "blocked_valuation_incomplete"
            gap = None
            blocker_code = "realtime_valuation_incomplete"
            blocker_reason = "组合实时估值不完整；" + "；".join(blockers)
            next_action = "补齐所有持仓实时行情后重试；禁止用旧价或成本价兜底"
        else:
            gap = float(assets.get("deployment_gap") or 0.0)
            if gap <= 0.01:
                status = "completed"
                blocker_code = ""
                blocker_reason = "资金部署达到100%目标"
                next_action = "继续逐仓生命周期复核"
            elif trade_count > 0:
                status = "partially_redeployed"
                blocker_code = "residual_operational_cash"
                blocker_reason = "已完成部分再配置；" + ("；".join(blockers) or "余款受整手/手续费约束")
                next_action = "下一轮继续消费剩余部署缺口"
            elif candidate_count <= 0:
                status = "blocked_no_approved_candidates"
                blocker_code = "missing_approved_candidates"
                blocker_reason = "投委会没有批准可执行的多头候选；" + "；".join(blockers)
                next_action = (
                    f"下一轮优先处理拒绝码: {rejection_summary}；补齐对应证据或输入后再提交候选"
                    if rejection_summary
                    else "下一轮投委会必须给出合格候选或逐项记录证据否决原因"
                )
            else:
                status = "blocked_candidate_execution"
                blocker_code = "candidate_execution_blocked"
                blocker_reason = "候选未能成交；" + ("；".join(blockers) or "未返回可执行成交")
                next_action = "按记录的实时行情/证据/整手阻塞逐项重试"
        await self._write_redeployment_state(
            status=status,
            deployment_gap=gap,
            blocker_code=blocker_code,
            blocker_reason=blocker_reason,
            next_action=next_action,
            source=source,
            increment_attempt=True,
            candidate_count=candidate_count,
            trade_count=trade_count,
            rejection_counts=rejection_counts,
        )
        return await self.get_redeployment_state()

    async def count_trades_on_date(self, day: datetime | None = None) -> int:
        """Count persisted simulated fills so the daily cap survives restarts."""
        if not self.db_service:
            return 0
        target = (day or datetime.now()).date().isoformat()
        try:
            async with self.db_service._connection.execute(
                "SELECT COUNT(*) FROM simulation_trades WHERE date(traded_at) = ?",
                (target,),
            ) as cursor:
                row = await cursor.fetchone()
            return int(row[0] if row else 0)
        except Exception:
            return 0

    async def record_pending_decision(
        self,
        *,
        ticker: str,
        direction: str,
        target_position: float,
        reason: str,
        defer_code: str,
        confidence: float | None = None,
        source: str = "investment_simulation",
    ) -> int | None:
        """Persist an unfilled ruling without trusting or storing a proposed price.

        Pending decisions are evidence for the next trading session, not orders.
        Any later executor must pass the normal market-hours, realtime-quote,
        valuation, risk-cap, and daily-frequency gates again.
        """
        if not self.db_service:
            return None
        conn = self.db_service._connection
        if conn is None:
            return None
        now = datetime.now().isoformat()
        try:
            cursor = await conn.execute(
                """
                INSERT INTO simulation_pending_decisions (
                    ticker, direction, target_position, confidence, reason,
                    defer_code, source, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_next_trading_session', ?, ?)
                """,
                (
                    self._normalize_ticker(ticker),
                    str(direction or "hold").lower(),
                    max(0.0, min(float(target_position or 0.0), 1.0)),
                    confidence,
                    str(reason or "")[:2000],
                    str(defer_code or "execution_deferred"),
                    str(source or "investment_simulation"),
                    now,
                    now,
                ),
            )
            await conn.commit()
            return int(cursor.lastrowid)
        except Exception as exc:
            logger.warning("Failed to persist pending simulated decision for %s: %s", ticker, exc)
            return None

    async def pending_decision_count(self) -> int:
        """Return unresolved deferred rulings for entry-point diagnostics."""
        if not self.db_service or self.db_service._connection is None:
            return 0
        try:
            async with self.db_service._connection.execute(
                "SELECT COUNT(*) FROM simulation_pending_decisions "
                "WHERE status = 'pending_next_trading_session'"
            ) as cursor:
                row = await cursor.fetchone()
            return int(row[0] if row else 0)
        except Exception:
            return 0

    async def get_current_price(self, ticker: str) -> Optional[float]:
        """获取实时行情价格；不使用AI预测或历史价格替代。"""
        from .market_data import get_market_data

        return await get_market_data().get_current_price(ticker)

    async def get_current_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """获取带来源和抓取时间的实时行情。"""
        from .market_data import get_market_data

        market_data = get_market_data()
        if hasattr(market_data, "get_current_quote"):
            return await market_data.get_current_quote(ticker)
        if not hasattr(market_data, "get_current_price"):
            return None
        price = await market_data.get_current_price(ticker)
        if not price:
            return None
        return {
            "ticker": self._normalize_ticker(ticker),
            "price": float(price),
            "source": "realtime_quote",
            "fetched_at": datetime.now().isoformat(),
        }

    @staticmethod
    def _normalize_ticker(ticker: str) -> str:
        code = str(ticker or "").strip().upper()
        return code.split(".")[0] if "." in code else code

    @staticmethod
    def realtime_quotes_enabled() -> bool:
        """Realtime quotes are mandatory by default; opt-out means no valuation/trade."""
        value = os.environ.get("SOVEREIGN_HALL_REALTIME_QUOTES", "1").strip().lower()
        return value in {"1", "true", "yes", "on"}

    async def resolve_trade_price_detail(self, ticker: str) -> Dict[str, Any]:
        """Resolve a realtime simulated fill price; never fall back to local marks."""
        code = self._normalize_ticker(ticker)
        if self.realtime_quotes_enabled():
            quote = await self.get_current_quote(code)
            if quote and quote.get("price") and float(quote["price"]) > 0:
                return {
                    "price": float(quote["price"]),
                    "source": str(quote.get("source") or "realtime_quote"),
                    "price_at": str(quote.get("fetched_at") or datetime.now().isoformat()),
                }
        return {
            "price": None,
            "source": "realtime_quote_unavailable",
            "price_at": "",
        }

    async def resolve_trade_price(self, ticker: str) -> tuple[Optional[float], str]:
        """Resolve a realtime-only simulated-trade price."""
        detail = await self.resolve_trade_price_detail(ticker)
        source = str(detail.get("source", ""))
        price_at = str(detail.get("price_at", ""))
        label = f"{source} {price_at}".strip()
        return detail.get("price"), label

    async def execute_trade(
        self,
        ticker: str,
        direction: str,
        target_position: float,
        current_price: float,
        llm: LLMClient = None,
        reason: str = "",
        confidence: float | None = None,
        signal_count: int | None = None,
        risk_cap_already_applied: bool = False,
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
        from .market_data import get_market_data

        market_data = get_market_data()
        direction_norm = (direction or "").lower()

        if not await market_data.is_trading_day():
            pending_id = await self.record_pending_decision(
                ticker=ticker,
                direction=direction_norm,
                target_position=target_position,
                confidence=confidence,
                reason=reason or "非交易日裁决",
                defer_code="non_trading_day",
            )
            return {
                'success': False,
                'action': 'pending',
                'ticker': ticker,
                'pending_decision_id': pending_id,
                'reason': '当前非交易日，裁决已延至下一交易时段；届时必须重新取得实时行情'
            }
        if (
            self.trade_during_market_hours_only
            and hasattr(market_data, "is_market_open")
            and not await market_data.is_market_open()
        ):
            pending_id = await self.record_pending_decision(
                ticker=ticker,
                direction=direction_norm,
                target_position=target_position,
                confidence=confidence,
                reason=reason or "闭市裁决",
                defer_code="market_closed",
            )
            return {
                'success': False,
                'action': 'pending',
                'ticker': ticker,
                'pending_decision_id': pending_id,
                'reason': '当前不在A股交易时段，仅记录待执行裁决；下一交易时段重新取得实时行情后再判断'
            }

        trades_today = await self.count_trades_on_date()
        if trades_today >= self.max_daily_trades:
            pending_id = await self.record_pending_decision(
                ticker=ticker,
                direction=direction_norm,
                target_position=target_position,
                confidence=confidence,
                reason=reason or "超过每日成交硬门",
                defer_code="daily_trade_limit",
            )
            return {
                'success': False,
                'action': 'pending',
                'ticker': ticker,
                'pending_decision_id': pending_id,
                'reason': (
                    f'今日模拟成交已达硬上限 {self.max_daily_trades} 笔；'
                    '裁决已持久化到下一交易时段，不允许任何调用方绕过'
                ),
            }

        # 检查冷却期
        if self.is_in_cooldown(ticker) and direction_norm not in ("short", "sell"):
            return {
                'success': False,
                'action': 'hold',
                'ticker': ticker,
                'reason': f'冷却期内，上次交易{self.last_trade_records.get(ticker, "")[:10]}'
            }

        current_shares = self.positions.get(ticker, {}).get('shares', 0)
        if direction_norm in ("hold", "neutral", "观望"):
            return {
                'success': True,
                'action': 'hold',
                'ticker': ticker,
                'reason': '投委会裁决为观望'
            }
        if direction_norm in ("short", "sell") and current_shares <= 0:
            return {
                'success': False,
                'action': 'hold',
                'ticker': ticker,
                'reason': '模拟账户不支持裸做空，空仓不交易'
            }

        # 委员会/调用方给出的价格仅是意见上下文，模拟成交必须重新取得实时行情。
        price, price_source = await self.resolve_trade_price(ticker)
        if price is None or price <= 0:
            return {
                'success': False,
                'action': 'hold',
                'ticker': ticker,
                'reason': '无法获取实时现价，拒绝模拟交易；不使用本地估值或历史价格兜底'
            }

        # 计算当前持仓
        if direction_norm in ("short", "sell"):
            target_position = 0.0

        position_values, total_assets, missing_price_tickers = await self._estimate_trade_assets(ticker, price)
        if missing_price_tickers and direction_norm == "long":
            return {
                'success': False,
                'action': 'hold',
                'ticker': ticker,
                'reason': (
                    '组合实时估值不完整，拒绝新增/扩大模拟仓位；缺少实时现价: '
                    + ', '.join(missing_price_tickers)
                ),
            }
        current_position_value = position_values.get(ticker, 0.0)
        current_gross_exposure = sum(position_values.values()) / total_assets if total_assets > 0 else 0.0
        current_position_pct = current_position_value / total_assets if total_assets > 0 else 0.0

        if direction_norm == "long" and not risk_cap_already_applied:
            await self.refresh_simulation_risk_memory()
            observed_signal_count = (
                signal_count
                if signal_count is not None
                else recent_prediction_observation_count(ticker)
            )
            capped_position, cap_reason = apply_heuristic_risk_cap(
                ticker,
                float(target_position),
                confidence,
                signal_count=observed_signal_count,
                current_position=current_position_pct,
                current_gross_exposure=current_gross_exposure,
            )
            if cap_reason:
                reason = f"{reason}; {cap_reason}" if reason else cap_reason
            target_position = capped_position

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
                existing = self.positions[ticker]
                existing.update({
                    'shares': new_shares,
                    'avg_cost': new_cost / new_shares,
                    'peak_price': max(float(existing.get('peak_price') or price), price),
                })
            else:
                self.positions[ticker] = {
                    'shares': shares_to_buy,
                    'avg_cost': price,
                    'opened_at': datetime.now().isoformat(),
                    'peak_price': price,
                    'last_mark_price': price,
                    'last_mark_at': datetime.now().isoformat(),
                    'last_mark_source': price_source,
                    'last_reviewed_at': datetime.now().isoformat(),
                    'review_status': 'opened',
                    'review_reason': reason or '新建模拟持仓',
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
                reason=(reason or f"加仓至{target_position*100:.0f}%") + (
                    f"; price_source={price_source}" if price_source else ""
                )
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
            shares_to_sell = (
                current_shares
                if target_position <= 0
                else int(sell_value / price / self.min_unit) * self.min_unit
            )

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
            trade_id = await self._record_trade(
                ticker=ticker,
                direction='sell',
                shares=shares_to_sell,
                price=price,
                fee=total_fee,
                reason=(reason or f"减仓至{target_position*100:.0f}%") + (
                    f"; price_source={price_source}" if price_source else ""
                )
            )

            await self.save_state()
            await self.mark_redeployment_required(
                net_proceeds,
                source=f"simulation_sell_trade:{trade_id or 'unknown'}:{ticker}",
            )
            await self.refresh_simulation_risk_memory()

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
    ) -> int | None:
        """记录交易到数据库"""
        if not self.db_service:
            return

        try:
            conn = self.db_service._connection
            cursor = await conn.execute("""
                INSERT INTO simulation_trades (ticker, direction, shares, price, fee, reason, traded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker, direction, shares, price, fee, reason, datetime.now().isoformat()
            ))
            await conn.commit()
            return int(cursor.lastrowid)
        except Exception as e:
            logger.warning(f"Failed to record trade: {e}")
            return None

    async def refresh_simulation_risk_memory(self) -> List[Dict]:
        """Persist recent realized-loss memory derived from local simulated trades."""
        if not self.db_service:
            return []

        try:
            conn = self.db_service._connection
            if conn is None:
                return []

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS simulation_risk_memory (
                    ticker TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    failure_count INTEGER NOT NULL,
                    last_loss_pct REAL NOT NULL,
                    worst_loss_pct REAL NOT NULL,
                    last_trade_id INTEGER,
                    last_updated TEXT NOT NULL,
                    expires_at TEXT,
                    reason TEXT
                )
            """)

            async with conn.execute("""
                SELECT id, ticker, direction, shares, price, fee, reason, traded_at
                FROM simulation_trades
                ORDER BY datetime(traded_at), id
            """) as cursor:
                rows = [dict(row) async for row in cursor]

            failures = derive_simulation_risk_memory(
                rows,
                loss_threshold=self.risk_memory_loss_threshold,
                memory_days=self.risk_memory_days,
            )
            for failure in failures:
                await conn.execute("""
                    INSERT INTO simulation_risk_memory (
                        ticker, source, failure_count, last_loss_pct, worst_loss_pct,
                        last_trade_id, last_updated, expires_at, reason
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        source = excluded.source,
                        failure_count = excluded.failure_count,
                        last_loss_pct = excluded.last_loss_pct,
                        worst_loss_pct = excluded.worst_loss_pct,
                        last_trade_id = excluded.last_trade_id,
                        last_updated = excluded.last_updated,
                        expires_at = excluded.expires_at,
                        reason = excluded.reason
                """, (
                    failure["ticker"],
                    failure["source"],
                    failure["failure_count"],
                    failure["last_loss_pct"],
                    failure["worst_loss_pct"],
                    failure["last_trade_id"],
                    failure["last_updated"],
                    failure["expires_at"],
                    failure["reason"],
                ))

            await conn.commit()
            return failures
        except Exception as e:
            logger.warning(f"Failed to refresh simulation risk memory: {e}")
            return []

    async def review_open_positions(self) -> List[Dict[str, Any]]:
        """Review every open position before considering new simulated trades."""
        reviews: List[Dict[str, Any]] = []
        now = datetime.now()
        for ticker in list(self.positions):
            pos = self.positions.get(ticker)
            if not pos:
                continue
            detail = await self.resolve_trade_price_detail(ticker)
            executable_price = detail.get("price")
            diagnostic_price = executable_price or detail.get("candidate_price")
            review = review_position(
                ticker=ticker,
                avg_cost=float(pos.get("avg_cost", 0.0) or 0.0),
                opened_at=pos.get("opened_at"),
                price=float(diagnostic_price) if diagnostic_price else None,
                price_at=detail.get("price_at"),
                price_source=str(detail.get("source", "")),
                now=now,
                max_price_age_days=self.max_trade_price_age_days,
                stop_loss_pct=self.stop_loss_pct,
                take_profit_pct=self.take_profit_pct,
                max_holding_days=self.max_holding_days,
            )
            pos["last_reviewed_at"] = now.isoformat()
            pos["review_status"] = review.action
            pos["review_reason"] = review.reason
            pos["last_mark_price"] = diagnostic_price
            pos["last_mark_at"] = str(detail.get("price_at", ""))
            pos["last_mark_source"] = str(detail.get("source", ""))
            if diagnostic_price:
                pos["peak_price"] = max(float(pos.get("peak_price") or diagnostic_price), float(diagnostic_price))

            result = review.as_dict()
            if review.action == "exit" and executable_price:
                execution = await self.execute_trade(
                    ticker=ticker,
                    direction="sell",
                    target_position=0.0,
                    current_price=float(executable_price),
                    reason=f"逐仓强制复核: {review.reason}; price_source={detail.get('source')} {detail.get('price_at')}",
                    risk_cap_already_applied=True,
                )
                result["execution"] = execution
                if not execution.get("success") and ticker in self.positions:
                    pos["review_status"] = "exit_pending_execution"
                    pos["review_reason"] = f"{review.reason}；待执行: {execution.get('reason', 'unknown')}"
                    result["action"] = "exit_pending_execution"
                    result["reason"] = pos["review_reason"]
            reviews.append(result)

        await self.save_state()
        return reviews

    async def calculate_assets(self, prices: Dict[str, float] = None) -> Dict:
        """按实时现价计算资产；任一行情缺失时明确标记估值不完整。"""
        known_total_assets = self.cash
        # Keep the argument for API compatibility, but never trust caller-supplied
        # marks for current account valuation.
        position_prices: Dict[str, float] = {}
        position_values: Dict[str, float] = {}
        quote_details: Dict[str, Dict[str, Any]] = {}
        missing_price_tickers: List[str] = []
        for ticker, pos in self.positions.items():
            code = self._normalize_ticker(ticker)
            price = None
            if self.realtime_quotes_enabled():
                quote = await self.get_current_quote(code)
                if quote and quote.get("price") and float(quote["price"]) > 0:
                    price = float(quote["price"])
                    quote_details[code] = dict(quote)
            if not price:
                missing_price_tickers.append(code)
                continue
            position_prices[code] = price
            value = float(pos['shares']) * price
            position_values[code] = value
            known_total_assets += value

        valuation_complete = not missing_price_tickers
        deployment = (
            deployment_status(self.cash, known_total_assets, self.target_invested_ratio)
            if valuation_complete
            else None
        )
        return {
            'cash': self.cash,
            'positions_value': known_total_assets - self.cash if valuation_complete else None,
            'total_assets': known_total_assets if valuation_complete else None,
            'known_total_assets': known_total_assets,
            'positions': self.positions.copy(),
            'position_prices': position_prices,
            'position_values': position_values,
            'quote_details': quote_details,
            'valuation_complete': valuation_complete,
            'missing_price_tickers': missing_price_tickers,
            'last_trade_date': self.last_trade_date.isoformat() if self.last_trade_date else None,
            'target_invested_ratio': self.target_invested_ratio,
            'invested_ratio': deployment['invested_ratio'] if deployment else None,
            'deployment_gap': deployment['deployment_gap'] if deployment else None,
            'valuation_rule': 'realtime quotes only; no local/prediction/cost fallback',
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
        if not assets.get("valuation_complete"):
            return (
                "实时行情不完整，跳过资产收益反思与调仓推断；缺少实时现价: "
                + ", ".join(assets.get("missing_price_tickers", []))
            )

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
1. 当前资产变化是否由真实交易结果驱动
2. 哪个持仓或交易最需要调整
3. 策略是否有效：只写证据，不写泛泛总结
4. 下一步建议：买入/卖出/观望、仓位变化、触发条件

请用中文回答；没有新交易时不要重复历史反思，但可以充分展开会改变下一步操作的风险、反证和触发条件。
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
                updated_at TEXT NOT NULL,
                opened_at TEXT,
                peak_price REAL,
                last_mark_price REAL,
                last_mark_at TEXT,
                last_mark_source TEXT,
                last_reviewed_at TEXT,
                review_status TEXT,
                review_reason TEXT
            )
        """)

        async with conn.execute("PRAGMA table_info(simulation_positions)") as cursor:
            existing_position_columns = {row[1] for row in await cursor.fetchall()}
        required_position_columns = {
            "opened_at": "TEXT",
            "peak_price": "REAL",
            "last_mark_price": "REAL",
            "last_mark_at": "TEXT",
            "last_mark_source": "TEXT",
            "last_reviewed_at": "TEXT",
            "review_status": "TEXT",
            "review_reason": "TEXT",
        }
        for column, column_type in required_position_columns.items():
            if column not in existing_position_columns:
                await conn.execute(
                    f"ALTER TABLE simulation_positions ADD COLUMN {column} {column_type}"
                )
        await conn.execute("""
            UPDATE simulation_positions
            SET opened_at = COALESCE(
                opened_at,
                (SELECT MAX(t.traded_at)
                 FROM simulation_trades t
                 WHERE t.direction = 'buy'
                   AND replace(replace(t.ticker, '.SH', ''), '.SZ', '') =
                       replace(replace(simulation_positions.ticker, '.SH', ''), '.SZ', '')),
                updated_at
            ),
                peak_price = COALESCE(peak_price, avg_cost),
                review_status = COALESCE(review_status, 'pending_first_review'),
                review_reason = COALESCE(review_reason, '等待逐仓生命周期复核')
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

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_risk_memory (
                ticker TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                failure_count INTEGER NOT NULL,
                last_loss_pct REAL NOT NULL,
                worst_loss_pct REAL NOT NULL,
                last_trade_id INTEGER,
                last_updated TEXT NOT NULL,
                expires_at TEXT,
                reason TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_redeployment_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                status TEXT NOT NULL,
                deployment_gap REAL,
                blocker_code TEXT,
                blocker_reason TEXT,
                next_action TEXT,
                source TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                last_candidate_count INTEGER NOT NULL DEFAULT 0,
                last_trade_count INTEGER NOT NULL DEFAULT 0,
                last_rejection_counts TEXT NOT NULL DEFAULT '{}',
                rejection_counts_total TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS simulation_pending_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,
                target_position REAL NOT NULL,
                confidence REAL,
                reason TEXT,
                defer_code TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                resolved_at TEXT,
                resolution TEXT
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_simulation_pending_status "
            "ON simulation_pending_decisions(status, created_at)"
        )

        async with conn.execute("PRAGMA table_info(simulation_redeployment_state)") as cursor:
            redeployment_columns = {row[1] for row in await cursor.fetchall()}
        for column in ("last_rejection_counts", "rejection_counts_total"):
            if column not in redeployment_columns:
                await conn.execute(
                    f"ALTER TABLE simulation_redeployment_state "
                    f"ADD COLUMN {column} TEXT NOT NULL DEFAULT '{{}}'"
                )

        await conn.commit()
        await self.refresh_simulation_risk_memory()

    async def save_snapshot(self, reflection: str = ""):
        """保存每日资产快照"""
        if not self.db_service:
            return

        try:
            assets = await self.calculate_assets()
            if not assets.get("valuation_complete"):
                logger.warning(
                    "Skip simulation snapshot: realtime quotes unavailable for %s",
                    ", ".join(assets.get("missing_price_tickers", [])),
                )
                return
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
    from .market_data import get_market_data

    market_data = get_market_data()
    if not await market_data.is_trading_day():
        logger.info("Skip daily simulation: today is not a trading day")
        reflection = await simulation.daily_reflection(llm)
        await simulation.save_snapshot(reflection)
        return await simulation.calculate_assets(), reflection

    # 计算当前资产
    assets = await simulation.calculate_assets()

    # 如果有提案，可以执行交易
    if proposals:
        for proposal in proposals[:3]:  # 最多处理3个提案
            ticker = proposal.get('ticker')
            direction = proposal.get('direction', 'long')
            target_position = proposal.get('target_position', 0.1)

            current_price, price_source = await simulation.resolve_trade_price(ticker)
            if current_price is None:
                logger.info(f"Skip simulation trade for {ticker}: no realtime quote")
                continue

            result = await simulation.execute_trade(
                ticker=ticker,
                direction=direction,
                target_position=target_position,
                current_price=current_price,
                llm=llm,
                reason=f"proposal simulation; price_source={price_source}",
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
    if not assets.get("valuation_complete"):
        missing = ", ".join(assets.get("missing_price_tickers", []))
        return (
            "\n📊 投资模拟状态\n================\n"
            f"初始资金: {simulation.initial_capital:.2f} 元\n"
            f"当前资产: N/A（缺少实时现价: {missing}）\n"
            f"现金: {assets['cash']:.2f} 元\n"
            "规则: 只接受实时现价，不使用本地估值、历史预测价或成本价兜底。\n"
        )
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
