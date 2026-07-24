"""
🏛️ Sovereign Hall - 学习引擎
从历史决策中学习，生成教训Prompt供讨论使用
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict

import aiosqlite

logger = logging.getLogger(__name__)


class LearningEngine:
    """学习引擎 - 从历史决策中学习"""

    def __init__(self, db_path: str = None):
        from ..core import DATA_DIR
        self.db_path = db_path or str(DATA_DIR / "sovereign_hall.db")

    async def analyze_errors(self, limit: int = 20) -> List[Dict]:
        """分析错误决策的特征，返回教训列表"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE status = 'validated'
                AND result IN ('wrong', 'partial')
                ORDER BY predicted_at DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            return []

        lessons = []
        confidence_groups = defaultdict(list)
        for row in rows:
            conf = row['confidence']
            if conf >= 0.8:
                group = "high"
            elif conf >= 0.5:
                group = "medium"
            else:
                group = "low"
            confidence_groups[group].append(dict(row))

        for group, decisions in confidence_groups.items():
            if group == "high" and decisions:
                wrong_count = sum(1 for d in decisions if d['result'] == 'wrong')
                scores = [d['accuracy_score'] or 0 for d in decisions]
                avg_accuracy = sum(scores) / len(scores)
                lessons.append({
                    "type": "high_confidence_error",
                    "count": len(decisions),
                    "wrong_count": wrong_count,
                    "avg_accuracy": avg_accuracy,
                    "description": f"高置信度决策({len(decisions)}次)中{wrong_count}次错误，准确率仅{avg_accuracy:.0%}，需谨慎评估",
                    "avg_confidence": sum(d['confidence'] for d in decisions) / len(decisions),
                })

        return lessons[:10]

    async def analyze_error_profiles(self, limit: int = 80) -> List[Dict]:
        """Build reusable error profiles from validated prediction outcomes."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE status = 'validated'
                AND result IN ('correct', 'partial', 'wrong')
                ORDER BY validated_at DESC, predicted_at DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = [dict(row) for row in await cursor.fetchall()]

        if not rows:
            return []

        groups = defaultdict(list)
        for row in rows:
            key = (
                row.get("direction") or "unknown",
                self._confidence_bucket(row.get("confidence")),
                self._horizon_bucket(row.get("expected_days")),
            )
            groups[key].append(row)

        profiles = []
        for (direction, confidence_bucket, horizon_bucket), decisions in groups.items():
            total = len(decisions)
            wrong = sum(1 for item in decisions if item.get("result") == "wrong")
            partial = sum(1 for item in decisions if item.get("result") == "partial")
            avg_accuracy = sum(float(item.get("accuracy_score") or 0.0) for item in decisions) / total
            if total < 2 and wrong == 0:
                continue
            if wrong == 0 and avg_accuracy >= 0.45:
                continue

            tickers = []
            for item in decisions:
                ticker = item.get("ticker")
                if ticker and ticker not in tickers:
                    tickers.append(ticker)

            examples = []
            for item in decisions:
                if item.get("result") not in ("wrong", "partial"):
                    continue
                context = (item.get("discussion_context") or "").replace("\n", " ")[:120]
                examples.append(f"{item.get('ticker')}:{item.get('result')}({context})")
                if len(examples) >= 3:
                    break

            profiles.append({
                "direction": direction,
                "confidence_bucket": confidence_bucket,
                "horizon_bucket": horizon_bucket,
                "total": total,
                "wrong": wrong,
                "partial": partial,
                "avg_accuracy": avg_accuracy,
                "tickers": tickers[:5],
                "examples": examples,
                "description": (
                    f"{direction}/{confidence_bucket}/{horizon_bucket}: "
                    f"{total}次验证，wrong={wrong}, partial={partial}, 平均准确度{avg_accuracy:.0%}"
                ),
            })

        profiles.sort(key=lambda item: (item["wrong"], -item["avg_accuracy"], item["total"]), reverse=True)
        return profiles[:8]

    async def generate_lessons_prompt(self) -> str:
        """生成教训Prompt，注入到讨论中"""
        lessons = await self.analyze_errors()
        profiles = await self.analyze_error_profiles()
        if not lessons and not profiles:
            return ""

        prompt = "\n【历史投资教训】\n"
        prompt += "以下教训只用于否决或修正当前判断，不要复述；若与当前标的无关请忽略：\n"

        for i, lesson in enumerate(lessons[:5], 1):
            prompt += f"{i}. {lesson['description'][:160]}\n"

        if profiles:
            prompt += "\n【错误画像】\n"
            prompt += "优先检查当前提案是否落入这些低胜率模式；若落入，必须降低仓位或给出新证据：\n"
            for i, profile in enumerate(profiles[:5], 1):
                tickers = ",".join(profile.get("tickers") or [])
                examples = "；".join(profile.get("examples") or [])
                prompt += (
                    f"{i}. {profile['description']}；样本: {tickers or '无'}"
                    f"{'；例: ' + examples[:180] if examples else ''}\n"
                )

        stats = await self.get_accuracy_stats()
        if stats['total'] > 0:
            prompt += f"\n当前历史预测胜率: {stats['accuracy']:.1%} ({stats['correct']}/{stats['total']})"

        return prompt

    async def generate_research_memory_prompt(
        self,
        topic: str,
        conclusion_limit: int = 5,
        prediction_limit: int = 12,
    ) -> str:
        """Bring prior conclusions and their prediction outcomes into the next loop.

        Historical text is explicitly labelled as a falsifiable prior so fresh
        network evidence remains authoritative.
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    """
                    SELECT id, question, conclusion, ticker, position, stop_loss,
                           take_profit, holding_period, confidence, created_at
                    FROM report_conclusions
                    WHERE question = ?
                    ORDER BY datetime(created_at) DESC, rowid DESC
                    LIMIT ?
                    """,
                    (topic, conclusion_limit),
                ) as cursor:
                    conclusions = [dict(row) for row in await cursor.fetchall()]

                tickers = list(dict.fromkeys(
                    str(row.get("ticker") or "").strip().upper()
                    for row in conclusions
                    if str(row.get("ticker") or "").strip()
                ))
                predictions = []
                if tickers:
                    placeholders = ",".join("?" for _ in tickers)
                    async with db.execute(
                        f"""
                        SELECT ticker, direction, confidence, expected_days, status,
                               result, accuracy_score, predicted_at, validated_at,
                               target_price, stop_loss
                        FROM price_predictions
                        WHERE ticker IN ({placeholders})
                        ORDER BY datetime(predicted_at) DESC, rowid DESC
                        LIMIT ?
                        """,
                        (*tickers, prediction_limit),
                    ) as cursor:
                        predictions = [dict(row) for row in await cursor.fetchall()]
        except Exception as exc:
            logger.warning("加载历史研究记忆失败: %s", exc)
            return ""

        if not conclusions and not predictions:
            return ""

        lines = [
            "【同议题历史研究记忆】",
            "以下内容是旧结论与旧预测结果，不是当前事实。必须用本轮联网资料重新验证，并明确维持、修正或推翻。",
        ]
        for index, row in enumerate(conclusions, 1):
            conclusion = " ".join(str(row.get("conclusion") or "").split())
            lines.append(
                f"{index}. {row.get('created_at') or '未知时间'} | "
                f"{row.get('ticker') or '无标的'} | 置信度={float(row.get('confidence') or 0):.0%} | "
                f"期限={row.get('holding_period') or '未记录'} | {conclusion[:360]}"
            )

        if predictions:
            lines.append("【关联预测验证】")
            for row in predictions:
                score = row.get("accuracy_score")
                score_text = f"{float(score):.2f}" if score is not None else "N/A"
                lines.append(
                    f"- {row.get('ticker')} {row.get('direction')} | "
                    f"{int(row.get('expected_days') or 30)}天 | "
                    f"{row.get('status')}/{row.get('result')} | accuracy={score_text} | "
                    f"预测于={row.get('predicted_at') or 'N/A'}"
                )

        return "\n".join(lines)

    @staticmethod
    def _confidence_bucket(confidence) -> str:
        value = float(confidence or 0.0)
        if value >= 0.75:
            return "high_confidence"
        if value >= 0.55:
            return "medium_confidence"
        return "low_confidence"

    @staticmethod
    def _horizon_bucket(expected_days) -> str:
        days = int(expected_days or 30)
        if days <= 14:
            return "short_horizon"
        if days <= 60:
            return "medium_horizon"
        return "long_horizon"

    async def get_accuracy_stats(self) -> Dict:
        """获取准确率统计"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'correct' THEN 1 ELSE 0 END) as correct,
                    SUM(CASE WHEN result = 'partial' THEN 1 ELSE 0 END) as partial,
                    SUM(CASE WHEN result = 'wrong' THEN 1 ELSE 0 END) as wrong,
                    AVG(accuracy_score) as avg_accuracy
                FROM price_predictions
                WHERE status = 'validated'
                AND result IN ('correct', 'partial', 'wrong')
            """) as cursor:
                row = await cursor.fetchone()
                if not row or row[0] == 0:
                    return {"total": 0, "correct": 0, "partial": 0, "wrong": 0, "accuracy": 0.0}

                total, correct, partial, wrong, avg_acc = row
                accuracy = (correct + partial * 0.5) / total if total > 0 else 0

                return {
                    "total": total,
                    "correct": correct or 0,
                    "partial": partial or 0,
                    "wrong": wrong or 0,
                    "accuracy": accuracy,
                    "avg_accuracy": avg_acc or 0,
                }

    async def update_playbook(self) -> int:
        """将验证结果自动写入playbook表"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM price_predictions
                WHERE status = 'validated'
                AND result IN ('wrong', 'partial')
                ORDER BY validated_at DESC
                LIMIT 10
            """) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            return 0

        count = 0
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("PRAGMA table_info(playbook)") as cursor:
                playbook_columns = {row[1] async for row in cursor}

        for row in rows:
            record = dict(row)
            ticker = record['ticker']
            result = record['result']
            context = (record.get('discussion_context') or '')[:200]

            lesson = f"高置信度决策但实际错误，需更谨慎评估" if result == 'wrong' else f"部分正确但未达到目标，需要更好的入场时机"

            async with aiosqlite.connect(self.db_path) as db:
                existing = None
                if "id" in playbook_columns:
                    async with db.execute("SELECT id FROM playbook WHERE ticker = ?", (ticker,)) as cursor:
                        existing = await cursor.fetchone()
                elif "entry_id" in playbook_columns:
                    async with db.execute("SELECT entry_id FROM playbook WHERE ticker = ?", (ticker,)) as cursor:
                        existing = await cursor.fetchone()
                else:
                    continue

                if not existing:
                    if {"id", "ticker", "situation", "lesson", "outcome", "success", "created_at"}.issubset(playbook_columns):
                        await db.execute("""
                            INSERT INTO playbook (id, ticker, situation, lesson, outcome, success, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            f"lesson_{ticker}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                            ticker,
                            context[:100],
                            lesson,
                            result,
                            0 if result == 'wrong' else 0.5,
                            datetime.now().isoformat(),
                        ))
                    elif {"entry_id", "category", "situation", "action_taken", "outcome", "lesson", "confidence_delta", "ticker", "refs", "created_at"}.issubset(playbook_columns):
                        await db.execute("""
                            INSERT INTO playbook
                            (entry_id, category, situation, action_taken, outcome, lesson, confidence_delta, ticker, refs, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            f"lesson_{ticker}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                            "prediction_validation",
                            context[:100],
                            record.get("direction", ""),
                            result,
                            lesson,
                            -0.1 if result == 'wrong' else -0.05,
                            ticker,
                            record.get("id", ""),
                            datetime.now().isoformat(),
                        ))
                    else:
                        logger.warning("playbook表结构不兼容，跳过教训写入")
                        continue
                    await db.commit()
                    count += 1

        logger.info(f"已将{count}条教训写入playbook")
        return count


_engine: Optional[LearningEngine] = None


def get_learning_engine(db_path: str = None) -> LearningEngine:
    global _engine
    if _engine is None:
        _engine = LearningEngine(db_path)
    return _engine
