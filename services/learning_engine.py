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

    async def generate_lessons_prompt(self) -> str:
        """生成教训Prompt，注入到讨论中"""
        lessons = await self.analyze_errors()
        if not lessons:
            return ""

        prompt = "\n【历史投资教训】\n"
        prompt += "以下是从过去决策中总结的教训，请务必注意：\n"

        for i, lesson in enumerate(lessons[:5], 1):
            prompt += f"{i}. {lesson['description']}\n"

        stats = await self.get_accuracy_stats()
        if stats['total'] > 0:
            prompt += f"\n当前历史预测胜率: {stats['accuracy']:.1%} ({stats['correct']}/{stats['total']})"

        return prompt

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
        for row in rows:
            record = dict(row)
            ticker = record['ticker']
            result = record['result']
            context = (record.get('discussion_context') or '')[:200]

            lesson = f"高置信度决策但实际错误，需更谨慎评估" if result == 'wrong' else f"部分正确但未达到目标，需要更好的入场时机"

            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("SELECT id FROM playbook WHERE ticker = ?", (ticker,)) as cursor:
                    existing = await cursor.fetchone()

                if not existing:
                    await db.execute("""
                        INSERT INTO playbook (ticker, situation, lesson, outcome, success, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (ticker, context[:100], lesson, result, 0 if result == 'wrong' else 0.5, datetime.now().isoformat()))
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
