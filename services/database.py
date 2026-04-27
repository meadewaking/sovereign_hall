"""
🏛️ Sovereign Hall - SQLite Database Service
数据库服务 - 使用 aiosqlite 实现真正的异步操作
"""

import asyncio
import aiosqlite
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from ..core import DATA_DIR

logger = logging.getLogger(__name__)


class DatabaseService:
    """SQLite数据库服务（异步版本）"""

    _instance = None

    @classmethod
    async def get_instance(cls, db_path: str = None) -> "DatabaseService":
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls(db_path)
        return cls._instance

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(DATA_DIR / "sovereign_hall.db")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[aiosqlite.Connection] = None
        self._initialized = False
        logger.info(f"DatabaseService initialized: {self.db_path}")

    async def _get_connection(self) -> aiosqlite.Connection:
        """获取数据库连接（延迟初始化）"""
        if self._connection is None:
            self._connection = await aiosqlite.connect(str(self.db_path))
            self._connection.row_factory = aiosqlite.Row
        return self._connection

    async def _get_existing_tables(self, conn) -> set:
        """获取现有表列表"""
        tables = set()
        async with conn.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
            async for row in cursor:
                tables.add(row[0])
        return tables

    async def _init_db(self):
        """初始化数据库表"""
        conn = await self._get_connection()

        # 获取现有表结构
        existing_tables = await self._get_existing_tables(conn)

        # documents 表
        if 'documents' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    url TEXT,
                    source TEXT,
                    sector TEXT,
                    keywords TEXT,
                    publish_time TEXT,
                    embedding BLOB,
                    crawled_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # 添加缺失字段的迁移
        try:
            await conn.execute("ALTER TABLE documents ADD COLUMN crawled_at TEXT")
        except:
            pass  # 字段已存在

        # 创建索引
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_sector ON documents(sector)")

        # proposals 表
        if 'proposals' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS proposals (
                    proposal_id TEXT PRIMARY KEY,
                    ticker TEXT,
                    direction TEXT,
                    target_position REAL,
                    entry_price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    holding_period INTEGER,
                    confidence REAL,
                    thesis TEXT,
                    analyst_role TEXT,
                    sector TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            # 添加缺失的字段
            try:
                await conn.execute("ALTER TABLE proposals ADD COLUMN status TEXT DEFAULT 'pending'")
            except:
                pass

        # meetings 表
        if 'meetings' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS meetings (
                    id TEXT PRIMARY KEY,
                    proposal_id TEXT,
                    ticker TEXT,
                    decision TEXT,
                    discussion TEXT,
                    vote_details TEXT,
                    action_items TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # playbook 表
        if 'playbook' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS playbook (
                    id TEXT PRIMARY KEY,
                    ticker TEXT,
                    situation TEXT,
                    lesson TEXT,
                    outcome TEXT,
                    success BOOLEAN,
                    confidence_adjustment REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # blacklist 表
        if 'blacklist' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS blacklist (
                    ticker TEXT PRIMARY KEY,
                    reason TEXT,
                    failure_count INTEGER,
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    expires_at TEXT
                )
            """)

        # system_stats 表
        if 'system_stats' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS system_stats (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # checkpoints 表
        if 'checkpoints' not in existing_tables:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    iteration INTEGER,
                    stats TEXT,
                    blacklist TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # 创建索引（忽略已存在索引的错误）
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_sector ON documents(sector)")
        except:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url)")
        except:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_proposals_ticker ON proposals(ticker)")
        except:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status)")
        except:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_meetings_proposal ON meetings(proposal_id)")
        except:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_playbook_ticker ON playbook(ticker)")
        except:
            pass

        await conn.commit()
        logger.info(f"Database initialized: {self.db_path}")

    # ===================== 文档操作 =====================

    async def add_document(self, doc: Any):
        """添加文档"""
        if isinstance(doc, dict):
            from ..core import Document
            doc = Document.from_dict(doc)

        def _attr(name: str, default=None):
            if isinstance(doc, dict):
                return doc.get(name, default)
            return getattr(doc, name, default)

        publish_time = _attr('publish_time')
        if isinstance(publish_time, datetime):
            publish_time = publish_time.isoformat()

        conn = await self._get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO documents
            (id, title, content, url, source, sector, keywords, publish_time, embedding, crawled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            _attr('id') or _attr('doc_id'),
            _attr('title', ''),
            _attr('content', ''),
            _attr('url', ''),
            _attr('source', ''),
            _attr('sector', ''),
            json.dumps(_attr('keywords', []), ensure_ascii=False) if _attr('keywords', []) else None,
            publish_time,
            json.dumps(_attr('embedding')) if _attr('embedding') else None
        ))
        await conn.commit()

    async def get_document(self, doc_id: str) -> Optional[Dict]:
        """获取文档"""
        conn = await self._get_connection()
        async with conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def search_documents(
        self,
        query: str = None,
        sector: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """搜索文档"""
        conn = await self._get_connection()
        sql = "SELECT * FROM documents WHERE 1=1"
        params = []

        if sector:
            sql += " AND sector = ?"
            params.append(sector)

        if query:
            sql += " AND (title LIKE ? OR content LIKE ?)"
            params.extend([f"%{query}%", f"%{query}%"])

        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        async with conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_documents(self) -> int:
        """统计文档数量"""
        conn = await self._get_connection()
        async with conn.execute("SELECT COUNT(*) FROM documents") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # ===================== 提案操作 =====================

    async def add_proposal(self, proposal: Any):
        """添加提案"""
        if isinstance(proposal, dict):
            class _Proposal:
                pass
            obj = _Proposal()
            for key, value in proposal.items():
                setattr(obj, key, value)
            if not getattr(obj, "proposal_id", None):
                from ..utils import generate_id
                obj.proposal_id = generate_id("proposal")
            proposal = obj

        analyst_role = getattr(proposal, 'analyst_role', None)
        if hasattr(analyst_role, "value"):
            analyst_role = analyst_role.value

        def pget(name: str, default=None):
            return getattr(proposal, name, default)

        conn = await self._get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO proposals
            (proposal_id, ticker, direction, target_position, entry_price, stop_loss,
             take_profit, holding_period, confidence, thesis, analyst_role, sector, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pget('proposal_id') or pget('id'),
            pget('ticker', ''),
            pget('direction', 'long'),
            pget('target_position', 0.0),
            pget('entry_price', 0.0),
            pget('stop_loss', 0.0),
            pget('take_profit', pget('target_price', 0.0)),
            pget('holding_period', 30),
            pget('confidence', 0.0),
            pget('thesis', '')[:10000] if pget('thesis') else None,
            analyst_role,
            pget('sector', None),
            pget('status', 'pending')
        ))
        await conn.commit()

    async def get_proposals(self, status: str = None, limit: int = 100) -> List[Dict]:
        """获取提案列表"""
        conn = await self._get_connection()
        sql = "SELECT * FROM proposals"
        params = []

        if status:
            sql += " WHERE status = ?"
            params.append(status)

        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        async with conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # ===================== 会议操作 =====================

    async def add_meeting(self, meeting: Any):
        """添加会议纪要"""
        conn = await self._get_connection()
        await conn.execute("""
            INSERT INTO meetings
            (id, proposal_id, ticker, decision, discussion, vote_details, action_items)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            meeting.id,
            getattr(meeting, 'proposal_id', None),
            getattr(meeting, 'ticker', None),
            meeting.decision,
            json.dumps(meeting.discussion, ensure_ascii=False) if meeting.discussion else None,
            json.dumps(getattr(meeting, 'vote_details', {}), ensure_ascii=False),
            json.dumps(getattr(meeting, 'action_items', []), ensure_ascii=False)
        ))
        await conn.commit()

    async def get_meetings(self, limit: int = 100) -> List[Dict]:
        """获取会议纪要列表"""
        conn = await self._get_connection()
        async with conn.execute(
            "SELECT * FROM meetings ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_meetings(self) -> int:
        """统计会议数量"""
        conn = await self._get_connection()
        async with conn.execute("SELECT COUNT(*) FROM meetings") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # ===================== 机构记忆操作 =====================

    async def add_playbook_entry(self, entry: Any):
        """添加经验条目"""
        conn = await self._get_connection()
        await conn.execute("""
            INSERT INTO playbook
            (id, ticker, situation, lesson, outcome, success, confidence_adjustment, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            getattr(entry, 'id', None) or getattr(entry, 'entry_id', None),
            getattr(entry, 'ticker', None),
            getattr(entry, 'situation', None),
            entry.lesson,
            getattr(entry, 'outcome', None),
            getattr(entry, 'success', None),
            getattr(entry, 'confidence_delta', 0.0) or getattr(entry, 'confidence_adjustment', 0.0),
            datetime.now().isoformat()
        ))
        await conn.commit()

    async def get_playbook_by_ticker(self, ticker: str) -> List[Dict]:
        """根据股票代码获取经验"""
        conn = await self._get_connection()
        async with conn.execute(
            "SELECT * FROM playbook WHERE ticker = ? ORDER BY created_at DESC",
            (ticker,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_playbook(self) -> int:
        """统计经验条目"""
        conn = await self._get_connection()
        async with conn.execute("SELECT COUNT(*) FROM playbook") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # ===================== 黑名单操作 =====================

    async def add_to_blacklist(self, ticker: str, reason: str = None):
        """添加到黑名单"""
        conn = await self._get_connection()
        # 检查是否存在
        async with conn.execute(
            "SELECT failure_count FROM blacklist WHERE ticker = ?",
            (ticker,)
        ) as cursor:
            row = await cursor.fetchone()

        if row:
            # 更新失败次数
            await conn.execute("""
                UPDATE blacklist
                SET failure_count = failure_count + 1, added_at = ?
                WHERE ticker = ?
            """, (datetime.now().isoformat(), ticker))
        else:
            # 新增
            await conn.execute("""
                INSERT INTO blacklist (ticker, reason, failure_count, added_at)
                VALUES (?, ?, 1, ?)
            """, (ticker, reason, datetime.now().isoformat()))

        await conn.commit()

    async def is_blacklisted(self, ticker: str) -> bool:
        """检查是否在黑名单"""
        conn = await self._get_connection()
        async with conn.execute(
            "SELECT 1 FROM blacklist WHERE ticker = ?",
            (ticker,)
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def get_blacklist(self) -> List[str]:
        """获取黑名单列表"""
        conn = await self._get_connection()
        async with conn.execute("SELECT ticker FROM blacklist") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    # ===================== 统计操作 =====================

    async def save_checkpoint(self, iteration: int, stats: Dict, blacklist: List[str]):
        """保存检查点"""
        conn = await self._get_connection()
        await conn.execute("""
            INSERT INTO checkpoints (iteration, stats, blacklist)
            VALUES (?, ?, ?)
        """, (iteration, json.dumps(stats, ensure_ascii=False), json.dumps(blacklist)))
        await conn.commit()

    async def get_latest_checkpoint(self) -> Optional[Dict]:
        """获取最新检查点"""
        conn = await self._get_connection()
        async with conn.execute(
            "SELECT * FROM checkpoints ORDER BY id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_stats_summary(self) -> Dict:
        """获取统计摘要"""
        conn = await self._get_connection()
        return {
            'documents': await self.count_documents(),
            'proposals': await self._count_table('proposals'),
            'meetings': await self.count_meetings(),
            'playbook': await self.count_playbook(),
            'blacklist': await self._count_table('blacklist'),
            'checkpoints': await self._count_table('checkpoints'),
        }

    async def _count_table(self, table: str) -> int:
        """统计表记录数"""
        conn = await self._get_connection()
        async with conn.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def clear_old_checkpoints(self, keep: int = 5):
        """清理旧检查点"""
        conn = await self._get_connection()
        # 获取要删除的ID
        async with conn.execute(
            "SELECT id FROM checkpoints ORDER BY id DESC OFFSET ?",
            (keep,)
        ) as cursor:
            rows = await cursor.fetchall()

        if rows:
            id_list = [r[0] for r in rows]
            placeholders = ','.join('?' * len(id_list))
            await conn.execute(f"DELETE FROM checkpoints WHERE id IN ({placeholders})", id_list)
            await conn.commit()

    @asynccontextmanager
    async def transaction(self):
        """事务上下文"""
        conn = await self._get_connection()
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def close(self):
        """关闭连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

    # ===================== 报告结论存储 =====================

    async def init_report_tables(self):
        """初始化报告结论表"""
        conn = await self._get_connection()

        await conn.execute('''CREATE TABLE IF NOT EXISTS report_conclusions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT,
            conclusion TEXT,
            ticker TEXT,
            position REAL,
            stop_loss REAL,
            take_profit REAL,
            holding_period TEXT,
            confidence REAL,
            key_points TEXT,
            risks TEXT,
            created_at TEXT
        )''')

        await conn.execute('''CREATE TABLE IF NOT EXISTS reflection_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT,
            previous_conclusions TEXT,
            reflection_text TEXT,
            verification_results TEXT,
            adjusted_conclusion TEXT,
            lessons_learned TEXT,
            created_at TEXT
        )''')

        await conn.commit()

    async def save_report_conclusion(self, question: str, conclusion: str, ticker: str = "",
                                      position: float = 0, stop_loss: float = 0,
                                      take_profit: float = 0, holding_period: str = "",
                                      confidence: float = 0, key_points: str = "",
                                      risks: str = ""):
        """保存报告结论"""
        conn = await self._get_connection()
        await conn.execute('''INSERT INTO report_conclusions
            (question, conclusion, ticker, position, stop_loss, take_profit,
             holding_period, confidence, key_points, risks, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (question, conclusion, ticker, position, stop_loss, take_profit,
             holding_period, confidence, key_points, risks, datetime.now().isoformat()))
        await conn.commit()

    async def get_recent_conclusions(self, limit: int = 5) -> List[Dict]:
        """获取最近N次结论"""
        conn = await self._get_connection()
        async with conn.execute(
            '''SELECT * FROM report_conclusions ORDER BY created_at DESC LIMIT ?''',
            (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def save_reflection_summary(self, question: str, previous_conclusions: str,
                                       reflection_text: str, verification_results: str = "",
                                       adjusted_conclusion: str = "", lessons_learned: str = ""):
        """保存反思总结"""
        conn = await self._get_connection()
        await conn.execute('''INSERT INTO reflection_summary
            (question, previous_conclusions, reflection_text, verification_results,
             adjusted_conclusion, lessons_learned, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (question, previous_conclusions, reflection_text, verification_results,
             adjusted_conclusion, lessons_learned, datetime.now().isoformat()))
        await conn.commit()

    async def get_recent_reflections(self, limit: int = 10) -> List[Dict]:
        """获取最近反思总结"""
        conn = await self._get_connection()
        async with conn.execute(
            '''SELECT * FROM reflection_summary ORDER BY created_at DESC LIMIT ?''',
            (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
