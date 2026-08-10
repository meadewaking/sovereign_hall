"""
🏛️ Sovereign Hall - SQLite Database Service
数据库服务 - 使用 aiosqlite 实现真正的异步操作
"""

import asyncio
import aiosqlite
import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ..core import DATA_DIR
from ..infrastructure.sqlite.migrations import apply_architecture_migrations
from ..infrastructure.sqlite.unit_of_work import SQLiteUnitOfWork
from .prediction_store import ensure_prediction_schema

logger = logging.getLogger(__name__)

MIN_STORED_DOCUMENT_CHARS = 20
MAX_CANONICAL_LINEAGE_PER_PRESENTED_DOCUMENT = 50
IGNORED_DOCUMENT_SOURCES = {"obsidian_wiki"}
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {"from", "spm", "fbclid", "gclid", "yclid"}
NOISY_TITLE_RE = re.compile(
    r"(?:^|\b)(403|404|operations too frequent|google search|microsoft bing|search -|"
    r"youtube|tiktok|doubao\.com|chrome web store)(?:\b|$)",
    re.IGNORECASE,
)


def normalize_document_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_document_url(value: Any) -> str:
    url = str(value or "").strip().strip("\"'")
    if not url:
        return ""
    try:
        parsed = urlsplit(url)
    except ValueError:
        return url
    if not parsed.scheme or not parsed.netloc:
        return url
    query = [
        (key, val)
        for key, val in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_KEYS
        and not key.lower().startswith(TRACKING_QUERY_PREFIXES)
    ]
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/") or parsed.path,
            urlencode(query, doseq=True),
            "",
        )
    )


def document_content_hash(content: Any) -> str:
    return hashlib.sha256(normalize_document_text(content).encode("utf-8")).hexdigest()


def is_storable_document(title: Any, content: Any, source: Any, doc_id: Any = "") -> bool:
    source_text = str(source or "").strip()
    doc_id_text = str(doc_id or "").strip()
    title_text = normalize_document_text(title)
    content_text = normalize_document_text(content)
    if source_text in IGNORED_DOCUMENT_SOURCES or doc_id_text.startswith("wiki:"):
        return False
    if len(content_text) < MIN_STORED_DOCUMENT_CHARS:
        return False
    if len(title_text) < 4 or NOISY_TITLE_RE.search(title_text):
        return False
    if content_text in {
        "暂无内容",
        "内容获取失败",
        "关于的搜索结果",
        "关于的深度分析报告",
    }:
        return False
    if len(set(content_text)) < 8:
        return False
    return True


class DatabaseService:
    """SQLite数据库服务（异步版本）"""

    _instance = None
    _instances: Dict[str, "DatabaseService"] = {}

    @classmethod
    async def get_instance(cls, db_path: str = None) -> "DatabaseService":
        """Return one service per resolved database path.

        The previous process-wide singleton could silently return a connection
        to the wrong database in tests, imports, and consultation sessions.
        """
        resolved = str(Path(db_path or (DATA_DIR / "sovereign_hall.db")).resolve())
        if cls._instance is None:
            # Preserve the historical reset hook while clearing path-scoped
            # instances when callers deliberately reset ``_instance``.
            cls._instances = {}
        instance = cls._instances.get(resolved)
        if instance is None:
            instance = cls(resolved)
            cls._instances[resolved] = instance
        cls._instance = instance
        return instance

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
            await self._connection.execute("PRAGMA foreign_keys = ON")
            await self._connection.execute("PRAGMA busy_timeout = 5000")
            await self._connection.execute("PRAGMA journal_mode = WAL")
        return self._connection

    async def _get_existing_tables(self, conn) -> set:
        """获取现有表列表"""
        tables = set()
        async with conn.execute("SELECT name FROM sqlite_master WHERE type='table'") as cursor:
            async for row in cursor:
                tables.add(row[0])
        return tables

    async def _get_table_columns(self, conn, table: str) -> set:
        """获取表字段列表，用于轻量级迁移。"""
        columns = set()
        async with conn.execute(f"PRAGMA table_info({table})") as cursor:
            async for row in cursor:
                columns.add(row[1])
        return columns

    async def _add_column_if_missing(self, conn, table: str, column: str, definition: str):
        columns = await self._get_table_columns(conn, table)
        if column not in columns:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            logger.info("Migrated %s: added column %s", table, column)

    async def _create_index_if_possible(self, conn, sql: str):
        try:
            await conn.execute(sql)
        except Exception as exc:
            logger.warning("Failed to create index with SQL %r: %s", sql, exc)

    async def _next_integer_id(self, conn, table: str, column: str = "id") -> int:
        async with conn.execute(
            f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row[0] if row else 1)

    async def _backfill_report_conclusion_ids(self, conn):
        """旧库里 report_conclusions.id 可能只是普通 INT，历史写入会留下 NULL。"""
        columns = await self._get_table_columns(conn, "report_conclusions")
        if "id" not in columns:
            await self._add_column_if_missing(conn, "report_conclusions", "id", "INTEGER")

        next_id = await self._next_integer_id(conn, "report_conclusions", "id")
        async with conn.execute(
            "SELECT rowid FROM report_conclusions WHERE id IS NULL ORDER BY rowid"
        ) as cursor:
            rows = await cursor.fetchall()

        for row in rows:
            await conn.execute(
                "UPDATE report_conclusions SET id = ? WHERE rowid = ?",
                (next_id, row[0]),
            )
            next_id += 1

    async def _ensure_report_conclusion_id_key(self, conn):
        """Make the legacy conclusion id a valid SQLite foreign-key parent.

        A partial unique index does not qualify as a parent key in SQLite.
        Older databases therefore reported ``foreign key mismatch`` as soon as
        foreign-key enforcement was enabled, even though all non-null ids were
        unique. SQLite's ordinary UNIQUE index already permits multiple NULLs;
        ids are backfilled first, then the legacy partial index is replaced.
        """
        await self._backfill_report_conclusion_ids(conn)
        async with conn.execute(
            """
            SELECT id, COUNT(*)
            FROM report_conclusions
            GROUP BY id
            HAVING id IS NULL OR COUNT(*) > 1
            LIMIT 1
            """
        ) as cursor:
            invalid = await cursor.fetchone()
        if invalid:
            raise RuntimeError(
                "report_conclusions.id cannot become a foreign-key parent: "
                f"invalid id={invalid[0]!r}, count={invalid[1]}"
            )
        async with conn.execute(
            "PRAGMA index_list('report_conclusions')"
        ) as cursor:
            indexes = await cursor.fetchall()
        for index in indexes:
            if (
                str(index[1]) == "ux_report_conclusions_id"
                and int(index[2]) == 1
                and int(index[4]) == 0
            ):
                return
        await conn.execute("DROP INDEX IF EXISTS ux_report_conclusions_id")
        await conn.execute(
            "CREATE UNIQUE INDEX ux_report_conclusions_id "
            "ON report_conclusions(id)"
        )

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

        await self._add_column_if_missing(conn, "documents", "crawled_at", "TEXT")
        await self._add_column_if_missing(conn, "documents", "content_hash", "TEXT")
        await self._backfill_document_hashes(conn)

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
            await self._add_column_if_missing(conn, "proposals", "analyst_role", "TEXT")
            await self._add_column_if_missing(conn, "proposals", "sector", "TEXT")
            await self._add_column_if_missing(conn, "proposals", "status", "TEXT DEFAULT 'pending'")
        await self._add_column_if_missing(conn, "proposals", "holding_period_reason", "TEXT")
        await self._add_column_if_missing(conn, "proposals", "evidence", "TEXT")
        await self._add_column_if_missing(conn, "proposals", "resolved_rejection", "TEXT")
        await self._add_column_if_missing(conn, "proposals", "evidence_delta", "TEXT")
        await self._add_column_if_missing(conn, "proposals", "reject_if", "TEXT")

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
        else:
            await self._add_column_if_missing(conn, "blacklist", "reason", "TEXT")
            await self._add_column_if_missing(conn, "blacklist", "failure_count", "INTEGER DEFAULT 1")
            await self._add_column_if_missing(conn, "blacklist", "added_at", "TEXT")
            await self._add_column_if_missing(conn, "blacklist", "expires_at", "TEXT")
            columns = await self._get_table_columns(conn, "blacklist")
            if "created_at" in columns:
                await conn.execute(
                    "UPDATE blacklist SET added_at = COALESCE(added_at, created_at) WHERE added_at IS NULL"
                )
            await conn.execute("UPDATE blacklist SET failure_count = 1 WHERE failure_count IS NULL")

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

        # Stage-level diagnostics are first-class learning memory.  In
        # particular, candidate-bearing prose that failed JSON extraction must
        # not disappear as a generic "0 proposals" warning.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS research_stage_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                parse_mode TEXT,
                repair_modes TEXT,
                detected_tickers TEXT,
                raw_excerpt TEXT,
                reason TEXT,
                source TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Prediction rows declare a foreign key to report conclusions.  Create
        # the parent table in the base schema before enabling prediction writes;
        # relying on disabled SQLite foreign keys previously hid this ordering
        # defect in fresh databases.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS report_conclusions (
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
                created_at TEXT,
                learned_at TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reflection_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT,
                previous_conclusions TEXT,
                reflection_text TEXT,
                verification_results TEXT,
                adjusted_conclusion TEXT,
                lessons_learned TEXT,
                created_at TEXT,
                learned_at TEXT
            )
        """)
        await self._ensure_report_conclusion_id_key(conn)

        await ensure_prediction_schema(conn)
        await apply_architecture_migrations(conn)

        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_documents_sector ON documents(sector)")
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url)")
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_documents_content_hash ON documents(content_hash)")
        await self._create_index_if_possible(
            conn,
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_url "
            "ON documents(url) WHERE url IS NOT NULL AND url <> ''",
        )
        await self._create_index_if_possible(
            conn,
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_content_hash "
            "ON documents(content_hash) WHERE content_hash IS NOT NULL AND content_hash <> ''",
        )
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_proposals_ticker ON proposals(ticker)")
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status)")
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_meetings_proposal ON meetings(proposal_id)")
        await self._create_index_if_possible(conn, "CREATE INDEX IF NOT EXISTS idx_playbook_ticker ON playbook(ticker)")
        await self._create_index_if_possible(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_research_stage_diagnostics_created "
            "ON research_stage_diagnostics(stage, created_at DESC)",
        )

        await conn.commit()
        self._initialized = True
        logger.info(f"Database initialized: {self.db_path}")

    async def _ensure_initialized(self):
        if not self._initialized:
            await self._init_db()

    async def _backfill_document_hashes(self, conn):
        columns = await self._get_table_columns(conn, "documents")
        if "content_hash" not in columns:
            return
        async with conn.execute(
            "SELECT id, content FROM documents WHERE content_hash IS NULL OR content_hash = ''"
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            await conn.execute(
                "UPDATE documents SET content_hash = ? WHERE id = ?",
                (document_content_hash(row["content"]), row["id"]),
            )

    # ===================== 文档操作 =====================

    async def add_document(self, doc: Any, *, round_id: str | None = None) -> bool:
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

        title = normalize_document_text(_attr('title', ''))
        content = normalize_document_text(_attr('content', ''))
        url = normalize_document_url(_attr('url', ''))
        source = str(_attr('source', '') or '').strip()
        sector = str(_attr('sector', '') or '').strip()
        content_hash = document_content_hash(content)
        doc_id = _attr('id') or _attr('doc_id') or f"doc_{content_hash[:16]}"

        if not is_storable_document(title, content, source, doc_id):
            logger.debug("Skipped non-storable document: source=%s title=%r", source, title[:80])
            return False

        conn = await self._get_connection()
        await self._ensure_initialized()

        async with conn.execute(
            """
            SELECT id, LENGTH(COALESCE(content, '')) AS content_len
            FROM documents
            WHERE id = ?
               OR (? <> '' AND url = ?)
               OR content_hash = ?
            ORDER BY
                CASE
                    WHEN id = ? THEN 0
                    WHEN ? <> '' AND url = ? THEN 1
                    ELSE 2
                END
            LIMIT 1
            """,
            (doc_id, url, url, content_hash, doc_id, url, url),
        ) as cursor:
            existing = await cursor.fetchone()

        keywords = _attr('keywords', [])
        if isinstance(keywords, str):
            try:
                keywords = json.loads(keywords)
            except json.JSONDecodeError:
                keywords = [keywords] if keywords.strip() else []

        values = (
            doc_id,
            title,
            content,
            url,
            source,
            sector,
            json.dumps(keywords, ensure_ascii=False) if keywords else None,
            publish_time,
            json.dumps(_attr('embedding')) if _attr('embedding') else None,
            content_hash,
        )

        if existing:
            if len(content) > int(existing["content_len"] or 0):
                # The matching URL/id row and matching content-hash row can be
                # different legacy records.  Updating the former to the
                # latter's hash would violate the unique index and turn an
                # ordinary duplicate into a noisy per-round warning.
                async with conn.execute(
                    """
                    SELECT id
                    FROM documents
                    WHERE content_hash = ? AND id <> ?
                    LIMIT 1
                    """,
                    (content_hash, existing["id"]),
                ) as cursor:
                    hash_owner = await cursor.fetchone()
                if hash_owner:
                    await self._link_round_document(
                        conn,
                        round_id,
                        str(hash_owner["id"]),
                    )
                    if round_id:
                        await conn.commit()
                    logger.debug(
                        "Skipped cross-key duplicate document: existing_id=%s hash_owner=%s",
                        existing["id"],
                        hash_owner["id"],
                    )
                    return False
                await conn.execute(
                    """
                    UPDATE documents
                    SET title = ?, content = ?, url = ?, source = ?, sector = ?,
                        keywords = ?, publish_time = ?, embedding = ?, content_hash = ?,
                        crawled_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    values[1:] + (existing["id"],),
                )
                await self._link_round_document(conn, round_id, str(existing["id"]))
                await conn.commit()
                return True
            await self._link_round_document(conn, round_id, str(existing["id"]))
            if round_id:
                await conn.commit()
            return False

        await conn.execute("""
            INSERT INTO documents
            (id, title, content, url, source, sector, keywords, publish_time,
             embedding, content_hash, crawled_at, round_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """, values + (round_id,))
        await self._link_round_document(conn, round_id, str(doc_id))
        await conn.commit()
        return True

    async def _link_round_document(
        self,
        conn,
        round_id: str | None,
        document_id: str,
        *,
        usage: str = "source",
    ) -> None:
        if not round_id:
            return
        await conn.execute(
            """
            INSERT OR IGNORE INTO round_documents(
                round_id, document_id, usage, linked_at
            ) VALUES (?, ?, ?, ?)
            """,
            (round_id, document_id, usage, datetime.now().isoformat()),
        )

    async def resolve_document_lineage(
        self,
        docs: List[Any],
    ) -> List[List[str]]:
        """Resolve presented research documents to canonical SQLite sources.

        Wiki search results are derived pages and are deliberately not inserted
        into ``documents``.  Their frontmatter retains a source id, URL, or
        source-title references that can be mapped back to the raw durable
        documents.  The aligned return value lets the production loop exclude
        any presented document whose evidence cannot be audited by round id.
        """
        await self._ensure_initialized()
        conn = await self._get_connection()

        identities: List[Dict[str, set[str]]] = []
        all_values: Dict[str, set[str]] = {
            "id": set(),
            "url": set(),
            "title": set(),
            "content_hash": set(),
        }
        for doc in docs or []:
            metadata = getattr(doc, "metadata", {}) or {}
            doc_id = str(
                getattr(doc, "id", "")
                or getattr(doc, "doc_id", "")
                or ""
            ).strip()
            source = str(getattr(doc, "source", "") or "").strip()
            is_wiki = source == "obsidian_wiki" or doc_id.startswith("wiki:")
            values = {name: set() for name in all_values}

            source_id = str(metadata.get("source_id") or "").strip()
            if source_id:
                values["id"].add(source_id)
            if doc_id and not is_wiki:
                values["id"].add(doc_id)

            url = normalize_document_url(
                getattr(doc, "url", "") or metadata.get("url", "")
            )
            if url:
                values["url"].add(url)

            source_references = metadata.get("source_ids") or []
            if isinstance(source_references, str):
                source_references = [source_references]
            for reference in source_references:
                reference_text = str(reference or "").strip()
                if not reference_text:
                    continue
                if reference_text.startswith(("http://", "https://")):
                    values["url"].add(normalize_document_url(reference_text))
                elif reference_text.startswith("doc_"):
                    values["id"].add(reference_text)
                else:
                    values["title"].add(normalize_document_text(reference_text))

            if is_wiki and str(metadata.get("wiki_type") or "") == "source":
                title = normalize_document_text(
                    getattr(doc, "title", "") or metadata.get("title", "")
                )
                if title:
                    values["title"].add(title)
            if not is_wiki:
                content = getattr(doc, "content", "")
                if content:
                    values["content_hash"].add(document_content_hash(content))

            identities.append(values)
            for field, candidates in values.items():
                all_values[field].update(value for value in candidates if value)

        canonical: Dict[str, Dict[str, set[str]]] = {
            name: {} for name in all_values
        }
        for field, candidates in all_values.items():
            ordered = sorted(candidates)
            for offset in range(0, len(ordered), 300):
                chunk = ordered[offset : offset + 300]
                placeholders = ",".join("?" for _ in chunk)
                async with conn.execute(
                    f"SELECT id, {field} FROM documents "
                    f"WHERE {field} IN ({placeholders})",
                    chunk,
                ) as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    key = str(row[field] or "")
                    canonical[field].setdefault(key, set()).add(str(row["id"]))

        resolved: List[List[str]] = []
        for values in identities:
            # IDs and URLs are explicit lineage and take precedence over
            # weaker hashes/titles.  Unioning every identity class allowed a
            # derived page with a large ``source_ids`` accumulator (or a
            # generic exact title) to attach tens of thousands of unrelated
            # documents to one round even though only hundreds were shown.
            document_ids: set[str] = set()
            for field in ("id", "url"):
                for candidate in values[field]:
                    document_ids.update(canonical[field].get(candidate, set()))

            if not document_ids:
                for candidate in values["content_hash"]:
                    matches = canonical["content_hash"].get(candidate, set())
                    if len(matches) == 1:
                        document_ids.update(matches)

            if not document_ids:
                for candidate in values["title"]:
                    matches = canonical["title"].get(candidate, set())
                    if len(matches) == 1:
                        document_ids.update(matches)

            if len(document_ids) > MAX_CANONICAL_LINEAGE_PER_PRESENTED_DOCUMENT:
                logger.warning(
                    "Excluded presented document with lineage fanout=%s above limit=%s",
                    len(document_ids),
                    MAX_CANONICAL_LINEAGE_PER_PRESENTED_DOCUMENT,
                )
                document_ids = set()
            resolved.append(sorted(document_ids))
        return resolved

    async def get_document(self, doc_id: str) -> Optional[Dict]:
        """获取文档"""
        await self._ensure_initialized()
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
        await self._ensure_initialized()
        conn = await self._get_connection()
        sql = "SELECT * FROM documents WHERE COALESCE(source, '') <> 'obsidian_wiki'"
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
        await self._ensure_initialized()
        conn = await self._get_connection()
        async with conn.execute("SELECT COUNT(*) FROM documents") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # ===================== 提案操作 =====================

    async def add_proposal(self, proposal: Any, *, round_id: str | None = None):
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

        # Older project databases have a ``created_at`` column without a
        # DEFAULT, so omitting it silently stored NULL for every proposal.
        # Preserve an explicitly supplied timestamp for reproducible imports;
        # otherwise stamp only the new row.  Historical NULLs are deliberately
        # not backfilled because their true creation time is unknowable.
        created_at = pget("created_at") or datetime.now().isoformat()
        round_id = round_id or pget("round_id")
        proposal_id = pget('proposal_id') or pget('id')
        conn = await self._get_connection()
        await conn.execute("""
            INSERT OR REPLACE INTO proposals
            (proposal_id, ticker, direction, target_position, entry_price, stop_loss,
             take_profit, holding_period, confidence, thesis, analyst_role, sector, status,
             created_at, holding_period_reason, evidence, resolved_rejection,
             evidence_delta, reject_if, round_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            proposal_id,
            pget('ticker', ''),
            pget('direction', 'long'),
            pget('target_position', 0.0),
            pget('entry_price', 0.0),
            pget('stop_loss', 0.0),
            pget('take_profit', pget('target_price', 0.0)),
            pget('holding_period', 30),
            pget('confidence', 0.0),
            pget('thesis', '') if pget('thesis') else None,
            analyst_role,
            pget('sector', None),
            pget('status', 'pending'),
            created_at,
            pget('holding_period_reason', None),
            json.dumps(pget('evidence', []), ensure_ascii=False),
            pget('resolved_rejection', None),
            pget('evidence_delta', None),
            pget('reject_if', None),
            round_id,
        ))
        await conn.commit()
        return proposal_id

    async def get_proposals(self, status: str = None, limit: int = 100) -> List[Dict]:
        """获取提案列表"""
        conn = await self._get_connection()
        sql = "SELECT * FROM proposals"
        params = []

        if status:
            sql += " WHERE status = ?"
            params.append(status)

        # Legacy proposal rows may have NULL timestamps.  Keep deterministic
        # rowid ordering for those records while always preferring timestamped
        # rows written by the repaired path.
        sql += (
            " ORDER BY CASE WHEN created_at IS NULL OR trim(created_at) = '' "
            "THEN 1 ELSE 0 END, datetime(created_at) DESC, rowid DESC LIMIT ?"
        )
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

    async def add_meeting_record(
        self,
        *,
        meeting_id: str,
        proposal_id: str,
        ticker: str,
        decision: str,
        discussion: str,
        vote_details: Dict[str, Any],
        action_items: List[str],
        round_id: str | None = None,
    ) -> str:
        """Persist one committee deliberation without requiring a domain object."""
        conn = await self._get_connection()
        await conn.execute(
            """
            INSERT INTO meetings
            (id, proposal_id, ticker, decision, discussion, vote_details,
             action_items, round_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                proposal_id,
                ticker,
                decision,
                discussion,
                json.dumps(vote_details, ensure_ascii=False, default=str),
                json.dumps(action_items, ensure_ascii=False, default=str),
                round_id,
            ),
        )
        await conn.commit()
        return meeting_id

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
        columns = await self._get_table_columns(conn, "playbook")
        entry_id = getattr(entry, 'id', None) or getattr(entry, 'entry_id', None)
        confidence_delta = (
            getattr(entry, 'confidence_delta', 0.0)
            or getattr(entry, 'confidence_adjustment', 0.0)
        )
        now = datetime.now().isoformat()

        if {"id", "ticker", "situation", "lesson", "outcome", "success", "confidence_adjustment", "created_at"}.issubset(columns):
            await conn.execute("""
                INSERT OR REPLACE INTO playbook
                (id, ticker, situation, lesson, outcome, success, confidence_adjustment, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry_id,
                getattr(entry, 'ticker', None),
                getattr(entry, 'situation', None),
                entry.lesson,
                getattr(entry, 'outcome', None),
                getattr(entry, 'success', None),
                confidence_delta,
                now
            ))
        elif {"entry_id", "category", "situation", "action_taken", "outcome", "lesson", "confidence_delta", "ticker", "refs", "created_at"}.issubset(columns):
            await conn.execute("""
                INSERT OR REPLACE INTO playbook
                (entry_id, category, situation, action_taken, outcome, lesson, confidence_delta, ticker, refs, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry_id,
                getattr(entry, 'pattern', None) or "manual",
                getattr(entry, 'situation', None),
                getattr(entry, 'action', None),
                getattr(entry, 'outcome', None),
                entry.lesson,
                confidence_delta,
                getattr(entry, 'ticker', None),
                json.dumps(getattr(entry, 'examples', []), ensure_ascii=False),
                now,
            ))
        else:
            raise RuntimeError(f"Unsupported playbook schema: {sorted(columns)}")
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

    async def record_research_stage_diagnostic(
        self,
        *,
        topic: str,
        stage: str,
        status: str,
        parse_mode: str = "",
        repair_modes: Optional[List[str]] = None,
        detected_tickers: Optional[List[str]] = None,
        raw_excerpt: str = "",
        reason: str = "",
        source: str = "run_discussion",
        round_id: str | None = None,
    ) -> int:
        """Persist an exact pipeline terminal state for next-round learning."""
        await self._ensure_initialized()
        conn = await self._get_connection()
        cursor = await conn.execute(
            """
            INSERT INTO research_stage_diagnostics (
                topic, stage, status, parse_mode, repair_modes,
                detected_tickers, raw_excerpt, reason, source, created_at,
                round_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(topic or ""),
                str(stage or ""),
                str(status or ""),
                str(parse_mode or ""),
                json.dumps(repair_modes or [], ensure_ascii=False),
                json.dumps(detected_tickers or [], ensure_ascii=False),
                str(raw_excerpt or "")[:8000],
                str(reason or "")[:2000],
                str(source or "run_discussion"),
                datetime.now().isoformat(),
                round_id,
            ),
        )
        await conn.commit()
        return int(cursor.lastrowid)

    async def get_recent_research_stage_diagnostics(
        self,
        *,
        stage: str = "stage2",
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Load recent diagnostics without interpreting them as market facts."""
        await self._ensure_initialized()
        conn = await self._get_connection()
        async with conn.execute(
            """
            SELECT *
            FROM research_stage_diagnostics
            WHERE stage = ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (str(stage), max(0, int(limit))),
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

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
    async def transaction(self, *, immediate: bool = False):
        """Canonical transaction boundary for atomic application writes."""
        conn = await self._get_connection()
        async with SQLiteUnitOfWork(conn).transaction(immediate=immediate) as active:
            yield active

    async def close(self):
        """关闭连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database connection closed")
        resolved = str(self.db_path.resolve())
        type(self)._instances.pop(resolved, None)
        if type(self)._instance is self:
            type(self)._instance = None

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

        await self._add_column_if_missing(conn, "report_conclusions", "learned_at", "TEXT")
        await self._add_column_if_missing(conn, "reflection_summary", "learned_at", "TEXT")
        await self._add_column_if_missing(conn, "report_conclusions", "round_id", "TEXT")
        await self._add_column_if_missing(conn, "reflection_summary", "round_id", "TEXT")
        await self._ensure_report_conclusion_id_key(conn)

        await conn.commit()

    async def save_report_conclusion(self, question: str, conclusion: str, ticker: str = "",
                                      position: float = 0, stop_loss: float = 0,
                                      take_profit: float = 0, holding_period: str = "",
                                      confidence: float = 0, key_points: str = "",
                                      risks: str = "", round_id: str | None = None):
        """保存报告结论"""
        conn = await self._get_connection()
        conclusion_id = await self._next_integer_id(conn, "report_conclusions", "id")
        await conn.execute('''INSERT INTO report_conclusions
            (id, question, conclusion, ticker, position, stop_loss, take_profit,
             holding_period, confidence, key_points, risks, created_at, round_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (conclusion_id, question, conclusion, ticker, position, stop_loss, take_profit,
             holding_period, confidence, key_points, risks, datetime.now().isoformat(),
             round_id))
        await conn.commit()
        return conclusion_id

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
                                       adjusted_conclusion: str = "", lessons_learned: str = "",
                                       round_id: str | None = None):
        """保存反思总结"""
        conn = await self._get_connection()
        await conn.execute('''INSERT INTO reflection_summary
            (question, previous_conclusions, reflection_text, verification_results,
             adjusted_conclusion, lessons_learned, created_at, round_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (question, previous_conclusions, reflection_text, verification_results,
             adjusted_conclusion, lessons_learned, datetime.now().isoformat(), round_id))
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
