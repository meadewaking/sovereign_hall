"""
🏛️ Sovereign Hall - Vector Database Service
向量数据库服务 - SQLite + FAISS 高效存储和检索
"""

import asyncio
import logging
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
import json
import shutil

import aiosqlite

from ..core.config import get_config
from ..core import Document as Doc, DATA_DIR
from ..utils import ensure_dir, generate_id
from .llm_client import LLMClient

logger = logging.getLogger(__name__)


class VectorDatabase:
    """向量数据库 - SQLite存储 + FAISS索引"""

    DEFAULT_MAX_DOCUMENTS = 10000
    DB_NAME = "vector_store.db"

    def __init__(
        self,
        dimension: int = 1024,
        index_type: str = "Flat",
        nlist: int = 100,
        metric: str = "cosine",
        storage_path: str = None,
        max_documents: int = None,
    ):
        if storage_path is None:
            storage_path = str(DATA_DIR / "vector_db")
        """
        初始化向量数据库

        Args:
            dimension: 向量维度
            index_type: 索引类型 (Flat/IVF/HNSW)
            nlist: IVF索引的聚类中心数
            metric: 距离度量 (cosine/euclidean/dot_product)
            storage_path: 存储路径
            max_documents: 最大文档数量
        """
        self.dimension = dimension
        self.index_type = index_type
        self.nlist = nlist
        self.metric = metric
        self.storage_path = Path(storage_path)
        self.max_documents = max_documents or self.DEFAULT_MAX_DOCUMENTS
        self.db_path = self.storage_path / self.DB_NAME

        # 内存缓存（LRU）
        self.documents: Dict[str, Doc] = {}
        self.embeddings: Dict[str, np.ndarray] = {}
        self._id_list: List[str] = []  # 有序ID列表用于LRU

        # FAISS索引
        self._index = None
        self._index_ready = False
        self._id_to_idx: Dict[str, int] = {}  # doc_id -> index position
        self._idx_to_id: Dict[int, str] = {}  # index position -> doc_id

        # 异步连接
        self._db: Optional[aiosqlite.Connection] = None

        # 自动持久化
        self._persist_task: Optional[asyncio.Task] = None
        self._persist_interval = 300
        self._last_persist_time = datetime.now()
        self._dirty = False

        ensure_dir(self.storage_path)
        logger.info(f"Vector DB initialized: dim={dimension}, type={index_type}, path={storage_path}")

    async def _get_db(self) -> aiosqlite.Connection:
        """获取数据库连接"""
        if self._db is None:
            self._db = await aiosqlite.connect(str(self.db_path))
            await self._init_tables()
        return self._db

    async def _init_tables(self):
        """初始化表结构"""
        db = self._db
        # 文档表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                url TEXT,
                source TEXT,
                sector TEXT,
                keywords TEXT,
                publish_time TEXT,
                crawled_at TEXT,
                embedding BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 向量索引表（FAISS序列化）
        await db.execute("""
            CREATE TABLE IF NOT EXISTS faiss_index (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                index_data BLOB,
                id_to_idx TEXT,
                idx_to_id TEXT,
                updated_at TEXT
            )
        """)
        # LRU顺序表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lru_order (
                doc_id TEXT PRIMARY KEY,
                position INTEGER,
                updated_at TEXT
            )
        """)
        await db.commit()

    async def initialize(self, llm_client: LLMClient = None):
        """初始化索引和加载现有数据"""
        await self._get_db()
        await self._load_from_db()

        if self.embeddings:
            await self._build_index()

        self._start_auto_persist()
        logger.info(f"VectorDB ready: {len(self.documents)} docs, {len(self.embeddings)} vectors")

    def _start_auto_persist(self):
        """启动自动持久化"""
        if self._persist_task is None or self._persist_task.done():
            self._persist_task = asyncio.create_task(self._auto_persist_loop())

    async def _auto_persist_loop(self):
        """自动持久化循环"""
        while True:
            try:
                await asyncio.sleep(self._persist_interval)
                if self._dirty and self._should_persist():
                    await self._persist_index()
                    await self._save_lru_order()
                    self._last_persist_time = datetime.now()
                    self._dirty = False
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-persist error: {e}")

    def _should_persist(self) -> bool:
        elapsed = (datetime.now() - self._last_persist_time).total_seconds()
        return elapsed >= self._persist_interval

    def _evict_oldest(self):
        """LRU淘汰最旧的文档"""
        if len(self.documents) >= self.max_documents and self._id_list:
            oldest_id = self._id_list[0]
            self._remove_doc(oldest_id)
            logger.info(f"LRU evicted: {oldest_id}, remaining: {len(self.documents)}")

    def _remove_doc(self, doc_id: str):
        """移除文档"""
        if doc_id in self.documents:
            del self.documents[doc_id]
        if doc_id in self.embeddings:
            del self.embeddings[doc_id]
        if doc_id in self._id_list:
            self._id_list.remove(doc_id)
        if doc_id in self._id_to_idx:
            idx = self._id_to_idx[doc_id]
            del self._id_to_idx[doc_id]
            del self._idx_to_id[idx]

    async def add_document(self, doc: Doc, embedding: List[float] = None, llm_client: LLMClient = None):
        """添加文档"""
        # LRU淘汰
        if len(self.documents) >= self.max_documents:
            self._evict_oldest()

        # 生成embedding
        if llm_client and embedding is None:
            try:
                text = f"{doc.title} {doc.content}"[:8000]
                emb = await llm_client.get_embedding(text)
                embedding = emb
            except Exception as e:
                logger.warning(f"Failed to generate embedding: {e}")

        # 存储到内存
        self.documents[doc.id] = doc
        if doc.id not in self._id_list:
            self._id_list.append(doc.id)

        if embedding:
            self.embeddings[doc.id] = np.array(embedding, dtype=np.float32)

        # 保存到数据库
        await self._save_doc_to_db(doc, embedding)
        self._dirty = True

        # 更新FAISS索引
        if embedding:
            await self._add_to_index(doc.id, self.embeddings[doc.id])

    async def _save_doc_to_db(self, doc: Doc, embedding: List[float] = None):
        """保存文档到SQLite"""
        db = await self._get_db()
        emb_bytes = None
        if embedding:
            emb_bytes = np.array(embedding, dtype=np.float32).tobytes()

        # Document使用metadata存储字段
        publish_time = doc.metadata.get("publish_time", "")
        crawled_at = doc.metadata.get("crawled_at", "")

        await db.execute("""
            INSERT OR REPLACE INTO documents
            (id, title, content, url, source, sector, keywords, publish_time, crawled_at, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            doc.id, doc.title, doc.content, doc.url, doc.source,
            doc.sector, json.dumps(doc.keywords), publish_time, crawled_at, emb_bytes
        ))
        await db.commit()

    async def _add_to_index(self, doc_id: str, vector: np.ndarray):
        """添加向量到FAISS索引"""
        if self._index is None:
            await self._build_index()

        if self._index_ready:
            try:
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = vector / norm
                # 新向量添加到末尾
                idx = len(self._idx_to_id)
                self._index.add(np.array([vector], dtype=np.float32))
                self._id_to_idx[doc_id] = idx
                self._idx_to_id[idx] = doc_id
            except Exception as e:
                logger.warning(f"Failed to add to index: {e}")

    async def add_documents_batch(self, docs: List[Doc], llm_client: LLMClient = None):
        """批量添加文档"""
        if not docs:
            return

        logger.info(f"Adding {len(docs)} documents in batch")

        for doc in docs:
            if isinstance(doc, dict):
                doc = Doc.from_dict(doc)
            await self.add_document(doc, llm_client=llm_client)

        logger.info(f"Batch add complete: {len(self.documents)} total docs")

    async def search(
        self,
        query: str,
        top_k: int = 10,
        filter_sector: str = None,
        min_similarity: float = 0.0,
        llm_client: LLMClient = None,
    ) -> List[Doc]:
        """语义搜索"""
        if not self.documents or not llm_client:
            return []

        try:
            # 获取查询向量
            query_vec = await llm_client.get_embedding(query)
            query_vec = np.array([query_vec], dtype=np.float32)
            norm = np.linalg.norm(query_vec)
            if norm > 0:
                query_vec = query_vec / norm

            # 使用FAISS搜索
            if self._index_ready and self._index is not None:
                scores, indices = self._index.search(query_vec, min(top_k * 2, len(self.documents)))

                results = []
                for score, idx in zip(scores[0], indices[0]):
                    if idx < 0:
                        continue
                    doc_id = self._idx_to_id.get(int(idx))
                    if not doc_id or doc_id not in self.documents:
                        continue
                    doc = self.documents[doc_id]
                    if filter_sector and doc.sector != filter_sector:
                        continue
                    if score >= min_similarity:
                        results.append((float(score), doc))

                results.sort(key=lambda x: x[0], reverse=True)
                return [doc for _, doc in results[:top_k]]
            else:
                # 后备：暴力搜索
                return await self._brute_force_search(query_vec[0], top_k, filter_sector, min_similarity)

        except Exception as e:
            logger.warning(f"Search failed: {e}")
            return []

    async def _brute_force_search(
        self,
        query_vec: np.ndarray,
        top_k: int,
        filter_sector: str = None,
        min_similarity: float = 0.0,
    ) -> List[Doc]:
        """暴力搜索（后备）"""
        results = []
        for doc_id, doc_vec in self.embeddings.items():
            doc = self.documents[doc_id]
            if filter_sector and doc.sector != filter_sector:
                continue
            similarity = self._cosine_similarity(query_vec, doc_vec)
            if similarity >= min_similarity:
                results.append((similarity, doc))

        results.sort(key=lambda x: x[0], reverse=True)
        return [doc for _, doc in results[:top_k]]

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """余弦相似度"""
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    async def _build_index(self):
        """构建FAISS索引"""
        if not self.embeddings:
            return

        try:
            import faiss

            # 准备数据
            ids = list(self.embeddings.keys())
            vectors = np.array([self.embeddings[id_] for id_ in ids], dtype=np.float32)

            # 归一化向量（用于余弦相似度）
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            norms[norms == 0] = 1
            vectors = vectors / norms

            # 创建索引
            if self.index_type == "Flat":
                self._index = faiss.IndexFlatIP(self.dimension)
            elif self.index_type == "IVF":
                quantizer = faiss.IndexFlatIP(self.dimension)
                self._index = faiss.IndexIVFFlat(quantizer, self.dimension, self.nlist, faiss.METRIC_INNER_PRODUCT)
                self._index.train(vectors)
            elif self.index_type == "HNSW":
                self._index = faiss.IndexHNSWFlat(self.dimension, 32)
            else:
                self._index = faiss.IndexFlatIP(self.dimension)

            self._index.add(vectors)

            # 建立ID映射
            for idx, doc_id in enumerate(ids):
                self._id_to_idx[doc_id] = idx
                self._idx_to_id[idx] = doc_id

            self._index_ready = True
            logger.info(f"FAISS index built: {len(ids)} vectors")

        except ImportError:
            logger.warning("FAISS not installed")
            self._index_ready = False
        except Exception as e:
            logger.error(f"Failed to build index: {e}")
            self._index_ready = False

    async def _load_from_db(self):
        """从SQLite加载数据"""
        db = await self._get_db()

        # 加载文档和向量
        async with db.execute("""
            SELECT id, title, content, url, source, sector, keywords, publish_time, crawled_at, embedding
            FROM documents
        """) as cursor:
            rows = await cursor.fetchall()

        for row in rows:
            doc_id, title, content, url, source, sector, keywords, publish_time, crawled_at, emb_bytes = row

            keywords_list = json.loads(keywords) if keywords else []
            doc = Doc(
                content=content or "",
                title=title or "",
                url=url,
                source=source,
                sector=sector,
                keywords=keywords_list,
                metadata={
                    "publish_time": publish_time,
                    "crawled_at": crawled_at,
                }
            )
            doc.doc_id = doc_id
            self.documents[doc_id] = doc
            self._id_list.append(doc_id)

            if emb_bytes:
                self.embeddings[doc_id] = np.frombuffer(emb_bytes, dtype=np.float32)

        # 加载LRU顺序
        async with db.execute("SELECT doc_id FROM lru_order ORDER BY position") as cursor:
            rows = await cursor.fetchall()
            if rows:
                self._id_list = [row[0] for row in rows]

        logger.info(f"Loaded {len(self.documents)} docs from SQLite")

    async def _save_lru_order(self):
        """保存LRU顺序"""
        db = await self._get_db()
        await db.execute("DELETE FROM lru_order")
        for pos, doc_id in enumerate(self._id_list):
            await db.execute(
                "INSERT INTO lru_order (doc_id, position, updated_at) VALUES (?, ?, ?)",
                (doc_id, pos, datetime.now().isoformat())
            )
        await db.commit()

    async def _persist_index(self):
        """持久化FAISS索引"""
        if self._index is None:
            return

        db = await self._get_db()
        try:
            import faiss
            index_data = faiss.serialize_index(self._index)
            await db.execute("""
                INSERT OR REPLACE INTO faiss_index (id, index_data, id_to_idx, idx_to_id, updated_at)
                VALUES (1, ?, ?, ?, ?)
            """, (
                index_data,
                json.dumps(self._id_to_idx),
                json.dumps(self._idx_to_id),
                datetime.now().isoformat()
            ))
            await db.commit()
            logger.info("FAISS index persisted")
        except Exception as e:
            logger.warning(f"Failed to persist index: {e}")

    async def save(self):
        """保存整个数据库"""
        await self._persist_index()
        await self._save_lru_order()
        self._dirty = False
        logger.info(f"DB saved: {len(self.documents)} docs")

    async def clear(self):
        """清空数据库"""
        if self._persist_task:
            self._persist_task.cancel()

        self.documents.clear()
        self.embeddings.clear()
        self._id_list.clear()
        self._index = None
        self._index_ready = False
        self._id_to_idx.clear()
        self._idx_to_id.clear()

        if self._db:
            await self._db.execute("DELETE FROM documents")
            await self._db.execute("DELETE FROM faiss_index")
            await self._db.execute("DELETE FROM lru_order")
            await self._db.commit()

        logger.info("Database cleared")

    async def close(self):
        """关闭数据库"""
        if self._persist_task:
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass

        await self.save()

        if self._db:
            await self._db.close()
            self._db = None

        logger.info("Vector database closed")

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'total_documents': len(self.documents),
            'indexed_vectors': len(self.embeddings),
            'dimension': self.dimension,
            'index_type': self.index_type,
            'index_ready': self._index_ready,
            'storage_path': str(self.storage_path),
            'sectors': self._get_sector_distribution(),
        }

    def _get_sector_distribution(self) -> Dict[str, int]:
        distribution = {}
        for doc in self.documents.values():
            sector = doc.sector or "未知"
            distribution[sector] = distribution.get(sector, 0) + 1
        return distribution

    def __len__(self):
        return len(self.documents)

    def __contains__(self, doc_id):
        return doc_id in self.documents
