"""
🏛️ Sovereign Hall - Vector Database Service
向量数据库服务 - 语义检索增强生成
"""

import asyncio
import logging
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
import json
import pickle
import shutil
from collections import OrderedDict

from ..core.config import get_config
from ..core import Document as Doc, DATA_DIR
from ..utils import ensure_dir, save_json, load_json, generate_id
from .llm_client import LLMClient

logger = logging.getLogger(__name__)


class VectorDatabase:
    """向量数据库 - 用于语义检索"""

    # 默认内存上限
    DEFAULT_MAX_DOCUMENTS = 10000

    def __init__(
        self,
        dimension: int = 1536,
        index_type: str = "IVF",
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
            index_type: 索引类型 (IVF/Flat/HNSW)
            nlist: IVF索引的聚类中心数
            metric: 距离度量 (cosine/euclidean/dot_product)
            storage_path: 存储路径
            max_documents: 最大文档数量（用于内存管理）
        """
        self.dimension = dimension
        self.index_type = index_type
        self.nlist = nlist
        self.metric = metric
        self.storage_path = Path(storage_path)
        self.max_documents = max_documents or self.DEFAULT_MAX_DOCUMENTS

        # 存储（使用 OrderedDict 实现 LRU）
        self.documents: OrderedDict[str, Doc] = OrderedDict()
        self.embeddings: OrderedDict[str, np.ndarray] = OrderedDict()

        # 索引（延迟初始化）
        self._index = None
        self._index_ready = False

        # 自动持久化任务
        self._persist_task: Optional[asyncio.Task] = None
        self._persist_interval = 300  # 5分钟
        self._last_persist_time = datetime.now()

        # 初始化存储目录
        ensure_dir(self.storage_path)
        ensure_dir(self.storage_path / "documents")
        ensure_dir(self.storage_path / "embeddings")

        logger.info(f"Vector DB initialized: dim={dimension}, type={index_type}, path={storage_path}, max_docs={self.max_documents}")

    async def initialize(self, llm_client: LLMClient = None):
        """初始化索引和加载现有数据"""
        # 尝试加载现有数据
        await self._load_from_disk()

        # 创建索引
        if self.documents:
            await self._build_index()

        # 启动自动持久化任务
        self._start_auto_persist()

    def _start_auto_persist(self):
        """启动自动持久化任务"""
        if self._persist_task is None or self._persist_task.done():
            self._persist_task = asyncio.create_task(self._auto_persist_loop())
            logger.info("Auto-persist task started")

    async def _auto_persist_loop(self):
        """自动持久化循环"""
        while True:
            try:
                await asyncio.sleep(self._persist_interval)
                # 检查是否需要持久化（距离上次持久化超过间隔，且有变更）
                if self._should_persist():
                    await self.save()
                    self._last_persist_time = datetime.now()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-persist error: {e}")

    def _should_persist(self) -> bool:
        """检查是否需要持久化"""
        elapsed = (datetime.now() - self._last_persist_time).total_seconds()
        return elapsed >= self._persist_interval

    def _evict_oldest(self):
        """LRU 淘汰：移除最旧的文档"""
        if len(self.documents) >= self.max_documents:
            # 移除最旧的文档（ OrderedDict 的第一个元素）
            oldest_doc_id = next(iter(self.documents))
            del self.documents[oldest_doc_id]
            if oldest_doc_id in self.embeddings:
                del self.embeddings[oldest_doc_id]

            # 删除磁盘文件
            try:
                (self.storage_path / "documents" / f"{oldest_doc_id}.json").unlink(missing_ok=True)
                (self.storage_path / "embeddings" / f"{oldest_doc_id}.npy").unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Failed to delete evicted doc files: {e}")

            logger.info(f"LRU evicted document: {oldest_doc_id}, current count: {len(self.documents)}")

    async def add_document(self, doc: Doc, embedding: List[float] = None, llm_client: LLMClient = None):
        """
        添加文档到向量库（生成 embedding 用于语义搜索）

        Args:
            doc: 文档对象
            embedding: 忽略（保留参数兼容）
            llm_client: LLM 客户端，用于生成 embedding
        """
        # 检查内存上限，必要时淘汰
        if len(self.documents) >= self.max_documents:
            self._evict_oldest()

        # 存储文档
        self.documents[doc.id] = doc
        self.documents.move_to_end(doc.id)  # 更新 LRU 顺序

        # 生成并存储 embedding（如果提供 llm_client）
        if llm_client:
            try:
                # 使用标题+内容生成 embedding
                text = f"{doc.title} {doc.content}"[:8000]
                emb = await llm_client.get_embedding(text)
                self.embeddings[doc.id] = np.array(emb, dtype=np.float32)
                self.embeddings.move_to_end(doc.id)  # 更新 LRU 顺序
                await self._save_embedding(doc.id, self.embeddings[doc.id])
            except Exception as e:
                logger.warning(f"Failed to generate embedding for {doc.id}: {e}")

        # 保存文档到磁盘
        await self._save_doc(doc)

    async def add_documents_batch(self, docs: List[Doc], llm_client: LLMClient = None):
        """
        批量添加文档（生成 embedding 用于语义搜索）

        Args:
            docs: 文档列表
            llm_client: LLM 客户端，用于生成 embedding
        """
        if not docs:
            return

        logger.info(f"Adding {len(docs)} documents in batch (semantic search mode)")

        # 添加文档（会生成 embedding）
        for doc in docs:
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
        """搜索 - 使用 embedding 语义搜索"""
        if not self.documents:
            return []

        # 如果有 embedding 客户端，使用语义搜索
        if llm_client and self.embeddings:
            try:
                # 获取查询向量
                query_vec = await llm_client.get_embedding(query)
                query_vec = np.array(query_vec, dtype=np.float32)

                # 计算相似度
                results = []
                for doc_id, doc_vec in self.embeddings.items():
                    doc = self.documents[doc_id]

                    # 行业过滤
                    if filter_sector and doc.sector != filter_sector:
                        continue

                    # 计算相似度
                    similarity = self._compute_similarity(query_vec, doc_vec)

                    if similarity >= min_similarity:
                        results.append((similarity, doc))

                # 排序并返回top_k
                results.sort(key=lambda x: x[0], reverse=True)
                logger.debug(f"Semantic search: query='{query}', results={len(results)}")
                return [doc for _, doc in results[:top_k]]
            except Exception as e:
                logger.warning(f"Semantic search failed: {e}, falling back to keyword")

        # 后备：关键词搜索
        logger.debug(f"Using keyword search for: {query}")
        return await self._keyword_search(query, top_k, filter_sector)

    async def _keyword_search(
        self,
        query: str,
        top_k: int,
        filter_sector: str = None,
    ) -> List[Doc]:
        """关键词搜索（后备方案）"""
        query_words = query.lower().split()

        scored = []
        for doc in self.documents.values():
            # 行业过滤
            if filter_sector and doc.sector != filter_sector:
                continue

            # 简单的词匹配评分
            text = (doc.title + ' ' + doc.content).lower()
            score = sum(1 for word in query_words if word in text)

            if score > 0:
                scored.append((score / len(query_words), doc))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [doc for _, doc in scored[:top_k]]

    def _compute_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """计算向量相似度"""
        if self.metric == "cosine":
            # 余弦相似度
            norm_a = np.linalg.norm(a)
            norm_b = np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0:
                return 0.0
            return np.dot(a, b) / (norm_a * norm_b)
        elif self.metric == "euclidean":
            # 欧氏距离转相似度
            dist = np.linalg.norm(a - b)
            return 1.0 / (1.0 + dist)
        elif self.metric == "dot_product":
            # 点积（需要归一化才能比较）
            return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
        else:
            # 默认余弦相似度
            norm_a = np.linalg.norm(a)
            norm_b = np.linalg.norm(b)
            if norm_a == 0 or norm_b == 0:
                return 0.0
            return np.dot(a, b) / (norm_a * norm_b)

    async def _build_index(self):
        """构建FAISS索引"""
        if not self.embeddings:
            return

        try:
            import faiss

            # 准备数据
            ids = list(self.embeddings.keys())
            vectors = np.array([self.embeddings[id_] for id_ in ids], dtype=np.float32)

            # 创建索引
            if self.index_type == "IVF":
                quantizer = faiss.IndexFlatIP(self.dimension)
                self._index = faiss.IndexIVFFlat(
                    quantizer, self.dimension, self.nlist, faiss.METRIC_INNER_PRODUCT
                )
                self._index.train(vectors)
            elif self.index_type == "Flat":
                self._index = faiss.IndexFlatIP(self.dimension)
            elif self.index_type == "HNSW":
                self._index = faiss.IndexHNSWFlat(self.dimension, 32)
            else:
                logger.warning(f"Unknown index type {self.index_type}, using Flat")
                self._index = faiss.IndexFlatIP(self.dimension)

            # 添加向量
            self._index.add(vectors)

            self._index_ready = True
            logger.info(f"Index built: {len(ids)} vectors")

        except ImportError:
            logger.warning("FAISS not installed, using brute force search")
            self._index_ready = False
        except Exception as e:
            logger.error(f"Failed to build index: {e}")
            self._index_ready = False

    async def _update_single_index(self, doc_id: str, vector: np.ndarray):
        """更新单个向量到索引"""
        if not self._index_ready or self._index is None:
            return

        try:
            import faiss
            self._index.add(np.array([vector], dtype=np.float32))
        except Exception as e:
            logger.error(f"Failed to update index: {e}")

    # =========================================================================
    # 持久化
    # =========================================================================

    async def _save_doc(self, doc: Doc):
        """保存文档到磁盘"""
        filepath = self.storage_path / "documents" / f"{doc.id}.json"
        save_json(doc.to_dict(), str(filepath))

    async def _save_embedding(self, doc_id: str, embedding: np.ndarray):
        """保存向量到磁盘"""
        filepath = self.storage_path / "embeddings" / f"{doc_id}.npy"
        np.save(str(filepath), embedding)

    async def _load_from_disk(self):
        """从磁盘加载数据"""
        # 加载文档
        doc_dir = self.storage_path / "documents"
        if doc_dir.exists():
            for file in doc_dir.glob("*.json"):
                try:
                    data = load_json(str(file))
                    if data:
                        doc = Doc.from_dict(data)
                        self.documents[doc.id] = doc
                except Exception as e:
                    logger.warning(f"Failed to load doc {file}: {e}")

        # 加载向量
        emb_dir = self.storage_path / "embeddings"
        if emb_dir.exists():
            for file in emb_dir.glob("*.npy"):
                try:
                    doc_id = file.stem
                    embedding = np.load(str(file))
                    if doc_id in self.documents:
                        self.embeddings[doc_id] = embedding
                except Exception as e:
                    logger.warning(f"Failed to load embedding {file}: {e}")

        logger.info(f"Loaded {len(self.documents)} docs from disk")

    async def save(self):
        """保存整个数据库"""
        # 保存索引
        if self._index is not None:
            try:
                import faiss
                index_path = self.storage_path / "index.faiss"
                faiss.write_index(self._index, str(index_path))
            except Exception as e:
                logger.warning(f"Failed to save index: {e}")

        logger.info(f"DB saved: {len(self.documents)} docs")

    async def clear(self):
        """清空数据库"""
        # 停止自动持久化任务
        if self._persist_task and not self._persist_task.done():
            self._persist_task.cancel()
            self._persist_task = None

        self.documents.clear()
        self.embeddings.clear()
        self._index = None
        self._index_ready = False

        # 清空存储目录
        for path in (self.storage_path / "documents", self.storage_path / "embeddings"):
            if path.exists():
                shutil.rmtree(path)
            ensure_dir(path)

        logger.info("Database cleared")

    async def close(self):
        """关闭数据库，保存并清理资源"""
        # 停止自动持久化
        if self._persist_task and not self._persist_task.done():
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass

        # 保存当前状态
        await self.save()

        logger.info("Vector database closed")

    # =========================================================================
    # 统计
    # =========================================================================

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
        """获取行业分布"""
        distribution = {}
        for doc in self.documents.values():
            sector = doc.sector or "未知"
            distribution[sector] = distribution.get(sector, 0) + 1
        return distribution

    def __len__(self):
        return len(self.documents)

    def __contains__(self, doc_id):
        return doc_id in self.documents