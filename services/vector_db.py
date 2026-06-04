"""
Compatibility adapter for the old VectorDatabase API.

The implementation now delegates to the Obsidian-compatible WikiKnowledgeBase.
The class name is kept so existing research flows do not need a broad rewrite.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core import DATA_DIR, Document
from .wiki_knowledge import WikiKnowledgeBase


class VectorDatabase:
    """VectorDatabase-compatible facade backed by an Obsidian Markdown wiki."""

    def __init__(
        self,
        dimension: int = 1024,
        index_type: str = "Flat",
        nlist: int = 100,
        metric: str = "cosine",
        storage_path: str = None,
        max_documents: int = None,
        wiki_root: str = None,
        embedding_enabled: Optional[bool] = None,
        database_service: Any = None,
    ):
        self.dimension = dimension
        self.index_type = "obsidian_wiki"
        self.legacy_index_type = index_type
        self.nlist = nlist
        self.metric = metric
        self.storage_path = Path(storage_path) if storage_path else DATA_DIR / "vector_db"
        self.max_documents = max_documents
        self.knowledge = WikiKnowledgeBase(
            root=wiki_root,
            embedding_enabled=embedding_enabled,
            database_service=database_service,
        )
        self.documents = self.knowledge.documents
        self.embeddings: Dict[str, Any] = {}
        self._db = None
        self._index_ready = False

    async def initialize(self, llm_client: Any = None):
        await self.knowledge.initialize(llm_client)
        self.documents = self.knowledge.documents
        self._db = self.knowledge
        self._index_ready = True

    async def has_document(self, doc_id: str = None, url: str = None) -> bool:
        return await self.knowledge.has_document(doc_id=doc_id, url=url)

    async def add_document(self, doc: Document, embedding: List[float] = None, llm_client: Any = None):
        await self.knowledge.add_document(doc, embedding=embedding, llm_client=llm_client)
        self.documents = self.knowledge.documents

    async def add_documents_batch(self, docs: List[Document], llm_client: Any = None):
        await self.knowledge.add_documents_batch(docs, llm_client=llm_client)
        self.documents = self.knowledge.documents

    async def search(
        self,
        query: str,
        top_k: int = 10,
        filter_sector: str = None,
        min_similarity: float = 0.0,
        llm_client: Any = None,
    ) -> List[Document]:
        return await self.knowledge.search(
            query=query,
            top_k=top_k,
            filter_sector=filter_sector,
            min_similarity=min_similarity,
            llm_client=llm_client,
        )

    async def save(self):
        return None

    async def clear(self):
        await self.knowledge.clear()
        self.documents = self.knowledge.documents

    async def close(self):
        await self.knowledge.close()
        self._db = None
        self._index_ready = False

    def set_database_service(self, database_service: Any) -> None:
        self.knowledge.set_database_service(database_service)

    def get_stats(self) -> Dict[str, Any]:
        stats = self.knowledge.get_stats()
        stats.update(
            {
                "dimension": self.dimension,
                "index_type": self.index_type,
                "legacy_index_type": self.legacy_index_type,
                "index_ready": self._index_ready,
                "storage_path": str(self.storage_path),
                "indexed_vectors": 0,
                "total_documents": len(self),
            }
        )
        return stats

    def __len__(self):
        return len(self.knowledge)

    def __contains__(self, doc_id):
        return doc_id in self.documents
