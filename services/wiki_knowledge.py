"""
Obsidian-compatible LLM Wiki knowledge base.

This module replaces the old raw-document vector RAG path with a persistent
Markdown wiki. The public surface is intentionally close to VectorDatabase so
existing research flows can keep consuming Document objects.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from ..core import DATA_DIR, PROJECT_ROOT, Document
from ..core.config import get_config

logger = logging.getLogger(__name__)


DEFAULT_CHUNK_TARGET = 1000
DEFAULT_CHUNK_MAX = 1500
DEFAULT_CHUNK_MIN = 200
DEFAULT_CHUNK_OVERLAP = 200
RRF_K = 60.0
TITLE_WEIGHT = 5.0
BODY_WEIGHT = 1.0
MAX_SEARCH_PAGES = 10000
MIN_WIKI_SOURCE_CHARS = 80
NOISY_SOURCE_TITLE_RE = re.compile(
    r"(?:^|\b)(403|404|operations too frequent|google search|microsoft bing|search -|"
    r"youtube|tiktok|doubao\.com|chrome web store)(?:\b|$)",
    re.IGNORECASE,
)
IGNORED_ENTITY_TOKENS = {
    "PDF",
    "URL",
    "HTTP",
    "HTTPS",
    "WWW",
    "ISSN",
    "ISBN",
    "DOI",
    "JEL",
    "NOTE",
    "FIGURE",
    "TABLE",
    "APPENDIX",
    "I",
    "II",
    "III",
    "IV",
    "V",
    "N/A",
    "NA",
}


@dataclass
class MarkdownChunk:
    index: int
    text: str
    heading_path: str
    char_start: int
    char_end: int
    oversized: bool = False


@dataclass
class WikiPage:
    path: Path
    rel_path: str
    title: str
    page_type: str
    body: str
    frontmatter: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchHit:
    page: WikiPage
    score: float
    snippet: str
    title_match: bool = False
    vector_score: Optional[float] = None


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_path(path: Path | str) -> str:
    return str(path).replace("\\", "/")


def stable_hash(value: str, length: int = 12) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def is_wiki_generated_document(doc: Document) -> bool:
    metadata = getattr(doc, "metadata", {}) or {}
    doc_id = str(getattr(doc, "id", "") or getattr(doc, "doc_id", "") or "")
    source = str(getattr(doc, "source", "") or "")
    return source == "obsidian_wiki" or doc_id.startswith("wiki:") or bool(metadata.get("wiki_path"))


def is_ingestable_source_document(doc: Document) -> bool:
    if is_wiki_generated_document(doc):
        return False
    title = compact_text(getattr(doc, "title", ""))
    content = compact_text(getattr(doc, "content", ""))
    if len(content) < MIN_WIKI_SOURCE_CHARS:
        return False
    if len(title) < 4 or NOISY_SOURCE_TITLE_RE.search(title):
        return False
    if len(set(content)) < 8:
        return False
    return True


def slugify(value: str, fallback: str = "untitled") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"https?://", "", text)
    text = re.sub(r"[\\/:*?\"<>|#^[\]]+", "-", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-. ")
    return text[:80] or fallback


def wiki_link(title: str, label: Optional[str] = None) -> str:
    safe_title = str(title or "").replace("[", "").replace("]", "").strip()
    if label and label != safe_title:
        safe_label = str(label).replace("[", "").replace("]", "").strip()
        return f"[[{safe_title}|{safe_label}]]"
    return f"[[{safe_title}]]"


def parse_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    if not content.startswith("---"):
        return {}, content
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", content, flags=re.S)
    if not match:
        return {}, content
    try:
        frontmatter = yaml.safe_load(match.group(1)) or {}
        if not isinstance(frontmatter, dict):
            frontmatter = {}
    except yaml.YAMLError:
        frontmatter = {}
    return frontmatter, match.group(2)


def dump_markdown(frontmatter: Dict[str, Any], body: str) -> str:
    rendered = yaml.safe_dump(
        frontmatter,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).strip()
    return f"---\n{rendered}\n---\n\n{body.strip()}\n"


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple) or isinstance(value, set):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def merge_unique(*values: Iterable[str]) -> List[str]:
    seen = set()
    merged = []
    for group in values:
        for item in group:
            text = str(item).strip()
            if text and text not in seen:
                seen.add(text)
                merged.append(text)
    return merged


def merge_markdown_page(
    existing_content: Optional[str],
    incoming_content: str,
    locked_fields: Sequence[str] = ("type", "title", "created"),
    union_fields: Sequence[str] = ("sources", "tags", "related"),
) -> str:
    """Deterministically merge two frontmatter-bearing wiki pages."""
    if not existing_content:
        return incoming_content
    if existing_content == incoming_content:
        return existing_content

    old_fm, old_body = parse_frontmatter(existing_content)
    new_fm, new_body = parse_frontmatter(incoming_content)
    final_fm = dict(new_fm)

    for field_name in locked_fields:
        if old_fm.get(field_name):
            final_fm[field_name] = old_fm[field_name]

    for field_name in union_fields:
        final_fm[field_name] = merge_unique(
            _as_list(old_fm.get(field_name)),
            _as_list(new_fm.get(field_name)),
        )

    final_fm["updated"] = utc_date()

    if old_body.strip() == new_body.strip():
        body = new_body
    else:
        body = "\n\n".join(
            part.strip()
            for part in [old_body, "## 更新", new_body]
            if part and part.strip()
        )

    return dump_markdown(final_fm, body)


def strip_frontmatter(content: str) -> Tuple[str, int]:
    if not content.startswith("---"):
        return content, 0
    match = re.match(r"^---\s*\n.*?\n---\s*\n?", content, flags=re.S)
    if not match:
        return content, 0
    return content[match.end() :], match.end()


def chunk_markdown(
    content: str,
    target_chars: int = DEFAULT_CHUNK_TARGET,
    max_chars: int = DEFAULT_CHUNK_MAX,
    min_chars: int = DEFAULT_CHUNK_MIN,
    overlap_chars: int = DEFAULT_CHUNK_OVERLAP,
) -> List[MarkdownChunk]:
    """Markdown-aware chunker with heading breadcrumbs and safe atomic blocks."""
    if max_chars < target_chars:
        max_chars = target_chars
    if overlap_chars >= target_chars:
        overlap_chars = target_chars // 2

    body, offset = strip_frontmatter(content)
    if not body.strip():
        return []

    sections = _split_sections(body, offset)
    chunks: List[MarkdownChunk] = []
    for heading_path, text, start in sections:
        for piece, piece_start, oversized in _split_atomic(text, start, max_chars):
            if len(piece) <= target_chars or oversized:
                chunks.append(
                    MarkdownChunk(
                        index=len(chunks),
                        text=piece.strip(),
                        heading_path=heading_path,
                        char_start=piece_start,
                        char_end=piece_start + len(piece),
                        oversized=oversized,
                    )
                )
                continue
            cursor = 0
            while cursor < len(piece):
                end = min(len(piece), cursor + target_chars)
                if end < len(piece):
                    split_at = max(
                        piece.rfind("\n\n", cursor, end),
                        piece.rfind("。", cursor, end),
                        piece.rfind(". ", cursor, end),
                        piece.rfind("；", cursor, end),
                        piece.rfind("; ", cursor, end),
                    )
                    if split_at > cursor + min_chars:
                        end = split_at + 1
                text_slice = piece[cursor:end].strip()
                if text_slice:
                    chunks.append(
                        MarkdownChunk(
                            index=len(chunks),
                            text=text_slice,
                            heading_path=heading_path,
                            char_start=piece_start + cursor,
                            char_end=piece_start + end,
                            oversized=False,
                        )
                    )
                if end >= len(piece):
                    break
                cursor = max(end - overlap_chars, cursor + 1)

    return _merge_tiny_chunks(chunks, min_chars)


def _split_sections(body: str, offset: int) -> List[Tuple[str, str, int]]:
    lines = body.splitlines(keepends=True)
    sections: List[Tuple[str, str, int]] = []
    headings: Dict[int, str] = {}
    current_lines: List[str] = []
    current_start = offset
    current_heading = ""
    cursor = offset
    in_fence = False
    fence_marker = ""

    def flush() -> None:
        if "".join(current_lines).strip():
            sections.append((current_heading, "".join(current_lines), current_start))

    for line in lines:
        fence_match = re.match(r"^(`{3,}|~{3,})", line)
        if fence_match:
            marker = fence_match.group(1)
            if not in_fence:
                in_fence = True
                fence_marker = marker[0] * len(marker)
            elif line.strip() == fence_marker:
                in_fence = False
            current_lines.append(line)
            cursor += len(line)
            continue

        heading_match = None if in_fence else re.match(r"^(#{1,6})\s+(.+?)\s*$", line.strip())
        if heading_match:
            flush()
            level = len(heading_match.group(1))
            title = heading_match.group(2).strip()
            headings[level] = title
            for deeper in range(level + 1, 7):
                headings.pop(deeper, None)
            current_heading = " > ".join(
                f"{'#' * level_idx} {headings[level_idx]}"
                for level_idx in range(1, 7)
                if level_idx in headings
            )
            current_lines = [line]
            current_start = cursor
        else:
            current_lines.append(line)
        cursor += len(line)

    flush()
    return sections


def _split_atomic(text: str, start: int, max_chars: int) -> List[Tuple[str, int, bool]]:
    """Split by paragraphs while keeping fenced code blocks and tables intact."""
    pieces: List[Tuple[str, int, bool]] = []
    lines = text.splitlines(keepends=True)
    current: List[str] = []
    current_start = start
    cursor = start
    in_fence = False
    fence_marker = ""
    in_table = False

    def flush() -> None:
        nonlocal current, current_start
        if current:
            block = "".join(current)
            pieces.append((block, current_start, len(block) > max_chars))
            current = []

    for line in lines:
        stripped = line.strip()
        fence_match = re.match(r"^(`{3,}|~{3,})", stripped)
        table_line = stripped.startswith("|")
        starts_atomic = bool(fence_match) or table_line
        blank = stripped == ""

        if fence_match:
            if not in_fence:
                flush()
                current_start = cursor
                in_fence = True
                fence_marker = fence_match.group(1)[0] * len(fence_match.group(1))
            elif stripped == fence_marker:
                current.append(line)
                cursor += len(line)
                in_fence = False
                flush()
                current_start = cursor
                continue

        if not in_fence and table_line and not in_table:
            flush()
            current_start = cursor
            in_table = True
        elif in_table and not table_line:
            flush()
            in_table = False
            current_start = cursor

        if not current:
            current_start = cursor
        current.append(line)
        cursor += len(line)

        if not in_fence and not in_table and blank:
            flush()
            current_start = cursor
        elif not in_fence and not in_table and len("".join(current)) >= max_chars:
            flush()
            current_start = cursor

    flush()
    return pieces


def _merge_tiny_chunks(chunks: List[MarkdownChunk], min_chars: int) -> List[MarkdownChunk]:
    if not chunks:
        return []
    merged: List[MarkdownChunk] = []
    for chunk in chunks:
        if (
            merged
            and len(chunk.text) < min_chars
            and merged[-1].heading_path == chunk.heading_path
            and not merged[-1].oversized
        ):
            previous = merged[-1]
            previous.text = f"{previous.text}\n\n{chunk.text}".strip()
            previous.char_end = chunk.char_end
            previous.oversized = previous.oversized or chunk.oversized
        else:
            merged.append(chunk)
    for index, chunk in enumerate(merged):
        chunk.index = index
    return merged


def tokenize_query(query: str) -> List[str]:
    stop_words = {
        "的",
        "是",
        "了",
        "什么",
        "在",
        "有",
        "和",
        "与",
        "the",
        "is",
        "a",
        "an",
        "to",
        "of",
        "for",
    }
    raw_tokens = [
        token.lower()
        for token in re.split(r"[\s,，。！？、；：\"'（）()\-_/\\·~～…]+", query or "")
        if len(token.strip()) > 1 and token.lower() not in stop_words
    ]
    tokens: List[str] = []
    for token in raw_tokens:
        if re.search(r"[\u4e00-\u9fff]", token) and len(token) > 2:
            chars = list(token)
            tokens.extend(chars[i] + chars[i + 1] for i in range(len(chars) - 1))
            tokens.extend(ch for ch in chars if ch not in stop_words)
        tokens.append(token)
    return list(dict.fromkeys(tokens))


class WikiStore:
    def __init__(self, root: Path):
        self.root = root
        self.raw_dir = root / "raw"
        self.wiki_dir = root / "wiki"
        self.topics_dir = self.wiki_dir / "topics"
        self.entities_dir = self.wiki_dir / "entities"
        self.sources_dir = self.wiki_dir / "sources"
        self.media_dir = root / "media"
        self.state_dir = root / ".state"
        self.obsidian_dir = root / ".obsidian"
        self.cache_path = self.state_dir / "ingest-cache.json"
        self.index_path = root / "index.md"
        self.log_path = root / "log.md"

    def ensure_vault(self) -> None:
        for directory in [
            self.raw_dir,
            self.topics_dir,
            self.entities_dir,
            self.sources_dir,
            self.media_dir,
            self.state_dir,
            self.obsidian_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

        if not self.index_path.exists():
            self.index_path.write_text(
                "# Sovereign Hall Knowledge Wiki\n\n"
                "## Topics\n\n"
                "## Entities\n\n"
                "## Sources\n",
                encoding="utf-8",
            )
        if not self.log_path.exists():
            self.log_path.write_text("# Knowledge Log\n\n", encoding="utf-8")

        graph_config = self.obsidian_dir / "graph.json"
        if not graph_config.exists():
            graph_config.write_text(
                json.dumps(
                    {
                        "collapse-filter": False,
                        "search": "",
                        "showTags": True,
                        "showAttachments": False,
                        "hideUnresolved": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

    def load_cache(self) -> Dict[str, Any]:
        if not self.cache_path.exists():
            return {"entries": {}}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {"entries": {}}
        except json.JSONDecodeError:
            return {"entries": {}}

    def save_cache(self, cache: Dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    def append_log(self, action: str, message: str) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"## [{utc_timestamp()}] {action}\n\n{message.strip()}\n\n")

    def all_wiki_pages(self) -> List[WikiPage]:
        pages: List[WikiPage] = []
        for path in sorted(self.wiki_dir.rglob("*.md")):
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            frontmatter, body = parse_frontmatter(content)
            pages.append(
                WikiPage(
                    path=path,
                    rel_path=normalize_path(path.relative_to(self.root)),
                    title=str(frontmatter.get("title") or path.stem),
                    page_type=str(frontmatter.get("type") or "page"),
                    body=body,
                    frontmatter=frontmatter,
                )
            )
        return pages

    def write_page(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = path.read_text(encoding="utf-8") if path.exists() else None
        path.write_text(merge_markdown_page(existing, content), encoding="utf-8")

    def rebuild_index(self) -> None:
        topics = self._index_links(self.topics_dir)
        entities = self._index_links(self.entities_dir)
        sources = self._index_links(self.sources_dir)
        content = [
            "# Sovereign Hall Knowledge Wiki",
            "",
            "## Topics",
            *(f"- {link}" for link in topics),
            "",
            "## Entities",
            *(f"- {link}" for link in entities),
            "",
            "## Sources",
            *(f"- {link}" for link in sources),
            "",
        ]
        self.index_path.write_text("\n".join(content), encoding="utf-8")

    def _index_links(self, directory: Path) -> List[str]:
        links = []
        for path in sorted(directory.glob("*.md")):
            frontmatter, _ = parse_frontmatter(path.read_text(encoding="utf-8"))
            title = str(frontmatter.get("title") or path.stem)
            links.append(wiki_link(title))
        return links


class WikiIngestor:
    def __init__(self, store: WikiStore):
        self.store = store

    def ingest_document(self, doc: Document) -> List[str]:
        content_hash = stable_hash(doc.content or "", 24)
        source_identity = doc.url or doc.id or content_hash
        cache_key = stable_hash(source_identity)
        cache = self.store.load_cache()
        entry = cache.get("entries", {}).get(cache_key)
        if entry and entry.get("content_hash") == content_hash:
            paths = entry.get("files", [])
            if all((self.store.root / path).exists() for path in paths):
                return paths

        source_title = doc.title or doc.url or doc.id or "Untitled Source"
        source_slug = f"{stable_hash(source_identity, 8)}-{slugify(source_title, 'source')}"
        raw_path = self.store.raw_dir / f"{source_slug}.md"
        source_path = self.store.sources_dir / f"{source_slug}.md"
        topic_title = self._topic_title(doc)
        topic_path = self.store.topics_dir / f"{slugify(topic_title, 'topic')}.md"
        entities = self._extract_entities(doc)

        source_link = wiki_link(source_title)
        topic_link = wiki_link(topic_title)
        entity_links = [wiki_link(entity) for entity in entities]

        raw_body = self._raw_body(doc, source_title)
        raw_path.write_text(raw_body, encoding="utf-8")

        source_page = self._source_page(
            doc=doc,
            title=source_title,
            source_slug=source_slug,
            topic_title=topic_title,
            entities=entities,
            raw_rel=normalize_path(raw_path.relative_to(self.store.root)),
        )
        self.store.write_page(source_path, source_page)

        topic_page = self._topic_page(topic_title, source_title, entities, doc)
        self.store.write_page(topic_path, topic_page)

        written = [
            normalize_path(raw_path.relative_to(self.store.root)),
            normalize_path(source_path.relative_to(self.store.root)),
            normalize_path(topic_path.relative_to(self.store.root)),
        ]

        for entity in entities:
            entity_path = self.store.entities_dir / f"{slugify(entity, 'entity')}.md"
            self.store.write_page(entity_path, self._entity_page(entity, topic_title, source_title, doc))
            written.append(normalize_path(entity_path.relative_to(self.store.root)))

        cache.setdefault("entries", {})[cache_key] = {
            "source_id": doc.id,
            "url": doc.url,
            "content_hash": content_hash,
            "timestamp": utc_timestamp(),
            "files": written,
        }
        self.store.save_cache(cache)
        self.store.append_log(
            "ingest",
            f"- source: {source_link}\n- topic: {topic_link}\n- entities: {', '.join(entity_links) if entity_links else 'none'}",
        )
        return written

    def _topic_title(self, doc: Document) -> str:
        if doc.sector:
            return f"{doc.sector}研究"
        for keyword in doc.keywords:
            if keyword:
                return f"{keyword}研究"
        return "综合研究"

    def _extract_entities(self, doc: Document) -> List[str]:
        entities = []
        for ticker in re.findall(r"\b(?:[036]\d{5}|[A-Z]{2,6})(?:\.(?:SH|SZ|HK|US))?\b", f"{doc.title} {doc.content}"):
            if ticker.upper() in IGNORED_ENTITY_TOKENS:
                continue
            entities.append(ticker.upper())
        for keyword in doc.keywords[:6]:
            if len(str(keyword).strip()) >= 2:
                entities.append(str(keyword).strip())
        if doc.sector:
            entities.append(doc.sector)
        return merge_unique(entities)[:10]

    def _raw_body(self, doc: Document, title: str) -> str:
        frontmatter = {
            "type": "raw_source",
            "title": title,
            "created": utc_date(),
            "source_id": doc.id,
            "url": doc.url,
            "sector": doc.sector,
            "tags": ["raw-source"],
        }
        body = f"# {title}\n\n{doc.content or ''}\n"
        return dump_markdown(frontmatter, body)

    def _source_page(
        self,
        doc: Document,
        title: str,
        source_slug: str,
        topic_title: str,
        entities: List[str],
        raw_rel: str,
    ) -> str:
        frontmatter = {
            "type": "source",
            "title": title,
            "created": utc_date(),
            "updated": utc_date(),
            "source_id": doc.id,
            "url": doc.url,
            "sector": doc.sector,
            "sources": [doc.url or doc.id],
            "tags": merge_unique(["source", doc.sector], doc.keywords),
            "related": [topic_title, *entities],
        }
        excerpt = (doc.content or "").strip()
        if len(excerpt) > 3000:
            excerpt = excerpt[:3000] + "..."
        body = (
            f"# {title}\n\n"
            f"- 主题: {wiki_link(topic_title)}\n"
            f"- 实体: {', '.join(wiki_link(entity) for entity in entities) if entities else '无'}\n"
            f"- 原文: [[{raw_rel}|raw snapshot]]\n\n"
            "## 摘要\n\n"
            f"{excerpt}\n"
        )
        return dump_markdown(frontmatter, body)

    def _topic_page(self, topic_title: str, source_title: str, entities: List[str], doc: Document) -> str:
        frontmatter = {
            "type": "topic",
            "title": topic_title,
            "created": utc_date(),
            "updated": utc_date(),
            "sources": [source_title],
            "tags": merge_unique(["topic", doc.sector], doc.keywords),
            "related": entities,
        }
        body = (
            f"# {topic_title}\n\n"
            "## 相关来源\n\n"
            f"- {wiki_link(source_title)}\n\n"
            "## 相关实体\n\n"
            + "\n".join(f"- {wiki_link(entity)}" for entity in entities)
            + "\n\n## 累积判断\n\n"
            f"- 新增资料显示：{(doc.content or '')[:500]}\n"
        )
        return dump_markdown(frontmatter, body)

    def _entity_page(self, entity: str, topic_title: str, source_title: str, doc: Document) -> str:
        frontmatter = {
            "type": "entity",
            "title": entity,
            "created": utc_date(),
            "updated": utc_date(),
            "sources": [source_title],
            "tags": merge_unique(["entity", doc.sector], doc.keywords),
            "related": [topic_title],
        }
        body = (
            f"# {entity}\n\n"
            f"- 相关主题: {wiki_link(topic_title)}\n"
            f"- 相关来源: {wiki_link(source_title)}\n\n"
            "## 证据记录\n\n"
            f"- {(doc.content or '')[:500]}\n"
        )
        return dump_markdown(frontmatter, body)


class WikiSearchIndex:
    def __init__(self, store: WikiStore, llm_client: Any = None, embedding_enabled: bool = True):
        self.store = store
        self.llm_client = llm_client
        self.embedding_enabled = embedding_enabled
        self._page_embeddings: Dict[str, List[float]] = {}

    async def search(
        self,
        query: str,
        top_k: int = 10,
        filter_sector: Optional[str] = None,
        min_similarity: float = 0.0,
    ) -> List[SearchHit]:
        pages = self.store.all_wiki_pages()[:MAX_SEARCH_PAGES]
        if filter_sector:
            pages = [
                page
                for page in pages
                if str(page.frontmatter.get("sector") or "") == filter_sector
                or filter_sector in _as_list(page.frontmatter.get("tags"))
                or filter_sector in _as_list(page.frontmatter.get("related"))
            ]
        if not pages:
            return []

        keyword_hits = self._keyword_search(query, pages)
        vector_hits: List[Tuple[str, float]] = []
        if self.embedding_enabled and self.llm_client:
            vector_hits = await self._vector_search(query, pages)

        hits = self._merge_hits(query, pages, keyword_hits, vector_hits)
        hits = [hit for hit in hits if hit.score >= min_similarity]
        return hits[:top_k]

    def _keyword_search(self, query: str, pages: List[WikiPage]) -> List[Tuple[str, float]]:
        tokens = tokenize_query(query)
        phrase = (query or "").lower().strip()
        scored: List[Tuple[str, float]] = []
        for page in pages:
            hay_title = page.title.lower()
            hay_body = page.body.lower()
            score = 0.0
            if phrase and phrase in hay_title:
                score += 100.0
            if phrase and phrase in hay_body:
                score += min(hay_body.count(phrase), 10) * 10.0
            for token in tokens:
                score += hay_title.count(token) * TITLE_WEIGHT
                score += hay_body.count(token) * BODY_WEIGHT
            if score > 0:
                scored.append((page.rel_path, score))
        scored.sort(key=lambda item: item[1], reverse=True)
        return scored

    async def _vector_search(self, query: str, pages: List[WikiPage]) -> List[Tuple[str, float]]:
        t0 = datetime.now()
        try:
            query_vec = await asyncio.wait_for(self.llm_client.get_embedding(query), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("wiki embedding query timeout (30s)")
            return []
        except Exception as exc:
            logger.warning("wiki embedding query failed: %s", exc)
            return []

        # 限制并发 embedding 请求数，避免连接池耗尽
        sem = asyncio.Semaphore(16)

        async def _safe_page_vec(page: WikiPage) -> Optional[List[float]]:
            async with sem:
                try:
                    return await asyncio.wait_for(self._page_embedding(page), timeout=30)
                except asyncio.TimeoutError:
                    logger.debug("wiki page embedding timeout: %s", page.rel_path)
                    return None
                except Exception as exc:
                    logger.debug("wiki page embedding failed for %s: %s", page.rel_path, exc)
                    return None

        page_vecs = await asyncio.gather(*[_safe_page_vec(p) for p in pages])

        results: List[Tuple[str, float]] = []
        for page, page_vec in zip(pages, page_vecs):
            if page_vec is None:
                continue
            score = cosine_similarity(query_vec, page_vec)
            if score > 0:
                results.append((page.rel_path, score))
        results.sort(key=lambda item: item[1], reverse=True)
        logger.debug(f"wiki vector_search done in {(datetime.now()-t0).total_seconds():.1f}s, {len(results)} hits")
        return results

    async def _page_embedding(self, page: WikiPage) -> List[float]:
        key = f"{page.rel_path}:{stable_hash(page.body, 16)}"
        if key in self._page_embeddings:
            return self._page_embeddings[key]
        text = f"{page.title}\n{page.body[:8000]}"
        vector = await self.llm_client.get_embedding(text)
        self._page_embeddings[key] = vector
        return vector

    def _merge_hits(
        self,
        query: str,
        pages: List[WikiPage],
        keyword_hits: List[Tuple[str, float]],
        vector_hits: List[Tuple[str, float]],
    ) -> List[SearchHit]:
        page_by_rel = {page.rel_path: page for page in pages}
        scores: Dict[str, float] = {}
        vector_scores = dict(vector_hits)
        keyword_rank = {rel: idx + 1 for idx, (rel, _) in enumerate(keyword_hits)}
        vector_rank = {rel: idx + 1 for idx, (rel, _) in enumerate(vector_hits)}
        for rel, rank in keyword_rank.items():
            scores[rel] = scores.get(rel, 0.0) + 1.0 / (RRF_K + rank)
        for rel, rank in vector_rank.items():
            scores[rel] = scores.get(rel, 0.0) + 1.0 / (RRF_K + rank)
        if not scores and query:
            for rel, score in keyword_hits:
                scores[rel] = score

        hits: List[SearchHit] = []
        phrase = (query or "").lower().strip()
        for rel, score in sorted(scores.items(), key=lambda item: item[1], reverse=True):
            page = page_by_rel.get(rel)
            if not page:
                continue
            title_match = bool(phrase and phrase in page.title.lower())
            hits.append(
                SearchHit(
                    page=page,
                    score=score,
                    snippet=make_snippet(page.body, query),
                    title_match=title_match,
                    vector_score=vector_scores.get(rel),
                )
            )
        return hits


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b:
        return 0.0
    length = min(len(a), len(b))
    dot = sum(float(a[i]) * float(b[i]) for i in range(length))
    norm_a = math.sqrt(sum(float(a[i]) ** 2 for i in range(length)))
    norm_b = math.sqrt(sum(float(b[i]) ** 2 for i in range(length)))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def make_snippet(body: str, query: str, context: int = 160) -> str:
    clean = re.sub(r"\s+", " ", body or "").strip()
    if not clean:
        return ""
    tokens = tokenize_query(query)
    lower = clean.lower()
    positions = [lower.find(token) for token in tokens if lower.find(token) >= 0]
    start = max(0, min(positions) - context // 2) if positions else 0
    end = min(len(clean), start + context)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(clean) else ""
    return f"{prefix}{clean[start:end]}{suffix}"


class WikiKnowledgeBase:
    """Persistent Obsidian wiki with VectorDatabase-compatible methods."""

    def __init__(
        self,
        root: str | Path | None = None,
        embedding_enabled: Optional[bool] = None,
        database_service: Any = None,
        lazy_migration_batch_size: Optional[int] = None,
        min_wiki_hits: Optional[int] = None,
    ):
        cfg = get_config().get("knowledge_wiki", {})
        configured_root = cfg.get("root") or str(DATA_DIR / "knowledge")
        self.root = Path(root or configured_root)
        if not self.root.is_absolute():
            self.root = PROJECT_ROOT / self.root
        self.embedding_enabled = bool(
            cfg.get("embedding_enabled", True) if embedding_enabled is None else embedding_enabled
        )
        self.lazy_migration_batch_size = int(
            lazy_migration_batch_size
            if lazy_migration_batch_size is not None
            else cfg.get("lazy_migration_batch_size", 10)
        )
        self.min_wiki_hits = int(min_wiki_hits if min_wiki_hits is not None else cfg.get("min_wiki_hits", 5))
        self.database_service = database_service
        self.store = WikiStore(self.root)
        self.ingestor = WikiIngestor(self.store)
        self.search_index: Optional[WikiSearchIndex] = None
        self.llm_client = None
        self.documents: Dict[str, Document] = {}
        self._initialized = False

    async def initialize(self, llm_client: Any = None) -> None:
        self.llm_client = llm_client
        self.store.ensure_vault()
        self.search_index = WikiSearchIndex(
            self.store,
            llm_client=llm_client,
            embedding_enabled=self.embedding_enabled,
        )
        self._refresh_documents()
        self._initialized = True

    async def add_document(self, doc: Document | Dict[str, Any], embedding: List[float] = None, llm_client: Any = None) -> bool:
        await self._ensure_initialized(llm_client)
        if isinstance(doc, dict):
            doc = Document.from_dict(doc)
        if not is_ingestable_source_document(doc):
            logger.debug("Skipped wiki ingest for generated or low-quality document: %s", doc.title or doc.id)
            return False
        self.ingestor.ingest_document(doc)
        self.documents[doc.id] = doc
        self.store.rebuild_index()
        return True

    async def add_documents_batch(self, docs: List[Document | Dict[str, Any]], llm_client: Any = None) -> int:
        await self._ensure_initialized(llm_client)
        added = 0
        for doc in docs or []:
            if isinstance(doc, dict):
                doc = Document.from_dict(doc)
            if not is_ingestable_source_document(doc):
                logger.debug("Skipped wiki ingest for generated or low-quality document: %s", doc.title or doc.id)
                continue
            self.ingestor.ingest_document(doc)
            self.documents[doc.id] = doc
            added += 1
        if added:
            self.store.rebuild_index()
        return added

    async def has_document(self, doc_id: str = None, url: str = None) -> bool:
        await self._ensure_initialized()
        cache = self.store.load_cache().get("entries", {})
        for entry in cache.values():
            if doc_id and entry.get("source_id") == doc_id:
                return True
            if url and entry.get("url") == url:
                return True
        return False

    async def search(
        self,
        query: str,
        top_k: int = 10,
        filter_sector: str = None,
        min_similarity: float = 0.0,
        llm_client: Any = None,
    ) -> List[Document]:
        await self._ensure_initialized(llm_client)
        assert self.search_index is not None
        hits = await self.search_index.search(query, top_k=top_k, filter_sector=filter_sector, min_similarity=min_similarity)
        if len(hits) < min(self.min_wiki_hits, top_k):
            migrated = await self._lazy_migrate(query, filter_sector=filter_sector)
            if migrated:
                hits = await self.search_index.search(query, top_k=top_k, filter_sector=filter_sector, min_similarity=min_similarity)
        self.store.append_log("search", f"- query: {query}\n- hits: {len(hits)}")
        return [self._hit_to_document(hit) for hit in hits[:top_k]]

    async def clear(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        self.documents.clear()
        self._initialized = False
        await self.initialize(self.llm_client)

    async def close(self) -> None:
        self._initialized = False

    def get_stats(self) -> Dict[str, Any]:
        pages = self.store.all_wiki_pages() if self.store.wiki_dir.exists() else []
        chunks = sum(len(chunk_markdown(page.body)) for page in pages)
        return {
            "provider": "obsidian_wiki",
            "root": str(self.root),
            "raw_sources": len(list(self.store.raw_dir.glob("*.md"))) if self.store.raw_dir.exists() else 0,
            "wiki_pages": len(pages),
            "indexed_chunks": chunks,
            "embedding_enabled": self.embedding_enabled,
            "topics": len(list(self.store.topics_dir.glob("*.md"))) if self.store.topics_dir.exists() else 0,
            "entities": len(list(self.store.entities_dir.glob("*.md"))) if self.store.entities_dir.exists() else 0,
            "sources": len(list(self.store.sources_dir.glob("*.md"))) if self.store.sources_dir.exists() else 0,
        }

    def set_database_service(self, database_service: Any) -> None:
        self.database_service = database_service

    async def _ensure_initialized(self, llm_client: Any = None) -> None:
        if not self._initialized:
            await self.initialize(llm_client or self.llm_client)
        elif llm_client and llm_client is not self.llm_client:
            self.llm_client = llm_client
            self.search_index = WikiSearchIndex(
                self.store,
                llm_client=llm_client,
                embedding_enabled=self.embedding_enabled,
            )

    async def _lazy_migrate(self, query: str, filter_sector: str = None) -> int:
        db = await self._resolve_database_service()
        if not db or not hasattr(db, "search_documents"):
            return 0
        try:
            rows = await db.search_documents(query=query, sector=filter_sector, limit=self.lazy_migration_batch_size)
        except Exception as exc:
            logger.debug("lazy wiki migration failed to query sqlite documents: %s", exc)
            return 0

        migrated = 0
        for row in rows:
            doc = Document.from_dict(dict(row))
            if await self.has_document(doc_id=doc.id, url=doc.url):
                continue
            if await self.add_document(doc, llm_client=self.llm_client):
                migrated += 1
        if migrated:
            self.store.append_log("lazy-migrate", f"- query: {query}\n- migrated: {migrated}")
        return migrated

    async def _resolve_database_service(self) -> Any:
        if self.database_service:
            return self.database_service
        try:
            from .database import DatabaseService

            self.database_service = await DatabaseService.get_instance()
            return self.database_service
        except Exception:
            return None

    def _hit_to_document(self, hit: SearchHit) -> Document:
        page = hit.page
        metadata = {
            "title": page.title,
            "url": page.frontmatter.get("url", ""),
            "sector": page.frontmatter.get("sector", ""),
            "keywords": _as_list(page.frontmatter.get("tags")),
            "wiki_path": page.rel_path,
            "wiki_type": page.page_type,
            "source_ids": _as_list(page.frontmatter.get("sources")),
            "search_score": hit.score,
            "vector_score": hit.vector_score,
            "title_match": hit.title_match,
            "snippet": hit.snippet,
        }
        return Document(
            content=page.body,
            metadata=metadata,
            source="obsidian_wiki",
            doc_id=f"wiki:{page.rel_path}",
        )

    def _refresh_documents(self) -> None:
        self.documents = {
            page.rel_path: Document(
                content=page.body,
                metadata={
                    "title": page.title,
                    "sector": page.frontmatter.get("sector", ""),
                    "keywords": _as_list(page.frontmatter.get("tags")),
                    "wiki_path": page.rel_path,
                },
                source="obsidian_wiki",
                doc_id=f"wiki:{page.rel_path}",
            )
            for page in self.store.all_wiki_pages()
        }

    def __len__(self) -> int:
        return self.get_stats().get("raw_sources", 0)
