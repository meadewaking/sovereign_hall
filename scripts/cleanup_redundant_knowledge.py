#!/usr/bin/env python3
"""Compact duplicated Sovereign Hall knowledge stores.

The database keeps external research documents as the source of truth. The
Obsidian wiki keeps readable/source-linked projections of those documents. This
script removes old feedback loops and duplicate rows, then rebuilds derived wiki
indexes from the remaining source pages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "sovereign_hall.db"
KNOWLEDGE_DIR = DATA_DIR / "knowledge"
VECTOR_STORE_PATH = DATA_DIR / "vector_db" / "vector_store.db"

MIN_DB_CONTENT_CHARS = 80
MIN_WIKI_SOURCE_BODY_CHARS = 240
NOISY_TITLE_RE = re.compile(
    r"(?:^|\b)(403|404|operations too frequent|google search|microsoft bing|search -|"
    r"youtube|tiktok|doubao\.com|chrome web store)(?:\b|$)",
    re.IGNORECASE,
)
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {"from", "spm", "fbclid", "gclid", "yclid"}


def compact_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def content_hash(value: object) -> str:
    return hashlib.sha256(compact_text(value).encode("utf-8")).hexdigest()


def normalize_url(value: object) -> str:
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


def slugify(value: str, fallback: str = "untitled") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"https?://", "", text)
    text = re.sub(r"[\\/:*?\"<>|#^[\]]+", "-", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-. ")
    return text[:80] or fallback


def wiki_link(title: str) -> str:
    return f"[[{str(title or '').replace('[', '').replace(']', '').strip()}]]"


def dump_markdown(frontmatter: dict, body: str) -> str:
    rendered = yaml.safe_dump(
        frontmatter,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).strip()
    return f"---\n{rendered}\n---\n\n{body.strip()}\n"


def parse_frontmatter(text: str) -> tuple[dict, str]:
    if not text.startswith("---"):
        return {}, text
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", text, flags=re.S)
    if not match:
        return {}, text
    try:
        frontmatter = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError:
        frontmatter = {}
    if not isinstance(frontmatter, dict):
        frontmatter = {}
    return frontmatter, match.group(2)


def as_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, (tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def source_page_raw_rel(body: str) -> str:
    match = re.search(r"\[\[(raw/[^|\]]+)", body)
    return match.group(1) if match else ""


def source_page_topic(body: str, frontmatter: dict) -> str:
    match = re.search(r"主题:\s*\[\[([^\]|]+)", body)
    if match:
        return match.group(1).strip()
    sector = str(frontmatter.get("sector") or "").strip()
    return f"{sector}研究" if sector else "综合研究"


def source_page_entities(body: str, frontmatter: dict, topic: str) -> list[str]:
    match = re.search(r"实体:\s*(.+)", body)
    if match:
        entities = re.findall(r"\[\[([^\]|]+)", match.group(1))
        return [entity.strip() for entity in entities if entity.strip() and entity.strip() != topic]
    return [item for item in as_list(frontmatter.get("related")) if item != topic]


def created_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_backups(backup_dir: Path, dry_run: bool) -> dict:
    backups = {
        "dir": str(backup_dir),
        "db": str(backup_dir / DB_PATH.name),
        "knowledge_archive": str(backup_dir / "knowledge.tar.gz"),
        "vector_store": str(backup_dir / "vector_store.db"),
    }
    if dry_run:
        return backups
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DB_PATH, backup_dir / DB_PATH.name)
    if KNOWLEDGE_DIR.exists():
        shutil.make_archive(
            str(backup_dir / "knowledge"),
            "gztar",
            root_dir=KNOWLEDGE_DIR.parent,
            base_dir=KNOWLEDGE_DIR.name,
        )
    if VECTOR_STORE_PATH.exists():
        shutil.move(str(VECTOR_STORE_PATH), backup_dir / "vector_store.db")
    return backups


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def fetch_document_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return list(
        conn.execute(
            """
            SELECT rowid, id, title, content, url, source, sector, keywords,
                   publish_time, embedding, crawled_at, created_at
            FROM documents
            """
        )
    )


def document_bad_reason(row: sqlite3.Row) -> str:
    source = str(row["source"] or "").strip()
    doc_id = str(row["id"] or "").strip()
    title = compact_text(row["title"])
    body = compact_text(row["content"])
    if source == "obsidian_wiki" or doc_id.startswith("wiki:"):
        return "wiki_feedback"
    if len(body) < MIN_DB_CONTENT_CHARS:
        return "too_short"
    if len(title) < 4 or NOISY_TITLE_RE.search(title):
        return "noisy_title"
    if len(set(body)) < 8:
        return "low_entropy"
    return ""


def choose_best_row(rows: list[sqlite3.Row]) -> sqlite3.Row:
    return max(
        rows,
        key=lambda row: (
            len(compact_text(row["content"])),
            1 if normalize_url(row["url"]) else 0,
            str(row["created_at"] or ""),
            str(row["id"] or ""),
        ),
    )


def delete_rowids(conn: sqlite3.Connection, rowids: set[int]) -> None:
    ids = list(rowids)
    for idx in range(0, len(ids), 500):
        chunk = ids[idx : idx + 500]
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(f"DELETE FROM documents WHERE rowid IN ({placeholders})", chunk)


def cleanup_database(dry_run: bool) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    before = table_count(conn, "documents")
    conn.execute("ALTER TABLE documents ADD COLUMN content_hash TEXT") if "content_hash" not in {
        row[1] for row in conn.execute("PRAGMA table_info(documents)")
    } else None

    rows = fetch_document_rows(conn)
    stats = {
        "documents_before": before,
        "rows_removed_total": 0,
        "quality_rows_removed": 0,
        "url_duplicate_rows": 0,
        "content_duplicate_rows": 0,
    }
    delete_ids: set[int] = set()
    bad_reasons: dict[str, int] = defaultdict(int)

    for row in rows:
        reason = document_bad_reason(row)
        if reason:
            delete_ids.add(int(row["rowid"]))
            bad_reasons[reason] += 1

    active_rows = [row for row in rows if int(row["rowid"]) not in delete_ids]

    by_url: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in active_rows:
        url = normalize_url(row["url"])
        if url:
            by_url[url].append(row)
    for group in by_url.values():
        if len(group) > 1:
            keep = int(choose_best_row(group)["rowid"])
            dupes = {int(row["rowid"]) for row in group if int(row["rowid"]) != keep}
            delete_ids.update(dupes)
            stats["url_duplicate_rows"] += len(dupes)

    active_rows = [row for row in active_rows if int(row["rowid"]) not in delete_ids]

    by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in active_rows:
        body_hash = content_hash(row["content"])
        if body_hash:
            by_hash[body_hash].append(row)
    for group in by_hash.values():
        if len(group) > 1:
            keep = int(choose_best_row(group)["rowid"])
            dupes = {int(row["rowid"]) for row in group if int(row["rowid"]) != keep}
            delete_ids.update(dupes)
            stats["content_duplicate_rows"] += len(dupes)

    stats["rows_removed_total"] = len(delete_ids)
    stats["quality_rows_removed"] = sum(bad_reasons.values())
    stats["bad_reasons"] = dict(sorted(bad_reasons.items()))

    if dry_run:
        stats["documents_after"] = before - len(delete_ids)
        conn.close()
        return stats

    for row in rows:
        if int(row["rowid"]) in delete_ids:
            continue
        conn.execute(
            "UPDATE documents SET url = ?, content_hash = ? WHERE rowid = ?",
            (normalize_url(row["url"]), content_hash(row["content"]), int(row["rowid"])),
        )
    delete_rowids(conn, delete_ids)
    conn.execute("DROP INDEX IF EXISTS ux_documents_url")
    conn.execute("DROP INDEX IF EXISTS ux_documents_content_hash")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_url "
        "ON documents(url) WHERE url IS NOT NULL AND url <> ''"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_content_hash "
        "ON documents(content_hash) WHERE content_hash IS NOT NULL AND content_hash <> ''"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_content_hash ON documents(content_hash)")
    conn.commit()
    conn.execute("VACUUM")
    conn.execute("ANALYZE")
    stats["documents_after"] = table_count(conn, "documents")
    conn.close()
    return stats


def load_remaining_db_identity() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = list(conn.execute("SELECT id, url, content, content_hash FROM documents"))
    conn.close()
    return {
        "ids": {str(row["id"] or "") for row in rows if row["id"]},
        "urls": {normalize_url(row["url"]) for row in rows if normalize_url(row["url"])},
        "hashes": {str(row["content_hash"] or content_hash(row["content"])) for row in rows},
    }


def raw_content_hash(raw_path: Path) -> str:
    if not raw_path.exists():
        return ""
    _, body = parse_frontmatter(raw_path.read_text(encoding="utf-8", errors="ignore"))
    body = re.sub(r"^# .*?\n\n", "", body, count=1, flags=re.S)
    return content_hash(body)


def cleanup_wiki(dry_run: bool) -> dict:
    wiki_dir = KNOWLEDGE_DIR / "wiki"
    sources_dir = wiki_dir / "sources"
    raw_dir = KNOWLEDGE_DIR / "raw"
    topics_dir = wiki_dir / "topics"
    entities_dir = wiki_dir / "entities"
    cache_path = KNOWLEDGE_DIR / ".state" / "ingest-cache.json"
    index_path = KNOWLEDGE_DIR / "index.md"
    log_path = KNOWLEDGE_DIR / "log.md"

    identities = load_remaining_db_identity()
    source_paths = sorted(sources_dir.glob("*.md"))
    delete_sources: set[Path] = set()
    delete_raws: set[Path] = set()
    page_data = []
    delete_reasons: dict[str, int] = defaultdict(int)

    for path in source_paths:
        text = path.read_text(encoding="utf-8", errors="ignore")
        frontmatter, body = parse_frontmatter(text)
        title = compact_text(frontmatter.get("title") or path.stem)
        url = normalize_url(frontmatter.get("url"))
        source_id = str(frontmatter.get("source_id") or "")
        raw_rel = source_page_raw_rel(body)
        raw_path = KNOWLEDGE_DIR / raw_rel if raw_rel else None
        raw_hash = raw_content_hash(raw_path) if raw_path else ""
        body_len = len(compact_text(body))
        db_match = source_id in identities["ids"] or url in identities["urls"] or raw_hash in identities["hashes"]
        reason = ""
        if NOISY_TITLE_RE.search(title):
            reason = "noisy_title"
        elif body_len < MIN_WIKI_SOURCE_BODY_CHARS:
            reason = "too_short"
        elif not db_match:
            reason = "orphan"
        if reason:
            delete_sources.add(path)
            if raw_path:
                delete_raws.add(raw_path)
            delete_reasons[reason] += 1
        else:
            page_data.append((path, frontmatter, body, title, url, raw_hash))

    by_identity: dict[str, list[tuple]] = defaultdict(list)
    for item in page_data:
        _, _, _, title, url, raw_hash = item
        key = url or raw_hash or title
        by_identity[key].append(item)
    for group in by_identity.values():
        if len(group) <= 1:
            continue
        keep = max(group, key=lambda item: len(compact_text(item[2])))
        for item in group:
            if item is keep:
                continue
            delete_sources.add(item[0])
            raw_rel = source_page_raw_rel(item[2])
            if raw_rel:
                delete_raws.add(KNOWLEDGE_DIR / raw_rel)
            delete_reasons["duplicate"] += 1

    stats = {
        "wiki_sources_before": len(source_paths),
        "wiki_sources_removed": len(delete_sources),
        "wiki_raw_removed": len(delete_raws),
        "wiki_delete_reasons": dict(sorted(delete_reasons.items())),
    }
    if dry_run:
        stats["wiki_sources_after"] = len(source_paths) - len(delete_sources)
        return stats

    for path in delete_sources:
        path.unlink(missing_ok=True)
    for path in delete_raws:
        path.unlink(missing_ok=True)

    remaining = []
    for path in sorted(sources_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        frontmatter, body = parse_frontmatter(text)
        title = compact_text(frontmatter.get("title") or path.stem)
        topic = source_page_topic(body, frontmatter)
        entities = source_page_entities(body, frontmatter, topic)
        remaining.append((path, frontmatter, body, title, topic, entities))

    shutil.rmtree(topics_dir, ignore_errors=True)
    shutil.rmtree(entities_dir, ignore_errors=True)
    topics_dir.mkdir(parents=True, exist_ok=True)
    entities_dir.mkdir(parents=True, exist_ok=True)

    topics: dict[str, list[str]] = defaultdict(list)
    entities: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"sources": set(), "topics": set()})
    for _, _, _, title, topic, names in remaining:
        topics[topic].append(title)
        for entity in names:
            entities[entity]["sources"].add(title)
            entities[entity]["topics"].add(topic)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for topic, titles in topics.items():
        unique_titles = sorted(set(titles))
        body = "# {0}\n\n## 相关来源\n\n{1}\n".format(
            topic,
            "\n".join(f"- {wiki_link(title)}" for title in unique_titles),
        )
        (topics_dir / f"{slugify(topic, 'topic')}.md").write_text(
            dump_markdown(
                {
                    "type": "topic",
                    "title": topic,
                    "created": today,
                    "updated": today,
                    "sources": unique_titles,
                    "tags": ["topic"],
                    "related": [],
                },
                body,
            ),
            encoding="utf-8",
        )

    for entity, refs in entities.items():
        source_titles = sorted(refs["sources"])
        topic_titles = sorted(refs["topics"])
        body = "# {0}\n\n- 相关主题: {1}\n\n## 证据来源\n\n{2}\n".format(
            entity,
            ", ".join(wiki_link(topic) for topic in topic_titles) if topic_titles else "无",
            "\n".join(f"- {wiki_link(title)}" for title in source_titles),
        )
        (entities_dir / f"{slugify(entity, 'entity')}.md").write_text(
            dump_markdown(
                {
                    "type": "entity",
                    "title": entity,
                    "created": today,
                    "updated": today,
                    "sources": source_titles,
                    "tags": ["entity"],
                    "related": topic_titles,
                },
                body,
            ),
            encoding="utf-8",
        )

    if cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
        entries = cache.get("entries", {})
        kept_entries = {}
        for key, entry in entries.items():
            files = [KNOWLEDGE_DIR / rel for rel in entry.get("files", [])]
            if files and all(path.exists() for path in files):
                kept_entries[key] = entry
        cache["entries"] = kept_entries
        cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    referenced_raws = {
        KNOWLEDGE_DIR / source_page_raw_rel(parse_frontmatter(path.read_text(encoding="utf-8", errors="ignore"))[1])
        for path in sources_dir.glob("*.md")
        if source_page_raw_rel(parse_frontmatter(path.read_text(encoding="utf-8", errors="ignore"))[1])
    }
    unreferenced_raws = [path for path in raw_dir.glob("*.md") if path not in referenced_raws]
    for path in unreferenced_raws:
        path.unlink(missing_ok=True)
    stats["wiki_unreferenced_raw_removed"] = len(unreferenced_raws)

    topic_links = sorted(wiki_link(path.stem) for path in topics_dir.glob("*.md"))
    entity_links = sorted(wiki_link(path.stem) for path in entities_dir.glob("*.md"))
    source_links = []
    for path in sorted(sources_dir.glob("*.md")):
        frontmatter, _ = parse_frontmatter(path.read_text(encoding="utf-8", errors="ignore"))
        source_links.append(wiki_link(str(frontmatter.get("title") or path.stem)))
    index_path.write_text(
        "\n".join(
            [
                "# Sovereign Hall Knowledge Wiki",
                "",
                "## Topics",
                *[f"- {link}" for link in topic_links],
                "",
                "## Entities",
                *[f"- {link}" for link in entity_links],
                "",
                "## Sources",
                *[f"- {link}" for link in source_links],
                "",
            ]
        ),
        encoding="utf-8",
    )
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"## [{datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')}] cleanup\n\n"
            f"- removed_sources: {len(delete_sources)}\n"
            f"- remaining_sources: {len(list(sources_dir.glob('*.md')))}\n\n"
        )

    stats["wiki_sources_after"] = len(list(sources_dir.glob("*.md")))
    stats["wiki_topics_after"] = len(list(topics_dir.glob("*.md")))
    stats["wiki_entities_after"] = len(list(entities_dir.glob("*.md")))
    return stats


def collect_final_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    duplicate_url = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT url FROM documents
            WHERE COALESCE(url, '') <> ''
            GROUP BY url HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    duplicate_hash = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT content_hash FROM documents
            WHERE COALESCE(content_hash, '') <> ''
            GROUP BY content_hash HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    stats = {
        "documents": table_count(conn, "documents"),
        "duplicate_url_groups": int(duplicate_url),
        "duplicate_content_hash_groups": int(duplicate_hash),
        "wiki_feedback_rows": int(
            conn.execute("SELECT COUNT(*) FROM documents WHERE source='obsidian_wiki'").fetchone()[0]
        ),
        "wiki_sources": len(list((KNOWLEDGE_DIR / "wiki" / "sources").glob("*.md"))),
        "wiki_raw": len(list((KNOWLEDGE_DIR / "raw").glob("*.md"))),
        "wiki_topics": len(list((KNOWLEDGE_DIR / "wiki" / "topics").glob("*.md"))),
        "wiki_entities": len(list((KNOWLEDGE_DIR / "wiki" / "entities").glob("*.md"))),
        "legacy_vector_store_exists": VECTOR_STORE_PATH.exists(),
    }
    conn.close()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report changes without editing files")
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=DATA_DIR / "cleanup_backups" / created_stamp(),
        help="directory for pre-cleanup backups",
    )
    args = parser.parse_args()

    result = {"dry_run": args.dry_run}
    result["backups"] = create_backups(args.backup_dir, args.dry_run)
    result["database"] = cleanup_database(args.dry_run)
    result["wiki"] = cleanup_wiki(args.dry_run)
    if not args.dry_run:
        result["final"] = collect_final_stats()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
