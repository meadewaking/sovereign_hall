"""Source-reported publication time is distinct from local retrieval time.

No network I/O. Missing/legacy provenance stays unknown; dates inside an
article's business narrative are never guessed to be its publication date.
"""
from datetime import datetime, timezone
import hashlib
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup


def _date(value: str) -> str | None:
    value = str(value or "").strip()
    value = re.sub(r"(\d{4})年(\d{1,2})月(\d{1,2})日", r"\1-\2-\3", value)
    match = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
    try:
        if match:
            return datetime(*map(int, match.groups())).date().isoformat()
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except (ValueError, TypeError):
        for format_string in ("%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(value, format_string).date().isoformat()
            except ValueError:
                continue
        return None


def source_time_metadata(*, snippet: str = "", html: str = "", url: str = "",
                         now=None) -> dict:
    observed = now or datetime.now().astimezone()
    metadata = {"crawled_at": observed.isoformat(), "publish_time_source": "unknown"}
    candidates = []
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select('meta[property="article:published_time"], '
                                'meta[itemprop="datePublished"], '
                                'time[itemprop="datePublished"]'):
            candidates.append((node.get("content") or node.get("datetime") or
                               node.get_text(strip=True), "page_publication_metadata"))
        # These article templates expose publication fields outside standard
        # meta tags. Scope both host/path and article header: search forms,
        # related-story dates and dates in the business narrative are excluded.
        try:
            parsed_url = urlsplit(url)
            host, path = (parsed_url.hostname or "").lower(), parsed_url.path
        except ValueError:
            host, path = "", ""
        page_dates = []
        if host in {"www.egsea.com", "egs.stcn.com"} and re.fullmatch(r"/news/detail/\d+\.html", path):
            page_dates = [node.get_text(strip=True) for node in
                          soup.select(".detail-wrapper > .detail-info > span")]
        elif host in {"m.jiemian.com", "www.jiemian.com"} and re.fullmatch(r"/article/\d+\.html", path):
            for node in soup.select(".article-detail > .info[data-article-publish-time]"):
                value = node.get("data-article-publish-time", "")
                if re.fullmatch(r"\d{10}", value):
                    page_dates.append(datetime.fromtimestamp(int(value), timezone.utc).isoformat())
        elif host == "stock.finance.sina.com.cn" and "/vReport_Show/" in path:
            page_dates = [node.get_text(strip=True).removeprefix("日期：") for node in
                          soup.select(".creab > span") if node.get_text(strip=True).startswith("日期：")]
        valid_dates = list(dict.fromkeys(value for raw in page_dates if (value := _date(raw))))
        # Multiple conflicting header dates are ambiguous; do not choose one.
        if len(valid_dates) == 1:
            candidates.append((valid_dates[0], "page_publication_metadata"))
    # Search snippets explicitly prefix a displayed date with a separator.
    # Relative dates are left unknown: provider crawl/update time is ambiguous.
    match = re.match(
        r"^\s*(\d{4}(?:-\d{1,2}-\d{1,2}|年\d{1,2}月\d{1,2}日)"
        r"|[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s*[·•|]", snippet,
    )
    if match:
        candidates.append((match.group(1), "search_snippet_date"))
    # CMS route paths embed the publish date as a path segment (e.g. sina,
    # eastmoney, 10jqka). Strict segment boundaries plus _date's ValueError
    # filter article ids like /article/14721952.html that look like YYYYMMDD
    # but parse to invalid month/day.
    if url:
        # Lookahead/lookbehind so consecutive /YYYYMMDD/ segments don't
        # consume the trailing slash and shadow the next match.
        url_date_patterns = [
            r"(?<=/)(\d{4})-(\d{1,2})-(\d{1,2})(?=/)",
            r"(?<=/)(\d{4})/(\d{1,2})/(\d{1,2})(?=/)",
            r"(?<=/)(\d{4})(\d{2})(\d{2})(?=/)",
            # eastmoney CMS: /a/YYYYMMDD<article-id>.html — the date is the
            # leading 8 digits of a longer numeric segment.
            r"(?<=/a/)(\d{4})(\d{2})(\d{2})\d+(?=\D|$)",
        ]
        for pat in url_date_patterns:
            for m in re.finditer(pat, url):
                y, mo, d = m.groups()
                try:
                    datetime(int(y), int(mo), int(d))
                except ValueError:
                    continue
                candidates.append((f"{y}-{mo}-{d}", "url_path_date"))
                break
    for raw, provenance in candidates:
        parsed = _date(raw)
        if parsed and datetime.fromisoformat(parsed).date() <= observed.date():
            metadata.update(publish_time=parsed, publish_time_source=provenance)
            break
    return metadata


def document_time_audit(doc) -> dict:
    metadata = getattr(doc, "metadata", {}) or {}
    provenance = metadata.get("publish_time_source") or "legacy_unverified"
    reported = getattr(doc, "publish_time", None)
    known = provenance in {"page_publication_metadata", "search_snippet_date",
                           "url_path_date", "structured_disclosure_notice"} and bool(reported)
    return {
        "document_id": getattr(doc, "id", None),
        "url": getattr(doc, "url", ""),
        "publish_time": str(reported) if known else None,
        "publish_time_source": provenance,
        "crawled_at": getattr(doc, "crawled_at", None),
        "publication_time_known": known,
    }


def format_document_time(doc) -> str:
    row = document_time_audit(doc)
    return (f"来源报告日期: {row['publish_time'] or 'N/A'} "
            f"(依据={row['publish_time_source']})；"
            f"抓取时间: {row['crawled_at'] or 'N/A'}；抓取时间不是发布时间")


def build_evidence_context(docs, *, max_docs: int, doc_chars: int, context_chars: int):
    """Budget excerpts before auditing; never truncate a source's URL/time header.

    Input is already evidence-ranked. Excluded sources remain in durable storage,
    but are not reported as seen by the model. Character offsets refer to the
    exact returned context, including separators.
    """
    blocks, audit = [], []
    used = 0
    max_docs, doc_chars, context_chars = map(
        lambda value: max(0, int(value)), (max_docs, doc_chars, context_chars)
    )
    excluded = {"short_content": 0, "document_limit": 0, "context_budget": 0}
    for doc in docs:
        content = str(getattr(doc, "content", "") or "")
        if len(content) <= 50:
            excluded["short_content"] += 1
            continue
        if len(audit) >= max_docs:
            excluded["document_limit"] += 1
            continue
        prefix = f"【{getattr(doc, 'title', '') or ''}】\n{format_document_time(doc)}\n"
        suffix = f"\n来源: {getattr(doc, 'url', '') or ''}"
        separator = "\n\n" if blocks else ""
        available = context_chars - used - len(separator) - len(prefix) - len(suffix)
        excerpt = content[:max(0, min(doc_chars, available))]
        if len(excerpt) <= 50:
            excluded["context_budget"] += 1
            continue
        block = prefix + excerpt + suffix
        start = used + len(separator)
        used = start + len(block)
        blocks.append(block)
        audit.append({**document_time_audit(doc), "context_start": start,
                      "context_end": used, "excerpt_chars": len(excerpt),
                      "excerpt_truncated": len(excerpt) < len(content)})
    context = "\n\n".join(blocks)
    return context, {"documents": audit, "input_document_count": len(docs),
                     "excluded_counts": excluded, "context_chars_used": len(context),
                     "context_sha256": hashlib.sha256(context.encode()).hexdigest()}
