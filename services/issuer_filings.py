"""Opt-in primary filing acquisition; no quotes, account, or execution access.

CNINFO's issuer disclosure index selects the latest half-year summary. Each
complete PDF page becomes an attributable Document. No keyword snippet is
treated as a verified financial fact, and an announcement is not a market price.
The caller owns raw artifact persistence and the information cutoff.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import io
import re
from zoneinfo import ZoneInfo

from ..core import Document

VERSION = "issuer_filings_v1"
INDEX_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
PDF_ORIGIN = "https://static.cninfo.com.cn/"
ZONE = ZoneInfo("Asia/Shanghai")
MAX_BYTES = 8_000_000
MAX_PAGES = 30


def index_request(ticker: str, *, cutoff: datetime, org_id: str = "") -> tuple[str, dict]:
    if not re.fullmatch(r"(?:0|3|6)\d{5}", ticker):
        raise ValueError("explicit A-share issuer required")
    if cutoff.tzinfo is None:
        raise ValueError("timezone required")
    if org_id and not re.fullmatch(r"[a-zA-Z0-9]{1,40}", org_id):
        raise ValueError("invalid issuer organization ID")
    day = cutoff.astimezone(ZONE).date()
    return INDEX_URL, dict(stock=f"{ticker},{org_id}" if org_id else ticker, tabName="fulltext", pageSize="30", pageNum="1",
                          column="sse" if ticker.startswith("6") else "szse",
                          category="category_bndbg_szsh;", searchkey="", secid="",
                          seDate=f"{day.year - 1}-01-01~{day}",
                          sortName="time", sortType="desc", isHLtitle="false")


def select_summary(payload: dict, *, ticker: str, captured_at: datetime,
                   cutoff: datetime) -> dict:
    index_request(ticker, cutoff=cutoff)
    if captured_at.tzinfo is None or captured_at > cutoff:
        raise ValueError("index capture unavailable at cutoff")
    rows = payload.get("announcements")
    if not isinstance(rows, list):
        raise ValueError("invalid disclosure index")
    # An incomplete response cannot establish that the selected filing is latest.
    total = payload.get("totalAnnouncement")
    if type(total) is not int or total != len(rows):
        raise ValueError("incomplete disclosure index; pagination required")
    eligible = []
    for row in rows:
        if not isinstance(row, dict) or row.get("secCode") != ticker:
            raise ValueError("wrong issuer in disclosure index")
        title = re.sub(r"\s+", "", str(row.get("announcementTitle", "")))
        match = re.fullmatch(r"(20\d{2})年半年度报告摘要", title)
        if not match:
            continue
        stamp = row.get("announcementTime")
        if type(stamp) not in (int, float):
            raise ValueError("missing announcement timestamp")
        published = datetime.fromtimestamp(stamp / 1000, ZONE).date()
        period = datetime(int(match[1]), 6, 30).date()
        if not period <= published < cutoff.astimezone(ZONE).date():
            continue
        adjunct = str(row.get("adjunctUrl", ""))
        path = re.fullmatch(r"finalpage/(\d{4}-\d{2}-\d{2})/(\d+)\.[Pp][Dd][Ff]", adjunct)
        if (not path or path[1] != str(published) or
                path[2] != str(row.get("announcementId")) or row.get("adjunctType") != "PDF"):
            raise ValueError("invalid filing path/date/identity")
        eligible.append({"ticker": ticker, "issuer_name": row["secName"],
                         "announcement_id": path[2], "title": title,
                         "report_period": str(period), "published_at": str(published),
                         "index_captured_at": captured_at.isoformat(),
                         "url": PDF_ORIGIN + adjunct})
    if not eligible:
        raise ValueError("no eligible half-year summary")
    period = max(r["report_period"] for r in eligible)
    latest = [r for r in eligible if r["report_period"] == period]
    if len(latest) != 1:
        raise ValueError("ambiguous latest filing; review amendments")
    return latest[0]


def extract_pages(pdf: bytes) -> list[str]:
    if not pdf.startswith(b"%PDF-") or len(pdf) > MAX_BYTES:
        raise ValueError("invalid or oversized PDF")
    # Optional dependency is imported only for this opt-in architecture.
    import pdfplumber
    with pdfplumber.open(io.BytesIO(pdf)) as doc:
        if not 1 <= len(doc.pages) <= MAX_PAGES:
            raise ValueError("summary page budget exceeded")
        # Keep rows together; a page is retained whole, including units/footnotes.
        pages = [page.extract_text(x_tolerance=2, y_tolerance=3) or "" for page in doc.pages]
    if any(len(p.strip()) < 30 for p in pages):
        raise ValueError("unreadable/scanned PDF; OCR verification required")
    return pages


def build_filing_documents(pdf: bytes, *, filing: dict, captured_at: datetime,
                           cutoff: datetime) -> tuple[list[Document], dict]:
    if cutoff.tzinfo is None or captured_at.tzinfo is None or captured_at > cutoff:
        raise ValueError("PDF capture unavailable at cutoff")
    index_capture = datetime.fromisoformat(filing["index_captured_at"])
    if index_capture.tzinfo is None or index_capture > captured_at:
        raise ValueError("invalid index/PDF capture order")
    # Repeat source checks at the artifact boundary; never fetch caller URLs.
    url = filing["url"]
    if not re.fullmatch(re.escape(PDF_ORIGIN) + r"finalpage/\d{4}-\d{2}-\d{2}/\d+\.[Pp][Dd][Ff]", url):
        raise ValueError("untrusted PDF origin")
    if not filing["report_period"] <= filing["published_at"] < str(cutoff.astimezone(ZONE).date()):
        raise ValueError("unavailable filing date")
    pages = extract_pages(pdf)
    normalized = re.sub(r"\s+", "", "\n".join(pages))
    if not all(str(filing[k]) in normalized for k in ("ticker", "issuer_name", "title")):
        raise ValueError("PDF issuer/title does not match index")
    digest = hashlib.sha256(pdf).hexdigest()
    docs = []
    for page, text in enumerate(pages, 1):
        body = (f"证券代码={filing['ticker']}；发行人={filing['issuer_name']}；"
                f"报告期={filing['report_period']}；PDF物理页={page}/{len(pages)}。\n"
                "以下是发行人披露原文；数据口径以表头、单位及注释为准。"
                "公告不能证明当前估值、已派息或可执行报价。\n" + text)
        docs.append(Document(doc_id=f"{VERSION}_{filing['ticker']}_{digest[:16]}_p{page}",
                             title=f"{filing['ticker']} {filing['title']} PDF第{page}页",
                             content=body, url=url + f"#page={page}", source="cninfo_issuer_filing",
                             metadata={"publish_time": filing["published_at"],
                                       "publish_time_source": "structured_disclosure_notice",
                                       "crawled_at": captured_at.isoformat(),
                                       "source_kind": "issuer_filing", "filing_version": VERSION,
                                       "pdf_sha256": digest, "pdf_page": page,
                                       "report_period": filing["report_period"],
                                       "announcement_id": filing["announcement_id"],
                                       "shared_source_group_id": f"cninfo:{filing['announcement_id']}"}))
    return docs, {"version": VERSION, "filing": filing, "pdf_sha256": digest,
                  "captured_at": captured_at.isoformat(), "cutoff": cutoff.isoformat(),
                  "pages": len(pages), "independent_sources": 1,
                  "historical_pit": False, "verified_numeric_facts": False}


def bundle_filing_documents(pages: list[Document]) -> Document:
    """Persist one canonical URL once, preserving every complete page in order.

    The normal database intentionally strips URL fragments for deduplication.
    Storing pages individually would silently retain only the longest page.
    The caller must provision enough model context for this complete bundle.
    """
    if not pages:
        raise ValueError("no filing pages")
    first = pages[0]
    expected = list(range(1, len(pages) + 1))
    if [d.metadata.get('pdf_page') for d in pages] != expected:
        raise ValueError("missing or unordered filing pages")
    for doc in pages:
        for key in ('pdf_sha256', 'announcement_id', 'shared_source_group_id',
                    'report_period', 'publish_time', 'crawled_at'):
            if doc.metadata.get(key) != first.metadata.get(key):
                raise ValueError("mixed filing lineage")
        if doc.url.split('#')[0] != first.url.split('#')[0]:
            raise ValueError("mixed filing URLs")
    content = '\n\n'.join(d.content for d in pages)
    metadata = dict(first.metadata)
    metadata.pop('pdf_page', None)
    metadata.update(pdf_pages=expected, page_content_sha256=[hashlib.sha256(d.content.encode()).hexdigest() for d in pages],
                    title=first.title.rsplit(' PDF第',1)[0]+' 完整摘要',
                    url=first.url.split('#')[0])
    return Document(doc_id=first.id.rsplit('_p',1)[0], content=content,
                    metadata=metadata, source=first.source)
