"""Opt-in primary-event agenda, rather than a recurring thematic search loop.

Discovery is a bounded CNINFO index scan, not a complete market universe.
An index row establishes a disclosure identity/date, never an earnings fact or
an investment recommendation. Full originals and a fresh account/quote packet
must reach the unchanged proposal/committee pipeline before any evaluation.
This module has no account writer, order interface, or model-memory fallback.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import math
import re
from zoneinfo import ZoneInfo

from ..core import Document
from .issuer_filings import INDEX_URL, PDF_ORIGIN, extract_pages

VERSION = "primary_event_agenda_v1"
ZONE = ZoneInfo("Asia/Shanghai")


def discovery_request(exchange: str, *, as_of: datetime, lookback_days: int = 14):
    if exchange not in {"szse", "sse"}:
        raise ValueError("explicit exchange required")
    if as_of.tzinfo is None or type(lookback_days) is not int or not 1 <= lookback_days <= 31:
        raise ValueError("timezone and bounded lookback required")
    # A notice index normally reports midnight, not the actual publication
    # instant. Exclude today's notices instead of assuming intraday availability.
    last = as_of.astimezone(ZONE).date() - timedelta(days=1)
    first = last - timedelta(days=lookback_days - 1)
    return INDEX_URL, dict(stock="", tabName="fulltext", pageSize="30", pageNum="1",
        column=exchange, plate="sh" if exchange == "sse" else "sz",
        category="category_yjyg_szsh;", searchkey=f"{last.year}年前三季度业绩预告", secid="",
        seDate=f"{first}~{last}", sortName="time", sortType="desc", isHLtitle="false")


def select_events(payload: dict, *, exchange: str, as_of: datetime,
                  lookback_days: int = 14, limit: int = 2) -> dict:
    """Return the latest distinct issuers in the observed page, including bad news.

    No price, return, positive-growth or model-confidence filter chooses cases.
    Revisions are kept and flagged, never silently replaced by a favourable row.
    Pagination coverage is explicit; no inference about unobserved issuers.
    """
    _, request = discovery_request(exchange, as_of=as_of, lookback_days=lookback_days)
    if type(limit) is not int or not 1 <= limit <= 10:
        raise ValueError("bounded event count required")
    rows, total = payload.get("announcements"), payload.get("totalAnnouncement")
    if not isinstance(rows, list) or type(total) is not int or total < len(rows) or len(rows) > 30:
        raise ValueError("invalid disclosure index")
    first, last = request["seDate"].split("~")
    events = []
    excluded = []
    for row in rows:
        try:
            ticker = row["secCode"]
            if not isinstance(ticker, str) or not re.fullmatch(r"[036]\d{5}", ticker):
                raise ValueError("not_supported_issuer")
            if (ticker.startswith("6")) != (exchange == "sse"):
                raise ValueError("exchange_mismatch")
            stamp = row["announcementTime"]
            if type(stamp) not in (int, float) or not math.isfinite(stamp):
                raise ValueError("missing_publication_time")
            published = datetime.fromtimestamp(stamp / 1000, ZONE).date().isoformat()
            if not first <= published <= last:
                raise ValueError("outside_frozen_discovery_window")
            title = re.sub(r"\s+", "", str(row["announcementTitle"]))
            if (not re.search(str(as_of.astimezone(ZONE).year) + r"年", title)
                    or not re.search(r"(?:前三季度|第三季度|1-9月|三季度).*业绩预告", title)):
                raise ValueError("outside_q3_event_scope")
            path = re.fullmatch(r"finalpage/(\d{4}-\d{2}-\d{2})/(\d+)\.[Pp][Dd][Ff]", str(row["adjunctUrl"]))
            if (not path or path[1] != published or path[2] != str(row["announcementId"])
                    or row.get("adjunctType") != "PDF" or not str(row["secName"]).strip()):
                raise ValueError("invalid_disclosure_identity")
            events.append(dict(ticker=ticker, issuer_name=row["secName"], title=title,
                announcement_id=path[2], published_at=published,
                url=PDF_ORIGIN + row["adjunctUrl"],
                revision=bool(re.search(r"修正|更正|补充", title)), exchange=exchange))
        except (KeyError, ValueError, TypeError, OverflowError, OSError) as exc:
            excluded.append(dict(announcement_id=str(row.get("announcementId", "")) if isinstance(row, dict) else "",
                                 reason=str(exc)))
    events.sort(key=lambda e: (e["published_at"], int(e["announcement_id"])), reverse=True)
    selected, seen = [], set()
    for event in events:
        if event["ticker"] not in seen:
            selected.append(event)
            seen.add(event["ticker"])
        if len(selected) >= limit:
            break
    return dict(version=VERSION, request=request, events=selected, exclusions=excluded,
        observed_rows=len(rows), index_total=total, page_coverage_complete=total == len(rows),
        universe_claim="observed_first_page_only", selection="publication_then_notice_id; no_outcome_filter")


def event_documents(pdf: bytes, *, event: dict, captured_at: datetime,
                    cutoff: datetime) -> tuple[list[Document], dict]:
    """Keep complete original pages, including ranges, losses, caveats and units."""
    if captured_at.tzinfo is None or cutoff.tzinfo is None or captured_at > cutoff:
        raise ValueError("source unavailable at cutoff")
    if not event["published_at"] < cutoff.astimezone(ZONE).date().isoformat():
        raise ValueError("publication instant not established")
    expected = PDF_ORIGIN + f"finalpage/{event['published_at']}/{event['announcement_id']}.PDF"
    if event["url"].lower() != expected.lower():
        raise ValueError("untrusted disclosure URL")
    pages = extract_pages(pdf)
    normalized = re.sub(r"\s+", "", "\n".join(pages))
    if not all(re.sub(r"\s+", "", str(event[k])) in normalized
               for k in ("ticker", "issuer_name", "title")):
        raise ValueError("PDF issuer/title differs from disclosure index")
    digest = hashlib.sha256(pdf).hexdigest()
    documents = []
    for number, content in enumerate(pages, 1):
        documents.append(Document(doc_id=f"{VERSION}_{event['announcement_id']}_{digest[:16]}_p{number}",
            title=f"{event['ticker']} {event['title']} 原始公告第{number}页",
            content=(f"证券代码={event['ticker']}；发行人={event['issuer_name']}；"
                     f"公告日期={event['published_at']}；原始PDF页={number}/{len(pages)}。\n"
                     "以下为业绩预告，属于发行人预测而非审计后业绩、当前估值或投资回报保证。\n" + content),
            source="cninfo_primary_earnings_event", url=event["url"] + f"#page={number}",
            metadata=dict(publish_time=event["published_at"], publish_time_source="structured_disclosure_notice",
                crawled_at=captured_at.isoformat(), source_kind="issuer_filing", pdf_sha256=digest,
                pdf_page=number, announcement_id=event["announcement_id"],
                shared_source_group_id="cninfo:" + event["announcement_id"],
                event_agenda_version=VERSION, forecast_not_audited_actual=True)))
    return documents, dict(version=VERSION, event=event, captured_at=captured_at.isoformat(),
        cutoff=cutoff.isoformat(), pdf_sha256=digest, pages=len(pages), verified_numeric_facts=False,
        publication_identity_checked=True, historical_pit=False, production_promotion=False)
