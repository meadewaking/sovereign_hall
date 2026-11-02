from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from sovereign_hall.services.primary_event_research import discovery_request, select_events, event_documents

NOW = datetime(2026, 10, 15, 14, tzinfo=ZoneInfo("Asia/Shanghai"))


def notice(ticker="300434", aid="12345", day="2026-10-14", **changes):
    row = dict(secCode=ticker, secName="测试股份", announcementTitle="2026年前三季度业绩预告",
        announcementTime=datetime.fromisoformat(day).replace(tzinfo=NOW.tzinfo).timestamp() * 1000,
        adjunctUrl=f"finalpage/{day}/{aid}.PDF", announcementId=aid, adjunctType="PDF")
    row.update(changes)
    return row


def scan(rows, total=None, **kw):
    return select_events(dict(announcements=rows, totalAnnouncement=len(rows) if total is None else total),
                         exchange="szse", as_of=NOW, **kw)


def test_request_is_bounded_and_excludes_same_day():
    url, params = discovery_request("szse", as_of=NOW)
    assert url.startswith("https://www.cninfo.com.cn/")
    assert params["seDate"] == "2026-10-01~2026-10-14"
    assert params["plate"] == "sz" and params["searchkey"] == "2026年前三季度业绩预告"
    assert params["pageSize"] == "30"


@pytest.mark.parametrize("kw", [dict(exchange="bad", as_of=NOW),
    dict(exchange="szse", as_of=NOW.replace(tzinfo=None)),
    dict(exchange="szse", as_of=NOW, lookback_days=32)])
def test_discovery_refuses_unbounded_or_ambiguous_time(kw):
    with pytest.raises(ValueError):
        discovery_request(**kw)


def test_outcome_independent_selection_keeps_loss_and_revision():
    loss = notice("300434", "3", announcementTitle="2026年前三季度业绩预告修正公告")
    profit = notice("301282", "2")
    old_same = notice("300434", "1", day="2026-10-13")
    rows = scan([old_same, profit, loss])["events"]
    assert [r["announcement_id"] for r in rows] == ["3", "2"]
    assert rows[0]["revision"] is True


@pytest.mark.parametrize("row", [notice(day="2026-10-15"), notice(day="2026-09-30"),
    notice(ticker="600000"), notice(adjunctUrl="https://evil.invalid/12345.PDF"),
    notice(adjunctUrl="finalpage/2026-10-14/999.PDF"), notice(announcementTime=float("nan")),
    notice(announcementTitle="2025年前三季度业绩预告"), notice(announcementTitle="董事会决议公告")])
def test_bad_or_inapplicable_notice_never_creates_an_event(row):
    result = scan([row])
    assert result["events"] == [] and len(result["exclusions"]) == 1


def test_page_is_not_claimed_as_complete_universe():
    result = scan([notice()], total=45)
    assert result["page_coverage_complete"] is False
    assert result["universe_claim"] == "observed_first_page_only"
    with pytest.raises(ValueError):
        scan([notice()], total=0)


def test_original_pages_are_complete_and_numeric_meaning_not_certified(monkeypatch):
    event = scan([notice()])["events"][0]
    originals = ["证券代码300434 测试股份 2026年前三季度业绩预告 单位：万元 净利润亏损100-200",
                 "这是完整第二页。尚未经审计，最终以正式报告为准，风险和注释不能截掉。"]
    monkeypatch.setattr("sovereign_hall.services.primary_event_research.extract_pages", lambda _: originals)
    docs, receipt = event_documents(b"original bytes", event=event, captured_at=NOW, cutoff=NOW)
    assert len(docs) == 2 and all(raw in doc.content for raw, doc in zip(originals, docs))
    assert docs[0].metadata["shared_source_group_id"] == docs[1].metadata["shared_source_group_id"]
    assert receipt["verified_numeric_facts"] is False
    assert receipt["production_promotion"] is False


def test_wrong_original_or_capture_time_is_rejected(monkeypatch):
    event = scan([notice()])["events"][0]
    monkeypatch.setattr("sovereign_hall.services.primary_event_research.extract_pages", lambda _: ["wrong issuer"])
    with pytest.raises(ValueError, match="issuer/title"):
        event_documents(b"bytes", event=event, captured_at=NOW, cutoff=NOW)
    with pytest.raises(ValueError, match="cutoff"):
        event_documents(b"bytes", event=event, captured_at=NOW, cutoff=NOW.replace(hour=13))
