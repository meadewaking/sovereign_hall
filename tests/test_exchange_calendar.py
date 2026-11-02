"""Trusted calendar recovery, unknown coverage, provenance and atomic persistence."""
from datetime import date, datetime, timezone
import json
import shutil
import sqlite3

import pytest

from sovereign_hall.services import market_data as market_module
from sovereign_hall.services.exchange_calendar import CALENDAR_DIR, load_exchange_calendar
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.scripts.backfill_daily_prices import ensure_market_truth_schema, persist_market_truth_metadata, persist_trading_calendar


def test_verified_dual_exchange_schedule_includes_holidays_and_makeup_weekends():
    calendar = load_exchange_calendar(2026)
    assert len(calendar.sessions) == 365
    for day in ['2026-01-01', '2026-02-23', '2026-05-05', '2026-09-25', '2026-10-07', '2026-10-10']:
        assert calendar.sessions[day] == 0
    for day in ['2026-01-05', '2026-02-24', '2026-05-06', '2026-09-28', '2026-10-08']:
        assert calendar.sessions[day] == 1
    with pytest.raises(FileNotFoundError):
        load_exchange_calendar(2027)


@pytest.mark.parametrize('change', ['raw_hash', 'day', 'missing_holiday', 'source_host', 'missing_source'])
def test_incomplete_tampered_or_disagreeing_calendar_fails_closed(tmp_path, change):
    for p in CALENDAR_DIR.iterdir():
        shutil.copyfile(p, tmp_path / p.name)
    bundle_path = tmp_path / 'cn_2026.json'
    bundle = json.loads(bundle_path.read_text())
    if change == 'raw_hash':
        (tmp_path / 'sse_2026.html').write_text('replaced source')
    elif change == 'day':
        bundle['sessions']['2026-10-07'] = 1
    elif change == 'missing_holiday':
        # A correctly hashed but partial notice cannot authorize any session.
        from hashlib import sha256
        raw = (tmp_path / 'sse_2026.html').read_bytes().replace('国庆节：'.encode(), b'')
        (tmp_path / 'sse_2026.html').write_bytes(raw)
        bundle['sources'][0]['sha256'] = sha256(raw).hexdigest()
    elif change == 'source_host':
        bundle['sources'][0]['url'] = 'https://example.com/calendar'
    else:
        bundle['sources'].pop()
    bundle_path.write_text(json.dumps(bundle))
    with pytest.raises((ValueError, IndexError)):
        load_exchange_calendar(2026, tmp_path)


@pytest.mark.asyncio
async def test_market_timezone_coverage_and_session_boundaries(monkeypatch):
    md = MarketDataService()
    try:
        assert await md.is_trading_day(date(2026, 10, 8))
        assert not await md.is_trading_day(date(2026, 10, 7))
        assert not await md.is_trading_day(date(2027, 1, 4))
        assert await md.is_trading_day(datetime(2026, 10, 7, 23, tzinfo=timezone.utc))
        for stamp in ['2026-10-08T09:30:00', '2026-10-08T11:30:00', '2026-10-08T13:00:00', '2026-10-08T15:00:00']:
            assert await md.is_market_open(datetime.fromisoformat(stamp))
        for stamp in ['2026-10-08T09:29:59', '2026-10-08T11:30:01', '2026-10-08T12:00:00', '2026-10-08T15:00:01', '2026-10-10T10:00:00']:
            assert not await md.is_market_open(datetime.fromisoformat(stamp))
        assert await md.is_market_open(datetime(2026, 10, 8, 2, tzinfo=timezone.utc))
        rows = await md.get_trading_calendar('2026-10-01', '2026-10-10')
        assert len(rows) == 10 and sum(r['is_open'] for r in rows) == 2
        assert all(r['source_kind'] == 'exchange_calendar' and r['validity_end'] == '2026-12-31' for r in rows)
        md._exchange_calendars.clear()
        def missing(year):
            raise FileNotFoundError('no trusted certificate')
        monkeypatch.setattr(market_module, 'load_exchange_calendar', missing)
        assert not await md.is_market_open(datetime(2026, 10, 8, 10))
    finally:
        await md.close()


def test_calendar_persistence_is_atomic_idempotent_and_preserves_provenance(tmp_path):
    path = tmp_path / 'calendar.db'
    calendar = load_exchange_calendar(2026)
    rows = calendar.rows(date(2026, 1, 1), date(2026, 12, 31))
    assert persist_trading_calendar(path, rows) == 365
    assert persist_trading_calendar(path, rows) == 0
    with sqlite3.connect(path) as c:
        row = c.execute("SELECT is_open,source_identity,source_kind,validity_start,validity_end,inference_rule FROM trading_calendar WHERE trade_date='2026-10-08'").fetchone()
        assert row == (1, calendar.source_identity, 'exchange_calendar', '2026-01-01', '2026-12-31', 'dual_exchange_annual_notice_plus_weekends')
    rollback_path = tmp_path / 'rollback.db'
    with sqlite3.connect(rollback_path) as c:
        ensure_market_truth_schema(c)
        c.execute("CREATE TRIGGER abort_calendar BEFORE INSERT ON trading_calendar WHEN NEW.trade_date='2026-06-01' BEGIN SELECT RAISE(ABORT,'injected failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected failure'):
        persist_trading_calendar(rollback_path, rows)
    with sqlite3.connect(rollback_path) as c:
        assert c.execute('SELECT COUNT(*) FROM trading_calendar').fetchone()[0] == 0


def test_observed_bars_cannot_overwrite_verified_open_closed_calendar(tmp_path):
    path = tmp_path / 'calendar.db'
    calendar = load_exchange_calendar(2026)
    rows = calendar.rows(date(2026, 10, 7), date(2026, 10, 8))
    persist_trading_calendar(path, rows)
    with sqlite3.connect(path) as c:
        before = c.execute('SELECT * FROM trading_calendar ORDER BY trade_date').fetchall()
        persist_market_truth_metadata(c, set(), {'2026-10-07', '2026-10-08'},
                                      source='observed_index_bars', fetched_at='2026-10-08T14:00:00')
        assert c.execute('SELECT * FROM trading_calendar ORDER BY trade_date').fetchall() == before
    assert persist_trading_calendar(path, [{'trade_date': '2026-10-07', 'is_open': 1,
                                          'source': 'observed_index_bars'}]) == 0
    with sqlite3.connect(path) as c:
        assert c.execute('SELECT * FROM trading_calendar ORDER BY trade_date').fetchall() == before
