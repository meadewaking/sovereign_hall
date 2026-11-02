"""Prediction feedback must only consume observations inside its frozen horizon."""
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest
from sovereign_hall.services.decision_tracker import DecisionRecorder


def bar(day, high=10.2, low=9.8, close=10.0):
    return dict(date=day, open=10., high=high, low=low, close=close,
                provider='fixture', fetched_at='2026-08-04T17:00:00')


async def setup_case(tmp_path, monkeypatch, bars, *, start='2026-07-28T23:34:18', days=3, direction='hold', calendar=None):
    db = tmp_path/'feedback.db'
    rec = DecisionRecorder(str(db))
    key = await rec.record_decision(ticker='600522', decision=direction,
                                   confidence=.7, target_price=.05, stop_loss=.05,
                                   entry_price=10., expected_days=days)
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE price_predictions SET predicted_at=?, entry_date=? WHERE id=?', (start,start,key))
    market = type('FrozenMarket', (), {})()
    market.get_current_price = AsyncMock(return_value=1000.)
    market.get_ohlc = AsyncMock(return_value=bars)
    market.get_trading_calendar = AsyncMock(return_value=calendar if calendar is not None else [dict(trade_date=b['date'], is_open=1) for b in bars])
    monkeypatch.setattr('sovereign_hall.services.market_data.get_market_data', lambda: market)
    return db, rec, key, market


def stored(db):
    with sqlite3.connect(db) as conn:
        return conn.execute('select status,result,actual_hit_date from price_predictions').fetchone()


@pytest.mark.asyncio
async def test_post_close_entry_cannot_miss_earlier_upside(tmp_path, monkeypatch):
    db,rec,key,market=await setup_case(tmp_path,monkeypatch,[bar('2026-07-28',high=11),bar('2026-07-29',low=9)])
    result=await rec.validate_single(key)
    assert stored(db)==('validated','correct','2026-07-29')
    assert result['validation_window']['start']=='2026-07-28T23:34:18'
    market.get_current_price.assert_not_awaited()
    args=market.get_ohlc.await_args.args
    assert args[2].isoformat()=='2026-07-31T23:34:18'


@pytest.mark.asyncio
async def test_post_expiry_price_cannot_change_neutral_feedback(tmp_path, monkeypatch):
    db,rec,key,market=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29'),bar('2026-07-30'),bar('2026-07-31'),bar('2026-08-03',high=1000)])
    result=await rec.validate_single(key)
    assert stored(db)==('validated','correct','2026-07-31')
    assert result['actual_return']==0
    assert len(result['validation_window']['bars'])==3
    market.get_current_price.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('direction,high,low,expected', [('long',11,9.8,'correct'),('long',10.2,9,'wrong'),('short',10.2,9,'correct'),('short',11,9.8,'wrong'),('hold',11,9.8,'wrong'),('hold',10.2,9,'correct'),('hold',11,9,'partial')])
async def test_valid_in_window_winners_and_losers(tmp_path,monkeypatch,direction,high,low,expected):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29',high=high,low=low)],direction=direction)
    result=await rec.validate_single(key)
    assert result['result']==expected
    assert stored(db)[2]=='2026-07-29'


@pytest.mark.asyncio
async def test_intraday_entry_crossing_is_ambiguous(tmp_path,monkeypatch):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-28',high=11),bar('2026-07-29',low=9)],start='2026-07-28T11:00:00')
    assert (await rec.validate_single(key))['error']=='prediction_entry_session_ambiguous'
    assert stored(db)==('pending','unknown',None)


@pytest.mark.asyncio
async def test_partial_expiry_does_not_use_after_deadline_bar(tmp_path,monkeypatch):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-28'),bar('2026-07-29'),bar('2026-07-30'),bar('2026-07-31',high=11)],start='2026-07-28T11:00:00')
    assert (await rec.validate_single(key))['error']=='prediction_expiry_evidence_incomplete'
    assert stored(db)==('pending','unknown',None)


@pytest.mark.asyncio
@pytest.mark.parametrize('bars,calendar,reason', [([], [], 'prediction_window_data_unavailable'),([bar('2026-07-29')], [], 'prediction_window_calendar_unavailable'),([bar('2026-07-30',high=11)],[dict(trade_date='2026-07-29',is_open=1),dict(trade_date='2026-07-30',is_open=1)],'prediction_window_incomplete')])
async def test_missing_evidence_remains_pending(tmp_path,monkeypatch,bars,calendar,reason):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,bars,calendar=calendar)
    assert (await rec.validate_single(key))['error']==reason
    assert stored(db)==('pending','unknown',None)


@pytest.mark.asyncio
async def test_delayed_entry_and_direct_call_respect_maturity(tmp_path,monkeypatch):
    db,rec,key,market=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29',high=11)])
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE price_predictions SET entry_date='2099-07-28T16:00:00'")
    assert (await rec.validate_single(key))['error']=='prediction_window_not_matured'
    assert (await rec.validate_pending())['attempted']==0
    market.get_ohlc.assert_not_awaited()
    assert stored(db)==('pending','unknown',None)


@pytest.mark.asyncio
async def test_atomic_event_and_idempotency(tmp_path,monkeypatch):
    db,rec,key,market=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29',high=11)])
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE round_events(round_id TEXT,sequence INTEGER,event_type TEXT,payload_json TEXT,created_at TEXT,UNIQUE(round_id,sequence))")
        conn.execute("UPDATE price_predictions SET round_id='round_fixture'")
        conn.execute("CREATE TRIGGER fail_event BEFORE INSERT ON round_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
    with pytest.raises(sqlite3.IntegrityError,match='fixture failure'):
        await rec.validate_single(key)
    assert stored(db)==('pending','unknown',None)
    with sqlite3.connect(db) as conn: conn.execute('DROP TRIGGER fail_event')
    await rec.validate_single(key)
    assert (await rec.validate_single(key))['unchanged'] is True
    with sqlite3.connect(db) as conn:
        rows=conn.execute('SELECT event_type,payload_json FROM round_events').fetchall()
    assert len(rows)==1
    assert rows[0][0]=='PredictionValidationWindowApplied'
    assert json.loads(rows[0][1])['prediction_id']==key
    assert market.get_ohlc.await_count==2


@pytest.mark.asyncio
async def test_missing_partial_expiry_bar_is_not_neutral_success(tmp_path,monkeypatch):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29'),bar('2026-07-30')],start='2026-07-28T11:00:00',calendar=[dict(trade_date=d,is_open=1) for d in ['2026-07-29','2026-07-30','2026-07-31']])
    assert (await rec.validate_single(key))['error']=='prediction_expiry_evidence_incomplete'
    assert stored(db)==('pending','unknown',None)


@pytest.mark.asyncio
async def test_utc_entry_is_compared_in_exchange_timezone(tmp_path,monkeypatch):
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-28',high=11),bar('2026-07-29',low=9)],start='2026-07-28T15:34:18Z')
    assert (await rec.validate_single(key))['result']=='correct'
    assert stored(db)[2]=='2026-07-29'


@pytest.mark.asyncio
async def test_concurrent_validation_has_one_terminal_event(tmp_path,monkeypatch):
    import asyncio
    db,rec,key,_=await setup_case(tmp_path,monkeypatch,[bar('2026-07-29',high=11)])
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE round_events(round_id TEXT,sequence INTEGER,event_type TEXT,payload_json TEXT,created_at TEXT,UNIQUE(round_id,sequence))")
        conn.execute("UPDATE price_predictions SET round_id='round_fixture'")
    results=await asyncio.gather(rec.validate_single(key),rec.validate_single(key))
    assert all(r['result']=='wrong' for r in results)
    assert sum(bool(r.get('unchanged')) for r in results)==1
    with sqlite3.connect(db) as conn: assert conn.execute('SELECT count(*) FROM round_events').fetchone()[0]==1


@pytest.mark.asyncio
async def test_missing_intraday_entry_bar_cannot_grade_later_move(tmp_path, monkeypatch):
    # The calendar proves a partial entry session existed, but its bar is absent.
    # A later day cannot rule out an earlier target or stop crossing.
    db,rec,key,_=await setup_case(
        tmp_path,monkeypatch,[bar('2026-07-29', high=11)],
        start='2026-07-28T11:00:00',
        calendar=[dict(trade_date=d,is_open=1) for d in ['2026-07-28','2026-07-29']])
    result=await rec.validate_single(key)
    assert result.get('error')=='prediction_entry_session_incomplete'
    assert stored(db)==('pending','unknown',None)
