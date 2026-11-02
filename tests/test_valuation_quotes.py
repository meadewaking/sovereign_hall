"""Session marks value the account; they never become executable ticks."""
from datetime import datetime,timedelta
import asyncio
import sqlite3
import httpx
import pytest
from sovereign_hall.domain.portfolio.quote_freshness import MARKET_ZONE,is_fresh_quote
from sovereign_hall.services import valuation_quotes as policy
from sovereign_hall.services.market_data import MarketDataService
from sovereign_hall.services.investment_simulation import InvestmentSimulation


def dt(value):
    return datetime.fromisoformat(value).replace(tzinfo=MARKET_ZONE)


def raw(quoted,now):
    return dict(ticker='512800',price=.866,source='tencent_realtime_quote',
                quoted_at=dt(quoted).isoformat(),fetched_at=dt(now).isoformat())


@pytest.mark.parametrize('now,quoted,kind',[
 ('2026-10-20T17:00:00','2026-10-20T15:34:59','session_close'),
 ('2026-10-21T09:20:00','2026-10-20T16:14:58','session_close'),
 ('2026-10-25T12:00:00','2026-10-23T15:00:00','session_close'),
 ('2026-10-06T12:00:00','2026-09-30T15:00:00','session_close'),
 ('2026-10-20T12:00:00','2026-10-20T11:29:59','session_pause'),
])
def test_latest_completed_session_can_value_but_cannot_execute(now,quoted,kind):
    quote=policy.valuation_quote(raw(quoted,now),now=dt(now))
    assert quote and quote['valuation_kind']==kind and quote['valuation_only']
    assert quote['quoted_at']==dt(quoted).isoformat()
    assert policy.is_valid_valuation_quote(quote,now=dt(now))
    assert not is_fresh_quote(quote,now=dt(now))


@pytest.mark.parametrize('now,quoted',[
 ('2026-10-20T17:00:00','2026-10-19T15:00:00'),
 ('2026-10-20T17:00:00','2026-10-20T11:30:00'),
 ('2026-10-20T12:00:00','2026-10-20T10:00:00'),
 ('2026-10-20T14:00:00','2026-10-20T11:30:00'),
 ('2026-10-25T12:00:00','2026-10-22T15:00:00'),
 ('2026-10-06T12:00:00','2026-10-01T15:00:00'),
])
def test_old_session_or_intraday_outage_stays_unavailable(now,quoted):
    assert policy.valuation_quote(raw(quoted,now),now=dt(now)) is None


def test_new_session_and_expired_retrieval_invalidate_reference_cache():
    now='2026-10-20T12:59:00'
    quote=policy.valuation_quote(raw('2026-10-20T11:30:00',now),now=dt(now))
    assert quote
    assert not policy.is_valid_valuation_quote(quote,now=dt('2026-10-20T13:00:00'))
    close=policy.valuation_quote(raw('2026-10-20T15:00:00','2026-10-20T17:00:00'),now=dt('2026-10-20T17:00:00'))
    assert not policy.is_valid_valuation_quote(close,now=dt('2026-10-20T17:05:01'))


def test_unknown_calendar_and_forged_reference_cannot_loosen_execution(monkeypatch):
    now=dt('2026-10-20T15:00:30')
    marked={**raw('2026-10-20T15:00:00','2026-10-20T15:00:30'),
            'valuation_only':True,'valuation_kind':'session_close','valuation_session':dt('2026-10-20T15:00:00').isoformat()}
    assert not is_fresh_quote(marked,now=now)  # Even within the 120-second age.
    def unknown(year):raise ValueError('unknown calendar')
    monkeypatch.setattr(policy,'_calendar',unknown)
    assert not policy.is_valid_valuation_quote(marked,now=now)


@pytest.fixture
def clock(monkeypatch):
    value=[dt('2026-10-20T17:00:00')]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):
            return value[0].astimezone(tz) if tz else value[0].replace(tzinfo=None)
    import sovereign_hall.services.market_data as market_module
    import sovereign_hall.domain.portfolio.quote_freshness as freshness
    monkeypatch.setattr(policy,'datetime',Clock)
    monkeypatch.setattr(market_module,'datetime',Clock)
    monkeypatch.setattr(freshness,'datetime',Clock)
    return value


async def test_provider_mark_is_cached_and_next_open_fetches_live_again(clock):
    calls=[]
    def handler(request):
        calls.append(request.url.host)
        if request.url.host.endswith('gtimg.cn'):
            fields=['']*40;fields[1:4]=['ETF','512800','.866']
            fields[30]='20261020161458'
            return httpx.Response(200,content=('v_sh512800="'+'~'.join(fields)+'";').encode())
        return httpx.Response(200,text='')
    market=MarketDataService();await market._client.aclose()
    market._client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        mark=await market.get_valuation_quote('512800')
        assert mark and mark['valuation_only']
        previous=len(calls)
        assert await market.get_valuation_quote('512800')==mark
        assert await market.get_current_quote('512800') is None
        assert len(calls)==previous
        clock[0]=dt('2026-10-21T09:30:00')
        assert await market.get_valuation_quote('512800') is None
        assert len(calls)>previous
    finally:await market.close()


async def test_account_valuation_recovers_without_making_reference_executable(clock):
    mark=policy.valuation_quote(raw('2026-10-20T15:34:59','2026-10-20T17:00:00'))
    class Quotes:
        async def get_valuation_quote(self,ticker):return mark
        async def get_current_quote(self,ticker):return mark  # Deliberately wrong trading seam.
    sim=InvestmentSimulation(quote_provider=Quotes());sim.cash=7077.61023
    sim.positions={'512800':{'shares':1800,'avg_cost':.88,'opened_at':'2026-10-20'}}
    assets=await sim.calculate_assets()
    assert assets['valuation_complete'] and assets['total_assets']==pytest.approx(8636.41023)
    assert assets['valuation_basis']=='session_close' and assets['valuation_label']=='闭市参考价'
    assert assets['valuation_as_of']==mark['quoted_at']
    assert (await sim.resolve_trade_price_detail('512800'))['price'] is None
    performance=await sim.get_performance_metrics()
    assert performance['performance_standard']=='simulation_account_session_valued_v2'
    assert performance['net_total_return']==pytest.approx(-.136358977)


async def test_strict_closed_observations_avoid_repeated_network_but_never_return_price(clock):
    from unittest.mock import AsyncMock
    market=MarketDataService()
    market._fetch_tencent_quote=AsyncMock(return_value=(.866,'ETF',dt('2026-10-20T16:14:58').isoformat()))
    market._fetch_eastmoney_quote=AsyncMock(return_value=(None,'',''))
    market._fetch_sina_quote=AsyncMock(return_value=(None,'',''))
    try:
        assert await market.get_current_quote('512800') is None
        assert await market.get_current_quote('512800') is None
        assert market._fetch_tencent_quote.await_count==1
        assert not market._quote_cache
        assert (await market.get_valuation_quote('512800'))['valuation_only']
    finally:
        await market.close()


def test_status_view_labels_reference_and_preserves_source_time(tmp_path,monkeypatch,capsys,clock):
    import sovereign_hall.check_db as view
    path=tmp_path/'view.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE system_stats(key TEXT PRIMARY KEY,value TEXT)')
        conn.execute("INSERT INTO system_stats VALUES ('simulation_cash','7077.61023')")
        conn.execute('CREATE TABLE simulation_positions(ticker TEXT,shares INTEGER,avg_cost REAL)')
        conn.execute("INSERT INTO simulation_positions VALUES ('512800',1800,.88)")
        conn.execute('CREATE TABLE simulation_trades(ticker TEXT,direction TEXT,shares INTEGER,price REAL,reason TEXT,traded_at TEXT)')
    mark=policy.valuation_quote(raw('2026-10-20T16:14:58','2026-10-20T17:00:00'))
    monkeypatch.setattr(view,'get_valuation_prices',lambda tickers:{'512800':mark})
    performance=view.show_investment_status(path)
    output=capsys.readouterr().out
    assert '当前资产: 8636.41 元（闭市参考价；来源时间 2026-10-20T16:14:58+08:00）' in output
    assert 'action=market_closed' in output
    assert performance['performance_standard']=='simulation_account_session_valued_v2'
