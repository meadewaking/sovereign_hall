"""Calendar-verified reference marks for valuation, separate from fill prices."""
from datetime import datetime,time,timedelta
from functools import lru_cache

from ..domain.portfolio.quote_freshness import MARKET_ZONE,is_fresh_quote,quote_age_seconds
from .exchange_calendar import load_exchange_calendar


@lru_cache(maxsize=4)
def _calendar(year):
    return load_exchange_calendar(year)


def market_window(now=None):
    current=now or datetime.now(MARKET_ZONE)
    current=current.astimezone(MARKET_ZONE) if current.tzinfo else current.replace(tzinfo=MARKET_ZONE)
    try:
        today=current.date();opened=_calendar(today.year).sessions[today.isoformat()]
        clock=current.time()
        if opened and (time(9,30)<=clock<=time(11,30) or time(13)<=clock<=time(15)):
            return {'kind':'realtime','now':current,'boundary':None}
        if opened and time(11,30)<clock<time(13):
            boundary=current.replace(hour=11,minute=30,second=0,microsecond=0)
            return {'kind':'session_pause','now':current,'boundary':boundary}
        if opened and clock>time(15):
            day=today
        else:
            day=today-timedelta(days=1)
            for _ in range(32):
                if _calendar(day.year).sessions[day.isoformat()]:
                    break
                day-=timedelta(days=1)
            else:
                return None
        return {'kind':'session_close','now':current,
                'boundary':datetime.combine(day,time(15),tzinfo=MARKET_ZONE)}
    except (OSError,ValueError,KeyError,TypeError):
        return None


def is_valid_valuation_quote(quote,*,now=None,max_retrieval_age_seconds=300):
    if is_fresh_quote(quote,now=now):
        return True
    if not isinstance(quote,dict) or quote.get('valuation_only') is not True:
        return False
    window=market_window(now)
    if window is None or window['kind']=='realtime':
        return False
    current=window['now'];boundary=window['boundary']
    raw={**quote,'valuation_only':False}
    if not is_fresh_quote(raw,max_age_seconds=float('inf'),now=current):
        return False
    if quote_age_seconds(quote.get('fetched_at'),quote.get('fetched_at'),now=current)>max_retrieval_age_seconds:
        return False
    quoted=datetime.fromisoformat(str(quote['quoted_at']).replace('Z','+00:00'))
    quoted=quoted.astimezone(MARKET_ZONE) if quoted.tzinfo else quoted.replace(tzinfo=MARKET_ZONE)
    return (quoted.date()==boundary.date() and quoted>=boundary-timedelta(seconds=120)
            and quote.get('valuation_kind')==window['kind']
            and quote.get('valuation_session')==boundary.isoformat())


def valuation_quote(raw,*,now=None):
    if is_fresh_quote(raw,now=now):
        return {**raw,'valuation_only':False,'valuation_kind':'realtime'}
    window=market_window(now)
    if window is None or window['kind']=='realtime':
        return None
    candidate={**raw,'valuation_only':True,'valuation_kind':window['kind'],
               'valuation_session':window['boundary'].isoformat()}
    return candidate if is_valid_valuation_quote(candidate,now=now) else None


def valuation_summary(quotes):
    values=list(quotes.values())
    kinds={q.get('valuation_kind','realtime') for q in values}
    kind='session_close' if 'session_close' in kinds else 'session_pause' if 'session_pause' in kinds else 'realtime'
    return {'valuation_basis':kind,
            'valuation_label':{'realtime':'实时现价','session_close':'闭市参考价','session_pause':'午间休市参考价'}[kind],
            'valuation_as_of':min((str(q.get('quoted_at','')) for q in values),default=None)}
