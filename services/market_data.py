"""
Market data service.

Centralizes quote and daily OHLC retrieval so predictions, validation, and
simulation all share the same price source and never fall back to fake prices.
"""

import asyncio
import logging
import re
from datetime import date, datetime, time, timedelta
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from ..domain.portfolio.instruments import is_etf_ticker, normalize_ticker
from ..domain.portfolio.quote_freshness import MARKET_ZONE, is_fresh_quote, quote_age_seconds, retrieved_after
from .exchange_calendar import ExchangeCalendar, load_exchange_calendar
from ..utils import sync_retry_with_backoff

logger = logging.getLogger(__name__)

DEFAULT_QUOTE_RECOVERY_PASSES = 1
MAX_QUOTE_RECOVERY_PASSES = 2


async def collect_realtime_quote_batch(
    tickers: Iterable[str],
    fetch_quote: Callable[[str], Awaitable[Optional[Dict[str, Any]]]],
    *,
    recovery_passes: int = DEFAULT_QUOTE_RECOVERY_PASSES,
) -> Dict[str, Dict[str, Any]]:
    """Fetch a portfolio quote set and retry only transiently missing symbols.

    The recovery pass runs after the complete first pass.  That ordering lets a
    short provider/network interruption clear without hammering one symbol, and
    every successful value is still a newly fetched provider quote.  Permanent
    failures stay absent so callers' freshness gates keep valuation and trading
    blocked instead of falling back to historical or caller-supplied prices.
    """
    pending: List[str] = []
    seen: Set[str] = set()
    for ticker in tickers:
        code = MarketDataService.normalize_ticker(ticker)
        if code and code not in seen:
            pending.append(code)
            seen.add(code)

    quotes: Dict[str, Dict[str, Any]] = {}
    bounded_recovery_passes = max(
        0,
        min(int(recovery_passes or 0), MAX_QUOTE_RECOVERY_PASSES),
    )
    for pass_index in range(bounded_recovery_passes + 1):
        if not pending:
            break
        if pass_index:
            logger.info(
                "Retrying transiently missing realtime quotes after portfolio pass: %s",
                ",".join(pending),
            )
        still_missing: List[str] = []
        for code in pending:
            quote = await fetch_quote(code)
            if isinstance(quote, dict):
                quotes[code] = quote
            else:
                still_missing.append(code)
        pending = still_missing
    return quotes


class MarketDataError(RuntimeError):
    """Raised when market data cannot be fetched."""


class _AkSharePermanentError(RuntimeError):
    """Raised when AkShare fails for a reason that won't resolve on retry.

    Used to distinguish permanent failures (parsing errors, invalid ticker
    shape) from transient network errors so the OHLC negative cache can pick
    the right TTL.
    """


class MarketDataService:
    """Small async client for A-share/ETF quotes and daily bars."""

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=5.0, read=timeout),
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        self._quote_cache: Dict[str, Dict[str, Any]] = {}
        self._valuation_cache: Dict[str, Dict[str, Any]] = {}
        self._observed_quotes: Dict[str, Dict[str, Any]] = {}
        self._quote_ttl_seconds = 60
        self._max_quote_age_seconds = 120
        self._quote_request_timeout = min(float(timeout), 4.0)
        self.last_quote_attempts: Dict[str, List[Dict[str, Any]]] = {}
        self._exchange_calendars: Dict[int, ExchangeCalendar] = {}
        self._eastmoney_ohlc_failures = 0
        self._eastmoney_ohlc_cooldown_until: Optional[datetime] = None
        self._eastmoney_ohlc_failure_threshold = 3
        self._eastmoney_ohlc_cooldown_seconds = 300
        # Ticker-level negative cache: code -> (expiry_ts, reason).
        # Only populated when ALL providers fail for a ticker, so successful
        # fallbacks are never cached as failures.
        self._ohlc_negative_cache: Dict[str, Tuple[float, str]] = {}
        self._ohlc_negative_lock = asyncio.Lock()
        self._ohlc_neg_cache_short_ttl = 60      # transient errors
        self._ohlc_neg_cache_long_ttl = 3600     # permanent errors (e.g. Tencent 501 on qfq)

    @staticmethod
    def normalize_ticker(ticker: str) -> str:
        return normalize_ticker(ticker)

    _SUPPORTED_PREFIXES = (
        "600", "601", "603", "605", "688",
        "000", "001", "002", "003", "300", "301",
        "510", "511", "512", "513", "515", "516", "517", "518",
        "560", "561", "562", "563", "588",
        "159",
    )

    @classmethod
    def is_supported_ticker(cls, ticker: str) -> bool:
        """Return whether ``ticker`` can identify an A-share/ETF quote.

        Proposal prompts contain human-readable placeholders such as
        ``推荐标的代码``.  Treating those as symbols pollutes committee memory and
        needlessly reaches quote providers, so validation lives at the shared
        market-data boundary rather than in prompt-only cleanup.
        """
        code = cls.normalize_ticker(ticker)
        if len(code) != 6 or not code.isdigit():
            return False
        return code.startswith(cls._SUPPORTED_PREFIXES)

    @classmethod
    def infer_market(cls, ticker: str) -> Optional[str]:
        code = cls.normalize_ticker(ticker)
        if not code.isdigit() or len(code) != 6:
            return None
        if code.startswith(("600", "601", "603", "605", "688", "510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588")):
            return "sh"
        if code.startswith(("000", "001", "002", "003", "300", "301", "159")):
            return "sz"
        return "sh" if code.startswith("6") else "sz"

    @classmethod
    def eastmoney_secid(cls, ticker: str) -> Optional[str]:
        market = cls.infer_market(ticker)
        code = cls.normalize_ticker(ticker)
        if not market:
            return None
        prefix = "1" if market == "sh" else "0"
        return f"{prefix}.{code}"

    async def close(self):
        global _market_data
        await self._client.aclose()
        if _market_data is self:
            _market_data = None

    def _ensure_client(self):
        if self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=5.0, read=self.timeout),
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            )

    async def is_trading_day(self, when: Optional[date | datetime] = None) -> bool:
        """Return whether the exchange is open on the given date.

        Unknown calendars fail closed; weekdays alone do not establish an open session.
        """
        if isinstance(when, datetime):
            target = (when.astimezone(MARKET_ZONE) if when.tzinfo else when).date()
        else:
            target = when or datetime.now(MARKET_ZONE).date()
        if not isinstance(target, date):
            return False

        if target.weekday() >= 5:
            return False

        trade_days = await self._load_trade_days(target.year)
        if trade_days is None:
            logger.warning("Trading calendar unavailable; simulation execution blocked")
            return False
        return target in trade_days

    async def is_market_open(self, when: Optional[datetime] = None) -> bool:
        """Return whether A-share continuous trading is currently open."""
        current = when or datetime.now(MARKET_ZONE)
        current = current.astimezone(MARKET_ZONE) if current.tzinfo else current.replace(tzinfo=MARKET_ZONE)
        if not await self.is_trading_day(current):
            return False
        clock = current.time()
        return time(9, 30) <= clock <= time(11, 30) or time(13) <= clock <= time(15)

    def _get_exchange_calendar(self, year: int) -> Optional[ExchangeCalendar]:
        try:
            if year not in self._exchange_calendars:
                self._exchange_calendars[year] = load_exchange_calendar(year)
                calendar = self._exchange_calendars[year]
                logger.info('Verified exchange calendar loaded: year=%s days=%s source_identity=%s',
                            year, len(calendar.sessions), calendar.source_identity)
            return self._exchange_calendars[year]
        except Exception as exc:
            logger.warning("Verified exchange calendar unavailable for %s: %s", year, exc)
            return None

    async def _load_trade_days(self, year: Optional[int] = None) -> Optional[Set[date]]:
        calendar = self._get_exchange_calendar(year or datetime.now(MARKET_ZONE).year)
        return {date.fromisoformat(day) for day, opened in calendar.sessions.items() if opened} if calendar else None

    async def get_current_price(self, ticker: str) -> Optional[float]:
        """Return the latest realtime quote price, or None when unavailable."""
        quote = await self.get_current_quote(ticker)
        return float(quote["price"]) if quote else None

    async def get_current_quotes(
        self,
        tickers: Iterable[str],
        *,
        recovery_passes: int = DEFAULT_QUOTE_RECOVERY_PASSES,
    ) -> Dict[str, Dict[str, Any]]:
        """Return a portfolio quote set with bounded missing-symbol recovery."""
        return await collect_realtime_quote_batch(
            tickers,
            self.get_current_quote,
            recovery_passes=recovery_passes,
        )

    async def get_current_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return only fresh quotes with both provider event and retrieval timestamps."""
        from .valuation_quotes import is_valid_valuation_quote
        cached=self._valuation_cache.get(self.normalize_ticker(ticker))
        if cached and cached.get('valuation_only') and is_valid_valuation_quote(cached):
            return None
        return await self._get_quote(ticker,valuation=False)

    async def get_execution_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Acquire a new controlled observation; research caches cannot execute."""
        requested = datetime.now(MARKET_ZONE)
        quote = await self._get_quote(ticker, valuation=False, use_cache=False)
        if (is_fresh_quote(quote, max_age_seconds=self._max_quote_age_seconds)
                and retrieved_after(quote, requested.isoformat())):
            return quote
        return None

    async def get_valuation_quote(self,ticker):
        return await self._get_quote(ticker,valuation=True)

    async def get_valuation_quotes(self,tickers):
        return await collect_realtime_quote_batch(tickers,self.get_valuation_quote)

    async def _get_quote(self,ticker,*,valuation,use_cache=True):
        from .valuation_quotes import valuation_quote,is_valid_valuation_quote,market_window
        code = self.normalize_ticker(ticker)
        if not self.is_supported_ticker(code):
            logger.warning("Reject unsupported realtime quote ticker: %r", ticker)
            return None

        if valuation:
            for cached in (self._quote_cache.get(code),self._valuation_cache.get(code),self._observed_quotes.get(code)):
                if cached:
                    marked=valuation_quote(cached)
                    if marked is not None:
                        self._valuation_cache[code]=marked
                        return dict(marked)
        cached = self._quote_cache.get(code)
        if use_cache and cached and is_fresh_quote(cached, max_age_seconds=self._quote_ttl_seconds):
            return dict(cached)
        self._quote_cache.pop(code, None)
        self._ensure_client()
        providers = (
            ((lambda code:self._fetch_tencent_quote(code,accept_quote=valuation_quote))
             if valuation else self._fetch_tencent_quote, "tencent_realtime_quote"),
            (self._fetch_eastmoney_quote, "eastmoney_realtime_quote"),
            (self._fetch_sina_quote, "sina_realtime_quote"),
        )
        attempts: List[Dict[str, Any]] = []
        self.last_quote_attempts[code] = attempts

        async def fetch_one(fetch, source, pass_index):
            record = {"source": source, "pass": pass_index,
                      "started_at": datetime.now(MARKET_ZONE).isoformat()}
            attempts.append(record)
            request = None
            try:
                request = asyncio.create_task(fetch(code), name="quote-" + source)
                price, name, quoted_at = await asyncio.wait_for(
                    request, timeout=self._quote_request_timeout)
                quote = {"ticker": code, "price": price, "name": name,
                         "source": source, "quoted_at": quoted_at,
                         "fetched_at": datetime.now(MARKET_ZONE).isoformat()}
                age = quote_age_seconds(quote['fetched_at'], quoted_at)
                record.update(quoted_at=quoted_at,
                              source_age_seconds=round(age, 1) if age is not None else None,
                              freshness_limit_seconds=self._max_quote_age_seconds)
                if is_fresh_quote(quote,max_age_seconds=float('inf')):
                    previous=self._observed_quotes.get(code)
                    if previous is None or str(quote['quoted_at'])>=str(previous.get('quoted_at','')):
                        self._observed_quotes[code]=dict(quote)
                        reference=valuation_quote(quote)
                        if reference and reference.get('valuation_only'):
                            self._valuation_cache[code]=reference
                if valuation:
                    marked=valuation_quote(quote)
                    if marked is not None:
                        record['status']='valuation_reference' if marked.get('valuation_only') else 'fresh'
                        return marked
                if is_fresh_quote(quote, max_age_seconds=self._max_quote_age_seconds):
                    record["status"] = "fresh"
                    return quote
                record["status"] = ("stale_quote" if is_fresh_quote(quote, max_age_seconds=float('inf'))
                                    else "invalid_quote")
            except asyncio.CancelledError:
                record["status"] = "cancelled"
                raise
            except (TimeoutError, httpx.TransportError) as exc:
                record.update(status="transport_failure", error_type=type(exc).__name__, retryable=True)
            except httpx.HTTPStatusError as exc:
                record.update(status="http_error", http_status=exc.response.status_code,
                              retryable=exc.response.status_code in {502, 503, 504})
            except Exception as exc:
                record.update(status="invalid_response", error_type=type(exc).__name__)
            finally:
                # A provider can fail while a winner or the caller cancels its
                # timeout wrapper. Own and drain that inner task explicitly.
                if request is not None:
                    if not request.done():
                        request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
                record["finished_at"] = datetime.now(MARKET_ZONE).isoformat()
            return None

        pending_providers = list(providers)
        for pass_index in range(2):
            tasks = [asyncio.create_task(fetch_one(fetch, source, pass_index))
                     for fetch, source in pending_providers]
            winner = None
            try:
                for completed in asyncio.as_completed(tasks):
                    candidate = await completed
                    if candidate is not None:
                        winner = candidate
                        break
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            # Cancellation cleanup must not rejuvenate a source timestamp.
            if ((valuation and is_valid_valuation_quote(winner)) or
                    is_fresh_quote(winner, max_age_seconds=self._max_quote_age_seconds)):
                if winner.get('valuation_only'):
                    self._valuation_cache[code]=winner
                else:
                    self._quote_cache[code] = winner
                return dict(winner)
            retry_sources = {a["source"] for a in attempts
                             if a["pass"] == pass_index and a.get("retryable")}
            pending_providers = [(fetch, source) for fetch, source in providers if source in retry_sources]
            if pass_index or not pending_providers:
                break
            await asyncio.sleep(0.2)
        window=market_window()
        log=logger.info if not valuation and window and window['kind']!='realtime' else logger.warning
        log("No qualified %s quote for %s: %s", 'valuation' if valuation else 'realtime',code,
                       [{key: a[key] for key in ('source','status','error_type','quoted_at',
                                                'source_age_seconds','freshness_limit_seconds') if key in a}
                        for a in attempts])
        return None

    async def _fetch_tencent_quote(self, ticker: str, *,accept_quote=None) -> tuple[Optional[float], str, str]:
        market = self.infer_market(ticker)
        if not market:
            return None, "", ""
        # Both hosts serve Tencent's original event-time quote format. Give
        # each a share of the existing provider deadline so an unreachable
        # host cannot consume the fallback's entire time allowance.
        hosts = ('web.sqt.gtimg.cn', 'qt.gtimg.cn')
        best, last_error = None, None
        for host in hosts:
            try:
                async with asyncio.timeout(max(.01, self._quote_request_timeout / len(hosts))):
                    resp = await self._client.get(f'https://{host}/q={market}{ticker}')
                    resp.raise_for_status()
                    parts = resp.content.decode('gbk', errors='strict').split('~')
                    if len(parts) <= 30 or parts[2] != ticker:
                        continue
                    quoted = datetime.strptime(parts[30], '%Y%m%d%H%M%S').replace(tzinfo=MARKET_ZONE)
                    candidate = float(parts[3]), parts[1].strip(), quoted.isoformat()
                    quote = {'price': candidate[0], 'source': 'tencent_realtime_quote',
                             'quoted_at': candidate[2], 'fetched_at': datetime.now(MARKET_ZONE).isoformat()}
                    if accept_quote is not None and accept_quote(quote) is not None:
                        return candidate
                    if is_fresh_quote(quote, max_age_seconds=self._max_quote_age_seconds):
                        return candidate
                    if is_fresh_quote(quote, max_age_seconds=float('inf')):
                        if best is None or candidate[2] > best[2]:
                            best = candidate
            except Exception as exc:
                last_error = exc
                logger.debug('Tencent quote failed for %s via %s (%s): %s',
                             ticker, host, type(exc).__name__, exc)
        if best is not None:
            return best  # The caller records the real age and rejects stale ticks.
        if last_error is not None:
            raise last_error
        return None, "", ""

    async def _fetch_eastmoney_quote(self, ticker: str) -> tuple[Optional[float], str, str]:
        secid = self.eastmoney_secid(ticker)
        if not secid:
            return None, "", ""
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": secid, "fields": "f43,f57,f58,f59,f86"}
        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") or {}
            if payload.get("rc") != 0 or data.get("f57") != ticker:
                return None, "", ""
            raw, timestamp = data.get("f43"), data.get("f86")
            if raw in (None, "-", "") or type(timestamp) is not int or timestamp <= 0:
                return None, "", ""
            quoted = datetime.fromtimestamp(timestamp, MARKET_ZONE)
            name = str(data.get("f58") or "").strip()
            return self._parse_eastmoney_price(raw, data.get("f59"), ticker), name, quoted.isoformat()
        except Exception as exc:
            logger.debug("Eastmoney quote failed for %s (%s): %s", ticker, type(exc).__name__, exc)
            raise
        return None, "", ""

    @staticmethod
    def _parse_eastmoney_price(raw: object, precision: object = None, ticker: str = "") -> Optional[float]:
        """Convert Eastmoney scaled integer quote fields to a decimal price."""
        try:
            value = float(raw)
            if precision not in (None, "-", ""):
                decimals = int(precision)
                if decimals >= 0:
                    return value / (10 ** decimals)

            code = MarketDataService.normalize_ticker(ticker)
            decimals = 3 if is_etf_ticker(code) else 2
            return value / (10 ** decimals)
        except (TypeError, ValueError, OverflowError):
            return None

    async def _fetch_sina_quote(self, ticker: str) -> tuple[Optional[float], str, str]:
        """Sina realtime quote fallback.

        Sina's ``hq.sinajs.cn`` endpoint returns a GBK-encoded JS snippet
        like ``var hq_str_sh600519="贵州茅台,昨收,今开,最新,...,日期,时间,00;"``.
        The server requires a ``Referer: finance.sina.com.cn`` header or it
        returns 403, so we set it explicitly.
        """
        market = self.infer_market(ticker)
        if not market:
            return None, "", ""
        url = f"https://hq.sinajs.cn/list={market}{ticker}"
        try:
            resp = await self._client.get(
                url,
                headers={"Referer": "https://finance.sina.com.cn"},
            )
            resp.raise_for_status()
            body = resp.content.decode("gbk", errors="strict")
            # Body: var hq_str_sh600519="name,昨收,今开,最新价,最高,最低,...,日期,时间,00,";
            match = re.fullmatch(r'\s*var\s+hq_str_' + market + ticker + r'\s*=\s*"([^"]*)"\s*;\s*', body)
            if not match:
                return None, "", ""
            payload = match.group(1)
            parts = payload.split(",")
            if len(parts) < 32:
                return None, "", ""
            price = float(parts[3]) if parts[3] else None
            if price is None or price <= 0:
                return None, "", ""
            name = parts[0].strip()
            date_str, time_str = parts[30], parts[31]
            quoted = datetime.strptime(
                f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=MARKET_ZONE)
            return price, name, quoted.isoformat()
        except Exception as exc:
            logger.debug("Sina quote failed for %s (%s): %s", ticker, type(exc).__name__, exc)
            raise
        return None, "", ""

    async def _fetch_sina_ohlc(
        self, ticker: str, start_s: str, end_s: str
    ) -> Tuple[List[Dict], bool]:
        """Sina daily-K fallback. Returns ``(bars, permanent_failure)``.

        Sina's ``getKLineData`` endpoint only accepts ``datalen`` (number of
        most-recent bars) — there is no start/end parameter.  When the caller's
        requested window exceeds ``datalen`` bars we skip Sina entirely so the
        negative cache uses the right TTL instead of a false-positive miss.
        """
        market = self.infer_market(ticker)
        code = self.normalize_ticker(ticker)
        if not market or not code:
            return [], False
        try:
            start_day = datetime.strptime(start_s, "%Y%m%d").date()
            end_day = datetime.strptime(end_s, "%Y%m%d").date()
        except ValueError:
            return [], False
        # ~250 trading days/year; cap at 1000 (Sina's max datalen) and only
        # attempt when the window is short enough to be fully covered.
        calendar_days = (end_day - start_day).days
        if calendar_days > 1000:
            return [], False
        datalen = max(int(calendar_days * 1.5) + 10, 30)
        datalen = min(datalen, 1000)
        url = (
            "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "CN_MarketData.getKLineData"
        )
        try:
            resp = await self._client.get(
                url,
                params={
                    "symbol": f"{market}{code}",
                    "scale": "240",  # daily
                    "ma": "no",
                    "datalen": str(datalen),
                },
                headers={"Referer": "https://finance.sina.com.cn"},
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception as exc:
            logger.warning("Sina OHLC fetch failed for %s: %s", code, exc)
            return [], False
        bars: List[Dict] = []
        # start_s/end_s are YYYYMMDD; Sina's day is "YYYY-MM-DD".  Normalize
        # both sides to YYYYMMDD for lexicographic comparison.
        start_cmp = start_s.replace("-", "")
        end_cmp = end_s.replace("-", "")
        for row in rows or []:
            try:
                day = str(row["day"])[:10]
                day_cmp = day.replace("-", "")
                if day_cmp < start_cmp or day_cmp > end_cmp:
                    continue
                bars.append({
                    "date": day,
                    "open": float(row["open"]),
                    "close": float(row["close"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "volume": float(row.get("volume") or 0),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return bars, False

    async def get_ohlc(
        self,
        ticker: str,
        start: date | datetime | str,
        end: date | datetime | str = None,
        *, adjustment: str = "qfq",
    ) -> List[Dict]:
        """Fetch daily OHLC bars from Eastmoney, with AkShare as a fallback."""
        if adjustment not in {"qfq", "raw"}:
            raise ValueError("OHLC adjustment must be qfq or raw")
        secid = self.eastmoney_secid(ticker)
        if not secid:
            return []

        code = self.normalize_ticker(ticker)
        cache_key = code if adjustment == "qfq" else code + ":raw"
        cached = self._ohlc_negative_cache.get(cache_key)
        if cached:
            expiry, reason = cached
            if datetime.now().timestamp() < expiry:
                logger.debug(
                    "OHLC negative cache hit for %s: %s", code, reason
                )
                return []
            # expired entry; drop it so the next attempt actually fires.
            self._ohlc_negative_cache.pop(cache_key, None)

        self._ensure_client()
        start_s = self._format_date(start)
        end_s = self._format_date(end or datetime.now())
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "1" if adjustment == "qfq" else "0",
            "beg": start_s,
            "end": end_s,
        }
        if not self._eastmoney_ohlc_in_cooldown():
            # Retry with exponential backoff for transient connection issues
            max_retries = 3
            base_delay = 0.5
            for attempt in range(max_retries):
                try:
                    resp = await self._client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json().get("data") or {}
                    bars = []
                    for raw in data.get("klines") or []:
                        parts = raw.split(",")
                        if len(parts) < 6:
                            continue
                        bars.append({
                            "date": parts[0],
                            "open": float(parts[1]),
                            "close": float(parts[2]),
                            "high": float(parts[3]),
                            "low": float(parts[4]),
                            "volume": float(parts[5]),
                        })
                    if bars:
                        self._eastmoney_ohlc_failures = 0
                        self._eastmoney_ohlc_cooldown_until = None
                        return self._decorate_bars(bars, "eastmoney_ohlc_" + adjustment)
                    break
                except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError) as exc:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.debug(
                            "Eastmoney OHLC transient error for %s (attempt %d/%d), retrying in %.1fs: %s",
                            ticker, attempt + 1, max_retries, delay, exc
                        )
                        await asyncio.sleep(delay)
                        continue
                    self._record_eastmoney_ohlc_failure(ticker, exc)
                except Exception as exc:
                    self._record_eastmoney_ohlc_failure(ticker, exc)
                    break

        if adjustment == "qfq":
            tencent_bars, tencent_permanent = await self._fetch_tencent_ohlc(ticker, start_s, end_s)
        else:
            tencent_bars, tencent_permanent = await self._fetch_tencent_ohlc(
                ticker, start_s, end_s, adjustment="raw")
        if tencent_bars:
            return self._decorate_bars(tencent_bars, "tencent_ohlc_qfq")

        akshare_bars, akshare_permanent = (
            await self._fetch_akshare_ohlc(ticker, start_s, end_s)
            if adjustment == "qfq" else ([], False))
        if akshare_bars:
            return self._decorate_bars(akshare_bars, "akshare_ohlc_qfq")

        sina_bars, sina_permanent = await self._fetch_sina_ohlc(
            ticker, start_s, end_s
        )
        if sina_bars:
            return self._decorate_bars(sina_bars, "sina_ohlc")

        # All providers failed for this ticker — record in the negative cache
        # so repeat calls within the same sweep (and across callers) short-circuit.
        ttl = (
            self._ohlc_neg_cache_long_ttl
            if tencent_permanent or akshare_permanent or sina_permanent
            else self._ohlc_neg_cache_short_ttl
        )
        reason = "all_providers_failed"
        async with self._ohlc_negative_lock:
            self._ohlc_negative_cache[cache_key] = (
                datetime.now().timestamp() + ttl,
                reason,
            )
        logger.info(
            "OHLC negative cache set for %s (%s) ttl=%ss",
            code,
            reason,
            ttl,
        )
        return []

    async def get_unadjusted_ohlc(self, ticker, start, end=None):
        """Match historical validation barriers to unadjusted execution anchors."""
        return await self.get_ohlc(ticker, start, end, adjustment="raw")

    @staticmethod
    def _is_permanent_http_failure(exc: BaseException) -> bool:
        """Classify an exception as a permanent (vs transient) HTTP failure."""
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            if 400 <= status < 500 or status == 501:
                return True
        return False

    @staticmethod
    def _decorate_bars(bars: List[Dict], provider: str) -> List[Dict]:
        fetched_at = datetime.now().isoformat()
        # Sina's getKLineData returns raw (unadjusted) bars; all other
        # providers apply qfq.  Downstream code uses this field to decide
        # whether back-adjustment is needed when stitching windows.
        adjustment = "raw" if provider == "sina_ohlc" or provider.endswith("_raw") else "qfq"
        return [
            {
                **bar,
                "provider": ("tencent_ohlc_" + str(bar.get("adjustment") or adjustment)
                             if provider.startswith("tencent_ohlc") else provider),
                "fetched_at": fetched_at,
                "adjustment": str(bar.get("adjustment") or adjustment),
                "quality_status": "validated",
                "trade_status": str(bar.get("trade_status") or "normal"),
            }
            for bar in bars
        ]

    async def get_trading_calendar(
        self,
        start: date | datetime | str,
        end: date | datetime | str = None,
        *,
        index_identity: "InstrumentIdentity | None" = None,
    ) -> List[Dict[str, Any]]:
        """Return a verified annual calendar, or historical observed-open evidence.

        PR2.1: the calendar is sourced from an explicit index identity, not
        from the bare code ``"000001"``.  Missing bars for the index do NOT
        imply the market was closed — they only mean the index tape was
        unavailable for that session.  Callers that need a definitive
        open/closed decision must consult a dedicated calendar source; this
        method returns the observed-open sessions only and records the source
        and validity range so a downstream auditor can tell the difference.
        """
        start_day = datetime.strptime(self._format_date(start), '%Y%m%d').date()
        end_day = datetime.strptime(self._format_date(end or datetime.now(MARKET_ZONE)), '%Y%m%d').date()
        if start_day.year == end_day.year:
            calendar = self._get_exchange_calendar(start_day.year)
            if calendar is not None:
                return calendar.rows(start_day, end_day)

        from ..domain.portfolio.instruments import (
            SHANGHAI_COMPOSITE_INDEX,
            InstrumentIdentity,
        )

        identity = index_identity or SHANGHAI_COMPOSITE_INDEX
        if not isinstance(identity, InstrumentIdentity):
            identity = InstrumentIdentity(
                code=str(getattr(identity, "code", "000001")),
                market=str(getattr(identity, "market", "sh")),
                kind=str(getattr(identity, "kind", "index")),
            )
        end_value = end or datetime.now()
        # The Shanghai Composite Index trades under the SH secid prefix.
        bars = await self.get_ohlc(identity.code, start, end_value)
        fetched_at = datetime.now().isoformat()
        observed_open: list[dict[str, Any]] = []
        for bar in bars:
            observed_open.append(
                {
                    "trade_date": str(bar["date"])[:10],
                    "market": "CN",
                    "is_open": 1,
                    "source": str(bar.get("provider") or "market_data_service"),
                    "fetched_at": str(
                        bar.get("fetched_at") or fetched_at
                    ),
                    "quality_status": "validated",
                    "source_identity": identity.symbol,
                    "source_kind": identity.kind.value,
                    "validity_start": str(bar["date"])[:10] if bars else None,
                    "validity_end": str(bar["date"])[:10] if bars else None,
                    # A missing bar here does NOT mean the market was closed.
                    # Downstream code must not interpret absence as a holiday.
                    "inference_rule": "observed_open_only",
                }
            )
        if not observed_open:
            # Be explicit: the tape was empty.  This is not the same as "the
            # market was closed every day in the window".
            return [
                {
                    "trade_date": None,
                    "market": "CN",
                    "is_open": 0,
                    "source": "market_data_service",
                    "fetched_at": fetched_at,
                    "quality_status": "tape_unavailable",
                    "source_identity": identity.symbol,
                    "source_kind": identity.kind.value,
                    "validity_start": None,
                    "validity_end": None,
                    "inference_rule": "no_observation_no_inference",
                }
            ]
        # Bound the validity range so callers can tell which sessions were
        # actually observed versus which dates simply fell inside the request.
        dates = [row["trade_date"] for row in observed_open if row["trade_date"]]
        for row in observed_open:
            row["validity_start"] = min(dates) if dates else None
            row["validity_end"] = max(dates) if dates else None
        return observed_open

    def _eastmoney_ohlc_in_cooldown(self) -> bool:
        if not self._eastmoney_ohlc_cooldown_until:
            return False
        if datetime.now() < self._eastmoney_ohlc_cooldown_until:
            return True
        self._eastmoney_ohlc_cooldown_until = None
        self._eastmoney_ohlc_failures = 0
        logger.info("Eastmoney OHLC cooldown expired; retrying primary source")
        return False

    def _record_eastmoney_ohlc_failure(self, ticker: str, exc: Exception):
        self._eastmoney_ohlc_failures += 1
        if self._eastmoney_ohlc_failures >= self._eastmoney_ohlc_failure_threshold:
            if not self._eastmoney_ohlc_cooldown_until:
                self._eastmoney_ohlc_cooldown_until = datetime.now() + timedelta(seconds=self._eastmoney_ohlc_cooldown_seconds)
                logger.warning(
                    "Eastmoney OHLC unavailable after %s failures; cooling down for %ss and using fallbacks",
                    self._eastmoney_ohlc_failures,
                    self._eastmoney_ohlc_cooldown_seconds,
                )
            else:
                logger.debug("Eastmoney OHLC still unavailable for %s: %s", ticker, exc)
        else:
            logger.warning("Eastmoney OHLC fetch failed for %s: %s", ticker, exc)

    async def _fetch_tencent_ohlc(
        self, ticker: str, start_s: str, end_s: str, *, adjustment: str = "qfq"
    ) -> Tuple[List[Dict], bool]:
        """Fetch Tencent daily bars.

        Returns ``(bars, permanent_failure)``.  ``permanent_failure`` is True
        only when the server explicitly rejected the request shape (e.g.
        HTTP 501 for ``qfq`` on currency ETFs) AND the raw-bar fallback also
        failed — transient network errors stay False so the negative cache
        uses the short TTL.
        """
        market = self.infer_market(ticker)
        code = self.normalize_ticker(ticker)
        if not market or not code:
            return [], False

        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        try:
            start_day = datetime.strptime(start_s, "%Y%m%d").date()
            end_day = datetime.strptime(end_s, "%Y%m%d").date()
            if end_day < start_day:
                return [], False
            # Tencent caps one response at roughly 640 daily rows.  Chunking by
            # 700 calendar days keeps every request below that limit while
            # preserving the caller's full 2015-to-cutoff lifecycle.
            by_date: dict[str, Dict[str, Any]] = {}
            chunk_start = start_day
            qfq_rejected = False
            while chunk_start <= end_day:
                chunk_end = min(chunk_start + timedelta(days=699), end_day)
                bars_for_chunk, rejected = await self._tencent_fetch_chunk(
                    url, market, code, chunk_start, chunk_end, qfq=adjustment == "qfq"
                )
                if rejected:
                    qfq_rejected = True
                    # Retry the same chunk without qfq — Tencent 501s on the
                    # qfq param for currency/cross-border ETFs that have no
                    # dividend adjustments.  Raw bars are still valid.
                    bars_for_chunk, _ = await self._tencent_fetch_chunk(
                        url, market, code, chunk_start, chunk_end, qfq=False
                    )
                for bar in bars_for_chunk:
                    by_date[bar["date"]] = {**bar, "adjustment": (
                        "raw" if rejected or adjustment == "raw" else str(bar.get("adjustment") or "qfq"))}
                chunk_start = chunk_end + timedelta(days=1)
            if qfq_rejected and by_date:
                logger.info(
                    "Tencent qfq unavailable for %s, served raw bars", code
                )
            return [by_date[day] for day in sorted(by_date)], False
        except httpx.HTTPStatusError as exc:
            permanent = self._is_permanent_http_failure(exc)
            if permanent:
                logger.warning(
                    "Tencent OHLC permanently rejected for %s: %s",
                    code,
                    exc,
                )
            else:
                logger.warning(
                    "Tencent OHLC fetch failed for %s: %s", code, exc
                )
            return [], permanent
        except Exception as exc:
            logger.warning("Tencent OHLC fetch failed for %s: %s", code, exc)
            return [], False

    async def _tencent_fetch_chunk(
        self,
        url: str,
        market: str,
        code: str,
        chunk_start: date,
        chunk_end: date,
        *,
        qfq: bool = True,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch one Tencent chunk.  Returns ``(bars, qfq_rejected)``.

        On HTTP 501 with ``qfq=True`` the caller retries with ``qfq=False``.
        """
        param = (
            f"{market}{code},day,{chunk_start.isoformat()},"
            f"{chunk_end.isoformat()},640,qfq"
            if qfq
            else f"{market}{code},day,{chunk_start.isoformat()},"
            f"{chunk_end.isoformat()},640,"
        )
        params = {"param": param}
        response = None
        for attempt in range(3):
            try:
                response = await self._client.get(url, params=params)
                response.raise_for_status()
                break
            except httpx.HTTPStatusError:
                # 501 etc. — don't retry the same rejected shape.
                if qfq and response is not None and response.status_code == 501:
                    return [], True
                raise
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.2 * (attempt + 1))
        payload = response.json() if response is not None else {}
        rows = (payload.get("data") or {}).get(f"{market}{code}", {})
        raw_bars = rows.get("qfqday") or rows.get("day") or []
        bars: List[Dict[str, Any]] = []
        for parts in raw_bars:
            if len(parts) < 6:
                continue
            bars.append({
                "date": str(parts[0])[:10],
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]),
            })
        # If qfq was requested but only `day` (not `qfqday`) came back, the
        # server silently ignored qfq — flag so the caller can log + degrade.
        qfq_rejected = qfq and not rows.get("qfqday") and bool(rows.get("day"))
        for bar in bars:
            bar["adjustment"] = "qfq" if qfq and rows.get("qfqday") else "raw"
        return bars, qfq_rejected

    async def _fetch_akshare_ohlc(
        self, ticker: str, start_s: str, end_s: str
    ) -> Tuple[List[Dict], bool]:
        """Fetch daily bars through AkShare when the raw Eastmoney endpoint is unavailable.

        Returns ``(bars, permanent_failure)``.  Transient network errors are
        retried with exponential backoff; only truly permanent failures (e.g.
        akshare raising a parsing ``ValueError``) report ``True``.
        """
        code = self.normalize_ticker(ticker)
        if not code or not code.isdigit():
            return [], False

        try:
            bars = await asyncio.to_thread(
                self._fetch_akshare_ohlc_sync, code, start_s, end_s
            )
            return bars, False
        except _AkSharePermanentError as exc:
            logger.warning(
                "AkShare OHLC permanently failed for %s: %s", code, exc
            )
            return [], True
        except Exception as exc:
            logger.warning("AkShare OHLC fetch failed for %s: %s", code, exc)
            return [], False

    def _fetch_akshare_ohlc_sync(
        self, ticker: str, start_s: str, end_s: str
    ) -> List[Dict]:
        import akshare as ak

        def _fetch_df():
            if is_etf_ticker(ticker):
                return ak.fund_etf_hist_em(
                    symbol=ticker,
                    period="daily",
                    start_date=start_s,
                    end_date=end_s,
                    adjust="qfq",
                )
            return ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date=start_s,
                end_date=end_s,
                adjust="qfq",
            )

        # Only retry on transient network/protocol errors. Parsing errors or
        # akshare-level validation errors propagate as permanent failures.
        # Note: ``requests.exceptions.ConnectionError`` (used by akshare's
        # underlying HTTP stack) inherits from ``OSError``, NOT from the
        # builtin ``ConnectionError`` — so ``OSError`` is the right umbrella.
        transient_exceptions = (
            OSError,
            httpx.RemoteProtocolError,
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.ConnectTimeout,
        )
        try:
            df = sync_retry_with_backoff(
                _fetch_df,
                max_retries=3,
                base_delay=1.0,
                max_delay=8.0,
                exceptions=transient_exceptions,
            )
        except transient_exceptions:
            # Retries exhausted on transient errors — surface as transient so
            # the negative cache uses the short TTL.
            raise
        except Exception as exc:
            # Anything else (e.g. KeyError from a missing column, ValueError
            # from akshare's internal parsing) is permanent for this ticker.
            raise _AkSharePermanentError(str(exc)) from exc

        bars = []
        for row in df.to_dict("records"):
            try:
                bars.append({
                    "date": str(row["日期"])[:10],
                    "open": float(row["开盘"]),
                    "close": float(row["收盘"]),
                    "high": float(row["最高"]),
                    "low": float(row["最低"]),
                    "volume": float(row.get("成交量") or 0),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return bars

    @staticmethod
    def _format_date(value) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        if isinstance(value, str):
            return value[:10].replace("-", "")
        return (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")

    @staticmethod
    def _hyphen_date(value: str) -> str:
        value = str(value)
        if len(value) == 8 and value.isdigit():
            return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
        return value


_market_data: Optional[MarketDataService] = None


def get_market_data() -> MarketDataService:
    global _market_data
    if _market_data is None:
        _market_data = MarketDataService()
    return _market_data
