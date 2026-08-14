"""
Market data service.

Centralizes quote and daily OHLC retrieval so predictions, validation, and
simulation all share the same price source and never fall back to fake prices.
"""

import asyncio
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import httpx

from ..domain.portfolio.instruments import is_etf_ticker, normalize_ticker

logger = logging.getLogger(__name__)


class MarketDataError(RuntimeError):
    """Raised when market data cannot be fetched."""


class MarketDataService:
    """Small async client for A-share/ETF quotes and daily bars."""

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self._quote_cache: Dict[str, Dict[str, Any]] = {}
        self._quote_ttl_seconds = 60
        self._eastmoney_ohlc_failures = 0
        self._eastmoney_ohlc_cooldown_until: Optional[datetime] = None
        self._eastmoney_ohlc_failure_threshold = 3
        self._eastmoney_ohlc_cooldown_seconds = 300

    @staticmethod
    def normalize_ticker(ticker: str) -> str:
        return normalize_ticker(ticker)

    @classmethod
    def is_supported_ticker(cls, ticker: str) -> bool:
        """Return whether ``ticker`` can identify an A-share/ETF quote.

        Proposal prompts contain human-readable placeholders such as
        ``推荐标的代码``.  Treating those as symbols pollutes committee memory and
        needlessly reaches quote providers, so validation lives at the shared
        market-data boundary rather than in prompt-only cleanup.
        """
        code = cls.normalize_ticker(ticker)
        return len(code) == 6 and code.isdigit()

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
            self._client = httpx.AsyncClient(timeout=self.timeout, follow_redirects=True)

    async def is_trading_day(self, when: Optional[date | datetime] = None) -> bool:
        """Return whether the exchange is open on the given date.

        Falls back to weekday logic when the external trading calendar cannot be loaded.
        """
        target = when.date() if isinstance(when, datetime) else (when or datetime.now().date())
        if not isinstance(target, date):
            target = datetime.now().date()

        if target.weekday() >= 5:
            return False

        trade_days = await self._load_trade_days()
        if trade_days is None:
            return True
        return target in trade_days

    async def is_market_open(self, when: Optional[datetime] = None) -> bool:
        """Return whether A-share continuous trading is currently open."""
        current = when or datetime.now()
        if not await self.is_trading_day(current):
            return False
        minutes = current.hour * 60 + current.minute
        return (9 * 60 + 30 <= minutes <= 11 * 60 + 30) or (
            13 * 60 <= minutes <= 15 * 60
        )

    async def _load_trade_days(self) -> Optional[Set[date]]:
        global _trade_days_cache
        if _trade_days_cache is not None:
            return _trade_days_cache

        # The optional akshare stack is not required for realtime quotes and can be
        # binary-incompatible with the active numpy runtime. Avoid importing it in
        # the normal simulation path; explicit opt-in keeps the failure contained.
        use_akshare = os.environ.get("SOVEREIGN_HALL_USE_AKSHARE_CALENDAR", "0").strip().lower()
        if use_akshare not in {"1", "true", "yes", "on"}:
            return None

        try:
            import akshare as ak

            df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
            days = set()
            for value in df.get("trade_date", []):
                if hasattr(value, "to_pydatetime"):
                    value = value.to_pydatetime()
                if isinstance(value, datetime):
                    days.add(value.date())
                else:
                    days.add(datetime.strptime(str(value)[:10], "%Y-%m-%d").date())
            _trade_days_cache = days
            return _trade_days_cache
        except Exception as exc:
            logger.warning("Trading calendar unavailable, falling back to weekday check: %s", exc)
            return None

    async def get_current_price(self, ticker: str) -> Optional[float]:
        """Return the latest realtime quote price, or None when unavailable."""
        quote = await self.get_current_quote(ticker)
        return float(quote["price"]) if quote else None

    async def get_current_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Return a realtime quote with provider and retrieval timestamp."""
        code = self.normalize_ticker(ticker)
        if not self.is_supported_ticker(code):
            logger.warning("Reject unsupported realtime quote ticker: %r", ticker)
            return None

        cached = self._quote_cache.get(code)
        if cached:
            cached_at = cached.get("fetched_at_datetime")
            if isinstance(cached_at, datetime) and (
                datetime.now() - cached_at
            ).total_seconds() < self._quote_ttl_seconds:
                return {key: value for key, value in cached.items() if key != "fetched_at_datetime"}

        self._ensure_client()
        price, name = await self._fetch_tencent_quote(code)
        source = "tencent_realtime_quote"
        if price is None:
            price, name = await self._fetch_eastmoney_quote(code)
            source = "eastmoney_realtime_quote"

        if price is not None and price > 0:
            fetched_at = datetime.now()
            quote = {
                "ticker": code,
                "price": float(price),
                "name": name or "",
                "source": source,
                "fetched_at": fetched_at.isoformat(),
            }
            self._quote_cache[code] = {**quote, "fetched_at_datetime": fetched_at}
            return quote

        logger.warning("No market quote for %s", code)
        return None

    async def _fetch_tencent_quote(self, ticker: str) -> tuple[Optional[float], str]:
        market = self.infer_market(ticker)
        if not market:
            return None, ""
        url = f"http://qt.gtimg.cn/q={market}{ticker}"
        try:
            resp = await self._client.get(url)
            if resp.status_code != 200 or "none_match" in resp.text:
                return None, ""
            text = resp.content.decode("gbk", errors="ignore")
            parts = text.split("~")
            name = parts[1].strip() if len(parts) > 1 else ""
            if len(parts) > 3 and parts[3]:
                return float(parts[3]), name
        except Exception as exc:
            logger.debug("Tencent quote failed for %s: %s", ticker, exc)
        return None, ""

    async def _fetch_eastmoney_quote(self, ticker: str) -> tuple[Optional[float], str]:
        secid = self.eastmoney_secid(ticker)
        if not secid:
            return None, ""
        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": secid, "fields": "f43,f57,f58,f59"}
        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            raw = data.get("f43")
            if raw in (None, "-", ""):
                return None, ""
            name = str(data.get("f58") or "").strip()
            return self._parse_eastmoney_price(raw, data.get("f59"), ticker), name
        except Exception as exc:
            logger.debug("Eastmoney quote failed for %s: %s", ticker, exc)
        return None, ""

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

    async def get_ohlc(
        self,
        ticker: str,
        start: date | datetime | str,
        end: date | datetime | str = None,
    ) -> List[Dict]:
        """Fetch daily OHLC bars from Eastmoney, with AkShare as a fallback."""
        secid = self.eastmoney_secid(ticker)
        if not secid:
            return []

        self._ensure_client()
        start_s = self._format_date(start)
        end_s = self._format_date(end or datetime.now())
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "1",
            "beg": start_s,
            "end": end_s,
        }
        if not self._eastmoney_ohlc_in_cooldown():
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
                    return self._decorate_bars(bars, "eastmoney_ohlc_qfq")
            except Exception as exc:
                self._record_eastmoney_ohlc_failure(ticker, exc)

        bars = await self._fetch_tencent_ohlc(ticker, start_s, end_s)
        if bars:
            return self._decorate_bars(bars, "tencent_ohlc_qfq")

        bars = await self._fetch_akshare_ohlc(ticker, start_s, end_s)
        return self._decorate_bars(bars, "akshare_ohlc_qfq") if bars else []

    @staticmethod
    def _decorate_bars(bars: List[Dict], provider: str) -> List[Dict]:
        fetched_at = datetime.now().isoformat()
        return [
            {
                **bar,
                "provider": provider,
                "fetched_at": fetched_at,
                "adjustment": "qfq",
                "quality_status": "validated",
                "trade_status": str(bar.get("trade_status") or "normal"),
            }
            for bar in bars
        ]

    async def get_trading_calendar(
        self,
        start: date | datetime | str,
        end: date | datetime | str = None,
    ) -> List[Dict[str, Any]]:
        """Return an auditable open-session calendar from an index price tape."""
        bars = await self.get_ohlc("000001", start, end or datetime.now())
        return [
            {
                "trade_date": str(bar["date"])[:10],
                "market": "CN",
                "is_open": 1,
                "source": str(bar.get("provider") or "market_data_service"),
                "fetched_at": str(bar.get("fetched_at") or datetime.now().isoformat()),
                "quality_status": "validated",
            }
            for bar in bars
        ]

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

    async def _fetch_tencent_ohlc(self, ticker: str, start_s: str, end_s: str) -> List[Dict]:
        market = self.infer_market(ticker)
        code = self.normalize_ticker(ticker)
        if not market or not code:
            return []

        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        try:
            start_day = datetime.strptime(start_s, "%Y%m%d").date()
            end_day = datetime.strptime(end_s, "%Y%m%d").date()
            if end_day < start_day:
                return []
            # Tencent caps one response at roughly 640 daily rows.  Chunking by
            # 700 calendar days keeps every request below that limit while
            # preserving the caller's full 2015-to-cutoff lifecycle.
            by_date: dict[str, Dict[str, Any]] = {}
            chunk_start = start_day
            while chunk_start <= end_day:
                chunk_end = min(chunk_start + timedelta(days=699), end_day)
                params = {
                    "param": (
                        f"{market}{code},day,{chunk_start.isoformat()},"
                        f"{chunk_end.isoformat()},640,qfq"
                    )
                }
                response = None
                for attempt in range(3):
                    try:
                        response = await self._client.get(url, params=params)
                        response.raise_for_status()
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        await asyncio.sleep(0.2 * (attempt + 1))
                payload = response.json() if response is not None else {}
                rows = (payload.get("data") or {}).get(f"{market}{code}", {})
                raw_bars = rows.get("qfqday") or rows.get("day") or []
                for parts in raw_bars:
                    if len(parts) < 6:
                        continue
                    by_date[str(parts[0])[:10]] = {
                        "date": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                    }
                chunk_start = chunk_end + timedelta(days=1)
            return [by_date[day] for day in sorted(by_date)]
        except Exception as exc:
            logger.warning("Tencent OHLC fetch failed for %s: %s", code, exc)
            return []

    async def _fetch_akshare_ohlc(self, ticker: str, start_s: str, end_s: str) -> List[Dict]:
        """Fetch daily bars through AkShare when the raw Eastmoney endpoint is unavailable."""
        code = self.normalize_ticker(ticker)
        if not code or not code.isdigit():
            return []

        try:
            return await asyncio.to_thread(self._fetch_akshare_ohlc_sync, code, start_s, end_s)
        except Exception as exc:
            logger.warning("AkShare OHLC fetch failed for %s: %s", code, exc)
            return []

    def _fetch_akshare_ohlc_sync(self, ticker: str, start_s: str, end_s: str) -> List[Dict]:
        import akshare as ak

        if is_etf_ticker(ticker):
            df = ak.fund_etf_hist_em(
                symbol=ticker,
                period="daily",
                start_date=start_s,
                end_date=end_s,
                adjust="qfq",
            )
        else:
            df = ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date=start_s,
                end_date=end_s,
                adjust="qfq",
            )

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
_trade_days_cache: Optional[Set[date]] = None


def get_market_data() -> MarketDataService:
    global _market_data
    if _market_data is None:
        _market_data = MarketDataService()
    return _market_data
