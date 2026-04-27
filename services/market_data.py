"""
Market data service.

Centralizes quote and daily OHLC retrieval so predictions, validation, and
simulation all share the same price source and never fall back to fake prices.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


class MarketDataError(RuntimeError):
    """Raised when market data cannot be fetched."""


class MarketDataService:
    """Small async client for A-share/ETF quotes and daily bars."""

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self._quote_cache: Dict[str, Tuple[float, datetime]] = {}
        self._quote_ttl_seconds = 60

    @staticmethod
    def normalize_ticker(ticker: str) -> str:
        ticker = str(ticker or "").strip().upper()
        if "." in ticker:
            ticker = ticker.split(".")[0]
        return ticker

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

    async def get_current_price(self, ticker: str) -> Optional[float]:
        """Return the latest quote, or None if no reliable quote is available."""
        code = self.normalize_ticker(ticker)
        if not code:
            return None

        cached = self._quote_cache.get(code)
        if cached and (datetime.now() - cached[1]).total_seconds() < self._quote_ttl_seconds:
            return cached[0]

        self._ensure_client()
        price = await self._fetch_tencent_quote(code)
        if price is None:
            price = await self._fetch_eastmoney_quote(code)

        if price is not None and price > 0:
            self._quote_cache[code] = (price, datetime.now())
            return price

        logger.warning("No market quote for %s", code)
        return None

    async def _fetch_tencent_quote(self, ticker: str) -> Optional[float]:
        market = self.infer_market(ticker)
        if not market:
            return None
        url = f"http://qt.gtimg.cn/q={market}{ticker}"
        try:
            resp = await self._client.get(url)
            if resp.status_code != 200 or "none_match" in resp.text:
                return None
            text = resp.content.decode("gbk", errors="ignore")
            parts = text.split("~")
            if len(parts) > 3 and parts[3]:
                return float(parts[3])
        except Exception as exc:
            logger.debug("Tencent quote failed for %s: %s", ticker, exc)
        return None

    async def _fetch_eastmoney_quote(self, ticker: str) -> Optional[float]:
        secid = self.eastmoney_secid(ticker)
        if not secid:
            return None
        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": secid, "fields": "f43,f57,f58"}
        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            raw = data.get("f43")
            if raw in (None, "-", ""):
                return None
            value = float(raw)
            return value / 100 if value > 1000 else value
        except Exception as exc:
            logger.debug("Eastmoney quote failed for %s: %s", ticker, exc)
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
                return bars
        except Exception as exc:
            logger.warning("Eastmoney OHLC fetch failed for %s: %s", ticker, exc)

        bars = await self._fetch_tencent_ohlc(ticker, start_s, end_s)
        if bars:
            return bars

        return await self._fetch_akshare_ohlc(ticker, start_s, end_s)

    async def _fetch_tencent_ohlc(self, ticker: str, start_s: str, end_s: str) -> List[Dict]:
        market = self.infer_market(ticker)
        code = self.normalize_ticker(ticker)
        if not market or not code:
            return []

        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {"param": f"{market}{code},day,{self._hyphen_date(start_s)},{self._hyphen_date(end_s)},640,qfq"}
        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            rows = (payload.get("data") or {}).get(f"{market}{code}", {})
            raw_bars = rows.get("qfqday") or rows.get("day") or []
            bars = []
            for parts in raw_bars:
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
            return bars
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

        if self._is_etf(ticker):
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
    def _is_etf(ticker: str) -> bool:
        return ticker.startswith(("159", "510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588"))

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
