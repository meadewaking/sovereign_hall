"""Tests for PR2.1 — instrument identity and trading calendar source."""

from __future__ import annotations

from datetime import date

import aiosqlite
import pytest

from sovereign_hall.domain.portfolio.instruments import (
    CSI_300_INDEX,
    SHANGHAI_COMPOSITE_INDEX,
    SHENZHEN_COMPONENT_INDEX,
    InstrumentIdentity,
    InstrumentKind,
    InstrumentProfile,
    Market,
    classify_instrument,
    infer_market_for_code,
)
from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations


def test_instrument_identity_disambiguates_stock_from_index():
    # ``000001`` is ambiguous: Ping An Bank on Shenzhen, Shanghai Composite on
    # Shanghai.  The identity must record both the market and the kind.
    ping_an = InstrumentIdentity(
        code="000001", market=Market.SZ, kind=InstrumentKind.A_SHARE
    )
    composite = InstrumentIdentity(
        code="000001", market=Market.SH, kind=InstrumentKind.INDEX
    )
    assert ping_an.symbol == "SH000001" or ping_an.symbol == "SZ000001"
    assert ping_an.symbol == "SZ000001"
    assert composite.symbol == "SH000001"
    assert ping_an != composite


def test_instrument_identity_parse_handles_suffix_and_market_prefix():
    parsed = InstrumentIdentity.parse("SH000300")
    assert parsed.code == "000300"
    assert parsed.market is Market.SH
    parsed_sz = InstrumentIdentity.parse("sz.000001")
    assert parsed_sz.code == "000001"
    assert parsed_sz.market is Market.SZ


def test_infer_market_for_code_does_not_treat_000001_as_shanghai_stock():
    # ``000001`` is a Shenzhen-listed A-share; only the *index* of the same
    # code belongs on Shanghai.  The helper must keep the share on SZ.
    assert infer_market_for_code("000001") is Market.SZ
    assert infer_market_for_code("600519") is Market.SH
    assert infer_market_for_code("159915") is Market.SZ


def test_canonical_index_identities_are_explicit():
    # The canonical reference identities must carry kind=INDEX so callers
    # cannot accidentally use them as shares.
    assert SHANGHAI_COMPOSITE_INDEX.kind is InstrumentKind.INDEX
    assert SHANGHAI_COMPOSITE_INDEX.market is Market.SH
    assert SHENZHEN_COMPONENT_INDEX.kind is InstrumentKind.INDEX
    assert CSI_300_INDEX.kind is InstrumentKind.INDEX


def test_instrument_profile_defaults_market_and_kind_from_code():
    profile = InstrumentProfile.from_ticker("600519")
    assert profile.market is Market.SH
    assert profile.kind is InstrumentKind.A_SHARE

    etf = InstrumentProfile.from_ticker("510300")
    assert etf.kind is InstrumentKind.ETF


def test_instrument_profile_accepts_explicit_trading_rules():
    # PR2.2: a single lot_size cannot express minimum first-buy size and
    # incremental step separately.  Both must be storable on the profile.
    profile = InstrumentProfile.from_ticker(
        "688981",
        lot_size=1,
        min_first_buy_size=200,
        incremental_step=1,
        settlement="T+1",
        trading_venue="star_market",
    )
    assert profile.min_first_buy_size == 200
    assert profile.incremental_step == 1
    assert profile.settlement == "T+1"
    assert profile.trading_venue == "star_market"


@pytest.mark.asyncio
async def test_apply_architecture_migrations_extends_instrument_and_calendar(
    tmp_path,
):
    db_path = tmp_path / "instrument.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        async with conn.execute("PRAGMA table_info(instrument_master)") as cursor:
            instrument_cols = {str(row[1]) for row in await cursor.fetchall()}
        async with conn.execute("PRAGMA table_info(trading_calendar)") as cursor:
            calendar_cols = {str(row[1]) for row in await cursor.fetchall()}
    assert {"market", "code", "kind", "min_first_buy_size",
            "incremental_step", "sell_odd_lot_rule", "settlement",
            "trading_venue"} <= instrument_cols
    assert {"source_identity", "source_kind", "validity_start",
            "validity_end", "inference_rule"} <= calendar_cols
