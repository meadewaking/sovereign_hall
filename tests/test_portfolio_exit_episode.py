"""Synthetic process regressions; these outcomes are not investment evidence."""
from datetime import datetime, timedelta
import importlib.util
from pathlib import Path
import sqlite3

import pytest

from sovereign_hall.core import PROJECT_ROOT
from sovereign_hall.services import market_data
from sovereign_hall.services.exchange_calendar import load_exchange_calendar
from sovereign_hall.services.market_state_policy import UNIVERSE
from test_portfolio_regime_experiment import frozen, FixtureMarket, ZONE


def cli():
    spec = importlib.util.spec_from_file_location("exit_episode_cli",
        PROJECT_ROOT / "scripts/continue_portfolio_exit_episode.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
async def episode(frozen, monkeypatch, tmp_path):
    prior, origin, _ = frozen
    monkeypatch.setattr(market_data, "MarketDataService", FixtureMarket)
    await prior.execute(origin, origin / "execution")
    result = prior.read(origin / "execution/result.json")
    # This entire prefix is synthetic. Place its observation on the prior
    # date to exercise resumption; never pass this fixture to a real account.
    result["observed_at"] = (datetime.now(ZONE) - timedelta(days=1)).isoformat()
    prior.write(origin / "execution/result.json", result)
    module = cli()
    directory = tmp_path / "episode"
    protocol = module.initialize(origin, directory)
    return module, directory, origin, protocol


class EpisodeMarket(FixtureMarket):
    price = .98

    async def get_current_quote(self, ticker):
        result = await super().get_current_quote(ticker)
        result["price"] = self.price
        return result


def fixture_capture(module, price, *, ordinary=False):
    async def capture(directory, packet, protocol, market):
        if not await market.is_trading_day() or not await market.is_market_open():
            raise RuntimeError("current verified open trading session required")
        cutoff = datetime.now(ZONE)
        cal = load_exchange_calendar(cutoff.year)
        days = sorted(d for d, opened in cal.sessions.items() if opened and d < cutoff.date().isoformat())[-70:]
        receipts = []
        (packet / "raw").mkdir()
        for ticker in UNIVERSE:
            symbol = ("sh" if ticker.startswith("5") else "sz") + ticker
            prices = [100+i if ordinary else 300-i for i in range(len(days))]
            rows = [[d, p, p, p+1, p-1, 1000] for d, p in zip(days, prices)]
            path = packet / "raw" / (ticker + ".json")
            module.write(path, dict(code=0, data={symbol: dict(qfqday=rows)}))
            receipts.append(dict(ticker=ticker, status=200, sha256=module.sha(path),
                requested_at=cutoff.isoformat(), captured_at=cutoff.isoformat(),
                url=f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,{days[0]},{days[-1]},200,qfq"))
        before = {arm: module.snapshot(directory / (arm + ".db")) for arm in module.ARMS}
        quotes = {"512800": dict(ticker="512800", price=price, source="sina_realtime_quote",
            quoted_at=cutoff.isoformat(), fetched_at=cutoff.isoformat())}
        module.write(packet / "input.json", dict(cutoff=datetime.now(ZONE).isoformat(), before=before,
            receipts=receipts, review_quotes=quotes, latest_completed_session=days[-1],
            calendar=dict(source_identity=cal.source_identity, sessions=cal.sessions), scope="synthetic fixture only"))
    return capture


@pytest.mark.asyncio
async def test_resume_keeps_actual_prefix_cost_and_recovery_opportunity_cost(episode):
    module, directory, origin, protocol = episode
    prefix = {a: module.snapshot(origin / "execution" / ("inherited_"+a+".db")) for a in module.ARMS}
    result = await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98))
    assert all(not a["new_trades"] for a in result["accounts"].values())
    f = result["feedback"]
    assert f["extra_candidate_paid_fees"] == pytest.approx(5.9)
    assert f["exposure_effect_before_extra_fees"] == pytest.approx(36)
    assert f["candidate_minus_control_value"] == pytest.approx(36-5.9)
    assert f["candidate_minus_control_return_from_opening_nav"] == pytest.approx((36-5.9)/protocol["opening_nav"])
    assert f["attributable_investment_return_improvement"] is None
    assert f["independent_episodes"] == 1
    assert f["sampled_risk"]["candidate"]["invested_ratio"] == 0
    assert f["sampled_risk"]["control"]["max_drawdown_at_observed_checkpoints"] < 0
    assert module.verify_books(directory)["checkpoints"] == 1
    assert module.replay(directory)["historical_fills"] == 0
    after = {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    assert (await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98)))["new_fills"] == 0
    assert after == {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    assert prefix == {a: module.snapshot(origin / "execution" / ("inherited_"+a+".db")) for a in module.ARMS}
    # A rising quote hurts the exited arm by exactly the actual carried shares.
    higher = {a: dict(result["accounts"][a]) for a in module.ARMS}
    higher["control"] = {**higher["control"], "assets": {**higher["control"]["assets"], "total_assets": result["accounts"]["control"]["assets"]["total_assets"]+72}}
    assert module.feedback(protocol, {"checkpoints": 1}, higher)["candidate_minus_control_value"] == pytest.approx(f["candidate_minus_control_value"]-72)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stale_source", "prefetched_cache"])
async def test_stale_exit_survives_restart_same_intent_then_closes_without_reentry(episode, failure):
    module, directory, _, _ = episode
    opening = module.snapshot(directory / "control.db")
    class Missing(EpisodeMarket):
        stale, price = failure == "stale_source", .85

        async def get_current_quote(self, ticker):
            quote = await super().get_current_quote(ticker)
            if failure == "prefetched_cache":
                quote["fetched_at"] = (datetime.now(ZONE)-timedelta(seconds=1)).isoformat()
            return quote
    first = await module.observe(directory, market=Missing(), capture_fn=fixture_capture(module, .85))
    assert not first["feedback"]["valuation_complete"]
    assert first["feedback"]["candidate_minus_control_value"] is None
    assert first["accounts"]["control"]["pending_exits"] == 1
    assert not first["accounts"]["control"]["new_trades"]
    if failure == "prefetched_cache":
        market = module.read(Path(module.read(directory / "status.json")["latest_packet"])/"execution_market.json")
        assert market["quotes"]["512800"] is None
        assert market["rejected_quotes"]["512800"]["reason"] == "retrieval_before_execution_request"
    before = module.snapshot(directory / "control.db")
    for table in ("simulation_trades", "simulation_ledger_entries", "quote_snapshots"):
        assert before["tables"][table] == opening["tables"][table]
    assert before["cash"] == opening["cash"]
    pending = [r for r in before["tables"]["execution_intents"] if r["idempotency_key"].startswith("exit_episode:")]
    assert len(pending) == 1 and pending[0]["status"] == "deferred"
    class Fresh(EpisodeMarket):
        price = .85
    # A new process retries the original durable sell; an ordinary regime
    # cannot create a buy in either arm of this finite exit episode.
    second = await module.observe(directory, market=Fresh(), capture_fn=fixture_capture(module, .85, ordinary=True))
    assert len(second["accounts"]["control"]["new_trades"]) == 1
    assert second["accounts"]["control"]["new_trades"][0]["intent_id"] == pending[0]["id"]
    assert module.verify_books(directory)["state"] == "closed"
    assert not second["accounts"]["candidate"]["new_trades"]
    assert module.replay(directory)["checkpoints"] == 2


@pytest.mark.asyncio
async def test_closed_session_does_not_admit_checkpoint_or_account_writes(episode):
    module, directory, _, _ = episode
    class Closed(EpisodeMarket):
        opened = False
    before = {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    with pytest.raises(RuntimeError, match="open trading session"):
        await module.observe(directory, market=Closed(), capture_fn=fixture_capture(module, .98))
    assert before == {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    assert not (directory / "active_observation.json").exists()


@pytest.mark.asyncio
async def test_failed_pair_blocks_continuation_and_never_reseeds(episode):
    module, directory, _, _ = episode
    with sqlite3.connect(directory / "control.db") as c:
        c.execute("CREATE TRIGGER synthetic_fail BEFORE INSERT ON simulation_trades BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END")
    before = module.snapshot(directory / "control.db")
    class Stop(EpisodeMarket):
        price = .85
    with pytest.raises((RuntimeError, ValueError), match=".*"):
        await module.observe(directory, market=Stop(), capture_fn=fixture_capture(module, .85))
    assert (directory / "active_observation.json").exists()
    after = module.snapshot(directory / "control.db")
    for table in ("simulation_trades", "simulation_ledger_entries", "quote_snapshots", "simulation_positions"):
        assert before["tables"][table] == after["tables"][table]
    with pytest.raises(RuntimeError, match="interrupted paired checkpoint"):
        await module.observe(directory, market=Stop(), capture_fn=fixture_capture(module, .85))


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["protocol", "source", "book", "interruption", "duplicate_owner"])
async def test_frozen_identity_book_and_owner_guards(episode, target):
    module, directory, _, _ = episode
    lock = None
    if target == "protocol":
        p = module.read(directory / "protocol.json"); p["simulation"]["max_daily_trades"] = 6
        module.write(directory / "protocol.json", p)
    elif target == "source":
        with (directory / "runtime/sovereign_hall/services/portfolio_regime_policy.py").open("a") as f:
            f.write("\n# synthetic tamper\n")
    elif target == "book":
        with sqlite3.connect(directory / "control.db") as c:
            c.execute("UPDATE system_stats SET value='20000' WHERE key='simulation_cash'")
    elif target == "interruption":
        module.write(directory / "active_observation.json", {"synthetic": True})
    else:
        import fcntl
        lock = (directory / "episode.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises((RuntimeError, BlockingIOError)):
            await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98))
    finally:
        if lock:
            lock.close()
    assert not (directory / "observations").exists()


def test_expired_and_future_common_accounts_are_not_readmitted():
    module = cli()
    with pytest.raises(RuntimeError, match="expired"):
        module.current_window((datetime.now(ZONE)-timedelta(minutes=3)).isoformat())
    with pytest.raises(RuntimeError, match="future"):
        module.current_window((datetime.now(ZONE)+timedelta(seconds=5)).isoformat())


@pytest.mark.asyncio
@pytest.mark.parametrize("access", ["sqlite", "file", "network"])
async def test_worker_denies_outside_access_before_financial_changes(episode, tmp_path, access):
    module, directory, _, _ = episode
    outside = tmp_path / "forbidden.db"
    payloads = {
        "sqlite": f"\nimport sqlite3\nsqlite3.connect({str(outside)!r})\n",
        "file": f"\nopen({str(outside)!r}, 'w').write('forbidden')\n",
        "network": "\nimport socket\nsocket.socket().connect(('127.0.0.1', 9))\n",
    }
    relative = "runtime/sovereign_hall/services/portfolio_regime_policy.py"
    path = directory / relative
    path.write_text(path.read_text()+payloads[access])
    # Intentionally sign this synthetic fault injection as fixture code, so
    # the real subprocess guard, not only manifest verification, is exercised.
    protocol = module.read(directory / "protocol.json")
    protocol["artifacts"][relative] = module.sha(path)
    module.write(directory / "protocol.json", protocol)
    module.write(directory / "identity.json", dict(protocol_sha256=module.digest(protocol)))
    before = {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    with pytest.raises(RuntimeError, match="worker ended"):
        await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98))
    assert not outside.exists()
    assert before == {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    stderr = next((directory / "observations").glob("*/worker.stderr.log")).read_text()
    assert "denied" in stderr


@pytest.mark.asyncio
async def test_explicit_driver_revision_preserves_books_protocol_and_legacy_block(episode):
    module, directory, _, _ = episode
    protocol = module.read(directory / "protocol.json")
    protocol.pop("driver_version")  # explicitly synthetic pre-repair metadata
    module.write(directory / "protocol.json", protocol)
    module.write(directory / "identity.json", dict(protocol_sha256=module.digest(protocol)))
    status = module.read(directory / "status.json")
    status.pop("driver_version")
    module.write(directory / "status.json", status)
    old_protocol = (directory / "protocol.json").read_bytes()
    before = {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    with pytest.raises(RuntimeError, match="explicit repair"):
        await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98))
    module.write(directory / "active_observation.json", dict(kind="quote_boundary_repair_pending",
        base_protocol_sha256=module.digest(protocol)))
    revision = module.repair_quote_boundary(directory)
    assert revision["strategy_and_books_unchanged"]
    assert before == {a: module.snapshot(directory / (a+".db")) for a in module.ARMS}
    assert (directory / "protocol.json").read_bytes() == old_protocol
    result = await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98))
    assert result["safety_revision_validation"] and result["driver_version"] == module.DRIVER_VERSION
    assert result["feedback"]["independent_episodes"] == 1
    assert module.read(directory / "active_observation.json")["kind"] == "driver_upgrade_required"
    assert module.replay(directory)["checkpoints"] == 1
    assert (await module.observe(directory, market=EpisodeMarket(), capture_fn=fixture_capture(module, .98)))["new_fills"] == 0


@pytest.mark.parametrize("offset", [-1, 0, 1, 3])
def test_execution_receipt_cannot_precede_request_or_follow_completion(offset):
    module = cli(); requested = datetime.now(ZONE)
    batch = dict(requested_at=requested.isoformat(), completed_at=(requested+timedelta(seconds=2)).isoformat(),
        quotes={"512800": {"fetched_at": (requested+timedelta(seconds=offset)).isoformat()}})
    admitted = module.admitted_quotes(batch)
    assert (admitted["quotes"]["512800"] is not None) == (0 <= offset <= 2)
