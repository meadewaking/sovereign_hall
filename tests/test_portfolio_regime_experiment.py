"""Process safety tests with explicitly synthetic quotes and account fixtures."""
import asyncio
from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import pytest

from sovereign_hall.core import PROJECT_ROOT
from sovereign_hall.core.config import get_config
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.exchange_calendar import load_exchange_calendar
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.market_state_policy import UNIVERSE
from sovereign_hall.services.portfolio_regime_policy import build_review
from sovereign_hall.services import market_data

ZONE = ZoneInfo("Asia/Shanghai")


def cli():
    spec = importlib.util.spec_from_file_location("portfolio_regime_process_cli",
        PROJECT_ROOT / "scripts/run_portfolio_regime_experiment.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FixtureMarket:
    opened, stale = True, False
    last_quote_attempts = {"scope": "synthetic test only"}

    def __init__(self, **kwargs):
        pass

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.opened

    async def get_current_quote(self, ticker):
        now = datetime.now(ZONE)
        return dict(ticker=ticker, price=1.0, source="sina_realtime_quote",
            fetched_at=now.isoformat(), quoted_at=(now - timedelta(minutes=5) if self.stale else now).isoformat())

    async def close(self):
        pass


@pytest.fixture
async def frozen(tmp_path, request):
    module = cli()
    cases = {}
    for name in ("inherited", "cash"):
        db = DatabaseService(str(tmp_path / (name + ".db")))
        await db._init_db()
        sim = InvestmentSimulation(db, FixtureMarket())
        try:
            await sim.initialize()
            await db._connection.execute("INSERT OR REPLACE INTO system_stats(key,value) VALUES('simulation_cash','10000')")
            await db._connection.commit()
            if name == "inherited":
                intent = await sim.create_execution_intent(ticker="512800", direction="long",
                    target_position=.18, confidence=1, reason="synthetic test seed", idempotency_key="seed")
                outcome = await sim.execute_intent(intent)
                assert outcome["action"] == "buy", outcome
                condition = getattr(request.node, "callspec", None)
                if condition and condition.params.get("condition") == "daily_limit":
                    for ticker in ("510300", "510500", "159915", "512000"):
                        intent = await sim.create_execution_intent(ticker=ticker, direction="long",
                            target_position=.05, confidence=1, reason="synthetic daily gate seed",
                            idempotency_key="seed_" + ticker)
                        assert (await sim.execute_intent(intent))["action"] == "buy"
        finally:
            await db.close()
        cases[name] = module.snapshot_account(tmp_path / (name + ".db"))
    run = tmp_path / "experiment"
    run.mkdir()
    (run / "raw").mkdir()
    market = FixtureMarket()
    quote = await market.get_current_quote("512800")
    cutoff = datetime.now(ZONE).isoformat()
    cal = load_exchange_calendar(int(cutoff[:4]))
    days = sorted(day for day, opened in cal.sessions.items() if opened and day < cutoff[:10])[-70:]
    receipts = []
    for ticker in UNIVERSE:
        symbol = ("sh" if ticker.startswith("5") else "sz") + ticker
        rows = [[day, 300-i, 300-i, 301-i, 299-i, 1000] for i, day in enumerate(days)]
        path = run / "raw" / (ticker + ".json")
        module.write(path, dict(code=0, data={symbol: dict(qfqday=rows)}))
        receipts.append(dict(ticker=ticker, status=200, sha256=module.sha(path),
            requested_at=quote["fetched_at"], captured_at=quote["fetched_at"],
            url=f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,{days[0]},{days[-1]},200,qfq"))
    for case in cases.values():
        case["state"] = dict(cutoff=cutoff, cash=case["cash"], positions=case["positions"],
            quotes={"512800": quote}, policy=dict(get_config().get("simulation", {})),
            latest_completed_session=days[-1])
    module.write(run / "input.json", dict(cutoff=cutoff, cases=cases, receipts=receipts,
        calendar=dict(sessions=cal.sessions), scope="synthetic integration test"))
    data, tapes = module.parse_inputs(run)
    module.write(run / "plans.json", {name: build_review(tapes, case["state"]) for name, case in cases.items()})
    from sovereign_hall.services.policy_snapshot_service import runtime_code_artifacts
    for relative in [*runtime_code_artifacts(PROJECT_ROOT, "run_discussion.py"), "config.yaml"]:
        target = run / "runtime/sovereign_hall" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PROJECT_ROOT / relative, target)
    shutil.copyfile(PROJECT_ROOT / "scripts/run_portfolio_regime_experiment.py", run / "worker.py")
    artifacts = {str(path.relative_to(run)): module.sha(path) for path in run.rglob("*") if path.is_file()}
    module.write(run / "protocol.json", dict(experiment_id="synthetic_portfolio_regression",
        artifacts=artifacts, worker_sha256=module.sha(run / "worker.py")))
    return module, run, cases


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", ["fresh", "stale", "closed", "daily_limit"])
async def test_real_process_intent_requote_atomic_ledger_idempotence_and_gates(frozen, monkeypatch, condition):
    module, run, cases = frozen
    class ExecutionFixture(FixtureMarket):
        opened = condition != "closed"
        stale = condition == "stale"
    monkeypatch.setattr(market_data, "MarketDataService", ExecutionFixture)
    before = {name: module.snapshot_account(case["source"])["tables"] for name, case in cases.items()}
    output = run / "execution"
    await module.execute(run, output)
    result = module.read(output / "result.json")
    account = result["accounts"]["inherited_candidate"]
    assert account["added_fills"] == (1 if condition == "fresh" else 0)
    assert account["repeat_idempotent"] and account["balance"]["status"] == "balanced_since_checkpoint"
    if condition == "fresh":
        assert account["added_fees"] == pytest.approx(5.9)
        assert not account["assets"]["positions"]
        control = result["accounts"]["inherited_control"]
        assert control["assets"]["total_assets"] - account["assets"]["total_assets"] == pytest.approx(5.9)
    else:
        assert account["added_fees"] == 0
        assert account["assets"]["positions"]["512800"]["shares"] == 1800
    assert all(not result["accounts"][name]["added_fills"]
               for name in ("inherited_control", "cash_control", "cash_candidate"))
    batch = module.read(output / "execution_market.json")
    prepared = module.read(output / "prepared.json")
    created = datetime.fromisoformat(prepared["inherited_candidate"][0]["created_at"]).replace(tzinfo=ZONE)
    assert datetime.fromisoformat(batch["requested_at"]) > created
    assert {name: module.snapshot_account(case["source"])["tables"] for name, case in cases.items()} == before
    with pytest.raises(FileExistsError):
        await module.execute(run, output)


@pytest.mark.asyncio
async def test_offline_replay_never_reexecutes_preserved_prices(frozen, monkeypatch):
    module, run, _ = frozen
    def deny(*args, **kwargs):
        raise AssertionError("replay must not request a market client")
    monkeypatch.setattr(market_data, "MarketDataService", deny)
    module.replay(run, run / "replay")
    result = module.read(run / "replay/result.json")
    assert result["historical_fills"] == result["database_writes"] == result["network_calls"] == 0
    with pytest.raises(FileExistsError):
        module.replay(run, run / "replay")


@pytest.mark.asyncio
async def test_export_denies_active_unresolved_intents(frozen):
    module, _, cases = frozen
    import sqlite3
    source = cases["cash"]["source"]
    with sqlite3.connect(source) as conn:
        conn.execute("INSERT INTO execution_intents(id,ticker,direction,target_position,reason,status,idempotency_key,created_at,updated_at) "
            "VALUES('unresolved','512800','sell',0,'synthetic','pending','pending_fixture','now','now')")
    with pytest.raises(RuntimeError, match="active intents"):
        module.snapshot_account(source)


@pytest.mark.asyncio
async def test_candidate_fill_failure_rolls_back_quote_fee_cash_position_and_intent(frozen):
    module, run, cases = frozen
    import sqlite3
    output = run / "failure_execution"
    process = await asyncio.create_subprocess_exec(
        __import__("sys").executable, "-B", str(run / "worker.py"), "--worker", str(run), str(output),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        prepared = json.loads(await asyncio.wait_for(process.stdout.readline(), 30))
        assert prepared["kind"] == "prepared"
        path = output / "inherited_candidate.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TRIGGER fail_atomic_fee BEFORE INSERT ON simulation_ledger_entries "
                         "BEGIN SELECT RAISE(FAIL,'synthetic atomic rollback'); END")
        quote = await FixtureMarket().get_current_quote("512800")
        batch = dict(requested_at=datetime.now(ZONE).isoformat(), quotes={"512800": quote},
                     trading_day=True, market_open=True)
        process.stdin.write((json.dumps(batch) + "\n").encode())
        await process.stdin.drain()
        process.stdin.close()
        await asyncio.wait_for(process.wait(), 30)
        errors = (await process.stderr.read()).decode()
        assert process.returncode and "synthetic atomic rollback" in errors
        tables = cases["inherited"]["tables"]
        with sqlite3.connect(path) as conn:
            for table in ("simulation_trades", "quote_snapshots", "simulation_ledger_entries"):
                assert conn.execute('SELECT count(*) FROM "' + table + '"').fetchone()[0] == len(tables[table])
            assert conn.execute("SELECT shares FROM simulation_positions WHERE ticker='512800'").fetchone()[0] == 1800
            cash = float(conn.execute("SELECT value FROM system_stats WHERE key='simulation_cash'").fetchone()[0])
            assert cash == cases["inherited"]["cash"]
            identity = prepared["intents"]["inherited_candidate"][0]["id"]
            assert conn.execute("SELECT executed_at FROM execution_intents WHERE id=?", (identity,)).fetchone()[0] is None
        assert not (output / "result.json").exists()
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("attack", ["sqlite", "network"])
async def test_worker_denies_access_outside_private_books_before_imported_code_runs(frozen, attack):
    module, run, cases = frozen
    source = cases["cash"]["source"]
    before = module.snapshot_account(source)["tables"]
    path = run / "runtime/sovereign_hall/services/portfolio_regime_policy.py"
    injected = ("\nimport sqlite3\nsqlite3.connect(" + repr(source) + ")\n" if attack == "sqlite" else
                "\nimport socket\nsocket.create_connection(('127.0.0.1',9),timeout=.1)\n")
    path.write_text(path.read_text() + injected)
    protocol = module.read(run / "protocol.json")
    protocol["artifacts"][str(path.relative_to(run))] = module.sha(path)
    module.write(run / "protocol.json", protocol)
    process = await asyncio.create_subprocess_exec(__import__("sys").executable, "-B", str(run / "worker.py"),
        "--worker", str(run), str(run / "denied"), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode and not stdout
    message = stderr.decode()
    assert ("SQLite access outside arm denied" if attack == "sqlite" else "external access denied") in message
    assert module.snapshot_account(source)["tables"] == before
