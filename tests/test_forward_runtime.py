"""Process integration regressions. All decisions, funds and quotes are fixtures."""
import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
import shutil
import sqlite3

import pytest

from sovereign_hall.core import PROJECT_ROOT
from sovereign_hall.core.config import get_config
from sovereign_hall.services.continuous_evaluation import ContinuousEvaluation
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.services.forward_runtime import FrozenForwardEvaluation
from sovereign_hall.services.investment_simulation import InvestmentSimulation
from sovereign_hall.services.policy_snapshot_service import runtime_code_artifacts


class ControlledFixture:
    opened = False
    stale = False

    async def is_trading_day(self):
        return True

    async def is_market_open(self):
        return self.opened

    async def get_current_quote(self, ticker):
        when = datetime.now() - (timedelta(hours=1) if self.stale else timedelta())
        return {"ticker": ticker, "price": 1, "source": "runtime_fixture_realtime",
                "quoted_at": when.isoformat(), "fetched_at": datetime.now().isoformat()}

    async def get_unadjusted_ohlc(self, ticker, start, end):
        return []  # Never turn missing data into a return observation.


def financial_state(directory):
    result = {}
    for name in ("control", "candidate", "stress", "benchmark"):
        with sqlite3.connect((directory / (name + ".db")).as_uri() + "?mode=ro", uri=True) as conn:
            result[name] = {
                table: conn.execute("SELECT * FROM " + table + " ORDER BY 1").fetchall()
                for table in ("simulation_trades", "simulation_ledger_entries", "execution_intents", "quote_snapshots")}
            result[name]["cash"] = conn.execute("SELECT value FROM system_stats WHERE key='simulation_cash'").fetchone()
    return result


@pytest.fixture
async def runtime(tmp_path):
    source = tmp_path / "producer"
    for relative in runtime_code_artifacts(PROJECT_ROOT, "run_discussion.py"):
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PROJECT_ROOT / relative, target)
    db = DatabaseService(str(tmp_path / "production.db"))
    await db._init_db()
    market = ControlledFixture()
    account = InvestmentSimulation(db, market)
    await account.initialize()
    policy = dict(get_config().get("simulation", {}))
    service = FrozenForwardEvaluation(db, market, root=tmp_path / "forward", policy=policy, source_root=source)
    try:
        await service.initialize()
        yield db, market, service, source
    finally:
        await service.close()
        await db.close()


@pytest.mark.asyncio
async def test_frozen_engine_retains_books_and_identity_after_producer_changes_and_restart(runtime):
    db, market, service, source = runtime
    decision = dict(ticker="588170", direction="long", confidence=.58, target_position=.20, decision_id="d1")
    await service.capture_round("fixture_round", [decision])
    market.opened = True
    await service.tick()
    before = financial_state(service.directory)
    assert before["control"]["simulation_trades"][0][3] == 5000
    assert before["candidate"]["simulation_trades"][0][3] == 1000
    assert (await (await db._connection.execute("SELECT count(*) FROM simulation_trades")).fetchone())[0] == 0
    identity, started = service.experiment_id, service.started_at
    protocol = (service.directory / "protocol.json").read_bytes()
    await service.close()
    (source / "services/investment_simulation.py").write_text("raise RuntimeError('changed producer engine must not load')\n")
    (source / "run_discussion.py").write_text("# changed producer\n")
    replacement = FrozenForwardEvaluation(db, market, root=service.root,
        policy={**service.policy, "max_single_position": .01}, source_root=source)
    try:
        await replacement.initialize()
        assert (replacement.experiment_id, replacement.started_at) == (identity, started)
        assert replacement.frozen_policy == service.policy
        assert (replacement.directory / "protocol.json").read_bytes() == protocol
        await replacement.capture_round("fixture_round", [decision])
        await replacement.tick()
        assert financial_state(replacement.directory) == before
        assert (await (await db._connection.execute("SELECT count(*) FROM experiment_registry")).fetchone())[0] == 1
        assert replacement.receipt["sqlite_write_root"] == str(service.directory)
        assert replacement.receipt["pid"] != service.receipt["pid"]
    finally:
        await replacement.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("artifact", ["protocol.json", "config.yaml", "runtime_worker.py", "source/services/investment_simulation.py"])
async def test_frozen_artifact_tampering_fails_without_reset(runtime, artifact):
    db, market, service, source = runtime
    await service.close()
    before = financial_state(service.directory)
    path = service.directory / artifact
    path.write_bytes(path.read_bytes() + b"\n ")
    replacement = FrozenForwardEvaluation(db, market, root=service.root, policy=service.policy, source_root=source)
    try:
        with pytest.raises(RuntimeError, match="changed"):
            await replacement.initialize()
        assert financial_state(service.directory) == before
        assert len(list(service.root.glob("*/protocol.json"))) == 1
    finally:
        await replacement.close()


@pytest.mark.asyncio
async def test_interrupted_operation_stays_blocked_across_pinned_restart(runtime):
    db, market, service, source = runtime
    await service.close()
    service._write(service.directory / "active_input.json", {"round_id": "cancelled_fixture"})
    before = financial_state(service.directory)
    replacement = FrozenForwardEvaluation(db, market, root=service.root, policy=service.policy, source_root=source)
    try:
        await replacement.initialize()
        assert replacement.last_status["evaluation_status"] == "data_blocked"
        await replacement.capture_round("new_round", [])
        await replacement.tick()
        assert financial_state(service.directory) == before
        assert json.loads((service.directory / "active_input.json").read_text())["round_id"] == "cancelled_fixture"
    finally:
        await replacement.close()


@pytest.mark.asyncio
async def test_second_worker_owner_cannot_access_books(runtime):
    db, market, service, source = runtime
    replacement = FrozenForwardEvaluation(db, market, root=service.root, policy=service.policy, source_root=source)
    before = financial_state(service.directory)
    try:
        with pytest.raises(RuntimeError, match="stopped"):
            await replacement.initialize()
        assert financial_state(service.directory) == before
        assert (await service.tick())["experiment_id"] == service.experiment_id
    finally:
        await replacement.close()


@pytest.mark.asyncio
async def test_stale_quote_and_changed_common_input_cannot_create_fills(runtime):
    db, market, service, _ = runtime
    market.opened = True
    market.stale = True
    decision = dict(ticker="588170", direction="long", confidence=.8, target_position=.2, decision_id="d")
    await service.capture_round("stale_fixture", [decision])
    assert not financial_state(service.directory)["candidate"]["simulation_trades"]
    with pytest.raises(RuntimeError, match="decisions changed"):
        await service.capture_round("stale_fixture", [{**decision, "target_position": .8}])
    assert (await (await db._connection.execute("SELECT count(*) FROM simulation_trades")).fetchone())[0] == 0


@pytest.mark.asyncio
async def test_adoption_keeps_existing_protocol_inputs_and_finances(tmp_path):
    db = DatabaseService(str(tmp_path / "production.db"))
    await db._init_db()
    market = ControlledFixture()
    sim = InvestmentSimulation(db, market)
    await sim.initialize()
    policy = dict(get_config().get("simulation", {}))
    old = ContinuousEvaluation(db, market, root=tmp_path / "forward", policy=policy)
    replacement = None
    try:
        await old.initialize()
        await old.capture_round("genuine_fixture", [dict(ticker="588170", direction="long", confidence=.8,
                                                     target_position=.2, decision_id="d")])
        await old._publish({"evaluation_status": "data_blocked",
            "reason": "forward engine changed; a new frozen experiment is required"})
        await old.close()
        before = financial_state(old.directory)
        protocol = (old.directory / "protocol.json").read_bytes()
        replacement = FrozenForwardEvaluation(db, market, root=old.root, policy=policy)
        await replacement.initialize()
        assert replacement.experiment_id == old.experiment_id
        assert replacement.started_at == old.started_at
        assert (old.directory / "protocol.json").read_bytes() == protocol
        assert financial_state(old.directory) == before
        assert replacement.last_status["evaluation_status"] == "insufficient_samples"
        assert replacement.last_status["reason"] == "frozen_engine_resumed"
        await replacement.capture_round("genuine_fixture", [dict(ticker="588170", direction="long", confidence=.8,
                                                     target_position=.2, decision_id="d")])
        assert financial_state(old.directory) == before
    finally:
        if replacement is not None:
            await replacement.close()
        await old.close()
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", ["sqlite", "file", "network"])
async def test_worker_enforces_external_access_boundary_before_operation(tmp_path, attempt):
    source = tmp_path / "producer"
    for relative in runtime_code_artifacts(PROJECT_ROOT, "run_discussion.py"):
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PROJECT_ROOT / relative, target)
    forbidden = tmp_path / "forbidden.db"
    operations = {
        "sqlite": f"import sqlite3; sqlite3.connect({str(forbidden)!r})",
        "file": f"Path({str(forbidden)!r}).write_text('forbidden')",
        "network": "import socket; socket.socket().connect(('127.0.0.1', 9))",
    }
    # Explicit hostile engineering fixture, frozen before registration. It is
    # never used by production and supplies no investment/return evidence.
    path = source / "services/continuous_evaluation.py"
    with path.open("a") as stream:
        stream.write("\nasync def boundary_probe(self):\n    " + operations[attempt] +
                     "\nContinuousEvaluation.tick = boundary_probe\n")
    db = DatabaseService(str(tmp_path / "production.db"))
    await db._init_db()
    market = ControlledFixture()
    account = InvestmentSimulation(db, market)
    await account.initialize()
    service = FrozenForwardEvaluation(db, market, root=tmp_path / "forward",
        policy=dict(get_config().get("simulation", {})), source_root=source)
    try:
        await service.initialize()
        with pytest.raises(RuntimeError, match="denied|Review tests cannot use network"):
            await service.tick()
        assert not forbidden.exists()
        assert (await (await db._connection.execute("SELECT count(*) FROM simulation_trades")).fetchone())[0] == 0
    finally:
        await service.close()
        await db.close()


@pytest.mark.asyncio
async def test_caller_timeout_does_not_kill_worker_or_repeat_pending_tick(runtime,monkeypatch):
    db,market,service,_=runtime
    entered,release=asyncio.Event(),asyncio.Event()
    original=market.get_current_quote
    calls=[]
    async def slow_quote(ticker):
        calls.append(ticker);entered.set();await release.wait()
        return await original(ticker)
    monkeypatch.setattr(market,'get_current_quote',slow_quote)
    pid=service._process.pid
    caller=asyncio.create_task(service.tick())
    await asyncio.wait_for(entered.wait(),2)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert service._process.pid==pid and service._process.returncode is None
    assert len(service._requests)==1
    for _ in range(3):
        assert (await service.tick())['runtime_pending']
    assert len(service._requests)==1 and calls==['510300']
    request=next(iter(service._requests))
    owners=[]
    publish=service._publish
    async def owned_publish(value):
        owners.append(asyncio.current_task());await publish(value)
    monkeypatch.setattr(service,'_publish',owned_publish)
    release.set();await asyncio.wait_for(asyncio.shield(request),3)
    assert owners==[]  # The background transport cannot commit the account DB.
    await service.tick()
    assert owners and all(owner is asyncio.current_task() for owner in owners)
    assert service._process.pid==pid
    assert (await (await db._connection.execute('SELECT count(*) FROM simulation_trades')).fetchone())[0]==0


@pytest.mark.asyncio
async def test_slow_capture_is_queued_once_and_copies_submitted_decisions(runtime,monkeypatch):
    _,market,service,_=runtime
    entered,release=asyncio.Event(),asyncio.Event()
    original=market.get_current_quote
    async def slow_quote(ticker):
        entered.set();await release.wait();return await original(ticker)
    monkeypatch.setattr(market,'get_current_quote',slow_quote)
    service._response_wait_seconds=.02
    decision=dict(ticker='588170',direction='long',confidence=.8,target_position=.2,decision_id='slow')
    submitted=[dict(decision)]
    report=await service.capture_round('slow_round',submitted)
    assert report['runtime_pending']
    await asyncio.wait_for(entered.wait(),2)
    submitted[0]['target_position']=.9
    assert (await service.capture_round('slow_round',[decision]))['runtime_pending']
    assert len(service._requests)==1
    request=next(iter(service._requests));release.set()
    await asyncio.wait_for(asyncio.shield(request),3)
    await service.tick()
    packet=json.loads((service.directory/'inputs/slow_round.json').read_text())
    assert packet['decisions'][0]['target_position']==.2
    assert (service.directory/'inputs/slow_round.complete.json').exists()
    assert len(list((service.directory/'producers').glob('slow_round.json')))==1


@pytest.mark.asyncio
async def test_idle_worker_exit_restarts_same_frozen_experiment(runtime):
    _,market,service,_=runtime
    # Exercise real fixture fills, rather than an unused initial cash default.
    await service.capture_round('before_exit',[dict(ticker='588170',direction='long',
        confidence=.8,target_position=.2,decision_id='before_exit')])
    market.opened=True
    await service.tick()
    market.opened=False
    before=financial_state(service.directory)
    identity,started=service.experiment_id,service.started_at
    protocol=(service.directory/'protocol.json').read_bytes()
    previous=service._process.pid
    service._process.kill();await service._process.wait()
    await service.tick()
    assert service._process.pid!=previous and service._process.returncode is None
    assert (service.experiment_id,service.started_at)==(identity,started)
    assert (service.directory/'protocol.json').read_bytes()==protocol
    assert financial_state(service.directory)==before


@pytest.mark.asyncio
async def test_quote_request_deadline_is_missing_data_and_keeps_worker_alive(runtime,monkeypatch):
    _,market,service,_=runtime
    async def unavailable(ticker):
        await asyncio.Event().wait()
    monkeypatch.setattr(market,'get_current_quote',unavailable)
    market.opened=True
    service._data_timeout_seconds=.02
    previous=service._process.pid
    result=await service.capture_round('timeout_round',[dict(ticker='588170',direction='long',
        confidence=.8,target_position=.2,decision_id='missing_quote')])
    assert not result['runtime_pending']
    assert (service.directory/'inputs/timeout_round.complete.json').exists()
    packet=json.loads((service.directory/'inputs/timeout_round.json').read_text())
    assert packet['quotes']=={} and packet['research_quotes']=={}
    await service.tick()
    assert service._process.pid==previous and service._process.returncode is None
    state=financial_state(service.directory)
    assert all(not book['simulation_trades'] and not book['simulation_ledger_entries']
               and not book['quote_snapshots'] for book in state.values())
