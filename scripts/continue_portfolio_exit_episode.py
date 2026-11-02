"""Continue an observed exit experiment without reseeding its paired books.

This finite episode tests exiting an inherited sleeve, through its original
protective exit/horizon. Neither arm admits new entries. It is not a complete
market-timing/re-entry policy or a production-equivalent intraday comparison.
Only current, shared, controlled observations can advance the private books.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ZONE = ZoneInfo("Asia/Shanghai")
ARMS = ("control", "candidate")
TABLES = ("execution_intents", "quote_snapshots", "simulation_trades",
          "simulation_ledger_entries", "simulation_positions", "simulation_pending_decisions",
          "simulation_redeployment_state", "simulation_risk_memory")
STATS = ("simulation_cash", "last_trade_date", "simulation_balance_checkpoint")
VERSION = "finite_exit_episode_v1"
DRIVER_VERSION = "post_intent_quote_v2"


def now():
    return datetime.now(ZONE)


def read(path):
    return json.loads(Path(path).read_bytes())


def write(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def snapshot(path):
    """One read transaction; allow our own durable pending exits to survive."""
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        tables = {t: [dict(r) for r in conn.execute(f'SELECT * FROM "{t}" ORDER BY rowid')]
                  for t in TABLES}
        tables["system_stats"] = [dict(r) for r in conn.execute(
            "SELECT * FROM system_stats WHERE key IN (?,?,?) ORDER BY key", STATS)]
        return dict(tables=tables, state_sha256=digest(tables),
            cash=next(float(r["value"]) for r in tables["system_stats"] if r["key"] == "simulation_cash"),
            positions=tables["simulation_positions"])


def verify(directory):
    directory = Path(directory).resolve()
    protocol = read(directory / "protocol.json")
    if protocol["version"] != VERSION:
        raise RuntimeError("unknown episode version")
    if digest(protocol) != read(directory / "identity.json")["protocol_sha256"]:
        raise RuntimeError("episode protocol changed")
    for relative, expected in protocol["artifacts"].items():
        path = (directory / relative).resolve()
        if not path.is_relative_to(directory) or sha(path) != expected:
            raise RuntimeError("frozen episode artifact changed: " + relative)
    return protocol


def verify_books(directory):
    status = read(directory / "status.json")
    if status["checkpoints"]:
        packet = Path(status["latest_packet"]).resolve()
        if not packet.is_relative_to(directory / "observations"):
            raise RuntimeError("checkpoint outside episode")
        for relative, expected in read(packet / "complete.json").items():
            path = (packet / relative).resolve()
            if not path.is_relative_to(packet) or sha(path) != expected:
                raise RuntimeError("completed checkpoint changed")
        result = read(packet / "result.json")
        if (status["accounts"] != result["accounts"] or status["feedback"] != result["feedback"]
                or status["checkpoints"] != result["feedback"]["observed_checkpoint_count"]
                or status["last_observed_at"] != result["observed_at"]
                or status["last_checkpoint_date"] != result["cutoff"][:10]):
            raise RuntimeError("status differs from completed checkpoint")
    else:
        if any(status["accounts"][arm]["state_sha256"] != read(directory / "prefix" / (arm + "_state.json"))["state_sha256"]
               for arm in ARMS):
            raise RuntimeError("opening status differs from immutable prefix")
    for arm in ARMS:
        if snapshot(directory / (arm + ".db"))["state_sha256"] != status["accounts"][arm]["state_sha256"]:
            raise RuntimeError("persistent book changed outside completed checkpoint: " + arm)
    return status


def driver(directory, protocol):
    """Bind an append-only safety revision without rewriting the old protocol."""
    path = directory / "driver_revision.json"
    if path.exists():
        revision = read(path)
        if (digest(revision) != read(directory / "driver_revision.identity.json")["sha256"]
                or revision["base_protocol_sha256"] != digest(protocol)
                or revision["version"] != DRIVER_VERSION):
            raise RuntimeError("driver revision identity differs")
        worker_path = (directory / revision["worker"]).resolve()
        if not worker_path.is_relative_to(directory) or sha(worker_path) != revision["worker_sha256"]:
            raise RuntimeError("frozen driver revision changed")
        return revision
    if protocol.get("driver_version") != DRIVER_VERSION:
        raise RuntimeError("legacy observer requires explicit repair-quote-boundary revision")
    return dict(version=DRIVER_VERSION, worker="worker.py", worker_sha256=sha(directory / "worker.py"))


def repair_quote_boundary(directory):
    directory = Path(directory).resolve()
    protocol = verify(directory)
    with (directory / "episode.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        marker_path = directory / "active_observation.json"
        pending_repair = dict(kind="quote_boundary_repair_pending", base_protocol_sha256=digest(protocol))
        if marker_path.exists() and read(marker_path) != pending_repair:
            raise RuntimeError("resolve active checkpoint before installing a driver revision")
        previous = verify_books(directory)
        if (directory / "driver_revision.json").exists() or protocol.get("driver_version") == DRIVER_VERSION:
            raise RuntimeError("quote-boundary driver already installed")
        target = directory / "driver_revisions" / DRIVER_VERSION / "worker.py"
        target.parent.mkdir(parents=True, exist_ok=False)
        shutil.copyfile(__file__, target)
        revision = dict(version=DRIVER_VERSION, installed_at=now().isoformat(),
            base_protocol_sha256=digest(protocol), worker=str(target.relative_to(directory)),
            worker_sha256=sha(target), prior_status_sha256=sha(directory / "status.json"),
            previous_observation=previous["last_observed_at"],
            reason="review cache may predate intent; require separate execution client and fetched_at >= batch request",
            common_to_both_arms=True, strategy_and_books_unchanged=True, economic_episode_count=1,
            validation="one additional current same-date engineering checkpoint after this explicit safety revision; not an independent sample")
        write(directory / "driver_revision.json", revision)
        write(directory / "driver_revision.identity.json", dict(sha256=digest(revision)))
        # The old frozen observe entry sees this marker and stops. The new
        # driver keeps it installed outside its exclusive operation lock.
        write(directory / "active_observation.json", dict(kind="driver_upgrade_required", revision_sha256=digest(revision)))
        return revision


def initialize(origin, output):
    """Import the actual paired prefix ONCE; never re-age or replay its fill."""
    origin, output = Path(origin).resolve(), Path(output).resolve()
    old = read(origin / "protocol.json")
    for relative, expected in old["artifacts"].items():
        source = (origin / relative).resolve()
        if not source.is_relative_to(origin) or sha(source) != expected:
            raise RuntimeError("original experiment artifact changed: " + relative)
    original = read(origin / "input.json")
    executed = read(origin / "execution/result.json")
    opening = original["cases"]["inherited"]
    if any(row["status"] in {"pending", "deferred", "executing"}
           for row in opening["tables"]["execution_intents"]):
        raise RuntimeError("original opening has unrelated active orders")
    if not opening["positions"] or any(row["ticker"] != "512800" for row in opening["positions"]):
        raise RuntimeError("this episode is scoped to the observed inherited 512800 sleeve")
    sys.path.insert(0, str(ROOT.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code
    from sovereign_hall.services.policy_snapshot_service import _strip_sensitive
    import yaml
    # This is a continuation of the same exit hypothesis, not an unnoticed edit.
    relative = "runtime/sovereign_hall/services/portfolio_regime_policy.py"
    if sha(ROOT / "services/portfolio_regime_policy.py") != old["artifacts"][relative]:
        raise RuntimeError("exit policy differs from the actually executed prefix")
    output.mkdir(parents=True, exist_ok=False)
    (output / "prefix").mkdir()
    for relative in ("protocol.json", "input.json", "plans.json", "execution/result.json",
                     "execution/prepared.json", "execution/execution_market.json"):
        target = output / "prefix" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin / relative, target)
    accounts = {}
    for arm in ARMS:
        source = origin / "execution" / ("inherited_" + arm + ".db")
        before = snapshot(source)
        witness = executed["accounts"]["inherited_" + arm]
        if (before["cash"] != witness["assets"]["cash"]
                or {p["ticker"]: p["shares"] for p in before["positions"]}
                != {t: p["shares"] for t, p in witness["assets"]["positions"].items()}
                or len(before["tables"]["simulation_trades"]) != witness["counts"]["simulation_trades"]
                or sum(r["fee"] for r in before["tables"]["simulation_trades"]) != witness["counts"]["fees"]):
            raise RuntimeError("original private book differs from its actual execution witness")
        if any(r["status"] in {"pending", "deferred", "executing"}
               for r in before["tables"]["execution_intents"]):
            raise RuntimeError("unresolved prefix order")
        with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as src:
            with sqlite3.connect(output / (arm + ".db")) as dest:
                src.backup(dest)
        after = snapshot(output / (arm + ".db"))
        if before != after:
            raise RuntimeError("prefix book copy differs")
        write(output / "prefix" / (arm + "_state.json"), before)
        accounts[arm] = dict(state_sha256=after["state_sha256"], assets=witness["assets"],
                             cumulative_recorded_fees=sum(r["fee"] for r in after["tables"]["simulation_trades"]))
    snapshot_code(ROOT, output / "runtime/sovereign_hall")
    config = output / "runtime/sovereign_hall/config.yaml"
    config.write_text(yaml.safe_dump(_strip_sensitive(yaml.safe_load(config.read_text())),
                                   allow_unicode=True, sort_keys=False))
    config.chmod(0o600)
    shutil.copytree(ROOT / "services/calendar_data", output / "runtime/sovereign_hall/services/calendar_data")
    shutil.copyfile(__file__, output / "worker.py")
    # SQLite WAL/SHM files belong to evolving books, never to frozen code.
    immutable = [output / "worker.py", *(output / "prefix").rglob("*"),
                 *(output / "runtime").rglob("*")]
    artifacts = {str(p.relative_to(output)): sha(p) for p in immutable if p.is_file()}
    def opened_at(position):
        value = datetime.fromisoformat(position["opened_at"])
        # Existing simulator ledger timestamps are explicitly local exchange
        # time, including the naive timestamp in the genuine opening book.
        return value.replace(tzinfo=ZONE) if value.tzinfo is None else value.astimezone(ZONE)
    end = max(opened_at(p) + timedelta(days=opening["state"]["policy"]["max_holding_days"])
              for p in opening["positions"])
    baseline = executed["accounts"]["inherited_control"]["assets"]["total_assets"]
    protocol = dict(version=VERSION, driver_version=DRIVER_VERSION, experiment_id="portfolio_exit_episode_20261022_v1",
        origin_experiment_id=old["experiment_id"], origin=str(origin), prefix_observed_at=executed["observed_at"],
        frozen_at=now().isoformat(), artifacts=artifacts, simulation=opening["state"]["policy"],
        opening_nav=baseline, original_horizon=end.isoformat(), independent_episodes=1,
        control="existing protective lifecycle, no entries", candidate="same lifecycle plus portfolio_regime_review_v1 exits, no entries",
        endpoint="paired net exit-timing value through closure of the inherited sleeve",
        checkpoint_schedule="at most one current open-session checkpoint per date when invoked; no intraday coverage promised",
        continuity="actual prefix copied once; prior observation gap retained; no historical actions backfilled",
        engine_transition="common current engine explicitly frozen after 2026-10-21 SQLite reliability repair",
        reentry="neither arm re-enters; recovery opportunity cost stays in paired mark-to-market difference",
        qualification="development observation only; no formal return promotion or production-equivalence claim",
        policy_changes=0, production_writes=0, model_calls=0)
    write(output / "protocol.json", protocol)
    write(output / "identity.json", dict(protocol_sha256=digest(protocol)))
    write(output / "status.json", dict(state="observing", experiment_id=protocol["experiment_id"],
        last_observed_at=executed["observed_at"], last_checkpoint_date=executed["observed_at"][:10],
        checkpoints=0, independent_episodes=1, accounts=accounts,
        driver_version=DRIVER_VERSION, return_improvement=None, promotion_eligible=False))
    return protocol


def current_window(cutoff):
    value = datetime.fromisoformat(cutoff)
    if value.tzinfo is None or not 0 <= (now() - value).total_seconds() <= 120:
        raise RuntimeError("current account observation expired or lies in future")


def parse_packet(directory, packet):
    from sovereign_hall.services.market_state_policy import UNIVERSE, parse_tape
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    data = read(packet / "input.json")
    receipts = data["receipts"]
    if len(receipts) != len(UNIVERSE) or {r["ticker"] for r in receipts} != set(UNIVERSE):
        raise RuntimeError("complete fixed receipt universe required")
    calendar = load_exchange_calendar(datetime.fromisoformat(data["cutoff"]).year)
    if (calendar.sessions != data["calendar"]["sessions"]
            or calendar.source_identity != data["calendar"]["source_identity"]):
        raise RuntimeError("trusted calendar differs")
    tapes = {r["ticker"]: parse_tape((packet / "raw" / (r["ticker"] + ".json")).read_bytes(), r,
        ticker=r["ticker"], cutoff=data["cutoff"], sessions=calendar.sessions) for r in receipts}
    return data, tapes


def feedback(protocol, previous, accounts):
    values = [accounts[a]["assets"].get("total_assets") for a in ARMS]
    complete = all(accounts[a]["assets"]["valuation_complete"] and values[i] is not None
                   for i, a in enumerate(ARMS))
    difference = values[1] - values[0] if complete else None
    extra_fees = accounts["candidate"]["cumulative_recorded_fees"] - accounts["control"]["cumulative_recorded_fees"]
    risk = {}
    for i, arm in enumerate(ARMS):
        prior = previous.get("feedback", {}).get("sampled_risk", {}).get(arm, {})
        initial = previous.get("accounts", {}).get(arm, {}).get("assets", {}).get("total_assets")
        peak = max(protocol["opening_nav"], prior.get("peak_observed_assets", 0), values[i] or 0)
        current_drawdown = values[i] / peak - 1 if values[i] is not None else None
        prior_drawdown = prior.get("max_drawdown_at_observed_checkpoints",
            min(0.0, initial / protocol["opening_nav"] - 1) if initial is not None else 0.0)
        risk[arm] = dict(peak_observed_assets=peak,
            max_drawdown_at_observed_checkpoints=min(prior_drawdown, current_drawdown or 0.0),
            current_drawdown=current_drawdown, invested_ratio=accounts[arm]["assets"].get("invested_ratio"),
            scope="observed checkpoints only; unobserved intraday risk unavailable")
    return dict(valuation_complete=complete, candidate_minus_control_value=difference,
        candidate_minus_control_return_from_opening_nav=difference / protocol["opening_nav"] if complete else None,
        extra_candidate_paid_fees=extra_fees,
        exposure_effect_before_extra_fees=difference + extra_fees if complete else None,
        independent_episodes=1, observed_checkpoint_count=previous["checkpoints"] + 1, sampled_risk=risk,
        attributable_investment_return_improvement=None, formal_qualification="unproven",
        interpretation="one dependent exit episode; paired result includes missed recoveries and actual paid costs")


def send(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    sys.stdout.flush()


async def message(process, expected, packet):
    line = await asyncio.wait_for(process.stdout.readline(), 45)
    if not line:
        raise RuntimeError(f"worker ended before {expected}; inspect {packet / 'worker.stderr.log'}")
    value = json.loads(line)
    if value.get("kind") != expected:
        raise RuntimeError("unexpected episode worker response")
    return value


def admitted_quotes(batch):
    """A new API call can return its client's earlier research cache."""
    accepted = dict(batch)
    accepted["quotes"] = dict(batch["quotes"])
    accepted["rejected_quotes"] = dict(batch.get("rejected_quotes", {}))
    requested = datetime.fromisoformat(batch["requested_at"])
    completed = datetime.fromisoformat(batch["completed_at"])
    if requested.tzinfo is None or completed.tzinfo is None or completed < requested:
        raise RuntimeError("execution request requires an explicit timezone")
    for ticker, quote in batch["quotes"].items():
        if quote is None:
            continue
        try:
            fetched = datetime.fromisoformat(quote["fetched_at"])
            valid = fetched.tzinfo is not None and requested <= fetched <= completed
        except (TypeError, KeyError, ValueError):
            valid = False
        if not valid:
            accepted["quotes"][ticker] = None
            accepted["rejected_quotes"][ticker] = dict(reason="retrieval_before_execution_request", quote=quote)
    return accepted


async def worker(directory, packet):
    protocol = verify(directory)
    revision = driver(directory, protocol)
    if sha(__file__) != revision["worker_sha256"]:
        raise RuntimeError("worker is not the current frozen safety revision")
    sys.path.insert(0, str(directory / "runtime"))
    sys.dont_write_bytecode = True
    os.chdir(directory)
    def guard(event, args):
        if event in {"socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"}:
            raise RuntimeError("episode worker external access denied")
        if event == "sqlite3.connect":
            value = str(args[0])
            if value != ":memory:" and (value.startswith("file:") or not Path(value).resolve().is_relative_to(directory)):
                raise RuntimeError("episode worker SQLite outside private directory denied")
        if event == "open":
            path, mode, flags = args
            writing = (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
                isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            if writing and not isinstance(path, int) and not Path(path).resolve().is_relative_to(directory):
                raise RuntimeError("episode worker outside write denied")
        if event in {"os.rename", "os.remove", "os.rmdir", "os.mkdir"}:
            paths = args[:2] if event == "os.rename" else args[:1]
            if any(not Path(p).resolve().is_relative_to(directory) for p in paths):
                raise RuntimeError("episode worker outside mutation denied")
    sys.addaudithook(guard)
    from sovereign_hall import core
    core.DATA_DIR = directory / "data"
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.investment_simulation import InvestmentSimulation
    from sovereign_hall.services.portfolio_regime_policy import build_review
    from sovereign_hall.services.continuous_evaluation import SharedQuoteBatch
    from sovereign_hall.application.execute_simulation_cycle import validate_balance_checkpoint
    config = get_config()
    if config.get("simulation", {}) != protocol["simulation"]:
        raise RuntimeError("effective simulation policy differs")
    data, tapes = parse_packet(directory, packet)
    current_window(data["cutoff"])
    accounts, plans, prepared = {}, {}, {}
    try:
        for arm in ARMS:
            # Use ordinary SQLite only for read verification inside this boundary.
            with sqlite3.connect(directory / (arm + ".db")) as c:
                c.row_factory = sqlite3.Row
                tables = {t: [dict(r) for r in c.execute(f'SELECT * FROM "{t}" ORDER BY rowid')] for t in TABLES}
                tables["system_stats"] = [dict(r) for r in c.execute(
                    "SELECT * FROM system_stats WHERE key IN (?,?,?) ORDER BY key", STATS)]
            if digest(tables) != data["before"][arm]["state_sha256"]:
                raise RuntimeError("book changed since common input capture")
            db = DatabaseService(str(directory / (arm + ".db")))
            accounts[arm] = (db, None)
            await db._init_db()
            sim = InvestmentSimulation(db, SharedQuoteBatch({}, False, False))
            accounts[arm] = (db, sim)
            await sim.initialize()
            state = dict(cutoff=data["cutoff"], latest_completed_session=data["latest_completed_session"],
                cash=sim.cash, positions=[dict(ticker=t, **p) for t, p in sim.positions.items()],
                quotes=data["review_quotes"], policy=protocol["simulation"])
            plans[arm] = build_review(tapes, state)["arms"][arm]
            identities = []
            for draft in plans[arm]["intent_drafts"]:
                if draft["direction"] != "sell" or draft["target_position"] != 0:
                    raise RuntimeError("finite exit episode cannot add risk or re-enter")
                identities.append(await sim.create_execution_intent(**draft,
                    idempotency_key=f"exit_episode:{arm}:{draft['ticker']}"))
            async with db._connection.execute("SELECT * FROM execution_intents WHERE status IN ('pending','deferred','executing')") as cursor:
                active = [dict(r) for r in await cursor.fetchall()]
            if any(not r["idempotency_key"].startswith(f"exit_episode:{arm}:") or r["direction"] != "sell" for r in active):
                raise RuntimeError("unrelated active intent in finite episode")
            prepared[arm] = active
        write(packet / "plans.json", plans)
        write(packet / "prepared.json", prepared)
        send(dict(kind="prepared", intents=prepared,
            tickers=sorted({t for _, sim in accounts.values() for t in sim.positions})))
        reader = asyncio.StreamReader()
        await asyncio.get_running_loop().connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        batch = admitted_quotes(json.loads(await reader.readline()))
        requested = datetime.fromisoformat(batch["requested_at"])
        intent_times = [datetime.fromisoformat(r["created_at"]).replace(tzinfo=ZONE)
                        for rows in prepared.values() for r in rows]
        if intent_times and requested <= max(intent_times):
            raise RuntimeError("common execution quote requested before durable intent")
        current_window(data["cutoff"])
        write(packet / "execution_market.json", batch)
        result = {}
        for arm, (db, sim) in accounts.items():
            sim.quote_provider = SharedQuoteBatch(batch["quotes"], batch["trading_day"], batch["market_open"])
            outcomes = []
            for intent in prepared[arm]:
                async with db._connection.execute("SELECT id FROM simulation_pending_decisions WHERE intent_id=? AND status LIKE 'pending%' LIMIT 1", (intent["id"],)) as cursor:
                    pending = await cursor.fetchone()
                outcomes.append(await sim.execute_intent(intent["id"], pending_decision_id=pending[0] if pending else None))
            if any(r.get("action") == "error" for r in outcomes):
                raise RuntimeError("private execution failed; retain marker and rollback evidence")
            assets = await sim.calculate_assets()
            async with db.transaction() as conn:
                balance = await validate_balance_checkpoint(conn, sim.cash,
                    {t: p["shares"] for t, p in sim.positions.items()}, create=False)
                async with conn.execute("PRAGMA foreign_key_check") as cursor:
                    foreign = await cursor.fetchall()
                async with conn.execute("SELECT * FROM simulation_trades ORDER BY id") as cursor:
                    trades = [dict(r) for r in await cursor.fetchall()]
            if balance["status"] != "balanced_since_checkpoint" or foreign:
                raise RuntimeError("private book failed reconciliation")
            async with db._connection.execute("SELECT count(*) FROM execution_intents WHERE status IN ('pending','deferred','executing')") as cursor:
                pending_count = (await cursor.fetchone())[0]
            last_id = max((r["id"] for r in data["before"][arm]["tables"]["simulation_trades"]), default=0)
            result[arm] = dict(assets=assets, outcomes=outcomes, balance=balance, pending_exits=pending_count,
                new_trades=[r for r in trades if r["id"] > last_id],
                cumulative_recorded_fees=sum(r["fee"] for r in trades))
        write(packet / "worker_result.json", result)
        send(dict(kind="result", accounts=result))
    finally:
        for db, _ in accounts.values():
            await db.close()


async def capture(directory, packet, protocol, market):
    import httpx
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    from sovereign_hall.services.market_state_policy import UNIVERSE, completed_sessions
    start = now()
    if not await market.is_trading_day() or not await market.is_market_open():
        raise RuntimeError("current verified open trading session required; no historical fill catch-up")
    cal = load_exchange_calendar(start.year)
    last = completed_sessions(cal.sessions, start.isoformat())[-1]
    first = (start.date() - timedelta(days=200)).isoformat()
    (packet / "raw").mkdir()
    receipts = []
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        for ticker in UNIVERSE:
            symbol = ("sh" if ticker.startswith("5") else "sz") + ticker
            requested = now().isoformat()
            response = await client.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                params={"param": f"{symbol},day,{first},{last},200,qfq"})
            path = packet / "raw" / (ticker + ".json")
            path.write_bytes(response.content)
            receipts.append(dict(ticker=ticker, url=str(response.url), status=response.status_code,
                requested_at=requested, captured_at=now().isoformat(), sha256=sha(path)))
    before = {arm: snapshot(directory / (arm + ".db")) for arm in ARMS}
    tickers = sorted({p["ticker"] for state in before.values() for p in state["positions"]})
    quotes = dict(zip(tickers, await asyncio.gather(*(market.get_current_quote(t) for t in tickers))))
    data = dict(cutoff=now().isoformat(), before=before, receipts=receipts, review_quotes=quotes,
        latest_completed_session=last, calendar=dict(source_identity=cal.source_identity, sessions=cal.sessions))
    write(packet / "input.json", data)


async def observe(directory, *, market=None, capture_fn=capture):
    directory = Path(directory).resolve()
    protocol = verify(directory)
    revision = driver(directory, protocol)
    if sha(__file__) != revision["worker_sha256"]:
        raise RuntimeError("continuation tool changed; invoke the frozen worker.py observe command")
    lock = (directory / "episode.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    owned = market is None
    process = None
    safety_marker = None
    try:
        if (directory / "active_observation.json").exists():
            marker = read(directory / "active_observation.json")
            if marker == dict(kind="driver_upgrade_required", revision_sha256=digest(revision)):
                safety_marker = marker
            else:
                raise RuntimeError("interrupted paired checkpoint; inspect retained evidence before any continuation")
        previous = verify_books(directory)
        if (previous["state"] == "closed" or
                (previous["last_checkpoint_date"] == now().date().isoformat()
                 and previous.get("feedback", {}).get("valuation_complete", True)
                 and previous.get("driver_version") == DRIVER_VERSION)):
            return dict(status="already_observed_or_closed", state=previous["state"], new_fills=0)
        sys.path.insert(0, str(directory / "runtime"))
        if owned:
            from sovereign_hall.services.market_data import MarketDataService
            market = MarketDataService(timeout=4)
        packet = directory / "observations" / now().strftime("%Y%m%d_%H%M%S_%f")
        packet.mkdir(parents=True, exist_ok=False)
        await capture_fn(directory, packet, protocol, market)
        data, _ = parse_packet(directory, packet)
        current_window(data["cutoff"])
        for arm in ARMS:
            if data["before"][arm]["state_sha256"] != previous["accounts"][arm]["state_sha256"]:
                raise RuntimeError("common account state changed during capture")
        write(directory / "active_observation.json", dict(packet=str(packet), started_at=now().isoformat()))
        with (packet / "worker.stderr.log").open("x") as log:
            process = await asyncio.create_subprocess_exec(sys.executable, "-B", str(directory / revision["worker"]),
                "--worker", str(directory), str(packet), stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=log)
            prepared = await message(process, "prepared", packet)
            # A separate actual client has never seen research prices. Tests
            # may inject a producer; the receipt gate still rejects its cache.
            execution_market = market
            if owned:
                from sovereign_hall.services.market_data import MarketDataService
                execution_market = MarketDataService(timeout=4)
            try:
                requested = now().isoformat()
                tickers = prepared["tickers"]
                quotes = dict(zip(tickers, await asyncio.gather(*(execution_market.get_current_quote(t) for t in tickers))))
                batch = admitted_quotes(dict(requested_at=requested, completed_at=now().isoformat(), quotes=quotes,
                    trading_day=await execution_market.is_trading_day(), market_open=await execution_market.is_market_open(),
                    quote_attempts=execution_market.last_quote_attempts, driver_version=DRIVER_VERSION,
                    driver_sha256=revision["worker_sha256"]))
            finally:
                if execution_market is not market:
                    await execution_market.close()
            process.stdin.write((json.dumps(batch, allow_nan=False) + "\n").encode())
            await process.stdin.drain()
            process.stdin.close()
            result = await message(process, "result", packet)
            await asyncio.wait_for(process.wait(), 10)
            if process.returncode or result.get("kind") != "result":
                raise RuntimeError("paired checkpoint failed; inspect retained worker evidence")
        accounts = result["accounts"]
        for arm in ARMS:
            accounts[arm]["state_sha256"] = snapshot(directory / (arm + ".db"))["state_sha256"]
        observation = dict(cutoff=data["cutoff"], observed_at=now().isoformat(), accounts=accounts,
            feedback=feedback(protocol, previous, accounts), gap_seconds=(now() - datetime.fromisoformat(previous["last_observed_at"])).total_seconds(),
            prior_observation=previous["last_observed_at"], no_backfill=True, production_writes=0, model_calls=0,
            driver_version=DRIVER_VERSION, driver_sha256=revision["worker_sha256"],
            safety_revision_validation=previous.get("driver_version") != DRIVER_VERSION)
        write(packet / "result.json", observation)
        write(packet / "complete.json", {str(p.relative_to(packet)): sha(p) for p in packet.rglob("*") if p.is_file()})
        status = dict(state="closed" if all(not a["assets"]["positions"] and not a["pending_exits"] for a in accounts.values()) else "observing",
            experiment_id=protocol["experiment_id"], last_observed_at=observation["observed_at"],
            last_checkpoint_date=data["cutoff"][:10], checkpoints=previous["checkpoints"] + 1,
            independent_episodes=1, accounts=accounts, latest_packet=str(packet), feedback=observation["feedback"],
            driver_version=DRIVER_VERSION, return_improvement=None, promotion_eligible=False)
        write(directory / "status.json", status)
        (directory / "active_observation.json").unlink()
        return observation
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            await process.wait()
        if owned and market is not None:
            await market.close()
        if safety_marker is not None and not (directory / "active_observation.json").exists():
            write(directory / "active_observation.json", safety_marker)
        lock.close()


def replay(directory):
    protocol = verify(directory)
    driver(directory, protocol)
    verify_books(directory)
    sys.path.insert(0, str(directory / "runtime"))
    from sovereign_hall.services.portfolio_regime_policy import build_review
    count = 0
    for packet in sorted((directory / "observations").glob("*")):
        if not (packet / "complete.json").exists():
            continue
        for relative, expected in read(packet / "complete.json").items():
            path = (packet / relative).resolve()
            if not path.is_relative_to(packet) or sha(path) != expected:
                raise RuntimeError("recorded checkpoint artifact changed")
        data, tapes = parse_packet(directory, packet)
        plans = {}
        for arm in ARMS:
            state = dict(cutoff=data["cutoff"], latest_completed_session=data["latest_completed_session"],
                cash=data["before"][arm]["cash"], positions=data["before"][arm]["positions"],
                quotes=data["review_quotes"], policy=read(directory / "protocol.json")["simulation"])
            plans[arm] = build_review(tapes, state)["arms"][arm]
        if plans != read(packet / "plans.json"):
            raise RuntimeError("recorded exit review differs")
        count += 1
    return dict(status="matched", checkpoints=count, database_writes=0, network_calls=0, historical_fills=0)


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        asyncio.run(worker(Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve()))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("initialize")
    init.add_argument("--origin", required=True, type=Path)
    init.add_argument("--output", required=True, type=Path)
    for command in ("observe", "replay", "repair-quote-boundary"):
        sub.add_parser(command).add_argument("--episode", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "initialize":
        value = initialize(args.origin, args.output)
        print(json.dumps(dict(experiment_id=value["experiment_id"], episode=str(args.output.resolve()))))
    elif args.command == "observe":
        value = asyncio.run(observe(args.episode))
        print(json.dumps({k: v for k, v in value.items() if k != "accounts"}, ensure_ascii=False))
    elif args.command == "repair-quote-boundary":
        print(json.dumps(repair_quote_boundary(args.episode.resolve()), ensure_ascii=False))
    else:
        print(json.dumps(replay(args.episode.resolve())))


if __name__ == "__main__":
    main()
