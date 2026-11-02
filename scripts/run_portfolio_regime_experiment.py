"""Freeze and test a portfolio regime hypothesis in exact private account copies.

Only execute requests new controlled quotes. Replay never creates historical
fills. Production and the already running forward experiment are read-only.
This is a current portfolio transition experiment, not a return qualification.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ZONE = ZoneInfo("Asia/Shanghai")
TABLES = ("policy_snapshots", "research_rounds", "round_events", "execution_intents",
          "quote_snapshots", "simulation_trades", "simulation_ledger_entries",
          "simulation_positions", "simulation_pending_decisions",
          "simulation_redeployment_state", "simulation_risk_memory", "system_stats")
STATS = ("simulation_cash", "last_trade_date", "simulation_balance_checkpoint")


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    path = Path(path)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def snapshot_account(path):
    """One read transaction, retaining the financial lineage's reference closure."""
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        tables = {}
        for name in TABLES[3:-1]:
            tables[name] = [dict(row) for row in conn.execute(f'SELECT * FROM "{name}" ORDER BY rowid')]
        marks = ",".join("?" for _ in STATS)
        tables["system_stats"] = [dict(row) for row in conn.execute(
            f"SELECT * FROM system_stats WHERE key IN ({marks}) ORDER BY key", STATS)]
        rounds = sorted({row["round_id"] for rows in tables.values() for row in rows if row.get("round_id")})
        marks = ",".join("?" for _ in rounds)
        for name in ("research_rounds", "round_events"):
            key = "id" if name == "research_rounds" else "round_id"
            tables[name] = [dict(row) for row in conn.execute(
                f'SELECT * FROM "{name}" WHERE "{key}" IN ({marks}) ORDER BY rowid', rounds)] if rounds else []
        policies = sorted({row["policy_snapshot_id"] for row in tables["research_rounds"] if row.get("policy_snapshot_id")})
        marks = ",".join("?" for _ in policies)
        tables["policy_snapshots"] = [dict(row) for row in conn.execute(
            f"SELECT * FROM policy_snapshots WHERE id IN ({marks}) ORDER BY rowid", policies)] if policies else []
        # Do not quietly import an active earlier order into a different policy.
        if any(row["status"] in {"pending", "deferred", "executing"} for row in tables["execution_intents"]):
            raise RuntimeError("opening account has active intents; resolve the common starting boundary first")
        if any(row["status"] in {"pending", "pending_next_trading_session", "deferred"}
               for row in tables["simulation_pending_decisions"]):
            raise RuntimeError("opening account has active pending decisions")
        cash = next(float(row["value"]) for row in tables["system_stats"] if row["key"] == "simulation_cash")
        return dict(source=str(Path(path).resolve()), read_at=datetime.now(ZONE).isoformat(),
                    cash=cash, positions=tables["simulation_positions"], tables=tables)


def parse_inputs(run):
    from sovereign_hall.services.market_state_policy import parse_tape
    data = read(run / "input.json")
    tapes = {receipt["ticker"]: parse_tape(
        (run / "raw" / (receipt["ticker"] + ".json")).read_bytes(), receipt,
        ticker=receipt["ticker"], cutoff=data["cutoff"], sessions=data["calendar"]["sessions"])
        for receipt in data["receipts"]}
    return data, tapes


def verify(run):
    protocol = read(run / "protocol.json")
    if sha(run / "worker.py") != protocol["worker_sha256"]:
        raise RuntimeError("frozen worker changed")
    for relative, expected in protocol["artifacts"].items():
        path = (run / relative).resolve()
        if not path.is_relative_to(run) or sha(path) != expected:
            raise RuntimeError("frozen artifact changed: " + relative)
    return protocol


def check_execution_window(data, *, now=None):
    """A replayed opening book is not a newly observed current account."""
    cutoff = datetime.fromisoformat(data["cutoff"])
    current = now or datetime.now(ZONE)
    if cutoff.tzinfo is None or current.tzinfo is None:
        raise ValueError("explicit execution/cutoff timezone required")
    if not 0 <= (current - cutoff).total_seconds() <= 120:
        raise RuntimeError("current transition window elapsed or future input; capture fresh common state, never re-age inputs")


async def freeze(market_capture, output):
    sys.path.insert(0, str(ROOT.parent))
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.experiment_runtime import snapshot_code
    from sovereign_hall.services.market_data import MarketDataService
    from sovereign_hall.services.market_state_policy import UNIVERSE
    from sovereign_hall.services.portfolio_regime_policy import build_review, VERSION
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    source = read(market_capture / "input.json")
    if (len(source["receipts"]) != len(UNIVERSE)
            or {r["ticker"] for r in source["receipts"]} != set(UNIVERSE)):
        raise ValueError("complete fixed receipt universe required")
    output.mkdir(parents=True, exist_ok=False)
    pinned = read(ROOT / "runs/forward_evaluation/active_runtime.json")
    cases = {
        "inherited": snapshot_account(ROOT / "data/sovereign_hall.db"),
        "cash": snapshot_account(ROOT / "runs/forward_evaluation" / pinned["experiment_id"] / "candidate.db"),
    }
    market = MarketDataService(timeout=4)
    try:
        tickers = sorted(set(UNIVERSE) | {p["ticker"] for case in cases.values() for p in case["positions"]})
        quotes = dict(zip(tickers, await asyncio.gather(*(market.get_current_quote(t) for t in tickers))))
        cutoff = datetime.now(ZONE).isoformat()
        calendar = load_exchange_calendar(datetime.fromisoformat(cutoff).year)
        if calendar.sessions != source["calendar"]["sessions"] or calendar.source_identity != source["calendar"]["source_identity"]:
            raise RuntimeError("capture calendar differs from current trusted calendar")
        (output / "raw").mkdir()
        for receipt in source["receipts"]:
            filename = receipt["ticker"] + ".json"
            if sha(market_capture / "raw" / filename) != receipt["sha256"]:
                raise RuntimeError("original tape changed")
            shutil.copyfile(market_capture / "raw" / filename, output / "raw" / filename)
        policy = dict(get_config().get("simulation", {}))
        for case in cases.values():
            case["state"] = dict(cutoff=cutoff, cash=case["cash"], positions=case["positions"],
                quotes=quotes, policy=policy, latest_completed_session=source["state"]["latest_completed_session"])
        write(output / "input.json", dict(cutoff=cutoff, cases=cases, receipts=source["receipts"],
            calendar=source["calendar"], quote_attempts=market.last_quote_attempts,
            production_writes=0, origin_forward_experiment=pinned, historical_returns_verified=False))
        data, tapes = parse_inputs(output)
        write(output / "plans.json", {name: build_review(tapes, case["state"]) for name, case in cases.items()})
    finally:
        await market.close()
    snapshot_code(ROOT, output / "runtime/sovereign_hall")
    # No model call is part of this experiment. Keep simulation settings exact,
    # but do not retain credential values in a general source artifact.
    import yaml
    config_path = output / "runtime/sovereign_hall/config.yaml"
    config = yaml.safe_load(config_path.read_text())
    def redact(value):
        if isinstance(value, dict):
            return {k: ("" if any(s in k.lower() for s in ("api_key", "password", "secret", "token"))
                        and isinstance(v, str) else redact(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value
    config_path.write_text(yaml.safe_dump(redact(config), allow_unicode=True, sort_keys=False))
    config_path.chmod(0o600)
    calendar_dir = ROOT / "services/calendar_data"
    shutil.copytree(calendar_dir, output / "runtime/sovereign_hall/services/calendar_data")
    shutil.copyfile(__file__, output / "worker.py")
    artifacts = {str(path.relative_to(output)): sha(path) for path in sorted(output.rglob("*")) if path.is_file()}
    write(output / "protocol.json", dict(experiment_id="portfolio_regime_20261021_v1", version=VERSION,
        frozen_at=datetime.now(ZONE).isoformat(), cutoff=cutoff, artifacts=artifacts,
        worker_sha256=sha(output / "worker.py"), production_root=str(ROOT),
        control="existing lifecycle review at common cutoff", candidate="lifecycle plus declared covered-equity regime exit",
        sample_scope="two actual books in one observed market state; not independent market outcomes",
        execution="one current private transition; persist all intents before common fresh controlled re-quote",
        return_comparison="N/A; transition cost/exposure only, no research-alpha or full forward policy claim",
        promotion="isolated_candidate", model_calls=0, production_writes=0))
    print(json.dumps(dict(frozen=str(output), cutoff=cutoff,
        actions={name: {arm: len(value["intent_drafts"]) for arm, value in plan["arms"].items()}
                 for name, plan in read(output / "plans.json").items()})))


def send(value):
    sys.stdout.write(json.dumps(value, allow_nan=False) + "\n")
    sys.stdout.flush()


async def worker(run, output):
    verify(run)
    check_execution_window(read(run / "input.json"))
    sys.path.insert(0, str(run / "runtime"))
    from sovereign_hall.services.experiment_runtime import isolate_process
    isolate_process(output)
    import os
    def guard(event, args):
        if event in {"socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"}:
            raise RuntimeError("portfolio worker external access denied")
        if event in {"os.rename", "os.remove", "os.rmdir", "os.mkdir"}:
            paths = args[:2] if event == "os.rename" else args[:1]
            if any(not Path(path).resolve().is_relative_to(output) for path in paths):
                raise RuntimeError("portfolio worker filesystem mutation outside output denied")
    sys.addaudithook(guard)
    from sovereign_hall.application.execute_simulation_cycle import validate_balance_checkpoint
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.investment_simulation import InvestmentSimulation
    from sovereign_hall.services.portfolio_regime_policy import build_review
    from sovereign_hall.services.continuous_evaluation import SharedQuoteBatch
    data, tapes = parse_inputs(run)
    plans = {name: build_review(tapes, case["state"]) for name, case in data["cases"].items()}
    if plans != read(run / "plans.json"):
        raise RuntimeError("frozen portfolio plan mismatch")
    accounts, prepared = {}, {}
    try:
        for case_name, case in data["cases"].items():
            for arm in ("control", "candidate"):
                name = case_name + "_" + arm
                db = DatabaseService(str(output / (name + ".db")))
                accounts[name] = (db, None)
                await db._init_db()
                sim = InvestmentSimulation(db, SharedQuoteBatch({}, False, False))
                accounts[name] = (db, sim)
                await sim.init_tables()
                async with db.transaction() as conn:
                    for table in TABLES:
                        for row in case["tables"][table]:
                            columns = ",".join('"' + key + '"' for key in row)
                            marks = ",".join("?" for _ in row)
                            await conn.execute(f'INSERT INTO "{table}" ({columns}) VALUES ({marks})', tuple(row.values()))
                await sim.initialize()
                async with db.transaction() as conn:
                    balance = await validate_balance_checkpoint(conn, sim.cash,
                        {t: p["shares"] for t, p in sim.positions.items()})
                if balance["status"] not in {"balanced_since_checkpoint", "checkpoint_recorded"}:
                    raise RuntimeError("opening financial state does not reconcile")
                if sim.config != case["state"]["policy"]:
                    raise RuntimeError("frozen effective simulation policy differs")
                prepared[name] = []
                for draft in plans[case_name]["arms"][arm]["intent_drafts"]:
                    identity = await sim.create_execution_intent(**draft,
                        idempotency_key=f"regime:{case_name}:{arm}:{draft['ticker']}")
                    if not identity:
                        raise RuntimeError("price-free intent did not persist")
                    row = await (await db._connection.execute("SELECT * FROM execution_intents WHERE id=?", (identity,))).fetchone()
                    prepared[name].append(dict(row))
                if await (await db._connection.execute("PRAGMA foreign_key_check")).fetchall():
                    raise RuntimeError("opening financial lineage has missing references")
        write(output / "prepared.json", prepared)
        send(dict(kind="prepared", intents=prepared))
        reader = asyncio.StreamReader()
        await asyncio.get_running_loop().connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        batch = json.loads(await reader.readline())
        requested = datetime.fromisoformat(batch["requested_at"])
        intent_times = [datetime.fromisoformat(row["created_at"]).replace(tzinfo=ZONE)
                        for rows in prepared.values() for row in rows]
        if intent_times and requested <= max(intent_times):
            raise RuntimeError("execution quotes were fetched before intent persistence")
        shared = SharedQuoteBatch(batch["quotes"], batch["trading_day"], batch["market_open"])
        write(output / "execution_market.json", batch)
        results = {}
        for name, (db, sim) in accounts.items():
            case = data["cases"][name.rsplit("_", 1)[0]]
            sim.quote_provider = shared
            outcomes = [await sim.execute_intent(row["id"]) for row in prepared[name]]
            added = await (await db._connection.execute("SELECT count(*) FROM simulation_trades")).fetchone()
            # A second invocation must not add a fill, quote or fee.
            before_repeat = await financial_counts(db)
            repeated = [await sim.execute_intent(row["id"]) for row in prepared[name]]
            if await financial_counts(db) != before_repeat:
                raise RuntimeError("repeated intent changed financial records")
            balance = await validate_balance_checkpoint(db._connection, sim.cash,
                {t: p["shares"] for t, p in sim.positions.items()}, create=False)
            assets = await sim.calculate_assets()
            counts = await financial_counts(db)
            last_original_id = max((row["id"] for row in case["tables"]["simulation_trades"]), default=0)
            new_trades = [dict(row) for row in await (await db._connection.execute(
                "SELECT * FROM simulation_trades WHERE id>? ORDER BY id", (last_original_id,))).fetchall()]
            foreign = await (await db._connection.execute("PRAGMA foreign_key_check")).fetchall()
            if balance["status"] != "balanced_since_checkpoint" or foreign:
                raise RuntimeError("executed account does not reconcile")
            if any(outcome.get("action") == "error" for outcome in outcomes):
                raise RuntimeError("private transition failed; original rollback evidence retained")
            results[name] = dict(outcomes=outcomes, repeated=repeated, assets=assets,
                added_fills=added[0] - len(case["tables"]["simulation_trades"]),
                added_fees=sum(row["fee"] for row in new_trades), new_trades=new_trades,
                counts=counts, balance=balance, foreign_key_check=[], repeat_idempotent=True)
        result = dict(cutoff=data["cutoff"], observed_at=datetime.now(ZONE).isoformat(), accounts=results,
            production_writes=0, model_calls=0, investment_quality_improvement=None, return_improvement=None,
            scope="current private portfolio transition; no historical fills or full forward return claim")
        write(output / "result.json", result)
        send(dict(kind="result", result=result))
    finally:
        for db, _ in accounts.values():
            await db.close()


async def financial_counts(db):
    conn = db._connection
    result = {table: (await (await conn.execute(f'SELECT count(*) FROM "{table}"')).fetchone())[0]
              for table in ("simulation_trades", "simulation_ledger_entries", "quote_snapshots", "execution_intents")}
    result["fees"] = (await (await conn.execute("SELECT coalesce(sum(fee),0) FROM simulation_trades")).fetchone())[0]
    return result


async def execute(run, output):
    protocol = verify(run)
    if output.exists():
        raise FileExistsError("execution output already exists; never refill old quotes or reuse a book")
    check_execution_window(read(run / "input.json"))
    sys.path.insert(0, str(run / "runtime"))
    from sovereign_hall.services.market_data import MarketDataService
    market = MarketDataService(timeout=4)
    log = (run / (output.name + ".stderr.log")).open("x")
    process = await asyncio.create_subprocess_exec(sys.executable, "-B", str(run / "worker.py"),
        "--worker", str(run), str(output), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=log)
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 60)
        prepared = json.loads(line)
        if prepared.get("kind") != "prepared":
            raise RuntimeError("worker did not reach durable intent boundary")
        data = read(run / "input.json")
        tickers = sorted({p["ticker"] for case in data["cases"].values() for p in case["positions"]})
        requested = datetime.now(ZONE).isoformat()
        quotes = dict(zip(tickers, await asyncio.gather(*(market.get_current_quote(t) for t in tickers))))
        batch = dict(requested_at=requested, completed_at=datetime.now(ZONE).isoformat(), quotes=quotes,
            trading_day=await market.is_trading_day(), market_open=await market.is_market_open(),
            quote_attempts=market.last_quote_attempts)
        process.stdin.write((json.dumps(batch, allow_nan=False) + "\n").encode())
        await process.stdin.drain()
        process.stdin.close()
        line = await asyncio.wait_for(process.stdout.readline(), 60)
        response = json.loads(line)
        await asyncio.wait_for(process.wait(), 15)
        if process.returncode or response.get("kind") != "result":
            raise RuntimeError("private portfolio execution failed; inspect retained stderr")
        result = response["result"]
        print(json.dumps(dict(experiment_id=protocol["experiment_id"], output=str(output),
            accounts={name: dict(added_fills=account["added_fills"], added_fees=account["added_fees"],
                cash=account["assets"]["cash"], total_assets=account["assets"]["total_assets"])
                for name, account in result["accounts"].items()}, return_improvement=None)))
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()
        log.close()
        await market.close()


def replay(run, output):
    verify(run)
    sys.path.insert(0, str(run / "runtime"))
    from sovereign_hall.services.portfolio_regime_policy import build_review
    data, tapes = parse_inputs(run)
    plans = {name: build_review(tapes, case["state"]) for name, case in data["cases"].items()}
    if plans != read(run / "plans.json"):
        raise RuntimeError("replayed plan differs")
    output.mkdir(parents=True, exist_ok=False)
    write(output / "result.json", dict(status="matched", plans=plans, network_calls=0,
        database_writes=0, historical_fills=0, model_calls=0, return_improvement=None))
    print(json.dumps(dict(status="matched", historical_fills=0, output=str(output))))


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        asyncio.run(worker(Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve()))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze_args = sub.add_parser("freeze")
    freeze_args.add_argument("--market-capture", required=True, type=Path)
    for command in ("execute", "replay"):
        sub.add_parser(command).add_argument("--experiment", required=True, type=Path)
    for command in sub.choices.values():
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "freeze":
        asyncio.run(freeze(args.market_capture.resolve(), args.output.resolve()))
    elif args.command == "execute":
        asyncio.run(execute(args.experiment.resolve(), args.output.resolve()))
    else:
        replay(args.experiment.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
