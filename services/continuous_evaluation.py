"""Forward allocation comparison, using the production simulator in private books.

Only the allocation policy differs. Research decisions and each controlled quote
batch are shared; no new model calls, historical signals or production fills are
created. Engineering repairs are common to both arms. Qualification remains
unproven until the original sample/risk/statistical requirements are satisfied.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, time
import hashlib
import json
import logging
import math
from pathlib import Path
import shutil
from time import monotonic
from zoneinfo import ZoneInfo

from ..core import PROJECT_ROOT
from ..domain.portfolio.execution_policy import approved_execution_target
from ..domain.portfolio.shadow import ExecutionClock, ExperimentStatus
from .database import DatabaseService
from .exchange_calendar import load_exchange_calendar
from .experiment_registry_service import ExperimentRegistryService
from .experiment_store import ExperimentStore, stable_hash
from .evaluation_engine import EvaluationResult, EvaluationSpec
from .statistical_validation import validate_experiment_family
from ..domain.portfolio.shadow import SleeveId
from .heuristic_policy import HeuristicRiskContext
from .investment_simulation import InvestmentSimulation
from .market_data import collect_realtime_quote_batch, MarketDataService
from .policy_snapshot_service import PolicyManifest, PolicySnapshotService, runtime_code_artifacts

logger = logging.getLogger(__name__)
ZONE = ZoneInfo("Asia/Shanghai")


class SharedQuoteBatch:
    def __init__(self, quotes, trading, opened):
        self.quotes, self.trading, self.opened = quotes, trading, opened

    async def is_trading_day(self):
        return self.trading

    async def is_market_open(self):
        return self.opened

    async def get_current_quote(self, ticker):
        return self.quotes.get(ticker)


def arm_target(decision, assets, candidate_count, *, control, market_open):
    """Frozen comparator: old deployment sizing versus the repaired budget."""
    requested = float(decision.get("target_position") or 0)
    confidence = float(decision.get("confidence") or 0)
    if not control:
        return approved_execution_target(requested, confidence)
    equity = float(assets.get("total_assets") or 0)
    floor = float(assets.get("deployment_gap") or 0) / equity / candidate_count if equity > 0 and candidate_count else 0
    target = max(requested, floor)
    if market_open:
        target = approved_execution_target(target, confidence)
    return min(target, .3 if confidence < .4 else .5 if confidence < .6 else 1.0)


class ContinuousEvaluation:
    experiment_prefix = "approved_budget_forward_"
    allocation_names = ("legacy_deployment_sizing_v1", "approved_budget_v2")
    hypothesis = "Honoring approved position budgets improves net return versus the former deployment floor"
    decision_node = "allocation_only; shared recorded committee decisions; common repaired engine"
    protocol_extension = {}

    def __init__(self, db_service, market, *, root: Path, policy: dict,
                 source_root: Path = PROJECT_ROOT):
        self.db = db_service
        self.market = market
        self.root, self.source_root = Path(root), Path(source_root)
        self.policy = dict(policy)
        self.registry = ExperimentRegistryService(db_service.db_path)
        self.books = {}
        self._next_due = 0.0
        self._lock = asyncio.Lock()
        self.last_status = {}

    async def initialize(self):
        artifacts = runtime_code_artifacts(self.source_root, "run_discussion.py")
        version = hashlib.sha256(json.dumps([artifacts, self.policy], sort_keys=True).encode()).hexdigest()[:20]
        self.experiment_id = self.experiment_prefix + version
        self.directory = self.root / self.experiment_id
        self.directory.mkdir(parents=True, exist_ok=True)
        self.artifacts = artifacts
        existing = await self.registry.get(self.experiment_id)
        if existing is None and (self.directory / "metadata.db").exists():
            raise RuntimeError("existing private metadata cannot be replaced by a new bootstrap")
        protocol_path = self.directory / "protocol.json"
        protocol = json.loads(protocol_path.read_text()) if protocol_path.exists() else None
        if protocol and (protocol["artifacts"] != artifacts or protocol["simulation"] != self.policy):
            raise RuntimeError("existing forward protocol differs from its frozen engine")
        if protocol:
            self.started_at = protocol["started_at"]
        elif existing:
            async with self.db._connection.execute("SELECT observation_start FROM experiment_registry WHERE experiment_id=?",
                                                   (self.experiment_id,)) as cursor:
                self.started_at = (await cursor.fetchone())[0]
        else:
            self.started_at = datetime.now(ZONE).isoformat()
        protocol = {"experiment_id": self.experiment_id, "started_at": self.started_at,
                    "artifacts": artifacts, "simulation": self.policy,
                    "scope": "allocation comparison, not independent LLM research alpha",
                    "qualification": {"sessions": 252, "closed_trades": 100,
                        "dsr_probability": .95, "pbo_probability_below": .50,
                        "positive_excess_and_3x_slippage_required": True},
                    **self.protocol_extension}
        # Persist the protocol before registration. A restart after either
        # write retains the original observation window and experiment ID.
        if not protocol_path.exists():
            self._write(protocol_path, protocol)
        if existing is None:
            source = self.directory / "source"
            source.mkdir(exist_ok=True)
            for relative in artifacts:
                target = source / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != artifacts[relative]:
                    raise RuntimeError("existing frozen source differs")
                if not target.exists():
                    shutil.copyfile(self.source_root / relative, target)
            snapshots = []
            for allocation in self.allocation_names:
                record = await PolicySnapshotService(self.db.db_path).register(PolicyManifest(
                    policy_family="forward_allocation", version=allocation + "_" + version,
                    code_artifacts=artifacts,
                    effective_config={"simulation": self.policy, "allocation": allocation},
                    source_manifest_path=str(source), changes_behavior=True,
                    change_reason="Compare allocation budgets with one common repaired execution engine"))
                snapshots.append(record.snapshot_id)
            await self.registry.register(experiment_id=self.experiment_id,
                hypothesis=self.hypothesis,
                control_policy_snapshot_id=snapshots[0], candidate_policy_snapshot_id=snapshots[1],
                decision_node_changed=self.decision_node,
                information_cutoff_rule="persist common decisions, account views and source-time quotes before either arm",
                observation_start=self.started_at, benchmark_definition="510300 executable buy-and-hold, same quote batch and fees",
                execution_clock=ExecutionClock.PRODUCTION_EQUIVALENT_FORWARD,
                universe_policy="naturally approved tickers only; no historical candidate injection",
                fee_version=str(self.policy.get("cost_model_version", "configured")),
                instrument_rule_version="shared_production_simulation",
                primary_metric="candidate_minus_control_net_return",
                failure_rule="retain losses; reject invalid common inputs; never promote on engineering tests",
                minimum_samples=252, review_schedule="each completed session; 100 closed trades plus original qualification gates",
                rollback_rule="stop comparison arms only; preserve ledger; never write production account",
                source_runtime_manifest=str(source))
        else:
            for relative, expected in artifacts.items():
                frozen = self.directory / "source" / relative
                if not frozen.exists() or hashlib.sha256(frozen.read_bytes()).hexdigest() != expected:
                    raise RuntimeError("registered forward source is missing or changed")
        for name in ("control", "candidate", "stress", "benchmark"):
            db = DatabaseService(str(self.directory / (name + ".db")))
            await db._init_db()
            context = HeuristicRiskContext(None, "frozen_forward_allocation", None,
                float(self.policy.get("max_single_position", 1)), False, "", [],
                min_confidence=float(self.policy.get("min_committee_confidence", .55)),
                max_gross=float(self.policy.get("max_gross", 1)))
            sim = InvestmentSimulation(db, SharedQuoteBatch({}, False, False), risk_context=context)
            await sim.initialize()
            if name == "stress":
                sim.cost_schedule = replace(sim.cost_schedule, slippage_rate=sim.cost_schedule.slippage_rate * 3,
                    version=sim.cost_schedule.version + "_3x_slippage")
            self.books[name] = sim
        previous = self.directory / "status.json"
        status = json.loads(previous.read_text()) if previous.exists() else {
            "evaluation_status": "insufficient_samples", "reason": "forward_observation_started",
            "last_usable_session": None, "candidate_minus_control_net_return": None}
        for packet in (self.directory / "inputs").glob("*.json"):
            if not packet.name.endswith(".complete.json") and not packet.with_suffix(".complete.json").exists():
                await self._block("interrupted_common_input_batch", round_id=packet.stem)
                break
        if any((self.directory / name).exists() for name in ("active_input.json", "active_tick.json")):
            await self._block("interrupted_forward_operation")
        if (self.directory / "blocked.json").exists():
            status = json.loads((self.directory / "blocked.json").read_text())
        await self._publish(status)

    @staticmethod
    def _write(path, value):
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))
        temporary.replace(path)

    async def _publish(self, value):
        value.setdefault("last_usable_session", self.last_status.get("last_usable_session"))
        value = {**value, "experiment_id": self.experiment_id,
                 "measured_at": datetime.now(ZONE).isoformat(), "promotion_eligible": False}
        self._write(self.directory / "status.json", value)
        async with self.db.transaction() as conn:
            await conn.execute("INSERT OR REPLACE INTO system_stats(key,value,updated_at) "
                "VALUES ('continuous_evaluation_status',?,?)",
                (json.dumps(value, ensure_ascii=False), value["measured_at"]))
        self.last_status = value

    def _check_frozen(self):
        from ..core.config import get_config
        if dict(get_config().get("simulation", {})) != self.policy:
            raise RuntimeError("forward policy changed; a new experiment is required")
        for relative, expected in self.artifacts.items():
            if hashlib.sha256((self.source_root / relative).read_bytes()).hexdigest() != expected:
                raise RuntimeError("forward engine changed; a new frozen experiment is required")

    async def _batch(self, extra=()):
        tickers = {"510300", *extra}
        for sim in self.books.values():
            tickers.update(sim.positions)
            async with sim.db_service._connection.execute("SELECT DISTINCT ticker FROM execution_intents "
                    "WHERE status IN ('pending','deferred','executing')") as cursor:
                tickers.update(row[0] for row in await cursor.fetchall())
        quotes = await collect_realtime_quote_batch(tickers, self.market.get_current_quote)
        trading = await self.market.is_trading_day()
        opened = trading and await self.market.is_market_open()
        batch = SharedQuoteBatch(quotes, trading, opened)
        for sim in self.books.values():
            sim.quote_provider = batch
        return batch

    async def capture_round(self, round_id, decisions):
        """Capture genuine forward inputs once; each private intent is idempotent."""
        async with self._lock:
            self._check_frozen()
            if (self.directory / "blocked.json").exists():
                return
            if any((self.directory / name).exists() for name in ("active_input.json", "active_tick.json")):
                await self._block("interrupted_forward_operation")
                return
            path = self.directory / "inputs" / (round_id + ".json")
            if path.exists():
                if not path.with_suffix(".complete.json").exists():
                    await self._block("interrupted_common_input_batch", round_id=round_id)
                return  # Never replay old market ticks as fresh fills.
            compact = [{k: d.get(k) for k in ("ticker", "direction", "confidence", "target_position", "decision_id")}
                       for d in decisions if MarketDataService.is_supported_ticker(str(d.get("ticker") or ""))]
            batch = await self._batch(d["ticker"] for d in compact)
            views = {name: await sim.calculate_assets() for name, sim in self.books.items()}
            active = self.directory / "active_input.json"
            self._write(active, {"round_id": round_id, "decisions": compact, "accounts": views,
                                 "started_at": datetime.now(ZONE).isoformat()})
            prepared = {name: [] for name in ("control", "candidate", "stress")}
            for name in ("control", "candidate", "stress"):
                sim = self.books[name]
                eligible = [d for d in compact if d["direction"] in {"long", "short", "sell"}
                    and (d["direction"] != "long" or (d["ticker"] not in sim.positions and
                        float(d.get("confidence") or 0) >= float(self.policy.get("min_committee_confidence", .55))))]
                count = sum(d["direction"] == "long" for d in eligible)
                for d in eligible[:5]:
                    target = arm_target(d, views[name], count, control=name == "control",
                                        market_open=batch.opened) if d["direction"] == "long" else 0
                    if d["direction"] != "long" and d["ticker"] not in sim.positions:
                        continue
                    intent = await sim.create_execution_intent(ticker=d["ticker"], direction=d["direction"],
                        target_position=target, confidence=d.get("confidence"),
                        reason=f"forward_comparison:{round_id}:{name}; original_budget={d.get('target_position')}",
                        idempotency_key=f"forward:{round_id}:{d.get('decision_id') or d['ticker']}")
                    prepared[name].append(intent)
            # Execution quotes are captured only after every price-free intent
            # is durable. Neither arm gets to see the other's fills first.
            execution = await self._batch(d["ticker"] for d in compact)
            self._write_packet(path, {"round_id": round_id, "captured_at": datetime.now(ZONE).isoformat(),
                "decisions": compact, "research_quotes": batch.quotes, "quotes": execution.quotes,
                "market_open": execution.opened, "accounts": views, "intents": prepared})
            outcomes = {}
            for name, intents in prepared.items():
                outcomes[name] = []
                for intent in intents:
                    result = await self.books[name].execute_intent(intent)
                    outcomes[name].append(result)
                    if result.get("action") == "error":
                        await self._block("incomplete_arm_execution", round_id=round_id, arm=name)
                        raise RuntimeError("forward arm execution failed; retained for inspection")
            self._write(path.with_suffix(".complete.json"), outcomes)
            active.unlink()

    async def _block(self, reason, **detail):
        value = {"evaluation_status": "data_blocked", "reason": reason, **detail}
        self._write(self.directory / "blocked.json", value)
        await self._publish(value)

    def _write_packet(self, path, packet):
        path.parent.mkdir(exist_ok=True, parents=True)
        self._write(path, packet)

    async def tick(self):
        if monotonic() < self._next_due or self._lock.locked():
            return self.last_status
        self._next_due = monotonic() + 60
        async with self._lock:
            self._check_frozen()
            if (self.directory / "blocked.json").exists():
                await self._publish(json.loads((self.directory / "blocked.json").read_text()))
                return self.last_status
            if any((self.directory / name).exists() for name in ("active_input.json", "active_tick.json")):
                await self._block("interrupted_forward_operation")
                return self.last_status
            for packet in (self.directory / "inputs").glob("*.json"):
                if not packet.name.endswith(".complete.json") and not packet.with_suffix(".complete.json").exists():
                    await self._block("interrupted_common_input_batch", round_id=packet.stem)
                    return self.last_status
            batch = await self._batch()
            active = self.directory / "active_tick.json"
            packet = {"started_at": datetime.now(ZONE).isoformat(), "quotes": batch.quotes,
                      "market_open": batch.opened, "outcomes": {}}
            self._write(active, packet)
            # All books use exactly the same lifecycle and deferred-fill engine.
            for name, sim in self.books.items():
                if name != "benchmark":
                    sim.risk_context = replace(sim.risk_context,
                        simulation_failures=await sim.refresh_simulation_risk_memory())
                    reviews = await sim.review_open_positions()
                    replay = await sim.replay_pending_decisions()
                    packet["outcomes"][name] = {"reviews": reviews, "pending_replay": replay}
                    if any((item.get("execution") or item).get("action") == "error"
                           for item in list(reviews or []) + list((replay or {}).get("results", []))):
                        await self._block("incomplete_arm_maintenance", arm=name)
                        raise RuntimeError("forward maintenance failed; retained for inspection")
                elif batch.opened and not sim.positions and await sim.count_trades_on_date() == 0:
                    async with sim.db_service._connection.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
                        already_bought = (await cursor.fetchone())[0]
                    if not already_bought:
                        intent = await sim.create_execution_intent(ticker="510300", direction="long", target_position=1,
                            confidence=1, reason="same-window executable buy and hold benchmark",
                            idempotency_key="forward_benchmark_entry")
                        result = await sim.execute_intent(intent)
                        packet["outcomes"][name] = result
                        if result.get("action") == "error":
                            await self._block("incomplete_benchmark_entry")
                            raise RuntimeError("forward benchmark failed; retained for inspection")
            self._write(self.directory / "last_tick.json", packet)
            active.unlink()
            await self._completed_session()
            await self._publish({**self.last_status, "last_observed_at": packet["started_at"]})
        return self.last_status

    async def _completed_session(self):
        now = datetime.now(ZONE)
        sessions = {}
        for year in range(int(self.started_at[:4]), now.year + 1):
            sessions.update(load_exchange_calendar(year).sessions)
        completed = [day for day, opened in sessions.items() if opened and
                     (day < now.date().isoformat() or (day == now.date().isoformat() and now.time() >= time(15, 5)))]
        if not completed:
            return
        pending_days = [day for day in sorted(completed) if day > self.started_at[:10]
                        and not (self.directory / "sessions" / (day + ".json")).exists()]
        for day in pending_days[:3]:
            if not await self._value_day(day):
                break

    async def _value_day(self, day):
        # No samples before this experiment's forward observation actually began.
        if day <= self.started_at[:10]:
            return
        path = self.directory / "sessions" / (day + ".json")
        if path.exists():
            return
        accounts = {}
        for name, sim in self.books.items():
            cash, fees, closed, trades = 10000.0, 0.0, 0, 0
            positions = {}
            async with sim.db_service._connection.execute(
                "SELECT ticker,direction,shares,price,fee FROM simulation_trades "
                "WHERE datetime(traded_at)<=datetime(?) ORDER BY traded_at,id", (day + "T15:00:00",)) as cursor:
                for ticker, side, shares, price, fee in await cursor.fetchall():
                    delta = shares if side == "buy" else -shares
                    cash -= delta * price + fee
                    positions[ticker] = positions.get(ticker, 0) + delta
                    if side == "sell" and positions[ticker] == 0:
                        closed += 1
                    fees += fee
                    trades += 1
            accounts[name] = {"cash": cash, "fees": fees, "closed_sales": closed, "trades": trades,
                              "positions": {t: s for t, s in positions.items() if s}}
        tickers = set().union(*(row["positions"] for row in accounts.values()))
        marks, gaps = {}, []
        bars = await asyncio.gather(*(self.market.get_unadjusted_ohlc(ticker, day, day) for ticker in sorted(tickers)),
                                    return_exceptions=True)
        for ticker, series in zip(sorted(tickers), bars):
            valid = [bar for bar in series if bar.get("date") == day and bar.get("adjustment") == "raw"
                     and math.isfinite(float(bar.get("close", 0))) and float(bar["close"]) > 0] if isinstance(series, list) else []
            if len(valid) != 1:
                gaps.append(ticker)
            else:
                marks[ticker] = valid[0]
        if gaps:
            await self._publish({"evaluation_status": "data_blocked", "reason": "missing_common_completed_session",
                                 "requested_session": day, "missing_tickers": gaps})
            return False
        values = {}
        for name, account in accounts.items():
            assets = account["cash"] + sum(shares * float(marks[t]["close"]) for t, shares in account["positions"].items())
            values[name] = {**account, "total_assets": assets, "net_return": assets / 10000 - 1}
        payload = {"session": day, "marks": marks, "accounts": values,
                   "candidate_minus_control_net_return": values["candidate"]["net_return"] - values["control"]["net_return"],
                   "candidate_minus_benchmark_net_return": values["candidate"]["net_return"] - values["benchmark"]["net_return"],
                   "valuation_method": "same completed unadjusted daily close; execution remains realtime only"}
        self._write_packet(path, payload)
        await self._evaluate_saved_sessions()
        return True

    async def _evaluate_saved_sessions(self):
        rows = [json.loads(p.read_text()) for p in sorted((self.directory / "sessions").glob("*.json"))]
        if not rows:
            return
        curves = {name: [] for name in self.books}
        for name in curves:
            previous, peak = 10000.0, 10000.0
            for row in rows:
                assets = row["accounts"][name]["total_assets"]
                peak = max(peak, assets)
                curves[name].append({"date": row["session"], "total_assets": assets,
                    "net_return": assets / previous - 1, "drawdown": assets / peak - 1})
                previous = assets
        family_returns = {name: [p["net_return"] for p in curves[name]] for name in ("control", "candidate", "stress")}
        count = len(rows)
        latest = rows[-1]
        missing_profiles = []
        universe = set()
        for sim in self.books.values():
            async with sim.db_service._connection.execute("SELECT DISTINCT ticker FROM simulation_trades") as cursor:
                universe.update(row[0] for row in await cursor.fetchall())
        for ticker in sorted(universe):
            async with self.db._connection.execute("SELECT listed_at FROM instrument_master WHERE ticker=?", (ticker,)) as cursor:
                profile = await cursor.fetchone()
            if not profile or not profile[0]:
                missing_profiles.append(ticker)
        metrics = {}
        for name in ("control", "candidate", "stress"):
            validation = validate_experiment_family(family_returns[name], family_returns)
            account = latest["accounts"][name]
            net = account["net_return"]
            cagr = (1 + net) ** (252 / count) - 1 if count >= 252 and net > -1 else None
            drawdown = min(p["drawdown"] for p in curves[name])
            reasons = []
            if count < 252: reasons.append("insufficient_sessions")
            if account["closed_sales"] < 100: reasons.append("insufficient_closed_trades")
            if cagr is None or cagr < .15: reasons.append("cagr_target_not_proven")
            if drawdown < -.15: reasons.append("drawdown_budget_breached")
            if net <= latest["accounts"]["benchmark"]["net_return"]: reasons.append("positive_benchmark_excess_not_proven")
            if net <= latest["accounts"]["control"]["net_return"] and name == "candidate": reasons.append("positive_control_excess_not_proven")
            if latest["accounts"]["stress"]["net_return"] <= 0: reasons.append("three_x_slippage_profit_not_proven")
            if validation.dsr_probability is None or validation.dsr_probability < .95: reasons.append("dsr_not_proven")
            if validation.pbo_probability is None or validation.pbo_probability >= .50: reasons.append("pbo_not_proven")
            if missing_profiles: reasons.append("instrument_lifecycle_metadata_incomplete")
            if not latest["accounts"]["benchmark"]["trades"]: reasons.append("executable_benchmark_entry_missing")
            metrics[name] = {"total_return": net, "sessions": count, "closed_trades": account["closed_sales"],
                "cost_paid": account["fees"], "net_cagr": cagr, "max_drawdown": drawdown,
                "data_quality": {"usable": not missing_profiles, "missing_lifecycle_profiles": missing_profiles},
                "statistical_validation": validation.as_dict(),
                "qualification": {"qualified": not reasons, "label": "待独立审查" if not reasons else "未证明", "reasons": reasons}}
            result = EvaluationResult(EvaluationSpec(SleeveId.LLM_RESEARCH,
                self.experiment_id + "_" + name, qualification_sessions=252, minimum_closed_trades=100),
                metrics[name], tuple(curves[name]), (), (), (), "not_proven" if reasons else "qualified",
                tuple(sorted(universe)), latest["session"], engine_version="production_forward_allocation_v2")
            await ExperimentStore(self.db.db_path).persist(result, cost_schedule=self.books[name].cost_schedule,
                data_hash=stable_hash(rows), config={"scope": "forward_allocation_only", "origin_inputs": str(self.directory / "inputs")},
                experiment_id=self.experiment_id, arm=name)
        candidate = metrics["candidate"]
        validation = candidate["statistical_validation"]
        mature = count >= 252 and latest["accounts"]["candidate"]["closed_sales"] >= 100
        status = (ExperimentStatus.ELIGIBLE if candidate["qualification"]["qualified"] else
                  ExperimentStatus.REJECTED if mature else ExperimentStatus.INSUFFICIENT_SAMPLES)
        await self.registry.transition(self.experiment_id, status, event_type="ForwardSessionValued",
            payload={"session": latest["session"], "sessions": count, "qualification": candidate["qualification"]},
            last_usable_session=latest["session"])
        await self._publish({"evaluation_status": "current" if mature else "insufficient_samples",
            "last_usable_session": latest["session"], "sessions": count, "accounts": latest["accounts"],
            "candidate_minus_control_net_return": latest["candidate_minus_control_net_return"],
            "qualification": candidate["qualification"], "statistical_validation": validation})

    async def close(self):
        for sim in self.books.values():
            await sim.db_service.close()
