"""A new forward window: approved entries versus approved later top-ups.

The producer is a common external input. This experiment makes no claim about
research alpha. Both books first persist every ruling, then receive one shared
uncached execution observation. Archived legacy experiments are never upgraded.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import json
from time import monotonic

from ..domain.portfolio.execution_policy import approved_execution_target
from ..domain.portfolio.quote_freshness import is_fresh_quote, retrieved_after
from .continuous_evaluation import ContinuousEvaluation, SharedQuoteBatch, ZONE
from .market_data import collect_realtime_quote_batch, MarketDataService


ENGINE = "paired_approved_topup_v1"


def instant(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.replace(tzinfo=ZONE) if result.tzinfo is None else result


class ExecutionBatch(SharedQuoteBatch):
    async def get_execution_quote(self, ticker):
        return self.quotes.get(ticker)


class PairedAllocationEvaluation(ContinuousEvaluation):
    experiment_prefix = "approved_topup_forward_"
    allocation_names = ("entry_only_approved_v1", "later_approved_topup_v1")
    hypothesis = "Later qualified top-ups improve net return after fees versus entry-only approved budgets"
    decision_node = "allocation only; later approved top-ups; same producer, lifecycle, cooldown and safety"
    protocol_extension = {
        "engine_version": ENGINE,
        "allocations": dict(zip(("control", "candidate"), allocation_names)),
        "execution_phases": "review -> persist all arms' intents -> common uncached quote -> execute",
        "approval_rule": "exact forward producer round and committee rows; no reconstructed historical approvals",
        "candidate_scope": "top-ups only; no target reductions, changed cooldown or automatic re-entry",
        "observation_rule": "new independent window; old safety-blocked prefix excluded; missing time is not backfilled",
    }

    async def _batch(self, extra=(), *, execution=False):
        tickers = {"510300", *(t for t in extra if MarketDataService.is_supported_ticker(str(t or "")))}
        for sim in self.books.values():
            tickers.update(sim.positions)
            async with sim.db_service._connection.execute(
                "SELECT DISTINCT ticker FROM execution_intents WHERE status IN ('pending','deferred','executing')"
            ) as cursor:
                tickers.update(row[0] for row in await cursor.fetchall())
        trading = await self.market.is_trading_day()
        opened = trading and await self.market.is_market_open()
        requested = datetime.now(ZONE)
        if execution and not hasattr(self.market, "get_execution_quote"):
            raise RuntimeError("paired engine requires an uncached execution quote channel")
        getter = self.market.get_execution_quote if execution else self.market.get_current_quote
        quotes = await collect_realtime_quote_batch(tickers, getter)
        completed = datetime.now(ZONE)
        rejected = {}
        for ticker, quote in quotes.items():
            if execution and quote is not None and (
                not retrieved_after(quote, requested.isoformat(), now=completed)
                or not is_fresh_quote(quote, max_age_seconds=self.policy.get("max_realtime_quote_age_seconds", 120), now=completed)
            ):
                rejected[ticker] = quote
                quotes[ticker] = None
        batch = (ExecutionBatch if execution else SharedQuoteBatch)(quotes, trading, opened)
        batch.receipt = dict(requested_at=requested.isoformat(), completed_at=completed.isoformat(),
            purpose="execution" if execution else "review", quotes=quotes, rejected_quotes=rejected,
            trading_day=trading, market_open=opened)
        for sim in self.books.values():
            sim.quote_provider = batch
        return batch

    async def _ready(self):
        self._check_frozen()
        if (self.directory / "blocked.json").exists():
            await self._publish(json.loads((self.directory / "blocked.json").read_text()))
            return False
        if any((self.directory / p).exists() for p in ("active_input.json", "active_tick.json")):
            await self._block("interrupted_forward_operation")
            return False
        return True

    async def _import_producer(self, round_id, decisions, producer):
        """Copy observed provenance, never manufacture a round or an approval."""
        if not producer or producer.get("round", {}).get("id") != round_id:
            raise RuntimeError("exact producer round is required")
        approvals = producer.get("committee", [])
        if len(approvals) != len(decisions):
            raise RuntimeError("producer committee coverage differs")
        for decision in decisions:
            rows = [r for r in approvals if r["round_id"] == round_id
                    and r["decision_id"] == decision.get("decision_id") and r["ticker"] == decision.get("ticker")]
            if len(rows) != 1:
                raise RuntimeError("unique producer approval is required")
            row = rows[0]
            if (row["direction"] != decision.get("direction")
                or float(row["confidence"]) != float(decision.get("confidence") or 0)
                or float(row["target_position"]) != float(decision.get("target_position") or 0)
                or bool(row["quorum_met"]) != bool(decision.get("vote_quorum_met"))
                or not instant(self.started_at) <= instant(row["created_at"]) <= instant(producer["captured_at"])):
                raise RuntimeError("decision differs from its observed forward committee row")
        records = {"policy_snapshots": producer.get("policies", []),
                   "research_rounds": [producer["round"]], "simulation_committee_outcomes": approvals}
        for sim in self.books.values():
            async with sim.db_service.transaction(immediate=True) as conn:
                for table, rows in records.items():
                    for row in rows:
                        columns = list(row)
                        await conn.execute(f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) "
                            f"VALUES ({','.join('?' for _ in columns)})", tuple(row[c] for c in columns))
                        key = "id"
                        async with conn.execute(f"SELECT {','.join(columns)} FROM {table} WHERE {key}=?", (row[key],)) as cursor:
                            saved = await cursor.fetchone()
                        if saved is None or tuple(saved) != tuple(row[c] for c in columns):
                            raise RuntimeError("conflicting producer record: " + table)

    async def _prepare_lifecycle(self):
        reviews, prepared = {}, {}
        for name, sim in self.books.items():
            if name == "benchmark":
                continue
            sim.risk_context = replace(sim.risk_context, simulation_failures=await sim.refresh_simulation_risk_memory())
            reviews[name] = await sim.review_open_positions(prepare_only=True)
            prepared[name] = [r["execution"]["intent_id"] for r in reviews[name]
                             if (r.get("execution") or {}).get("action") == "prepared"]
            if any((r.get("execution") or {}).get("action") == "error" for r in reviews[name]):
                raise RuntimeError("lifecycle intent persistence failed")
        return reviews, prepared

    async def allocation_target(self, name, decision, assets, review):
        """Extension point for independently registered allocation hypotheses."""
        return approved_execution_target(float(decision.get("target_position") or 0), decision.get("confidence"))

    def contract_terms(self, name, decision):
        return {}

    async def _queue_prepared(self, prepared):
        """Use the existing durable deferred queue without attempting a fill."""
        for name, identities in prepared.items():
            sim = self.books[name]
            for identity in dict.fromkeys(identities):
                async with sim.db_service._connection.execute("SELECT * FROM execution_intents WHERE id=?", (identity,)) as cursor:
                    row = dict(await cursor.fetchone())
                if row["status"] not in {"pending", "deferred"}:
                    continue
                pending = await sim.record_pending_decision(ticker=row["ticker"], direction=row["direction"],
                    target_position=row["target_position"], confidence=row["confidence"],
                    reason=row["reason"], defer_code="paired_quote_pending", source=ENGINE,
                    round_id=row["round_id"], intent_id=identity)
                if pending is None:
                    raise RuntimeError("paired deferred ruling persistence failed")
                await sim._set_execution_intent_status(identity, status="deferred",
                    resolution="price-free ruling awaits common fresh execution batch", defer_code="paired_quote_pending")
        for sim in self.books.values():
            async with sim.db_service._connection.execute("SELECT COUNT(*) FROM simulation_pending_decisions "
                "WHERE status IN ('pending_next_trading_session','replaying') AND intent_id IS NULL") as cursor:
                if (await cursor.fetchone())[0]:
                    raise RuntimeError("paired queue contains a ruling without a durable intent")

    async def _execute(self):
        outcomes = {}
        for name, sim in self.books.items():
            # Absent observations keep the same ruling pending. Expiry still
            # runs for every entry; the normal queue applies all execution gates.
            replay = await sim.replay_pending_decisions(eligible_tickers={
                t for t, q in sim.quote_provider.quotes.items() if q is not None})
            items = replay.get("results", [])
            if any(r.get("action") == "error" for r in items):
                await self._block("incomplete_paired_execution", arm=name)
                raise RuntimeError("paired execution failed; original books and marker retained")
            outcomes[name] = items
        return outcomes

    async def capture_round(self, round_id, decisions, *, producer_records=None):
        async with self._lock:
            if not await self._ready():
                return
            path = self.directory / "inputs" / (round_id + ".json")
            if path.exists():
                if not path.with_suffix(".complete.json").exists():
                    await self._block("interrupted_common_input_batch", round_id=round_id)
                return
            await self._import_producer(round_id, decisions, producer_records)
            review = await self._batch(d.get("ticker") for d in decisions)
            views = {name: await sim.calculate_assets() for name, sim in self.books.items()}
            active = self.directory / "active_input.json"
            packet = dict(round_id=round_id, started_at=datetime.now(ZONE).isoformat(),
                decisions=decisions, producer=producer_records, accounts=views, review_market=review.receipt)
            self._write(active, packet)
            reviews, prepared = await self._prepare_lifecycle()
            filters = {}
            for name in ("control", "candidate", "stress"):
                sim = self.books[name]
                filters[name] = []
                exits = {r["ticker"] for r in reviews[name] if (r.get("execution") or {}).get("intent_id")}
                for d in decisions:
                    ticker, direction = d.get("ticker"), d.get("direction")
                    code = None
                    if not MarketDataService.is_supported_ticker(str(ticker or "")):
                        code = "unsupported_ticker"
                    elif not d.get("vote_quorum_met"):
                        code = "committee_incomplete"
                    elif direction not in {"long", "sell", "short"}:
                        code = "committee_hold"
                    elif ticker in exits:
                        code = "lifecycle_exit_has_priority"
                    elif direction == "long" and float(d.get("confidence") or 0) < float(self.policy.get("min_committee_confidence", .55)):
                        code = "confidence_below_threshold"
                    elif direction == "long" and not views[name].get("valuation_complete"):
                        code = "valuation_incomplete"
                    elif direction == "long" and name == "control" and ticker in sim.positions:
                        code = "already_held_long"
                    elif direction in {"sell", "short"} and ticker not in sim.positions:
                        code = "short_without_position"
                    if code:
                        filters[name].append(dict(ticker=ticker, decision_id=d.get("decision_id"), code=code))
                        continue
                    target = await self.allocation_target(name, d, views[name], review) if direction == "long" else 0
                    if direction == "long" and target <= 0:
                        filters[name].append(dict(ticker=ticker, code="allocation_budget_unavailable"))
                        continue
                    identity = await sim.create_execution_intent(ticker=ticker, direction=direction,
                        target_position=target, confidence=d.get("confidence"),
                        reason=f"paired_allocation:{round_id}:{name}; original_budget={d.get('target_position')}",
                        round_id=round_id, decision_id=d["decision_id"],
                        idempotency_key=f"paired:{round_id}:{d['decision_id']}",
                        **self.contract_terms(name, d))
                    if not identity:
                        raise RuntimeError("paired intent persistence failed")
                    prepared[name].append(identity)
            await self._queue_prepared(prepared)
            # Every new lifecycle and committee intent in every arm is durable.
            execution = await self._batch((d.get("ticker") for d in decisions), execution=True)
            packet.update(reviews=reviews, intents=prepared, filters=filters, execution_market=execution.receipt)
            self._write_packet(path, packet)
            outcomes = await self._execute()
            self._write(path.with_suffix(".complete.json"), outcomes)
            active.unlink()
            await self._publish({**self.last_status, "last_input_round": round_id,
                "last_input_at": packet["started_at"], "last_input_decisions": len(decisions),
                "last_input_filters": filters, "engine_version": ENGINE})

    async def tick(self):
        if monotonic() < self._next_due or self._lock.locked():
            return self.last_status
        self._next_due = monotonic() + 60
        async with self._lock:
            if not await self._ready():
                return self.last_status
            review = await self._batch()
            active = self.directory / "active_tick.json"
            previous = self.directory / "last_tick.json"
            prior = json.loads(previous.read_text()).get("started_at") if previous.exists() else None
            packet = dict(started_at=datetime.now(ZONE).isoformat(), review_market=review.receipt,
                previous_observed_at=prior, observation_gap_seconds=(datetime.now(ZONE)-instant(prior)).total_seconds() if prior else None)
            self._write(active, packet)
            reviews, prepared = await self._prepare_lifecycle()
            benchmark = self.books["benchmark"]
            if review.opened:
                async with benchmark.db_service._connection.execute("SELECT COUNT(*) FROM simulation_trades") as cursor:
                    bought = (await cursor.fetchone())[0]
                if not bought:
                    identity = await benchmark.create_execution_intent(ticker="510300", direction="long",
                        target_position=1, confidence=1, reason="same-window executable benchmark; frozen protocol mandate",
                        idempotency_key="paired_benchmark_entry")
                    if not identity:
                        raise RuntimeError("benchmark intent persistence failed")
                    prepared["benchmark"] = [identity]
            await self._queue_prepared(prepared)
            execution = await self._batch(execution=True)
            packet.update(reviews=reviews, intents=prepared, execution_market=execution.receipt)
            packet["outcomes"] = await self._execute()
            self._write(previous, packet)
            active.unlink()
            await self._completed_session()
            await self._publish({**self.last_status, "last_observed_at": packet["started_at"], "engine_version": ENGINE})
        return self.last_status
