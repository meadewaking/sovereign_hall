"""Pin a complete forward engine across changes to the production workspace.

The child owns only private books and metadata. The parent supplies controlled
market observations and mirrors evaluation records, never financial records.
Existing protocols, observation starts and interrupted experiments are retained.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
import shutil
import sys

import yaml

from ..core import PROJECT_ROOT
from .continuous_evaluation import ContinuousEvaluation
from .database import DatabaseService

logger = logging.getLogger(__name__)


class ForwardOperationError(RuntimeError):
    """A valid worker error response leaves the RPC stream synchronized."""


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class FrozenForwardEvaluation:
    def __init__(self, db_service, market, *, root: Path, policy: dict,
                 source_root: Path = PROJECT_ROOT, bootstrap_engine: str = "legacy",
                 selection_name: str = "active_runtime.json", new_window_reason: str | None = None):
        self.db, self.market = db_service, market
        self.root, self.source_root = Path(root).resolve(), Path(source_root).resolve()
        self.policy = dict(policy)
        if Path(selection_name).name != selection_name:
            raise ValueError("forward selection must be a local filename")
        if new_window_reason and (bootstrap_engine != "paired_approved_topup_v1"
                                  or selection_name == "active_runtime.json"):
            raise ValueError("a new window must first be prepared under a separate selection")
        self.bootstrap_engine, self.selection_name = bootstrap_engine, selection_name
        self.new_window_reason = new_window_reason
        self.last_status = {}
        self._process = None
        self._lock = asyncio.Lock()
        self._requests = {}
        self._captures = {}
        self._response_wait_seconds = 2.0
        self._data_timeout_seconds = 12.0
        self._closing = False

    @staticmethod
    def _write(path, value):
        ContinuousEvaluation._write(path, value)

    async def initialize(self):
        if self._process is not None:
            raise RuntimeError("forward runtime is already initialized")
        self.root.mkdir(parents=True, exist_ok=True)
        pointer = self.root / self.selection_name
        if pointer.exists():
            selected = json.loads(pointer.read_text())
            experiment_id = selected["experiment_id"]
        else:
            async with self.db._connection.execute(
                    "SELECT value FROM system_stats WHERE key='continuous_evaluation_status'") as cursor:
                saved = await cursor.fetchone()
            experiment_id = json.loads(saved[0]).get("experiment_id") if saved and not self.new_window_reason else None
            if experiment_id and not (self.root / experiment_id / "protocol.json").is_file():
                raise RuntimeError("recorded forward experiment unavailable; refusing to reset")
            if not experiment_id:
                if list(self.root.glob("*/protocol.json")) and not self.new_window_reason:
                    raise RuntimeError("existing forward experiments need an explicit active identity")
                # Bootstrap one protocol in private metadata, never the account DB.
                staging = self.root / "bootstrap.db"
                if staging.exists():
                    raise RuntimeError("unfinished forward bootstrap requires explicit recovery")
                db = DatabaseService(str(staging))
                inner = None
                try:
                    await db._init_db()
                    engine = ContinuousEvaluation
                    if self.bootstrap_engine != "legacy":
                        from .paired_allocation import ENGINE, PairedAllocationEvaluation
                        if self.bootstrap_engine != ENGINE:
                            raise ValueError("unsupported forward bootstrap engine")
                        engine = PairedAllocationEvaluation
                    inner = engine(db, self.market, root=self.root,
                        policy=self.policy, source_root=self.source_root)
                    await inner.initialize()
                    experiment_id = inner.experiment_id
                finally:
                    if inner is not None:
                        await inner.close()
                    await db.close()
                metadata_path = self.root / experiment_id / "metadata.db"
                if metadata_path.exists():
                    raise RuntimeError("refusing to replace existing forward metadata")
                staging.replace(metadata_path)
                # A newly registered private metadata DB also needs the
                # observed instrument profiles used by qualification.
                import aiosqlite
                async with self.db._connection.execute("SELECT * FROM instrument_master") as cursor:
                    columns = [item[0] for item in cursor.description]
                    profiles = [dict(zip(columns, row)) for row in await cursor.fetchall()]
                async with aiosqlite.connect(self.root / experiment_id / "metadata.db") as conn:
                    for row in profiles:
                        await self._insert(conn, "instrument_master", row)
                    await conn.commit()
            selected = None

        if not isinstance(experiment_id, str) or Path(experiment_id).name != experiment_id:
            raise RuntimeError("invalid forward identity")
        self.experiment_id = experiment_id
        self.directory = self.root / experiment_id
        protocol = json.loads((self.directory / "protocol.json").read_text())
        if protocol["experiment_id"] != experiment_id:
            raise RuntimeError("forward protocol identity differs")
        self.started_at = protocol["started_at"]
        self.frozen_policy = protocol["simulation"]
        self.engine_version = protocol.get("engine_version", "legacy")
        self._verify_source(protocol)

        if selected is None:
            # Adoption uses the exact current experiment. No past inputs are
            # replayed, no balances are reset and no blocked marker is removed.
            await self._seed_metadata()
            worker = self.directory / "runtime_worker.py"
            if not (self.directory / "runtime.json").exists():
                shutil.copyfile(Path(__file__).with_name("forward_worker.py"), worker)
                config = {"simulation": self.frozen_policy,
                          "database": {"sqlite_path": str(self.directory / "metadata.db")}}
                (self.directory / "config.yaml").write_text(yaml.safe_dump(config, allow_unicode=True))
                self._write(self.directory / "runtime.json", {
                    "version": "frozen_forward_runtime_v2" if self.engine_version != "legacy" else "frozen_forward_runtime_v1",
                    "engine_version": self.engine_version, "new_window_reason": self.new_window_reason,
                    "experiment_id": experiment_id,
                    "protocol_sha256": digest(self.directory / "protocol.json"),
                    "config_sha256": digest(self.directory / "config.yaml"),
                    "worker_sha256": digest(worker), "adopted_at": datetime.now().astimezone().isoformat(),
                    "scope": "unchanged frozen allocation engine; producer decisions and controlled data are common inputs"})
            selected = {"experiment_id": experiment_id,
                        "runtime_sha256": digest(self.directory / "runtime.json")}
            self._write(pointer, selected)

        runtime = json.loads((self.directory / "runtime.json").read_text())
        self.runtime_version = runtime["version"]
        if digest(self.directory / "runtime.json") != selected["runtime_sha256"]:
            raise RuntimeError("pinned forward runtime changed")
        for name, key in (("protocol.json", "protocol_sha256"), ("config.yaml", "config_sha256"),
                          ("runtime_worker.py", "worker_sha256")):
            if digest(self.directory / name) != runtime[key]:
                raise RuntimeError("pinned forward artifact changed: " + name)
        if not (self.directory / "metadata.db").is_file():
            raise RuntimeError("pinned forward metadata missing; refusing to reset")

        stderr = self.directory / "runtime.stderr.log"
        if stderr.exists() and stderr.stat().st_size > 2 * 1024**2:
            stderr.replace(stderr.with_suffix(".log.1"))
        with stderr.open("ab") as log:
            self._process = await asyncio.create_subprocess_exec(sys.executable, "-B",
                str(self.directory / "runtime_worker.py"), str(self.directory), cwd=self.directory,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=log,
                limit=8 * 1024 * 1024)
        try:
            message = await asyncio.wait_for(self._receive(), 15)
            if message["kind"] != "ready":
                raise RuntimeError("frozen worker did not initialize")
            self.receipt = message["receipt"]
            await self._sync_metadata()
            await self._publish(message["status"])
        except BaseException:
            await self._abort()
            raise

    def _verify_source(self, protocol):
        source = self.directory / "source"
        for relative, expected in protocol["artifacts"].items():
            path = (source / relative).resolve()
            if not path.is_relative_to(source) or digest(path) != expected:
                raise RuntimeError("frozen forward source changed: " + relative)

    async def _seed_metadata(self):
        path = self.directory / "metadata.db"
        if path.exists():
            return
        db = DatabaseService(str(path))
        try:
            await db._init_db()
            rows = await self._read_rows(self.db._connection, include_profiles=True)
            async with db.transaction() as conn:
                for table, records in rows.items():
                    for row in records:
                        await self._insert(conn, table, row)
        finally:
            await db.close()

    async def _read_rows(self, conn, *, include_profiles=False):
        queries = {
            "policy_snapshots": ("SELECT * FROM policy_snapshots WHERE id IN "
                "(SELECT control_policy_snapshot_id FROM experiment_registry WHERE experiment_id=? UNION "
                "SELECT candidate_policy_snapshot_id FROM experiment_registry WHERE experiment_id=?)",
                (self.experiment_id, self.experiment_id)),
            "experiment_registry": ("SELECT * FROM experiment_registry WHERE experiment_id=?", (self.experiment_id,)),
            "experiment_events": ("SELECT * FROM experiment_events WHERE experiment_id=? ORDER BY sequence", (self.experiment_id,)),
            "evaluation_runs": ("SELECT * FROM evaluation_runs WHERE experiment_id=?", (self.experiment_id,)),
        }
        if include_profiles:
            queries["instrument_master"] = ("SELECT * FROM instrument_master", ())
        result = {}
        for table, (query, args) in queries.items():
            async with conn.execute(query, args) as cursor:
                columns = [item[0] for item in cursor.description]
                result[table] = [dict(zip(columns, row)) for row in await cursor.fetchall()]
        return result

    @staticmethod
    async def _insert(conn, table, row):
        row = dict(row)
        if table == "experiment_events":
            row.pop("id", None)  # Sequence is local to this experiment, SQL id is not.
        columns = list(row)
        await conn.execute(f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) "
                           f"VALUES ({','.join('?' for _ in columns)})", tuple(row.values()))
        keys = {"policy_snapshots": ("id",), "experiment_registry": ("experiment_id",),
                "experiment_events": ("experiment_id", "sequence"), "evaluation_runs": ("id",),
                "instrument_master": ("ticker",)}[table]
        mutable = {"status", "last_usable_session", "updated_at"} if table == "experiment_registry" else set()
        immutable = [column for column in columns if column not in mutable]
        where = " AND ".join(key + "=?" for key in keys)
        async with conn.execute(f"SELECT {','.join(immutable)} FROM {table} WHERE {where}",
                                tuple(row[key] for key in keys)) as cursor:
            saved = await cursor.fetchone()
        if saved is None or tuple(saved) != tuple(row[column] for column in immutable):
            raise RuntimeError("conflicting immutable forward metadata: " + table)

    async def _sync_metadata(self):
        import aiosqlite
        async with aiosqlite.connect((self.directory / "metadata.db").as_uri() + "?mode=ro", uri=True) as conn:
            rows = await self._read_rows(conn)
        async with self.db.transaction() as conn:
            for table, records in rows.items():
                for row in records:
                    await self._insert(conn, table, row)
            registry = rows["experiment_registry"]
            if len(registry) != 1:
                raise RuntimeError("frozen forward registry missing")
            row = registry[0]
            await conn.execute("UPDATE experiment_registry SET status=?,last_usable_session=?,updated_at=? "
                "WHERE experiment_id=?", (row["status"], row["last_usable_session"], row["updated_at"], self.experiment_id))

    async def _publish(self, value):
        value = {**value, "experiment_id": self.experiment_id,
            "runtime": self.runtime_version, "worker_pid": getattr(self, "receipt", {}).get("pid"),
            "observation_start": self.started_at, "promotion_eligible": False}
        self._write(self.directory / "runtime_status.json", value)
        async with self.db.transaction() as conn:
            await conn.execute("INSERT OR REPLACE INTO system_stats(key,value,updated_at) "
                "VALUES ('continuous_evaluation_status',?,?)", (json.dumps(value, ensure_ascii=False),
                datetime.now().astimezone().isoformat()))
        self.last_status = value

    async def _receive(self):
        line = await self._process.stdout.readline()
        if not line:
            raise RuntimeError("frozen forward worker stopped; see runtime.stderr.log")
        return json.loads(line)

    async def _rpc(self, method, **payload):
        async with self._lock:
            if self._process is None or self._process.returncode is not None:
                raise RuntimeError("frozen forward worker is unavailable")
            try:
                self._process.stdin.write((json.dumps({"method": method, **payload}, allow_nan=False) + "\n").encode())
                await self._process.stdin.drain()
                while True:
                    message = await self._receive()
                    if message["kind"] == "data_request":
                        name = message["method"]
                        if name not in {"get_current_quote", "get_execution_quote", "is_trading_day", "is_market_open", "get_unadjusted_ohlc"}:
                            raise RuntimeError("unsupported forward data request")
                        try:
                            requested = datetime.now().astimezone()
                            value = await asyncio.wait_for(
                                getattr(self.market, name)(*message["args"]),
                                timeout=self._data_timeout_seconds)
                            if name == "get_execution_quote" and value is not None:
                                from ..domain.portfolio.quote_freshness import retrieved_after
                                if not retrieved_after(value, requested.isoformat(), now=datetime.now().astimezone()):
                                    logger.warning("FORWARD_EXECUTION_CACHE_REJECTED args=%s", message["args"])
                                    value = None
                            reply = {"kind": "data_response", "value": value}
                        except TimeoutError:
                            # Missing observations are already handled by the
                            # frozen engine. A data deadline is not an engine
                            # failure, and must never create a substitute price.
                            logger.warning("FORWARD_DATA_TIMEOUT method=%s args=%s", name, message["args"])
                            if name in {"get_current_quote", "get_execution_quote", "get_unadjusted_ohlc"}:
                                reply = {"kind": "data_response",
                                         "value": [] if name == "get_unadjusted_ohlc" else None}
                            else:
                                reply = {"kind": "data_response", "error": "calendar request timed out"}
                        except Exception as exc:
                            reply = {"kind": "data_response", "error": f"{type(exc).__name__}: {exc}"}
                        self._process.stdin.write((json.dumps(reply, allow_nan=False) + "\n").encode())
                        await self._process.stdin.drain()
                    elif message["kind"] == "result":
                        # The caller owns the production account connection.
                        # A detached transport task must never commit it.
                        return message["status"]
                    elif message["kind"] == "error":
                        raise ForwardOperationError(message.get("error", "forward operation failed"))
                    else:
                        raise RuntimeError(message.get("error", "invalid forward worker message"))
            except ForwardOperationError:
                raise
            except BaseException:
                await self._abort()
                raise

    def _submit(self, method, **payload):
        if self._closing:
            raise RuntimeError("forward runtime is closing")
        task = asyncio.create_task(self._rpc(method, **payload), name="forward-" + method)
        self._requests[task] = (method, payload.get("round_id"))
        # Retain completed requests for the account owner to collect. Reading
        # exceptions here avoids an unhandled-task warning after caller timeout.
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        return task

    async def _collect_ready(self):
        """Mirror metadata only when called by the account owner, never in RPC."""
        statuses, errors = [], []
        for task, (method, round_id) in list(self._requests.items()):
            if not task.done():
                continue
            del self._requests[task]
            if round_id is not None:
                self._captures.pop(round_id, None)
            try:
                statuses.append(task.result())
            except asyncio.CancelledError:
                errors.append(RuntimeError("forward request interrupted during shutdown"))
            except Exception as exc:
                errors.append(exc)
        if statuses:
            await self._sync_metadata()
            await self._publish(statuses[-1])
        if errors:
            raise errors[0]

    async def _ensure_running(self):
        if self._closing:
            raise RuntimeError("forward runtime is closing")
        if self._process is None or self._process.returncode is not None:
            await self._abort()
            # initialize re-verifies the existing pinned artifacts and books.
            # Interrupted-operation markers remain authoritative on restart.
            await self.initialize()
            logger.info("FROZEN_FORWARD_RESTARTED experiment=%s pid=%s",
                        self.experiment_id, self.receipt["pid"])

    async def _wait_response(self, task):
        # External 20/30-second research timeouts must not cancel a private
        # book operation midway through writes or terminate its worker.
        await asyncio.wait({task}, timeout=self._response_wait_seconds)
        await self._collect_ready()
        return {**self.last_status, "runtime_pending": bool(self._requests),
                "pending_operations": [method for method, _ in self._requests.values()]}

    async def tick(self):
        await self._collect_ready()
        if self._requests:
            return {**self.last_status, "runtime_pending": True,
                    "pending_operations": [method for method, _ in self._requests.values()]}
        await self._ensure_running()
        return await self._wait_response(self._submit("tick"))

    async def capture_round(self, round_id, decisions):
        if not isinstance(round_id, str) or Path(round_id).name != round_id:
            raise ValueError("invalid forward round identity")
        provenance = self.directory / "producers" / (round_id + ".json")
        data_hash = hashlib.sha256(json.dumps(decisions, sort_keys=True, allow_nan=False).encode()).hexdigest()
        if provenance.exists():
            if json.loads(provenance.read_text())["decisions_sha256"] != data_hash:
                raise RuntimeError("common forward decisions changed for an existing round")
        else:
            provenance.parent.mkdir(exist_ok=True)
            async with self.db._connection.execute("SELECT policy_snapshot_id FROM research_rounds WHERE id=?",
                                                   (round_id,)) as cursor:
                round_state = await cursor.fetchone()
            self._write(provenance, {"round_id": round_id, "captured_at": datetime.now().astimezone().isoformat(),
                "producer_pid": __import__("os").getpid(), "decisions_sha256": data_hash,
                "producer_policy_snapshot_id": round_state[0] if round_state else None,
                "producer_entrypoint_sha256": digest(self.source_root / "run_discussion.py"),
                "producer_adapter_sha256": digest(__file__),
                "producer_simulation": self.policy,
                "scope": "shared external research input; allocation engine and policy remain frozen"})
        await self._collect_ready()
        # Freeze the submitted payload too: a caller may reuse its input list
        # while the serialized request is waiting behind a slow market batch.
        decisions = json.loads(json.dumps(decisions, allow_nan=False))
        producer_records = None
        if self.engine_version != "legacy":
            async with self.db.transaction() as conn:
                async with conn.execute("SELECT * FROM research_rounds WHERE id=?", (round_id,)) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    raise ValueError("paired input requires an observed producer round")
                round_record = dict(row)
                async with conn.execute("SELECT * FROM policy_snapshots WHERE id=?", (round_record["policy_snapshot_id"],)) as cursor:
                    policies = [dict(r) for r in await cursor.fetchall()]
                approvals = []
                for d in decisions:
                    async with conn.execute("SELECT * FROM simulation_committee_outcomes "
                        "WHERE round_id=? AND decision_id=? AND ticker=? ORDER BY id DESC LIMIT 1",
                        (round_id, d.get("decision_id"), d.get("ticker"))) as cursor:
                        row = await cursor.fetchone()
                    if row is None:
                        raise ValueError("paired input lacks its recorded committee outcome")
                    approvals.append(dict(row))
                producer_records = dict(round=round_record, policies=policies, committee=approvals,
                    captured_at=datetime.now().astimezone().isoformat())
        task = self._captures.get(round_id)
        if task is None:
            if not self._requests:
                await self._ensure_running()
            payload = dict(round_id=round_id, decisions=decisions)
            if self.engine_version != "legacy":
                payload["producer_records"] = producer_records
            task = self._submit("capture_round", **payload)
            self._captures[round_id] = task
        return await self._wait_response(task)

    async def _abort(self):
        process, self._process = self._process, None
        if process is not None and process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                await process.wait()
                return
            try:
                await asyncio.wait_for(process.wait(), 5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

    async def close(self):
        self._closing = True
        pending = set(self._requests)
        if pending:
            _, unfinished = await asyncio.wait(pending, timeout=20)
            for task in unfinished:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            try:
                await self._collect_ready()
            except Exception:
                logger.exception("Forward shutdown retained interrupted-operation evidence")
        if self._process is not None and self._process.returncode is None:
            try:
                status = await asyncio.wait_for(self._rpc("close"), 5)
                await self._sync_metadata()
                await self._publish(status)
                await asyncio.wait_for(self._process.wait(), 5)
            except Exception:
                await self._abort()
        self._process = None
