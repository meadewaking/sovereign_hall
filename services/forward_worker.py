"""Frozen forward evaluator process. Its only external data channel is RPC.

Run a verified copy of this file, never the production package. Network and
SQLite/file writes outside the experiment are denied before engine imports.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def send(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")
    sys.stdout.flush()


async def serve(directory):
    directory = directory.resolve()
    runtime = json.loads((directory / "runtime.json").read_text())
    protocol = json.loads((directory / "protocol.json").read_text())
    if digest(__file__) != runtime["worker_sha256"]:
        raise RuntimeError("frozen worker changed")
    if digest(directory / "protocol.json") != runtime["protocol_sha256"]:
        raise RuntimeError("frozen forward protocol changed")
    if digest(directory / "config.yaml") != runtime["config_sha256"]:
        raise RuntimeError("frozen forward configuration changed")
    source = directory / "source"
    for relative, expected in protocol["artifacts"].items():
        path = (source / relative).resolve()
        if not path.is_relative_to(source) or digest(path) != expected:
            raise RuntimeError("frozen forward source changed: " + relative)

    lock = (directory / "runtime.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.chdir(directory)
    sys.dont_write_bytecode = True
    # The frozen compatibility package resolves every sovereign_hall import,
    # including lazy absolute imports, inside this source tree.
    sys.path.insert(0, str(source))

    def inside(value):
        return Path(value).resolve().is_relative_to(directory)

    def guard(event, args):
        if event in {"socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"}:
            raise RuntimeError("forward worker external access denied; use controlled data RPC")
        if event == "sqlite3.connect":
            value = str(args[0])
            if value != ":memory:" and (value.startswith("file:") or not inside(value)):
                raise RuntimeError("forward worker SQLite outside experiment denied")
        if event == "open":
            path, mode, flags = args
            writing = (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
                isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            if writing and not isinstance(path, int) and not inside(path):
                raise RuntimeError("forward worker file write outside experiment denied")
        if event in {"os.rename", "os.remove", "os.rmdir", "os.mkdir"}:
            paths = args[:2] if event == "os.rename" else args[:1]
            if any(not inside(p) for p in paths):
                raise RuntimeError("forward worker filesystem mutation outside experiment denied")

    sys.addaudithook(guard)
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.continuous_evaluation import ContinuousEvaluation
    engine = ContinuousEvaluation
    if protocol.get("engine_version"):
        from sovereign_hall.services.paired_allocation import ENGINE, PairedAllocationEvaluation
        if protocol["engine_version"] != ENGINE:
            raise RuntimeError("unsupported frozen forward engine")
        engine = PairedAllocationEvaluation

    reader = asyncio.StreamReader(limit=8 * 1024 * 1024)
    await asyncio.get_running_loop().connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)

    class ControlledData:
        def __init__(self):
            self.lock = asyncio.Lock()

        async def request(self, method, *args):
            async with self.lock:
                send({"kind": "data_request", "method": method, "args": args})
                reply = json.loads(await reader.readline())
                if reply.get("kind") != "data_response":
                    raise RuntimeError("invalid forward data response")
                if reply.get("error"):
                    raise RuntimeError(reply["error"])
                return reply["value"]

        async def get_current_quote(self, ticker):
            return await self.request("get_current_quote", ticker)

        async def get_execution_quote(self, ticker):
            return await self.request("get_execution_quote", ticker)

        async def is_trading_day(self):
            return await self.request("is_trading_day")

        async def is_market_open(self):
            return await self.request("is_market_open")

        async def get_unadjusted_ohlc(self, ticker, start, end):
            return await self.request("get_unadjusted_ohlc", ticker, start, end)

    metadata = DatabaseService(str(directory / "metadata.db"))
    evaluator = None
    try:
        await metadata._init_db()
        evaluator = engine(metadata, ControlledData(), root=directory.parent,
            policy=protocol["simulation"], source_root=source)
        await evaluator.initialize()
        if evaluator.experiment_id != directory.name:
            raise RuntimeError("frozen forward identity mismatch")
        receipt = {"pid": os.getpid(), "experiment_id": evaluator.experiment_id,
            "started_at": datetime.now().astimezone().isoformat(),
            "source_root": str(source), "worker_sha256": digest(__file__),
            "protocol_sha256": digest(directory / "protocol.json"),
            "network": "denied; controlled parent data RPC only", "sqlite_write_root": str(directory)}
        previous_tick = directory / "last_tick.json"
        receipt["previous_observed_at"] = json.loads(previous_tick.read_text()).get("started_at") if previous_tick.exists() else None
        registered = await evaluator.registry.get(evaluator.experiment_id)
        await evaluator.registry.transition(evaluator.experiment_id, registered.status,
            event_type="FrozenRuntimeAttached", payload=receipt)
        # A live-tree hash failure is transient once the exact archived engine
        # has actually loaded and verified. Interrupted financial operations
        # and missing-market-data blockers are deliberately left untouched.
        if (evaluator.last_status.get("reason") == "forward engine changed; a new frozen experiment is required"
                and not any((directory / name).exists() for name in
                    ("blocked.json", "active_input.json", "active_tick.json"))):
            evaluator._check_frozen()
            await evaluator._publish({**evaluator.last_status,
                "evaluation_status": "insufficient_samples", "reason": "frozen_engine_resumed",
                "recovered_runtime_fault": "live source changed; archived engine verified without resetting observations"})
        evaluator._write(directory / "runtime_receipt.json", receipt)
        send({"kind": "ready", "status": evaluator.last_status, "receipt": receipt})
        while line := await reader.readline():
            command = json.loads(line)
            try:
                if command["method"] == "close":
                    registered = await evaluator.registry.get(evaluator.experiment_id)
                    await evaluator.registry.transition(evaluator.experiment_id, registered.status,
                        event_type="FrozenRuntimeDetached", payload={"pid": os.getpid()})
                    send({"kind": "result", "status": evaluator.last_status})
                    break
                if command["method"] == "tick":
                    await evaluator.tick()
                elif command["method"] == "capture_round":
                    kwargs = ({"producer_records": command.get("producer_records")}
                              if protocol.get("engine_version") else {})
                    await evaluator.capture_round(command["round_id"], command["decisions"], **kwargs)
                else:
                    raise ValueError("unsupported forward command")
                send({"kind": "result", "status": evaluator.last_status})
            except Exception as exc:
                send({"kind": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        if evaluator is not None:
            await evaluator.close()
        await metadata.close()
        lock.close()


if __name__ == "__main__":
    import logging
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    asyncio.run(serve(Path(sys.argv[1])))
