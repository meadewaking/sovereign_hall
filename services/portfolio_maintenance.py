"""Cooperative account duty while the sole portfolio owner pauses research.

This is deliberately awaited by the runner, never a background account writer.
It reuses existing lifecycle, deferred-ruling and valuation paths; research
admission, model budgets and topic cooldowns do not gate those paths.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import os
from pathlib import Path
from time import monotonic

logger = logging.getLogger(__name__)


class PortfolioMaintenance:
    def __init__(self, simulation, *, status_path: Path,
                 policy_snapshot_id: str | None = None, interval_seconds: float = 60):
        self.simulation = simulation
        self.status_path = Path(status_path)
        self.policy_snapshot_id = policy_snapshot_id
        self.interval_seconds = max(1.0, float(interval_seconds))
        self._next_due = 0.0
        self.last_result = None
        self._last_review_clock = None
        self.evaluation = None
        self.round_id = None
        self.round_reviews = []

    async def run_if_due(self, *, context: str):
        if monotonic() < self._next_due:
            return self.last_result
        report = {
            "version": "account_maintenance_v1",
            "runner_pid": os.getpid(),
            "policy_snapshot_id": self.policy_snapshot_id,
            "context": context,
            "started_at": datetime.now().astimezone().isoformat(),
            "status": "ready",
            "reviews": None,
            "pending_replay": None,
            "performance": None,
            "errors": {},
            "review_gap_seconds": (monotonic() - self._last_review_clock
                                   if self._last_review_clock is not None else None),
        }
        for stage, operation in (
            ("reviews", (lambda: self.simulation.review_open_positions(round_id=self.round_id))
                         if self.round_id else self.simulation.review_open_positions),
            ("pending_replay", self.simulation.replay_pending_decisions),
            ("performance", self.simulation.get_performance_metrics),
        ):
            try:
                report[stage] = await operation()
                if stage == "performance" and (report[stage].get("reconciliation") or {}).get("status") == "mismatch":
                    report["errors"]["reconciliation"] = "account does not reconcile to the recorded balance boundary"
                if stage == "reviews":
                    self._last_review_clock = monotonic()
                    if self.round_id:
                        self.round_reviews.extend(item for item in (report[stage] or [])
                            if (item.get("execution") or {}).get("action") in {"buy", "sell"}
                            and (item.get("execution") or {}).get("success") is not False)
                outcomes = []
                if stage == "reviews":
                    outcomes = report[stage] or []
                elif stage == "pending_replay":
                    outcomes = (report[stage] or {}).get("results", [])
                if any((item.get("execution") or item).get("action") == "error" for item in outcomes):
                    report["errors"][stage] = "simulated execution returned an error; ruling remains retryable"
            except Exception as exc:
                report["errors"][stage] = str(exc)
                logger.exception("PORTFOLIO_MAINTENANCE_FAILED stage=%s context=%s", stage, context)
        if report["errors"]:
            report["status"] = "degraded"
        elif not (report["performance"] or {}).get("valuation_complete"):
            report["status"] = "valuation_unavailable"
        report["completed_at"] = datetime.now().astimezone().isoformat()
        report["review_interval_seconds"] = self.interval_seconds
        report["review_overdue"] = bool(report["review_gap_seconds"] is not None
            and report["review_gap_seconds"] > self.interval_seconds * 2 + 5)
        if report["review_overdue"]:
            logger.warning("PORTFOLIO_REVIEW_OVERDUE gap_seconds=%s context=%s",
                           report["review_gap_seconds"], context)
        self._next_due = monotonic() + self.interval_seconds
        if self.evaluation is not None:
            try:
                report["evaluation"] = await asyncio.wait_for(self.evaluation.tick(), timeout=20)
                if report["performance"] and report["evaluation"]:
                    report["performance"]["evaluation_status"] = report["evaluation"]["evaluation_status"]
            except Exception as exc:
                report["errors"]["evaluation"] = str(exc)
                logger.exception("CONTINUOUS_EVALUATION_FAILED")
                report["status"] = "degraded"
                try:
                    await self.evaluation._publish({"evaluation_status": "data_blocked", "reason": str(exc)})
                except Exception as publish_error:
                    report["errors"]["evaluation_status"] = str(publish_error)
                    logger.exception("EVALUATION_STATUS_DEFERRED")
        db = getattr(self.simulation, "db_service", None)
        if db and report.get("performance"):
            runtime = {key: report["performance"].get(key) for key in
                       ("research_status", "execution_status", "capital_status", "evaluation_status")}
            runtime["measured_at"] = report["completed_at"]
            try:
                async with db.transaction(immediate=True) as conn:
                    await conn.execute("INSERT OR REPLACE INTO system_stats(key,value,updated_at) "
                        "VALUES ('simulation_runtime_status',?,?)",
                        (json.dumps(runtime), report["completed_at"]))
            except Exception as status_error:
                report["errors"]["runtime_status"] = str(status_error)
                report["status"] = "degraded"
                logger.exception("ACCOUNT_STATUS_DEFERRED")
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.status_path.with_suffix(self.status_path.suffix + ".tmp")
            temporary.write_text(json.dumps(report, ensure_ascii=False, allow_nan=False), encoding="utf-8")
            temporary.replace(self.status_path)
        except (OSError, ValueError, TypeError):
            logger.exception("Cannot publish portfolio maintenance status")
        summary = (report["status"], len(report["reviews"] or []),
                   (report["pending_replay"] or {}).get("executed"),
                   (report["performance"] or {}).get("score"))
        previous = self.last_result or {}
        unchanged = summary[1] == 0 and not summary[2] and summary == (
            previous.get("status"), len(previous.get("reviews") or []),
            (previous.get("pending_replay") or {}).get("executed"),
            (previous.get("performance") or {}).get("score"))
        (logger.debug if unchanged else logger.info)(
            "PORTFOLIO_MAINTENANCE context=%s status=%s reviews=%s replay_fills=%s score=%s snapshot=%s",
            context, *summary, self.policy_snapshot_id,
        )
        # Publish one completed result. Readers must not observe "ready" while
        # evaluation, runtime persistence or the status file is still pending.
        self.last_result = report
        return report

    async def during(self, research, *, context: str):
        """Await research while the calling account owner services its book.

        Only research is a child task. All account duty stays in this caller;
        cancelling the owner also cancels and joins the unfinished research.
        """
        task = asyncio.ensure_future(research)
        try:
            while not task.done():
                await self.run_if_due(context=context)
                delay = max(.01, min(self.interval_seconds, self._next_due - monotonic()))
                await asyncio.wait({task}, timeout=delay)
            return await task
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def sleep(self, seconds: float, *, context: str):
        """Keep the original research delay, servicing the account every minute."""
        deadline = monotonic() + max(0.0, float(seconds))
        while monotonic() < deadline:
            await self.run_if_due(context=context)
            delay = min(deadline - monotonic(), self._next_due - monotonic())
            if delay > 0:
                await asyncio.sleep(delay)
