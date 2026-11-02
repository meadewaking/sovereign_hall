"""Research-round state machine.

One round may legitimately end without a trade, but it must never disappear
between pipeline stages.  The terminal state is workflow health metadata and
does not replace realtime simulated-account return as the reward.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import Optional

from ..common.ids import new_id


class ResearchRoundStatus(str, Enum):
    CREATED = "created"
    MEMORY_LOADED = "memory_loaded"
    SOURCES_PERSISTED = "sources_persisted"
    PROPOSALS_EXTRACTED = "proposals_extracted"
    COMMITTEE_DECIDED = "committee_decided"
    PREDICTIONS_RECORDED = "predictions_recorded"
    PORTFOLIO_REVIEWED = "portfolio_reviewed"
    EXECUTION_INTENTS_CREATED = "execution_intents_created"
    NO_EVIDENCE = "no_evidence"
    COMMITTEE_HOLD = "committee_hold"
    COMMITTEE_INCOMPLETE = "committee_incomplete"
    MARKET_CLOSED_PENDING = "market_closed_pending"
    EXECUTION_REJECTED = "execution_rejected"
    FILLED = "filled"
    FAILED = "failed"
    REFLECTED = "reflected"
    COMPLETED = "completed"


TERMINAL_ROUND_STATUSES = frozenset(
    {
        ResearchRoundStatus.NO_EVIDENCE,
        ResearchRoundStatus.COMMITTEE_HOLD,
        ResearchRoundStatus.COMMITTEE_INCOMPLETE,
        ResearchRoundStatus.MARKET_CLOSED_PENDING,
        ResearchRoundStatus.EXECUTION_REJECTED,
        ResearchRoundStatus.FILLED,
        ResearchRoundStatus.FAILED,
        ResearchRoundStatus.COMPLETED,
    }
)


_ALLOWED_TRANSITIONS = {
    ResearchRoundStatus.CREATED: {
        ResearchRoundStatus.MEMORY_LOADED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.MEMORY_LOADED: {
        ResearchRoundStatus.SOURCES_PERSISTED,
        ResearchRoundStatus.NO_EVIDENCE,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.SOURCES_PERSISTED: {
        ResearchRoundStatus.PROPOSALS_EXTRACTED,
        ResearchRoundStatus.NO_EVIDENCE,
        # A real proposal may be removed by the repeated-candidate or fresh
        # whole-lot feasibility hard gate before committee persistence.  That
        # is an execution-screen rejection, not an absence of evidence.
        ResearchRoundStatus.EXECUTION_REJECTED,
        # A mandatory pre-research lifecycle exit can be deferred because the
        # market is closed even when the network source stage later fails.
        ResearchRoundStatus.MARKET_CLOSED_PENDING,
        # A mandatory pre-research lifecycle exit can fill while the research
        # portion later produces no surviving proposal.
        ResearchRoundStatus.FILLED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.PROPOSALS_EXTRACTED: {
        ResearchRoundStatus.COMMITTEE_DECIDED,
        ResearchRoundStatus.NO_EVIDENCE,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.COMMITTEE_DECIDED: {
        ResearchRoundStatus.PREDICTIONS_RECORDED,
        ResearchRoundStatus.COMMITTEE_HOLD,
        ResearchRoundStatus.COMMITTEE_INCOMPLETE,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.PREDICTIONS_RECORDED: {
        ResearchRoundStatus.PORTFOLIO_REVIEWED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.PORTFOLIO_REVIEWED: {
        ResearchRoundStatus.EXECUTION_INTENTS_CREATED,
        # A pre-research lifecycle intent can already have filled. A later
        # committee HOLD creates no new intent, but that durable fill still
        # needs its workflow terminal before reflection and completion.
        ResearchRoundStatus.FILLED,
        ResearchRoundStatus.COMMITTEE_HOLD,
        ResearchRoundStatus.COMMITTEE_INCOMPLETE,
        ResearchRoundStatus.EXECUTION_REJECTED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.EXECUTION_INTENTS_CREATED: {
        ResearchRoundStatus.MARKET_CLOSED_PENDING,
        ResearchRoundStatus.EXECUTION_REJECTED,
        ResearchRoundStatus.FILLED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.FILLED: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.MARKET_CLOSED_PENDING: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.EXECUTION_REJECTED: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.COMMITTEE_HOLD: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.COMMITTEE_INCOMPLETE: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.NO_EVIDENCE: {
        ResearchRoundStatus.REFLECTED,
        ResearchRoundStatus.COMPLETED,
        ResearchRoundStatus.FAILED,
    },
    ResearchRoundStatus.REFLECTED: {ResearchRoundStatus.COMPLETED},
    ResearchRoundStatus.FAILED: set(),
    ResearchRoundStatus.COMPLETED: set(),
}


@dataclass(frozen=True)
class ResearchRound:
    base_topic: str
    research_objective: str
    id: str = field(default_factory=lambda: new_id("round"))
    status: ResearchRoundStatus = ResearchRoundStatus.CREATED
    current_stage: str = "created"
    terminal_code: Optional[str] = None
    terminal_reason: Optional[str] = None
    policy_snapshot_id: Optional[str] = None
    prompt_version: Optional[str] = None
    input_hash: Optional[str] = None
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None

    def transition(
        self,
        next_status: ResearchRoundStatus,
        *,
        terminal_code: Optional[str] = None,
        terminal_reason: Optional[str] = None,
    ) -> "ResearchRound":
        if next_status not in _ALLOWED_TRANSITIONS[self.status]:
            raise ValueError(f"invalid research round transition: {self.status} -> {next_status}")
        now = datetime.now().isoformat()
        completed_at = now if next_status in TERMINAL_ROUND_STATUSES else None
        # A lifecycle exit can fill before the research stages finish, and a
        # deferred intent can fill after its originating round is completed.
        # Once that durable fill terminal exists, weaker workflow outcomes
        # must not overwrite it merely because the remaining research produced
        # no proposal or no further execution.  A genuine pipeline failure is
        # still allowed to become the final terminal and remains separately
        # auditable through the already-committed fill/event/ledger rows.
        weaker_execution_terminals = {
            "no_evidence",
            "committee_hold",
            "committee_incomplete",
            "market_closed_pending",
            "execution_rejected",
        }
        preserve_filled_terminal = (
            self.terminal_code == "filled"
            and terminal_code in weaker_execution_terminals
        )
        effective_terminal_code = (
            self.terminal_code if preserve_filled_terminal else terminal_code
        )
        effective_terminal_reason = (
            self.terminal_reason if preserve_filled_terminal else terminal_reason
        )
        return replace(
            self,
            status=next_status,
            current_stage=next_status.value,
            terminal_code=(
                effective_terminal_code
                if effective_terminal_code is not None
                else self.terminal_code
            ),
            terminal_reason=(
                effective_terminal_reason
                if effective_terminal_reason is not None
                else self.terminal_reason
            ),
            updated_at=now,
            completed_at=completed_at,
        )
