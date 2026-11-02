"""Forward-only SQLite migrations for the canonical architecture.

The project has substantial legacy history.  Migrations therefore add new
tables and nullable lineage columns only.  They never infer historical
relationships or quote metadata.
"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Iterable

import aiosqlite


ARCHITECTURE_MIGRATION_VERSION = "20260729_architecture_v1"
TRADING_READINESS_MIGRATION_VERSION = "20260814_trading_readiness_v2"
ROUND_FILL_TERMINAL_REPAIR_VERSION = "20260731_round_fill_terminal_v1"
ROUND_FAILED_TERMINAL_REPAIR_VERSION = "20260731_round_failed_terminal_v1"
EXECUTED_INTENT_DEFER_REPAIR_VERSION = (
    "20260825_executed_intent_defer_terminal_v1"
)
EXECUTED_INTENT_SEMANTIC_GUARD_VERSION = (
    "20260826_executed_intent_semantic_guard_v1"
)
EXECUTED_PENDING_DEFER_REPAIR_VERSION = (
    "20260829_executed_pending_defer_terminal_v1"
)
EXECUTED_PENDING_SEMANTIC_GUARD_VERSION = (
    "20260829_executed_pending_semantic_guard_v1"
)
PENDING_EXIT_NONEXPIRING_REPAIR_VERSION = (
    "20260830_pending_exit_nonexpiring_v1"
)
PENDING_EXIT_NONEXPIRING_GUARD_VERSION = (
    "20260830_pending_exit_nonexpiring_guard_v1"
)
REDEPLOYMENT_DIRECTION_AUDIT_VERSION = (
    "20260831_redeployment_direction_audit_v1"
)
COMMITTEE_MEETING_PROTOCOL_VERSION = (
    "20260831_committee_meeting_protocol_v1"
)
SIGTERM_BUSINESS_TERMINAL_REPAIR_VERSION = (
    "20260827_sigterm_business_terminal_v1"
)
POLICY_SNAPSHOT_MANIFEST_VERSION = "20260914_policy_snapshot_manifest_v1"
INSTRUMENT_IDENTITY_VERSION = "20260914_instrument_identity_v1"
DAILY_PRICE_REVISIONS_VERSION = "20260914_daily_price_revisions_v1"
EXPERIMENT_REGISTRY_VERSION = "20260914_experiment_registry_v1"
EMPIRICAL_OBSERVATIONS_VERSION = "20260914_empirical_observations_v1"
STRUCTURED_EVIDENCE_PACKAGE_VERSION = "20260914_structured_evidence_v1"


async def _tables(conn: aiosqlite.Connection) -> set[str]:
    async with conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
    ) as cursor:
        return {str(row[0]) for row in await cursor.fetchall()}


async def _columns(conn: aiosqlite.Connection, table: str) -> set[str]:
    async with conn.execute(f'PRAGMA table_info("{table}")') as cursor:
        return {str(row[1]) for row in await cursor.fetchall()}


async def _add_columns(
    conn: aiosqlite.Connection,
    table: str,
    definitions: Iterable[tuple[str, str]],
) -> None:
    available_tables = await _tables(conn)
    if table not in available_tables:
        return
    current = await _columns(conn, table)
    for name, definition in definitions:
        if name not in current:
            await conn.execute(
                f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}'
            )
            current.add(name)


async def apply_architecture_migrations(conn: aiosqlite.Connection) -> None:
    """Apply idempotent architecture migrations to an open connection."""
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA busy_timeout = 5000")

    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS policy_snapshots (
            id TEXT PRIMARY KEY,
            policy_family TEXT NOT NULL,
            version TEXT NOT NULL,
            config_json TEXT NOT NULL,
            source TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(policy_family, version)
        );

        CREATE TABLE IF NOT EXISTS research_rounds (
            id TEXT PRIMARY KEY,
            base_topic TEXT NOT NULL,
            research_objective TEXT NOT NULL,
            status TEXT NOT NULL,
            current_stage TEXT NOT NULL,
            terminal_code TEXT,
            terminal_reason TEXT,
            policy_snapshot_id TEXT,
            prompt_version TEXT,
            input_hash TEXT,
            engine_version TEXT NOT NULL DEFAULT 'canonical_v1',
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY(policy_snapshot_id) REFERENCES policy_snapshots(id)
        );

        CREATE TABLE IF NOT EXISTS round_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(round_id) REFERENCES research_rounds(id),
            UNIQUE(round_id, sequence)
        );

        CREATE TABLE IF NOT EXISTS round_documents (
            round_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            usage TEXT NOT NULL DEFAULT 'source',
            linked_at TEXT NOT NULL,
            PRIMARY KEY(round_id, document_id, usage),
            FOREIGN KEY(round_id) REFERENCES research_rounds(id),
            FOREIGN KEY(document_id) REFERENCES documents(id)
        );

        CREATE TABLE IF NOT EXISTS committee_meetings (
            id TEXT PRIMARY KEY,
            round_id TEXT NOT NULL,
            proposal_key TEXT NOT NULL,
            proposal_id TEXT,
            ticker TEXT NOT NULL,
            status TEXT NOT NULL,
            current_stage TEXT NOT NULL,
            review_depth TEXT,
            prompt_version TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            decision_id TEXT,
            outcome_code TEXT,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY(round_id) REFERENCES research_rounds(id),
            UNIQUE(round_id, proposal_key, prompt_version, input_hash)
        );

        CREATE TABLE IF NOT EXISTS committee_tasks (
            id TEXT PRIMARY KEY,
            meeting_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            seat_role TEXT NOT NULL,
            seat_label TEXT NOT NULL,
            session_id TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            input_hash TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            model TEXT,
            provider TEXT,
            token_usage_json TEXT NOT NULL DEFAULT '{}',
            error TEXT,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            elapsed_seconds REAL,
            FOREIGN KEY(meeting_id) REFERENCES committee_meetings(id)
                ON DELETE CASCADE,
            UNIQUE(meeting_id, stage, seat_label, input_hash, attempt)
        );

        CREATE TABLE IF NOT EXISTS committee_turns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id TEXT NOT NULL,
            task_id TEXT NOT NULL UNIQUE,
            stage TEXT NOT NULL,
            seat_label TEXT NOT NULL,
            raw_output TEXT NOT NULL,
            parsed_output_json TEXT NOT NULL DEFAULT '{}',
            parse_mode TEXT,
            output_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(meeting_id) REFERENCES committee_meetings(id)
                ON DELETE CASCADE,
            FOREIGN KEY(task_id) REFERENCES committee_tasks(id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS execution_intents (
            id TEXT PRIMARY KEY,
            round_id TEXT,
            decision_id TEXT,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            target_position REAL NOT NULL,
            confidence REAL,
            priority INTEGER NOT NULL DEFAULT 100,
            reason TEXT NOT NULL,
            status TEXT NOT NULL,
            defer_code TEXT,
            resolution TEXT,
            idempotency_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT,
            executed_at TEXT,
            FOREIGN KEY(round_id) REFERENCES research_rounds(id)
        );

        CREATE INDEX IF NOT EXISTS idx_committee_meetings_round
        ON committee_meetings(round_id, started_at, id);

        CREATE INDEX IF NOT EXISTS idx_committee_tasks_meeting_stage
        ON committee_tasks(meeting_id, stage, seat_label, attempt);

        CREATE INDEX IF NOT EXISTS idx_committee_tasks_status
        ON committee_tasks(status, updated_at);

        CREATE TABLE IF NOT EXISTS quote_snapshots (
            id TEXT PRIMARY KEY,
            round_id TEXT,
            intent_id TEXT,
            ticker TEXT NOT NULL,
            price REAL NOT NULL CHECK(price > 0),
            provider TEXT NOT NULL,
            purpose TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            received_at TEXT NOT NULL,
            freshness_seconds REAL,
            FOREIGN KEY(round_id) REFERENCES research_rounds(id),
            FOREIGN KEY(intent_id) REFERENCES execution_intents(id)
        );

        CREATE TABLE IF NOT EXISTS simulation_ledger_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER,
            intent_id TEXT,
            round_id TEXT,
            entry_type TEXT NOT NULL,
            ticker TEXT,
            share_delta INTEGER NOT NULL DEFAULT 0,
            cash_delta REAL NOT NULL DEFAULT 0,
            commission REAL NOT NULL DEFAULT 0,
            stamp_duty REAL NOT NULL DEFAULT 0,
            slippage_cost REAL NOT NULL DEFAULT 0,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(intent_id) REFERENCES execution_intents(id),
            FOREIGN KEY(round_id) REFERENCES research_rounds(id)
        );

        CREATE TABLE IF NOT EXISTS instrument_master (
            ticker TEXT PRIMARY KEY,
            instrument_type TEXT NOT NULL CHECK(instrument_type IN ('a_share', 'etf')),
            lot_size INTEGER NOT NULL DEFAULT 100 CHECK(lot_size > 0),
            listed_at TEXT,
            delisted_at TEXT,
            source TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            source TEXT,
            adjustment TEXT,
            provider TEXT,
            fetched_at TEXT,
            quality_status TEXT,
            trade_status TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(ticker, date)
        );

        CREATE TABLE IF NOT EXISTS trading_calendar (
            trade_date TEXT NOT NULL,
            market TEXT NOT NULL DEFAULT 'CN',
            is_open INTEGER NOT NULL CHECK(is_open IN (0, 1)),
            source TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            quality_status TEXT NOT NULL DEFAULT 'validated',
            PRIMARY KEY(trade_date, market)
        );

        CREATE TABLE IF NOT EXISTS evaluation_runs (
            id TEXT PRIMARY KEY,
            sleeve TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            strategy_hash TEXT NOT NULL,
            engine_version TEXT NOT NULL,
            data_cutoff TEXT NOT NULL,
            data_hash TEXT NOT NULL,
            universe_hash TEXT NOT NULL,
            universe_json TEXT NOT NULL,
            cost_model_version TEXT NOT NULL,
            cost_schedule_json TEXT NOT NULL,
            initial_capital REAL NOT NULL CHECK(initial_capital = 10000),
            trial_count INTEGER NOT NULL DEFAULT 1 CHECK(trial_count > 0),
            status TEXT NOT NULL,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            result_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cost_model_registry (
            version TEXT PRIMARY KEY,
            schedule_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_signals (
            id TEXT PRIMARY KEY,
            evaluation_run_id TEXT,
            sleeve TEXT NOT NULL,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            score REAL NOT NULL,
            signal_at TEXT NOT NULL,
            available_at TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(evaluation_run_id) REFERENCES evaluation_runs(id)
        );

        CREATE TABLE IF NOT EXISTS shadow_portfolios (
            id TEXT PRIMARY KEY,
            evaluation_run_id TEXT,
            sleeve TEXT NOT NULL,
            as_of TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            targets_json TEXT NOT NULL,
            risk_status TEXT NOT NULL,
            risk_reasons_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            FOREIGN KEY(evaluation_run_id) REFERENCES evaluation_runs(id)
        );

        CREATE TABLE IF NOT EXISTS paper_orders (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL DEFAULT 'shadow_paper_10000',
            idempotency_key TEXT NOT NULL UNIQUE,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            shares INTEGER NOT NULL CHECK(shares > 0),
            order_type TEXT NOT NULL,
            limit_price REAL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            signal_at TEXT NOT NULL,
            earliest_fill_date TEXT NOT NULL,
            submitted_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_accounts (
            account_id TEXT PRIMARY KEY,
            initial_capital REAL NOT NULL CHECK(initial_capital = 10000),
            cash REAL NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_positions (
            account_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            shares INTEGER NOT NULL CHECK(shares >= 0),
            average_cost REAL NOT NULL CHECK(average_cost >= 0),
            last_mark REAL,
            last_mark_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, ticker),
            FOREIGN KEY(account_id) REFERENCES paper_accounts(account_id)
        );

        CREATE TABLE IF NOT EXISTS paper_position_lots (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            shares INTEGER NOT NULL CHECK(shares > 0),
            remaining_shares INTEGER NOT NULL CHECK(remaining_shares >= 0),
            unit_cost REAL NOT NULL CHECK(unit_cost >= 0),
            FOREIGN KEY(account_id) REFERENCES paper_accounts(account_id)
        );

        CREATE TABLE IF NOT EXISTS paper_fills (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL DEFAULT 'shadow_paper_10000',
            order_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            shares INTEGER NOT NULL,
            price REAL NOT NULL,
            notional REAL NOT NULL,
            commission REAL NOT NULL,
            stamp_duty REAL NOT NULL,
            slippage_cost REAL NOT NULL,
            total_cost REAL NOT NULL,
            asset_type TEXT NOT NULL,
            cost_model_version TEXT NOT NULL,
            filled_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES paper_orders(id)
        );

        CREATE TABLE IF NOT EXISTS paper_ledger_entries (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            order_id TEXT,
            fill_id TEXT,
            ticker TEXT,
            share_delta INTEGER NOT NULL DEFAULT 0,
            cash_delta REAL NOT NULL DEFAULT 0,
            commission REAL NOT NULL DEFAULT 0,
            stamp_duty REAL NOT NULL DEFAULT 0,
            slippage_cost REAL NOT NULL DEFAULT 0,
            cost_model_version TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(account_id) REFERENCES paper_accounts(account_id),
            FOREIGN KEY(order_id) REFERENCES paper_orders(id),
            FOREIGN KEY(fill_id) REFERENCES paper_fills(id)
        );

        CREATE TABLE IF NOT EXISTS paper_account_snapshots (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL DEFAULT 'shadow_paper_10000',
            cash REAL NOT NULL,
            positions_json TEXT NOT NULL,
            total_assets REAL,
            reconciliation_status TEXT NOT NULL,
            reconciliation_json TEXT NOT NULL DEFAULT '{}',
            snapshot_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_research_rounds_updated
            ON research_rounds(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_execution_intents_status
            ON execution_intents(status, priority, created_at);
        CREATE INDEX IF NOT EXISTS idx_quote_snapshots_ticker_time
            ON quote_snapshots(ticker, fetched_at DESC);
        CREATE INDEX IF NOT EXISTS idx_simulation_ledger_trade
            ON simulation_ledger_entries(trade_id);
        CREATE INDEX IF NOT EXISTS idx_trading_calendar_open
            ON trading_calendar(market, is_open, trade_date);
        CREATE INDEX IF NOT EXISTS idx_evaluation_runs_sleeve_time
            ON evaluation_runs(sleeve, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_shadow_signals_sleeve_time
            ON shadow_signals(sleeve, signal_at, ticker);
        CREATE INDEX IF NOT EXISTS idx_shadow_portfolios_sleeve_time
            ON shadow_portfolios(sleeve, as_of);
        CREATE TRIGGER IF NOT EXISTS evaluation_runs_append_only_update
        BEFORE UPDATE ON evaluation_runs
        BEGIN
            SELECT RAISE(ABORT, 'evaluation_runs is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS evaluation_runs_append_only_delete
        BEFORE DELETE ON evaluation_runs
        BEGIN
            SELECT RAISE(ABORT, 'evaluation_runs is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_signals_append_only_update
        BEFORE UPDATE ON shadow_signals
        BEGIN
            SELECT RAISE(ABORT, 'shadow_signals is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_signals_append_only_delete
        BEFORE DELETE ON shadow_signals
        BEGIN
            SELECT RAISE(ABORT, 'shadow_signals is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_portfolios_append_only_update
        BEFORE UPDATE ON shadow_portfolios
        BEGIN
            SELECT RAISE(ABORT, 'shadow_portfolios is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS shadow_portfolios_append_only_delete
        BEFORE DELETE ON shadow_portfolios
        BEGIN
            SELECT RAISE(ABORT, 'shadow_portfolios is append-only');
        END;
        """
    )

    lineage_columns = {
        "documents": (("round_id", "TEXT"),),
        "proposals": (("round_id", "TEXT"),),
        "meetings": (("round_id", "TEXT"),),
        "report_conclusions": (("round_id", "TEXT"),),
        "reflection_summary": (("round_id", "TEXT"),),
        "research_stage_diagnostics": (("round_id", "TEXT"),),
        "price_predictions": (("round_id", "TEXT"), ("decision_id", "TEXT")),
        "simulation_committee_outcomes": (
            ("round_id", "TEXT"),
            ("decision_id", "TEXT"),
            ("meeting_id", "TEXT"),
            ("outcome_code", "TEXT"),
        ),
        "simulation_pending_decisions": (
            ("round_id", "TEXT"),
            ("intent_id", "TEXT"),
        ),
        "simulation_snapshots": (("round_id", "TEXT"),),
    }
    for table, definitions in lineage_columns.items():
        await _add_columns(conn, table, definitions)

    # Unknown historical source times remain NULL; never copy fetched_at.
    await _add_columns(conn, "quote_snapshots", (("quoted_at", "TEXT"),))
    await conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
        ("20261007_quote_event_time_v1", datetime.now().isoformat()),
    )

    async with conn.execute("SELECT type FROM sqlite_master WHERE name='round_documents'") as cursor:
        lineage_kind = await cursor.fetchone()
    if lineage_kind and lineage_kind[0] == 'table':
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_round_documents_document ON round_documents(document_id,round_id)')
    # The UNIQUE(round_id,sequence) B-tree already serves this lookup. Keep
    # the secondary index only for legacy schemas without that covering key.
    covered = False
    async with conn.execute('PRAGMA index_list(round_events)') as cursor:
        event_indexes = await cursor.fetchall()
    for index in event_indexes:
        if index[1] == 'idx_round_events_round' or index[4]:
            continue
        async with conn.execute(f'PRAGMA index_info("{index[1]}")') as cursor:
            columns = [row[2] for row in await cursor.fetchall()]
        if columns[:2] == ['round_id','sequence']:
            covered = True
    if covered:
        await conn.execute('DROP INDEX IF EXISTS idx_round_events_round')
    else:
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_round_events_round ON round_events(round_id,sequence)')
    current_tables = await _tables(conn)
    if "simulation_committee_outcomes" in current_tables:
        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                ux_simulation_committee_outcomes_decision
            ON simulation_committee_outcomes(decision_id)
            WHERE decision_id IS NOT NULL AND decision_id <> ''
            """
        )

    await _add_columns(
        conn,
        "simulation_trades",
        (
            ("round_id", "TEXT"),
            ("intent_id", "TEXT"),
            ("quote_snapshot_id", "TEXT"),
            ("quote_provider", "TEXT"),
            ("quote_fetched_at", "TEXT"),
            ("gross_amount", "REAL"),
            ("commission", "REAL"),
            ("stamp_duty", "REAL"),
            ("slippage_cost", "REAL"),
            ("idempotency_key", "TEXT"),
            ("asset_type", "TEXT"),
            ("cost_model_version", "TEXT"),
        ),
    )
    # Versioned, price-free investment terms shared by research and execution.
    # Nullable fields preserve historical records without inventing old terms.
    await _add_columns(
        conn,
        "execution_intents",
        (
            ("holding_period_days", "INTEGER"),
            ("strategy_stop_loss_pct", "REAL"),
            ("strategy_take_profit_pct", "REAL"),
            ("failure_conditions", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "simulation_ledger_entries",
        (
            ("asset_type", "TEXT"),
            ("cost_model_version", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "daily_prices",
        (
            ("adjustment", "TEXT"),
            ("provider", "TEXT"),
            ("fetched_at", "TEXT"),
            ("quality_status", "TEXT"),
            ("trade_status", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "evaluation_runs",
        (
            ("data_hash", "TEXT"),
            ("universe_json", "TEXT"),
            ("cost_schedule_json", "TEXT"),
            ("config_json", "TEXT"),
            ("result_json", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "paper_orders",
        (
            ("account_id", "TEXT NOT NULL DEFAULT 'shadow_paper_10000'"),
            ("signal_at", "TEXT"),
            ("earliest_fill_date", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "paper_fills",
        (
            ("account_id", "TEXT NOT NULL DEFAULT 'shadow_paper_10000'"),
            ("notional", "REAL"),
            ("total_cost", "REAL"),
            ("asset_type", "TEXT"),
        ),
    )
    await _add_columns(
        conn,
        "paper_account_snapshots",
        (("account_id", "TEXT NOT NULL DEFAULT 'shadow_paper_10000'"),),
    )
    await conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_paper_orders_account_status
            ON paper_orders(account_id, status, earliest_fill_date);
        CREATE INDEX IF NOT EXISTS idx_paper_fills_account_time
            ON paper_fills(account_id, filled_at);
        CREATE INDEX IF NOT EXISTS idx_paper_lots_account_ticker
            ON paper_position_lots(account_id, ticker, acquired_at);
        CREATE INDEX IF NOT EXISTS idx_paper_ledger_account_time
            ON paper_ledger_entries(account_id, created_at);
        """
    )
    current_tables = await _tables(conn)
    if "simulation_trades" in current_tables:
        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_simulation_trades_idempotency
            ON simulation_trades(idempotency_key)
            WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''
            """
        )

    # ``defer_code`` is the current execution blocker, not immutable history.
    # Older replay fills changed the intent to ``executed`` but left the
    # market-closure code in place, causing user entries to report a successful
    # fill as still blocked. Reconcile only rows with exact trade lineage and
    # an exact ``simulation_trade:<id>`` resolution. Unknown legacy rows stay
    # untouched; the previous code is preserved in an append-only round event.
    if {
        "execution_intents",
        "simulation_trades",
        "research_rounds",
        "round_events",
    }.issubset(current_tables):
        async with conn.execute(
            """
            SELECT ei.id, ei.round_id, ei.defer_code, ei.resolution, st.id
            FROM execution_intents ei
            JOIN simulation_trades st ON st.intent_id = ei.id
            WHERE ei.status = 'executed'
              AND ei.defer_code IS NOT NULL
              AND ei.defer_code <> ''
              AND ei.resolution = 'simulation_trade:' || st.id
            ORDER BY datetime(ei.executed_at), ei.id
            """
        ) as cursor:
            resolved_intents = await cursor.fetchall()
        reconciled_at = datetime.now().isoformat()
        for (
            intent_id,
            round_id,
            previous_defer_code,
            resolution,
            trade_id,
        ) in resolved_intents:
            cursor = await conn.execute(
                """
                UPDATE execution_intents
                SET defer_code = NULL
                WHERE id = ? AND status = 'executed' AND defer_code = ?
                """,
                (intent_id, previous_defer_code),
            )
            if not cursor.rowcount or not round_id:
                continue
            async with conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 "
                "FROM round_events WHERE round_id = ?",
                (round_id,),
            ) as event_cursor:
                sequence_row = await event_cursor.fetchone()
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'ExecutionIntentTerminalReconciled', ?, ?)
                """,
                (
                    round_id,
                    int(sequence_row[0] if sequence_row else 1),
                    json.dumps(
                        {
                            "intent_id": intent_id,
                            "previous_defer_code": previous_defer_code,
                            "resolution": resolution,
                            "status": "executed",
                            "trade_id": int(trade_id),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    reconciled_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (EXECUTED_INTENT_DEFER_REPAIR_VERSION, reconciled_at),
        )

    # Enforce the current-state meaning of ``defer_code`` at the database
    # boundary.  The exact-lineage repair above runs first so known historical
    # contradictions are reconciled with append-only audit evidence before the
    # guard becomes active.  Unknown legacy rows are intentionally preserved,
    # but no writer (including a stale runner or an ad-hoc SQLite caller) may
    # create another executed intent that still advertises a current blocker.
    if "execution_intents" in current_tables:
        await conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS
                execution_intents_executed_defer_guard_insert
            BEFORE INSERT ON execution_intents
            WHEN NEW.status = 'executed' AND NEW.defer_code IS NOT NULL
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'executed execution_intent must clear defer_code'
                );
            END;

            CREATE TRIGGER IF NOT EXISTS
                execution_intents_executed_defer_guard_update
            BEFORE UPDATE ON execution_intents
            WHEN NEW.status = 'executed' AND NEW.defer_code IS NOT NULL
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'executed execution_intent must clear defer_code'
                );
            END;
            """
        )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (
                EXECUTED_INTENT_SEMANTIC_GUARD_VERSION,
                datetime.now().isoformat(),
            ),
        )

    # ``defer_code`` on a pending decision is also current-state data.  Older
    # fill transactions marked the exact replay row executed but left the
    # market-closure blocker in place.  Reconcile only rows linked to a real
    # fill through their durable intent id; unknown legacy rows stay untouched.
    if {
        "simulation_pending_decisions",
        "simulation_trades",
        "research_rounds",
        "round_events",
    }.issubset(current_tables):
        async with conn.execute(
            """
            SELECT pending.id, pending.round_id, pending.intent_id,
                   pending.defer_code, pending.resolution, trade.id
            FROM simulation_pending_decisions pending
            JOIN simulation_trades trade
              ON trade.intent_id = pending.intent_id
            WHERE pending.status = 'executed'
              AND pending.defer_code <> ''
              AND pending.intent_id IS NOT NULL
            ORDER BY datetime(pending.resolved_at), pending.id
            """
        ) as cursor:
            resolved_pending = await cursor.fetchall()
        reconciled_at = datetime.now().isoformat()
        for (
            pending_id,
            round_id,
            intent_id,
            previous_defer_code,
            resolution,
            trade_id,
        ) in resolved_pending:
            cursor = await conn.execute(
                """
                UPDATE simulation_pending_decisions
                SET defer_code = ''
                WHERE id = ? AND status = 'executed' AND defer_code = ?
                """,
                (int(pending_id), previous_defer_code),
            )
            if not cursor.rowcount or not round_id:
                continue
            async with conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 "
                "FROM round_events WHERE round_id = ?",
                (round_id,),
            ) as event_cursor:
                sequence_row = await event_cursor.fetchone()
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'PendingDecisionTerminalReconciled', ?, ?)
                """,
                (
                    round_id,
                    int(sequence_row[0] if sequence_row else 1),
                    json.dumps(
                        {
                            "intent_id": intent_id,
                            "pending_decision_id": int(pending_id),
                            "previous_defer_code": previous_defer_code,
                            "resolution": resolution,
                            "status": "executed",
                            "trade_id": int(trade_id),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    reconciled_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (EXECUTED_PENDING_DEFER_REPAIR_VERSION, reconciled_at),
        )

    # Install the guard after the exact-lineage repair.  A stale writer must
    # fail its whole transaction instead of recreating an executed-but-blocked
    # replay row.
    if "simulation_pending_decisions" in current_tables:
        await conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS
                simulation_pending_executed_defer_guard_insert
            BEFORE INSERT ON simulation_pending_decisions
            WHEN NEW.status = 'executed' AND NEW.defer_code <> ''
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'executed pending decision must clear defer_code'
                );
            END;

            CREATE TRIGGER IF NOT EXISTS
                simulation_pending_executed_defer_guard_update
            BEFORE UPDATE ON simulation_pending_decisions
            WHEN NEW.status = 'executed' AND NEW.defer_code <> ''
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'executed pending decision must clear defer_code'
                );
            END;
            """
        )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (
                EXECUTED_PENDING_SEMANTIC_GUARD_VERSION,
                datetime.now().isoformat(),
            ),
        )

    # A linked exit intent represents a mandatory open-position lifecycle
    # obligation.  It must not age out merely because the market stayed closed
    # or a realtime quote provider remained unavailable longer than the entry
    # thesis TTL.  Reconcile only active pending rows with exact intent lineage;
    # unlinked legacy rows and terminal history remain untouched.  Preserve the
    # former expiry in the append-only event when a real round is known.
    if {
        "simulation_pending_decisions",
        "execution_intents",
        "research_rounds",
        "round_events",
    }.issubset(current_tables):
        async with conn.execute(
            """
            SELECT pending.id, pending.round_id, pending.intent_id,
                   pending.status, pending.expires_at
            FROM simulation_pending_decisions pending
            JOIN execution_intents intent ON intent.id = pending.intent_id
            WHERE pending.status IN (
                      'pending_next_trading_session', 'replaying'
                  )
              AND pending.direction IN ('sell', 'short')
              AND intent.direction IN ('sell', 'short')
              AND intent.status IN ('pending', 'deferred', 'executing')
              AND pending.expires_at IS NOT NULL
            ORDER BY datetime(pending.created_at), pending.id
            """
        ) as cursor:
            expiring_exits = await cursor.fetchall()
        reconciled_at = datetime.now().isoformat()
        for (
            pending_id,
            round_id,
            intent_id,
            pending_status,
            previous_expires_at,
        ) in expiring_exits:
            cursor = await conn.execute(
                """
                UPDATE simulation_pending_decisions
                SET expires_at = NULL, updated_at = ?
                WHERE id = ?
                  AND intent_id = ?
                  AND status IN (
                      'pending_next_trading_session', 'replaying'
                  )
                  AND direction IN ('sell', 'short')
                  AND expires_at = ?
                """,
                (
                    reconciled_at,
                    int(pending_id),
                    intent_id,
                    previous_expires_at,
                ),
            )
            if not cursor.rowcount or not round_id:
                continue
            async with conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 "
                "FROM round_events WHERE round_id = ?",
                (round_id,),
            ) as event_cursor:
                sequence_row = await event_cursor.fetchone()
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'PendingExitExpiryReconciled', ?, ?)
                """,
                (
                    round_id,
                    int(sequence_row[0] if sequence_row else 1),
                    json.dumps(
                        {
                            "intent_id": intent_id,
                            "pending_decision_id": int(pending_id),
                            "previous_expires_at": previous_expires_at,
                            "status": pending_status,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    reconciled_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (PENDING_EXIT_NONEXPIRING_REPAIR_VERSION, reconciled_at),
        )

    # Protect the durable-exit contract at SQLite itself so a stale runner or
    # direct writer cannot silently restore a TTL after the repair.
    if "simulation_pending_decisions" in current_tables:
        await conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS
                simulation_pending_exit_expiry_guard_insert
            BEFORE INSERT ON simulation_pending_decisions
            WHEN NEW.status IN (
                     'pending_next_trading_session', 'replaying'
                 )
             AND NEW.direction IN ('sell', 'short')
             AND NEW.intent_id IS NOT NULL
             AND NEW.expires_at IS NOT NULL
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'active linked exit pending decision must not expire'
                );
            END;

            CREATE TRIGGER IF NOT EXISTS
                simulation_pending_exit_expiry_guard_update
            BEFORE UPDATE ON simulation_pending_decisions
            WHEN NEW.status IN (
                     'pending_next_trading_session', 'replaying'
                 )
             AND NEW.direction IN ('sell', 'short')
             AND NEW.intent_id IS NOT NULL
             AND NEW.expires_at IS NOT NULL
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'active linked exit pending decision must not expire'
                );
            END;
            """
        )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (
                PENDING_EXIT_NONEXPIRING_GUARD_VERSION,
                datetime.now().isoformat(),
            ),
        )

    # Re-audit deterministic legacy states on every startup. An older runner
    # can finish after the migration version was first recorded during a
    # rolling source upgrade.
    if {
        "research_rounds",
        "round_events",
        "simulation_trades",
    }.issubset(current_tables):
        async with conn.execute(
            """
            SELECT rr.id, rr.terminal_code, MIN(st.id), MAX(st.id), COUNT(*)
            FROM research_rounds rr
            JOIN simulation_trades st ON st.round_id = rr.id
            WHERE COALESCE(rr.terminal_code, '') <> 'filled'
              AND rr.status <> 'failed'
              AND COALESCE(rr.terminal_code, '') NOT IN (
                  'failed', 'pipeline_exception', 'cancelled', 'interrupted'
              )
            GROUP BY rr.id, rr.terminal_code
            ORDER BY rr.started_at, rr.id
            """
        ) as cursor:
            linked_fill_rounds = await cursor.fetchall()
        repaired_at = datetime.now().isoformat()
        for round_id, previous_code, first_trade_id, last_trade_id, fill_count in (
            linked_fill_rounds
        ):
            async with conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM round_events
                WHERE round_id = ?
                """,
                (round_id,),
            ) as cursor:
                sequence_row = await cursor.fetchone()
            await conn.execute(
                """
                UPDATE research_rounds
                SET terminal_code = 'filled',
                    terminal_reason = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    (
                        "已由可审计模拟成交修复终态；"
                        f"fills={int(fill_count)}; "
                        f"trade_ids={int(first_trade_id)}..{int(last_trade_id)}"
                    ),
                    repaired_at,
                    round_id,
                ),
            )
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'RoundTerminalReconciled', ?, ?)
                """,
                (
                    round_id,
                    int(sequence_row[0] if sequence_row else 1),
                    json.dumps(
                        {
                            "fill_count": int(fill_count),
                            "first_trade_id": int(first_trade_id),
                            "last_trade_id": int(last_trade_id),
                            "previous_terminal_code": previous_code,
                            "terminal_code": "filled",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    repaired_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (ROUND_FILL_TERMINAL_REPAIR_VERSION, repaired_at),
        )

    if {
        "research_rounds",
        "round_events",
    }.issubset(current_tables):
        async with conn.execute(
            """
            SELECT id, terminal_code
            FROM research_rounds
            WHERE status = 'failed'
              AND terminal_code IN (
                  'pipeline_exception', 'cancelled', 'interrupted'
              )
            ORDER BY started_at, id
            """
        ) as cursor:
            legacy_failed_rounds = await cursor.fetchall()
        repaired_at = datetime.now().isoformat()
        for round_id, previous_code in legacy_failed_rounds:
            async with conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM round_events
                WHERE round_id = ?
                """,
                (round_id,),
            ) as cursor:
                sequence_row = await cursor.fetchone()
            await conn.execute(
                """
                UPDATE research_rounds
                SET terminal_code = 'failed', updated_at = ?
                WHERE id = ? AND status = 'failed'
                """,
                (repaired_at, round_id),
            )
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'RoundTerminalReconciled', ?, ?)
                """,
                (
                    round_id,
                    int(sequence_row[0] if sequence_row else 1),
                    json.dumps(
                        {
                            "previous_terminal_code": previous_code,
                            "terminal_code": "failed",
                            "reason": "canonical_failed_terminal_contract",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    repaired_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (ROUND_FAILED_TERMINAL_REPAIR_VERSION, repaired_at),
        )

    # A cooperative deployment stop can land after a business terminal was
    # already appended but before reflection/completed finalization.  Older
    # shutdown handling overwrote that exact outcome with ``failed``.  Repair
    # only rows whose own append-only lineage proves NoEvidenceTerminal came
    # before a SIGTERM RoundFailed, with no linked fill or RoundCompleted.
    # Unknown historical failures remain untouched.  This audit runs on every
    # startup because a stale runner may finish after the version is recorded.
    if {
        "research_rounds",
        "round_events",
    }.issubset(current_tables):
        linked_fill_filter = (
            """
              AND NOT EXISTS (
                  SELECT 1 FROM simulation_trades st
                  WHERE st.round_id = rr.id
              )
            """
            if "simulation_trades" in current_tables
            else ""
        )
        async with conn.execute(
            f"""
            SELECT rr.id, rr.terminal_reason,
                   ne.sequence, ne.payload_json, ne.created_at,
                   rf.sequence, rf.payload_json
            FROM research_rounds rr
            JOIN round_events ne
              ON ne.round_id = rr.id
             AND ne.event_type = 'NoEvidenceTerminal'
            JOIN round_events rf
              ON rf.round_id = rr.id
             AND rf.event_type = 'RoundFailed'
             AND rf.sequence > ne.sequence
            WHERE rr.status = 'failed'
              AND rr.terminal_code = 'failed'
              {linked_fill_filter}
              AND NOT EXISTS (
                  SELECT 1 FROM round_events completed
                  WHERE completed.round_id = rr.id
                    AND completed.event_type = 'RoundCompleted'
              )
            ORDER BY rr.started_at, rr.id, ne.sequence, rf.sequence
            """
        ) as cursor:
            candidates = await cursor.fetchall()

        repaired_round_ids: set[str] = set()
        repaired_at = datetime.now().isoformat()
        for (
            round_id,
            previous_reason,
            business_sequence,
            business_payload_json,
            business_created_at,
            shutdown_sequence,
            shutdown_payload_json,
        ) in candidates:
            round_id = str(round_id)
            if round_id in repaired_round_ids:
                continue
            try:
                shutdown_payload = json.loads(str(shutdown_payload_json or "{}"))
                business_payload = json.loads(str(business_payload_json or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if (
                shutdown_payload.get("shutdown_signal") != "SIGTERM"
                or str(shutdown_payload.get("code") or "failed") != "failed"
            ):
                continue

            cursor = await conn.execute(
                """
                UPDATE research_rounds
                SET status = 'no_evidence', current_stage = 'no_evidence',
                    terminal_code = 'no_evidence',
                    terminal_reason = ?, updated_at = ?, completed_at = ?
                WHERE id = ? AND status = 'failed' AND terminal_code = 'failed'
                """,
                (
                    "阶段2没有形成满足证据约束的结构化提案",
                    repaired_at,
                    business_created_at,
                    round_id,
                ),
            )
            if not cursor.rowcount:
                continue
            repaired_round_ids.add(round_id)

            async with conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 "
                "FROM round_events WHERE round_id = ?",
                (round_id,),
            ) as event_cursor:
                sequence_row = await event_cursor.fetchone()
            next_sequence = int(sequence_row[0] if sequence_row else 1)
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'RoundTerminalReconciled', ?, ?)
                """,
                (
                    round_id,
                    next_sequence,
                    json.dumps(
                        {
                            "business_terminal_event": "NoEvidenceTerminal",
                            "business_terminal_sequence": int(business_sequence),
                            "previous_terminal_code": "failed",
                            "previous_terminal_reason": previous_reason,
                            "shutdown_event_sequence": int(shutdown_sequence),
                            "terminal_code": "no_evidence",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    repaired_at,
                ),
            )
            await conn.execute(
                """
                INSERT INTO round_events (
                    round_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, 'RoundFinalizationInterrupted', ?, ?)
                """,
                (
                    round_id,
                    next_sequence + 1,
                    json.dumps(
                        {
                            "business_terminal_code": "no_evidence",
                            "business_terminal_payload": business_payload,
                            "interrupted_stage": "no_evidence",
                            "interrupted_status": "no_evidence",
                            "reconciled_from_overwrite": True,
                            "shutdown_signal": "SIGTERM",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    repaired_at,
                ),
            )
        await conn.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
            VALUES (?, ?)
            """,
            (SIGTERM_BUSINESS_TERMINAL_REPAIR_VERSION, repaired_at),
        )

    await _add_columns(
        conn,
        "simulation_redeployment_state",
        (
            ("last_entry_trade_count", "INTEGER NOT NULL DEFAULT 0"),
            ("last_exit_trade_count", "INTEGER NOT NULL DEFAULT 0"),
        ),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (REDEPLOYMENT_DIRECTION_AUDIT_VERSION, datetime.now().isoformat()),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (COMMITTEE_MEETING_PROTOCOL_VERSION, datetime.now().isoformat()),
    )

    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (ARCHITECTURE_MIGRATION_VERSION, datetime.now().isoformat()),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (TRADING_READINESS_MIGRATION_VERSION, datetime.now().isoformat()),
    )

    # PR1.3 — policy_snapshots is the authoritative, immutable record of what
    # was actually loaded at production start. The legacy columns (config_json,
    # source, active) remain for backwards compatibility; new columns capture
    # the full version manifest: code hashes, prompt versions, model settings,
    # policy-module versions, content hash, change reason and behaviour flag.
    # Credentials, API keys and proxy passwords are never part of the manifest
    # or its hash input.
    await _add_columns(
        conn,
        "policy_snapshots",
        (
            ("content_hash", "TEXT"),
            ("manifest_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("effective_config_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("prompt_versions_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("model_versions_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("policy_module_versions_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("source_manifest_path", "TEXT"),
            ("effective_at", "TEXT"),
            ("change_reason", "TEXT"),
            ("changes_behavior", "INTEGER NOT NULL DEFAULT 0"),
        ),
    )
    # Version content is immutable once written. Activation is tracked through
    # round_events (PolicySnapshotActivated), never by mutating the row. The
    # legacy ``active`` column is preserved but the trigger below blocks any
    # attempt to rewrite manifest/content fields after the fact.
    if "policy_snapshots" in await _tables(conn):
        await conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS policy_snapshots_immutable_update
            BEFORE UPDATE OF
                content_hash, manifest_json, effective_config_json,
                prompt_versions_json, model_versions_json,
                policy_module_versions_json, source_manifest_path,
                policy_family, version
            ON policy_snapshots
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'policy_snapshots content is immutable; activate a new version instead'
                );
            END;

            CREATE TRIGGER IF NOT EXISTS policy_snapshots_immutable_delete
            BEFORE DELETE ON policy_snapshots
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'policy_snapshots is append-only; supersede instead of deleting'
                );
            END;
            """
        )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (POLICY_SNAPSHOT_MANIFEST_VERSION, datetime.now().isoformat()),
    )

    # PR2.1 / PR2.2 — instrument identity and trading rules.  A six-digit code
    # cannot disambiguate a stock from an index (000001 is Ping An Bank on SZ
    # and the Shanghai Composite on SH), and a single ``lot_size`` cannot
    # express minimum first-buy size, incremental step, odd-lot sell rules,
    # settlement cycle or trading venue.  New nullable columns capture these;
    # legacy rows remain valid and the existing ``lot_size`` continues to
    # drive backwards-compatible execution paths.
    await _add_columns(
        conn,
        "instrument_master",
        (
            ("market", "TEXT"),
            ("code", "TEXT"),
            ("kind", "TEXT"),
            ("min_first_buy_size", "INTEGER"),
            ("incremental_step", "INTEGER"),
            ("sell_odd_lot_rule", "TEXT"),
            ("settlement", "TEXT"),
            ("trading_venue", "TEXT"),
        ),
    )
    # PR2.1 — trading calendar records the source identity, the kind of tape
    # it was derived from, and the validity range of the observation.  A row
    # whose ``is_open=0`` because the index tape was empty is NOT the same as
    # a confirmed market holiday; ``inference_rule`` and ``quality_status``
    # let downstream code tell them apart.
    await _add_columns(
        conn,
        "trading_calendar",
        (
            ("source_identity", "TEXT"),
            ("source_kind", "TEXT"),
            ("validity_start", "TEXT"),
            ("validity_end", "TEXT"),
            ("inference_rule", "TEXT"),
        ),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (INSTRUMENT_IDENTITY_VERSION, datetime.now().isoformat()),
    )

    # PR2.3 / PR2.4 — preserve every daily-price write as an immutable
    # revision, and log every data-fetch failure with a concrete next action.
    # ``daily_prices`` stays as the "latest" view used by live decisions; a
    # frozen evaluation references a specific revision via ``batch_id`` so a
    # provider revision cannot silently rewrite history.
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_price_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            source TEXT,
            adjustment TEXT,
            provider TEXT,
            fetched_at TEXT NOT NULL,
            raw_response_hash TEXT,
            batch_coverage_start TEXT,
            batch_coverage_end TEXT,
            quality_status TEXT,
            trade_status TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_daily_price_revisions_ticker_date
            ON daily_price_revisions(ticker, date, created_at);

        CREATE INDEX IF NOT EXISTS idx_daily_price_revisions_batch
            ON daily_price_revisions(batch_id, ticker, date);

        CREATE TRIGGER IF NOT EXISTS daily_price_revisions_immutable_update
        BEFORE UPDATE ON daily_price_revisions
        BEGIN
            SELECT RAISE(
                ABORT,
                'daily_price_revisions is append-only; insert a new revision instead'
            );
        END;

        CREATE TRIGGER IF NOT EXISTS daily_price_revisions_immutable_delete
        BEFORE DELETE ON daily_price_revisions
        BEGIN
            SELECT RAISE(
                ABORT,
                'daily_price_revisions is append-only; insert a new revision instead'
            );
        END;

        CREATE TABLE IF NOT EXISTS data_fetch_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            date TEXT,
            provider TEXT,
            error TEXT NOT NULL,
            next_action TEXT NOT NULL,
            context_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_data_fetch_failures_ticker
            ON data_fetch_failures(ticker, date, created_at);
        """
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (DAILY_PRICE_REVISIONS_VERSION, datetime.now().isoformat()),
    )

    # PR3.4 — experiment registry.  One ``experiment_id`` identifies the
    # whole experiment (hypothesis, control snapshot, candidate snapshot,
    # clock, capital, universe, fee and rule versions, primary metric,
    # failure rule, minimum samples, rollback rule).  ``experiment_events``
    # is append-only lifecycle: registered → collecting → data_blocked /
    # insufficient_samples / rejected / eligible; ``promoted`` is recorded
    # separately and never auto-applied to main config.  ``evaluation_runs``
    # references ``experiment_id`` and ``arm`` so a daily估值 update does
    # not manufacture a new "trial" of the same experiment.
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS experiment_registry (
            experiment_id TEXT PRIMARY KEY,
            hypothesis TEXT NOT NULL,
            frozen_at TEXT NOT NULL,
            control_policy_snapshot_id TEXT NOT NULL,
            candidate_policy_snapshot_id TEXT NOT NULL,
            source_runtime_manifest TEXT,
            input_scope TEXT NOT NULL DEFAULT 'shared_input_package',
            decision_node_changed TEXT NOT NULL,
            information_cutoff_rule TEXT NOT NULL,
            observation_start TEXT NOT NULL,
            benchmark_definition TEXT NOT NULL,
            execution_clock TEXT NOT NULL,
            initial_capital REAL NOT NULL CHECK(initial_capital = 10000),
            universe_policy TEXT NOT NULL,
            fee_version TEXT NOT NULL,
            instrument_rule_version TEXT NOT NULL,
            primary_metric TEXT NOT NULL,
            failure_rule TEXT NOT NULL,
            minimum_samples INTEGER NOT NULL,
            review_schedule TEXT NOT NULL,
            rollback_rule TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'registered',
            last_usable_session TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(control_policy_snapshot_id)
                REFERENCES policy_snapshots(id),
            FOREIGN KEY(candidate_policy_snapshot_id)
                REFERENCES policy_snapshots(id)
        );

        CREATE TABLE IF NOT EXISTS experiment_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(experiment_id)
                REFERENCES experiment_registry(experiment_id),
            UNIQUE(experiment_id, sequence)
        );

        CREATE INDEX IF NOT EXISTS idx_experiment_registry_status
            ON experiment_registry(status, frozen_at);
        CREATE INDEX IF NOT EXISTS idx_experiment_events_experiment
            ON experiment_events(experiment_id, sequence);
        """
    )
    # evaluation_runs gains experiment_id and arm columns so daily估值
    # updates reference the experiment they belong to.  Existing rows keep
    # NULL; the legacy ``sleeve`` column continues to drive older queries.
    await _add_columns(
        conn,
        "evaluation_runs",
        (
            ("experiment_id", "TEXT"),
            ("arm", "TEXT"),
            ("origin_signal_id", "TEXT"),
        ),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (EXPERIMENT_REGISTRY_VERSION, datetime.now().isoformat()),
    )

    # PR5.1 — empirical observations register.  Each row is one observation
    # with its scope, supporting and counter-examples, deduplication
    # metadata, lifecycle and retraction conditions.  Status is one of
    # hypothesis / under_test / supported / retracted; a retraction never
    # deletes the row, it records the retraction reason and timestamp.
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS empirical_observations (
            id TEXT PRIMARY KEY,
            observation_type TEXT NOT NULL,
            scope_json TEXT NOT NULL DEFAULT '{}',
            supporting_examples_json TEXT NOT NULL DEFAULT '[]',
            counter_examples_json TEXT NOT NULL DEFAULT '[]',
            origin_prediction_ids_json TEXT NOT NULL DEFAULT '[]',
            origin_round_ids_json TEXT NOT NULL DEFAULT '[]',
            deduplicated_sample_count INTEGER NOT NULL DEFAULT 0,
            overlap_group_ids_json TEXT NOT NULL DEFAULT '[]',
            first_recorded_at TEXT NOT NULL,
            last_validated_at TEXT,
            expiry_condition TEXT,
            retraction_condition TEXT,
            status TEXT NOT NULL DEFAULT 'hypothesis',
            retracted_reason TEXT,
            retracted_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (status IN (
                'hypothesis', 'under_test', 'supported', 'retracted'
            ))
        );

        CREATE INDEX IF NOT EXISTS idx_empirical_observations_status
            ON empirical_observations(status, last_validated_at);

        CREATE TRIGGER IF NOT EXISTS empirical_observations_immutable_delete
        BEFORE DELETE ON empirical_observations
        BEGIN
            SELECT RAISE(
                ABORT,
                'empirical_observations is append-only; retract instead of deleting'
            );
        END;
        """
    )

    # PR5.2 — structured evidence package.  One row per claim extracted by
    # the candidate evidence organizer.  Numbers must trace back to a
    # source excerpt; inference and fact are separated by ``claim_type``;
    # reprints of the same announcement share a ``shared_source_group_id``
    # so multi-seat same-source cannot be counted as independent
    # confirmation.
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS structured_evidence_claims (
            id TEXT PRIMARY KEY,
            claim_id TEXT NOT NULL UNIQUE,
            source_document_id TEXT,
            source_url TEXT,
            original_excerpt TEXT NOT NULL,
            publication_time TEXT,
            first_seen_at TEXT NOT NULL,
            effective_period TEXT,
            claim_type TEXT NOT NULL CHECK(
                claim_type IN ('fact', 'inference', 'unknown')
            ),
            ticker_relevance TEXT NOT NULL DEFAULT '[]',
            valuation_inputs TEXT NOT NULL DEFAULT '{}',
            priced_in_counterargument TEXT,
            catalyst TEXT,
            catalyst_deadline TEXT,
            failure_condition TEXT,
            unknown_fields TEXT NOT NULL DEFAULT '[]',
            shared_source_group_id TEXT,
            experiment_id TEXT,
            arm TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(experiment_id)
                REFERENCES experiment_registry(experiment_id)
        );

        CREATE INDEX IF NOT EXISTS idx_structured_evidence_experiment
            ON structured_evidence_claims(experiment_id, arm, claim_id);

        CREATE INDEX IF NOT EXISTS idx_structured_evidence_source_group
            ON structured_evidence_claims(shared_source_group_id, claim_id);
        """
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (EMPIRICAL_OBSERVATIONS_VERSION, datetime.now().isoformat()),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations(version, applied_at)
        VALUES (?, ?)
        """,
        (STRUCTURED_EVIDENCE_PACKAGE_VERSION, datetime.now().isoformat()),
    )
    await conn.commit()
