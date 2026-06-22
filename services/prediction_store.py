"""
Shared prediction storage schema.

This module owns the lightweight SQLite schema used by decision recording,
prediction validation, and offline learning. Callers should not define their
own competing `price_predictions` table shape.
"""

from __future__ import annotations

import aiosqlite


PRICE_PREDICTION_COLUMNS: dict[str, str] = {
    "id": "TEXT PRIMARY KEY",
    "conclusion_id": "TEXT",
    "ticker": "TEXT NOT NULL",
    "current_price": "REAL",
    "target_price": "REAL",
    "stop_loss": "REAL",
    "direction": "TEXT",
    "confidence": "REAL",
    "predicted_at": "TEXT",
    "expected_days": "INTEGER",
    "actual_hit_price": "REAL",
    "actual_hit_date": "TEXT",
    "actual_hit_type": "TEXT",
    "max_price_reached": "REAL",
    "min_price_reached": "REAL",
    "status": "TEXT DEFAULT 'pending'",
    "result": "TEXT DEFAULT 'unknown'",
    "accuracy_score": "REAL",
    "created_at": "TEXT",
    "validated_at": "TEXT",
    "entry_date": "TEXT",
    "discussion_context": "TEXT",
}


async def ensure_prediction_tables(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as db:
        await ensure_prediction_schema(db)


async def ensure_prediction_schema(db: aiosqlite.Connection) -> None:
    await db.execute(_create_price_predictions_sql())

    existing = await _table_columns(db, "price_predictions")
    for name, definition in PRICE_PREDICTION_COLUMNS.items():
        if name not in existing:
            await db.execute(f"ALTER TABLE price_predictions ADD COLUMN {name} {definition}")

    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_ticker ON price_predictions(ticker)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_status ON price_predictions(status)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at ON price_predictions(predicted_at)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_due "
        "ON price_predictions(status, predicted_at, expected_days)"
    )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            period_days INTEGER,
            total_predictions INTEGER DEFAULT 0,
            correct_predictions INTEGER DEFAULT 0,
            partial_correct INTEGER DEFAULT 0,
            wrong_predictions INTEGER DEFAULT 0,
            avg_accuracy_score REAL,
            avg_return_pct REAL,
            win_rate REAL,
            sharpe_ratio REAL,
            calculated_at TEXT,
            UNIQUE(ticker, period_days)
        )
        """
    )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date)")
    await db.commit()


async def _table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return {row[1] async for row in cursor}


def _create_price_predictions_sql() -> str:
    columns = ",\n            ".join(
        f"{name} {definition}" for name, definition in PRICE_PREDICTION_COLUMNS.items()
    )
    return f"""
        CREATE TABLE IF NOT EXISTS price_predictions (
            {columns},
            FOREIGN KEY (conclusion_id) REFERENCES report_conclusions(id)
        )
    """
