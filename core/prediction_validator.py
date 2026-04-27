"""
Sovereign Hall - prediction validation helpers.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PredictionStatus(Enum):
    """Simple in-memory prediction status used by legacy tests."""

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class PredictionRecord:
    """A lightweight prediction record for unit tests and legacy scripts."""

    id: str
    ticker: str
    entry_price: float
    target_price: float
    stop_loss: float
    confidence: float
    status: PredictionStatus = PredictionStatus.PENDING
    actual_return: Optional[float] = None


class PredictionValidator:
    """In-memory validator kept for compatibility with older refactor tests."""

    def __init__(self, db_service=None):
        self.db_service = db_service
        self.predictions: Dict[str, PredictionRecord] = {}

    async def create_prediction(
        self,
        ticker: str,
        entry: float,
        target: float,
        stop: float,
        confidence: float,
    ) -> PredictionRecord:
        pred_id = f"{ticker}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        record = PredictionRecord(
            id=pred_id,
            ticker=ticker,
            entry_price=entry,
            target_price=target,
            stop_loss=stop,
            confidence=confidence,
        )
        self.predictions[pred_id] = record
        logger.info("Created prediction: %s", pred_id)
        return record

    def validate(self, pred_id: str, current_price: float) -> PredictionStatus:
        record = self.predictions.get(pred_id)
        if not record:
            return PredictionStatus.PENDING

        current_return = (current_price - record.entry_price) / record.entry_price

        if current_price >= record.target_price:
            record.status = PredictionStatus.SUCCESS
            record.actual_return = current_return
        elif current_price <= record.stop_loss:
            record.status = PredictionStatus.FAILED
            record.actual_return = current_return

        return record.status

    def get_stats(self) -> Dict:
        closed = [p for p in self.predictions.values() if p.status != PredictionStatus.PENDING]
        total = len(closed)
        if total == 0:
            return {"total": 0, "success": 0, "success_rate": 0.0, "avg_return": 0.0}

        success = len([p for p in closed if p.status == PredictionStatus.SUCCESS])
        returns = [p.actual_return for p in closed if p.actual_return is not None]
        return {
            "total": total,
            "success": success,
            "success_rate": success / total,
            "avg_return": sum(returns) / len(returns) if returns else 0.0,
        }
