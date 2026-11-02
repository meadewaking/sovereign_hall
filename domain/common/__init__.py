"""Shared domain primitives."""

from .events import DomainEvent
from .ids import new_id

__all__ = ["DomainEvent", "new_id"]

