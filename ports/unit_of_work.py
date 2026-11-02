"""Unit-of-work protocol."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol


class UnitOfWork(Protocol):
    def transaction(self, *, immediate: bool = False) -> AbstractAsyncContextManager:
        ...

