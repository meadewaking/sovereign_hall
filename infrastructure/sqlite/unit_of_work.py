"""SQLite transaction boundary."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio

import aiosqlite


class SQLiteUnitOfWork:
    def __init__(self, connection: aiosqlite.Connection):
        self.connection = connection

    @asynccontextmanager
    async def transaction(self, *, immediate: bool = False):
        if self.connection.in_transaction:
            raise RuntimeError("nested SQLite unit of work is not allowed")
        try:
            # BEGIN itself can finish in the worker after its awaiter is
            # cancelled. Queue rollback even when entry was interrupted.
            async with self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN"):
                pass
            yield self.connection
            # Commit can fail (for example on a deferred foreign key). It
            # belongs inside the cleanup boundary just like statement errors.
            await self.connection.commit()
        except BaseException:
            cleanup = asyncio.create_task(self.connection.rollback())
            # A repeated shutdown request must not release ownership while a
            # rollback is still queued behind the cancelled SQLite operation.
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
            raise
