from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_registry (
  id         TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  card_url   TEXT NOT NULL,
  card       TEXT NOT NULL,
  health     TEXT NOT NULL DEFAULT 'unknown',
  last_seen  TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contexts (
  context_id     TEXT PRIMARY KEY,
  state          TEXT NOT NULL DEFAULT '{}',
  title          TEXT NOT NULL DEFAULT '',
  rewind_markers TEXT NOT NULL DEFAULT '[]',
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL
);
"""


class Database:
    """Thin aiosqlite wrapper used for the agent registry."""

    def __init__(self, path: Path | str):
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._tx_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None (autocommit): statements never leave an implicit
        # read transaction open, which would otherwise upgrade to a write while
        # another connection (the A2A task store) has committed and surface as
        # "database is locked" (SQLITE_BUSY_SNAPSHOT) under parallel nodes.
        self._conn = await aiosqlite.connect(
            self._path, timeout=30, isolation_level=None
        )
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA busy_timeout=30000")
        await self._conn.execute("PRAGMA foreign_keys=ON")

    async def initialize(self) -> None:
        if self._conn is None:
            await self.connect()
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database is not connected; call initialize() first")
        return self._conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._tx_lock:
            conn = self.conn
            await conn.execute("BEGIN")
            try:
                yield conn
            except BaseException:
                await conn.rollback()
                raise
            else:
                await conn.commit()
