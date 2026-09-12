from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT NOT NULL,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq);

CREATE TABLE IF NOT EXISTS orchestration_tasks (
  id           TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  request      TEXT NOT NULL,
  policy       TEXT,
  plan_version INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plans (
  id         TEXT PRIMARY KEY,
  task_id    TEXT NOT NULL,
  version    INTEGER NOT NULL,
  dag        TEXT NOT NULL,
  rationale  TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, version)
);

CREATE TABLE IF NOT EXISTS nodes (
  id                TEXT PRIMARY KEY,
  task_id           TEXT NOT NULL,
  plan_id           TEXT NOT NULL,
  name              TEXT NOT NULL,
  agent_url         TEXT,
  skill_id          TEXT,
  deps              TEXT NOT NULL,
  input             TEXT,
  status            TEXT NOT NULL,
  attempt           INTEGER NOT NULL DEFAULT 0,
  requires_approval INTEGER NOT NULL DEFAULT 0,
  a2a_task_id       TEXT,
  a2a_context_id    TEXT,
  output            TEXT,
  error             TEXT,
  started_at        TEXT,
  ended_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_nodes_task ON nodes(task_id);
CREATE INDEX IF NOT EXISTS idx_nodes_a2a ON nodes(a2a_task_id);

CREATE TABLE IF NOT EXISTS interventions (
  id          TEXT PRIMARY KEY,
  task_id     TEXT NOT NULL,
  node_id     TEXT,
  source      TEXT NOT NULL,
  policy      TEXT NOT NULL,
  question    TEXT NOT NULL,
  answer      TEXT,
  responder   TEXT,
  status      TEXT NOT NULL,
  deadline_at TEXT,
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_interventions_task_status
  ON interventions(task_id, status);

CREATE TABLE IF NOT EXISTS checkpoints (
  id           TEXT PRIMARY KEY,
  task_id      TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  plan_version INTEGER NOT NULL,
  frontier     TEXT NOT NULL,
  artifacts    TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_registry (
  id         TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  card_url   TEXT NOT NULL,
  card       TEXT NOT NULL,
  health     TEXT NOT NULL DEFAULT 'unknown',
  last_seen  TEXT,
  created_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path | str):
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._tx_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.commit()

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
