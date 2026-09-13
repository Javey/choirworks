from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT,
  conversation_id TEXT,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq);

CREATE TABLE IF NOT EXISTS conversations (
  id         TEXT PRIMARY KEY,
  title      TEXT NOT NULL,
  created_at TEXT NOT NULL
);

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
  assigned_node_id TEXT,
  assigned_to TEXT,
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

CREATE TABLE IF NOT EXISTS messages (
  id              TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  seq             INTEGER NOT NULL,
  role            TEXT NOT NULL,
  sender          TEXT,
  text            TEXT NOT NULL,
  mentions        TEXT NOT NULL DEFAULT '[]',
  quote_id        TEXT,
  task_id         TEXT,
  node_id         TEXT,
  intervention_id TEXT,
  queued_for_node_id TEXT,
  delivered_at    TEXT,
  created_at      TEXT NOT NULL,
  UNIQUE(conversation_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_messages_room_seq ON messages(conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_messages_node ON messages(node_id);

CREATE TABLE IF NOT EXISTS room_members (
  conversation_id TEXT NOT NULL,
  agent_name      TEXT NOT NULL,
  agent_url       TEXT NOT NULL,
  reason          TEXT,
  joined_at       TEXT NOT NULL,
  PRIMARY KEY (conversation_id, agent_name)
);

CREATE TABLE IF NOT EXISTS room_summaries (
  conversation_id TEXT PRIMARY KEY,
  covers_seq      INTEGER NOT NULL,
  summary         TEXT NOT NULL,
  updated_at      TEXT NOT NULL
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
        await self._migrate(self.conn)
        await self.conn.commit()

    async def _migrate(self, conn: aiosqlite.Connection) -> None:
        cursor = await conn.execute("PRAGMA table_info(events)")
        event_columns = {row["name"] for row in await cursor.fetchall()}
        if "conversation_id" not in event_columns:
            await conn.execute("ALTER TABLE events RENAME TO events_legacy")
            await conn.execute(
                "CREATE TABLE events ("
                "  seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  task_id TEXT,"
                "  conversation_id TEXT,"
                "  type TEXT NOT NULL,"
                "  payload TEXT NOT NULL,"
                "  created_at TEXT NOT NULL"
                ")"
            )
            await conn.execute(
                "INSERT INTO events (seq, task_id, conversation_id, type, payload,"
                " created_at)"
                " SELECT seq, task_id, NULL, type, payload, created_at FROM events_legacy"
            )
            await conn.execute("DROP TABLE events_legacy")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_conversation_seq"
            " ON events(conversation_id, seq)"
        )
        cursor = await conn.execute("PRAGMA table_info(nodes)")
        columns = {row["name"] for row in await cursor.fetchall()}
        for name, ddl in (
            ("agent_name", "ALTER TABLE nodes ADD COLUMN agent_name TEXT"),
            ("policy_override", "ALTER TABLE nodes ADD COLUMN policy_override TEXT"),
        ):
            if name not in columns:
                await conn.execute(ddl)
        cursor = await conn.execute("PRAGMA table_info(orchestration_tasks)")
        task_columns = {row["name"] for row in await cursor.fetchall()}
        if "conversation_id" not in task_columns:
            await conn.execute(
                "ALTER TABLE orchestration_tasks ADD COLUMN conversation_id TEXT"
            )
        cursor = await conn.execute("PRAGMA table_info(interventions)")
        intervention_columns = {row["name"] for row in await cursor.fetchall()}
        if "assigned_node_id" not in intervention_columns:
            await conn.execute(
                "ALTER TABLE interventions ADD COLUMN assigned_node_id TEXT"
            )
        if "assigned_to" not in intervention_columns:
            await conn.execute("ALTER TABLE interventions ADD COLUMN assigned_to TEXT")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_conversation"
            " ON orchestration_tasks(conversation_id)"
        )
        await conn.commit()

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
