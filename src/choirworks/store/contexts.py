from __future__ import annotations

from dataclasses import dataclass

from choirworks.core.util import now_iso
from choirworks.store.db import Database


@dataclass
class ContextRecord:
    context_id: str
    state: str
    title: str
    created_at: str
    updated_at: str


class ContextStore:
    """Canonical per-conversation state, keyed by A2A context_id."""

    def __init__(self, db: Database):
        self._db = db

    async def get(self, context_id: str) -> ContextRecord | None:
        cursor = await self._db.conn.execute(
            "SELECT context_id, state, title, created_at, updated_at"
            " FROM contexts WHERE context_id = ?",
            (context_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return ContextRecord(
            context_id=row["context_id"],
            state=row["state"],
            title=row["title"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def list(self) -> list[ContextRecord]:
        cursor = await self._db.conn.execute(
            "SELECT context_id, state, title, created_at, updated_at"
            " FROM contexts ORDER BY updated_at ASC, rowid ASC"
        )
        rows = await cursor.fetchall()
        return [
            ContextRecord(
                context_id=row["context_id"],
                state=row["state"],
                title=row["title"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def create(self, context_id: str, *, title: str = "") -> None:
        now = now_iso()
        await self._db.conn.execute(
            "INSERT INTO contexts (context_id, state, title, created_at, updated_at)"
            " VALUES (?, '{}', ?, ?, ?)"
            " ON CONFLICT(context_id) DO UPDATE SET title = excluded.title"
            " WHERE contexts.title = ''",
            (context_id, title, now, now),
        )
        await self._db.conn.commit()

    async def upsert_state(self, context_id: str, state: str) -> None:
        now = now_iso()
        await self._db.conn.execute(
            "INSERT INTO contexts (context_id, state, title, created_at, updated_at)"
            " VALUES (?, ?, '', ?, ?)"
            " ON CONFLICT(context_id) DO UPDATE SET"
            " state = excluded.state, updated_at = excluded.updated_at",
            (context_id, state, now, now),
        )
        await self._db.conn.commit()
