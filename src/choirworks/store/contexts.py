from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

from choirworks.store.db import Database


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class ContextRecord:
    context_id: str
    state: str
    title: str
    created_at: str
    updated_at: str
    rewind_markers: str = field(default="[]")


class ContextStore:
    """Canonical per-conversation state, keyed by A2A context_id."""

    def __init__(self, db: Database):
        self._db = db

    async def get(self, context_id: str) -> ContextRecord | None:
        cursor = await self._db.conn.execute(
            "SELECT context_id, state, title, created_at, updated_at,"
            " rewind_markers FROM contexts WHERE context_id = ?",
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
            rewind_markers=row["rewind_markers"] or "[]",
        )

    async def list(self) -> list[ContextRecord]:
        cursor = await self._db.conn.execute(
            "SELECT context_id, state, title, created_at, updated_at,"
            " rewind_markers FROM contexts ORDER BY updated_at ASC, rowid ASC"
        )
        rows = await cursor.fetchall()
        return [
            ContextRecord(
                context_id=row["context_id"],
                state=row["state"],
                title=row["title"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                rewind_markers=row["rewind_markers"] or "[]",
            )
            for row in rows
        ]

    async def rewind(
        self,
        context_id: str,
        *,
        state: str,
        before_task_id: str,
        cut_task_id: str,
    ) -> None:
        record = await self.get(context_id)
        if record is None:
            raise KeyError(context_id)
        markers = json.loads(record.rewind_markers)
        markers.append({
            "before_task_id": before_task_id,
            "cut_task_id": cut_task_id,
            "created_at": _now(),
        })
        await self._db.conn.execute(
            "UPDATE contexts SET state = ?, rewind_markers = ?, updated_at = ?"
            " WHERE context_id = ?",
            (state, json.dumps(markers, ensure_ascii=False), _now(), context_id),
        )
        await self._db.conn.commit()

    async def create(self, context_id: str, *, title: str = "") -> None:
        now = _now()
        await self._db.conn.execute(
            "INSERT INTO contexts (context_id, state, title, created_at, updated_at)"
            " VALUES (?, '{}', ?, ?, ?)"
            " ON CONFLICT(context_id) DO UPDATE SET title = excluded.title"
            " WHERE contexts.title = ''",
            (context_id, title, now, now),
        )
        await self._db.conn.commit()

    async def upsert_state(self, context_id: str, state: str) -> None:
        now = _now()
        await self._db.conn.execute(
            "INSERT INTO contexts (context_id, state, title, created_at, updated_at)"
            " VALUES (?, ?, '', ?, ?)"
            " ON CONFLICT(context_id) DO UPDATE SET"
            " state = excluded.state, updated_at = excluded.updated_at",
            (context_id, state, now, now),
        )
        await self._db.conn.commit()
