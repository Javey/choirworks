from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from choirworks.models.enums import EventType
from choirworks.store.db import Database
from choirworks.store.projections import apply_event


class Event(BaseModel):
    seq: int = 0
    task_id: str | None = None
    conversation_id: str | None = None
    type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class EventStore:
    def __init__(self, db: Database, bus: Any | None = None):
        self._db = db
        self._bus = bus

    async def append(
        self,
        task_id: str | None,
        event_type: EventType,
        payload: dict[str, Any] | None = None,
        *,
        conversation_id: str | None = None,
    ) -> Event:
        now = datetime.now(UTC)
        data = payload or {}
        async with self._db.transaction() as conn:
            resolved_conversation_id = conversation_id or data.get("conversation_id")
            if resolved_conversation_id is None and task_id is not None:
                cursor = await conn.execute(
                    "SELECT conversation_id FROM orchestration_tasks WHERE id = ?",
                    (task_id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    resolved_conversation_id = row["conversation_id"]
            cursor = await conn.execute(
                "INSERT INTO events (task_id, conversation_id, type, payload, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    task_id,
                    resolved_conversation_id,
                    event_type.value,
                    json.dumps(data, ensure_ascii=False, default=str),
                    now.isoformat(),
                ),
            )
            event = Event(
                seq=int(cursor.lastrowid),
                task_id=task_id,
                conversation_id=resolved_conversation_id,
                type=event_type,
                payload=data,
                created_at=now,
            )
            await apply_event(conn, event)
            if self._bus is not None:
                self._bus.publish(event)
        return event

    async def replay(self, task_id: str, after_seq: int = 0) -> list[Event]:
        cursor = await self._db.conn.execute(
            "SELECT seq, task_id, conversation_id, type, payload, created_at FROM events"
            " WHERE task_id = ? AND seq > ? ORDER BY seq",
            (task_id, after_seq),
        )
        return [self._row_to_event(row) for row in await cursor.fetchall()]

    async def replay_conversation(
        self, conversation_id: str, after_seq: int = 0
    ) -> list[Event]:
        cursor = await self._db.conn.execute(
            "SELECT seq, task_id, conversation_id, type, payload, created_at FROM events"
            " WHERE conversation_id = ? AND seq > ? ORDER BY seq",
            (conversation_id, after_seq),
        )
        return [self._row_to_event(row) for row in await cursor.fetchall()]

    async def replay_all(self) -> list[Event]:
        cursor = await self._db.conn.execute(
            "SELECT seq, task_id, conversation_id, type, payload, created_at"
            " FROM events ORDER BY seq"
        )
        return [self._row_to_event(row) for row in await cursor.fetchall()]

    async def latest_seq(self, task_id: str) -> int:
        cursor = await self._db.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS seq FROM events WHERE task_id = ?",
            (task_id,),
        )
        return int((await cursor.fetchone())["seq"])

    @staticmethod
    def _row_to_event(row: Any) -> Event:
        return Event(
            seq=row["seq"],
            task_id=row["task_id"],
            conversation_id=row["conversation_id"],
            type=EventType(row["type"]),
            payload=json.loads(row["payload"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
