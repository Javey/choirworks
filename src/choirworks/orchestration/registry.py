from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from uuid import uuid4

from a2a.client.errors import AgentCardResolutionError

from choirworks.a2a.client import RemoteAgentClient
from choirworks.models.domain import AgentRecord, AgentRegistration
from choirworks.store.db import Database

_UPSERT_COLUMNS = "id, name, card_url, card, health, last_seen, created_at"


class DuplicateAgentName(RuntimeError):
    pass


class AgentRegistry:
    def __init__(self, db: Database, remote: RemoteAgentClient):
        self._db = db
        self._remote = remote

    async def register(self, name: str, card_url: str) -> AgentRecord:
        registration = AgentRegistration(name=name, card_url=card_url)
        name, card_url = registration.name, registration.card_url
        if await self.get_by_name(name) is not None:
            raise DuplicateAgentName(f"agent name already registered: {name}")
        card = await self._remote.resolve_card(card_url)
        now = datetime.now(UTC)
        record = AgentRecord(
            id=uuid4().hex,
            name=name,
            card_url=card_url,
            card=self._remote.card_to_dict(card),
            health="ok",
            last_seen=now,
            created_at=now,
        )
        try:
            async with self._db.transaction() as conn:
                await conn.execute(
                    f"INSERT INTO agent_registry ({_UPSERT_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.id,
                        record.name,
                        record.card_url,
                        json.dumps(record.card, ensure_ascii=False),
                        record.health,
                        record.last_seen.isoformat(),
                        record.created_at.isoformat(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            if await self.get_by_name(name) is not None:
                raise DuplicateAgentName(f"agent name already registered: {name}") from exc
            raise
        self._remote.cache_card(card_url, card)
        return record

    async def list(self) -> list[AgentRecord]:
        cursor = await self._db.conn.execute("SELECT * FROM agent_registry ORDER BY name")
        return [self._row_to_record(row) for row in await cursor.fetchall()]

    async def get(self, agent_id: str) -> AgentRecord | None:
        cursor = await self._db.conn.execute(
            "SELECT * FROM agent_registry WHERE id = ?", (agent_id,)
        )
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def get_by_name(self, name: str) -> AgentRecord | None:
        cursor = await self._db.conn.execute("SELECT * FROM agent_registry WHERE name = ?", (name,))
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def delete(self, agent_id: str) -> bool:
        record = await self.get(agent_id)
        async with self._db.transaction() as conn:
            cursor = await conn.execute("DELETE FROM agent_registry WHERE id = ?", (agent_id,))
        if cursor.rowcount > 0 and record is not None:
            self._remote.invalidate(record.card_url)
        return cursor.rowcount > 0

    async def refresh(self, agent_id: str) -> AgentRecord:
        record = await self.get(agent_id)
        if record is None:
            raise KeyError(f"agent not found: {agent_id}")
        try:
            card = await self._remote.resolve_card(record.card_url)
        except AgentCardResolutionError:
            async with self._db.transaction() as conn:
                await conn.execute(
                    "UPDATE agent_registry SET health = 'unavailable' WHERE id = ?", (agent_id,)
                )
            raise
        now = datetime.now(UTC)
        updated = record.model_copy(
            update={
                "card": self._remote.card_to_dict(card),
                "health": "ok",
                "last_seen": now,
            }
        )
        async with self._db.transaction() as conn:
            cursor = await conn.execute(
                "UPDATE agent_registry SET card = ?, health = ?, last_seen = ? WHERE id = ?",
                (
                    json.dumps(updated.card, ensure_ascii=False),
                    updated.health,
                    updated.last_seen.isoformat(),
                    agent_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError(f"agent not found: {agent_id}")
        self._remote.cache_card(record.card_url, card)
        return updated

    @staticmethod
    def agent_url(record: AgentRecord) -> str:
        return record.card_url

    @staticmethod
    def _row_to_record(row) -> AgentRecord:
        return AgentRecord(
            id=row["id"],
            name=row["name"],
            card_url=row["card_url"],
            card=json.loads(row["card"]),
            health=row["health"],
            last_seen=(datetime.fromisoformat(row["last_seen"]) if row["last_seen"] else None),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
