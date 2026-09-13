from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

from choirworks.a2a.client import RemoteAgentClient
from choirworks.models.domain import AgentRecord
from choirworks.store.db import Database

_UPSERT_COLUMNS = "id, name, card_url, card, health, last_seen, created_at"


class DuplicateAgentName(RuntimeError):
    pass


class AgentRegistry:
    def __init__(self, db: Database, remote: RemoteAgentClient):
        self._db = db
        self._remote = remote

    async def register(self, name: str, card_url: str) -> AgentRecord:
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
        cursor = await self._db.conn.execute(
            "SELECT * FROM agent_registry WHERE name = ?", (name,)
        )
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def delete(self, agent_id: str) -> bool:
        async with self._db.transaction() as conn:
            cursor = await conn.execute(
                "DELETE FROM agent_registry WHERE id = ?", (agent_id,)
            )
        return cursor.rowcount > 0

    async def refresh(self, agent_id: str) -> AgentRecord:
        record = await self.get(agent_id)
        if record is None:
            raise KeyError(f"agent not found: {agent_id}")
        card = await self._remote.resolve_card(record.card_url)
        now = datetime.now(UTC)
        updated = record.model_copy(
            update={
                "card": self._remote.card_to_dict(card),
                "health": "ok",
                "last_seen": now,
            }
        )
        async with self._db.transaction() as conn:
            await conn.execute(
                "UPDATE agent_registry SET card = ?, health = ?, last_seen = ? WHERE id = ?",
                (
                    json.dumps(updated.card, ensure_ascii=False),
                    updated.health,
                    updated.last_seen.isoformat(),
                    agent_id,
                ),
            )
        return updated

    @staticmethod
    def agent_url(record: AgentRecord) -> str:
        interfaces = record.card.get("supportedInterfaces") or []
        if not interfaces:
            raise ValueError(f"agent card has no supported interfaces: {record.name}")
        return str(interfaces[0]["url"])

    @staticmethod
    def _row_to_record(row) -> AgentRecord:
        return AgentRecord(
            id=row["id"],
            name=row["name"],
            card_url=row["card_url"],
            card=json.loads(row["card"]),
            health=row["health"],
            last_seen=(
                datetime.fromisoformat(row["last_seen"]) if row["last_seen"] else None
            ),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
