from __future__ import annotations

import aiosqlite

from choirworks.core.events import EventBus
from choirworks.models.enums import EventType
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_store(tmp_path) -> tuple[Database, EventStore]:
    db = Database(tmp_path / "room.db")
    await db.initialize()
    return db, EventStore(db, bus=EventBus())


async def seed_conversation(store: EventStore) -> None:
    await store.append(
        "t1",
        EventType.TASK_CREATED,
        {
            "request": "群目标",
            "policy": None,
            "conversation_id": "c1",
            "conversation_title": "群目标",
        },
    )


async def test_room_event_has_conversation_and_no_task(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        await seed_conversation(store)
        event = await store.append(
            None,
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "sender": "CEO",
                "text": "大家好",
                "mentions": [],
            },
            conversation_id="c1",
        )
        assert event.task_id is None
        assert event.conversation_id == "c1"
        replayed = await store.replay_conversation("c1")
        message_ids = [
            item.payload["message_id"]
            for item in replayed
            if item.type is EventType.MESSAGE_POSTED
        ]
        assert message_ids == ["m1"]

        task_event = await store.append(
            "t1", EventType.NODE_INVALIDATED, {"node_id": "n1"}
        )
        assert task_event.conversation_id == "c1"
        assert len(await store.replay_conversation("c1")) == 3
    finally:
        await db.close()


async def test_message_projection_and_queries(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        await seed_conversation(store)
        messages = [
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "sender": "CEO",
                "text": "请调研 A",
                "mentions": ["researcher"],
            },
            {
                "message_id": "m2",
                "conversation_id": "c1",
                "seq": 2,
                "role": "agent",
                "sender": "researcher",
                "text": "调研完成",
                "mentions": [],
                "node_id": "p1:n1",
            },
        ]
        for payload in messages:
            await store.append(
                None, EventType.MESSAGE_POSTED, payload, conversation_id="c1"
            )
        await store.append(
            None,
            EventType.MESSAGE_DELIVERED,
            {"message_id": "m1", "node_id": "p1:n1"},
            conversation_id="c1",
        )
        await store.append(
            None,
            EventType.ROOM_PARTICIPANT_JOINED,
            {
                "agent_name": "researcher",
                "agent_url": "http://r",
                "reason": "human_mention",
            },
            conversation_id="c1",
        )
        await store.append(
            None,
            EventType.ROOM_SUMMARY_UPDATED,
            {
                "conversation_id": "c1",
                "covers_seq": 2,
                "summary": {
                    "goal": "调研 A",
                    "decisions": [],
                    "artifacts": [],
                    "todos": [],
                    "open_questions": [],
                },
            },
            conversation_id="c1",
        )

        assert await projections.next_message_seq(db, "c1") == 2
        fetched = await projections.fetch_messages(db, "c1")
        assert [message.text for message in fetched] == ["请调研 A", "调研完成"]
        assert fetched[0].mentions == ["researcher"]
        assert fetched[0].delivered_at is not None
        assert [
            message.id
            for message in await projections.fetch_messages(db, "c1", after_seq=1)
        ] == ["m2"]

        node_messages = await projections.fetch_messages_for_node(db, "p1:n1")
        assert [message.id for message in node_messages] == ["m2"]

        members = await projections.fetch_room_members(db, "c1")
        assert [member.agent_name for member in members] == ["researcher"]

        summary = await projections.fetch_room_summary(db, "c1")
        assert summary is not None and summary.covers_seq == 2
        assert summary.summary["goal"] == "调研 A"
    finally:
        await db.close()


async def projection_rows(db: Database) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for table in ("messages", "room_members", "room_summaries"):
        cursor = await db.conn.execute(f"SELECT * FROM {table} ORDER BY 1")
        result[table] = [dict(row) for row in await cursor.fetchall()]
    return result


async def test_rebuild_restores_room_state(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        await seed_conversation(store)
        await store.append(
            None,
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "sender": "CEO",
                "text": "hi",
                "mentions": [],
            },
            conversation_id="c1",
        )
        before = await projection_rows(db)
        await projections.rebuild(db)
        after = await projection_rows(db)
        assert before == after
    finally:
        await db.close()


async def test_legacy_events_migration_allows_room_events(tmp_path):
    path = tmp_path / "legacy.db"
    async with aiosqlite.connect(path) as conn:
        await conn.executescript(
            "CREATE TABLE events ("
            " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
            " task_id TEXT NOT NULL,"
            " type TEXT NOT NULL,"
            " payload TEXT NOT NULL,"
            " created_at TEXT NOT NULL"
            ");"
            "INSERT INTO events (task_id, type, payload, created_at)"
            " VALUES ('t1', 'task.created', '{}', '2026-01-01T00:00:00+00:00');"
        )
        await conn.commit()
    db = Database(path)
    await db.initialize()
    try:
        store = EventStore(db)
        assert len(await store.replay_all()) == 1
        event = await store.append(
            None,
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "sender": "CEO",
                "text": "hi",
                "mentions": [],
            },
            conversation_id="c1",
        )
        assert event.task_id is None and event.conversation_id == "c1"
    finally:
        await db.close()
