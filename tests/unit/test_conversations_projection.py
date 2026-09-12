import aiosqlite

from agent_hub.models.enums import EventType, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore

OLD_SCHEMA = """
CREATE TABLE events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT NOT NULL,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE orchestration_tasks (
  id           TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  request      TEXT NOT NULL,
  policy       TEXT,
  plan_version INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);
"""


async def test_task_created_creates_conversation(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.TASK_CREATED,
            {
                "request": "第一问",
                "policy": None,
                "conversation_id": "c1",
                "conversation_title": "第一问",
            },
        )
        conversation = await projections.fetch_conversation(db, "c1")
        assert conversation is not None
        assert conversation.title == "第一问"
        task = await projections.fetch_task(db, "t1")
        assert task is not None
        assert task.conversation_id == "c1"
        assert await projections.fetch_task_ids_for_conversation(db, "c1") == ["t1"]
    finally:
        await db.close()


async def test_follow_up_reuses_conversation_and_summaries(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.TASK_CREATED,
            {
                "request": "第一问",
                "policy": None,
                "conversation_id": "c1",
                "conversation_title": "第一问",
            },
        )
        await store.append("t1", EventType.TASK_COMPLETED, {})
        await store.append(
            "t2",
            EventType.TASK_CREATED,
            {"request": "追问", "policy": None, "conversation_id": "c1"},
        )
        await store.append(
            "t2",
            EventType.TASK_STATE_CHANGED,
            {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
        )

        conversation = await projections.fetch_conversation(db, "c1")
        assert conversation is not None and conversation.title == "第一问"
        assert await projections.fetch_task_ids_for_conversation(db, "c1") == ["t1", "t2"]

        summaries = await projections.fetch_conversation_summaries(db)
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.id == "c1"
        assert summary.task_count == 2
        assert summary.last_status is TaskStatus.RUNNING
        assert summary.updated_at >= summary.created_at
    finally:
        await db.close()


async def test_rebuild_restores_conversations(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.TASK_CREATED,
            {
                "request": "x",
                "policy": None,
                "conversation_id": "c1",
                "conversation_title": "x",
            },
        )
        await projections.rebuild(db)
        conversation = await projections.fetch_conversation(db, "c1")
        assert conversation is not None
        task = await projections.fetch_task(db, "t1")
        assert task is not None and task.conversation_id == "c1"
    finally:
        await db.close()


async def test_migration_adds_conversation_column_to_old_db(tmp_path):
    path = tmp_path / "old.db"
    old = await aiosqlite.connect(path)
    await old.executescript(OLD_SCHEMA)
    await old.execute(
        "INSERT INTO orchestration_tasks VALUES ('t0', 'completed', '旧任务', NULL, 1,"
        " '2026-09-01T00:00:00+00:00', '2026-09-01T00:00:00+00:00')"
    )
    await old.commit()
    await old.close()

    db = Database(path)
    await db.initialize()
    try:
        task = await projections.fetch_task(db, "t0")
        assert task is not None and task.conversation_id is None
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.TASK_CREATED,
            {
                "request": "新任务",
                "policy": None,
                "conversation_id": "c1",
                "conversation_title": "新任务",
            },
        )
        assert await projections.fetch_task_ids_for_conversation(db, "c1") == ["t1"]
        assert await projections.fetch_conversation(db, "c1") is not None
    finally:
        await db.close()
