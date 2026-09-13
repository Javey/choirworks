# 工作群协作 M8：群消息底座 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为工作群协作模式打下数据与上下文底座：事件日志支持会话级聚合、房间消息/成员/摘要表与投影、上下文分级投喂、调度器注入上下文包、消息 REST/SSE、Agent 产出自动入群时间线。旧有任务流程零回归。

**Architecture:** 所有房间消息写 `message.posted` 等新事件，投影到 `messages/room_members/room_summaries`；`core/room.py` 串行分配房间 seq；`core/context.py` 纯渲染分级上下文包，调度器在派发/续跑时注入；FastAPI 暴露消息游标接口与房间级 SSE。

**Tech Stack:** Python 3.12 / FastAPI / aiosqlite / Pydantic / pytest / ruff；测试用 `tests/support/fakes.py::FakeLLM` 与 `tests/conftest.py::echo_agent`。

**权威 spec:** `docs/superpowers/specs/2026-09-13-group-chat-collaboration-design.md`（§4–§6、§8–§9、M8）

---

## File Structure

| 文件 | 职责 | 动作 |
|---|---|---|
| `src/choirworks/store/db.py` | events 会话列迁移；messages/room_members/room_summaries 建表 | 修改 |
| `src/choirworks/store/event_store.py` | `Event.conversation_id`、`append(..., conversation_id=)`、`replay_conversation` | 修改 |
| `src/choirworks/core/events.py` | EventBus 按 task/room 双键分发 | 修改 |
| `src/choirworks/models/enums.py` | 4 个新事件类型 | 修改 |
| `src/choirworks/models/domain.py` | `RoomMessage/RoomMember/RoomSummary` | 修改 |
| `src/choirworks/store/projections.py` | 新事件投影 + 查询 + rebuild 清单 | 修改 |
| `src/choirworks/core/room.py` | `post_message`（seq 锁）、`post_agent_messages`、`artifact_text` | 新建 |
| `src/choirworks/core/context.py` | `ContextPackage`、`build_agent_context` | 新建 |
| `src/choirworks/core/summary.py` | `SummaryDraft`、`maybe_update_summary` | 新建 |
| `src/choirworks/core/dispatcher.py` | 派发/续跑注入上下文包 + `context_included` 审计 | 修改 |
| `src/choirworks/api/messages.py` | `GET/POST /v1/conversations/{id}/messages` | 新建 |
| `src/choirworks/api/sse.py` | `GET /v1/conversations/{id}/stream` | 修改 |
| `src/choirworks/api/schemas.py` | 消息请求/响应模型 | 修改 |
| `src/choirworks/api/app.py` | 注册 messages 路由 | 修改 |
| `src/choirworks/core/orchestrator.py` | 节点完成后发布 Agent 消息 | 修改 |

测试文件：`tests/unit/test_room_data.py`、`test_room_messages.py`、`test_agent_context.py`、`test_room_summary.py`、`tests/integration/test_dispatch_room_context.py`、`test_messages_api.py`、`test_room_flow.py`。

---

### Task 1: 房间数据层（事件会话聚合 + 三张表 + 投影 + 查询）

**Files:**
- Modify: `src/choirworks/models/enums.py`
- Modify: `src/choirworks/models/domain.py`
- Modify: `src/choirworks/store/db.py`
- Modify: `src/choirworks/store/event_store.py`
- Modify: `src/choirworks/core/events.py`
- Modify: `src/choirworks/store/projections.py`
- Test: `tests/unit/test_room_data.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/unit/test_room_data.py`：

```python
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
        assert [item.payload["message_id"] for item in replayed] == ["m1"]

        task_event = await store.append(
            "t1", EventType.NODE_INVALIDATED, {"node_id": "n1"}
        )
        assert task_event.conversation_id == "c1"
        assert len(await store.replay_conversation("c1")) == 2
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
            {"agent_name": "researcher", "agent_url": "http://r", "reason": "human_mention"},
            conversation_id="c1",
        )
        await store.append(
            None,
            EventType.ROOM_SUMMARY_UPDATED,
            {
                "conversation_id": "c1",
                "covers_seq": 2,
                "summary": {"goal": "调研 A", "decisions": [], "artifacts": [], "todos": [], "open_questions": []},
            },
            conversation_id="c1",
        )

        assert await projections.next_message_seq(db, "c1") == 2
        fetched = await projections.fetch_messages(db, "c1")
        assert [message.text for message in fetched] == ["请调研 A", "调研完成"]
        assert fetched[0].mentions == ["researcher"]
        assert fetched[0].delivered_at is not None
        assert [m.id for m in await projections.fetch_messages(db, "c1", after_seq=1)] == ["m2"]

        node_messages = await projections.fetch_messages_for_node(db, "p1:n1")
        assert [m.id for m in node_messages] == ["m2"]

        members = await projections.fetch_room_members(db, "c1")
        assert [member.agent_name for member in members] == ["researcher"]

        summary = await projections.fetch_room_summary(db, "c1")
        assert summary is not None and summary.covers_seq == 2
        assert summary.summary["goal"] == "调研 A"
    finally:
        await db.close()


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


async def projection_rows(db: Database) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = {}
    for table in ("messages", "room_members", "room_summaries"):
        cursor = await db.conn.execute(f"SELECT * FROM {table} ORDER BY 1")
        result[table] = [dict(row) for row in await cursor.fetchall()]
    return result


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
```

运行：

```bash
uv run pytest tests/unit/test_room_data.py -p no:warnings -q
```

预期：FAIL（`EventType.MESSAGE_POSTED` 不存在 / 表不存在）。

- [ ] **Step 2: 新增事件类型与领域模型**

`src/choirworks/models/enums.py` 在 `EventType` 末尾（`ERROR` 之前）加入：

```python
    MESSAGE_POSTED = "message.posted"
    MESSAGE_DELIVERED = "message.delivered"
    ROOM_PARTICIPANT_JOINED = "room.participant_joined"
    ROOM_SUMMARY_UPDATED = "room.summary_updated"
```

`src/choirworks/models/domain.py` 末尾加入：

```python
class RoomMessage(BaseModel):
    id: str
    conversation_id: str
    seq: int
    role: str
    sender: str | None = None
    text: str
    mentions: list[str] = Field(default_factory=list)
    quote_id: str | None = None
    task_id: str | None = None
    node_id: str | None = None
    intervention_id: str | None = None
    queued_for_node_id: str | None = None
    delivered_at: datetime | None = None
    created_at: datetime


class RoomMember(BaseModel):
    conversation_id: str
    agent_name: str
    agent_url: str
    reason: str | None = None
    joined_at: datetime


class RoomSummary(BaseModel):
    conversation_id: str
    covers_seq: int
    summary: dict[str, Any]
    updated_at: datetime
```

- [ ] **Step 3: 建表与迁移**

`src/choirworks/store/db.py`：把 `events` 表定义改为（`task_id` 可空、新增 `conversation_id`）：

```sql
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT,
  conversation_id TEXT,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq);
```

在 `SCHEMA` 末尾（`agent_registry` 之后）追加：

```sql
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
```

在 `_migrate` 开头加入（旧库重建 events 表，放开 `task_id` NOT NULL 并新增列；索引在重命名/drop 后创建，避免与旧索引同名冲突）：

```python
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
```

- [ ] **Step 4: EventStore 支持会话聚合**

`src/choirworks/store/event_store.py` 全文替换为：

```python
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
```

- [ ] **Step 5: EventBus 双键分发**

`src/choirworks/core/events.py`：`EventSubscription.__init__` 参数 `task_id` 改名 `key`，把两处 `self.task_id` 改为 `self.key`；`EventBus.subscribe(self, key: str)`、`unsubscribe` 用 `subscription.key`；`publish` 改为：

```python
    def publish(self, event: Event) -> None:
        keys: list[str] = []
        if event.task_id:
            keys.append(event.task_id)
        if event.conversation_id:
            keys.append(f"room:{event.conversation_id}")
        for key in keys:
            for subscription in list(self._subscriptions.get(key, ())):
                subscription.offer(event)
```

- [ ] **Step 6: 投影与查询**

`src/choirworks/store/projections.py`：

a) imports 的 domain 中加入 `RoomMember, RoomMessage, RoomSummary`。

b) `apply_event` 在 `ERROR` 分支前加入：

```python
    elif event_type is EventType.MESSAGE_POSTED:
        await conn.execute(
            "INSERT INTO messages"
            " (id, conversation_id, seq, role, sender, text, mentions, quote_id, task_id,"
            "  node_id, intervention_id, queued_for_node_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payload["message_id"],
                payload["conversation_id"],
                payload["seq"],
                payload["role"],
                payload.get("sender"),
                payload["text"],
                json.dumps(payload.get("mentions") or [], ensure_ascii=False),
                payload.get("quote_id"),
                payload.get("task_id") or event.task_id,
                payload.get("node_id"),
                payload.get("intervention_id"),
                payload.get("queued_for_node_id"),
                ts,
            ),
        )
    elif event_type is EventType.MESSAGE_DELIVERED:
        await conn.execute(
            "UPDATE messages SET delivered_at = ? WHERE id = ?",
            (ts, payload["message_id"]),
        )
    elif event_type is EventType.ROOM_PARTICIPANT_JOINED:
        await conn.execute(
            "INSERT INTO room_members (conversation_id, agent_name, agent_url, reason,"
            " joined_at) VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(conversation_id, agent_name) DO UPDATE SET"
            " agent_url = excluded.agent_url, reason = excluded.reason",
            (
                payload["conversation_id"],
                payload["agent_name"],
                payload["agent_url"],
                payload.get("reason"),
                ts,
            ),
        )
    elif event_type is EventType.ROOM_SUMMARY_UPDATED:
        await conn.execute(
            "INSERT INTO room_summaries (conversation_id, covers_seq, summary, updated_at)"
            " VALUES (?, ?, ?, ?) ON CONFLICT(conversation_id) DO UPDATE SET"
            " covers_seq = excluded.covers_seq, summary = excluded.summary,"
            " updated_at = excluded.updated_at",
            (
                payload["conversation_id"],
                payload["covers_seq"],
                json.dumps(payload["summary"], ensure_ascii=False),
                ts,
            ),
        )
```

c) `rebuild` 的 DELETE 清单加入 `"messages", "room_members", "room_summaries"`。

d) 行转换与查询函数（追加到文件末尾）：

```python
def _row_to_room_message(row: aiosqlite.Row) -> RoomMessage:
    return RoomMessage(
        id=row["id"],
        conversation_id=row["conversation_id"],
        seq=row["seq"],
        role=row["role"],
        sender=row["sender"],
        text=row["text"],
        mentions=json.loads(row["mentions"]),
        quote_id=row["quote_id"],
        task_id=row["task_id"],
        node_id=row["node_id"],
        intervention_id=row["intervention_id"],
        queued_for_node_id=row["queued_for_node_id"],
        delivered_at=_parse_dt(row["delivered_at"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_room_member(row: aiosqlite.Row) -> RoomMember:
    return RoomMember(
        conversation_id=row["conversation_id"],
        agent_name=row["agent_name"],
        agent_url=row["agent_url"],
        reason=row["reason"],
        joined_at=datetime.fromisoformat(row["joined_at"]),
    )


def _row_to_room_summary(row: aiosqlite.Row) -> RoomSummary:
    return RoomSummary(
        conversation_id=row["conversation_id"],
        covers_seq=row["covers_seq"],
        summary=json.loads(row["summary"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


async def next_message_seq(db: Any, conversation_id: str) -> int:
    cursor = await db.conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS seq FROM messages WHERE conversation_id = ?",
        (conversation_id,),
    )
    return int((await cursor.fetchone())["seq"])


async def fetch_messages(
    db: Any, conversation_id: str, after_seq: int = 0, limit: int = 200
) -> list[RoomMessage]:
    cursor = await db.conn.execute(
        "SELECT * FROM messages WHERE conversation_id = ? AND seq > ?"
        " ORDER BY seq LIMIT ?",
        (conversation_id, after_seq, limit),
    )
    return [_row_to_room_message(row) for row in await cursor.fetchall()]


async def fetch_message(db: Any, message_id: str) -> RoomMessage | None:
    cursor = await db.conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
    row = await cursor.fetchone()
    return _row_to_room_message(row) if row else None


async def fetch_messages_for_node(db: Any, node_id: str) -> list[RoomMessage]:
    cursor = await db.conn.execute(
        "SELECT * FROM messages WHERE node_id = ? ORDER BY seq", (node_id,)
    )
    return [_row_to_room_message(row) for row in await cursor.fetchall()]


async def fetch_room_members(db: Any, conversation_id: str) -> list[RoomMember]:
    cursor = await db.conn.execute(
        "SELECT * FROM room_members WHERE conversation_id = ? ORDER BY joined_at, agent_name",
        (conversation_id,),
    )
    return [_row_to_room_member(row) for row in await cursor.fetchall()]


async def fetch_room_summary(db: Any, conversation_id: str) -> RoomSummary | None:
    cursor = await db.conn.execute(
        "SELECT * FROM room_summaries WHERE conversation_id = ?", (conversation_id,)
    )
    row = await cursor.fetchone()
    return _row_to_room_summary(row) if row else None
```

- [ ] **Step 7: 运行测试至绿**

```bash
uv run pytest tests/unit/test_room_data.py -p no:warnings -q
uv run pytest -p no:warnings -q
uv run ruff check .
```

预期：新测试通过，旧测试全绿（128+），ruff 无错误。

- [ ] **Step 8: 提交**

```bash
git add src/choirworks/models/enums.py src/choirworks/models/domain.py \
  src/choirworks/store/db.py src/choirworks/store/event_store.py \
  src/choirworks/core/events.py src/choirworks/store/projections.py \
  tests/unit/test_room_data.py
git commit -m "feat: 群消息数据层（事件会话聚合、房间表与投影）"
```

---

### Task 2: `core/room.py` 消息写入助手（房间 seq 串行分配）

**Files:**
- Create: `src/choirworks/core/room.py`
- Test: `tests/unit/test_room_messages.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/unit/test_room_messages.py`：

```python
from choirworks.core.room import post_message
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_room(tmp_path):
    db = Database(tmp_path / "room.db")
    await db.initialize()
    return db, EventStore(db)


async def test_post_message_allocates_room_seq(tmp_path):
    db, store = await make_room(tmp_path)
    try:
        first = await post_message(
            db, store, conversation_id="c1", role="user", sender="CEO", text="第一问"
        )
        second = await post_message(
            db,
            store,
            conversation_id="c1",
            role="agent",
            sender="researcher",
            text="答复",
            mentions=["writer"],
            quote_id=first.id,
            node_id="p1:n1",
        )
        assert (first.seq, second.seq) == (1, 2)
        assert second.mentions == ["writer"]
        assert second.quote_id == first.id
        assert second.node_id == "p1:n1"
        assert await projections.next_message_seq(db, "c1") == 2

        other = await post_message(
            db, store, conversation_id="c2", role="user", sender="CEO", text="另一个群"
        )
        assert other.seq == 1
    finally:
        await db.close()
```

运行：`uv run pytest tests/unit/test_room_messages.py -p no:warnings -q` → FAIL（模块不存在）。

- [ ] **Step 2: 实现**

创建 `src/choirworks/core/room.py`：

```python
from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

from choirworks.models.domain import RoomMessage
from choirworks.models.enums import EventType
from choirworks.store import projections

_seq_locks: dict[str, asyncio.Lock] = {}


def _lock_for(conversation_id: str) -> asyncio.Lock:
    lock = _seq_locks.get(conversation_id)
    if lock is None:
        lock = asyncio.Lock()
        _seq_locks[conversation_id] = lock
    return lock


async def post_message(
    db: Any,
    events: Any,
    *,
    conversation_id: str,
    role: str,
    sender: str | None,
    text: str,
    mentions: list[str] | None = None,
    quote_id: str | None = None,
    task_id: str | None = None,
    node_id: str | None = None,
    intervention_id: str | None = None,
    queued_for_node_id: str | None = None,
) -> RoomMessage:
    async with _lock_for(conversation_id):
        seq = await projections.next_message_seq(db, conversation_id) + 1
        message_id = uuid4().hex
        await events.append(
            task_id,
            EventType.MESSAGE_POSTED,
            {
                "message_id": message_id,
                "conversation_id": conversation_id,
                "seq": seq,
                "role": role,
                "sender": sender,
                "text": text,
                "mentions": mentions or [],
                "quote_id": quote_id,
                "task_id": task_id,
                "node_id": node_id,
                "intervention_id": intervention_id,
                "queued_for_node_id": queued_for_node_id,
            },
            conversation_id=conversation_id,
        )
        message = await projections.fetch_message(db, message_id)
        assert message is not None
        return message
```

- [ ] **Step 3: 运行测试至绿**

```bash
uv run pytest tests/unit/test_room_messages.py -p no:warnings -q && uv run ruff check .
```

- [ ] **Step 4: 提交**

```bash
git add src/choirworks/core/room.py tests/unit/test_room_messages.py
git commit -m "feat: 房间消息写入助手（seq 锁与事件发布）"
```

---

### Task 3: 上下文分级投喂 `core/context.py`

**Files:**
- Create: `src/choirworks/core/context.py`
- Test: `tests/unit/test_agent_context.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/unit/test_agent_context.py`：

```python
from choirworks.core.context import build_agent_context
from choirworks.core.room import post_message
from choirworks.models.enums import EventType
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_room(tmp_path):
    db = Database(tmp_path / "ctx.db")
    await db.initialize()
    events = EventStore(db)
    await events.append(
        "t1",
        EventType.TASK_CREATED,
        {
            "request": "调研 A2A",
            "policy": None,
            "conversation_id": "c1",
            "conversation_title": "调研 A2A",
        },
    )
    return db, events


async def test_small_room_is_fully_included(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        first = await post_message(
            db, events, conversation_id="c1", role="user", sender="CEO", text="大家好"
        )
        second = await post_message(
            db, events, conversation_id="c1", role="user", sender="CEO", text="第二句"
        )
        package = await build_agent_context(
            db, "c1", "researcher", "请调研", budget=8000, recent_window=5
        )
        assert package.included_message_ids == [first.id, second.id]
        assert package.truncated is False
        assert "调研 A2A" in package.text
        assert "大家好" in package.text
        assert "请调研" in package.text
    finally:
        await db.close()


async def test_mention_beyond_window_and_quote_are_included(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        question = await post_message(
            db, events, conversation_id="c1", role="agent", sender="writer", text="谁能补充数据？"
        )
        mentioned = await post_message(
            db,
            events,
            conversation_id="c1",
            role="agent",
            sender="writer",
            text="@researcher 请补充数据",
            mentions=["researcher"],
        )
        for index in range(25):
            await post_message(
                db,
                events,
                conversation_id="c1",
                role="user",
                sender="CEO",
                text=f"闲聊 {index}",
            )
        package = await build_agent_context(
            db, "c1", "researcher", "补充数据", budget=8000, recent_window=5
        )
        assert mentioned.id in package.included_message_ids
        assert question.id in package.included_message_ids  # 引用我（writer 引用了 researcher 的历史）
    finally:
        await db.close()


async def test_budget_drops_recent_but_keeps_mention(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        mention = await post_message(
            db,
            events,
            conversation_id="c1",
            role="user",
            sender="CEO",
            text="@researcher 关键请求",
            mentions=["researcher"],
        )
        for index in range(30):
            await post_message(
                db,
                events,
                conversation_id="c1",
                role="user",
                sender="CEO",
                text=f"很长的闲聊内容 {'x' * 200} {index}",
            )
        package = await build_agent_context(
            db, "c1", "researcher", "处理关键请求", budget=200, recent_window=20
        )
        assert mention.id in package.included_message_ids
        assert len(package.included_message_ids) < 31
        assert package.truncated is True
    finally:
        await db.close()
```

运行：`uv run pytest tests/unit/test_agent_context.py -p no:warnings -q` → FAIL（模块不存在）。

- [ ] **Step 2: 实现**

创建 `src/choirworks/core/context.py`：

```python
from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from choirworks.models.domain import RoomMessage
from choirworks.store import projections

HEADER_RULES = "需要他人配合时 @姓名 并说明需求；完成后给出结论"


class ContextPackage(BaseModel):
    text: str
    included_message_ids: list[str] = Field(default_factory=list)
    truncated: bool = False


def _estimate(text: str) -> int:
    return max(1, len(text) // 4)


def _render_message(message: RoomMessage, quotes: dict[str, RoomMessage]) -> str:
    label = message.sender or message.role
    prefix = f"#{message.seq} [{label}]"
    if message.quote_id:
        quoted = quotes.get(message.quote_id)
        if quoted is not None:
            snippet = quoted.text[:40]
            prefix += f"（引用 #{quoted.seq} [{quoted.sender or quoted.role}] {snippet}）"
        else:
            prefix += f"（引用 {message.quote_id[:8]}）"
    return f"{prefix} {message.text}"


async def build_agent_context(
    db: Any,
    conversation_id: str,
    agent_name: str,
    instruction: str,
    *,
    budget: int = 8000,
    recent_window: int = 20,
) -> ContextPackage:
    conversation = await projections.fetch_conversation(db, conversation_id)
    title = conversation.title if conversation is not None else conversation_id
    members = await projections.fetch_room_members(db, conversation_id)
    summary = await projections.fetch_room_summary(db, conversation_id)
    messages = await projections.fetch_messages(db, conversation_id, limit=1000)
    quotes = {message.id: message for message in messages}

    relevant: list[RoomMessage] = []
    for message in messages:
        quoted = quotes.get(message.quote_id) if message.quote_id else None
        if (
            agent_name in message.mentions
            or message.sender == agent_name
            or (quoted is not None and quoted.sender == agent_name)
        ):
            relevant.append(message)
    recent = messages[-recent_window:] if recent_window > 0 else []

    header = (
        f"[系统] 群聊：{title} ｜ 成员：{', '.join(m.agent_name for m in members) or '（暂无）'}\n"
        f"[系统] 规则：{HEADER_RULES}"
    )
    instruction_line = f"[当前任务] {instruction}"
    used = _estimate(header) + _estimate(instruction_line)
    truncated = False

    included: dict[str, RoomMessage] = {}
    for group in (relevant, recent):
        for message in group:
            if message.id in included:
                continue
            cost = _estimate(_render_message(message, quotes))
            if used + cost > budget:
                truncated = True
                continue
            used += cost
            included[message.id] = message

    ordered = sorted(included.values(), key=lambda message: message.seq)
    relevant_ids = {message.id for message in relevant}
    sections: list[str] = [header]
    if summary is not None:
        compact = json.dumps(summary.summary, ensure_ascii=False)
        sections.append(f"[摘要·截至#{summary.covers_seq}] {compact}")
    relevant_lines = [
        _render_message(message, quotes)
        for message in ordered
        if message.id in relevant_ids
    ]
    if relevant_lines:
        sections.append("[与我相关]\n" + "\n".join(relevant_lines))
    recent_lines = [
        _render_message(message, quotes)
        for message in ordered
        if message.id not in relevant_ids
    ]
    if recent_lines:
        sections.append("[最近消息]\n" + "\n".join(recent_lines))
    sections.append(instruction_line)
    return ContextPackage(
        text="\n".join(sections),
        included_message_ids=[message.id for message in ordered],
        truncated=truncated,
    )
```

- [ ] **Step 3: 运行测试至绿**

```bash
uv run pytest tests/unit/test_agent_context.py -p no:warnings -q && uv run ruff check .
```

- [ ] **Step 4: 提交**

```bash
git add src/choirworks/core/context.py tests/unit/test_agent_context.py
git commit -m "feat: Agent 上下文分级投喂（相关性 + 预算裁剪）"
```

---

### Task 4: 增量摘要 `core/summary.py`

**Files:**
- Create: `src/choirworks/core/summary.py`
- Test: `tests/unit/test_room_summary.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/unit/test_room_summary.py`：

```python
from choirworks.core.room import post_message
from choirworks.core.summary import SummaryDraft, maybe_update_summary
from choirworks.models.enums import EventType
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore
from tests.support.fakes import FakeLLM


async def make_room(tmp_path):
    db = Database(tmp_path / "summary.db")
    await db.initialize()
    events = EventStore(db)
    await events.append(
        "t1",
        EventType.TASK_CREATED,
        {
            "request": "目标",
            "policy": None,
            "conversation_id": "c1",
            "conversation_title": "目标",
        },
    )
    return db, events


async def seed_messages(db, events, count: int, prefix: str = "消息") -> None:
    for index in range(count):
        await post_message(
            db,
            events,
            conversation_id="c1",
            role="user",
            sender="CEO",
            text=f"{prefix} {index + 1}",
        )


async def test_summary_triggers_after_twelve_messages(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        await seed_messages(db, events, 12)
        draft = SummaryDraft(goal="目标", decisions=["决策一"], todos=["待办一"])
        summary = await maybe_update_summary(db, events, FakeLLM([draft]), "c1")
        assert summary is not None
        assert summary.covers_seq == 12
        assert summary.summary["goal"] == "目标"
        assert summary.summary["decisions"] == ["决策一"]
        # 未达到触发阈值不再调用 LLM（FakeLLM 已无脚本，调用会抛错）
        assert await maybe_update_summary(db, events, FakeLLM([draft]), "c1") is None
    finally:
        await db.close()


async def test_summary_failure_keeps_previous(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        await seed_messages(db, events, 12)
        llm = FakeLLM([RuntimeError("llm down")])
        assert await maybe_update_summary(db, events, llm, "c1") is None
        assert await projections.fetch_room_summary(db, "c1") is None
        errors = [
            event
            for event in await events.replay("t1")
            if event.type is EventType.ERROR
        ]
        assert any("摘要" in (event.payload.get("message") or "") for event in errors)
    finally:
        await db.close()
```

运行：`uv run pytest tests/unit/test_room_summary.py -p no:warnings -q` → FAIL（模块不存在）。

- [ ] **Step 2: 实现**

创建 `src/choirworks/core/summary.py`：

```python
from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from choirworks.models.domain import RoomSummary
from choirworks.models.enums import EventType
from choirworks.store import projections

SUMMARY_TRIGGER = 12
SUMMARY_SYSTEM = "你是多 Agent 工作群的会议纪要员，将群聊记录增量压缩为结构化摘要。"


class SummaryDraft(BaseModel):
    goal: str = ""
    decisions: list[str] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    todos: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)


async def maybe_update_summary(
    db: Any,
    events: Any,
    llm: Any,
    conversation_id: str,
    *,
    trigger: int = SUMMARY_TRIGGER,
) -> RoomSummary | None:
    summary = await projections.fetch_room_summary(db, conversation_id)
    covers_seq = summary.covers_seq if summary is not None else 0
    messages = await projections.fetch_messages(
        db, conversation_id, after_seq=covers_seq
    )
    if len(messages) < trigger:
        return None
    previous = (
        json.dumps(summary.summary, ensure_ascii=False) if summary is not None else "（无）"
    )
    transcript = "\n".join(
        f"#{message.seq} [{message.sender or message.role}] {message.text}"
        for message in messages
    )
    try:
        draft = await llm.structured(
            system=SUMMARY_SYSTEM,
            user=f"已有摘要：{previous}\n\n新增消息：\n{transcript}",
            schema=SummaryDraft,
        )
    except Exception as exc:  # noqa: BLE001 - 摘要失败不阻塞消息流
        await events.append(
            None,
            EventType.ERROR,
            {"message": f"摘要更新失败：{exc}"},
            conversation_id=conversation_id,
        )
        return None
    await events.append(
        None,
        EventType.ROOM_SUMMARY_UPDATED,
        {
            "conversation_id": conversation_id,
            "covers_seq": messages[-1].seq,
            "summary": draft.model_dump(),
        },
        conversation_id=conversation_id,
    )
    return await projections.fetch_room_summary(db, conversation_id)
```

- [ ] **Step 3: 运行测试至绿**

```bash
uv run pytest tests/unit/test_room_summary.py -p no:warnings -q && uv run ruff check .
```

- [ ] **Step 4: 提交**

```bash
git add src/choirworks/core/summary.py tests/unit/test_room_summary.py
git commit -m "feat: 房间增量摘要（结构化、事件化、失败降级）"
```

---

### Task 5: 调度器注入上下文包

**Files:**
- Modify: `src/choirworks/core/dispatcher.py`
- Test: `tests/integration/test_dispatch_room_context.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/integration/test_dispatch_room_context.py`：

```python
from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.core.dispatcher import NodeDispatcher
from choirworks.core.room import post_message
from choirworks.core.tasks import TargetSpec, TaskService
from choirworks.models.enums import EventType, NodeStatus
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def test_dispatch_includes_room_context(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", echo_agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        assert created.conversation_id
        message = await post_message(
            db,
            events,
            conversation_id=created.conversation_id,
            role="user",
            sender="CEO",
            text="群里的关键上下文",
        )
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.COMPLETED
        assert node.output is not None
        text = node.output["artifacts"][0]["text"]
        assert text.startswith("echo:")
        assert "群里的关键上下文" in text

        replayed = await events.replay(created.task_id)
        intent = next(
            event
            for event in replayed
            if event.type is EventType.NODE_DISPATCH_INTENT
        )
        assert intent.payload["context_included"] == [message.id]
    finally:
        await remote.close()
        await db.close()


async def test_dispatch_without_room_messages_keeps_legacy_text(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", echo_agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.output is not None
        assert node.output["artifacts"][0]["text"] == "echo:hi"
    finally:
        await remote.close()
        await db.close()
```

运行：`uv run pytest tests/integration/test_dispatch_room_context.py -p no:warnings -q` → FAIL（无上下文注入）。

- [ ] **Step 2: 实现**

`src/choirworks/core/dispatcher.py`：

a) imports 增加：

```python
from choirworks.core.context import ContextPackage, build_agent_context
```

b) 新增方法：

```python
    async def _context_for(self, node: Node) -> ContextPackage | None:
        task = await projections.fetch_task(self._db, node.task_id)
        if task is None or task.conversation_id is None or not node.agent_name:
            return None
        messages = await projections.fetch_messages(
            self._db, task.conversation_id, limit=1
        )
        if not messages:
            return None
        instruction = str((node.input or {}).get("text", ""))
        return await build_agent_context(
            self._db, task.conversation_id, node.agent_name, instruction
        )
```

c) `dispatch_node` 中，在 append `NODE_DISPATCH_INTENT` 之前计算包并带上审计字段：

```python
        attempt = node.attempt + 1
        message_id = f"{task_id}:{node_id}:{attempt}"
        package = await self._context_for(node)
        intent_payload: dict[str, Any] = {
            "node_id": node_id,
            "message_id": message_id,
            "attempt": attempt,
        }
        if package is not None:
            intent_payload["context_included"] = package.included_message_ids
        await self._events.append(
            task_id,
            EventType.NODE_DISPATCH_INTENT,
            intent_payload,
        )
```

d) `dispatch_node` 发送文本处：

```python
            text = (
                package.text
                if package is not None
                else str((node.input or {}).get("text", ""))
            )
```

e) `continue_node` 中把 `text` 参数改为优先注入上下文：

```python
        package = await self._context_for(node)
        continue_text = package.text if package is not None else text
```

并把 `send_text(...)` 的第一个文本参数由 `text` 改为 `continue_text`。

- [ ] **Step 3: 运行测试至绿**

```bash
uv run pytest tests/integration/test_dispatch_room_context.py tests/integration/test_dispatcher.py -p no:warnings -q && uv run ruff check .
```

- [ ] **Step 4: 提交**

```bash
git add src/choirworks/core/dispatcher.py tests/integration/test_dispatch_room_context.py
git commit -m "feat: 调度器注入房间上下文包（含 context_included 审计）"
```

---

### Task 6: 消息 REST API 与房间 SSE

**Files:**
- Modify: `src/choirworks/api/schemas.py`
- Create: `src/choirworks/api/messages.py`
- Modify: `src/choirworks/api/sse.py`
- Modify: `src/choirworks/api/app.py`
- Test: `tests/integration/test_messages_api.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/integration/test_messages_api.py`：

```python
import asyncio
import contextlib
import json

import httpx
import pytest
import uvicorn

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.ports import free_port
from tests.support.fakes import FakeLLM


def make_llm() -> FakeLLM:
    plan = PlanDraft(
        rationale="room",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    return FakeLLM(structured_results=[plan])


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(
        store={"db_path": tmp_path / "messages.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=make_llm())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def make_conversation(app) -> str:
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    assert snapshot.task.conversation_id
    return snapshot.task.conversation_id


async def wait_completed(client, task_id: str) -> dict:
    snapshot = None
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_post_message_creates_task_and_timeline(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["seq"] == 1
    assert body["task_id"]
    await wait_completed(client, body["task_id"])

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    assert [message["role"] for message in timeline["messages"]] == ["user"]
    assert timeline["messages"][0]["text"] == "hi"
    assert timeline["last_seq"] == 1

    page = (
        await client.get(
            f"/v1/conversations/{conversation_id}/messages?since_seq=1"
        )
    ).json()
    assert page["messages"] == []


async def test_post_message_validates_input(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    assert (
        await client.post(
            f"/v1/conversations/{conversation_id}/messages",
            json={"text": "hi", "quote_id": "m1"},
        )
    ).status_code == 400
    assert (
        await client.post(
            f"/v1/conversations/{conversation_id}/messages",
            json={"text": "hi", "mentions": ["ghost"]},
        )
    ).status_code == 400
    assert (
        await client.post("/v1/conversations/missing/messages", json={"text": "hi"})
    ).status_code == 404


async def test_conversation_sse_replays_room_and_task_events(tmp_path, echo_agent):
    settings = Settings(
        store={"db_path": tmp_path / "room-sse.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=make_llm())
    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态
        await asyncio.sleep(0.02)
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", timeout=10.0
        ) as client:
            await client.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
            task_id = await app.state.task_service.create_pending_task("群聊测试")
            snapshot = await app.state.task_service.get_snapshot(task_id)
            conversation_id = snapshot.task.conversation_id
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
            )
            message_task_id = resp.json()["task_id"]

            events: list[dict] = []

            async def collect() -> None:
                async with client.stream(
                    "GET", f"/v1/conversations/{conversation_id}/stream?since_seq=0"
                ) as response:
                    assert response.status_code == 200
                    current: dict = {}
                    async for line in response.aiter_lines():
                        if line == "":
                            if current:
                                events.append(current)
                                if current.get("event") == "task.completed":
                                    return
                                current = {}
                            continue
                        if line.startswith("id: "):
                            current["id"] = int(line[4:])
                        elif line.startswith("event: "):
                            current["event"] = line[7:]
                        elif line.startswith("data: "):
                            current["data"] = json.loads(line[6:])

            await asyncio.wait_for(collect(), timeout=10.0)
            types = [event["event"] for event in events]
            assert "message.posted" in types
            assert "node.dispatched" in types
            assert "task.completed" in types
            assert message_task_id
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5)
```

运行：`uv run pytest tests/integration/test_messages_api.py -p no:warnings -q` → FAIL（路由 404）。

- [ ] **Step 2: 实现 schemas**

`src/choirworks/api/schemas.py` 末尾加入：

```python
class PostMessageIn(BaseModel):
    text: str = Field(min_length=1)
    mentions: list[str] = Field(default_factory=list)
    quote_id: str | None = None
    interrupt: bool = False


class PostMessageOut(BaseModel):
    message_id: str
    seq: int
    task_id: str | None = None


class RoomMessagesOut(BaseModel):
    messages: list[RoomMessage]
    members: list[RoomMember]
    summary: RoomSummary | None = None
    last_seq: int = 0
```

（imports 加入 `RoomMember, RoomMessage, RoomSummary`；`Field` 已导入，若无则补 `from pydantic import BaseModel, Field`。）

- [ ] **Step 3: 实现路由**

创建 `src/choirworks/api/messages.py`：

```python
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from choirworks.api.schemas import PostMessageIn, PostMessageOut, RoomMessagesOut
from choirworks.core import room
from choirworks.store import projections

router = APIRouter(tags=["messages"])


@router.get("/conversations/{conversation_id}/messages", response_model=RoomMessagesOut)
async def list_messages(
    conversation_id: str,
    request: Request,
    since_seq: int = 0,
    limit: int = 200,
) -> RoomMessagesOut:
    db = request.app.state.db
    if await projections.fetch_conversation(db, conversation_id) is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    messages = await projections.fetch_messages(
        db, conversation_id, after_seq=since_seq, limit=limit
    )
    members = await projections.fetch_room_members(db, conversation_id)
    summary = await projections.fetch_room_summary(db, conversation_id)
    last_seq = (
        messages[-1].seq
        if messages
        else await projections.next_message_seq(db, conversation_id)
    )
    return RoomMessagesOut(
        messages=messages, members=members, summary=summary, last_seq=last_seq
    )


@router.post(
    "/conversations/{conversation_id}/messages",
    status_code=201,
    response_model=PostMessageOut,
)
async def create_message(
    conversation_id: str, body: PostMessageIn, request: Request
) -> PostMessageOut:
    db = request.app.state.db
    events = request.app.state.event_store
    service = request.app.state.task_service
    registry = request.app.state.registry
    if await projections.fetch_conversation(db, conversation_id) is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    if body.quote_id is not None or body.interrupt:
        raise HTTPException(
            status_code=400, detail="引用与打断路由将在 M9 提供，当前请直接发送新消息"
        )
    for agent_name in body.mentions:
        if await registry.get_by_name(agent_name) is None:
            raise HTTPException(
                status_code=400, detail=f"agent not registered: {agent_name}"
            )
    task_id = await service.create_pending_task(
        body.text, conversation_id=conversation_id
    )
    message = await room.post_message(
        db,
        events,
        conversation_id=conversation_id,
        role="user",
        sender="CEO",
        text=body.text,
        mentions=body.mentions,
        task_id=task_id,
    )
    request.app.state.orchestrator.start(task_id)
    return PostMessageOut(message_id=message.id, seq=message.seq, task_id=task_id)
```

- [ ] **Step 4: 注册路由与 SSE**

`src/choirworks/api/app.py`：

a) import 增加 `from choirworks.api import messages as messages_routes`。
b) `app.include_router(messages_routes.router, prefix="/v1")`。

`src/choirworks/api/sse.py` 追加：

```python
@router.get("/conversations/{conversation_id}/stream")
async def conversation_events(
    conversation_id: str, request: Request, since_seq: int = 0
):
    from choirworks.store import projections

    if await projections.fetch_conversation(request.app.state.db, conversation_id) is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    store = request.app.state.event_store
    bus = request.app.state.event_bus
    last_event_id = request.headers.get("last-event-id")
    start = since_seq
    if last_event_id is not None and last_event_id.isdigit():
        start = max(start, int(last_event_id))
    key = f"room:{conversation_id}"

    async def stream():
        subscription = bus.subscribe(key)
        try:
            seen = start
            for event in await store.replay_conversation(conversation_id, start):
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
        finally:
            subscription.close()
            bus.unsubscribe(subscription)

    return EventSourceResponse(stream(), ping=15)
```

（`projections` 的局部 import 移到文件顶部与其他 import 合并，保持 ruff 通过。）

- [ ] **Step 5: 运行测试至绿**

```bash
uv run pytest tests/integration/test_messages_api.py -p no:warnings -q && uv run ruff check .
```

- [ ] **Step 6: 提交**

```bash
git add src/choirworks/api/schemas.py src/choirworks/api/messages.py \
  src/choirworks/api/sse.py src/choirworks/api/app.py \
  tests/integration/test_messages_api.py
git commit -m "feat: 群消息 REST 与房间 SSE"
```

---

### Task 7: Agent 产出自动入群时间线（端到端）

**Files:**
- Modify: `src/choirworks/core/room.py`
- Modify: `src/choirworks/core/orchestrator.py`
- Test: `tests/integration/test_room_flow.py`（新建）

- [ ] **Step 1: 写失败测试**

创建 `tests/integration/test_room_flow.py`：

```python
import asyncio

import httpx
import pytest

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


@pytest.fixture
async def api(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="room",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    settings = Settings(
        store={"db_path": tmp_path / "flow.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=[plan]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def wait_completed(client, task_id: str) -> dict:
    snapshot = None
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_agent_output_becomes_room_message(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    conversation_id = snapshot.task.conversation_id

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
    )
    await wait_completed(client, resp.json()["task_id"])

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    messages = timeline["messages"]
    assert [message["role"] for message in messages] == ["user", "agent"]
    assert messages[1]["sender"] == "echo"
    assert messages[1]["text"] == "echo:" + messages[1]["text"].split("echo:", 1)[1]
    assert messages[1]["node_id"]
    assert [message["seq"] for message in messages] == [1, 2]
```

运行：`uv run pytest tests/integration/test_room_flow.py -p no:warnings -q` → FAIL（只有 user 消息）。

- [ ] **Step 2: 实现**

`src/choirworks/core/room.py` 追加：

```python
def artifact_text(output: dict[str, Any] | None) -> str:
    if not output:
        return ""
    parts = [
        artifact.get("text", "")
        for artifact in output.get("artifacts", [])
        if isinstance(artifact, dict)
    ]
    return " ".join(part for part in parts if part).strip()


async def post_agent_messages(db: Any, events: Any, task: Any, nodes: list[Any]) -> None:
    from choirworks.models.enums import NodeStatus

    if task.conversation_id is None:
        return
    for node in nodes:
        if node.status is not NodeStatus.COMPLETED or not node.agent_name:
            continue
        existing = await projections.fetch_messages_for_node(db, node.id)
        if any(message.role == "agent" for message in existing):
            continue
        await post_message(
            db,
            events,
            conversation_id=task.conversation_id,
            role="agent",
            sender=node.agent_name,
            text=artifact_text(node.output) or "已完成",
            node_id=node.id,
            task_id=task.id,
        )
```

`src/choirworks/core/orchestrator.py`：

a) import 增加：`from choirworks.core.room import post_agent_messages`
b) `run` 循环中取得 `nodes` 后（`self._inflight = ...` 之后）加入：

```python
            await post_agent_messages(self._db, self._events, task, nodes)
```

c) 已验证`_task_service.create_checkpoint` 等逻辑不变；`NodeStatus` 已导入。

- [ ] **Step 3: 运行测试至绿（含全量回归）**

```bash
uv run pytest tests/integration/test_room_flow.py -p no:warnings -q
uv run pytest -p no:warnings -q
uv run ruff check .
```

预期：全部通过（旧 128 + 新增）。

- [ ] **Step 4: 提交**

```bash
git add src/choirworks/core/room.py src/choirworks/core/orchestrator.py \
  tests/integration/test_room_flow.py
git commit -m "feat: Agent 产出自动入群时间线（去重、幂等）"
```

---

### Task 8: M8 收尾验证

- [ ] **Step 1: 全量验证**

```bash
uv run pytest -p no:warnings -q
uv run ruff check .
cd frontend && npm test 2>&1 | tail -3
```

预期：后端全绿、ruff 通过、前端 25 测试保持通过（本里程碑未改前端）。

- [ ] **Step 2: 实机冒烟（可选但推荐）**

```bash
uv run choirworks-sim --port 18081 --db /tmp/opencode/m8.db --fresh
```

浏览器或 curl：创建任务得到 conversation_id 后，`POST /v1/conversations/{id}/messages`，确认消息时间线包含 user/agent 消息，`GET /v1/conversations/{id}/stream` 有事件流。

- [ ] **Step 3: 提交文档状态（如有）**

如实现过程中发现 spec 需要修正，更新 `docs/superpowers/specs/2026-09-13-group-chat-collaboration-design.md` 并提交：

```bash
git add docs/superpowers/specs/2026-09-13-group-chat-collaboration-design.md
git commit -m "docs: M8 实现回填"
```

---

## Self-Review 结果

- **Spec 覆盖**：M8 的 4 项交付（表/事件/迁移、API/SSE、ContextBuilder、SummaryBuilder 骨架、调度器接入）分别在 Task 1/6/3/4/5 落地；Agent 消息时间线（spec §9.2 流式持久化前提）在 Task 7 落地。M9 的路由/仲裁/排队明确不在本计划内。
- **占位符扫描**：无 TBD/TODO；每个代码步骤含完整实现。
- **类型一致性**：`post_message`/`build_agent_context`/`maybe_update_summary`/`ContextPackage`/`RoomMessage` 命名在全部任务中一致；`EventStore.append` 的 `conversation_id` 关键字参数与 Task 1 定义一致。
- **已知取舍**：`maybe_update_summary` 在 M8 无自动触发点（M9 由 RoomCoordinator 在消息流中调用），符合 spec「SummaryBuilder 骨架 + 单测」的 M8 范围。
