# M12 Room Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 ChoirWorks 的「群/房间」以 A2A Room Extension v1 暴露：房间是一个合成 A2A Task（`id`/`contextId` = `conversation_id`），房间消息是合法 A2A Message，任务类事件不进房间流，未感知扩展的 client 也能正常读取。

**Architecture:** 纯投影 facade，零内核改动（`core/room.py`、`core/coordinator.py`、projections 只读复用）。读侧在 `a2a/mapping.py` 增加 `room_to_task` / `room_message_from_event` / `RoomStreamMapper` / `room_send_options`；写侧在 `a2a/server.py` 的 `HubA2AHandler` 增加房间回退查找、房间订阅流与房间发送参数解析；AgentCard 声明扩展 URI；规范文档落 `docs/extensions/room-v1.md`。

**Tech Stack:** Python 3.12 / FastAPI / a2a-sdk 1.1.2（v1.0 proto 类型）/ protobuf Struct / pytest + httpx + 真 uvicorn（`tests/support/hubs.py`）。

**Spec:** `docs/superpowers/specs/2026-09-13-a2a-facade-design.md` §6（Room Extension）、§7（错误）、§9 M12、§10（测试）。

---

## 文件结构

| 文件 | 职责 | 动作 |
|---|---|---|
| `src/choirworks/a2a/card.py` | AgentCard 声明 Room 扩展 | 修改 |
| `src/choirworks/a2a/mapping.py` | 房间 Task/Message/事件/发送参数映射 | 修改 |
| `src/choirworks/a2a/server.py` | `HubA2AHandler` 房间回退与流式 | 修改 |
| `src/choirworks/store/event_store.py` | `latest_conversation_seq` | 修改 |
| `tests/unit/test_a2a_mapping_room.py` | 映射单测 | 新建 |
| `tests/unit/test_event_store.py` | 会话最新事件 seq 单测 | 修改 |
| `tests/integration/test_a2a_room.py` | 房间 Get/Send/Subscribe 集成 | 新建 |
| `tests/integration/test_a2a_card.py` | 扩展声明断言 | 修改 |
| `docs/extensions/room-v1.md` | Room Extension v1 规范 | 新建 |
| `README.md` | A2A 章节补扩展说明 | 修改 |
| `docs/superpowers/specs/2026-09-13-a2a-facade-design.md` | M12 实施记录 | 修改 |

约定：代码不加注释；ruff E/F/I/UP/B/ASYNC、line-length 100；测试命令均在仓库根执行。

---

### Task 1: AgentCard 声明 Room Extension

**Files:**
- Modify: `src/choirworks/a2a/card.py`
- Test: `tests/integration/test_a2a_card.py`

- [ ] **Step 1: 写失败测试**

在 `tests/integration/test_a2a_card.py` 顶部加 import，并在 `test_agent_card_served` 末尾追加断言：

```python
from choirworks.a2a.mapping import A2A_ROOM_URI
```

```python
    extensions = card["capabilities"]["extensions"]
    assert extensions[0]["uri"] == A2A_ROOM_URI
    assert extensions[0]["required"] is False
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_card.py -q`
Expected: FAIL（`KeyError: 'extensions'`）

- [ ] **Step 3: 实现扩展声明**

`src/choirworks/a2a/card.py` 改为：

```python
from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentSkill,
)

from choirworks.a2a.mapping import A2A_ROOM_URI

_ROOM_DESCRIPTION = (
    "Conversations as long-lived A2A tasks; room messages as A2A Messages"
)


def build_agent_card(public_url: str) -> AgentCard:
    base = public_url.rstrip("/")
    return AgentCard(
        name="ChoirWorks",
        description="多 Agent 协作工作群（A2A facade）",
        version="0.1.0",
        capabilities=AgentCapabilities(
            streaming=True,
            extensions=[
                AgentExtension(
                    uri=A2A_ROOM_URI,
                    description=_ROOM_DESCRIPTION,
                    required=False,
                )
            ],
        ),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="orchestrate",
                name="多 Agent 编排",
                description="规划、调度并协调多个 A2A agent 完成复杂任务",
                tags=["orchestration"],
            )
        ],
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                url=f"{base}/v1/a2a",
                protocol_version="1.0",
            )
        ],
    )
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_card.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add src/choirworks/a2a/card.py tests/integration/test_a2a_card.py
git commit -m "feat(a2a): AgentCard 声明 Room 扩展（M12）"
```

---

### Task 2: 房间读侧映射基元

**Files:**
- Modify: `src/choirworks/a2a/mapping.py`
- Test: `tests/unit/test_a2a_mapping_room.py`（新建）

- [ ] **Step 1: 写失败测试**

新建 `tests/unit/test_a2a_mapping_room.py`：

```python
from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    room_message_from_event,
    room_to_task,
)
from choirworks.models.domain import (
    Conversation,
    RoomMember,
    RoomMessage,
    RoomSummary,
)
from choirworks.models.enums import EventType
from choirworks.store.event_store import Event

_NOW = datetime(2026, 9, 14, tzinfo=UTC)


def _conversation() -> Conversation:
    return Conversation(id="c1", title="测试群", created_at=_NOW)


def _message(
    seq: int = 1,
    role: str = "user",
    sender: str = "CEO",
    text: str = "大家好",
    **overrides,
) -> RoomMessage:
    data = {
        "id": f"msg-{seq}",
        "conversation_id": "c1",
        "seq": seq,
        "role": role,
        "sender": sender,
        "text": text,
        "mentions": ["echo"],
        "quote_id": None,
        "task_id": None,
        "node_id": None,
        "intervention_id": None,
        "queued_for_node_id": None,
        "created_at": _NOW,
    }
    data.update(overrides)
    return RoomMessage(**data)


def _member() -> RoomMember:
    return RoomMember(
        conversation_id="c1",
        agent_name="echo",
        agent_url="http://agent",
        reason="human_mention",
        joined_at=_NOW,
    )


def _summary() -> RoomSummary:
    return RoomSummary(
        conversation_id="c1",
        covers_seq=1,
        summary={"topics": ["问候"]},
        updated_at=_NOW,
    )


def test_room_to_task_maps_metadata_and_history():
    room = room_to_task(
        _conversation(), [_message()], [_member()], _summary(), running=False
    )
    assert room.id == "c1"
    assert room.context_id == "c1"
    assert room.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "room"
    assert fields["title"].string_value == "测试群"
    assert fields["message_count"].number_value == 1
    assert fields["last_seq"].number_value == 1
    member = fields["members"].list_value.values[0].struct_value.fields
    assert member["agent_name"].string_value == "echo"
    assert member["agent_url"].string_value == "http://agent"
    assert member["reason"].string_value == "human_mention"
    summary = fields["summary"].struct_value.fields
    assert summary["covers_seq"].number_value == 1
    assert summary["content"].struct_value.fields["topics"].list_value.values[
        0
    ].string_value == "问候"
    assert room.history[0].message_id == "msg-1"
    assert room.history[0].role is Role.ROLE_USER
    assert room.history[0].parts[0].text == "大家好"
    assert room.history[0].extensions == [A2A_ROOM_URI]


def test_room_to_task_working_without_messages_or_summary():
    room = room_to_task(_conversation(), [], [], None, running=True)
    assert room.status.state is TaskState.TASK_STATE_WORKING
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["message_count"].number_value == 0
    assert fields["last_seq"].number_value == 0
    assert not fields["members"].list_value.values
    assert not fields["summary"].struct_value.fields
    assert not room.history


def test_room_message_from_event_rebuilds_message():
    event = Event(
        seq=7,
        task_id="t1",
        conversation_id="c1",
        type=EventType.MESSAGE_POSTED,
        payload={
            "message_id": "msg-9",
            "conversation_id": "c1",
            "seq": 3,
            "role": "assistant",
            "sender": "assistant",
            "text": "已派发 @echo",
            "mentions": ["echo"],
            "quote_id": "msg-1",
            "task_id": "t1",
            "node_id": "p1:n1",
            "queued_for_node_id": None,
        },
        created_at=_NOW,
    )
    message = room_message_from_event(event)
    assert message.id == "msg-9"
    assert message.conversation_id == "c1"
    assert message.seq == 3
    assert message.role == "assistant"
    assert message.sender == "assistant"
    assert message.text == "已派发 @echo"
    assert message.mentions == ["echo"]
    assert message.quote_id == "msg-1"
    assert message.task_id == "t1"
    assert message.node_id == "p1:n1"
    assert message.created_at == _NOW
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: FAIL（`ImportError: cannot import name 'room_to_task'`）

- [ ] **Step 3: 实现映射函数**

在 `src/choirworks/a2a/mapping.py` 顶部 import 改为（加 `Conversation`/`RoomMember`/`RoomMessage`/`RoomSummary`）：

```python
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import (
    Conversation,
    Node,
    RoomMember,
    RoomMessage,
    RoomSummary,
)
```

在 `room_message_to_a2a` 之后插入：

```python
def room_message_from_event(event: Event) -> RoomMessage:
    payload = event.payload
    return RoomMessage(
        id=payload["message_id"],
        conversation_id=payload.get("conversation_id") or event.conversation_id or "",
        seq=payload["seq"],
        role=payload["role"],
        sender=payload.get("sender"),
        text=payload["text"],
        mentions=payload.get("mentions") or [],
        quote_id=payload.get("quote_id"),
        task_id=payload.get("task_id") or event.task_id,
        node_id=payload.get("node_id"),
        intervention_id=payload.get("intervention_id"),
        queued_for_node_id=payload.get("queued_for_node_id"),
        created_at=event.created_at,
    )


def room_to_task(
    conversation: Conversation,
    messages: list[RoomMessage],
    members: list[RoomMember],
    summary: RoomSummary | None,
    *,
    running: bool,
) -> Task:
    summary_payload: dict[str, Any] = {}
    if summary is not None:
        summary_payload = {
            "covers_seq": summary.covers_seq,
            "updated_at": summary.updated_at.isoformat(),
            "content": summary.summary,
        }
    metadata = {
        A2A_ROOM_URI: {
            "kind": "room",
            "title": conversation.title,
            "members": [
                {
                    "agent_name": member.agent_name,
                    "agent_url": member.agent_url,
                    "reason": member.reason,
                    "joined_at": member.joined_at.isoformat(),
                }
                for member in members
            ],
            "summary": summary_payload,
            "message_count": len(messages),
            "last_seq": messages[-1].seq if messages else 0,
        }
    }
    state = (
        TaskState.TASK_STATE_WORKING
        if running
        else TaskState.TASK_STATE_INPUT_REQUIRED
    )
    return Task(
        id=conversation.id,
        context_id=conversation.id,
        status=A2ATaskStatus(state=state),
        history=[room_message_to_a2a(message) for message in messages],
        metadata=struct_value(metadata),
    )
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: PASS（3 passed）

- [ ] **Step 5: 提交**

```bash
git add src/choirworks/a2a/mapping.py tests/unit/test_a2a_mapping_room.py
git commit -m "feat(a2a): 房间 Task/消息读侧映射（M12）"
```

---

### Task 3: RoomStreamMapper

**Files:**
- Modify: `src/choirworks/a2a/mapping.py`
- Test: `tests/unit/test_a2a_mapping_room.py`

- [ ] **Step 1: 写失败测试**

在 `tests/unit/test_a2a_mapping_room.py` 的 import 中加入 `RoomStreamMapper`（与 `room_to_task` 同段），并追加：

```python
def _event(event_type: EventType, payload: dict) -> Event:
    return Event(
        seq=1,
        task_id=None,
        conversation_id="c1",
        type=event_type,
        payload=payload,
        created_at=_NOW,
    )


def test_room_stream_mapper_maps_message_posted():
    mapper = RoomStreamMapper("c1", running=False)
    responses = mapper.map_event(
        _event(
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "assistant",
                "sender": "assistant",
                "text": "hi",
                "mentions": [],
                "task_id": None,
            },
        )
    )
    assert len(responses) == 1
    assert responses[0].WhichOneof("payload") == "message"
    message = responses[0].message
    assert message.message_id == "m1"
    assert message.context_id == "c1"
    assert message.task_id == "c1"
    assert message.role is Role.ROLE_AGENT
    assert message.parts[0].text == "hi"
    assert message.extensions == [A2A_ROOM_URI]
    fields = message.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"


def test_room_stream_mapper_maps_status_events():
    mapper = RoomStreamMapper("c1", running=True)
    delivered = mapper.map_event(
        _event(
            EventType.MESSAGE_DELIVERED,
            {"message_id": "m1", "node_id": "p1:n1"},
        )
    )[0]
    assert delivered.WhichOneof("payload") == "status_update"
    assert delivered.status_update.task_id == "c1"
    assert delivered.status_update.context_id == "c1"
    assert delivered.status_update.status.state is TaskState.TASK_STATE_WORKING
    fields = delivered.status_update.metadata.fields
    assert fields["kind"].string_value == "message.delivered"
    assert fields["message_id"].string_value == "m1"
    assert fields["node_id"].string_value == "p1:n1"

    joined = mapper.map_event(
        _event(
            EventType.ROOM_PARTICIPANT_JOINED,
            {
                "agent_name": "echo",
                "agent_url": "http://agent",
                "reason": "human_mention",
            },
        )
    )[0]
    fields = joined.status_update.metadata.fields
    assert fields["kind"].string_value == "room.participant_joined"
    assert fields["agent_name"].string_value == "echo"

    summary = mapper.map_event(
        _event(
            EventType.ROOM_SUMMARY_UPDATED,
            {"covers_seq": 5, "summary": {"topics": ["x"]}},
        )
    )[0]
    fields = summary.status_update.metadata.fields
    assert fields["kind"].string_value == "room.summary_updated"
    assert fields["covers_seq"].number_value == 5
    assert (
        fields["summary"].struct_value.fields["topics"].list_value.values[
            0
        ].string_value
        == "x"
    )


def test_room_stream_mapper_ignores_non_room_events():
    mapper = RoomStreamMapper("c1", running=False)
    assert mapper.map_event(_event(EventType.CONVERSATION_CREATED, {})) == []
    assert mapper.map_event(_event(EventType.TASK_COMPLETED, {})) == []
    assert mapper.map_event(_event(EventType.NODE_STATE_CHANGED, {})) == []
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: FAIL（`ImportError: cannot import name 'RoomStreamMapper'`）

- [ ] **Step 3: 实现 RoomStreamMapper**

在 `src/choirworks/a2a/mapping.py` 的 `TaskStreamMapper` 类之前插入：

```python
class RoomStreamMapper:
    def __init__(self, conversation_id: str, *, running: bool):
        self._conversation_id = conversation_id
        self._running = running

    def _status_update(self, metadata: dict[str, Any]) -> StreamResponse:
        state = (
            TaskState.TASK_STATE_WORKING
            if self._running
            else TaskState.TASK_STATE_INPUT_REQUIRED
        )
        return StreamResponse(
            status_update=TaskStatusUpdateEvent(
                task_id=self._conversation_id,
                context_id=self._conversation_id,
                status=A2ATaskStatus(state=state),
                metadata=struct_value(metadata),
            )
        )

    def map_event(self, event: Event) -> list[StreamResponse]:
        if event.type is EventType.MESSAGE_POSTED:
            message = room_message_from_event(event)
            return [StreamResponse(message=room_message_to_a2a(message))]
        if event.type is EventType.MESSAGE_DELIVERED:
            return [
                self._status_update(
                    {
                        "kind": "message.delivered",
                        "message_id": event.payload.get("message_id"),
                        "node_id": event.payload.get("node_id"),
                    }
                )
            ]
        if event.type is EventType.ROOM_PARTICIPANT_JOINED:
            return [
                self._status_update(
                    {
                        "kind": "room.participant_joined",
                        "agent_name": event.payload.get("agent_name"),
                        "agent_url": event.payload.get("agent_url"),
                        "reason": event.payload.get("reason"),
                    }
                )
            ]
        if event.type is EventType.ROOM_SUMMARY_UPDATED:
            return [
                self._status_update(
                    {
                        "kind": "room.summary_updated",
                        "covers_seq": event.payload.get("covers_seq"),
                        "summary": event.payload.get("summary") or {},
                    }
                )
            ]
        return []
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: PASS（6 passed）

- [ ] **Step 5: 提交**

```bash
git add src/choirworks/a2a/mapping.py tests/unit/test_a2a_mapping_room.py
git commit -m "feat(a2a): RoomStreamMapper 事件映射（M12）"
```

---

### Task 4: GetTask 房间回退

**Files:**
- Modify: `src/choirworks/store/event_store.py`
- Modify: `src/choirworks/a2a/server.py`
- Test: `tests/unit/test_event_store.py`、`tests/integration/test_a2a_room.py`（新建）

- [ ] **Step 1: 写失败单测（latest_conversation_seq）**

在 `tests/unit/test_event_store.py` 末尾追加：

```python
async def test_latest_conversation_seq(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        await store.append(
            None,
            EventType.CONVERSATION_CREATED,
            {"conversation_id": "c1", "title": "群"},
            conversation_id="c1",
        )
        posted = await store.append(
            None,
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "text": "hi",
            },
            conversation_id="c1",
        )
        await store.append("t1", EventType.TASK_COMPLETED, {})
        assert await store.latest_conversation_seq("c1") == posted.seq
        assert await store.latest_conversation_seq("missing") == 0
    finally:
        await db.close()
```

- [ ] **Step 2: 运行单测确认失败**

Run: `uv run pytest tests/unit/test_event_store.py::test_latest_conversation_seq -q`
Expected: FAIL（`AttributeError: 'EventStore' object has no attribute 'latest_conversation_seq'`）

- [ ] **Step 3: 实现 latest_conversation_seq**

在 `src/choirworks/store/event_store.py` 的 `latest_seq` 之后插入：

```python
    async def latest_conversation_seq(self, conversation_id: str) -> int:
        cursor = await self._db.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS seq FROM events"
            " WHERE conversation_id = ?",
            (conversation_id,),
        )
        return int((await cursor.fetchone())["seq"])
```

- [ ] **Step 4: 写失败集成测试（GetTask 房间）**

新建 `tests/integration/test_a2a_room.py`：

```python
import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import GetTaskRequest, TaskState

from choirworks.a2a.mapping import A2A_ROOM_URI
from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.core.room import post_message
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.fakes import FakeLLM


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1",
                name=agent_name,
                agent_name=agent_name,
                input={"text": "问题"},
            )
        ],
    )


def _settings(tmp_path, db_name: str, *, port: int | None = None) -> Settings:
    public = f"http://127.0.0.1:{port}" if port is not None else "http://test"
    return Settings(
        store={"db_path": tmp_path / db_name},
        a2a={"public_url": public},
        scheduler={"retry_backoff_seconds": 0.0},
    )


async def _connect(app):
    transport = httpx.ASGITransport(app=app)
    http = httpx.AsyncClient(transport=transport, base_url="http://test")
    card = await A2ACardResolver(
        httpx_client=http, base_url="http://test"
    ).get_agent_card()
    client = await create_client(
        agent=card,
        client_config=ClientConfig(streaming=False, httpx_client=http),
    )
    return http, client


async def _new_room(http, title: str = "测试群") -> str:
    created = (await http.post("/v1/conversations", json={"title": title})).json()
    return created["conversation_id"]


async def _post_room_message(app, conversation_id: str, text: str):
    return await post_message(
        app.state.db,
        app.state.event_store,
        conversation_id=conversation_id,
        role="user",
        sender="CEO",
        text=text,
    )


async def _wait_for_active_node(http, task_id: str) -> None:
    for _ in range(200):
        nodes = (await http.get(f"/v1/tasks/{task_id}")).json()["nodes"]
        if any(node["status"] in {"dispatched", "working"} for node in nodes):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("no active node appeared")


@pytest.fixture
async def hub_room(tmp_path, echo_agent):
    app = create_app(_settings(tmp_path, "room.db"), llm=FakeLLM(structured_results=[_plan("echo")] * 4))
    async with app.router.lifespan_context(app):
        http, client = await _connect(app)
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        yield app, http, client
        await client.close()
        await http.aclose()


async def test_get_room_task_maps_history_members(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)
    await _post_room_message(app, conversation_id, "大家早上好")
    await app.state.coordinator.join_new_members(conversation_id, ["echo"])

    room = await client.get_task(GetTaskRequest(id=conversation_id))

    assert room.id == conversation_id
    assert room.context_id == conversation_id
    assert room.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert [message.parts[0].text for message in room.history] == [
        "大家早上好",
        "已将 @echo 加入群聊",
    ]
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "room"
    assert fields["title"].string_value == "测试群"
    assert fields["message_count"].number_value == 2
    member = fields["members"].list_value.values[0].struct_value.fields
    assert member["agent_name"].string_value == "echo"


async def test_get_room_task_working_while_task_runs(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        app = create_app(_settings(tmp_path, "room_working.db"))
        async with app.router.lifespan_context(app):
            http, client = await _connect(app)
            try:
                await http.post(
                    "/v1/agents", json={"name": "slow", "card_url": slow.url}
                )
                conversation_id = await _new_room(http, "运行群")
                posted = (
                    await http.post(
                        f"/v1/conversations/{conversation_id}/messages",
                        json={"text": "@slow 开始", "mentions": ["slow"]},
                    )
                ).json()
                await _wait_for_active_node(http, posted["task_id"])

                room = await client.get_task(GetTaskRequest(id=conversation_id))
            finally:
                await client.close()
                await http.aclose()
    finally:
        await slow.stop()

    assert room.status.state is TaskState.TASK_STATE_WORKING
```

- [ ] **Step 5: 运行集成测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_room.py -q`
Expected: FAIL（GetTask 对 conversation id 抛 TaskNotFoundError）

- [ ] **Step 6: 实现 server 房间回退**

`src/choirworks/a2a/server.py`：

import 段调整：

```python
from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    TaskStreamMapper,
    room_message_to_a2a,
    room_to_task,
    snapshot_to_task,
)
```

```python
from choirworks.models.enums import (
    TERMINAL_TASK_STATUSES,
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)
```

在 `_a2a_task` 之后插入：

```python
    def _note_extensions(self, context: ServerCallContext) -> None:
        if A2A_ROOM_URI in context.requested_extensions:
            logger.debug("A2A room extension activated for request")

    async def _room_snapshot(self, conversation_id: str) -> tuple[Task, bool]:
        db = self._app.state.db
        conversation = await projections.fetch_conversation(db, conversation_id)
        if conversation is None:
            raise TaskNotFound(conversation_id)
        messages = await projections.fetch_messages(db, conversation_id, limit=1000)
        members = await projections.fetch_room_members(db, conversation_id)
        summary = await projections.fetch_room_summary(db, conversation_id)
        running = False
        task_ids = await projections.fetch_task_ids_for_conversation(
            db, conversation_id
        )
        for task_id in task_ids:
            task = await projections.fetch_task(db, task_id)
            if task is not None and task.status not in TERMINAL_TASK_STATUSES:
                running = True
                break
        room = room_to_task(
            conversation, messages, members, summary, running=running
        )
        return room, running

    async def _room_task(self, conversation_id: str) -> Task:
        room, _ = await self._room_snapshot(conversation_id)
        return room
```

`on_get_task` 改为：

```python
    async def on_get_task(
        self, params: GetTaskRequest, context: ServerCallContext
    ) -> Task | None:
        self._note_extensions(context)
        try:
            return await self._a2a_task(params.id)
        except TaskNotFound:
            try:
                return await self._room_task(params.id)
            except TaskNotFound:
                return None
```

`on_message_send` 改为（本 Task 只加协商日志）：

```python
    async def on_message_send(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> Task | Message:
        self._note_extensions(context)
        return await self._submit(params)
```

- [ ] **Step 7: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_event_store.py tests/integration/test_a2a_room.py -q`
Expected: PASS

- [ ] **Step 8: 提交**

```bash
git add src/choirworks/store/event_store.py src/choirworks/a2a/server.py \
  tests/unit/test_event_store.py tests/integration/test_a2a_room.py
git commit -m "feat(a2a): GetTask 房间回退与扩展协商（M12）"
```

---

### Task 5: SendMessage 房间发送（Task/Message 两分支）

**Files:**
- Modify: `src/choirworks/a2a/mapping.py`
- Modify: `src/choirworks/a2a/server.py`
- Test: `tests/unit/test_a2a_mapping_room.py`、`tests/integration/test_a2a_room.py`

- [ ] **Step 1: 写失败单测（room_send_options）**

`tests/unit/test_a2a_mapping_room.py` import 段加 `room_send_options`、`struct_value`，以及 `Message`/`Part`（`from a2a.types import Message, Part, Role, TaskState`）。追加：

```python
def test_room_send_options_reads_metadata():
    message = Message(
        message_id="m1",
        role=Role.ROLE_USER,
        parts=[Part(text="hi")],
        metadata=struct_value(
            {
                A2A_ROOM_URI: {
                    "mentions": ["echo", "writer"],
                    "quote_id": "q1",
                    "interrupt": True,
                }
            }
        ),
    )
    assert room_send_options(message) == {
        "mentions": ["echo", "writer"],
        "quote_id": "q1",
        "interrupt": True,
    }


def test_room_send_options_defaults_without_metadata():
    message = Message(message_id="m1", role=Role.ROLE_USER, parts=[Part(text="hi")])
    assert room_send_options(message) == {
        "mentions": [],
        "quote_id": None,
        "interrupt": False,
    }
```

- [ ] **Step 2: 运行单测确认失败**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: FAIL（`ImportError: cannot import name 'room_send_options'`）

- [ ] **Step 3: 实现 room_send_options**

在 `src/choirworks/a2a/mapping.py` 的 `room_to_task` 之后插入：

```python
def room_send_options(message: Message) -> dict[str, Any]:
    options: dict[str, Any] = {
        "mentions": [],
        "quote_id": None,
        "interrupt": False,
    }
    field = message.metadata.fields.get(A2A_ROOM_URI)
    if field is None or field.WhichOneof("kind") != "struct_value":
        return options
    fields = field.struct_value.fields
    mentions_value = fields.get("mentions")
    if mentions_value is not None and mentions_value.WhichOneof("kind") == "list_value":
        options["mentions"] = [
            item.string_value
            for item in mentions_value.list_value.values
            if item.WhichOneof("kind") == "string_value"
        ]
    quote_value = fields.get("quote_id")
    if quote_value is not None and quote_value.WhichOneof("kind") == "string_value":
        options["quote_id"] = quote_value.string_value
    interrupt_value = fields.get("interrupt")
    if (
        interrupt_value is not None
        and interrupt_value.WhichOneof("kind") == "bool_value"
    ):
        options["interrupt"] = interrupt_value.bool_value
    return options
```

- [ ] **Step 4: 运行单测确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py -q`
Expected: PASS（8 passed）

- [ ] **Step 5: 写失败集成测试（发送两分支）**

在 `tests/integration/test_a2a_room.py` 的 import 中补充：

```python
from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)
from a2a.utils.errors import InvalidParamsError
from choirworks.a2a.mapping import A2A_ROOM_URI, struct_value
```

将原 `from a2a.types import GetTaskRequest, TaskState` 合并进上面的 import。追加：

```python
def _send(
    text: str,
    *,
    context_id: str | None = None,
    room_meta: dict | None = None,
) -> SendMessageRequest:
    message = Message(
        message_id="m-1",
        role=Role.ROLE_USER,
        parts=[Part(text=text)],
        context_id=context_id or "",
    )
    if room_meta is not None:
        message.metadata.CopyFrom(struct_value({A2A_ROOM_URI: room_meta}))
    return SendMessageRequest(message=message)


async def test_send_with_context_creates_task_in_room(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)

    responses = [
        response
        async for response in client.send_message(
            _send("@echo 请处理", context_id=conversation_id)
        )
    ]

    task = responses[-1].task
    assert task.id != conversation_id
    assert task.context_id == conversation_id
    timeline = (
        await http.get(f"/v1/conversations/{conversation_id}/messages")
    ).json()["messages"]
    assert timeline[0]["text"] == "@echo 请处理"


async def test_send_queued_returns_message(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        app = create_app(_settings(tmp_path, "room_queue.db"))
        async with app.router.lifespan_context(app):
            http, client = await _connect(app)
            try:
                await http.post(
                    "/v1/agents", json={"name": "slow", "card_url": slow.url}
                )
                conversation_id = await _new_room(http, "排队群")
                posted = (
                    await http.post(
                        f"/v1/conversations/{conversation_id}/messages",
                        json={"text": "@slow 开始", "mentions": ["slow"]},
                    )
                ).json()
                await _wait_for_active_node(http, posted["task_id"])
                quote_id = None
                for _ in range(200):
                    messages = (
                        await http.get(
                            f"/v1/conversations/{conversation_id}/messages"
                        )
                    ).json()["messages"]
                    announcements = [
                        message
                        for message in messages
                        if message["role"] == "assistant" and message["node_id"]
                    ]
                    if announcements:
                        quote_id = announcements[-1]["id"]
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("no dispatch announcement")

                responses = [
                    response
                    async for response in client.send_message(
                        _send(
                            "补充一句",
                            context_id=conversation_id,
                            room_meta={"quote_id": quote_id},
                        )
                    )
                ]
            finally:
                await client.close()
                await http.aclose()
    finally:
        await slow.stop()

    assert responses[-1].WhichOneof("payload") == "message"
    reply = responses[-1].message
    assert reply.parts[0].text == "补充一句"
    fields = reply.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"
    assert fields["quote_id"].string_value == quote_id
    assert fields["queued_for_node_id"].string_value


async def test_send_interrupt_requires_quote(hub_room):
    _, _, client = hub_room
    with pytest.raises(InvalidParamsError):
        async for _ in client.send_message(
            _send("打断", room_meta={"interrupt": True})
        ):
            pass
```

- [ ] **Step 6: 运行集成测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_room.py -q -k "send"`
Expected: FAIL（queued 分支返回 Task；interrupt 未校验）

- [ ] **Step 7: 实现 server 发送语义**

`src/choirworks/a2a/server.py` 的 mapping import 中加入 `room_send_options`：

```python
from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    TaskStreamMapper,
    room_message_to_a2a,
    room_send_options,
    room_to_task,
    snapshot_to_task,
)
```

`_submit` 从 `if context_id is not None:` 到结尾改为：

```python
        options = room_send_options(message)
        if options["interrupt"] and not options["quote_id"]:
            raise InvalidParamsError("interrupt requires quote_id")
        if context_id is not None:
            conversation = await projections.fetch_conversation(
                self._app.state.db, context_id
            )
            if conversation is None:
                context_id = None
        if context_id is None:
            context_id = await self._app.state.coordinator.create_conversation(
                title=text[:30]
            )
        try:
            result = await self._app.state.coordinator.handle_human_message(
                context_id,
                text=text,
                mentions=options["mentions"],
                quote_id=options["quote_id"],
                interrupt=options["interrupt"],
            )
        except ValueError as exc:
            raise InvalidParamsError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - 统一映射为 A2A 内部错误
            logger.exception("handle_human_message failed")
            raise InternalError(str(exc)) from exc
        if result.task_id is None or result.routed in {"queued", "interrupted"}:
            return room_message_to_a2a(result.message)
        return await self._a2a_task(result.task_id)
```

- [ ] **Step 8: 运行相关测试确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_room.py tests/integration/test_a2a_room.py tests/integration/test_a2a_send.py -q`
Expected: PASS（既有 send 测试不回归）

- [ ] **Step 9: 提交**

```bash
git add src/choirworks/a2a/mapping.py src/choirworks/a2a/server.py \
  tests/unit/test_a2a_mapping_room.py tests/integration/test_a2a_room.py
git commit -m "feat(a2a): 房间发送 metadata 与 Task/Message 两分支（M12）"
```

---

### Task 6: SubscribeToTask 房间流

**Files:**
- Modify: `src/choirworks/a2a/server.py`
- Test: `tests/integration/test_a2a_room.py`

- [ ] **Step 1: 写失败集成测试（房间订阅）**

在 `tests/integration/test_a2a_room.py` 的 import 中补：

```python
from a2a.types import SubscribeToTaskRequest
from a2a.utils.errors import TaskNotFoundError
from tests.support.hubs import start_hub
```

追加：

```python
async def test_subscribe_room_streams_live_messages(tmp_path):
    async with start_hub(
        lambda port: _settings(tmp_path, "room_stream.db", port=port)
    ) as (app, base_url):
        http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        card = await A2ACardResolver(
            httpx_client=http, base_url=base_url
        ).get_agent_card()
        client = await create_client(
            agent=card,
            client_config=ClientConfig(streaming=True, httpx_client=http),
        )
        try:
            created = (
                await http.post("/v1/conversations", json={"title": "直播群"})
            ).json()
            conversation_id = created["conversation_id"]
            stream = client.subscribe(SubscribeToTaskRequest(id=conversation_id))
            first = await asyncio.wait_for(anext(stream), timeout=10)
            assert first.WhichOneof("payload") == "task"
            assert first.task.id == conversation_id
            assert first.task.status.state is TaskState.TASK_STATE_INPUT_REQUIRED

            posting = asyncio.create_task(
                _post_room_message(app, conversation_id, "直播消息")
            )
            frame = None
            for _ in range(20):
                candidate = await asyncio.wait_for(anext(stream), timeout=10)
                if candidate.WhichOneof("payload") == "message":
                    frame = candidate
                    break
            await posting
            await stream.aclose()
        finally:
            await client.close()
            await http.aclose()

    assert frame is not None
    assert frame.message.parts[0].text == "直播消息"
    fields = frame.message.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"
    assert fields["seq"].number_value == 1


async def test_subscribe_unknown_raises(hub_room):
    _, _, client = hub_room
    with pytest.raises(TaskNotFoundError):
        async for _ in client.subscribe(
            SubscribeToTaskRequest(id="missing")
        ):
            pass


async def test_room_task_is_readable_without_extension_activation(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)
    await _post_room_message(app, conversation_id, "纯文本消息")

    room = await client.get_task(GetTaskRequest(id=conversation_id))

    assert room.history
    for message in room.history:
        assert message.message_id
        assert message.role in {Role.ROLE_USER, Role.ROLE_AGENT}
        assert message.parts[0].text
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_room.py -q -k "subscribe or readable"`
Expected: FAIL（订阅房间报 TaskNotFoundError）

- [ ] **Step 3: 实现 server 房间流**

`src/choirworks/a2a/server.py` 的 mapping import 中加入 `RoomStreamMapper`：

```python
from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    RoomStreamMapper,
    TaskStreamMapper,
    room_message_to_a2a,
    room_send_options,
    room_to_task,
    snapshot_to_task,
)
```

在 `_stream_task` 之后插入：

```python
    async def _stream_room(self, conversation_id: str):
        store = self._app.state.event_store
        bus = self._app.state.event_bus
        seen = await store.latest_conversation_seq(conversation_id)
        room, running = await self._room_snapshot(conversation_id)
        mapper = RoomStreamMapper(conversation_id, running=running)
        subscription = bus.subscribe(f"room:{conversation_id}")
        try:
            yield room
            for event in await store.replay_conversation(conversation_id, seen):
                seen = event.seq
                for response in mapper.map_event(event):
                    yield _unwrap_stream_response(response)
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                for response in mapper.map_event(event):
                    yield _unwrap_stream_response(response)
        finally:
            subscription.close()
            bus.unsubscribe(subscription)
```

`on_subscribe_to_task` 改为：

```python
    async def on_subscribe_to_task(
        self,
        params: SubscribeToTaskRequest,
        context: ServerCallContext,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None
    ]:
        self._note_extensions(context)
        try:
            await self._snapshot(params.id)
        except TaskNotFound:
            try:
                await self._room_task(params.id)
            except TaskNotFound as exc:
                raise TaskNotFoundError(f"task not found: {params.id}") from exc
            async for response in self._stream_room(params.id):
                yield response
            return
        async for response in self._stream_task(params.id):
            yield response
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_room.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add src/choirworks/a2a/server.py tests/integration/test_a2a_room.py
git commit -m "feat(a2a): SubscribeToTask 房间常开流（M12）"
```

---

### Task 7: Room Extension 文档与全量验证

**Files:**
- Create: `docs/extensions/room-v1.md`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-09-13-a2a-facade-design.md`

- [ ] **Step 1: 写 Room Extension v1 规范**

新建 `docs/extensions/room-v1.md`：

````markdown
# ChoirWorks Room Extension v1

- **URI:** `https://github.com/Javey/choirworks/extensions/room/v1`
- **状态:** 阶段 1（无鉴权、无分页）

## 1. 概述

ChoirWorks 的「群」是一个长期存在的 A2A `Task`：群里的消息是该 Task 的 `history`，
发送消息即向该 Task 追加 `Message`。感知扩展的 client 可读取 `metadata[room-uri]`
还原成员、引用、排队、摘要等群语义；不感知扩展的 client 看到的是完全合法的
Task/Message/StatusUpdate 序列。

## 2. AgentCard 声明

```json
"capabilities": {
  "streaming": true,
  "extensions": [{
    "uri": "https://github.com/Javey/choirworks/extensions/room/v1",
    "description": "Conversations as long-lived A2A tasks; room messages as A2A Messages",
    "required": false
  }]
}
```

## 3. 激活

client 在 HTTP 头 `A2A-Extensions` 中声明 URI。阶段 1 服务端行为不因激活与否改变
（仅日志），因为合成 Task/Message 对未激活 client 同样合法。

## 4. 合成 Room Task

| 字段 | 值 |
|---|---|
| `id` / `contextId` | `conversation_id` |
| `status.state` | 群内有非终态任务 → `TASK_STATE_WORKING`；否则 `TASK_STATE_INPUT_REQUIRED` |
| `history` | 群消息按 `seq` 升序（阶段 1 最多 1000 条，分页见 §8） |
| `artifacts` | 无 |
| `metadata[room-uri]` | `{kind:"room", title, members:[{agent_name,agent_url,reason,joined_at}], summary:{covers_seq,updated_at,content}, message_count, last_seq}` |

`GetTask(id)` 先按任务投影查，未命中再按会话投影查；都未命中 → `TaskNotFoundError`。
`CancelTask` 只作用于任务；对 Room Task 调用会得到 `TaskNotFoundError`（房间不是可取消任务）。

## 5. Message 映射

| A2A 字段 | 来源 |
|---|---|
| `messageId` | 房间消息 id |
| `contextId` | `conversation_id` |
| `taskId` | 消息关联任务；无关联时为合成 Room Task id |
| `role` | `user` → `ROLE_USER`；`assistant`/`agent` → `ROLE_AGENT` |
| `parts` | `[{text}]` |
| `extensions` | `[room-uri]` |
| `metadata[room-uri]` | `{kind:"message", sender, seq, mentions[], quote_id, node_id, intervention_id, queued_for_node_id}` |

## 6. 事件映射（订阅）

| 内部事件 | A2A 输出 |
|---|---|
| `message.posted` | `StreamResponse.message` |
| `message.delivered` | `status_update`（`kind:"message.delivered"`，`message_id`、`node_id`，供排队角标） |
| `room.participant_joined` | `status_update`（`kind:"room.participant_joined"` + `agent_name/agent_url/reason`） |
| `room.summary_updated` | `status_update`（`kind:"room.summary_updated"` + `covers_seq/summary`） |
| 其他（任务/节点/计划事件） | 不转发 |

> 房间状态变化（WORKING ↔ INPUT_REQUIRED）以重新订阅时的快照为准；订阅期间不随任务
> 生命周期推送状态帧。

## 7. 订阅与发送

- `SubscribeToTask(conversation_id)`：先发合成 Task 快照（含 history），随后转发 live；
  房间流常开，由 client 主动断开。
- `SendMessage` + `message.contextId=conversation_id`：
  - 产生任务（@单人 / 自动拆解 / 作答干预 / 终态续接）→ 返回 `Task`；
  - 排队补投 / 打断转交 → 返回 `Message`（映射该条房间消息）。
- `metadata[room-uri]` 可携带 `mentions`、`quote_id`、`interrupt`，与文本 `@` 解析合并；
  `interrupt=true` 必须同时给 `quote_id`，否则 `InvalidParamsError`。

## 8. 已知限制（阶段 1）

- history 全量返回上限 1000 条；分页（`historyLength`/游标）未实现，`GetTask` 不接受分页参数；
- 无鉴权，仅限内网使用；
- 房间订阅不转发任务类事件；
- Room Task 的 `artifacts` 为空（任务产物在各自任务流中）。
````

- [ ] **Step 2: README 补充**

`README.md` 的「A2A 北向接口」章节末尾（> 引用块之前）插入：

```markdown
- Room Extension v1：`conversation_id` 即合成 Room Task 的 `id`/`contextId`，`GetTask`/`SubscribeToTask`/`SendMessage` 均可直接操作房间；规范见 `docs/extensions/room-v1.md`
```

- [ ] **Step 3: spec 补实施记录**

`docs/superpowers/specs/2026-09-13-a2a-facade-design.md` 的 §6.4 表格后插入：

```markdown
> **M12 实现说明**：房间订阅仅转发本表事件；任务/节点事件不进入房间流，房间状态变化在重新订阅时由快照体现。history 阶段 1 全量返回上限 1000 条，分页 TODO。
```

在文件末尾「执行后修正（M11 实施记录）」式章节后追加：

```markdown
## M12 实施记录

- `room_send_options`：`metadata[room-uri]` 支持 `mentions`/`quote_id`/`interrupt`；未给 metadata 时保持 M11 行为；
- 发送响应分支：`queued`/`interrupted` 路由返回 `Message`（消息 metadata 含 `task_id`/`queued_for_node_id`），其余产生任务的路由返回 `Task`；
- 房间订阅首帧为合成 Task 快照，`seen` 取订阅前会话最新事件 seq；快照与回放可能重复一条消息，client 按 `messageId` 去重；
- `CancelTask` 对 conversation id 返回 `TaskNotFoundError`（房间不是任务）。
```

- [ ] **Step 4: 全量验证**

Run: `uv run ruff check .`
Expected: All checks passed!

Run: `uv run pytest -p no:warnings -q`
Expected: 全部通过（M11 基线 232 + M12 新增 ≈ 14）

- [ ] **Step 5: 提交**

```bash
git add docs/extensions/room-v1.md README.md \
  docs/superpowers/specs/2026-09-13-a2a-facade-design.md
git commit -m "docs: Room Extension v1 规范与 M12 实施记录"
```

---

## Self-Review 记录

- **Spec 覆盖**：§6.1 声明/激活 → Task 1/4（`_note_extensions`）；§6.2 合成 Room Task → Task 2/4；§6.3 消息映射 → Task 2（含既有 `room_message_to_a2a`）；§6.4 事件映射 → Task 3/6；§6.5 订阅与发送 → Task 5/6；§6.6 优雅降级 → Task 6（无扩展头路径天然覆盖）；§7 错误 → Task 4/5/6（`TaskNotFoundError`/`InvalidParamsError`）；§9 M12 验收 → Task 4/5/6 集成测试；§10 测试策略 → 各 Task；文档 → Task 7。
- **占位符扫描**：无 TBD/TODO 式实现步骤；文档中的「分页 TODO」是产品限制描述，不是计划占位。
- **类型一致性**：`room_to_task`/`room_message_from_event`/`RoomStreamMapper`/`room_send_options`/`latest_conversation_seq`/`_room_snapshot`/`_room_task`/`_stream_room` 命名在任务间一致；`room_send_options` 返回 `{"mentions","quote_id","interrupt"}` 键在单测与 server 使用中一致。
- **已知取舍**：房间订阅首帧与回放可能重复一条消息（按 messageId 去重）；`_room_snapshot` 每任务一次 `fetch_task`（会话任务数少，阶段 1 可接受）。
