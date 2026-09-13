# A2A M11：核心 A2A Facade 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 ChoirWorks 成为标准 A2A v1.0 Server（AgentCard + JSON-RPC），把 hub task 映射为 A2A Task，支持发送/查询/取消/订阅，并通过双实例嵌套测试验证。

**Architecture:** 新增 `a2a/card.py`（AgentCard）、`a2a/mapping.py`（纯映射：投影/事件 ⇄ A2A 类型）、`a2a/server.py`（自定义 `RequestHandler` 桥接 TaskService/EventBus/Coordinator），路由经 `create_agent_card_routes` + `create_jsonrpc_routes` 挂进现有 FastAPI app。现有 REST/SSE 与内部事件体系零改动。

**Tech Stack:** Python 3.12 / FastAPI / `a2a-sdk[http-server]>=1.1,<2`（protobuf 类型）/ pytest + httpx + uvicorn（流式测试）。

**Spec:** `docs/superpowers/specs/2026-09-13-a2a-facade-design.md`（§3–§5、§7、§9–§10）

**约定：**
- 后端测试：`uv run pytest -p no:warnings -q`；单文件：`uv run pytest tests/<path> -q`
- Lint：`uv run ruff check .`
- 每个 Task 结束前跑定向测试 + `ruff`，然后提交
- 本计划只覆盖 M11；M12（Room Extension）与 M13（前端）在 M11 落地后另行写计划

**关键 SDK 事实（实现时依赖）：**
- JSON-RPC 方法名：`SendMessage` / `SendStreamingMessage` / `GetTask` / `CancelTask` / `SubscribeToTask`
- `RequestHandler` 签名（`a2a.server.request_handlers.RequestHandler`）：
  - `on_message_send(params: SendMessageRequest, context) -> Task | Message`
  - `on_message_send_stream(params, context) -> AsyncGenerator[Message|Task|TaskStatusUpdateEvent|TaskArtifactUpdateEvent]`
  - `on_get_task(params: GetTaskRequest, context) -> Task | None`（None → SDK 抛 `TaskNotFoundError`）
  - `on_cancel_task(params: CancelTaskRequest, context) -> Task | None`
  - `on_subscribe_to_task(params: SubscribeToTaskRequest, context) -> AsyncGenerator[...]`
  - `on_list_tasks(params: ListTasksRequest, context) -> ListTasksResponse`（本阶段抛 `UnsupportedOperationError`）
- 错误类位于 `a2a.utils.errors`；路由用 `create_jsonrpc_routes(request_handler=..., rpc_url="/v1/a2a")`，`enable_v0_3_compat` 默认 False（不加）

---

### Task 1: A2A 配置 + AgentCard 端点

**Files:**
- Modify: `src/choirworks/config.py`
- Modify: `config.example.yaml`
- Create: `src/choirworks/a2a/card.py`
- Modify: `src/choirworks/api/app.py`（在静态挂载**之前**注册 card 路由）
- Test: `tests/integration/test_a2a_card.py`

- [ ] **Step 1: 写失败测试**

`tests/integration/test_a2a_card.py`：

```python
import httpx

from choirworks.api.app import create_app
from choirworks.config import Settings


async def test_agent_card_served(tmp_path):
    settings = Settings(
        store={"db_path": tmp_path / "card.db"},
        a2a={"public_url": "http://127.0.0.1:9999"},
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get("/.well-known/agent-card.json")
    assert resp.status_code == 200
    card = resp.json()
    assert card["name"] == "ChoirWorks"
    assert card["capabilities"]["streaming"] is True
    assert card["supportedInterfaces"][0]["protocolBinding"] == "JSONRPC"
    assert card["supportedInterfaces"][0]["url"] == "http://127.0.0.1:9999/v1/a2a"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_card.py -q`
Expected: FAIL（404，路由不存在）

- [ ] **Step 3: 实现配置与 card**

`src/choirworks/config.py` 增加（放在 `RecoveryConfig` 之后）：

```python
class A2AConfig(BaseModel):
    public_url: str = "http://127.0.0.1:8080"
```

`Settings` 增加字段（放在 `recovery` 之后）：

```python
    a2a: A2AConfig = Field(default_factory=A2AConfig)
```

`config.example.yaml` 末尾追加：

```yaml
a2a:
  public_url: http://127.0.0.1:8080
```

`src/choirworks/a2a/card.py`：

```python
from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
)


def build_agent_card(public_url: str) -> AgentCard:
    base = public_url.rstrip("/")
    return AgentCard(
        name="ChoirWorks",
        description="多 Agent 协作工作群（A2A facade）",
        version="0.1.0",
        capabilities=AgentCapabilities(streaming=True),
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

- [ ] **Step 4: 在 app 中挂载 card 路由**

`src/choirworks/api/app.py` 顶部 import 增加：

```python
from a2a.server.routes import create_agent_card_routes

from choirworks.a2a.card import build_agent_card
```

在 `create_app` 中，`app.include_router(rollback_routes.router, prefix="/v1")` 之后、`@app.get("/healthz")` 之前插入：

```python
    card = build_agent_card(resolved.a2a.public_url)
    app.router.routes.extend(create_agent_card_routes(agent_card=card))
```

注意：必须在 `app.mount("/", StaticFiles(...))` 之前（当前静态挂载在函数末尾，满足）。

- [ ] **Step 5: 跑测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_card.py -q`
Expected: PASS

- [ ] **Step 6: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/config.py config.example.yaml src/choirworks/a2a/card.py src/choirworks/api/app.py tests/integration/test_a2a_card.py
git commit -m "feat(a2a): AgentCard 端点与 a2a.public_url 配置"
```

---

### Task 2: 映射基座（状态表 + 快照 → A2A Task）

**Files:**
- Create: `src/choirworks/a2a/mapping.py`
- Test: `tests/unit/test_a2a_mapping_task.py`

- [ ] **Step 1: 写失败测试**

`tests/unit/test_a2a_mapping_task.py`：

```python
from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import TASK_STATE_MAP, snapshot_to_task
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask
from choirworks.models.enums import NodeStatus, TaskStatus


def _snapshot(status: TaskStatus, node_status: NodeStatus = NodeStatus.COMPLETED):
    task = OrchestrationTask(
        id="task-1",
        status=status,
        request="分析 X",
        conversation_id="conv-1",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    node = Node(
        id="plan1:n1",
        task_id="task-1",
        plan_id="plan1",
        name="researcher",
        agent_name="researcher",
        status=node_status,
        attempt=2,
        output={"artifacts": [{"id": "a1", "name": "output", "text": "结论"}]},
    )
    return TaskSnapshot(task=task, plan=None, nodes=[node], last_seq=7)


def test_task_state_map_covers_all_statuses():
    assert set(TASK_STATE_MAP) == set(TaskStatus)


def test_snapshot_maps_state_and_artifacts():
    mapped = snapshot_to_task(_snapshot(TaskStatus.RUNNING))
    assert mapped.id == "task-1"
    assert mapped.context_id == "conv-1"
    assert mapped.status.state is TaskState.TASK_STATE_WORKING
    assert mapped.artifacts[0].artifact_id == "plan1:n1:a1"
    assert mapped.artifacts[0].parts[0].text == "结论"


def test_snapshot_history_contains_request():
    mapped = snapshot_to_task(_snapshot(TaskStatus.COMPLETED))
    assert mapped.history[0].message_id == "task-1:request"
    assert mapped.history[0].role is Role.ROLE_USER
    assert mapped.history[0].parts[0].text == "分析 X"


def test_snapshot_metadata_lists_nodes():
    mapped = snapshot_to_task(_snapshot(TaskStatus.RUNNING))
    nodes = mapped.metadata.fields["nodes"].list_value.values
    assert nodes[0].struct_value.fields["id"].string_value == "plan1:n1"
    assert nodes[0].struct_value.fields["agent_name"].string_value == "researcher"
    assert nodes[0].struct_value.fields["attempt"].number_value == 2


def test_failed_snapshot_carries_error_message():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED)
    snapshot.nodes[0].error = "boom"
    mapped = snapshot_to_task(snapshot)
    assert mapped.status.state is TaskState.TASK_STATE_FAILED
    assert mapped.status.message.parts[0].text == "boom"


def test_input_required_question_message():
    snapshot = _snapshot(TaskStatus.AWAITING_INPUT, node_status=NodeStatus.INPUT_REQUIRED)
    mapped = snapshot_to_task(snapshot, question="请补充预算口径")
    assert mapped.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert mapped.status.message.parts[0].text == "请补充预算口径"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/unit/test_a2a_mapping_task.py -q`
Expected: FAIL（`ModuleNotFoundError: choirworks.a2a.mapping`）

- [ ] **Step 3: 实现 mapping 基座**

`src/choirworks/a2a/mapping.py`：

```python
from __future__ import annotations

from typing import Any

from a2a.types import (
    Artifact,
    Message,
    Part,
    Role,
    Task,
    TaskState,
    TaskStatus as A2ATaskStatus,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node
from choirworks.models.enums import TaskStatus

A2A_ROOM_URI = "https://github.com/Javey/choirworks/extensions/room/v1"

TASK_STATE_MAP: dict[TaskStatus, TaskState] = {
    TaskStatus.PENDING: TaskState.TASK_STATE_SUBMITTED,
    TaskStatus.PLANNING: TaskState.TASK_STATE_WORKING,
    TaskStatus.RUNNING: TaskState.TASK_STATE_WORKING,
    TaskStatus.AWAITING_INPUT: TaskState.TASK_STATE_INPUT_REQUIRED,
    TaskStatus.COMPLETED: TaskState.TASK_STATE_COMPLETED,
    TaskStatus.FAILED: TaskState.TASK_STATE_FAILED,
    TaskStatus.CANCELED: TaskState.TASK_STATE_CANCELED,
}

TERMINAL_A2A_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_CANCELED,
}


def struct_value(data: dict[str, Any]) -> struct_pb2.Struct:
    value = struct_pb2.Struct()
    ParseDict(data, value)
    return value


def data_part(data: dict[str, Any]) -> Part:
    part = Part()
    ParseDict({"data": data}, part)
    return part


def artifact_id(node_id: str, inner_id: str) -> str:
    return f"{node_id}:{inner_id}"


def agent_message(text: str, *, message_id: str) -> Message:
    return Message(
        message_id=message_id,
        role=Role.ROLE_AGENT,
        parts=[Part(text=text)],
    )


def _node_metadata(node: Node) -> dict[str, Any]:
    return {
        "id": node.id,
        "name": node.name,
        "status": node.status.value,
        "agent_name": node.agent_name,
        "attempt": node.attempt,
    }


def _collect_artifacts(nodes: list[Node]) -> list[Artifact]:
    artifacts: list[Artifact] = []
    for node in nodes:
        for item in (node.output or {}).get("artifacts", []):
            artifacts.append(
                Artifact(
                    artifact_id=artifact_id(node.id, item["id"]),
                    name=item.get("name") or item["id"],
                    parts=[Part(text=item.get("text", ""))],
                )
            )
    return artifacts


def _status_message(
    snapshot: TaskSnapshot, question: str | None
) -> Message | None:
    task = snapshot.task
    if task.status is TaskStatus.AWAITING_INPUT:
        if question:
            return agent_message(question, message_id=f"{task.id}:question")
        return None
    if task.status is TaskStatus.FAILED:
        for node in snapshot.nodes:
            if node.error:
                return agent_message(node.error, message_id=f"{task.id}:error")
    return None


def snapshot_to_task(
    snapshot: TaskSnapshot, *, question: str | None = None
) -> Task:
    task = snapshot.task
    metadata = {
        "plan_version": task.plan_version,
        "nodes": [_node_metadata(node) for node in snapshot.nodes],
    }
    status = A2ATaskStatus(state=TASK_STATE_MAP[task.status])
    message = _status_message(snapshot, question)
    if message is not None:
        status.message.CopyFrom(message)
    return Task(
        id=task.id,
        context_id=task.conversation_id or "",
        status=status,
        artifacts=_collect_artifacts(snapshot.nodes),
        history=[
            Message(
                message_id=f"{task.id}:request",
                role=Role.ROLE_USER,
                parts=[Part(text=task.request)],
            )
        ],
        metadata=struct_value(metadata),
    )
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_task.py -q`
Expected: PASS（6 passed）

- [ ] **Step 5: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/a2a/mapping.py tests/unit/test_a2a_mapping_task.py
git commit -m "feat(a2a): 任务状态与快照映射"
```

---

### Task 3: 事件流映射器 TaskStreamMapper

**Files:**
- Modify: `src/choirworks/a2a/mapping.py`
- Test: `tests/unit/test_a2a_mapping_stream.py`

- [ ] **Step 1: 写失败测试**

`tests/unit/test_a2a_mapping_stream.py`：

```python
from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import TaskStreamMapper
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store.event_store import Event


def _snapshot() -> TaskSnapshot:
    task = OrchestrationTask(
        id="task-1",
        status=TaskStatus.RUNNING,
        request="分析 X",
        conversation_id="conv-1",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    node = Node(
        id="plan1:n1",
        task_id="task-1",
        plan_id="plan1",
        name="researcher",
        agent_name="researcher",
        status=NodeStatus.WORKING,
    )
    return TaskSnapshot(task=task, plan=None, nodes=[node], last_seq=0)


def _event(event_type: EventType, payload: dict, seq: int = 1) -> Event:
    return Event(
        seq=seq,
        task_id="task-1",
        conversation_id="conv-1",
        type=event_type,
        payload=payload,
        created_at=datetime.now(UTC),
    )


def test_task_state_change_maps_and_dedupes():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.TASK_STATE_CHANGED,
            {"from": "running", "to": "awaiting_input"},
        )
    )
    assert responses[0].status_update.status.state is (
        TaskState.TASK_STATE_INPUT_REQUIRED
    )
    assert mapper.terminal is False
    assert (
        mapper.map_event(
            _event(
                EventType.TASK_STATE_CHANGED,
                {"from": "running", "to": "awaiting_input"},
                seq=2,
            )
        )
        == []
    )


def test_task_completed_marks_terminal():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(_event(EventType.TASK_COMPLETED, {}, seq=3))
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_COMPLETED
    assert mapper.terminal is True


def test_node_state_change_carries_metadata():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {
                "node_id": "plan1:n1",
                "node_name": "researcher",
                "agent_name": "researcher",
                "attempt": 1,
                "from": "ready",
                "to": "working",
            },
        )
    )
    meta = responses[0].status_update.metadata
    assert meta.fields["node_id"].string_value == "plan1:n1"
    assert meta.fields["kind"].string_value == "node.state_changed"
    assert meta.fields["to"].string_value == "working"


def test_node_artifact_maps_append_and_prefix():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.NODE_ARTIFACT,
            {
                "node_id": "plan1:n1",
                "artifact_id": "a1",
                "name": "output",
                "text": "增量",
                "append": True,
            },
        )
    )
    update = responses[0].artifact_update
    assert update.artifact.artifact_id == "plan1:n1:a1"
    assert update.append is True
    assert update.last_chunk is False
    assert update.artifact.parts[0].text == "增量"


def test_node_terminal_emits_last_chunk_for_known_artifacts():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(
        _event(
            EventType.NODE_ARTIFACT,
            {
                "node_id": "plan1:n1",
                "artifact_id": "a1",
                "name": "output",
                "text": "增量",
                "append": True,
            },
        )
    )
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {"node_id": "plan1:n1", "from": "working", "to": "completed"},
            seq=2,
        )
    )
    assert len(responses) == 2
    last = responses[1].artifact_update
    assert last.artifact.artifact_id == "plan1:n1:a1"
    assert last.append is True
    assert last.last_chunk is True


def test_plan_created_maps_to_data_artifact():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_CREATED,
            {
                "plan_id": "plan1",
                "version": 1,
                "rationale": "因为",
                "dag": {"nodes": [{"id": "n1"}]},
            },
        )
    )
    update = responses[0].artifact_update
    assert update.artifact.artifact_id == "plan:plan1"
    assert update.artifact.parts[0].data.fields["nodes"].list_value
    assert update.last_chunk is True


def test_intervention_requested_sets_input_required():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "plan1:n1",
                "policy": "human",
                "question": {"text": "请确认口径"},
            },
        )
    )
    update = responses[0].status_update
    assert update.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert update.status.message.role is Role.ROLE_AGENT
    assert update.status.message.parts[0].text == "请确认口径"


def test_notification_events_do_not_change_state():
    mapper = TaskStreamMapper(_snapshot())
    for event_type in (
        EventType.NODE_DISPATCH_INTENT,
        EventType.NODE_DISPATCHED,
        EventType.NODE_RETRY_SCHEDULED,
        EventType.CHECKPOINT_CREATED,
        EventType.ROLLBACK_PERFORMED,
        EventType.ERROR,
    ):
        mapper = TaskStreamMapper(_snapshot())
        responses = mapper.map_event(
            _event(event_type, {"node_id": "plan1:n1", "message": "m"})
        )
        assert responses[0].status_update.status.state is (
            TaskState.TASK_STATE_WORKING
        )
        assert responses[0].status_update.metadata.fields["kind"].string_value == (
            event_type.value
        )


def test_node_output_is_not_emitted():
    mapper = TaskStreamMapper(_snapshot())
    assert mapper.map_event(_event(EventType.NODE_OUTPUT, {"node_id": "plan1:n1"})) == []
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/unit/test_a2a_mapping_stream.py -q`
Expected: FAIL（`ImportError: cannot import name 'TaskStreamMapper'`）

- [ ] **Step 3: 实现 TaskStreamMapper**

先把以下 import 合并进 `src/choirworks/a2a/mapping.py` 顶部 import 区（避免 E402 中段导入）：

```python
from collections import defaultdict

from a2a.types import StreamResponse, TaskArtifactUpdateEvent, TaskStatusUpdateEvent

from choirworks.models.enums import EventType, NodeStatus
from choirworks.store.event_store import Event
```

然后在 `mapping.py` 文件末尾追加：

```python
_TERMINAL_NODE_VALUES = {
    NodeStatus.COMPLETED.value,
    NodeStatus.FAILED.value,
    NodeStatus.CANCELED.value,
    NodeStatus.INVALIDATED.value,
}

_PLAN_EVENTS = {
    EventType.PLAN_CREATED,
    EventType.PLAN_EXTENDED,
    EventType.PLAN_SUPERSEDED,
}

_NOTIFICATION_STATES = {
    EventType.NODE_DISPATCH_INTENT,
    EventType.NODE_DISPATCHED,
    EventType.NODE_STATE_CHANGED,
    EventType.NODE_RETRY_SCHEDULED,
    EventType.NODE_INVALIDATED,
    EventType.NODE_CANCEL_SENT,
    EventType.INTERVENTION_RESOLVED,
    EventType.INTERVENTION_FAILED,
    EventType.CHECKPOINT_CREATED,
    EventType.ROLLBACK_PERFORMED,
    EventType.ERROR,
}


class TaskStreamMapper:
    def __init__(self, snapshot: TaskSnapshot):
        self.terminal = False
        self._task_id = snapshot.task.id
        self._context_id = snapshot.task.conversation_id or ""
        self._state = TASK_STATE_MAP[snapshot.task.status]
        self._node_artifacts: dict[str, set[str]] = defaultdict(set)
        for node in snapshot.nodes:
            for item in (node.output or {}).get("artifacts", []):
                self._node_artifacts[node.id].add(item["id"])

    def _status_update(
        self, state: TaskState | None = None, *, metadata: dict[str, Any] | None = None,
        message_text: str | None = None, message_id: str = "m",
    ) -> StreamResponse:
        status = A2ATaskStatus(state=state or self._state)
        if message_text is not None:
            status.message.CopyFrom(agent_message(message_text, message_id=message_id))
        return StreamResponse(
            status_update=TaskStatusUpdateEvent(
                task_id=self._task_id,
                context_id=self._context_id,
                status=status,
                metadata=struct_value(metadata or {}),
            )
        )

    def _apply_state(self, state: TaskState) -> list[StreamResponse]:
        if state is self._state:
            return []
        self._state = state
        if state in TERMINAL_A2A_STATES:
            self.terminal = True
        return [self._status_update(state)]

    def _last_chunks(self, node_id: str) -> list[StreamResponse]:
        responses: list[StreamResponse] = []
        for inner_id in sorted(self._node_artifacts.get(node_id, set())):
            responses.append(
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(artifact_id=artifact_id(node_id, inner_id)),
                        append=True,
                        last_chunk=True,
                    )
                )
            )
        return responses

    def map_event(self, event: Event) -> list[StreamResponse]:
        event_type = event.type
        payload = event.payload
        if event_type is EventType.TASK_STATE_CHANGED:
            state = TASK_STATE_MAP[TaskStatus(payload["to"])]
            responses = self._apply_state(state)
            if not responses:
                return []
            responses[0].status_update.metadata.CopyFrom(
                struct_value({"from": payload.get("from"), "to": payload.get("to")})
            )
            return responses
        if event_type is EventType.TASK_COMPLETED:
            return self._apply_state(TaskState.TASK_STATE_COMPLETED)
        if event_type is EventType.TASK_FAILED:
            return self._apply_state(TaskState.TASK_STATE_FAILED)
        if event_type is EventType.NODE_ARTIFACT:
            node_id = payload["node_id"]
            self._node_artifacts[node_id].add(payload["artifact_id"])
            return [
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(
                            artifact_id=artifact_id(node_id, payload["artifact_id"]),
                            name=payload.get("name") or "",
                            parts=[Part(text=payload.get("text", ""))],
                        ),
                        append=bool(payload.get("append")),
                        last_chunk=False,
                        metadata=struct_value({"node_id": node_id}),
                    )
                )
            ]
        if event_type in _PLAN_EVENTS:
            dag = payload.get("dag")
            parts = [data_part(dag)] if dag is not None else []
            return [
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(
                            artifact_id=f"plan:{payload.get('plan_id')}",
                            name="plan",
                            parts=parts,
                        ),
                        append=False,
                        last_chunk=True,
                        metadata=struct_value(
                            {
                                "version": payload.get("version"),
                                "rationale": payload.get("rationale"),
                            }
                        ),
                    )
                )
            ]
        if event_type is EventType.INTERVENTION_REQUESTED:
            question = (payload.get("question") or {}).get("text", "")
            self._state = TaskState.TASK_STATE_INPUT_REQUIRED
            return [
                self._status_update(
                    TaskState.TASK_STATE_INPUT_REQUIRED,
                    metadata={
                        "kind": event_type.value,
                        "intervention_id": payload.get("intervention_id"),
                        "node_id": payload.get("node_id"),
                        "policy": payload.get("policy"),
                    },
                    message_text=question,
                    message_id=payload.get("intervention_id") or "intervention",
                )
            ]
        if event_type in _NOTIFICATION_STATES:
            metadata = {"kind": event_type.value, **payload}
            responses = [self._status_update(metadata=metadata)]
            if (
                event_type is EventType.NODE_STATE_CHANGED
                and payload.get("to") in _TERMINAL_NODE_VALUES
            ):
                responses.extend(self._last_chunks(payload["node_id"]))
            return responses
        return []
```

注意：`_status_update` 里 `state or self._state` 对 `TASK_STATE_UNSPECIFIED`（0）不成立，但本映射不会传入；`INTERVENTION_RESOLVED` 后状态回到 WORKING 依赖后续 `task.state_changed` 事件，本设计不改 `self._state`（保持 STATUS 事件单独驱动），因此 `_NOTIFICATION_STATES` 通知不会把状态卡在 INPUT_REQUIRED。

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/unit/test_a2a_mapping_stream.py -q`
Expected: PASS（9 passed）

- [ ] **Step 5: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/a2a/mapping.py tests/unit/test_a2a_mapping_stream.py
git commit -m "feat(a2a): 事件流映射器 TaskStreamMapper"
```

---

### Task 4: 取消逻辑抽取 + 只读 Handler（GetTask/CancelTask）+ JSON-RPC 挂载

**Files:**
- Create: `src/choirworks/core/cancel.py`
- Modify: `src/choirworks/api/rollback.py`（cancel 端点改为调用共享函数）
- Create: `src/choirworks/a2a/server.py`（先只实现只读面 + unsupported）
- Modify: `src/choirworks/api/app.py`（挂载 jsonrpc 路由）
- Test: `tests/integration/test_a2a_read.py`

- [ ] **Step 1: 写失败测试**

`tests/integration/test_a2a_read.py`：

```python
import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import CancelTaskRequest, GetTaskRequest
from a2a.utils.errors import TaskNotCancelableError, TaskNotFoundError

from choirworks.api.app import create_app
from choirworks.config import Settings


@pytest.fixture
async def hub(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "read.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            card = await A2ACardResolver(
                httpx_client=http, base_url="http://test"
            ).get_agent_card()
            client = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=False, httpx_client=http),
            )
            yield app, http, client, ask_agent.url
            await client.close()


async def test_get_task_matches_rest_snapshot(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    mapped = await client.get_task(GetTaskRequest(id=created["task_id"]))
    rest = (await http.get(f"/v1/tasks/{created['task_id']}")).json()
    assert mapped.id == created["task_id"]
    assert mapped.context_id == rest["task"]["conversation_id"]
    assert mapped.metadata.fields["plan_version"].number_value == 1


async def test_get_task_unknown_raises(hub):
    _, _, client, _ = hub
    with pytest.raises(TaskNotFoundError):
        await client.get_task(GetTaskRequest(id="missing"))


async def test_cancel_task_marks_canceled(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    canceled = await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
    assert canceled.status.state.name == "TASK_STATE_CANCELED"


async def test_cancel_terminal_task_raises(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
    with pytest.raises(TaskNotCancelableError):
        await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
```

说明：用 `ask` agent（等待人工输入，稳定停在 `awaiting_input`）避免与 echo 的完成竞态，保证 cancel 断言确定性。

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_read.py -q`
Expected: FAIL（`ModuleNotFoundError: choirworks.a2a.server` 或 404）

- [ ] **Step 3: 抽取取消逻辑**

`src/choirworks/core/cancel.py`：

```python
from __future__ import annotations

from typing import Any

from choirworks.core.tasks import TaskNotFound
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store import projections


class TaskNotCancelable(ValueError):
    pass


CANCELLABLE_TASK_STATUSES = (
    TaskStatus.RUNNING,
    TaskStatus.AWAITING_INPUT,
    TaskStatus.PLANNING,
)


async def cancel_task(
    db: Any,
    events: Any,
    remote: Any,
    orchestrator: Any,
    task_id: str,
) -> None:
    task = await projections.fetch_task(db, task_id)
    if task is None:
        raise TaskNotFound(task_id)
    if task.status not in CANCELLABLE_TASK_STATUSES:
        raise TaskNotCancelable(f"task is {task.status.value}")

    plan = await projections.fetch_current_plan(db, task_id)
    if plan is not None:
        for node in await projections.fetch_nodes(db, task_id, plan.id):
            if node.a2a_task_id and node.status in (
                NodeStatus.DISPATCHED,
                NodeStatus.WORKING,
                NodeStatus.INPUT_REQUIRED,
            ):
                await remote.cancel_task(node.agent_url or "", node.a2a_task_id)
                await events.append(
                    task_id,
                    EventType.NODE_CANCEL_SENT,
                    {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
                )
    await events.append(
        task_id,
        EventType.TASK_STATE_CHANGED,
        {"from": task.status.value, "to": TaskStatus.CANCELED.value},
    )
    await orchestrator.stop_task(task_id)
```

`src/choirworks/api/rollback.py` 的 `cancel_task` 端点改为：

```python
@router.post("/tasks/{task_id}/cancel", response_model=TaskSnapshot)
async def cancel_task(task_id: str, request: Request) -> TaskSnapshot:
    try:
        await core_cancel_task(
            request.app.state.db,
            request.app.state.event_store,
            request.app.state.remote,
            request.app.state.orchestrator,
            task_id,
        )
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc
    except TaskNotCancelable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return await request.app.state.task_service.get_snapshot(task_id)
```

import 需调整：删除未再使用的 `EventType`、`NodeStatus`、`TaskStatus`、`projections`（若其他端点仍用则保留），新增：

```python
from choirworks.core.cancel import TaskNotCancelable
from choirworks.core.cancel import cancel_task as core_cancel_task
```

注意：模块内已有端点函数名 `cancel_task`，与导入别名 `core_cancel_task` 不冲突。

- [ ] **Step 4: 实现 HubA2AHandler 只读面**

`src/choirworks/a2a/server.py`：

```python
from __future__ import annotations

from collections.abc import AsyncGenerator

from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import RequestHandler
from a2a.types import (
    CancelTaskRequest,
    GetTaskRequest,
    ListTasksRequest,
    ListTasksResponse,
    Message,
    SendMessageRequest,
    SubscribeToTaskRequest,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a.utils.errors import (
    TaskNotCancelableError,
    UnsupportedOperationError,
)
from fastapi import FastAPI

from choirworks.a2a.mapping import snapshot_to_task
from choirworks.core.cancel import TaskNotCancelable, cancel_task
from choirworks.core.tasks import TaskNotFound, TaskSnapshot
from choirworks.models.enums import InterventionStatus, TaskStatus
from choirworks.store import projections


class HubA2AHandler(RequestHandler):
    def __init__(self, app: FastAPI):
        self._app = app

    async def _snapshot(self, task_id: str) -> TaskSnapshot:
        return await self._app.state.task_service.get_snapshot(task_id)

    async def _pending_question(self, snapshot: TaskSnapshot) -> str | None:
        if snapshot.task.status is not TaskStatus.AWAITING_INPUT:
            return None
        rows = await projections.fetch_interventions(
            self._app.state.db, snapshot.task.id, InterventionStatus.PENDING
        )
        if not rows:
            return None
        return (rows[-1].question or {}).get("text")

    async def _a2a_task(self, task_id: str) -> Task:
        snapshot = await self._snapshot(task_id)
        return snapshot_to_task(snapshot, question=await self._pending_question(snapshot))

    async def on_get_task(
        self, params: GetTaskRequest, context: ServerCallContext
    ) -> Task | None:
        try:
            return await self._a2a_task(params.id)
        except TaskNotFound:
            return None

    async def on_cancel_task(
        self, params: CancelTaskRequest, context: ServerCallContext
    ) -> Task | None:
        try:
            await cancel_task(
                self._app.state.db,
                self._app.state.event_store,
                self._app.state.remote,
                self._app.state.orchestrator,
                params.id,
            )
        except TaskNotFound:
            return None
        except TaskNotCancelable as exc:
            raise TaskNotCancelableError(str(exc)) from exc
        return await self._a2a_task(params.id)

    async def on_list_tasks(
        self, params: ListTasksRequest, context: ServerCallContext
    ) -> ListTasksResponse:
        raise UnsupportedOperationError("ListTasks is not supported yet")

    async def on_message_send(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> Task | Message:
        raise UnsupportedOperationError("SendMessage not implemented yet")

    async def on_message_send_stream(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> AsyncGenerator[Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None]:
        raise UnsupportedOperationError("SendStreamingMessage not implemented yet")
        yield  # pragma: no cover

    async def on_subscribe_to_task(
        self, params: SubscribeToTaskRequest, context: ServerCallContext
    ) -> AsyncGenerator[Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None]:
        raise UnsupportedOperationError("SubscribeToTask not implemented yet")
        yield  # pragma: no cover

    async def on_get_extended_agent_card(self, params, context):
        raise UnsupportedOperationError("extended agent card is not supported")

    async def on_create_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_get_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_list_task_push_notification_configs(self, params, context):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_delete_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError("push notifications are not supported")
```

- [ ] **Step 5: 挂载 JSON-RPC 路由**

`src/choirworks/api/app.py` import 增加：

```python
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes

from choirworks.a2a.server import HubA2AHandler
```

Task 1 的 card 路由注册处替换为：

```python
    card = build_agent_card(resolved.a2a.public_url)
    a2a_handler = HubA2AHandler(app)
    app.router.routes.extend(
        create_agent_card_routes(agent_card=card)
        + create_jsonrpc_routes(request_handler=a2a_handler, rpc_url="/v1/a2a")
    )
```

- [ ] **Step 6: 跑 read 测试与既有回归**

Run: `uv run pytest tests/integration/test_a2a_read.py -q`
Expected: PASS（4 passed）

Run: `uv run pytest -k cancel -q`
Expected: PASS（REST 取消路径回归，覆盖 `test_rollback.py` 等）

- [ ] **Step 7: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/core/cancel.py src/choirworks/api/rollback.py src/choirworks/a2a/server.py src/choirworks/api/app.py tests/integration/test_a2a_read.py
git commit -m "feat(a2a): 只读 Handler（GetTask/CancelTask）与 JSON-RPC 挂载，取消逻辑抽取"
```

---

### Task 5: 发送语义 `on_message_send`

**Files:**
- Modify: `src/choirworks/a2a/server.py`
- Test: `tests/integration/test_a2a_send.py`
- Modify: `src/choirworks/a2a/mapping.py`（补 `room_message_to_a2a`，供消息分支使用）

- [ ] **Step 1: 写失败测试**

`tests/integration/test_a2a_send.py`：

```python
import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
)
from a2a.utils.errors import TaskNotFoundError

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
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


def _message(
    text: str, *, task_id: str | None = None, context_id: str | None = None
) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id="m-1",
            role=Role.ROLE_USER,
            parts=[Part(text=text)],
            task_id=task_id or "",
            context_id=context_id or "",
        )
    )


@asynccontextmanager
async def _hub(tmp_path, db_name, agent_name, agent_url, plans):
    settings = Settings(
        store={"db_path": tmp_path / db_name},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=list(plans)))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            await http.post(
                "/v1/agents", json={"name": agent_name, "card_url": agent_url}
            )
            card = await A2ACardResolver(
                httpx_client=http, base_url="http://test"
            ).get_agent_card()
            client = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=False, httpx_client=http),
            )
            yield app, http, client
            await client.close()


@pytest.fixture
async def hub_echo(tmp_path, echo_agent):
    async with _hub(
        tmp_path, "echo.db", "echo", echo_agent.url, [_plan("echo")] * 4
    ) as value:
        yield value


@pytest.fixture
async def hub_ask(tmp_path, ask_agent):
    async with _hub(
        tmp_path, "ask.db", "ask", ask_agent.url, [_plan("ask")] * 2
    ) as value:
        yield value


async def _send(client, request: SendMessageRequest):
    responses = [response async for response in client.send_message(request)]
    assert responses
    last = responses[-1]
    if last.WhichOneof("payload") == "task":
        return last.task
    return last.message


async def test_send_creates_task_and_conversation(hub_echo):
    _, http, client = hub_echo
    task = await _send(client, _message("请评估这个问题"))
    assert task.id
    assert task.context_id
    timeline = (
        await http.get(f"/v1/conversations/{task.context_id}/messages")
    ).json()
    assert timeline["messages"][0]["role"] == "user"


async def test_send_with_context_reuses_conversation(hub_echo):
    _, _, client = hub_echo
    first = await _send(client, _message("第一个任务"))
    second = await _send(client, _message("第二个任务", context_id=first.context_id))
    assert second.context_id == first.context_id
    assert second.id != first.id


async def test_send_terminal_task_creates_followup(hub_echo):
    app, http, client = hub_echo
    first = await _send(client, _message("第一个任务"))
    for _ in range(200):
        status = (await http.get(f"/v1/tasks/{first.id}")).json()["task"]["status"]
        if status in {"completed", "failed", "canceled"}:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("first task never reached terminal state")
    second = await _send(client, _message("继续", task_id=first.id))
    assert second.id != first.id
    assert second.context_id == first.context_id


async def test_send_answers_pending_intervention(hub_ask):
    app, http, client = hub_ask
    first = await _send(client, _message("请评估"))
    for _ in range(200):
        task = await client.get_task(GetTaskRequest(id=first.id))
        if task.status.state.name == "TASK_STATE_INPUT_REQUIRED":
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("task never reached input-required")
    resumed = await _send(client, _message("这是答复", task_id=first.id))
    assert resumed.id == first.id


async def test_send_running_task_stays_same(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        async with _hub(tmp_path, "slow.db", "slow", slow.url, []) as (
            _app,
            _http,
            client,
        ):
            first = await _send(client, _message("@slow 开始"))
            second = await _send(client, _message("补充说明", task_id=first.id))
            assert second.id == first.id
    finally:
        await slow.stop()


async def test_send_unknown_task_raises(hub_echo):
    _, _, client = hub_echo
    with pytest.raises(TaskNotFoundError):
        await _send(client, _message("继续", task_id="missing"))
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_send.py -q`
Expected: FAIL（`UnsupportedOperationError`）

- [ ] **Step 3: 实现 `_submit` 与 `on_message_send`**

`src/choirworks/a2a/server.py` import 增加：

```python
from a2a.utils.errors import InvalidParamsError, InternalError, TaskNotFoundError

from choirworks.a2a.mapping import room_message_to_a2a
from choirworks.core.room import post_message
```

`on_message_send` 替换为：

```python
    async def _submit(self, params: SendMessageRequest) -> Task | Message:
        message = params.message
        text = "\n".join(
            part.text for part in message.parts if part.text
        ).strip()
        if not text:
            raise InvalidParamsError("message text is empty")

        task_id = message.task_id or None
        context_id = message.context_id or None

        if task_id is not None:
            try:
                snapshot = await self._snapshot(task_id)
            except TaskNotFound as exc:
                raise TaskNotFoundError(f"task not found: {task_id}") from exc
            status = snapshot.task.status
            if status is TaskStatus.AWAITING_INPUT:
                interventions = await projections.fetch_interventions(
                    self._app.state.db, task_id, InterventionStatus.PENDING
                )
                if not interventions:
                    raise InvalidParamsError(
                        "task is awaiting input but has no pending intervention"
                    )
                await self._app.state.orchestrator.answer_intervention(
                    interventions[-1].id, text, responder="user"
                )
                return await self._a2a_task(task_id)
            if status in (
                TaskStatus.PENDING,
                TaskStatus.PLANNING,
                TaskStatus.RUNNING,
            ):
                conversation_id = snapshot.task.conversation_id
                if conversation_id is None:
                    conversation_id = await self._app.state.coordinator.create_conversation(
                        title=text[:30]
                    )
                row = await post_message(
                    self._app.state.db,
                    self._app.state.event_store,
                    conversation_id=conversation_id,
                    role="user",
                    sender="user",
                    text=text,
                    task_id=task_id,
                )
                await self._app.state.coordinator.arbitrate_message(
                    snapshot.task, row
                )
                return await self._a2a_task(task_id)
            context_id = snapshot.task.conversation_id or context_id

        if context_id is None:
            context_id = await self._app.state.coordinator.create_conversation(
                title=text[:30]
            )
        try:
            result = await self._app.state.coordinator.handle_human_message(
                context_id, text=text, mentions=[]
            )
        except Exception as exc:  # noqa: BLE001 - 统一映射为 A2A 内部错误
            raise InternalError(str(exc)) from exc
        if result.task_id is None:
            return room_message_to_a2a(result.message)
        return await self._a2a_task(result.task_id)

    async def on_message_send(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> Task | Message:
        return await self._submit(params)
```

`src/choirworks/a2a/mapping.py` 追加消息映射（M12 会扩展 `kind`/成员等；本步先支持基础字段）：

```python
def room_message_to_a2a(message: Any) -> Message:
    metadata = {
        A2A_ROOM_URI: {
            "kind": "message",
            "sender": message.sender,
            "seq": message.seq,
            "mentions": list(message.mentions),
            "quote_id": message.quote_id,
            "node_id": message.node_id,
            "intervention_id": message.intervention_id,
            "queued_for_node_id": message.queued_for_node_id,
        }
    }
    return Message(
        message_id=message.id,
        context_id=message.conversation_id,
        task_id=message.task_id or message.conversation_id,
        role=Role.ROLE_USER if message.role == "user" else Role.ROLE_AGENT,
        parts=[Part(text=message.text)],
        extensions=[A2A_ROOM_URI],
        metadata=struct_value(metadata),
    )
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_send.py -q`
Expected: PASS（6 passed）

- [ ] **Step 5: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/a2a/server.py src/choirworks/a2a/mapping.py tests/integration/test_a2a_send.py
git commit -m "feat(a2a): SendMessage 四分支语义与房间消息映射"
```

---

### Task 6: 流式 `on_message_send_stream` / `on_subscribe_to_task`

**Files:**
- Create: `tests/support/hubs.py`（真实 uvicorn hub 启动助手）
- Modify: `src/choirworks/a2a/server.py`
- Test: `tests/integration/test_a2a_stream.py`

- [ ] **Step 1: 写 hub 助手与失败测试**

`tests/support/hubs.py`：

```python
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import uvicorn

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.sim.ports import free_port


@asynccontextmanager
async def start_hub(
    settings_factory: Callable[[int], Settings],
    llm: Any | None = None,
) -> AsyncIterator[tuple[Any, str]]:
    port = free_port()
    app = create_app(settings_factory(port), llm=llm)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态
        await asyncio.sleep(0.02)
    try:
        yield app, f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5)
```

`tests/integration/test_a2a_stream.py`：

```python
import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    Message,
    Part,
    Role,
    SendMessageRequest,
    SubscribeToTaskRequest,
    TaskState,
)

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub


def _settings(port: int, db: str) -> Settings:
    return Settings(
        store={"db_path": db},
        a2a={"public_url": f"http://127.0.0.1:{port}"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _send(text: str) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id="m-1", role=Role.ROLE_USER, parts=[Part(text=text)]
        )
    )


async def _connect(base_url: str):
    http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    card = await A2ACardResolver(httpx_client=http, base_url=base_url).get_agent_card()
    client = await create_client(
        agent=card, client_config=ClientConfig(streaming=True, httpx_client=http)
    )
    return http, client


@pytest.fixture
async def hub(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1", name="echo", agent_name="echo", input={"text": "hi"}
            )
        ],
    )
    async with start_hub(
        lambda port: _settings(port, str(tmp_path / "stream.db")),
        llm=FakeLLM(structured_results=[plan]),
    ) as (app, base_url):
        await app.state.registry.register("echo", echo_agent.url)
        yield app, base_url


async def test_streaming_send_emits_task_updates_and_artifacts(hub):
    _, base_url = hub
    http, client = await _connect(base_url)
    try:
        responses = [r async for r in client.send_message(_send("分析 X"))]
    finally:
        await client.close()
        await http.aclose()
    assert responses[0].WhichOneof("payload") == "task"
    kinds = [r.WhichOneof("payload") for r in responses]
    assert kinds[0] == "task"
    assert "artifact_update" in kinds
    assert "status_update" in kinds
    artifacts = [
        r.artifact_update
        for r in responses
        if r.WhichOneof("payload") == "artifact_update"
    ]
    assert any(a.artifact.artifact_id.startswith("n1:") for a in artifacts)
    assert any(a.append for a in artifacts)
    terminal = [
        r
        for r in responses
        if r.WhichOneof("payload") == "status_update"
        and r.status_update.status.state
        in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }
    ]
    assert terminal


async def test_subscribe_replays_snapshot_then_live(hub):
    app, base_url = hub
    http, client = await _connect(base_url)
    try:
        created = (
            await http.post(
                "/v1/tasks",
                json={"request": "hi", "target": {"agent_name": "echo", "name": "echo"}},
            )
        ).json()
        responses = [
            r async for r in client.subscribe(
                SubscribeToTaskRequest(id=created["task_id"])
            )
        ]
    finally:
        await client.close()
        await http.aclose()
    assert responses[0].WhichOneof("payload") == "task"
    assert responses[0].task.id == created["task_id"]
    last = responses[-1]
    if last.WhichOneof("payload") == "status_update":
        assert last.status_update.status.state in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }
    else:
        assert last.task.status.state in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }


async def test_subscribe_completed_task_returns_snapshot_only(hub):
    app, base_url = hub
    http, client = await _connect(base_url)
    try:
        created = (
            await http.post(
                "/v1/tasks",
                json={"request": "hi", "target": {"agent_name": "echo", "name": "echo"}},
            )
        ).json()
        for _ in range(200):
            status = (
                await http.get(f"/v1/tasks/{created['task_id']}")
            ).json()["task"]["status"]
            if status == "completed":
                break
            await asyncio.sleep(0.05)
        responses = [
            r async for r in client.subscribe(
                SubscribeToTaskRequest(id=created["task_id"])
            )
        ]
    finally:
        await client.close()
        await http.aclose()
    assert len(responses) == 1
    assert responses[0].WhichOneof("payload") == "task"
    assert responses[0].task.status.state is TaskState.TASK_STATE_COMPLETED
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/integration/test_a2a_stream.py -q`
Expected: FAIL（`UnsupportedOperationError`）

- [ ] **Step 3: 实现流式方法与事件富化**

`src/choirworks/a2a/server.py` 增加 import：

```python
from a2a.types import StreamResponse

from choirworks.a2a.mapping import TaskStreamMapper
from choirworks.core.events import SubscriptionClosed
from choirworks.models.enums import EventType
from choirworks.store.event_store import Event
```

追加方法（放在 `on_subscribe_to_task` 之前，随后替换 stub）：

```python
    async def _enrich(self, event: Event) -> Event:
        payload = dict(event.payload)
        if event.type in (
            EventType.PLAN_EXTENDED,
            EventType.PLAN_SUPERSEDED,
        ):
            plan = await projections.fetch_current_plan(
                self._app.state.db, event.task_id
            )
            if plan is not None:
                payload["dag"] = plan.dag
                payload["version"] = plan.version
                payload["rationale"] = plan.rationale
        elif event.type is EventType.NODE_STATE_CHANGED:
            node = await projections.fetch_node(
                self._app.state.db, payload.get("node_id", "")
            )
            if node is not None:
                payload.setdefault("node_name", node.name)
                payload.setdefault("agent_name", node.agent_name)
                payload.setdefault("attempt", node.attempt)
        if payload == event.payload:
            return event
        return event.model_copy(update={"payload": payload})

    async def _stream_task(self, task_id: str):
        bus = self._app.state.event_bus
        snapshot = await self._snapshot(task_id)
        question = await self._pending_question(snapshot)
        mapper = TaskStreamMapper(snapshot)
        seen = snapshot.last_seq
        subscription = bus.subscribe(task_id)
        try:
            yield StreamResponse(
                task=snapshot_to_task(snapshot, question=question)
            )
            if snapshot.task.status in (
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELED,
            ):
                return
            for event in await self._app.state.event_store.replay(task_id, seen):
                if mapper.terminal:
                    return
                seen = event.seq
                for response in mapper.map_event(await self._enrich(event)):
                    yield response
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                for response in mapper.map_event(await self._enrich(event)):
                    yield response
                if mapper.terminal:
                    return
        finally:
            subscription.close()
            bus.unsubscribe(subscription)

    async def on_message_send_stream(
        self, params: SendMessageRequest, context: ServerCallContext
    ):
        result = await self._submit(params)
        if isinstance(result, Message):
            yield StreamResponse(message=result)
            return
        async for response in self._stream_task(result.id):
            yield response

    async def on_subscribe_to_task(
        self, params: SubscribeToTaskRequest, context: ServerCallContext
    ):
        try:
            await self._snapshot(params.id)
        except TaskNotFound as exc:
            raise TaskNotFoundError(f"task not found: {params.id}") from exc
        async for response in self._stream_task(params.id):
            yield response
```

注意：SDK 的流式路由**不会 await** 而是直接 `anext()`（`jsonrpc_dispatcher.py:363-370`），因此这两个方法必须保持"async generator"形态；生成器首个 yield 之前的异常会在 SDK 的 eager `anext` 中被映射为 JSON-RPC error（这正是 `on_subscribe_to_task` 先校验存在性的原因）。

`TERMINAL_A2A_STATES` 仅在 mapping 内部使用（`_apply_state`），handler 不需要导入；`_stream_task` 的终止判断用 `mapper.terminal`。

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_stream.py -q`
Expected: PASS（3 passed）

- [ ] **Step 5: Lint + 提交**

```bash
uv run ruff check .
git add src/choirworks/a2a/server.py tests/support/hubs.py tests/integration/test_a2a_stream.py
git commit -m "feat(a2a): 流式 SendStreamingMessage 与 SubscribeToTask"
```

---

### Task 7: 双实例嵌套验收（ChoirWorks as subagent）

**Files:**
- Test: `tests/integration/test_a2a_nested.py`

- [ ] **Step 1: 写测试**

`tests/integration/test_a2a_nested.py`：

```python
import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import Message, Part, Role, SendMessageRequest

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub


def _settings(port: int, db: str) -> Settings:
    return Settings(
        store={"db_path": db},
        a2a={"public_url": f"http://127.0.0.1:{port}"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _inner_plan() -> PlanDraft:
    return PlanDraft(
        rationale="inner",
        nodes=[
            PlanNodeDraft(
                id="n1", name="echo", agent_name="echo", input={"text": "hi"}
            )
        ],
    )


async def _connect(base_url: str):
    http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    card = await A2ACardResolver(httpx_client=http, base_url=base_url).get_agent_card()
    client = await create_client(
        agent=card, client_config=ClientConfig(streaming=False, httpx_client=http)
    )
    return http, client


@pytest.fixture
async def hubs(tmp_path, echo_agent):
    async with start_hub(
        lambda port: _settings(port, str(tmp_path / "inner.db")),
        llm=FakeLLM(structured_results=[_inner_plan()]),
    ) as (inner_app, inner_url):
        await inner_app.state.registry.register("echo", echo_agent.url)
        async with start_hub(
            lambda port: _settings(port, str(tmp_path / "outer.db"))
        ) as (outer_app, outer_url):
            http, client = await _connect(outer_url)
            try:
                resp = await http.post(
                    "/v1/agents", json={"name": "inner", "card_url": inner_url}
                )
                assert resp.status_code == 201, resp.text
                yield outer_app, http, client, inner_app, inner_url
            finally:
                await client.close()
                await http.aclose()


async def test_outer_dispatches_task_to_inner_instance(hubs):
    _outer_app, http, client, inner_app, _inner_url = hubs
    responses = [
        response
        async for response in client.send_message(
            SendMessageRequest(
                message=Message(
                    message_id="m-1",
                    role=Role.ROLE_USER,
                    parts=[Part(text="@inner 请处理这个请求")],
                )
            )
        )
    ]
    assert responses[-1].WhichOneof("payload") == "task"
    outer_task = responses[-1].task
    assert outer_task.context_id
    for _ in range(400):
        snapshot = (await http.get(f"/v1/tasks/{outer_task.id}")).json()
        if snapshot["task"]["status"] in {"completed", "failed"}:
            break
        await asyncio.sleep(0.05)
    assert snapshot["task"]["status"] == "completed", snapshot
    artifacts = [
        node["output"]["artifacts"]
        for node in snapshot["nodes"]
        if node.get("output")
    ]
    assert artifacts
    assert artifacts[0][0]["text"]

    cursor = await inner_app.state.db.conn.execute("SELECT COUNT(*) FROM tasks")
    assert (await cursor.fetchone())[0] == 1

    timeline = (
        await http.get(f"/v1/conversations/{outer_task.context_id}/messages")
    ).json()
    senders = {message["sender"] for message in timeline["messages"]}
    assert "inner" in senders or any(
        message["role"] == "agent" for message in timeline["messages"]
    )
```

说明：最后一个断言允许两种叙事（`announce_dispatch` 会产生 `已派发 @inner` 的 assistant 播报；agent 产出后也会入群），核心断言是外层 task completed 且节点产物非空。

- [ ] **Step 2: 跑测试确认通过**

Run: `uv run pytest tests/integration/test_a2a_nested.py -q`
Expected: PASS

若失败，排查顺序：外层注册 card 是否成功（201）→ 外层节点状态（GET task）→ 内层是否收到 A2A 调用（内层日志/任务）→ 内层 FakeLLM 是否被调用（`structured_results` 应恰好 1 次）。

- [ ] **Step 3: Lint + 提交**

```bash
uv run ruff check .
git add tests/integration/test_a2a_nested.py
git commit -m "test(a2a): 双实例嵌套端到端验收"
```

---

### Task 8: README 与全量验证

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README 增加 A2A 北向接口章节**

在 README 的「工作群」章节之后追加：

````markdown
## A2A 北向接口（AgentCard + JSON-RPC）

ChoirWorks 自身是一个标准 A2A v1.0 Server，可被任意 A2A client（含另一个 ChoirWorks 实例）当作 agent 编排。

- AgentCard：`GET /.well-known/agent-card.json`
- JSON-RPC：`POST /v1/a2a`，方法：`SendMessage` / `SendStreamingMessage` / `GetTask` / `CancelTask` / `SubscribeToTask`
- 公共地址由 `a2a.public_url`（env `CHOIRWORKS_A2A__PUBLIC_URL`）配置
- task 级嵌套：外层把内层实例的 AgentCard 地址注册为 agent 即可派发任务

```bash
curl -s localhost:8080/.well-known/agent-card.json | python -m json.tool
curl -s -X POST localhost:8080/v1/a2a -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"@researcher 请分析 X"}]}}}'
```

> 当前仅支持 v1.0（未开启 v0.3 兼容）；`ListTasks` 与 push notification 返回不支持。
````

- [ ] **Step 2: 全量验证**

Run: `uv run pytest -p no:warnings -q`
Expected: 原有 162 + 新增约 30 用例全部 PASS

Run: `uv run ruff check .`
Expected: All checks passed!

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: A2A 北向接口说明（M11）"
```

---

## Self-Review 记录

- **Spec 覆盖**：§3 事实基线→实现依据；§4 架构→Task 1/4/6；§5.1–5.4 映射→Task 2/3；§5.5 订阅→Task 6；§5.6 发送→Task 5；§7 错误/背压→Task 4/5/6（背压沿用 EventSubscription 关闭语义）；§9 M11 验收（SDK 集成 + 嵌套）→Task 6/7；§10 测试→各 Task；§12 样例与实现一致。房间（§6）与前端（§8）属 M12/M13，不在本计划。
- **已知偏离**：`plan.extended` 的事件 payload 只有增量，由 handler `_enrich` 补当前完整 `dag` 后再映射，保持 spec 的"全量替换"语义。
- **类型一致性**：`TaskStreamMapper`、`snapshot_to_task`、`room_message_to_a2a`、`cancel_task`/`TaskNotCancelable`、`start_hub`、`HubA2AHandler` 命名在任务间一致。
