# A2A 编排平台 MVP — Plan 3：HITL 人工介入

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当远程 subagent 进入 `input-required`（或节点需审批/规划需澄清）时，按策略链 `auto_llm → peer_agent → human` 处置：自动回答、路由给其他 agent、或转人工等待 REST 答复后续跑；支持超时策略。

**Architecture:** 新增 `PolicyEngine` 解析策略；干预（intervention）事件化持久化；Orchestrator 在调度循环中处理 parked 节点：无策略直接创建干预、`auto_llm` 用 LLM 回答、`peer_agent` 派发一次性 A2A 子任务、`human` 停车等待 API；答复后由 Orchestrator 通过 `continue_node` 以同一远程 task id 追加消息续跑。超时由 watcher 任务按 `on_timeout` 处理。

**Tech Stack:** 现有代码 + 配置驱动策略。

**Spec:** `docs/.../2026-09-12-a2a-orchestration-platform-design.md`（§5.2）

**前置：** Plan 2 完成（61 tests 全绿）。

**关键约定：**
- 节点新增列 `agent_name`、`policy_override`（`Database.initialize` 自动 ALTER 兼容旧库）；计划 DAG 节点包含 `agent_name`。
- 策略优先级：`node.policy_override` > 配置 overrides（按声明顺序，agent_name/skill_id 任一匹配）> 任务级 policy > 全局 default；取值仅允许 `auto_llm|peer_agent|human`。
- 干预状态：`pending → resolved | expired | invalidated`；`expired` 用于超时降级后作废人工等待。
- `responder`：`auto_llm` | `agent_url` | `user`。
- 超时 `on_timeout`：`escalate`（重发提醒并继续等待）| `auto`（降级 auto_llm）| `fail`（节点失败）。

---

## 文件结构（Plan 3 增量）

```
src/agent_hub/
  config.py                    # +PolicyConfig/policies
  core/policy.py               # PolicyEngine
  core/orchestrator.py         # +intervention 处理、continue、超时 watcher
  core/dispatcher.py           # +continue_node（同 remote task id 追加消息）
  core/tasks.py                # +answer_intervention 辅助？(放 API/service)
  store/db.py                  # +nodes 新列 + 迁移
  store/projections.py         # +intervention 投影/fetch、dag agent_name
  api/schemas.py               # +AnswerInterventionIn
  api/interventions.py         # GET/POST interventions
  api/app.py                   # include router、装配 PolicyEngine
tests/
  unit/test_policy.py          # 策略解析
  integration/test_hitl.py     # auto/human/peer/timeout 全链路
```

---

### Task 1: 策略配置与引擎

**Files:**
- Modify: `src/agent_hub/config.py`、`config.example.yaml`
- Create: `src/agent_hub/core/policy.py`
- Test: `tests/unit/test_policy.py`

- [ ] **Step 1: 写失败测试 `tests/unit/test_policy.py`**

```python
import pytest

from agent_hub.config import PolicyConfig, PolicyOverride
from agent_hub.core.policy import POLICY_VALUES, PolicyEngine


def engine() -> PolicyEngine:
    return PolicyEngine(
        PolicyConfig(
            default="auto_llm",
            overrides=[
                PolicyOverride(agent_name="payment", policy="human"),
                PolicyOverride(skill_id="deploy_prod", policy="human"),
            ],
        )
    )


def test_node_override_wins():
    assert engine().resolve(node_override="peer_agent", agent_name="payment", skill_id=None) == "peer_agent"


def test_agent_override():
    assert engine().resolve(node_override=None, agent_name="payment", skill_id=None) == "human"


def test_skill_override():
    assert engine().resolve(node_override=None, agent_name="x", skill_id="deploy_prod") == "human"


def test_task_policy_then_default():
    assert engine().resolve(None, "x", None, task_policy="human") == "human"
    assert engine().resolve(None, "x", None) == "auto_llm"


def test_invalid_policy_rejected():
    with pytest.raises(ValueError):
        engine().resolve(node_override="banana", agent_name="x", skill_id=None)


def test_policy_values_exported():
    assert POLICY_VALUES == ("auto_llm", "peer_agent", "human")
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_policy.py -p no:warnings -q`
Expected: FAIL，`ImportError: cannot import name 'PolicyConfig'`

- [ ] **Step 3: 修改 `src/agent_hub/config.py`**

```python
class PolicyOverride(BaseModel):
    agent_name: str | None = None
    skill_id: str | None = None
    policy: str


class PolicyConfig(BaseModel):
    default: str = "auto_llm"
    on_timeout: str = "escalate"
    timeout_seconds: float = 900.0
    overrides: list[PolicyOverride] = Field(default_factory=list)


class Settings(BaseSettings):
    ...
    policies: PolicyConfig = Field(default_factory=PolicyConfig)
```

同步 `config.example.yaml`：

```yaml
policies:
  default: auto_llm
  on_timeout: escalate
  timeout_seconds: 900
  overrides:
    - agent_name: payment-agent
      policy: human
    - skill_id: deploy_prod
      policy: human
```

- [ ] **Step 4: 实现 `src/agent_hub/core/policy.py`**

```python
from __future__ import annotations

from agent_hub.config import PolicyConfig

POLICY_VALUES = ("auto_llm", "peer_agent", "human")


class PolicyEngine:
    def __init__(self, config: PolicyConfig):
        self._config = config
        self._validate(config.default)
        self._validate(config.on_timeout, allowed=("escalate", "auto", "fail"))
        for override in config.overrides:
            self._validate(override.policy)
            if override.agent_name is None and override.skill_id is None:
                raise ValueError("policy override requires agent_name or skill_id")

    def resolve(
        self,
        node_override: str | None,
        agent_name: str | None,
        skill_id: str | None,
        task_policy: str | None = None,
    ) -> str:
        if node_override is not None:
            self._validate(node_override)
            return node_override
        for override in self._config.overrides:
            if override.agent_name is not None and override.agent_name == agent_name:
                return override.policy
            if override.skill_id is not None and override.skill_id == skill_id:
                return override.policy
        if task_policy is not None:
            self._validate(task_policy)
            return task_policy
        return self._config.default

    @property
    def config(self) -> PolicyConfig:
        return self._config

    @staticmethod
    def _validate(policy: str, allowed: tuple[str, ...] = POLICY_VALUES) -> None:
        if policy not in allowed:
            raise ValueError(f"invalid policy: {policy}")
```

- [ ] **Step 5: 运行通过并提交**

Run: `uv run pytest tests/unit/test_policy.py -p no:warnings -q` → `8 passed`

```bash
git add src/agent_hub/config.py config.example.yaml src/agent_hub/core/policy.py tests/unit/test_policy.py
git commit -m "feat: HITL 策略配置与 PolicyEngine"
```

---

### Task 2: 干预投影与节点新列

**Files:**
- Modify: `src/agent_hub/store/db.py`（迁移 + 新列）、`src/agent_hub/store/projections.py`、`src/agent_hub/core/planner.py`（dag 带 agent_name）
- Test: `tests/unit/test_interventions_projection.py`

- [ ] **Step 1: 写失败测试**

```python
from datetime import UTC, datetime

from agent_hub.models.domain import Intervention
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from agent_hub.models.enums import EventType, InterventionStatus


async def test_intervention_lifecycle_projection(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append("t1", EventType.INTERVENTION_REQUESTED, {
            "intervention_id": "iv1",
            "node_id": "p1:n1",
            "source": "remote_input_required",
            "policy": "human",
            "question": {"text": "who?"},
            "responder": None,
            "deadline_at": datetime.now(UTC).isoformat(),
        })
        pending = await projections.fetch_interventions(db, "t1", InterventionStatus.PENDING)
        assert len(pending) == 1
        assert pending[0].question == {"text": "who?"}

        await store.append("t1", EventType.INTERVENTION_RESOLVED, {
            "intervention_id": "iv1",
            "answer": {"text": "Bob"},
            "responder": "user",
        })
        resolved = await projections.fetch_intervention(db, "iv1")
        assert resolved is not None
        assert resolved.status is InterventionStatus.RESOLVED
        assert resolved.answer == {"text": "Bob"}
        assert resolved.responder == "user"
    finally:
        await db.close()


async def test_nodes_store_agent_name_and_policy_override(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append("t1", EventType.TASK_CREATED, {"request": "x", "policy": None})
        await store.append("t1", EventType.PLAN_CREATED, {
            "plan_id": "p1",
            "version": 1,
            "rationale": "x",
            "dag": {"nodes": [{
                "id": "n1", "name": "step", "agent_url": "http://a",
                "agent_name": "helper", "skill_id": "s", "deps": [],
                "input": {}, "requires_approval": False,
                "policy_override": "human",
            }]},
        })
        node = await projections.fetch_node(db, "p1:n1")
        assert node is not None
        assert node.agent_name == "helper"
        assert node.policy_override == "human"
    finally:
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_interventions_projection.py -p no:warnings -q`
Expected: FAIL（`EventType.INTERVENTION_REQUESTED` 存在但投影未处理；`Node.agent_name` 不存在）

- [ ] **Step 3: 实现**

`store/db.py`：`initialize()` 在 `executescript` 后执行迁移：

```python
async def _migrate(self, conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("PRAGMA table_info(nodes)")
    columns = {row["name"] for row in await cursor.fetchall()}
    for name, ddl in (
        ("agent_name", "ALTER TABLE nodes ADD COLUMN agent_name TEXT"),
        ("policy_override", "ALTER TABLE nodes ADD COLUMN policy_override TEXT"),
    ):
        if name not in columns:
            await conn.execute(ddl)
    await conn.commit()
```

并在 `initialize()` 末尾调用 `await self._migrate(self.conn)`。

`models/domain.py` 的 `Node` 增加 `agent_name: str | None = None`、`policy_override: str | None = None`。

`store/projections.py`：
- `_materialize_nodes` INSERT 增加 `agent_name`、`policy_override` 两列（来自 dag node）。
- `_row_to_node` 读取两列。
- 新增投影分支：

```python
    elif event_type is EventType.INTERVENTION_REQUESTED:
        await conn.execute(
            "INSERT INTO interventions"
            " (id, task_id, node_id, source, policy, question, responder, status,"
            "  deadline_at, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payload["intervention_id"], event.task_id, payload.get("node_id"),
                payload["source"], payload["policy"],
                json.dumps(payload.get("question"), ensure_ascii=False),
                payload.get("responder"), InterventionStatus.PENDING.value,
                payload.get("deadline_at"), ts,
            ),
        )
    elif event_type is EventType.INTERVENTION_RESOLVED:
        await conn.execute(
            "UPDATE interventions SET status = ?, answer = ?, responder = ?,"
            " resolved_at = ? WHERE id = ?",
            (
                InterventionStatus.RESOLVED.value,
                json.dumps(payload.get("answer"), ensure_ascii=False),
                payload.get("responder"), ts, payload["intervention_id"],
            ),
        )
```

- 新增读取函数 `fetch_intervention(db, intervention_id)`、`fetch_interventions(db, task_id, status=None)`。

`core/planner.py` 的 `draft_to_dag` node dict 增加 `"agent_name": node.agent_name`。

- [ ] **Step 4: 运行通过并提交**

Run: `uv run pytest tests/unit/test_interventions_projection.py -p no:warnings -q` → `2 passed`

```bash
git add src/agent_hub/store/db.py src/agent_hub/store/projections.py src/agent_hub/models/domain.py src/agent_hub/core/planner.py tests/unit/test_interventions_projection.py
git commit -m "feat: 干预投影与节点 agent_name/policy_override 列"
```

---

### Task 3: 派发器续跑（continue_node）

**Files:**
- Modify: `src/agent_hub/core/dispatcher.py`
- Test: `tests/integration/test_dispatcher.py`（追加）

- [ ] **Step 1: 追加失败测试**

```python
async def test_continue_node_after_input_required(tmp_path):
    agent = await start_fake_agent("ask")
    db, remote, events, tasks, dispatcher = await setup(tmp_path, agent)
    try:
        created = await tasks.create_task("ask", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.INPUT_REQUIRED
        assert node.a2a_task_id

        node = await dispatcher.continue_node(created.task_id, node.id, "Bob")
        assert node.status is NodeStatus.COMPLETED
        assert node.output["artifacts"][0]["text"] == "answered:Bob"
    finally:
        await remote.close()
        await db.close()
        await agent.stop()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_dispatcher.py -p no:warnings -q`
Expected: FAIL，`'NodeDispatcher' object has no attribute 'continue_node'`

- [ ] **Step 3: 实现**

`continue_node`：节点必须为 `INPUT_REQUIRED` 且有 `a2a_task_id`；转 `WORKING`，以 `task_id=node.a2a_task_id, context_id=node.a2a_context_id` 发送文本；复用流消费逻辑。将 `dispatch_node` 的流消费部分重构为 `_consume(node, chunks, artifacts)`（行为不变）：

```python
    async def continue_node(self, task_id: str, node_id: str, text: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status is not NodeStatus.INPUT_REQUIRED:
            raise InvalidNodeState(f"node {node_id} is {node.status.value}, cannot continue")
        if not node.a2a_task_id:
            raise InvalidNodeState(f"node {node_id} has no remote task id")

        await self._transition(node, NodeStatus.WORKING)
        artifacts: list[dict[str, Any]] = []
        current = NodeStatus.WORKING
        try:
            async with asyncio.timeout(self._timeout):
                chunks = self._remote.send_text(
                    node.agent_url or "",
                    text,
                    task_id=node.a2a_task_id,
                    context_id=node.a2a_context_id,
                    message_id=f"{task_id}:{node_id}:continue:{node.attempt}",
                )
                current = await self._consume(node, chunks, artifacts)
            if current is NodeStatus.COMPLETED:
                await self._events.append(
                    task_id,
                    EventType.NODE_OUTPUT,
                    {"node_id": node_id, "output": {"artifacts": artifacts}},
                )
            elif current not in (NodeStatus.INPUT_REQUIRED, NodeStatus.FAILED, NodeStatus.CANCELED):
                await self._fail(node, "remote stream ended without terminal state")
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001
            await self._fail(node, str(exc))
        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed
```

`_consume(node, chunks, artifacts) -> NodeStatus`：抽取自 `dispatch_node` 中的 `async for chunk ...` 主体；`continue_node` 不重复发 `node.dispatched`（已有 a2a id），仅在收到 task chunk 且节点无 id 时补发。

- [ ] **Step 4: 运行通过并提交**

Run: `uv run pytest tests/integration/test_dispatcher.py -p no:warnings -q` → `6 passed`

```bash
git add src/agent_hub/core/dispatcher.py tests/integration/test_dispatcher.py
git commit -m "feat: 派发器 continue_node（同远程任务追加消息续跑）"
```

---

### Task 4: Orchestrator 介入处理与超时

**Files:**
- Modify: `src/agent_hub/core/orchestrator.py`
- Test: `tests/integration/test_hitl.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_hitl.py`**

```python
import asyncio
from datetime import UTC, datetime, timedelta

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.config import PolicyConfig, PolicyOverride
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import PlanDraft, PlanNodeDraft, Planner
from agent_hub.core.policy import PolicyEngine
from agent_hub.core.tasks import TaskService
from agent_hub.models.enums import EventType, InterventionStatus, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent
from tests.support.fakes import FakeLLM


def plan_ask() -> PlanDraft:
    return PlanDraft(
        rationale="ask",
        nodes=[
            PlanNodeDraft(id="n1", name="n1", agent_name="asker", input={"text": "ask"})
        ],
    )


async def setup_hitl(tmp_path, agents, policies, structured=None, text=None):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    for name, agent in agents.items():
        await registry.register(name, agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(structured_results=structured, text_results=text)
    orchestrator = Orchestrator(
        db, events, Planner(llm, registry), dispatcher, tasks,
        policy_engine=PolicyEngine(policies), retry_backoff_seconds=0.0,
    )
    return db, remote, events, tasks, orchestrator, llm


async def test_auto_llm_answers_input_required(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path, {"asker": asker},
        PolicyConfig(default="auto_llm"),
        structured=[plan_ask()], text=["Bob"],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        interventions = await projections.fetch_interventions(db, task_id)
        assert len(interventions) == 1
        assert interventions[0].status is InterventionStatus.RESOLVED
        assert interventions[0].responder == "auto_llm"
        types = [e.type for e in await events.replay(task_id)]
        assert EventType.INTERVENTION_REQUESTED in types
        assert EventType.INTERVENTION_RESOLVED in types
    finally:
        await orchestrator.stop(); await remote.close(); await db.close(); await asker.stop()


async def test_human_intervention_via_api(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path, {"asker": asker},
        PolicyConfig(default="human", timeout_seconds=30),
        structured=[plan_ask()],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.AWAITING_INPUT
        pending = await projections.fetch_interventions(
            db, task_id, InterventionStatus.PENDING
        )
        assert len(pending) == 1

        await orchestrator.answer_intervention(pending[0].id, "Bob", responder="user")
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        resolved = await projections.fetch_intervention(db, pending[0].id)
        assert resolved is not None and resolved.responder == "user"
    finally:
        await orchestrator.stop(); await remote.close(); await db.close(); await asker.stop()


async def test_peer_agent_answers(tmp_path):
    asker = await start_fake_agent("ask")
    helper = await start_fake_agent("echo")
    from agent_hub.core.orchestrator import PeerChoice

    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path, {"asker": asker, "helper": helper},
        PolicyConfig(default="peer_agent"),
        structured=[plan_ask(), PeerChoice(agent_name="helper", instruction="lookup name")],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        text = snapshot.nodes[0].output["artifacts"][0]["text"]
        assert text.startswith("answered:")
        assert "echo:lookup name" in text
        interventions = await projections.fetch_interventions(db, task_id)
        assert interventions[0].responder == helper.url
    finally:
        await orchestrator.stop(); await remote.close(); await db.close()
        await asker.stop(); await helper.stop()


async def test_timeout_auto_fallback(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path, {"asker": asker},
        PolicyConfig(default="human", on_timeout="auto", timeout_seconds=0.2),
        structured=[plan_ask()], text=["Bob"],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        interventions = await projections.fetch_interventions(db, task_id)
        assert any(i.responder == "auto_llm" for i in interventions)
    finally:
        await orchestrator.stop(); await remote.close(); await db.close(); await asker.stop()


async def test_policy_override_forces_human(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path, {"asker": asker},
        PolicyConfig(
            default="auto_llm", timeout_seconds=30,
            overrides=[PolicyOverride(agent_name="asker", policy="human")],
        ),
        structured=[plan_ask()], text=["Bob"],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.AWAITING_INPUT
    finally:
        await orchestrator.stop(); await remote.close(); await db.close(); await asker.stop()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_hitl.py -p no:warnings -q`
Expected: FAIL，`Orchestrator.__init__() got an unexpected keyword argument 'policy_engine'`

- [ ] **Step 3: 实现 `core/orchestrator.py` 增量**

关键结构：

```python
class PeerChoice(BaseModel):
    agent_name: str
    instruction: str


class Orchestrator:
    def __init__(..., policy_engine: PolicyEngine | None = None):
        self._policy = policy_engine or PolicyEngine(PolicyConfig())
        self._timeout_tasks: dict[str, asyncio.Task] = {}
```

循环中 parked 分支替换为 `await self._process_interventions(task, parked)`；无干预可处理的 parked 节点返回停车。

```python
    async def _process_interventions(self, task, parked):
        made_progress = False
        for node in parked:
            pending = await projections.fetch_interventions_for_node(self._db, node.id)
            open_iv = next((i for i in pending if i.status is InterventionStatus.PENDING), None)
            resolved_iv = next(
                (i for i in pending if i.status is InterventionStatus.RESOLVED
                 and i.answer is not None), None
            )
            if resolved_iv is not None:
                text = str((resolved_iv.answer or {}).get("text", ""))
                await self._inflight_add(self._dispatcher.continue_node(task.id, node.id, text))
                made_progress = True
                continue
            if open_iv is not None:
                continue
            policy = self._policy.resolve(
                node.policy_override, node.agent_name, node.skill_id
            )
            await self._create_intervention(task, node, policy)
            made_progress = True
        if made_progress:
            return
        if task.status is not TaskStatus.AWAITING_INPUT:
            await self._events.append(task.id, EventType.TASK_STATE_CHANGED, {
                "from": TaskStatus.RUNNING.value, "to": TaskStatus.AWAITING_INPUT.value})
        # 超时 watcher 在 _create_intervention 时已安排
```

`_create_intervention`：写 `intervention.requested`（含 deadline）后按策略处置：
- `auto_llm`：取问题文本，`answer = await self._assist_text(task, node, question, source="auto_llm")`，写 `intervention.resolved`（responder=`auto_llm`），不在此处续跑（下一轮循环处理 resolved 分支）。
- `peer_agent`：`choice = await llm.structured(schema=PeerChoice, ...)`；发一次性 A2A 任务收集文本；写 resolved（responder=peer url）。
- `human`：安排超时 watcher；不写 resolved。

`_arm_timeout(task_id, intervention_id)`：

```python
    async def _timeout_watcher(self, task_id: str, intervention_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            intervention = await projections.fetch_intervention(self._db, intervention_id)
            if intervention is None or intervention.status is not InterventionStatus.PENDING:
                return
            mode = self._policy.config.on_timeout
            if mode == "auto":
                node = await projections.fetch_node(self._db, intervention.node_id or "")
                if node is None:
                    return
                answer = await self._assist_text(task_id, node, intervention.question)
                await self._events.append(task_id, EventType.INTERVENTION_RESOLVED, {
                    "intervention_id": intervention_id,
                    "answer": {"text": answer}, "responder": "auto_llm",
                })
            elif mode == "fail":
                node = await projections.fetch_node(self._db, intervention.node_id or "")
                if node is not None:
                    await self._events.append(task_id, EventType.NODE_STATE_CHANGED, {
                        "node_id": node.id, "from": node.status.value,
                        "to": NodeStatus.FAILED.value})
                    await self._events.append(task_id, EventType.TASK_FAILED, {})
            else:  # escalate
                await self._events.append(task_id, EventType.INTERVENTION_REQUESTED, {
                    "intervention_id": intervention_id + ":escalated",
                    "node_id": intervention.node_id,
                    "source": intervention.source, "policy": intervention.policy,
                    "question": intervention.question, "responder": None,
                    "deadline_at": intervention.deadline_at.isoformat()
                    if intervention.deadline_at else None,
                })
            self.start(task_id)
        except asyncio.CancelledError:
            raise
        finally:
            self._timeout_tasks.pop(intervention_id, None)
```

`answer_intervention(intervention_id, text, responder="user")`：校验 pending → 写 resolved → `self.start(intervention.task_id)`；若节点已被超时接管则抛 `InvalidNodeState`。

`_assist_text`：用 `LLMClient.text`（assist 模型在装配时传入的同一个 llm）生成答复；peer 流程用 `RemoteAgentClient.send_text` 收集 `artifact_update`/最终 task 的文本。

`stop()` 同时取消 `_timeout_tasks`。

- [ ] **Step 4: 运行通过**

Run: `uv run pytest tests/integration/test_hitl.py -p no:warnings -q` → `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/core/orchestrator.py tests/integration/test_hitl.py
git commit -m "feat: HITL 策略处置（auto/peer/human）与超时降级"
```

---

### Task 5: 干预 REST API 与装配

**Files:**
- Create: `src/agent_hub/api/interventions.py`
- Modify: `src/agent_hub/api/schemas.py`、`src/agent_hub/api/app.py`
- Test: `tests/integration/test_api.py`（追加）

- [ ] **Step 1: 追加失败测试**

```python
from agent_hub.config import PolicyConfig


@pytest.fixture
async def api_human(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="ask",
        nodes=[PlanNodeDraft(id="n1", name="n1", agent_name="echo", input={"text": "ask"})],
    )
    llm = FakeLLM(structured_results=[plan])
    settings = Settings(
        store={"db_path": tmp_path / "human.db"},
        scheduler={"retry_backoff_seconds": 0.0},
        policies={"default": "human", "timeout_seconds": 30},
    )
    app = create_app(settings, llm=llm)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, app, echo_agent.url


async def test_human_intervention_endpoints(api_human):
    client, _, agent_url = api_human
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (await client.post("/v1/tasks", json={"request": "ask"})).json()

    for _ in range(100):
        resp = await client.get(f"/v1/tasks/{created['task_id']}/interventions?status=pending")
        items = resp.json()
        if items:
            break
        await asyncio.sleep(0.05)
    assert items
    intervention_id = items[0]["id"]

    resp = await client.post(
        f"/v1/tasks/{created['task_id']}/interventions/{intervention_id}",
        json={"text": "Bob"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "resolved"

    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{created['task_id']}")).json()
        if snapshot["task"]["status"] == "completed":
            break
        await asyncio.sleep(0.05)
    assert snapshot["nodes"][0]["output"]["artifacts"][0]["text"] == "answered:Bob"
```

- [ ] **Step 2: 运行确认失败**（404 / 422）

- [ ] **Step 3: 实现**

`api/schemas.py`：

```python
class AnswerInterventionIn(BaseModel):
    text: str
    responder: str = "user"
```

`api/interventions.py`：

```python
@router.get("/tasks/{task_id}/interventions", response_model=list[Intervention])
async def list_interventions(task_id: str, request: Request, status: InterventionStatus | None = None):
    try:
        await request.app.state.task_service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc
    return await projections.fetch_interventions(request.app.state.db, task_id, status)

@router.post("/tasks/{task_id}/interventions/{intervention_id}", response_model=Intervention)
async def answer_intervention(task_id: str, intervention_id: str, body: AnswerInterventionIn, request: Request):
    try:
        await request.app.state.orchestrator.answer_intervention(intervention_id, body.text, responder=body.responder)
    except (KeyError, InvalidNodeState) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    intervention = await projections.fetch_intervention(request.app.state.db, intervention_id)
    assert intervention is not None
    return intervention
```

`app.py`：`include_router(interventions.router, prefix="/v1")`；装配 `PolicyEngine(resolved.policies)` 传入 Orchestrator。

- [ ] **Step 4: 运行通过并提交**

Run: `uv run pytest tests/integration/test_api.py -p no:warnings -q` → `6 passed`

```bash
git add src/agent_hub/api/ tests/integration/test_api.py
git commit -m "feat: 干预 REST API（列表/答复）与 PolicyEngine 装配"
```

---

### Task 6: 回归、README 与收尾

- [ ] **Step 1: 全量测试与 lint**

Run: `uv run pytest -p no:warnings -q` → 全部通过（约 70 用例）
Run: `uv run ruff check .` → 全绿（修复告警后重跑）

- [ ] **Step 2: README 更新**

当前进度改为 **M1–M4**；补充人工介入流程示例（GET pending → POST answer）。

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: M4 HITL README 更新"
```

---

## Plan 3 完成标准（对照 spec M4）

- [ ] `auto_llm` 自动回答并续跑（有测试）。
- [ ] `human` 停车、REST 答复后以同一远程任务续跑完成（有测试）。
- [ ] `peer_agent` 路由第二个 agent 并把结果回送（有测试）。
- [ ] 策略优先级：节点 > 配置 overrides > 任务 > 默认（单元测试）。
- [ ] 超时 `on_timeout=auto` 降级完成；`fail` 标记失败；`escalate` 重发提醒（auto 有集成测试）。
- [ ] 干预全流程事件化（requested/resolved）且可查询。
- [ ] 全量测试与 ruff 全绿。

---

## 执行勘误（2026-09-12）

1. **escalate 语义简化**：超时 `escalate` 不重发 `intervention.requested`（避免重复插入），改为追加 `error` 审计提醒并重新计时。
2. **Orchestrator 依赖注入**：新增 `registry/remote/llm/policy_engine` 参数；测试与 `create_app` 均显式传入。
3. **`wait` 增加 `until_terminal` 参数**：人工答复后需等待任务离开 `awaiting_input` 并到终态。
4. **`start()` 补启动机制**：旧 run 尚未退出时的 `start()` 会记入 `_restart_requested`，当前 run 结束后自动再启动，避免答复后调度停摆。
5. **重试竞态修复**（dispatcher）：重试派发时节点残留的 `a2a_task_id` 曾导致 `node.dispatched` 被跳过、首包状态映射非法；现在除 `continue_node` 外总是宣告 `node.dispatched` 并覆盖远程任务标识。
6. **规划前需求澄清**（spec 触发源 3）未实现，延后到后续计划；当前覆盖 `input-required` 与 `requires_approval` 两个触发源。
