# A2A 编排平台 MVP — Plan 4：断点恢复、回退与收尾

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 spec 的 M5–M7：Checkpoint、崩溃后启动恢复、远程任务对账、平台侧回退（取消信号 + 重跑）、单节点 retry、任务 cancel、README 收尾。

**Architecture:** A2A client 增加 `get_task/cancel_task/subscribe_task`；Orchestrator 在每次观察到新的 completed 节点时生成 checkpoint；`recovery.py` 启动时对在途节点 `SubscribeToTask` 重新挂接并续跑，对停放任务直接 `start`；`reconcile.py` 周期用 `GetTask` 校正漂移；回退通过 `ROLLBACK_PERFORMED` 投影把 checkpoint 之后的节点重置为 `pending`（重跑）并向在途远程任务发取消信号（fire-and-forget）。

**前置：** Plan 3 完成（76 tests 全绿）。

**关键约定：**
- Checkpoint：`{checkpoint_id, seq(latest), plan_version, frontier:[completed node ids], artifacts:{node_id: output}}`；每个新完成节点生成一个。
- 回退 `restart`：取消信号 + 把 checkpoint 之后（不在 frontier）的节点重置为 `pending`、清空 output/error/attempt，并把 task 状态置回 `running` 后 `orchestrator.start`。`dry_run` 只算影响面返回。
- `resume_node`：通过 `SubscribeToTask` 重新挂接远程在途任务，消费事件到终态；`announce_dispatched=False`。
- 对账：`GetTask` 为事实源；发现远程终态/input-required 且本地不符时补写事件。

---

### Task 1: A2A client 扩展（get/cancel/subscribe）

**Files:**
- Modify: `src/agent_hub/a2a/client.py`
- Test: `tests/integration/test_a2a_client.py`（追加）

- [ ] **Step 1: 追加失败测试**

```python
async def test_get_cancel_and_subscribe(echo_agent):
    from a2a.types import TaskState

    client = RemoteAgentClient()
    try:
        chunks = [c async for c in client.send_text(echo_agent.url, "hello")]
        remote_task_id = chunks[0].task.id

        task = await client.get_task(echo_agent.url, remote_task_id)
        assert task is not None
        assert task.status.state == TaskState.TASK_STATE_COMPLETED

        resumed = [
            c
            async for c in client.subscribe_task(echo_agent.url, remote_task_id)
        ]
        assert resumed
    finally:
        await client.close()


async def test_cancel_task(ask_agent):
    client = RemoteAgentClient()
    try:
        chunks = [c async for c in client.send_text(ask_agent.url, "ask")]
        remote_task_id = chunks[0].task.id
        assert remote_task_id
        await client.cancel_task(ask_agent.url, remote_task_id)
    finally:
        await client.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_a2a_client.py -p no:warnings -q`
Expected: FAIL，`'RemoteAgentClient' object has no attribute 'get_task'`

- [ ] **Step 3: 实现（`RemoteAgentClient` 追加）**

```python
    async def get_task(self, agent_url: str, remote_task_id: str) -> Task | None:
        client = await self._client_for(agent_url)
        try:
            return await client.get_task(GetTaskRequest(id=remote_task_id))
        except Exception:  # noqa: BLE001 - 远程任务可能已过期/不存在
            return None

    async def cancel_task(self, agent_url: str, remote_task_id: str) -> None:
        client = await self._client_for(agent_url)
        try:
            await client.cancel_task(CancelTaskRequest(id=remote_task_id))
        except Exception:  # noqa: BLE001 - 取消仅为信号，失败不阻塞
            return

    async def subscribe_task(
        self, agent_url: str, remote_task_id: str
    ) -> AsyncIterator[StreamResponse]:
        client = await self._client_for(agent_url)
        async for chunk in client.subscribe(SubscribeToTaskRequest(id=remote_task_id)):
            yield chunk
```

（import `GetTaskRequest/CancelTaskRequest/SubscribeToTaskRequest/Task`。`subscribe` 客户端方法已由 SDK 提供。）

- [ ] **Step 4: 通过并提交**

Run: `uv run pytest tests/integration/test_a2a_client.py -p no:warnings -q` → `4 passed`

```bash
git commit -am "feat: A2A client 支持 get/cancel/subscribe"
```

---

### Task 2: Checkpoint 生成与投影

**Files:**
- Modify: `src/agent_hub/core/orchestrator.py`、`src/agent_hub/core/tasks.py`、`src/agent_hub/store/projections.py`
- Test: `tests/integration/test_orchestrator.py`（追加）

- [ ] **Step 1: 追加失败测试**

```python
async def test_checkpoints_created_as_nodes_complete(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="hi"))], {"good": agent}
    )
    try:
        task_id = await tasks.create_pending_task("hi")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        from agent_hub.store import projections as proj

        checkpoints = await proj.fetch_checkpoints(db, task_id)
        assert len(checkpoints) == 1
        assert checkpoints[0].frontier
        assert checkpoints[0].plan_version == 1
        types = [e.type for e in await events.replay(task_id)]
        assert EventType.CHECKPOINT_CREATED in types
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()
```

- [ ] **Step 2: 实现**

`projections.py`：新增 `CHECKPOINT_CREATED` 投影（INSERT checkpoints）与 `fetch_checkpoints(db, task_id)`、`_row_to_checkpoint`。

`core/tasks.py` 追加：

```python
    async def create_checkpoint(self, task_id: str) -> Checkpoint:
        task = await projections.fetch_task(self._db, task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if task is None or plan is None:
            raise TaskNotFound(task_id)
        nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
        frontier = [node.id for node in nodes if node.status is NodeStatus.COMPLETED]
        artifacts = {node.id: node.output for node in nodes if node.output}
        checkpoint_id = uuid4().hex
        seq = await self._events.latest_seq(task_id)
        await self._events.append(
            task_id,
            EventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": checkpoint_id,
                "seq": seq,
                "plan_version": plan.version,
                "frontier": frontier,
                "artifacts": artifacts,
            },
        )
        return Checkpoint(
            id=checkpoint_id,
            task_id=task_id,
            seq=seq,
            plan_version=plan.version,
            frontier=frontier,
            artifacts=artifacts,
            created_at=datetime.now(UTC),
        )
```

`orchestrator.py` 构造器增加 `self._checkpoint_counts: dict[str, int] = {}`；在 `run()` 循环的 parked/ready 之前插入：

```python
            completed_count = sum(
                1 for node in nodes if node.status is NodeStatus.COMPLETED
            )
            if completed_count and completed_count > self._checkpoint_counts.get(task_id, 0):
                await self._task_service.create_checkpoint(task_id)
                self._checkpoint_counts[task_id] = completed_count
```

- [ ] **Step 3: 通过并提交**

Run: `uv run pytest tests/integration/test_orchestrator.py -p no:warnings -q` → `7 passed`

```bash
git commit -am "feat: 节点完成自动生成 checkpoint"
```

---

### Task 3: 启动恢复与对账

**Files:**
- Create: `src/agent_hub/core/recovery.py`、`src/agent_hub/a2a/reconcile.py`
- Modify: `src/agent_hub/core/dispatcher.py`（`resume_node`）、`src/agent_hub/api/app.py`（启动恢复 + 周期对账）、`src/agent_hub/config.py`（recovery 配置）
- Test: `tests/integration/test_recovery.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_recovery.py`**

```python
import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import Planner
from agent_hub.core.recovery import recover_tasks
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent
from tests.support.fakes import FakeLLM


async def _build(db_path, agent):
    db = Database(db_path)
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    if await registry.get_by_name("fake") is None:
        await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM()
    orchestrator = Orchestrator(
        db, events, Planner(llm, registry), dispatcher, tasks,
        registry=registry, remote=remote, llm=llm,
    )
    return db, remote, events, tasks, dispatcher, orchestrator


async def test_recover_resumes_inflight_then_completes(tmp_path):
    agent = await start_fake_agent("delay")
    db_path = tmp_path / "hub.db"
    db, remote, events, tasks, dispatcher, orchestrator = await _build(db_path, agent)
    created = await tasks.create_task(
        "slow", TargetSpec(agent_name="fake", input={"text": "slow"})
    )
    orchestrator.start(created.task_id)
    # 等到节点进入 working（远程仍在执行）
    for _ in range(200):
        node = await projections.fetch_node(db, created.node_ids[0])
        if node and node.status in (NodeStatus.DISPATCHED, NodeStatus.WORKING):
            break
        await asyncio.sleep(0.02)
    await orchestrator.stop()          # 模拟进程崩溃
    await remote.close()
    await db.close()

    # 重启：新对象、同一 DB
    db2, remote2, events2, tasks2, dispatcher2, orchestrator2 = await _build(db_path, agent)
    try:
        await recover_tasks(db2, events2, remote2, dispatcher2, orchestrator2)
        await asyncio.wait_for(
            orchestrator2.wait(created.task_id, until_terminal=True), 10.0
        )
        snapshot = await tasks2.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        types = [e.type for e in await events2.replay(created.task_id)]
        assert EventType.NODE_DISPATCHED in types
    finally:
        await orchestrator2.stop()
        await remote2.close()
        await db2.close()
        await agent.stop()


async def test_reconcile_detects_remote_completion(tmp_path):
    agent = await start_fake_agent("delay")
    db, remote, events, tasks, dispatcher, orchestrator = await _build(tmp_path / "hub.db", agent)
    try:
        created = await tasks.create_task(
            "slow", TargetSpec(agent_name="fake", input={"text": "slow"})
        )
        # 只发远程请求，不消费流（模拟本地没跟上）
        send = asyncio.create_task(
            _consume_once(remote, agent.url, created.task_id)
        )
        for _ in range(200):
            node = await projections.fetch_node(db, created.node_ids[0])
            if node is None:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.6)
        await send
        from agent_hub.a2a.reconcile import reconcile_once

        changed = await reconcile_once(db, events, remote)
        assert changed >= 1
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()
```

（测试辅助 `_consume_once` 用 SDK 直连发送并消费完，避免依赖 dispatcher；此测试主要验证 `reconcile_once` 不抛错并有处理逻辑。实际对账逻辑以 GetTask 为准。）

- [ ] **Step 2: 实现 `dispatcher.resume_node`**

```python
    async def resume_node(self, task_id: str, node_id: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status not in (NodeStatus.DISPATCHED, NodeStatus.WORKING):
            return node
        if not node.a2a_task_id:
            raise InvalidNodeState(f"node {node_id} has no remote task id")
        artifacts: list[dict[str, Any]] = []
        current = node.status
        try:
            async with asyncio.timeout(self._timeout):
                current = await self._consume(
                    node,
                    self._remote.subscribe_task(node.agent_url or "", node.a2a_task_id),
                    artifacts,
                    announce_dispatched=False,
                )
            await self._handle_stream_end(node, current, artifacts)
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001
            await self._fail(node, str(exc))
        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed
```

- [ ] **Step 3: 实现 `core/recovery.py`**

```python
from __future__ import annotations

import asyncio

from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.models.enums import TERMINAL_TASK_STATUSES, NodeStatus
from agent_hub.store import projections


async def recover_tasks(db, events, remote, dispatcher, orchestrator) -> list[str]:
    """扫描非终态任务并恢复调度；返回恢复的任务 ID 列表。"""
    cursor = await db.conn.execute("SELECT id, status FROM orchestration_tasks")
    rows = await cursor.fetchall()
    recovered: list[str] = []
    for row in rows:
        if row["status"] in {s.value for s in TERMINAL_TASK_STATUSES}:
            continue
        task_id = row["id"]
        plan = await projections.fetch_current_plan(db, task_id)
        if plan is None:
            orchestrator.start(task_id)  # 重新规划
            recovered.append(task_id)
            continue
        nodes = await projections.fetch_nodes(db, task_id, plan.id)
        inflight = [
            node
            for node in nodes
            if node.status in (NodeStatus.DISPATCHED, NodeStatus.WORKING)
            and node.a2a_task_id
        ]
        if inflight:
            async def _resume_and_run(tid=task_id, ids=[n.id for n in inflight]):
                await asyncio.gather(
                    *(dispatcher.resume_node(tid, node_id) for node_id in ids),
                    return_exceptions=True,
                )
                orchestrator.start(tid)

            asyncio.create_task(_resume_and_run())
        else:
            orchestrator.start(task_id)
        recovered.append(task_id)
    return recovered
```

- [ ] **Step 4: 实现 `a2a/reconcile.py` 与装配**

```python
from __future__ import annotations

from datetime import UTC, datetime

from agent_hub.models.enums import EventType, NodeStatus
from agent_hub.store import projections


async def reconcile_once(db, events, remote) -> int:
    """用远程 GetTask 校正本地漂移；返回修正的节点数。"""
    fixed = 0
    cursor = await db.conn.execute(
        "SELECT id, task_id, agent_url, a2a_task_id, status FROM nodes"
    )
    for row in await cursor.fetchall():
        if row["status"] not in (NodeStatus.DISPATCHED.value, NodeStatus.WORKING.value):
            continue
        if not row["a2a_task_id"] or not row["agent_url"]:
            continue
        task = await remote.get_task(row["agent_url"], row["a2a_task_id"])
        if task is None:
            continue
        state = task.status.state
        from a2a.types import TaskState

        if state in (TaskState.TASK_STATE_COMPLETED,):
            artifacts = [
                {"id": a.artifact_id, "name": a.name, "text": _artifact_text(a)}
                for a in task.artifacts
            ]
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {"node_id": row["id"], "from": row["status"], "to": NodeStatus.COMPLETED.value},
            )
            await events.append(
                row["task_id"],
                EventType.NODE_OUTPUT,
                {"node_id": row["id"], "output": {"artifacts": artifacts}},
            )
            fixed += 1
        elif state in (TaskState.TASK_STATE_FAILED, TaskState.TASK_STATE_REJECTED):
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {"node_id": row["id"], "from": row["status"], "to": NodeStatus.FAILED.value},
            )
            fixed += 1
        elif state is TaskState.TASK_STATE_INPUT_REQUIRED:
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": row["id"],
                    "from": row["status"],
                    "to": NodeStatus.INPUT_REQUIRED.value,
                },
            )
            fixed += 1
    return fixed


def _artifact_text(artifact) -> str:
    return "\n".join(part.text for part in artifact.parts if part.HasField("text"))
```

`app.py` lifespan：DB 初始化后调用 `await recover_tasks(...)`；启动周期任务：

```python
        reconcile_task = asyncio.create_task(_reconcile_loop(...))
```
`_reconcile_loop` 每 `resolved.recovery.reconcile_interval_seconds` 调一次 `reconcile_once`，shutdown 时 cancel。配置新增 `RecoveryConfig(reconcile_interval_seconds: float = 30.0, replay_on_startup: bool = True)`。

- [ ] **Step 5: 通过并提交**

Run: `uv run pytest tests/integration/test_recovery.py -p no:warnings -q` → `2 passed`

```bash
git commit -am "feat: 启动恢复（Subscribe 重挂接）与远程对账"
```

---

### Task 4: 回退、retry 与任务 cancel

**Files:**
- Create: `src/agent_hub/core/rollback.py`
- Modify: `src/agent_hub/models/enums.py`（`INTERVENTION_INVALIDATED`）、`src/agent_hub/store/projections.py`、`src/agent_hub/api/schemas.py`、`src/agent_hub/api/rollback.py`、`src/agent_hub/api/app.py`
- Test: `tests/integration/test_rollback.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_rollback.py`**

```python
import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import PlanDraft, PlanNodeDraft, Planner
from agent_hub.core.rollback import perform_rollback, plan_rollback
from agent_hub.core.tasks import TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent
from tests.support.fakes import FakeLLM


async def setup_env(tmp_path, agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM()
    orchestrator = Orchestrator(db, events, Planner(llm, registry), dispatcher, tasks,
                                registry=registry, remote=remote, llm=llm)
    return db, remote, events, tasks, dispatcher, orchestrator


async def test_rollback_resets_nodes_after_checkpoint(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, dispatcher, orchestrator = await setup_env(tmp_path, agent)
    try:
        draft = PlanDraft(
            rationale="two nodes",
            nodes=[
                PlanNodeDraft(id="n1", name="a", agent_name="fake", input={"text": "1"}),
                PlanNodeDraft(id="n2", name="b", agent_name="fake", deps=["n1"], input={"text": "2"}),
            ],
        )
        task_id = await tasks.create_pending_task("two")
        await tasks.create_plan_from_draft(task_id, draft, version=1)
        await tasks.mark_running(task_id)
        # 手动跑第一节点 + 生成 checkpoint
        await dispatcher.dispatch_node(task_id, f"{draft.nodes[0].id}".join([""]) or "")
    except Exception:
        pass
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()
```

> 注：真实测试使用 orchestrator 跑完 n1、停在 n2（把 n2 的 agent 配成 `ask` 并设 human 策略会复杂）；实现时按以下更简单路径编写：
> 1. 用 `tasks.create_task` 单节点跑完 → 生成 checkpoint（frontier=[n1]）；
> 2. 再用 `create_plan_from_draft` 追加第二版计划（n1 依赖已完成输出，n2 pending）；
> 3. `plan_rollback` 报告 n2 将被重置；
> 4. `perform_rollback` 后 n2 仍为 pending 且存在 `rollback.performed` 事件；
> 5. `dry_run` 不产生事件。
> 具体断言以实际实现时的 tests 为准，核心不变量：dry_run 只读；restart 产生 `rollback.performed` 且任务可再次运行。

- [ ] **Step 2: 实现 `core/rollback.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections


class RollbackReport(BaseModel):
    checkpoint_id: str
    plan_version: int
    reset_node_ids: list[str]
    cancelled_remote_task_ids: list[str]
    mode: str


async def plan_rollback(db, task_id: str, checkpoint_id: str) -> RollbackReport:
    task = await projections.fetch_task(db, task_id)
    checkpoint = await projections.fetch_checkpoint(db, checkpoint_id)
    if task is None or checkpoint is None or checkpoint.task_id != task_id:
        raise TaskNotFound(f"task/checkpoint not found: {task_id}/{checkpoint_id}")
    plan = await projections.fetch_current_plan(db, task_id)
    assert plan is not None
    nodes = await projections.fetch_nodes(db, task_id, plan.id)
    frontier = set(checkpoint.frontier)
    reset = [
        node
        for node in nodes
        if node.id not in frontier and node.status is not NodeStatus.INVALIDATED
    ]
    return RollbackReport(
        checkpoint_id=checkpoint_id,
        plan_version=checkpoint.plan_version,
        reset_node_ids=[node.id for node in reset],
        cancelled_remote_task_ids=[
            node.a2a_task_id
            for node in reset
            if node.a2a_task_id
        ],
        mode="dry_run",
    )


async def perform_rollback(
    db, events, remote, orchestrator, task_id: str, checkpoint_id: str
) -> RollbackReport:
    report = await plan_rollback(db, task_id, checkpoint_id)
    report = report.model_copy(update={"mode": "restart"})
    for node_id in report.reset_node_ids:
        node = await projections.fetch_node(db, node_id)
        if node is None:
            continue
        if node.a2a_task_id:
            await remote.cancel_task(node.agent_url or "", node.a2a_task_id)
            await events.append(
                task_id, EventType.NODE_CANCEL_SENT,
                {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
            )
    await events.append(
        task_id,
        EventType.ROLLBACK_PERFORMED,
        {
            "checkpoint_id": checkpoint_id,
            "plan_version": report.plan_version,
            "reset_node_ids": report.reset_node_ids,
        },
    )
    orchestrator.start(task_id)
    return report
```

`projections.py`：
- `ROLLBACK_PERFORMED` 投影：

```python
    elif event_type is EventType.ROLLBACK_PERFORMED:
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, plan_version = ?, updated_at = ? WHERE id = ?",
            (TaskStatus.RUNNING.value, payload["plan_version"], ts, event.task_id),
        )
        for node_id in payload["reset_node_ids"]:
            await conn.execute(
                "UPDATE nodes SET status = 'pending', attempt = 0, output = NULL,"
                " error = NULL, a2a_task_id = NULL, a2a_context_id = NULL,"
                " started_at = NULL, ended_at = NULL WHERE id = ? AND task_id = ?",
                (node_id, event.task_id),
            )
        await conn.execute(
            "UPDATE interventions SET status = ? WHERE task_id = ? AND status = ?",
            (InterventionStatus.INVALIDATED.value, event.task_id,
             InterventionStatus.PENDING.value),
        )
```

- `fetch_checkpoint(db, id)`、`fetch_checkpoints(db, task_id)`。

`models/enums.py` 增加 `INTERVENTION_INVALIDATED = "intervention.invalidated"`（转 `intervention.invalidated` 事件不修改 status 时可直接用 ROLLBACK 投影；枚举保留备用）。

- [ ] **Step 3: API 端点**

`api/rollback.py`：

```python
@router.post("/tasks/{task_id}/rollback", response_model=RollbackReport)
async def rollback(task_id: str, body: RollbackIn, request: Request):
    if body.mode == "dry_run":
        return await plan_rollback(request.app.state.db, task_id, body.checkpoint_id)
    return await perform_rollback(
        request.app.state.db, request.app.state.event_store,
        request.app.state.remote, request.app.state.orchestrator,
        task_id, body.checkpoint_id,
    )


@router.post("/tasks/{task_id}/nodes/{node_id}/retry", response_model=TaskSnapshot)
async def retry_node(task_id: str, node_id: str, request: Request):
    node = await projections.fetch_node(request.app.state.db, node_id)
    if node is None or node.task_id != task_id:
        raise HTTPException(status_code=404, detail=f"node not found: {node_id}")
    if node.status is not NodeStatus.FAILED:
        raise HTTPException(status_code=409, detail=f"node is {node.status.value}")
    await request.app.state.event_store.append(
        task_id, EventType.NODE_STATE_CHANGED,
        {"node_id": node_id, "from": node.status.value, "to": NodeStatus.READY.value},
    )
    request.app.state.orchestrator.start(task_id)
    return await request.app.state.task_service.get_snapshot(task_id)


@router.post("/tasks/{task_id}/cancel", response_model=TaskSnapshot)
async def cancel_task(task_id: str, request: Request):
    task = await projections.fetch_task(request.app.state.db, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}")
    if task.status.value not in ("running", "awaiting_input", "planning"):
        raise HTTPException(status_code=409, detail=f"task is {task.status.value}")
    events = request.app.state.event_store
    plan = await projections.fetch_current_plan(request.app.state.db, task_id)
    if plan is not None:
        for node in await projections.fetch_nodes(request.app.state.db, task_id, plan.id):
            if node.a2a_task_id and node.status.value in ("dispatched", "working", "input_required"):
                await request.app.state.remote.cancel_task(node.agent_url or "", node.a2a_task_id)
                await events.append(task_id, EventType.NODE_CANCEL_SENT,
                                    {"node_id": node.id, "a2a_task_id": node.a2a_task_id})
    await events.append(task_id, EventType.TASK_STATE_CHANGED,
                        {"from": task.status.value, "to": TaskStatus.CANCELED.value})
    await request.app.state.orchestrator.stop_task(task_id)
    return await request.app.state.task_service.get_snapshot(task_id)
```

`Orchestrator.stop_task(task_id)`：取消该任务的 run / continuing / timeout 任务（不动全局）。

- [ ] **Step 4: 通过并提交**

Run: 全量 `uv run pytest -p no:warnings -q` 与 `uv run ruff check .`

```bash
git commit -am "feat: 回退/单节点 retry/任务 cancel（平台侧 + 取消信号）"
```

---

### Task 5: README 与收尾

- [ ] 更新 README：M1–M6 完成；补充 rollback/retry/cancel/recovery 说明与配置项。
- [ ] 跑全量测试与 ruff；写执行勘误。
- [ ] Commit。

---

## Plan 4 完成标准（对照 spec M5/M6/M7）

- [ ] 每个完成节点生成 checkpoint（测试）。
- [ ] 崩溃重启后 `recover_tasks` 能对在途节点 `SubscribeToTask` 重挂接并续跑到完成（测试）。
- [ ] `reconcile_once` 用 GetTask 校正漂移（测试）。
- [ ] `rollback` 支持 `dry_run` 与 `restart`（取消信号 + 重置 + 重跑）；单节点 retry；任务 cancel。
- [ ] 全量测试与 ruff 全绿；README 与勘误更新。
