# A2A 编排平台 MVP — Plan 2：LLM 规划、DAG 调度与 SSE

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让平台在无人工指定 agent 的情况下自动完成「LLM 拆解需求 → 生成 DAG → 并行/串行调度 A2A subagent → 失败重试/重规划 → 任务完成」，并提供 SSE 事件流（回放、实时、断线重连）。

**Architecture:** 新增 `LLMClient`（LiteLLM + instructor，可注入 FakeLLM 测试）、`Planner`（结构化输出 + 校验重试）、`Orchestrator`（asyncio 调度循环，并发上限、重试、重规划、input-required 停车）、`EventBus`（进程内发布订阅）；EventStore 在事务内发布事件保证 seq 顺序；SSE 端点先订阅后回放、按 seq 去重。

**Tech Stack:** 现有 M1 代码 + `litellm`、`instructor`、`sse-starlette`（已随 a2a-sdk 安装）。

**Spec:** `docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`（§4.2、§5.1、§9）

**前置：** M1 已完成（30 tests 全绿）。执行本计划时全部命令用 `uv run`，测试输出加 `-p no:warnings`。

**关键约定：**
- `target` 参数保留 M1 语义（手动单节点计划、不自动调度，供调试/测试）；不传 `target` 走 Planner 自动路径并后台调度。
- 重规划产生新 plan version；旧 plan 节点保留审计，调度只看当前 plan。
- `input-required` 在 M2 只做「任务进入 awaiting_input 并停车」，恢复由 Plan 3（HITL）实现。
- EventBus 发布在 EventStore 事务内（持锁）执行，保证订阅者收到的顺序与 seq 一致。

---

## 文件结构（Plan 2 增量）

```
src/agent_hub/
  core/llm.py               # LLMClient Protocol + LiteLLMClient
  core/planner.py           # PlanDraft/validate_plan/Planner
  core/orchestrator.py      # 调度循环
  core/events.py            # EventBus/EventSubscription
  core/tasks.py             # +create_pending_task/create_plan_from_draft/mark_running
  store/event_store.py      # +bus 发布
  api/schemas.py            # target 可选、CreateTaskOut
  api/tasks.py              # Planner 路径 + 自动调度
  api/sse.py                # SSE 端点
  api/app.py                # 装配 llm/planner/orchestrator/bus
tests/
  support/fakes.py          # FakeLLM
  unit/test_llm.py  unit/test_planner.py  unit/test_events.py
  integration/test_orchestrator.py  integration/test_sse.py
  fake_agents/echo_agent.py # +delay/fail_once 行为
```

---

### Task 1: 配置扩展（LLM + 调度参数）

**Files:**
- Modify: `src/agent_hub/config.py`
- Modify: `config.example.yaml`
- Test: `tests/unit/test_config.py`（追加用例）

- [ ] **Step 1: 追加失败测试到 `tests/unit/test_config.py`**

```python
def test_llm_and_scheduler_defaults():
    settings = Settings()
    assert settings.llm.planner_model
    assert settings.llm.max_plan_retries == 2
    assert settings.scheduler.max_plan_nodes == 20
    assert settings.scheduler.retry_backoff_seconds == 1.0
    assert settings.scheduler.replan_on_failure is True
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_config.py -p no:warnings -q`
Expected: FAIL，`AttributeError: 'Settings' object has no attribute 'llm'`

- [ ] **Step 3: 修改 `src/agent_hub/config.py`**

在 `SchedulerConfig` 中追加字段，并新增 `LLMConfig` / `Settings.llm`：

```python
class LLMConfig(BaseModel):
    planner_model: str = "openai/gpt-4.1"
    assist_model: str = "openai/gpt-4.1-mini"
    timeout_seconds: float = 60.0
    max_plan_retries: int = 2


class SchedulerConfig(BaseModel):
    max_parallel_nodes: int = 5
    node_timeout_seconds: float = 600.0
    max_node_attempts: int = 2
    retry_backoff_seconds: float = 1.0
    replan_on_failure: bool = True
    max_plan_nodes: int = 20


class Settings(BaseSettings):
    ...
    llm: LLMConfig = Field(default_factory=LLMConfig)
```

- [ ] **Step 4: 同步 `config.example.yaml`**

```yaml
llm:
  planner_model: openai/gpt-4.1
  assist_model: openai/gpt-4.1-mini
  timeout_seconds: 60
  max_plan_retries: 2

scheduler:
  max_parallel_nodes: 5
  node_timeout_seconds: 600
  max_node_attempts: 2
  retry_backoff_seconds: 1.0
  replan_on_failure: true
  max_plan_nodes: 20
```

- [ ] **Step 5: 运行确认通过并提交**

Run: `uv run pytest tests/unit/test_config.py -p no:warnings -q`
Expected: `4 passed`

```bash
git add src/agent_hub/config.py config.example.yaml tests/unit/test_config.py
git commit -m "feat: LLM 与调度配置项"
```

---

### Task 2: LLM 客户端抽象

**Files:**
- Create: `src/agent_hub/core/llm.py`
- Test: `tests/unit/test_llm.py`

- [ ] **Step 1: 写失败测试 `tests/unit/test_llm.py`**

```python
from types import SimpleNamespace

from pydantic import BaseModel

from agent_hub.core.llm import LiteLLMClient


class Answer(BaseModel):
    value: str


async def test_structured_passes_schema_and_model(monkeypatch):
    calls = []

    async def fake_create(**kwargs):
        calls.append(kwargs)
        return Answer(value="ok")

    client = LiteLLMClient(model="openai/test-model", timeout_seconds=5.0)
    monkeypatch.setattr(
        client,
        "_instructor",
        SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=fake_create))),
    )
    result = await client.structured(system="sys", user="usr", schema=Answer)
    assert result == Answer(value="ok")
    assert calls[0]["model"] == "openai/test-model"
    assert calls[0]["response_model"] is Answer
    assert calls[0]["timeout"] == 5.0
    assert calls[0]["messages"][0]["content"] == "sys"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_llm.py -p no:warnings -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.core.llm'`

- [ ] **Step 3: 实现 `src/agent_hub/core/llm.py`**

```python
from __future__ import annotations

from typing import Protocol, TypeVar

import instructor
import litellm
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMClient(Protocol):
    async def structured(self, *, system: str, user: str, schema: type[T]) -> T: ...

    async def text(self, *, system: str, user: str) -> str: ...


class LiteLLMClient:
    def __init__(self, model: str, timeout_seconds: float = 60.0):
        self._model = model
        self._timeout = timeout_seconds
        self._instructor = instructor.from_litellm(litellm.acompletion)

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        return await self._instructor.chat.completions.create(
            model=self._model,
            response_model=schema,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )

    async def text(self, *, system: str, user: str) -> str:
        response = await litellm.acompletion(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )
        return response.choices[0].message.content or ""
```

- [ ] **Step 4: 运行确认通过并提交**

Run: `uv run pytest tests/unit/test_llm.py -p no:warnings -q`
Expected: `1 passed`

```bash
git add src/agent_hub/core/llm.py tests/unit/test_llm.py
git commit -m "feat: LLM 客户端抽象（LiteLLM + instructor）"
```

---

### Task 3: 计划 schema 与校验

**Files:**
- Create: `src/agent_hub/core/planner.py`
- Create: `tests/support/__init__.py`（空）、`tests/support/fakes.py`
- Test: `tests/unit/test_planner.py`

- [ ] **Step 1: 实现 `tests/support/fakes.py`（供后续任务共用）**

```python
from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class FakeLLM:
    def __init__(
        self,
        structured_results: list[Any] | None = None,
        text_results: list[str] | None = None,
    ):
        self.structured_results = list(structured_results or [])
        self.text_results = list(text_results or [])
        self.structured_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        self.structured_calls.append({"system": system, "user": user, "schema": schema})
        if not self.structured_results:
            raise AssertionError("FakeLLM has no scripted structured result")
        result = self.structured_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def text(self, *, system: str, user: str) -> str:
        self.text_calls.append({"system": system, "user": user})
        if not self.text_results:
            raise AssertionError("FakeLLM has no scripted text result")
        return self.text_results.pop(0)
```

- [ ] **Step 2: 写失败测试 `tests/unit/test_planner.py`（校验部分）**

```python
import pytest

from agent_hub.core.planner import (
    PlanDraft,
    PlanNodeDraft,
    PlanValidationError,
    validate_plan,
)
from agent_hub.models.domain import AgentRecord


def make_agent(name: str, skills: list[str]) -> AgentRecord:
    from datetime import UTC, datetime

    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={
            "name": name,
            "description": f"{name} agent",
            "skills": [{"id": s, "name": s, "description": s} for s in skills],
        },
        created_at=datetime.now(UTC),
    )


AGENTS = [make_agent("research", ["search"]), make_agent("writer", ["write"])]


def node(node_id: str, agent: str, deps: list[str] | None = None, skill: str | None = None):
    return PlanNodeDraft(id=node_id, name=node_id, agent_name=agent, deps=deps or [], skill_id=skill)


def test_valid_plan_passes():
    draft = PlanDraft(
        rationale="two steps",
        nodes=[node("n1", "research", skill="search"), node("n2", "writer", deps=["n1"], skill="write")],
    )
    validate_plan(draft, AGENTS, max_nodes=10)


def test_unknown_agent_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "ghost")])
    with pytest.raises(PlanValidationError, match="unknown agent"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_unknown_skill_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research", skill="nope")])
    with pytest.raises(PlanValidationError, match="unknown skill"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_duplicate_node_id_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research"), node("n1", "writer")])
    with pytest.raises(PlanValidationError, match="duplicate node id"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_missing_dependency_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research", deps=["n9"])])
    with pytest.raises(PlanValidationError, match="unknown dependency"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_cycle_rejected():
    draft = PlanDraft(
        rationale="x",
        nodes=[node("n1", "research", deps=["n2"]), node("n2", "writer", deps=["n1"])],
    )
    with pytest.raises(PlanValidationError, match="cycle"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_too_many_nodes_rejected():
    draft = PlanDraft(
        rationale="x",
        nodes=[node(f"n{i}", "research") for i in range(4)],
    )
    with pytest.raises(PlanValidationError, match="too many nodes"):
        validate_plan(draft, AGENTS, max_nodes=3)


def test_empty_plan_rejected():
    draft = PlanDraft(rationale="x", nodes=[])
    with pytest.raises(PlanValidationError, match="no nodes"):
        validate_plan(draft, AGENTS, max_nodes=10)
```

- [ ] **Step 3: 运行确认失败**

Run: `uv run pytest tests/unit/test_planner.py -p no:warnings -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.core.planner'`

- [ ] **Step 4: 实现 `src/agent_hub/core/planner.py`（校验部分）**

```python
from __future__ import annotations

from typing import Any, Sequence

from pydantic import BaseModel, Field

from agent_hub.models.domain import AgentRecord


class PlanNodeDraft(BaseModel):
    id: str
    name: str
    agent_name: str
    skill_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)
    requires_approval: bool = False
    policy_override: str | None = None


class PlanDraft(BaseModel):
    rationale: str
    nodes: list[PlanNodeDraft]


class PlanValidationError(ValueError):
    pass


def validate_plan(
    draft: PlanDraft, agents: Sequence[AgentRecord], max_nodes: int = 20
) -> None:
    if not draft.nodes:
        raise PlanValidationError("plan has no nodes")
    if len(draft.nodes) > max_nodes:
        raise PlanValidationError(f"too many nodes: {len(draft.nodes)} > {max_nodes}")

    ids = [node.id for node in draft.nodes]
    if len(set(ids)) != len(ids):
        raise PlanValidationError("duplicate node id in plan")
    id_set = set(ids)
    agents_by_name = {agent.name: agent for agent in agents}

    for node in draft.nodes:
        record = agents_by_name.get(node.agent_name)
        if record is None:
            raise PlanValidationError(f"unknown agent: {node.agent_name}")
        if node.skill_id is not None:
            skills = {skill.get("id") for skill in record.card.get("skills", [])}
            if node.skill_id not in skills:
                raise PlanValidationError(
                    f"unknown skill '{node.skill_id}' for agent {node.agent_name}"
                )
        for dep in node.deps:
            if dep not in id_set:
                raise PlanValidationError(f"unknown dependency: {dep}")
            if dep == node.id:
                raise PlanValidationError(f"node {node.id} depends on itself")

    indegree = {node.id: len(set(node.deps)) for node in draft.nodes}
    children: dict[str, list[str]] = {node.id: [] for node in draft.nodes}
    for node in draft.nodes:
        for dep in set(node.deps):
            children[dep].append(node.id)
    queue = [node_id for node_id, degree in indegree.items() if degree == 0]
    visited = 0
    while queue:
        current = queue.pop()
        visited += 1
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited != len(ids):
        raise PlanValidationError("plan contains a cycle")


def draft_to_dag(draft: PlanDraft, agent_urls: dict[str, str]) -> dict[str, Any]:
    return {
        "nodes": [
            {
                "id": node.id,
                "name": node.name,
                "agent_url": agent_urls[node.agent_name],
                "skill_id": node.skill_id,
                "deps": node.deps,
                "input": node.input,
                "requires_approval": node.requires_approval,
                "policy_override": node.policy_override,
            }
            for node in draft.nodes
        ]
    }
```

- [ ] **Step 5: 运行确认通过**

Run: `uv run pytest tests/unit/test_planner.py -p no:warnings -q`
Expected: `8 passed`

- [ ] **Step 6: Commit**

```bash
git add src/agent_hub/core/planner.py tests/support/ tests/unit/test_planner.py
git commit -m "feat: 计划 schema、校验与 DAG 转换"
```

---

### Task 4: Planner 服务（LLM 结构化输出 + 校验重试）

**Files:**
- Modify: `src/agent_hub/core/planner.py`
- Modify: `tests/unit/test_planner.py`

- [ ] **Step 1: 追加失败测试到 `tests/unit/test_planner.py`**

```python
from agent_hub.core.planner import Planner, PlanningFailed
from tests.support.fakes import FakeLLM


async def test_planner_returns_valid_draft(tmp_path):
    llm = FakeLLM(structured_results=[
        PlanDraft(rationale="ok", nodes=[node("n1", "research", skill="search")])
    ])
    from agent_hub.a2a.client import RemoteAgentClient
    from agent_hub.a2a.registry import AgentRegistry
    from agent_hub.store.db import Database

    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        # 直接插入两条 agent 记录，避免依赖真实网络
        import json
        from datetime import UTC, datetime

        now = datetime.now(UTC).isoformat()
        async with db.transaction() as conn:
            for agent in AGENTS:
                await conn.execute(
                    "INSERT INTO agent_registry (id, name, card_url, card, health, last_seen, created_at)"
                    " VALUES (?, ?, ?, ?, 'ok', ?, ?)",
                    (agent.id, agent.name, agent.card_url,
                     json.dumps(agent.card), now, now),
                )
        planner = Planner(llm, registry, max_nodes=10, max_retries=2)
        draft = await planner.plan("研究并写一份报告")
        assert draft.nodes[0].agent_name == "research"
        assert "Available agents" in llm.structured_calls[0]["user"]
    finally:
        await remote.close()
        await db.close()


async def test_planner_retries_with_feedback(tmp_path):
    import json
    from datetime import UTC, datetime
    from agent_hub.a2a.client import RemoteAgentClient
    from agent_hub.a2a.registry import AgentRegistry
    from agent_hub.store.db import Database

    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    bad = PlanDraft(rationale="bad", nodes=[node("n1", "ghost")])
    good = PlanDraft(rationale="good", nodes=[node("n1", "research", skill="search")])
    llm = FakeLLM(structured_results=[bad, good])
    try:
        now = datetime.now(UTC).isoformat()
        async with db.transaction() as conn:
            agent = AGENTS[0]
            await conn.execute(
                "INSERT INTO agent_registry (id, name, card_url, card, health, last_seen, created_at)"
                " VALUES (?, ?, ?, ?, 'ok', ?, ?)",
                (agent.id, agent.name, agent.card_url, json.dumps(agent.card), now, now),
            )
        planner = Planner(llm, registry, max_nodes=10, max_retries=2)
        draft = await planner.plan("x")
        assert draft.rationale == "good"
        assert "unknown agent" in llm.structured_calls[1]["user"]
    finally:
        await remote.close()
        await db.close()


async def test_planner_fails_after_retries(tmp_path):
    import json
    from datetime import UTC, datetime
    from agent_hub.a2a.client import RemoteAgentClient
    from agent_hub.a2a.registry import AgentRegistry
    from agent_hub.store.db import Database

    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    bad = PlanDraft(rationale="bad", nodes=[node("n1", "ghost")])
    llm = FakeLLM(structured_results=[bad, bad, bad])
    try:
        now = datetime.now(UTC).isoformat()
        async with db.transaction() as conn:
            agent = AGENTS[0]
            await conn.execute(
                "INSERT INTO agent_registry (id, name, card_url, card, health, last_seen, created_at)"
                " VALUES (?, ?, ?, ?, 'ok', ?, ?)",
                (agent.id, agent.name, agent.card_url, json.dumps(agent.card), now, now),
            )
        planner = Planner(llm, registry, max_nodes=10, max_retries=2)
        with pytest.raises(PlanningFailed):
            await planner.plan("x")
        assert len(llm.structured_calls) == 3
    finally:
        await remote.close()
        await db.close()


async def test_planner_rejects_when_no_agents(tmp_path):
    from agent_hub.a2a.client import RemoteAgentClient
    from agent_hub.a2a.registry import AgentRegistry
    from agent_hub.store.db import Database

    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        planner = Planner(FakeLLM(), registry)
        with pytest.raises(PlanningFailed, match="no agents"):
            await planner.plan("x")
    finally:
        await remote.close()
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_planner.py -p no:warnings -q`
Expected: FAIL，`ImportError: cannot import name 'Planner'`

- [ ] **Step 3: 追加实现到 `src/agent_hub/core/planner.py`**

```python
class PlanningFailed(RuntimeError):
    pass


SYSTEM_PROMPT = """You are the planning brain of a multi-agent orchestration platform.
Decompose the user's request into a DAG of tasks, each assigned to one registered agent.
Return only JSON matching the required schema. Rules:
- Every node must reference an existing agent_name and, when provided, an existing skill_id.
- Use deps to express ordering; independent nodes run in parallel.
- Keep the plan minimal: only nodes required to fulfill the request.
- Put the exact instruction for the agent in each node's input.text."""


class Planner:
    def __init__(
        self,
        llm: LLMClient,
        registry: AgentRegistry,
        *,
        max_nodes: int = 20,
        max_retries: int = 2,
    ):
        self._llm = llm
        self._registry = registry
        self._max_nodes = max_nodes
        self._max_retries = max_retries

    async def plan(
        self,
        request: str,
        *,
        reason: str | None = None,
        context: str | None = None,
    ) -> PlanDraft:
        agents = await self._registry.list()
        if not agents:
            raise PlanningFailed("no agents registered; register at least one A2A agent first")
        capabilities = self._capabilities_text(agents)
        user = f"User request:\n{request}\n\nAvailable agents:\n{capabilities}"
        if reason:
            user += f"\n\nReason for replanning:\n{reason}"
        if context:
            user += f"\n\nCompleted work so far:\n{context}"

        last_error: Exception | None = None
        for _ in range(self._max_retries + 1):
            draft = await self._llm.structured(
                system=SYSTEM_PROMPT, user=user, schema=PlanDraft
            )
            try:
                validate_plan(draft, agents, self._max_nodes)
                return draft
            except PlanValidationError as exc:
                last_error = exc
                user += f"\n\nPrevious plan was invalid: {exc}. Return a corrected plan."
        raise PlanningFailed(
            f"planner failed after {self._max_retries + 1} attempts: {last_error}"
        )

    @staticmethod
    def _capabilities_text(agents: Sequence[AgentRecord]) -> str:
        lines = []
        for agent in agents:
            skills = agent.card.get("skills", [])
            skill_text = "; ".join(
                f"{skill.get('id')} ({skill.get('description', '')})" for skill in skills
            )
            lines.append(
                f"- {agent.name}: {agent.card.get('description', '')} skills=[{skill_text}]"
            )
        return "\n".join(lines)
```

同时在文件头部 import 中补上：

```python
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.llm import LLMClient
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/unit/test_planner.py -p no:warnings -q`
Expected: `12 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/core/planner.py tests/unit/test_planner.py
git commit -m "feat: Planner 服务（结构化规划、校验反馈重试）"
```

---

### Task 5: TaskService 扩展（pending 任务、按草稿建计划、标记 running）

**Files:**
- Modify: `src/agent_hub/core/tasks.py`
- Test: `tests/integration/test_task_service.py`（追加）

- [ ] **Step 1: 追加失败测试**

```python
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from agent_hub.models.enums import EventType


async def test_create_pending_task_and_materialize_draft(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        task_id = await service.create_pending_task("写一份报告")
        task = await service.get_snapshot(task_id)
        assert task.task.status is TaskStatus.PLANNING
        assert task.plan is None

        draft = PlanDraft(
            rationale="one step",
            nodes=[
                PlanNodeDraft(
                    id="n1", name="写", agent_name="echo", input={"text": "写一份报告"}
                )
            ],
        )
        created = await service.create_plan_from_draft(task_id, draft, version=1)
        await service.mark_running(task_id)
        snapshot = await service.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.RUNNING
        assert snapshot.plan is not None and snapshot.plan.version == 1
        assert snapshot.nodes[0].agent_url == echo_agent.url
        assert created.node_ids == [f"{created.plan_id}:n1"]
    finally:
        await db.close()


async def test_replan_emits_superseded_and_bumps_version(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        task_id = await service.create_pending_task("x")
        draft1 = PlanDraft(
            rationale="v1",
            nodes=[PlanNodeDraft(id="n1", name="a", agent_name="echo", input={"text": "x"})],
        )
        await service.create_plan_from_draft(task_id, draft1, version=1)
        await service.mark_running(task_id)
        draft2 = PlanDraft(
            rationale="v2",
            nodes=[PlanNodeDraft(id="n1", name="b", agent_name="echo", input={"text": "x"})],
        )
        await service.create_plan_from_draft(task_id, draft2, version=2)
        snapshot = await service.get_snapshot(task_id)
        assert snapshot.plan is not None and snapshot.plan.version == 2
        assert snapshot.plan.rationale == "v2"
        assert snapshot.nodes[0].name == "b"
        from agent_hub.store.event_store import EventStore

        events = await EventStore(db).replay(task_id)
        assert EventType.PLAN_SUPERSEDED in [event.type for event in events]
    finally:
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_task_service.py -p no:warnings -q`
Expected: FAIL，`AttributeError: 'TaskService' object has no attribute 'create_pending_task'`

- [ ] **Step 3: 修改 `src/agent_hub/core/tasks.py`**

追加 import 与方法（`create_task` 保持不变）：

```python
from agent_hub.core.planner import PlanDraft, draft_to_dag
```

```python
    async def create_pending_task(self, request: str) -> str:
        task_id = uuid4().hex
        await self._events.append(
            task_id, EventType.TASK_CREATED, {"request": request, "policy": None}
        )
        return task_id

    async def create_plan_from_draft(
        self, task_id: str, draft: PlanDraft, *, version: int
    ) -> CreatedTask:
        records = await self._registry.list()
        agent_urls = {
            record.name: self._registry.agent_url(record) for record in records
        }
        plan_id = uuid4().hex
        dag = draft_to_dag(draft, agent_urls)
        if version > 1:
            previous = await projections.fetch_current_plan(self._db, task_id)
            await self._events.append(
                task_id,
                EventType.PLAN_SUPERSEDED,
                {
                    "plan_id": previous.id if previous else None,
                    "superseded_by_version": version,
                },
            )
        await self._events.append(
            task_id,
            EventType.PLAN_CREATED,
            {
                "plan_id": plan_id,
                "version": version,
                "rationale": draft.rationale,
                "dag": dag,
            },
        )
        return CreatedTask(
            task_id=task_id,
            plan_id=plan_id,
            node_ids=[f"{plan_id}:{node.id}" for node in draft.nodes],
        )

    async def mark_running(self, task_id: str) -> OrchestrationTask:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        if task.status is TaskStatus.PLANNING:
            await self._events.append(
                task_id,
                EventType.TASK_STATE_CHANGED,
                {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
            )
            refreshed = await projections.fetch_task(self._db, task_id)
            assert refreshed is not None
            return refreshed
        return task
```

- [ ] **Step 4: 运行确认通过并提交**

Run: `uv run pytest tests/integration/test_task_service.py -p no:warnings -q`
Expected: `4 passed`

```bash
git add src/agent_hub/core/tasks.py tests/integration/test_task_service.py
git commit -m "feat: TaskService 支持 Planner 路径（pending/建计划/重规划版本）"
```

---

### Task 6: Orchestrator 调度循环

**Files:**
- Create: `src/agent_hub/core/orchestrator.py`
- Modify: `tests/fake_agents/echo_agent.py`（新增 `delay`、`fail_once` 行为）
- Test: `tests/integration/test_orchestrator.py`

- [ ] **Step 1: 修改假 agent 行为**

`tests/fake_agents/echo_agent.py` 的 `ScriptedExecutor.__init__` 增加调用计数，并在 `execute` 中新增行为分支：

```python
    def __init__(self, behavior: str = "echo"):
        self._behavior = behavior
        self._calls = 0

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        text = get_message_text(context.message) if context.message else ""
        if context.current_task is None:
            self._calls += 1
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.start_work()
            if self._behavior == "ask":
                await updater.requires_input(
                    updater.new_agent_message(parts=[Part(text="who are you?")])
                )
                return
            if self._behavior == "fail" or (
                self._behavior == "fail_once" and self._calls == 1
            ):
                await updater.failed(updater.new_agent_message(parts=[Part(text="boom")]))
                return
            if self._behavior == "slow":
                await asyncio.sleep(5)
            if self._behavior == "delay":
                await asyncio.sleep(0.4)
            await updater.add_artifact(
                parts=[Part(text=f"echo:{text}")], name="response", last_chunk=True
            )
            await updater.complete()
        else:
            ...（保持原样）
```

- [ ] **Step 2: 写失败测试 `tests/integration/test_orchestrator.py`**

```python
import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import PlanDraft, PlanNodeDraft, Planner
from agent_hub.core.tasks import TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent
from tests.support.fakes import FakeLLM


def draft(*nodes: PlanNodeDraft, rationale: str = "test") -> PlanDraft:
    return PlanDraft(rationale=rationale, nodes=list(nodes))


def n(node_id: str, agent: str = "good", deps: list[str] | None = None, text: str = "x"):
    return PlanNodeDraft(
        id=node_id, name=node_id, agent_name=agent, deps=deps or [], input={"text": text}
    )


async def setup(tmp_path, llm_results, agents, *, max_parallel=5, max_attempts=2,
                backoff=0.0, replan=True):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    for name, agent in agents.items():
        await registry.register(name, agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(structured_results=llm_results)
    planner = Planner(llm, registry)
    orchestrator = Orchestrator(
        db, events, planner, dispatcher, tasks,
        max_parallel=max_parallel, max_node_attempts=max_attempts,
        retry_backoff_seconds=backoff, replan_on_failure=replan,
    )
    return db, remote, events, tasks, orchestrator, llm


async def wait_terminal(orchestrator, task_id, timeout=10.0):
    await asyncio.wait_for(orchestrator.wait(task_id), timeout)


async def test_auto_single_node(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="hi"))], {"good": agent}
    )
    try:
        task_id = await tasks.create_pending_task("hi")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "echo:hi"
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()


async def test_diamond_runs_parallel_nodes_concurrently(tmp_path):
    slow = await start_fake_agent("delay")
    fast = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path,
        [draft(n("a", text="1"), n("b", text="2"), n("c", deps=["a", "b"], text="join"))],
        {"good": slow, "fast": fast},
    )
    try:
        task_id = await tasks.create_pending_task("diamond")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED

        event_list = await events.replay(task_id)
        dispatched = [e for e in event_list if e.type is EventType.NODE_DISPATCHED]
        first_completed = next(
            e for e in event_list
            if e.type is EventType.NODE_STATE_CHANGED and e.payload.get("to") == "completed"
        )
        a_dispatch = next(e for e in dispatched if e.payload["node_id"].endswith(":a"))
        b_dispatch = next(e for e in dispatched if e.payload["node_id"].endswith(":b"))
        assert a_dispatch.seq < first_completed.seq
        assert b_dispatch.seq < first_completed.seq
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await slow.stop()
        await fast.stop()


async def test_retry_then_success(tmp_path):
    flaky = await start_fake_agent("fail_once")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1"))], {"good": flaky}, max_attempts=2
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].attempt == 2
        assert EventType.NODE_RETRY_SCHEDULED in [e.type for e in await events.replay(task_id)]
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await flaky.stop()


async def test_replan_on_permanent_failure(tmp_path):
    bad = await start_fake_agent("fail")
    good = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, llm = await setup(
        tmp_path,
        [draft(n("n1", agent="bad")), draft(n("n1", agent="good", text="retry"))],
        {"bad": bad, "good": good},
        max_attempts=1,
        replan=True,
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.plan is not None and snapshot.plan.version == 2
        types = [e.type for e in await events.replay(task_id)]
        assert types.count(EventType.PLAN_CREATED) == 2
        assert EventType.PLAN_SUPERSEDED in types
        assert len(llm.structured_calls) == 2
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await bad.stop()
        await good.stop()


async def test_planning_failure_marks_task_failed(tmp_path):
    good = await start_fake_agent("echo")
    bad_plan = draft(n("n1", agent="ghost"))
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [bad_plan, bad_plan, bad_plan], {"good": good}
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.FAILED
        assert EventType.ERROR in [e.type for e in await events.replay(task_id)]
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await good.stop()


async def test_input_required_parks_task(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="ask"))], {"good": asker}
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await wait_terminal(orchestrator, task_id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.AWAITING_INPUT
        assert snapshot.nodes[0].status is NodeStatus.INPUT_REQUIRED
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()
```

- [ ] **Step 3: 运行确认失败**

Run: `uv run pytest tests/integration/test_orchestrator.py -p no:warnings -q`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.core.orchestrator'`

- [ ] **Step 4: 实现 `src/agent_hub/core/orchestrator.py`**

```python
from __future__ import annotations

import asyncio
import json

from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.planner import PlanningFailed, Planner
from agent_hub.core.tasks import TaskService
from agent_hub.models.domain import Node, OrchestrationTask
from agent_hub.models.enums import (
    TERMINAL_TASK_STATUSES,
    EventType,
    NodeStatus,
    TaskStatus,
)
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


class Orchestrator:
    def __init__(
        self,
        db: Database,
        events: EventStore,
        planner: Planner,
        dispatcher: NodeDispatcher,
        task_service: TaskService,
        *,
        max_parallel: int = 5,
        max_node_attempts: int = 2,
        retry_backoff_seconds: float = 1.0,
        replan_on_failure: bool = True,
    ):
        self._db = db
        self._events = events
        self._planner = planner
        self._dispatcher = dispatcher
        self._task_service = task_service
        self._max_parallel = max_parallel
        self._max_node_attempts = max_node_attempts
        self._retry_backoff = retry_backoff_seconds
        self._replan_on_failure = replan_on_failure
        self._runs: dict[str, asyncio.Task] = {}
        self._inflight: set[asyncio.Task] = set()

    def start(self, task_id: str) -> None:
        if task_id in self._runs and not self._runs[task_id].done():
            return
        run = asyncio.create_task(self.run(task_id), name=f"orchestrator:{task_id}")
        self._runs[task_id] = run
        run.add_done_callback(lambda _: self._runs.pop(task_id, None))

    async def stop(self) -> None:
        pending = list(self._runs.values()) + list(self._inflight)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._runs.clear()
        self._inflight.clear()

    async def wait(self, task_id: str, timeout: float = 30.0) -> None:
        async def _poll() -> None:
            while True:
                task = await projections.fetch_task(self._db, task_id)
                if task is not None and task.status in TERMINAL_TASK_STATUSES:
                    return
                if task is not None and task.status is TaskStatus.AWAITING_INPUT:
                    return
                await asyncio.sleep(0.02)

        await asyncio.wait_for(_poll(), timeout)

    async def run(self, task_id: str) -> None:
        task = await projections.fetch_task(self._db, task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return
        if task.plan_version is None:
            await self._initial_plan(task)

        while True:
            task = await projections.fetch_task(self._db, task_id)
            if task is None or task.status in TERMINAL_TASK_STATUSES:
                return
            plan = await projections.fetch_current_plan(self._db, task_id)
            if plan is None:
                return
            nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
            self._inflight = {item for item in self._inflight if not item.done()}

            failed = [node for node in nodes if node.status is NodeStatus.FAILED]
            if failed:
                await self._handle_failure(task, failed[0])
                continue

            parked = [node for node in nodes if node.status is NodeStatus.INPUT_REQUIRED]
            if parked:
                if task.status is not TaskStatus.AWAITING_INPUT:
                    await self._events.append(
                        task_id,
                        EventType.TASK_STATE_CHANGED,
                        {
                            "from": TaskStatus.RUNNING.value,
                            "to": TaskStatus.AWAITING_INPUT.value,
                        },
                    )
                return

            ready = [
                node
                for node in nodes
                if node.status is NodeStatus.PENDING and self._deps_completed(node, nodes)
            ] + [node for node in nodes if node.status is NodeStatus.READY]

            if ready:
                slots = max(0, self._max_parallel - len(self._inflight))
                for node in ready[:slots]:
                    self._inflight.add(
                        asyncio.create_task(
                            self._dispatcher.dispatch_node(task_id, node.id)
                        )
                    )
                if self._inflight:
                    await asyncio.wait(
                        self._inflight, return_when=asyncio.FIRST_COMPLETED
                    )
                continue

            if self._inflight:
                await asyncio.wait(self._inflight, return_when=asyncio.FIRST_COMPLETED)
                continue

            if nodes and all(node.status is NodeStatus.COMPLETED for node in nodes):
                await self._task_service.finalize_if_complete(task_id)
                return

            await self._events.append(
                task_id,
                EventType.ERROR,
                {"message": "scheduler stalled: no ready nodes and no in-flight work"},
            )
            await self._events.append(task_id, EventType.TASK_FAILED, {})
            return

    async def _initial_plan(self, task: OrchestrationTask) -> None:
        try:
            drafted = await self._planner.plan(task.request)
            await self._task_service.create_plan_from_draft(task.id, drafted, version=1)
            await self._task_service.mark_running(task.id)
        except Exception as exc:  # noqa: BLE001 - 规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _handle_failure(self, task: OrchestrationTask, node: Node) -> None:
        if node.attempt < self._max_node_attempts:
            delay = self._retry_backoff * node.attempt
            await self._events.append(
                task.id,
                EventType.NODE_RETRY_SCHEDULED,
                {
                    "node_id": node.id,
                    "attempt": node.attempt + 1,
                    "delay_seconds": delay,
                },
            )
            if delay > 0:
                await asyncio.sleep(delay)
            await self._events.append(
                task.id,
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": node.id,
                    "from": node.status.value,
                    "to": NodeStatus.READY.value,
                },
            )
            return
        if self._replan_on_failure:
            await self._replan(task, node)
            return
        await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _replan(self, task: OrchestrationTask, failed_node: Node) -> None:
        plan = await projections.fetch_current_plan(self._db, task.id)
        assert plan is not None
        nodes = await projections.fetch_nodes(self._db, task.id, plan.id)
        completed = [node for node in nodes if node.status is NodeStatus.COMPLETED]
        context = "\n".join(
            f"- {node.name}: {json.dumps(node.output, ensure_ascii=False)}"
            for node in completed
        )
        reason = f"node '{failed_node.name}' failed: {failed_node.error or 'unknown error'}"
        try:
            drafted = await self._planner.plan(task.request, reason=reason, context=context)
            await self._task_service.create_plan_from_draft(
                task.id, drafted, version=plan.version + 1
            )
        except Exception as exc:  # noqa: BLE001 - 重规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    @staticmethod
    def _deps_completed(node: Node, nodes: list[Node]) -> bool:
        by_id = {item.id: item for item in nodes}
        return all(
            dep in by_id and by_id[dep].status is NodeStatus.COMPLETED
            for dep in node.deps
        )
```

- [ ] **Step 5: 运行确认通过**

Run: `uv run pytest tests/integration/test_orchestrator.py -p no:warnings -q`
Expected: `6 passed`

- [ ] **Step 6: Commit**

```bash
git add src/agent_hub/core/orchestrator.py tests/fake_agents/echo_agent.py tests/integration/test_orchestrator.py
git commit -m "feat: Orchestrator 调度循环（并行、重试、重规划、停车）"
```

---

### Task 7: API 接入 Planner 自动路径

**Files:**
- Modify: `src/agent_hub/api/schemas.py`
- Modify: `src/agent_hub/api/tasks.py`
- Modify: `src/agent_hub/api/app.py`
- Test: `tests/integration/test_api.py`（追加）

- [ ] **Step 1: 追加失败测试到 `tests/integration/test_api.py`**

```python
import asyncio

from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


@pytest.fixture
async def api_auto(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="auto",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    llm = FakeLLM(structured_results=[plan])
    settings = Settings(
        store={"db_path": tmp_path / "auto.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=llm)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def test_planner_path_runs_to_completion(api_auto):
    client, _, agent_url = api_auto
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    resp = await client.post("/v1/tasks", json={"request": "hi"})
    assert resp.status_code == 201
    task_id = resp.json()["task_id"]

    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("task did not complete in time")
    assert snapshot["nodes"][0]["output"]["artifacts"][0]["text"] == "echo:hi"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_api.py -p no:warnings -q`
Expected: FAIL，`create_app() got an unexpected keyword argument 'llm'` 或 422（target 必填）

- [ ] **Step 3: 修改 `src/agent_hub/api/schemas.py`**

```python
class CreateTaskIn(BaseModel):
    request: str
    target: TargetIn | None = None


class CreateTaskOut(BaseModel):
    task_id: str
    plan_id: str | None = None
    node_ids: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: 修改 `src/agent_hub/api/tasks.py` 的 `create_task`**

```python
from agent_hub.api.schemas import CreateTaskIn, CreateTaskOut


@router.post("/tasks", status_code=201, response_model=CreateTaskOut)
async def create_task(body: CreateTaskIn, request: Request) -> CreateTaskOut:
    service = request.app.state.task_service
    orchestrator = request.app.state.orchestrator
    if body.target is not None:
        target = TargetSpec(
            agent_name=body.target.agent_name,
            skill_id=body.target.skill_id,
            name=body.target.name,
            input=body.target.input,
        )
        try:
            created = await service.create_task(body.request, target)
        except UnknownAgent as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return CreateTaskOut(
            task_id=created.task_id,
            plan_id=created.plan_id,
            node_ids=created.node_ids,
        )
    task_id = await service.create_pending_task(body.request)
    orchestrator.start(task_id)
    return CreateTaskOut(task_id=task_id)
```

- [ ] **Step 5: 修改 `src/agent_hub/api/app.py`**

签名改为 `create_app(settings=None, llm=None)`，装配 planner/orchestrator：

```python
from agent_hub.core.llm import LLMClient, LiteLLMClient
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import Planner


def create_app(settings: Settings | None = None, llm: LLMClient | None = None) -> FastAPI:
    resolved = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(resolved.store.db_path)
        await db.initialize()
        remote = RemoteAgentClient()
        bus = EventBus()
        event_store = EventStore(db, bus=bus)
        registry = AgentRegistry(db, remote)
        task_service = TaskService(db, event_store, registry)
        dispatcher = NodeDispatcher(
            db, event_store, remote,
            timeout_seconds=resolved.scheduler.node_timeout_seconds,
        )
        llm_client = llm or LiteLLMClient(
            model=resolved.llm.planner_model,
            timeout_seconds=resolved.llm.timeout_seconds,
        )
        planner = Planner(
            llm_client, registry,
            max_nodes=resolved.scheduler.max_plan_nodes,
            max_retries=resolved.llm.max_plan_retries,
        )
        orchestrator = Orchestrator(
            db, event_store, planner, dispatcher, task_service,
            max_parallel=resolved.scheduler.max_parallel_nodes,
            max_node_attempts=resolved.scheduler.max_node_attempts,
            retry_backoff_seconds=resolved.scheduler.retry_backoff_seconds,
            replan_on_failure=resolved.scheduler.replan_on_failure,
        )

        app.state.settings = resolved
        app.state.db = db
        app.state.remote = remote
        app.state.event_bus = bus
        app.state.event_store = event_store
        app.state.registry = registry
        app.state.task_service = task_service
        app.state.dispatcher = dispatcher
        app.state.orchestrator = orchestrator
        try:
            yield
        finally:
            await orchestrator.stop()
            await remote.close()
            await db.close()
    ...
```

并在 import 中补 `from agent_hub.core.events import EventBus`。注意：`EventBus` 在 Task 8 实现，本任务先创建 `core/events.py` 的最小版本（Task 8 再补测试与完整语义）：

```python
from __future__ import annotations

import asyncio

from agent_hub.store.event_store import Event

_CLOSED = object()


class SubscriptionClosed(RuntimeError):
    pass


class EventSubscription:
    def __init__(self, task_id: str, maxsize: int):
        self.task_id = task_id
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._closed = False

    def offer(self, event: Event) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._replace_with_closed()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(_CLOSED)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                pass

    def _replace_with_closed(self) -> None:
        self._closed = True
        self._queue = asyncio.Queue()
        self._queue.put_nowait(_CLOSED)

    async def get(self) -> Event:
        item = await self._queue.get()
        if item is _CLOSED:
            raise SubscriptionClosed()
        return item


class EventBus:
    def __init__(self, max_queue_size: int = 256):
        self._max_queue_size = max_queue_size
        self._subscriptions: dict[str, set[EventSubscription]] = {}

    def subscribe(self, task_id: str) -> EventSubscription:
        subscription = EventSubscription(task_id, self._max_queue_size)
        self._subscriptions.setdefault(task_id, set()).add(subscription)
        return subscription

    def unsubscribe(self, subscription: EventSubscription) -> None:
        subs = self._subscriptions.get(subscription.task_id)
        if subs is not None:
            subs.discard(subscription)
            if not subs:
                self._subscriptions.pop(subscription.task_id, None)

    def publish(self, event: Event) -> None:
        for subscription in list(self._subscriptions.get(event.task_id, ())):
            subscription.offer(event)

    def close_all(self) -> None:
        for subs in list(self._subscriptions.values()):
            for subscription in list(subs):
                subscription.close()
        self._subscriptions.clear()
```

修改 `EventStore.__init__` 为 `def __init__(self, db: Database, bus: EventBus | None = None)`，并在 `append` 的事务内、插入与投影之后发布：

```python
        async with self._db.transaction() as conn:
            ...
            await apply_event(conn, event)
            if self._bus is not None:
                self._bus.publish(event)
        return event
```

（循环 import 注意：`core/events.py` 从 `store/event_store.py` import `Event`；`store/event_store.py` 不 import core，避免环。）

- [ ] **Step 6: 运行确认通过并提交**

Run: `uv run pytest tests/integration/test_api.py -p no:warnings -q`
Expected: `5 passed`

```bash
git add src/agent_hub/api/ src/agent_hub/core/events.py src/agent_hub/store/event_store.py tests/integration/test_api.py
git commit -m "feat: API 接入 Planner 自动路径与 EventBus"
```

---

### Task 8: EventBus 单元测试

**Files:**
- Test: `tests/unit/test_events.py`

- [ ] **Step 1: 写测试 `tests/unit/test_events.py`**

```python
import asyncio
from datetime import UTC, datetime

from agent_hub.core.events import EventBus, SubscriptionClosed
from agent_hub.models.enums import EventType
from agent_hub.store.event_store import Event


def make_event(seq: int, task_id: str = "t1") -> Event:
    return Event(
        seq=seq,
        task_id=task_id,
        type=EventType.NODE_STATE_CHANGED,
        payload={"seq": seq},
        created_at=datetime.now(UTC),
    )


async def test_publish_delivers_to_subscriber():
    bus = EventBus()
    sub = bus.subscribe("t1")
    bus.publish(make_event(1))
    bus.publish(make_event(2))
    assert (await sub.get()).seq == 1
    assert (await sub.get()).seq == 2


async def test_subscribers_are_isolated_by_task():
    bus = EventBus()
    sub = bus.subscribe("t1")
    bus.publish(make_event(1, task_id="t2"))
    bus.publish(make_event(2, task_id="t1"))
    assert (await sub.get()).seq == 2


async def test_slow_consumer_is_closed():
    bus = EventBus(max_queue_size=2)
    sub = bus.subscribe("t1")
    for seq in range(5):
        bus.publish(make_event(seq))
    with pytest.raises(SubscriptionClosed):
        await sub.get()


async def test_close_unblocks_waiting_get():
    bus = EventBus()
    sub = bus.subscribe("t1")

    async def wait() -> None:
        await sub.get()

    waiter = asyncio.create_task(wait())
    await asyncio.sleep(0.01)
    sub.close()
    with pytest.raises(SubscriptionClosed):
        await waiter
```

顶部需要 `import pytest`。

- [ ] **Step 2: 运行确认通过**

Run: `uv run pytest tests/unit/test_events.py -p no:warnings -q`
Expected: `4 passed`（实现在 Task 7 已就位；如失败按错误修正 `core/events.py`）

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_events.py
git commit -m "test: EventBus 发布订阅、慢消费者与关闭语义"
```

---

### Task 9: SSE 端点

**Files:**
- Create: `src/agent_hub/api/sse.py`
- Modify: `src/agent_hub/api/app.py`（include router）
- Test: `tests/integration/test_sse.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_sse.py`**

```python
import asyncio
import json

import httpx
import pytest
from sse_starlette.sse import EventSourceResponse  # noqa: F401  仅示意依赖存在

from agent_hub.api.app import create_app
from agent_hub.config import Settings


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(store={"db_path": tmp_path / "sse.db"})
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def read_until_completed(response, timeout=5.0):
    events = []
    current: dict = {}

    async def _read():
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

    await asyncio.wait_for(_read(), timeout)
    return events


async def test_sse_replays_history(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    await client.post(
        f"/v1/tasks/{created['task_id']}/nodes/{created['node_ids'][0]}/dispatch"
    )

    async with client.stream(
        "GET", f"/v1/tasks/{created['task_id']}/events?after_seq=0"
    ) as response:
        assert response.status_code == 200
        events = await read_until_completed(response)

    ids = [event["id"] for event in events]
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids))
    assert events[0]["event"] == "task.created"
    assert events[-1]["event"] == "task.completed"
    assert any(event["event"] == "node.dispatched" for event in events)


async def test_sse_streams_live_events(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    task_id = created["task_id"]
    node_id = created["node_ids"][0]
    after = await app.state.event_store.latest_seq(task_id)

    async with client.stream(
        "GET", f"/v1/tasks/{task_id}/events?after_seq={after}"
    ) as response:
        dispatch = asyncio.create_task(
            client.post(f"/v1/tasks/{task_id}/nodes/{node_id}/dispatch")
        )
        events = await read_until_completed(response)
        await dispatch

    assert events, "expected live events"
    assert all(event["id"] > after for event in events)
    assert events[-1]["event"] == "task.completed"


async def test_sse_resume_with_last_event_id(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    task_id = created["task_id"]
    await client.post(f"/v1/tasks/{task_id}/nodes/{created['node_ids'][0]}/dispatch")
    first_seq = (await app.state.event_store.replay(task_id))[0].seq

    async with client.stream(
        "GET",
        f"/v1/tasks/{task_id}/events",
        headers={"Last-Event-ID": str(first_seq)},
    ) as response:
        events = await read_until_completed(response)

    assert all(event["id"] > first_seq for event in events)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_sse.py -p no:warnings -q`
Expected: FAIL 404（路由不存在）

- [ ] **Step 3: 实现 `src/agent_hub/api/sse.py`**

```python
from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.enums import EventType

router = APIRouter(tags=["events"])


def format_event(event_type: EventType, seq: int, payload: dict) -> dict[str, str]:
    return {
        "event": event_type.value,
        "id": str(seq),
        "data": json.dumps(payload, ensure_ascii=False),
    }


@router.get("/tasks/{task_id}/events")
async def task_events(task_id: str, request: Request, after_seq: int = 0):
    service = request.app.state.task_service
    try:
        await service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc

    store = request.app.state.event_store
    bus = request.app.state.event_bus
    last_event_id = request.headers.get("last-event-id")
    start = after_seq
    if last_event_id is not None and last_event_id.isdigit():
        start = max(start, int(last_event_id))

    async def stream():
        subscription = bus.subscribe(task_id)
        try:
            seen = start
            for event in await store.replay(task_id, start):
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
            while True:
                try:
                    event = await subscription.get()
                except Exception:
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

需要在 `src/agent_hub/api/app.py` 中 include：

```python
from agent_hub.api import sse as sse_routes
...
app.include_router(sse_routes.router, prefix="/v1")
```

注意：`except Exception` 会连 `SubscriptionClosed` 一起捕获，如 lint 报 BLE001，改为 `except SubscriptionClosed` 并 import。

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_sse.py -p no:warnings -q`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/api/sse.py src/agent_hub/api/app.py tests/integration/test_sse.py
git commit -m "feat: SSE 事件流（回放、实时、Last-Event-ID 续传）"
```

---

### Task 10: README、全量验证与收尾

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 更新 `README.md`**

把「当前进度」改为 M1–M3 完成，并补充自动规划与 SSE 示例：

```markdown
当前进度：**M1 骨架 + M2 规划调度 + M3 SSE**（HITL、断点恢复、回退见后续计划）。

## 自动规划

不传 `target` 时由 Planner（LLM）拆解 DAG 并自动调度：

curl -X POST localhost:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"调研 A2A 协议并写一份摘要"}'

## SSE

curl -N localhost:8080/v1/tasks/<task_id>/events
# 断线重连：携带 Last-Event-ID 头或 ?after_seq=，自动回放缺失事件
```

- [ ] **Step 2: 全量验证**

Run: `uv run pytest -p no:warnings -q`
Expected: 全部通过（约 55 个用例）。

Run: `uv run ruff check .`
Expected: `All checks passed!`（如有告警，修复后重跑）

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: M2/M3 README 更新"
```

---

## Plan 2 完成标准（对照 spec M2/M3）

- [ ] 不传 `target` 时 LLM 自动生成 DAG 并调度到完成；PlanDraft 校验含环检测、未知 agent/skill、节点上限。
- [ ] 独立节点并行（测试断言两个节点均在首个完成事件前派发）。
- [ ] 失败节点先重试（有上限与退避），仍失败触发重规划（新 plan version + `plan.superseded`）。
- [ ] `input-required` 节点使任务进入 `awaiting_input` 并停车（有测试）。
- [ ] SSE 支持历史回放、实时推送、`Last-Event-ID`/`after_seq` 续传；慢消费者被断开且不影响其他订阅者（单元测试）。
- [ ] `uv run pytest` 与 `uv run ruff check .` 全绿。

---

## 执行勘误（2026-09-12）

执行期间发现并修正的偏差，实际代码以此为准：

1. **SSE 集成测试必须用真实 uvicorn**：httpx `ASGITransport` 会缓冲完整响应体，无法读取无限 SSE 流；`tests/integration/test_sse.py` 改为在测试内启动 uvicorn（`free_port()` 辅助），平台其余 API 测试仍用 ASGITransport。
2. **sse-starlette 全局状态污染**：`AppStatus.should_exit` 是进程级类变量，其 watcher 通过 SIGTERM handler 反射 uvicorn Server；测试内顺序启停多个假 agent 时旧 server 退出会置位该全局，导致后续 SSE 流被提前终止。修复：`FakeAgent.stop()` 与 `tests/conftest.py` 的 autouse fixture 中重置 `AppStatus.should_exit = False`。
3. **EventBus 实现顺序**：先写 `tests/unit/test_events.py`（红）再实现 `core/events.py`，随后 Task 7 接线。
4. **ruff ASYNC109**：`Orchestrator.wait` 与 SSE 测试辅助函数的 `timeout` 参数更名为 `timeout_seconds`。
5. **`read_until_completed` 闭包 bug**：`current` 字典移入内层函数，避免 UnboundLocalError。
