# A2A 编排平台 MVP — Plan 1：骨架与单节点派发

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建 Agent Hub 的可运行骨架：配置、SQLite 事件存储与投影、A2A 客户端封装、Agent 注册表、假 A2A 测试 agent，以及「提交任务 → 手动派发单节点 → 远程完成 → 事件落库」的端到端链路。

**Architecture:** 事件日志（SQLite）为唯一事实源，所有状态变化先 `append` 事件、在同一事务内更新投影表；A2A 交互封装在 `RemoteAgentClient` 单点；本计划不包含 LLM 规划器与自动调度（Plan 2）、SSE 与 HITL（Plan 3/4）、恢复与回退（Plan 4）。任务由 `target` 参数直接指定 agent，产出一个单节点计划。

**Tech Stack:** Python 3.12（uv 管理）、a2a-sdk 1.1.x（`[http-server]`）、FastAPI、aiosqlite、httpx、pydantic v2 + pydantic-settings、structlog、pytest + pytest-asyncio。

**Spec:** `docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`

**计划拆分（本仓库共 4 份计划，按里程碑顺序执行）：**

- **Plan 1（本文档）**：M1 骨架与单节点派发
- Plan 2：M2 规划与 DAG 调度 + M3 SSE
- Plan 3：M4 HITL
- Plan 4：M5 断点恢复与对账 + M6 回退与 retry + M7 收尾

**前置条件：** `uv` 已安装（已验证 0.11.7）；可访问 PyPI；M1 不需要任何 LLM API Key。

**关键约定（贯穿全部任务）：**

- 节点 ID 格式：`"{plan_id}:{dag_node_id}"`（例如 `ab12:n1`），由计划物化时确定性生成。
- 事件类型见 `models/enums.py`；终态用专用事件 `task.completed` / `task.failed`，非终态流转用 `task.state_changed`。
- 远程 A2A 调用：`contextId = 编排任务 ID`，`messageId = "{task_id}:{node_id}:{attempt}"`（为 Plan 4 的崩溃对账预留确定性）。
- 时间戳统一 UTC ISO8601 字符串。
- 每次提交前跑 `uv run pytest`，保持全绿。

---

## 文件结构（Plan 1 完成后）

```
pyproject.toml                  # uv 项目定义、pytest/ruff 配置
.python-version                 # 3.12
.gitignore
config.example.yaml
README.md
src/agent_hub/
  __init__.py                   # __version__
  config.py                     # Settings、load_settings
  main.py                       # uvicorn 入口
  core/
    state.py                    # 任务/节点状态机与断言
    tasks.py                    # TaskService：建任务、单节点计划、快照、终态判定
    dispatcher.py               # NodeDispatcher：事件映射、超时、输出收集
  models/
    enums.py                    # TaskStatus/NodeStatus/EventType/InterventionStatus
    domain.py                   # 投影模型：OrchestrationTask/Plan/Node/...
  a2a/
    client.py                   # RemoteAgentClient：card 解析、客户端缓存、send_text
    registry.py                 # AgentRegistry：注册/查询/刷新
  store/
    db.py                       # Database：连接、schema、事务
    event_store.py              # append/replay/latest_seq
    projections.py              # apply_event/rebuild/fetch_* 读模型
  api/
    app.py                      # create_app + lifespan 装配
    schemas.py                  # 请求体模型
    tasks.py                    # /v1/tasks 路由
    agents.py                   # /v1/agents 路由
tests/
  conftest.py                   # echo_agent 等公共 fixture
  unit/                         # 配置、DB、状态机、事件存储
  integration/                  # fake agent / 客户端 / registry / service / dispatcher / API
  fake_agents/echo_agent.py     # 基于 a2a-sdk 的可控假 agent
```

---

### Task 1: 项目骨架（uv + pyproject + 冒烟测试）

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `src/agent_hub/__init__.py`
- Create: `tests/__init__.py`
- Test: `tests/test_smoke.py`

- [ ] **Step 1: 写 `pyproject.toml`**

```toml
[project]
name = "agent-hub"
version = "0.1.0"
description = "A2A multi-agent orchestration platform (MVP)"
requires-python = ">=3.12"
dependencies = [
    "a2a-sdk[http-server]>=1.1,<2",
    "aiosqlite>=0.20",
    "fastapi>=0.115",
    "httpx>=0.28",
    "pydantic>=2.11",
    "pydantic-settings>=2.6",
    "pyyaml>=6.0",
    "structlog>=24.4",
    "uvicorn>=0.30",
]

[project.scripts]
agent-hub = "agent_hub.main:main"

[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "ruff>=0.8",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/agent_hub"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]
```

- [ ] **Step 2: 创建包占位文件**

`src/agent_hub/__init__.py`：

```python
"""Agent Hub：A2A 多 Agent 编排平台。"""
```

`tests/__init__.py`（空文件），`tests/unit/__init__.py`（空文件），`tests/integration/__init__.py`（空文件），`tests/fake_agents/__init__.py`（空文件）。

- [ ] **Step 3: 写失败冒烟测试 `tests/test_smoke.py`**

```python
import agent_hub


def test_package_importable():
    assert agent_hub.__version__ == "0.1.0"
```

- [ ] **Step 4: 初始化环境并确认测试失败**

Run:
```bash
uv python pin 3.12 && uv sync && uv run pytest tests/test_smoke.py -v
```
Expected: 安装成功；测试 FAIL，报 `AttributeError: module 'agent_hub' has no attribute '__version__'`。

- [ ] **Step 5: 补上版本号使测试通过**

`src/agent_hub/__init__.py`：

```python
"""Agent Hub：A2A 多 Agent 编排平台。"""

__version__ = "0.1.0"
```

- [ ] **Step 6: 写 `.gitignore`**

```gitignore
.venv/
__pycache__/
*.py[co]
.pytest_cache/
.ruff_cache/
data/
```

- [ ] **Step 7: 运行测试确认通过**

Run: `uv run pytest tests/test_smoke.py -v`
Expected: `1 passed`

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml .python-version .gitignore src/agent_hub/__init__.py tests/
git commit -m "chore: 初始化 uv 项目骨架与冒烟测试"
```

---

### Task 2: 配置系统

**Files:**
- Create: `src/agent_hub/config.py`
- Test: `tests/unit/test_config.py`
- Create: `config.example.yaml`

- [ ] **Step 1: 写失败测试 `tests/unit/test_config.py`**

```python
from pathlib import Path

from agent_hub.config import Settings, load_settings


def test_defaults():
    settings = Settings()
    assert settings.server.host == "127.0.0.1"
    assert settings.server.port == 8080
    assert settings.scheduler.node_timeout_seconds == 600.0
    assert settings.store.db_path == Path("./data/agent_hub.db")


def test_env_override(monkeypatch):
    monkeypatch.setenv("AGENT_HUB_SERVER__PORT", "9999")
    monkeypatch.setenv("AGENT_HUB_STORE__DB_PATH", "/tmp/agent_hub_test.db")
    settings = Settings()
    assert settings.server.port == 9999
    assert str(settings.store.db_path) == "/tmp/agent_hub_test.db"


def test_yaml_load(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "server:\n  port: 7777\nscheduler:\n  max_parallel_nodes: 3\n",
        encoding="utf-8",
    )
    settings = load_settings(config_file)
    assert settings.server.port == 7777
    assert settings.scheduler.max_parallel_nodes == 3
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.config'`

- [ ] **Step 3: 实现 `src/agent_hub/config.py`**

```python
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8080


class SchedulerConfig(BaseModel):
    max_parallel_nodes: int = 5
    node_timeout_seconds: float = 600.0
    max_node_attempts: int = 2


class StoreConfig(BaseModel):
    db_path: Path = Path("./data/agent_hub.db")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AGENT_HUB_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    server: ServerConfig = Field(default_factory=ServerConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)


def load_settings(yaml_path: Path | str | None = None) -> Settings:
    if yaml_path is None:
        return Settings()
    data = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}
    return Settings(**data)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: `3 passed`

- [ ] **Step 5: 写 `config.example.yaml`**

```yaml
server:
  host: 127.0.0.1
  port: 8080

scheduler:
  max_parallel_nodes: 5
  node_timeout_seconds: 600
  max_node_attempts: 2

store:
  db_path: ./data/agent_hub.db
```

- [ ] **Step 6: Commit**

```bash
git add src/agent_hub/config.py tests/unit/test_config.py config.example.yaml
git commit -m "feat: 配置系统（环境变量 + YAML）"
```

---

### Task 3: 数据库与 schema

**Files:**
- Create: `src/agent_hub/store/__init__.py`（空）
- Create: `src/agent_hub/store/db.py`
- Test: `tests/unit/test_db.py`

- [ ] **Step 1: 写失败测试 `tests/unit/test_db.py`**

```python
import pytest

from agent_hub.store.db import Database


async def test_initialize_creates_tables_and_wal(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()

    cursor = await db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    names = {row["name"] for row in await cursor.fetchall()}
    assert {
        "events",
        "orchestration_tasks",
        "plans",
        "nodes",
        "interventions",
        "checkpoints",
        "agent_registry",
    } <= names

    cursor = await db.conn.execute("PRAGMA journal_mode")
    mode = (await cursor.fetchone())[0]
    assert mode.lower() == "wal"
    await db.close()


async def test_transaction_rolls_back_on_error(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    with pytest.raises(RuntimeError):
        async with db.transaction() as conn:
            await conn.execute(
                "INSERT INTO agent_registry (id, name, card_url, card, created_at)"
                " VALUES ('a1', 'a', 'http://x', '{}', '2026-01-01T00:00:00+00:00')"
            )
            raise RuntimeError("boom")
    cursor = await db.conn.execute("SELECT COUNT(*) AS c FROM agent_registry")
    assert (await cursor.fetchone())["c"] == 0
    await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_db.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.store'`

- [ ] **Step 3: 实现 `src/agent_hub/store/db.py`**

```python
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT NOT NULL,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq);

CREATE TABLE IF NOT EXISTS orchestration_tasks (
  id           TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  request      TEXT NOT NULL,
  policy       TEXT,
  plan_version INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plans (
  id         TEXT PRIMARY KEY,
  task_id    TEXT NOT NULL,
  version    INTEGER NOT NULL,
  dag        TEXT NOT NULL,
  rationale  TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, version)
);

CREATE TABLE IF NOT EXISTS nodes (
  id                TEXT PRIMARY KEY,
  task_id           TEXT NOT NULL,
  plan_id           TEXT NOT NULL,
  name              TEXT NOT NULL,
  agent_url         TEXT,
  skill_id          TEXT,
  deps              TEXT NOT NULL,
  input             TEXT,
  status            TEXT NOT NULL,
  attempt           INTEGER NOT NULL DEFAULT 0,
  requires_approval INTEGER NOT NULL DEFAULT 0,
  a2a_task_id       TEXT,
  a2a_context_id    TEXT,
  output            TEXT,
  error             TEXT,
  started_at        TEXT,
  ended_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_nodes_task ON nodes(task_id);
CREATE INDEX IF NOT EXISTS idx_nodes_a2a ON nodes(a2a_task_id);

CREATE TABLE IF NOT EXISTS interventions (
  id          TEXT PRIMARY KEY,
  task_id     TEXT NOT NULL,
  node_id     TEXT,
  source      TEXT NOT NULL,
  policy      TEXT NOT NULL,
  question    TEXT NOT NULL,
  answer      TEXT,
  responder   TEXT,
  status      TEXT NOT NULL,
  deadline_at TEXT,
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_interventions_task_status
  ON interventions(task_id, status);

CREATE TABLE IF NOT EXISTS checkpoints (
  id           TEXT PRIMARY KEY,
  task_id      TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  plan_version INTEGER NOT NULL,
  frontier     TEXT NOT NULL,
  artifacts    TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_registry (
  id         TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  card_url   TEXT NOT NULL,
  card       TEXT NOT NULL,
  health     TEXT NOT NULL DEFAULT 'unknown',
  last_seen  TEXT,
  created_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path | str):
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None
        self._tx_lock = asyncio.Lock()

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.commit()

    async def initialize(self) -> None:
        if self._conn is None:
            await self.connect()
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database is not connected; call initialize() first")
        return self._conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._tx_lock:
            conn = self.conn
            await conn.execute("BEGIN")
            try:
                yield conn
            except BaseException:
                await conn.rollback()
                raise
            else:
                await conn.commit()
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/unit/test_db.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/store/__init__.py src/agent_hub/store/db.py tests/unit/test_db.py
git commit -m "feat: SQLite schema 与事务化 Database"
```

---

### Task 4: 领域枚举、投影模型与状态机

**Files:**
- Create: `src/agent_hub/models/__init__.py`（空）
- Create: `src/agent_hub/models/enums.py`
- Create: `src/agent_hub/models/domain.py`
- Create: `src/agent_hub/core/__init__.py`（空）
- Create: `src/agent_hub/core/state.py`
- Test: `tests/unit/test_state.py`

- [ ] **Step 1: 写失败测试 `tests/unit/test_state.py`**

```python
import pytest

from agent_hub.core.state import (
    InvalidTransition,
    assert_node_transition,
    assert_task_transition,
    can_node_transition,
    can_task_transition,
)
from agent_hub.models.enums import NodeStatus, TaskStatus


def test_task_legal_transitions():
    assert can_task_transition(TaskStatus.PENDING, TaskStatus.PLANNING)
    assert can_task_transition(TaskStatus.PLANNING, TaskStatus.RUNNING)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.AWAITING_INPUT)
    assert can_task_transition(TaskStatus.AWAITING_INPUT, TaskStatus.RUNNING)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.COMPLETED)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.CANCELED)


def test_task_illegal_transitions():
    assert not can_task_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
    assert not can_task_transition(TaskStatus.COMPLETED, TaskStatus.RUNNING)
    with pytest.raises(InvalidTransition):
        assert_task_transition(TaskStatus.COMPLETED, TaskStatus.RUNNING)


def test_node_legal_transitions():
    assert can_node_transition(NodeStatus.PENDING, NodeStatus.READY)
    assert can_node_transition(NodeStatus.READY, NodeStatus.DISPATCHED)
    assert can_node_transition(NodeStatus.DISPATCHED, NodeStatus.WORKING)
    assert can_node_transition(NodeStatus.WORKING, NodeStatus.INPUT_REQUIRED)
    assert can_node_transition(NodeStatus.INPUT_REQUIRED, NodeStatus.WORKING)
    assert can_node_transition(NodeStatus.WORKING, NodeStatus.COMPLETED)
    assert can_node_transition(NodeStatus.READY, NodeStatus.FAILED)
    assert can_node_transition(NodeStatus.FAILED, NodeStatus.READY)


def test_node_invalidated_from_every_non_terminal_state():
    for status in [
        NodeStatus.PENDING,
        NodeStatus.READY,
        NodeStatus.DISPATCHED,
        NodeStatus.WORKING,
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.COMPLETED,
        NodeStatus.FAILED,
        NodeStatus.CANCELED,
    ]:
        assert can_node_transition(status, NodeStatus.INVALIDATED)


def test_node_cannot_leave_invalidated():
    with pytest.raises(InvalidTransition):
        assert_node_transition(NodeStatus.INVALIDATED, NodeStatus.READY)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_state.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.models'`

- [ ] **Step 3: 实现 `src/agent_hub/models/enums.py`**

```python
from __future__ import annotations

from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    PLANNING = "planning"
    RUNNING = "running"
    AWAITING_INPUT = "awaiting_input"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class NodeStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    WORKING = "working"
    INPUT_REQUIRED = "input_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    INVALIDATED = "invalidated"


class InterventionStatus(str, Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"


class EventType(str, Enum):
    TASK_CREATED = "task.created"
    TASK_STATE_CHANGED = "task.state_changed"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"

    PLAN_CREATED = "plan.created"
    PLAN_SUPERSEDED = "plan.superseded"

    NODE_DISPATCH_INTENT = "node.dispatch.intent"
    NODE_DISPATCHED = "node.dispatched"
    NODE_STATE_CHANGED = "node.state_changed"
    NODE_ARTIFACT = "node.artifact"
    NODE_OUTPUT = "node.output"
    NODE_RETRY_SCHEDULED = "node.retry.scheduled"
    NODE_INVALIDATED = "node.invalidated"
    NODE_CANCEL_SENT = "node.cancel.sent"

    INTERVENTION_REQUESTED = "intervention.requested"
    INTERVENTION_RESOLVED = "intervention.resolved"

    CHECKPOINT_CREATED = "checkpoint.created"
    ROLLBACK_PERFORMED = "rollback.performed"

    ERROR = "error"


TERMINAL_TASK_STATUSES = {
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELED,
}

TERMINAL_NODE_STATUSES = {
    NodeStatus.COMPLETED,
    NodeStatus.FAILED,
    NodeStatus.CANCELED,
    NodeStatus.INVALIDATED,
}
```

- [ ] **Step 4: 实现 `src/agent_hub/models/domain.py`**

```python
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from agent_hub.models.enums import InterventionStatus, NodeStatus, TaskStatus


class OrchestrationTask(BaseModel):
    id: str
    status: TaskStatus
    request: str
    policy: dict[str, Any] | None = None
    plan_version: int | None = None
    created_at: datetime
    updated_at: datetime


class Plan(BaseModel):
    id: str
    task_id: str
    version: int
    dag: dict[str, Any]
    rationale: str | None = None
    created_at: datetime


class Node(BaseModel):
    id: str
    task_id: str
    plan_id: str
    name: str
    agent_url: str | None = None
    skill_id: str | None = None
    deps: list[str] = Field(default_factory=list)
    input: dict[str, Any] | None = None
    status: NodeStatus = NodeStatus.PENDING
    attempt: int = 0
    requires_approval: bool = False
    a2a_task_id: str | None = None
    a2a_context_id: str | None = None
    output: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


class Intervention(BaseModel):
    id: str
    task_id: str
    node_id: str | None = None
    source: str
    policy: str
    question: dict[str, Any]
    answer: dict[str, Any] | None = None
    responder: str | None = None
    status: InterventionStatus = InterventionStatus.PENDING
    deadline_at: datetime | None = None
    created_at: datetime
    resolved_at: datetime | None = None


class Checkpoint(BaseModel):
    id: str
    task_id: str
    seq: int
    plan_version: int
    frontier: list[str]
    artifacts: dict[str, Any]
    created_at: datetime


class AgentRecord(BaseModel):
    id: str
    name: str
    card_url: str
    card: dict[str, Any]
    health: str = "unknown"
    last_seen: datetime | None = None
    created_at: datetime
```

- [ ] **Step 5: 实现 `src/agent_hub/core/state.py`**

```python
from __future__ import annotations

from agent_hub.models.enums import NodeStatus, TaskStatus


class InvalidTransition(RuntimeError):
    pass


_TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.PLANNING},
    TaskStatus.PLANNING: {
        TaskStatus.RUNNING,
        TaskStatus.AWAITING_INPUT,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.RUNNING: {
        TaskStatus.AWAITING_INPUT,
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.AWAITING_INPUT: {
        TaskStatus.PLANNING,
        TaskStatus.RUNNING,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.COMPLETED: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.CANCELED: set(),
}

_NODE_TRANSITIONS: dict[NodeStatus, set[NodeStatus]] = {
    NodeStatus.PENDING: {NodeStatus.READY, NodeStatus.INVALIDATED},
    NodeStatus.READY: {
        NodeStatus.DISPATCHED,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.DISPATCHED: {
        NodeStatus.WORKING,
        NodeStatus.FAILED,
        NodeStatus.COMPLETED,
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.WORKING: {
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.COMPLETED,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.INPUT_REQUIRED: {
        NodeStatus.WORKING,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.COMPLETED: {NodeStatus.INVALIDATED},
    NodeStatus.FAILED: {NodeStatus.READY, NodeStatus.INVALIDATED},
    NodeStatus.CANCELED: {NodeStatus.INVALIDATED},
    NodeStatus.INVALIDATED: set(),
}


def can_task_transition(source: TaskStatus, target: TaskStatus) -> bool:
    return target in _TASK_TRANSITIONS[source]


def assert_task_transition(source: TaskStatus, target: TaskStatus) -> None:
    if not can_task_transition(source, target):
        raise InvalidTransition(f"illegal task transition: {source.value} -> {target.value}")


def can_node_transition(source: NodeStatus, target: NodeStatus) -> bool:
    return target in _NODE_TRANSITIONS[source]


def assert_node_transition(source: NodeStatus, target: NodeStatus) -> None:
    if not can_node_transition(source, target):
        raise InvalidTransition(f"illegal node transition: {source.value} -> {target.value}")
```

- [ ] **Step 6: 运行确认通过**

Run: `uv run pytest tests/unit/test_state.py -v`
Expected: `5 passed`

- [ ] **Step 7: Commit**

```bash
git add src/agent_hub/models/ src/agent_hub/core/__init__.py src/agent_hub/core/state.py tests/unit/test_state.py
git commit -m "feat: 领域模型、枚举与状态机"
```

---

### Task 5: 事件存储与投影

**Files:**
- Create: `src/agent_hub/store/projections.py`
- Create: `src/agent_hub/store/event_store.py`
- Test: `tests/unit/test_event_store.py`

- [ ] **Step 1: 写失败测试 `tests/unit/test_event_store.py`**

```python
from typing import Any

from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


async def make_store(tmp_path) -> tuple[Database, EventStore]:
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    return db, EventStore(db)


async def seed_task(store: EventStore) -> tuple[str, str]:
    task_id = "t1"
    plan_id = "p1"
    await store.append(task_id, EventType.TASK_CREATED, {"request": "hi", "policy": None})
    await store.append(
        task_id,
        EventType.PLAN_CREATED,
        {
            "plan_id": plan_id,
            "version": 1,
            "rationale": "manual",
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "name": "step",
                        "agent_url": "http://agent",
                        "skill_id": None,
                        "deps": [],
                        "input": {"text": "hi"},
                        "requires_approval": False,
                        "policy_override": None,
                    }
                ]
            },
        },
    )
    await store.append(
        task_id,
        EventType.TASK_STATE_CHANGED,
        {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
    )
    return task_id, plan_id


async def test_append_assigns_increasing_seq_and_projects(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, plan_id = await seed_task(store)
        task = await projections.fetch_task(db, task_id)
        assert task is not None
        assert task.status is TaskStatus.RUNNING
        assert task.plan_version == 1

        plan = await projections.fetch_current_plan(db, task_id)
        assert plan is not None and plan.id == plan_id

        nodes = await projections.fetch_nodes(db, task_id)
        assert len(nodes) == 1
        assert nodes[0].id == f"{plan_id}:n1"
        assert nodes[0].status is NodeStatus.PENDING

        event = await store.append(task_id, EventType.NODE_DISPATCH_INTENT, {
            "node_id": f"{plan_id}:n1", "message_id": "m1", "attempt": 1,
        })
        assert event.seq > 0
        node = await projections.fetch_node(db, f"{plan_id}:n1")
        assert node is not None and node.attempt == 1
    finally:
        await db.close()


async def test_replay_after_seq(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, _ = await seed_task(store)
        events = await store.replay(task_id)
        assert len(events) == 3
        later = await store.replay(task_id, after_seq=events[0].seq)
        assert len(later) == 2
        assert await store.latest_seq(task_id) == events[-1].seq
    finally:
        await db.close()


async def test_rebuild_projections_is_deterministic(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, plan_id = await seed_task(store)
        node_id = f"{plan_id}:n1"
        await store.append(task_id, EventType.NODE_DISPATCH_INTENT, {
            "node_id": node_id, "message_id": "m1", "attempt": 1,
        })
        await store.append(task_id, EventType.NODE_DISPATCHED, {
            "node_id": node_id, "a2a_task_id": "r1", "a2a_context_id": task_id,
            "message_id": "m1",
        })
        await store.append(task_id, EventType.NODE_STATE_CHANGED, {
            "node_id": node_id, "from": NodeStatus.DISPATCHED.value,
            "to": NodeStatus.WORKING.value,
        })
        await store.append(task_id, EventType.NODE_STATE_CHANGED, {
            "node_id": node_id, "from": NodeStatus.WORKING.value,
            "to": NodeStatus.COMPLETED.value,
        })
        await store.append(task_id, EventType.NODE_OUTPUT, {
            "node_id": node_id, "output": {"artifacts": [{"text": "ok"}]},
        })
        await store.append(task_id, EventType.TASK_COMPLETED, {})

        before = await snapshot(db)
        await projections.rebuild(db)
        after = await snapshot(db)
        assert before == after

        task = await projections.fetch_task(db, task_id)
        assert task is not None and task.status is TaskStatus.COMPLETED
        node = await projections.fetch_node(db, node_id)
        assert node is not None and node.status is NodeStatus.COMPLETED
        assert node.output == {"artifacts": [{"text": "ok"}]}
    finally:
        await db.close()


async def snapshot(db: Database) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for table in (
        "orchestration_tasks",
        "plans",
        "nodes",
        "interventions",
        "checkpoints",
        "events",
    ):
        cursor = await db.conn.execute(f"SELECT * FROM {table} ORDER BY 1")
        result[table] = [dict(row) for row in await cursor.fetchall()]
    return result
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/unit/test_event_store.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.store.projections'`

- [ ] **Step 3: 实现 `src/agent_hub/store/projections.py`**

```python
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import aiosqlite

from agent_hub.models.domain import Node, OrchestrationTask, Plan
from agent_hub.models.enums import (
    TERMINAL_NODE_STATUSES,
    EventType,
    NodeStatus,
    TaskStatus,
)


async def apply_event(conn: aiosqlite.Connection, event: Any) -> None:
    payload = event.payload
    ts = event.created_at.isoformat()
    event_type = event.type

    if event_type is EventType.TASK_CREATED:
        await conn.execute(
            "INSERT INTO orchestration_tasks"
            " (id, status, request, policy, plan_version, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event.task_id,
                TaskStatus.PLANNING.value,
                payload["request"],
                json.dumps(payload.get("policy"), ensure_ascii=False),
                None,
                ts,
                ts,
            ),
        )
    elif event_type is EventType.PLAN_CREATED:
        await conn.execute(
            "INSERT INTO plans (id, task_id, version, dag, rationale, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                payload["plan_id"],
                event.task_id,
                payload["version"],
                json.dumps(payload["dag"], ensure_ascii=False),
                payload.get("rationale"),
                ts,
            ),
        )
        await conn.execute(
            "UPDATE orchestration_tasks SET plan_version = ?, updated_at = ? WHERE id = ?",
            (payload["version"], ts, event.task_id),
        )
        await _materialize_nodes(conn, event.task_id, payload["plan_id"], payload["dag"])
    elif event_type is EventType.TASK_STATE_CHANGED:
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, updated_at = ? WHERE id = ?",
            (payload["to"], ts, event.task_id),
        )
    elif event_type in (EventType.TASK_COMPLETED, EventType.TASK_FAILED):
        status = (
            TaskStatus.COMPLETED.value
            if event_type is EventType.TASK_COMPLETED
            else TaskStatus.FAILED.value
        )
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, updated_at = ? WHERE id = ?",
            (status, ts, event.task_id),
        )
    elif event_type is EventType.NODE_DISPATCH_INTENT:
        await conn.execute(
            "UPDATE nodes SET attempt = ?, started_at = COALESCE(started_at, ?)"
            " WHERE id = ? AND task_id = ?",
            (payload["attempt"], ts, payload["node_id"], event.task_id),
        )
    elif event_type is EventType.NODE_DISPATCHED:
        await conn.execute(
            "UPDATE nodes SET status = ?, a2a_task_id = ?, a2a_context_id = ?"
            " WHERE id = ? AND task_id = ?",
            (
                NodeStatus.DISPATCHED.value,
                payload["a2a_task_id"],
                payload.get("a2a_context_id"),
                payload["node_id"],
                event.task_id,
            ),
        )
    elif event_type is EventType.NODE_STATE_CHANGED:
        target = NodeStatus(payload["to"])
        ended_at = ts if target in TERMINAL_NODE_STATUSES else None
        await conn.execute(
            "UPDATE nodes SET status = ?, ended_at = COALESCE(?, ended_at)"
            " WHERE id = ? AND task_id = ?",
            (target.value, ended_at, payload["node_id"], event.task_id),
        )
    elif event_type is EventType.NODE_OUTPUT:
        await conn.execute(
            "UPDATE nodes SET output = ?, ended_at = COALESCE(ended_at, ?)"
            " WHERE id = ? AND task_id = ?",
            (
                json.dumps(payload["output"], ensure_ascii=False),
                ts,
                payload["node_id"],
                event.task_id,
            ),
        )
    elif event_type is EventType.ERROR and payload.get("node_id"):
        await conn.execute(
            "UPDATE nodes SET error = ? WHERE id = ? AND task_id = ?",
            (payload.get("message"), payload["node_id"], event.task_id),
        )


async def _materialize_nodes(
    conn: aiosqlite.Connection,
    task_id: str,
    plan_id: str,
    dag: dict[str, Any],
) -> None:
    for node in dag["nodes"]:
        node_id = f"{plan_id}:{node['id']}"
        deps = [f"{plan_id}:{dep}" for dep in node.get("deps", [])]
        await conn.execute(
            "INSERT INTO nodes"
            " (id, task_id, plan_id, name, agent_url, skill_id, deps, input,"
            "  status, attempt, requires_approval)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (
                node_id,
                task_id,
                plan_id,
                node["name"],
                node.get("agent_url"),
                node.get("skill_id"),
                json.dumps(deps),
                json.dumps(node.get("input"), ensure_ascii=False),
                NodeStatus.PENDING.value,
                1 if node.get("requires_approval") else 0,
            ),
        )


async def rebuild(db: Any) -> None:
    from agent_hub.store.event_store import EventStore

    store = EventStore(db)
    async with db.transaction() as conn:
        for table in ("nodes", "plans", "orchestration_tasks", "interventions", "checkpoints"):
            await conn.execute(f"DELETE FROM {table}")
    events = await store.replay_all()
    for event in events:
        async with db.transaction() as conn:
            await apply_event(conn, event)


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _row_to_task(row: aiosqlite.Row) -> OrchestrationTask:
    return OrchestrationTask(
        id=row["id"],
        status=TaskStatus(row["status"]),
        request=row["request"],
        policy=json.loads(row["policy"]) if row["policy"] else None,
        plan_version=row["plan_version"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _row_to_plan(row: aiosqlite.Row) -> Plan:
    return Plan(
        id=row["id"],
        task_id=row["task_id"],
        version=row["version"],
        dag=json.loads(row["dag"]),
        rationale=row["rationale"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_node(row: aiosqlite.Row) -> Node:
    return Node(
        id=row["id"],
        task_id=row["task_id"],
        plan_id=row["plan_id"],
        name=row["name"],
        agent_url=row["agent_url"],
        skill_id=row["skill_id"],
        deps=json.loads(row["deps"]),
        input=json.loads(row["input"]) if row["input"] else None,
        status=NodeStatus(row["status"]),
        attempt=row["attempt"],
        requires_approval=bool(row["requires_approval"]),
        a2a_task_id=row["a2a_task_id"],
        a2a_context_id=row["a2a_context_id"],
        output=json.loads(row["output"]) if row["output"] else None,
        error=row["error"],
        started_at=_parse_dt(row["started_at"]),
        ended_at=_parse_dt(row["ended_at"]),
    )


async def fetch_task(db: Any, task_id: str) -> OrchestrationTask | None:
    cursor = await db.conn.execute(
        "SELECT * FROM orchestration_tasks WHERE id = ?", (task_id,)
    )
    row = await cursor.fetchone()
    return _row_to_task(row) if row else None


async def fetch_current_plan(db: Any, task_id: str) -> Plan | None:
    cursor = await db.conn.execute(
        "SELECT p.* FROM plans p JOIN orchestration_tasks t"
        " ON p.task_id = t.id AND p.version = t.plan_version"
        " WHERE t.id = ?",
        (task_id,),
    )
    row = await cursor.fetchone()
    return _row_to_plan(row) if row else None


async def fetch_node(db: Any, node_id: str) -> Node | None:
    cursor = await db.conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
    row = await cursor.fetchone()
    return _row_to_node(row) if row else None


async def fetch_nodes(db: Any, task_id: str, plan_id: str | None = None) -> list[Node]:
    if plan_id is None:
        cursor = await db.conn.execute(
            "SELECT * FROM nodes WHERE task_id = ? ORDER BY id", (task_id,)
        )
    else:
        cursor = await db.conn.execute(
            "SELECT * FROM nodes WHERE task_id = ? AND plan_id = ? ORDER BY id",
            (task_id, plan_id),
        )
    return [_row_to_node(row) for row in await cursor.fetchall()]
```

- [ ] **Step 4: 实现 `src/agent_hub/store/event_store.py`**

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from agent_hub.models.enums import EventType
from agent_hub.store.db import Database
from agent_hub.store.projections import apply_event


class Event(BaseModel):
    seq: int = 0
    task_id: str
    type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class EventStore:
    def __init__(self, db: Database):
        self._db = db

    async def append(
        self, task_id: str, event_type: EventType, payload: dict[str, Any] | None = None
    ) -> Event:
        now = datetime.now(timezone.utc)
        data = payload or {}
        async with self._db.transaction() as conn:
            cursor = await conn.execute(
                "INSERT INTO events (task_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
                (
                    task_id,
                    event_type.value,
                    json.dumps(data, ensure_ascii=False, default=str),
                    now.isoformat(),
                ),
            )
            event = Event(
                seq=int(cursor.lastrowid),
                task_id=task_id,
                type=event_type,
                payload=data,
                created_at=now,
            )
            await apply_event(conn, event)
        return event

    async def replay(self, task_id: str, after_seq: int = 0) -> list[Event]:
        cursor = await self._db.conn.execute(
            "SELECT seq, task_id, type, payload, created_at FROM events"
            " WHERE task_id = ? AND seq > ? ORDER BY seq",
            (task_id, after_seq),
        )
        return [self._row_to_event(row) for row in await cursor.fetchall()]

    async def replay_all(self) -> list[Event]:
        cursor = await self._db.conn.execute(
            "SELECT seq, task_id, type, payload, created_at FROM events ORDER BY seq"
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
            type=EventType(row["type"]),
            payload=json.loads(row["payload"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
```

- [ ] **Step 5: 运行确认通过**

Run: `uv run pytest tests/unit/test_event_store.py -v`
Expected: `3 passed`

- [ ] **Step 6: Commit**

```bash
git add src/agent_hub/store/projections.py src/agent_hub/store/event_store.py tests/unit/test_event_store.py
git commit -m "feat: 事件存储、投影与重建"
```

---

### Task 6: 假 A2A agent 测试设施

**Files:**
- Create: `tests/fake_agents/echo_agent.py`
- Test: `tests/integration/test_fake_agent.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_fake_agent.py`**

```python
import httpx
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.helpers import get_artifact_text, new_text_message
from a2a.types import Role, SendMessageRequest, TaskState

from tests.fake_agents.echo_agent import start_fake_agent


async def test_fake_agent_echoes_with_official_client():
    agent = await start_fake_agent("echo")
    try:
        async with httpx.AsyncClient() as http:
            resolver = A2ACardResolver(httpx_client=http, base_url=agent.url)
            card = await resolver.get_agent_card()
            assert card.name == "fake-echo"

        client = await create_client(
            agent=agent.card, client_config=ClientConfig(streaming=True)
        )
        request = SendMessageRequest(message=new_text_message("hi", role=Role.ROLE_USER))
        task_id = None
        states = []
        artifact_texts = []
        async for chunk in client.send_message(request):
            if chunk.HasField("task"):
                task_id = chunk.task.id
            elif chunk.HasField("status_update"):
                states.append(chunk.status_update.status.state)
            elif chunk.HasField("artifact_update"):
                artifact_texts.append(get_artifact_text(chunk.artifact_update.artifact))
        assert task_id
        assert TaskState.TASK_STATE_COMPLETED in states
        assert "echo:hi" in artifact_texts
        await client.close()
    finally:
        await agent.stop()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_fake_agent.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'tests.fake_agents.echo_agent'`

- [ ] **Step 3: 实现 `tests/fake_agents/echo_agent.py`**

```python
from __future__ import annotations

import asyncio
import contextlib
import socket
from dataclasses import dataclass

import uvicorn
from a2a.helpers import get_message_text, new_task_from_user_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Part,
)
from starlette.applications import Starlette


class ScriptedExecutor(AgentExecutor):
    """可控行为的假 agent：

    - echo: 添加 artifact "echo:{text}" 后完成
    - ask:  先进入 input-required，收到后续消息后完成
    - fail: 直接失败
    - slow: 等待 5 秒后完成（用于超时测试）
    """

    def __init__(self, behavior: str = "echo"):
        self._behavior = behavior

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        text = get_message_text(context.message) if context.message else ""
        if context.current_task is None:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.start_work()
            if self._behavior == "ask":
                await updater.requires_input(
                    updater.new_agent_message(parts=[Part(text="who are you?")])
                )
                return
            if self._behavior == "fail":
                await updater.failed(
                    updater.new_agent_message(parts=[Part(text="boom")])
                )
                return
            if self._behavior == "slow":
                await asyncio.sleep(5)
            await updater.add_artifact(
                parts=[Part(text=f"echo:{text}")], name="response", last_chunk=True
            )
            await updater.complete()
        else:
            task = context.current_task
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.add_artifact(
                parts=[Part(text=f"answered:{text}")],
                name="response",
                last_chunk=True,
            )
            await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = TaskUpdater(
            event_queue, context.task_id or "", context.context_id or ""
        )
        await updater.cancel()


@dataclass
class FakeAgent:
    url: str
    card: AgentCard
    server: uvicorn.Server
    task: asyncio.Task
    handler: DefaultRequestHandler

    async def stop(self) -> None:
        await self.handler.aclose()
        self.server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.task, timeout=5)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _make_card(behavior: str, url: str) -> AgentCard:
    return AgentCard(
        name=f"fake-{behavior}",
        description=f"scripted test agent ({behavior})",
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="echo",
                name="echo",
                description="echoes input",
                tags=["test"],
            )
        ],
        supported_interfaces=[
            AgentInterface(protocol_binding="JSONRPC", url=url, protocol_version="1.0")
        ],
    )


async def start_fake_agent(behavior: str = "echo") -> FakeAgent:
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    card = _make_card(behavior, url)
    handler = DefaultRequestHandler(
        agent_executor=ScriptedExecutor(behavior),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    routes = create_agent_card_routes(agent_card=card) + create_jsonrpc_routes(
        request_handler=handler, rpc_url="/"
    )
    app = Starlette(routes=routes)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    return FakeAgent(url=url, card=card, server=server, task=task, handler=handler)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_fake_agent.py -v`
Expected: `1 passed`

- [ ] **Step 5: 在 `tests/conftest.py` 提供公共 fixture**

```python
import pytest

from tests.fake_agents.echo_agent import FakeAgent, start_fake_agent


@pytest.fixture
async def echo_agent() -> FakeAgent:
    agent = await start_fake_agent("echo")
    yield agent
    await agent.stop()
```

- [ ] **Step 6: Commit**

```bash
git add tests/fake_agents/echo_agent.py tests/integration/test_fake_agent.py tests/conftest.py
git commit -m "test: 基于 a2a-sdk 的可控假 agent 测试设施"
```

---

### Task 7: A2A 客户端封装

**Files:**
- Create: `src/agent_hub/a2a/__init__.py`（空）
- Create: `src/agent_hub/a2a/client.py`
- Test: `tests/integration/test_a2a_client.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_a2a_client.py`**

```python
from a2a.types import TaskState

from agent_hub.a2a.client import RemoteAgentClient


async def test_resolve_card_and_send_text(echo_agent):
    client = RemoteAgentClient()
    try:
        card = await client.resolve_card(echo_agent.url)
        assert card.name == "fake-echo"

        chunks = [chunk async for chunk in client.send_text(echo_agent.url, "hello")]
        assert chunks[0].HasField("task")

        states = [
            chunk.status_update.status.state
            for chunk in chunks
            if chunk.HasField("status_update")
        ]
        assert TaskState.TASK_STATE_COMPLETED in states

        task = chunks[0].task
        assert task.artifacts or any(c.HasField("artifact_update") for c in chunks)
    finally:
        await client.close()


async def test_send_text_sets_deterministic_message_id(echo_agent):
    client = RemoteAgentClient()
    try:
        chunks = [
            chunk
            async for chunk in client.send_text(
                echo_agent.url,
                "hi",
                context_id="ctx1",
                message_id="t1:n1:1",
            )
        ]
        assert chunks[0].task.context_id == "ctx1"
        assert chunks[0].task.id
    finally:
        await client.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_a2a_client.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.a2a'`

- [ ] **Step 3: 实现 `src/agent_hub/a2a/client.py`**

```python
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
from a2a.client import A2ACardResolver, Client, ClientConfig, create_client
from a2a.helpers import new_text_message
from a2a.types import AgentCard, Message, Role, SendMessageRequest, StreamResponse
from google.protobuf.json_format import MessageToDict, ParseDict


class RemoteAgentClient:
    """A2A 协议访问的唯一入口，便于 SDK 升级时集中修改。"""

    def __init__(self, httpx_client: httpx.AsyncClient | None = None):
        self._owns_http = httpx_client is None
        self._http = httpx_client or httpx.AsyncClient()
        self._clients: dict[str, Client] = {}
        self._cards: dict[str, AgentCard] = {}

    async def resolve_card(self, base_url: str) -> AgentCard:
        resolver = A2ACardResolver(httpx_client=self._http, base_url=base_url)
        return await resolver.get_agent_card()

    @staticmethod
    def card_to_dict(card: AgentCard) -> dict[str, Any]:
        return MessageToDict(card)

    @staticmethod
    def card_from_dict(data: dict[str, Any]) -> AgentCard:
        return ParseDict(data, AgentCard())

    async def _client_for(self, agent_url: str) -> Client:
        if agent_url not in self._clients:
            card = await self.resolve_card(agent_url)
            self._cards[agent_url] = card
            self._clients[agent_url] = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=True, httpx_client=self._http),
            )
        return self._clients[agent_url]

    async def send_text(
        self,
        agent_url: str,
        text: str,
        *,
        task_id: str | None = None,
        context_id: str | None = None,
        message_id: str | None = None,
    ) -> AsyncIterator[StreamResponse]:
        client = await self._client_for(agent_url)
        message: Message = new_text_message(
            text, role=Role.ROLE_USER, task_id=task_id, context_id=context_id
        )
        if message_id is not None:
            message.message_id = message_id
        request = SendMessageRequest(message=message)
        async for chunk in client.send_message(request):
            yield chunk

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()
        self._clients.clear()
        if self._owns_http:
            await self._http.aclose()
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_a2a_client.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/a2a/__init__.py src/agent_hub/a2a/client.py tests/integration/test_a2a_client.py
git commit -m "feat: A2A 客户端封装（card 解析、客户端缓存、流式发送）"
```

---

### Task 8: Agent 注册表

**Files:**
- Create: `src/agent_hub/a2a/registry.py`
- Test: `tests/integration/test_registry.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_registry.py`**

```python
import pytest

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry, DuplicateAgentName
from agent_hub.store.db import Database


async def test_register_list_refresh_delete(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        record = await registry.register("echo", echo_agent.url)
        assert record.name == "echo"
        assert registry.agent_url(record) == echo_agent.url

        listed = await registry.list()
        assert [r.name for r in listed] == ["echo"]

        by_name = await registry.get_by_name("echo")
        assert by_name is not None and by_name.id == record.id

        refreshed = await registry.refresh(record.id)
        assert refreshed.card["name"] == "fake-echo"

        assert await registry.delete(record.id) is True
        assert await registry.list() == []
    finally:
        await remote.close()
        await db.close()


async def test_duplicate_name_rejected(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        await registry.register("echo", echo_agent.url)
        with pytest.raises(DuplicateAgentName):
            await registry.register("echo", echo_agent.url)
    finally:
        await remote.close()
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_registry.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.a2a.registry'`

- [ ] **Step 3: 实现 `src/agent_hub/a2a/registry.py`**

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.models.domain import AgentRecord
from agent_hub.store.db import Database

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
        now = datetime.now(timezone.utc)
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
        cursor = await self._db.conn.execute(
            "SELECT * FROM agent_registry ORDER BY name"
        )
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
        now = datetime.now(timezone.utc)
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
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_registry.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/a2a/registry.py tests/integration/test_registry.py
git commit -m "feat: Agent 注册表（注册/查询/刷新/删除）"
```

---

### Task 9: 任务服务（单节点计划）

**Files:**
- Create: `src/agent_hub/core/tasks.py`
- Test: `tests/integration/test_task_service.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_task_service.py`**

```python
import pytest
from pydantic import BaseModel

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.tasks import (
    CreatedTask,
    TargetSpec,
    TaskNotFound,
    TaskService,
    UnknownAgent,
)
from agent_hub.models.enums import NodeStatus, TaskStatus
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


async def make_service(tmp_path, echo_agent) -> tuple[Database, TaskService]:
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("echo", echo_agent.url)
    return db, TaskService(db, EventStore(db), registry)


async def test_create_task_materializes_single_node(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        created = await service.create_task("做一件事", TargetSpec(agent_name="echo"))
        assert isinstance(created, CreatedTask)
        assert created.node_ids == [f"{created.plan_id}:n1"]

        snapshot = await service.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.RUNNING
        assert snapshot.plan is not None and snapshot.plan.version == 1
        assert len(snapshot.nodes) == 1
        node = snapshot.nodes[0]
        assert node.status is NodeStatus.PENDING
        assert node.input == {"text": "做一件事"}
    finally:
        await db.close()


async def test_unknown_agent_rejected(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        with pytest.raises(UnknownAgent):
            await service.create_task("x", TargetSpec(agent_name="missing"))
        with pytest.raises(TaskNotFound):
            await service.get_snapshot("nope")
    finally:
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_task_service.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.core.tasks'`

- [ ] **Step 3: 实现 `src/agent_hub/core/tasks.py`**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from agent_hub.a2a.registry import AgentRegistry
from agent_hub.models.domain import Node, OrchestrationTask, Plan
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


class TaskNotFound(KeyError):
    pass


class UnknownAgent(ValueError):
    pass


@dataclass
class TargetSpec:
    agent_name: str
    skill_id: str | None = None
    name: str = "single"
    input: dict[str, Any] | None = None


class CreatedTask(BaseModel):
    task_id: str
    plan_id: str
    node_ids: list[str]


class TaskSnapshot(BaseModel):
    task: OrchestrationTask
    plan: Plan | None
    nodes: list[Node]


class TaskService:
    def __init__(self, db: Database, event_store: EventStore, registry: AgentRegistry):
        self._db = db
        self._events = event_store
        self._registry = registry

    async def create_task(self, request: str, target: TargetSpec) -> CreatedTask:
        record = await self._registry.get_by_name(target.agent_name)
        if record is None:
            raise UnknownAgent(f"agent not registered: {target.agent_name}")
        agent_url = self._registry.agent_url(record)

        task_id = uuid4().hex
        plan_id = uuid4().hex
        dag_node_id = "n1"
        node_id = f"{plan_id}:{dag_node_id}"
        node_input = target.input or {"text": request}
        dag = {
            "nodes": [
                {
                    "id": dag_node_id,
                    "name": target.name,
                    "agent_url": agent_url,
                    "skill_id": target.skill_id,
                    "deps": [],
                    "input": node_input,
                    "requires_approval": False,
                    "policy_override": None,
                }
            ]
        }

        await self._events.append(
            task_id, EventType.TASK_CREATED, {"request": request, "policy": None}
        )
        await self._events.append(
            task_id,
            EventType.PLAN_CREATED,
            {
                "plan_id": plan_id,
                "version": 1,
                "rationale": "manual single-node target",
                "dag": dag,
            },
        )
        await self._events.append(
            task_id,
            EventType.TASK_STATE_CHANGED,
            {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
        )
        return CreatedTask(task_id=task_id, plan_id=plan_id, node_ids=[node_id])

    async def get_snapshot(self, task_id: str) -> TaskSnapshot:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        nodes = await projections.fetch_nodes(
            self._db, task_id, plan.id if plan else None
        )
        return TaskSnapshot(task=task, plan=plan, nodes=nodes)

    async def finalize_if_complete(self, task_id: str) -> OrchestrationTask:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if task.status is not TaskStatus.RUNNING or plan is None:
            return task
        nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
        if not nodes:
            return task
        statuses = {node.status for node in nodes}
        active = {
            NodeStatus.PENDING,
            NodeStatus.READY,
            NodeStatus.DISPATCHED,
            NodeStatus.WORKING,
            NodeStatus.INPUT_REQUIRED,
        }
        if all(status is NodeStatus.COMPLETED for status in statuses):
            await self._events.append(task_id, EventType.TASK_COMPLETED, {})
        elif not statuses & active:
            await self._events.append(task_id, EventType.TASK_FAILED, {})
        refreshed = await projections.fetch_task(self._db, task_id)
        assert refreshed is not None
        return refreshed
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_task_service.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/core/tasks.py tests/integration/test_task_service.py
git commit -m "feat: TaskService（单节点计划、快照、终态判定）"
```

---

### Task 10: 节点派发器

**Files:**
- Create: `src/agent_hub/core/dispatcher.py`
- Test: `tests/integration/test_dispatcher.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_dispatcher.py`**

```python
import pytest

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import InvalidNodeState, NodeDispatcher
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent


async def setup(tmp_path, agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    return db, remote, events, tasks, dispatcher


async def test_dispatch_echo_completes_task(tmp_path, echo_agent):
    db, remote, events, tasks, dispatcher = await setup(tmp_path, echo_agent)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.COMPLETED
        assert node.output is not None
        assert node.output["artifacts"][0]["text"] == "echo:hi"
        assert node.a2a_task_id

        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.COMPLETED

        types = [e.type for e in await events.replay(created.task_id)]
        assert types[0] is EventType.TASK_CREATED
        assert EventType.NODE_DISPATCHED in types
        assert EventType.NODE_OUTPUT in types
        assert types[-1] is EventType.TASK_COMPLETED
    finally:
        await remote.close()
        await db.close()


async def test_dispatch_input_required_stops_at_input_required(tmp_path):
    agent = await start_fake_agent("ask")
    db, remote, events, tasks, dispatcher = await setup(tmp_path, agent)
    try:
        created = await tasks.create_task("ask", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.INPUT_REQUIRED
        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.RUNNING
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_failure_marks_node_and_task_failed(tmp_path):
    agent = await start_fake_agent("fail")
    db, remote, events, tasks, dispatcher = await setup(tmp_path, agent)
    try:
        created = await tasks.create_task("fail", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.FAILED
        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.FAILED
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_timeout(tmp_path):
    agent = await start_fake_agent("slow")
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=0.5)
    try:
        created = await tasks.create_task("slow", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.FAILED
        assert node.error and "timed out" in node.error
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_twice_rejected(tmp_path, echo_agent):
    db, remote, events, tasks, dispatcher = await setup(tmp_path, echo_agent)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        with pytest.raises(InvalidNodeState):
            await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
    finally:
        await remote.close()
        await db.close()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_dispatcher.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.core.dispatcher'`

- [ ] **Step 3: 实现 `src/agent_hub/core/dispatcher.py`**

```python
from __future__ import annotations

import asyncio
from typing import Any

from a2a.types import TaskState

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.core.state import assert_node_transition
from agent_hub.models.domain import Node
from agent_hub.models.enums import EventType, NodeStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore

_REMOTE_STATE_MAP: dict[int, NodeStatus] = {
    TaskState.TASK_STATE_SUBMITTED: NodeStatus.DISPATCHED,
    TaskState.TASK_STATE_WORKING: NodeStatus.WORKING,
    TaskState.TASK_STATE_INPUT_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_COMPLETED: NodeStatus.COMPLETED,
    TaskState.TASK_STATE_FAILED: NodeStatus.FAILED,
    TaskState.TASK_STATE_CANCELED: NodeStatus.CANCELED,
    TaskState.TASK_STATE_REJECTED: NodeStatus.FAILED,
    TaskState.TASK_STATE_UNSPECIFIED: NodeStatus.WORKING,
}


class InvalidNodeState(RuntimeError):
    pass


def _join_text(parts: Any) -> str:
    return "\n".join(part.text for part in parts if part.HasField("text"))


class NodeDispatcher:
    def __init__(
        self,
        db: Database,
        event_store: EventStore,
        remote: RemoteAgentClient,
        timeout_seconds: float = 600.0,
    ):
        self._db = db
        self._events = event_store
        self._remote = remote
        self._timeout = timeout_seconds

    async def dispatch_node(self, task_id: str, node_id: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status is NodeStatus.PENDING:
            await self._transition(node, NodeStatus.READY)
        if node.status is not NodeStatus.READY:
            raise InvalidNodeState(f"node {node_id} is {node.status.value}, cannot dispatch")

        attempt = node.attempt + 1
        message_id = f"{task_id}:{node_id}:{attempt}"
        await self._events.append(
            task_id,
            EventType.NODE_DISPATCH_INTENT,
            {"node_id": node_id, "message_id": message_id, "attempt": attempt},
        )

        artifacts: list[dict[str, Any]] = []
        current = NodeStatus.READY
        try:
            async with asyncio.timeout(self._timeout):
                text = str((node.input or {}).get("text", ""))
                async for chunk in self._remote.send_text(
                    node.agent_url or "",
                    text,
                    context_id=task_id,
                    message_id=message_id,
                ):
                    if chunk.HasField("task"):
                        await self._events.append(
                            task_id,
                            EventType.NODE_DISPATCHED,
                            {
                                "node_id": node_id,
                                "a2a_task_id": chunk.task.id,
                                "a2a_context_id": chunk.task.context_id,
                                "message_id": message_id,
                            },
                        )
                        current = NodeStatus.DISPATCHED
                        node.status = NodeStatus.DISPATCHED
                    elif chunk.HasField("status_update"):
                        mapped = _REMOTE_STATE_MAP.get(chunk.status_update.status.state)
                        if mapped is not None and mapped is not current:
                            await self._transition(node, mapped)
                            current = mapped
                    elif chunk.HasField("artifact_update"):
                        artifact = chunk.artifact_update.artifact
                        artifact_text = _join_text(artifact.parts)
                        artifacts.append(
                            {
                                "id": artifact.artifact_id,
                                "name": artifact.name,
                                "text": artifact_text,
                            }
                        )
                        await self._events.append(
                            task_id,
                            EventType.NODE_ARTIFACT,
                            {
                                "node_id": node_id,
                                "artifact_id": artifact.artifact_id,
                                "name": artifact.name,
                                "text": artifact_text,
                            },
                        )
                    elif chunk.HasField("message"):
                        message_text = _join_text(chunk.message.parts)
                        artifacts.append(
                            {"id": "message", "name": "message", "text": message_text}
                        )

            if current is NodeStatus.COMPLETED:
                await self._events.append(
                    task_id,
                    EventType.NODE_OUTPUT,
                    {"node_id": node_id, "output": {"artifacts": artifacts}},
                )
            elif current is NodeStatus.INPUT_REQUIRED:
                pass
            elif current not in (
                NodeStatus.FAILED,
                NodeStatus.CANCELED,
            ):
                await self._fail(
                    node,
                    "remote stream ended without terminal state",
                )
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001 - 记录任意远端异常并标记节点失败
            await self._fail(node, str(exc))

        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def _transition(self, node: Node, target: NodeStatus) -> None:
        assert_node_transition(node.status, target)
        await self._events.append(
            node.task_id,
            EventType.NODE_STATE_CHANGED,
            {"node_id": node.id, "from": node.status.value, "to": target.value},
        )
        node.status = target

    async def _fail(self, node: Node, message: str) -> None:
        if node.status is not NodeStatus.FAILED:
            await self._transition(node, NodeStatus.FAILED)
        await self._events.append(
            node.task_id,
            EventType.ERROR,
            {"node_id": node.id, "message": message},
        )
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run pytest tests/integration/test_dispatcher.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/agent_hub/core/dispatcher.py tests/integration/test_dispatcher.py
git commit -m "feat: 节点派发器（A2A 事件映射、产物收集、超时）"
```

---

### Task 11: API 层与主程序

**Files:**
- Create: `src/agent_hub/api/__init__.py`（空）
- Create: `src/agent_hub/api/schemas.py`
- Create: `src/agent_hub/api/app.py`
- Create: `src/agent_hub/api/tasks.py`
- Create: `src/agent_hub/api/agents.py`
- Create: `src/agent_hub/main.py`
- Test: `tests/integration/test_api.py`

- [ ] **Step 1: 写失败测试 `tests/integration/test_api.py`**

```python
import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(store={"db_path": tmp_path / "api.db"})
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, app, echo_agent.url


async def test_end_to_end_single_node(api):
    client, app, agent_url = api

    resp = await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    assert resp.status_code == 201
    assert resp.json()["name"] == "echo"

    resp = await client.post(
        "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
    )
    assert resp.status_code == 201
    created = resp.json()

    resp = await client.post(
        f"/v1/tasks/{created['task_id']}/nodes/{created['node_ids'][0]}/dispatch"
    )
    assert resp.status_code == 200
    snapshot = resp.json()
    assert snapshot["task"]["status"] == "completed"
    assert snapshot["nodes"][0]["status"] == "completed"
    assert snapshot["nodes"][0]["output"]["artifacts"][0]["text"] == "echo:hi"

    cursor = await app.state.db.conn.execute(
        "SELECT type FROM events WHERE task_id = ? ORDER BY seq", (created["task_id"],)
    )
    types = [row["type"] for row in await cursor.fetchall()]
    assert types[0] == "task.created"
    assert "node.dispatched" in types
    assert types[-1] == "task.completed"


async def test_unknown_agent_returns_400(api):
    client, _, _ = api
    resp = await client.post(
        "/v1/tasks", json={"request": "x", "target": {"agent_name": "ghost"}}
    )
    assert resp.status_code == 400


async def test_duplicate_agent_returns_409(api):
    client, _, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    resp = await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    assert resp.status_code == 409


async def test_missing_task_returns_404(api):
    client, _, _ = api
    resp = await client.get("/v1/tasks/nope")
    assert resp.status_code == 404
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: FAIL，`ModuleNotFoundError: No module named 'agent_hub.api'`

- [ ] **Step 3: 实现 `src/agent_hub/api/schemas.py`**

```python
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TargetIn(BaseModel):
    agent_name: str
    skill_id: str | None = None
    name: str = "single"
    input: dict[str, Any] | None = None


class CreateTaskIn(BaseModel):
    request: str
    target: TargetIn


class RegisterAgentIn(BaseModel):
    name: str
    card_url: str = Field(..., description="A2A agent base URL")
```

- [ ] **Step 4: 实现 `src/agent_hub/api/app.py`**

```python
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.api import agents as agents_routes
from agent_hub.api import tasks as tasks_routes
from agent_hub.config import Settings
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.tasks import TaskService
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(resolved.store.db_path)
        await db.initialize()
        remote = RemoteAgentClient()
        event_store = EventStore(db)
        registry = AgentRegistry(db, remote)
        task_service = TaskService(db, event_store, registry)
        dispatcher = NodeDispatcher(
            db,
            event_store,
            remote,
            timeout_seconds=resolved.scheduler.node_timeout_seconds,
        )

        app.state.settings = resolved
        app.state.db = db
        app.state.remote = remote
        app.state.event_store = event_store
        app.state.registry = registry
        app.state.task_service = task_service
        app.state.dispatcher = dispatcher
        try:
            yield
        finally:
            await remote.close()
            await db.close()

    app = FastAPI(title="Agent Hub", version="0.1.0", lifespan=lifespan)
    app.include_router(tasks_routes.router, prefix="/v1")
    app.include_router(agents_routes.router, prefix="/v1")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
```

- [ ] **Step 5: 实现 `src/agent_hub/api/tasks.py`**

```python
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import CreateTaskIn
from agent_hub.core.dispatcher import InvalidNodeState
from agent_hub.core.tasks import CreatedTask, TargetSpec, TaskNotFound, TaskSnapshot, UnknownAgent

router = APIRouter(tags=["tasks"])


@router.post("/tasks", status_code=201, response_model=CreatedTask)
async def create_task(body: CreateTaskIn, request: Request) -> CreatedTask:
    service = request.app.state.task_service
    target = TargetSpec(
        agent_name=body.target.agent_name,
        skill_id=body.target.skill_id,
        name=body.target.name,
        input=body.target.input,
    )
    try:
        return await service.create_task(body.request, target)
    except UnknownAgent as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks/{task_id}", response_model=TaskSnapshot)
async def get_task(task_id: str, request: Request) -> TaskSnapshot:
    try:
        return await request.app.state.task_service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc


@router.post("/tasks/{task_id}/nodes/{node_id}/dispatch", response_model=TaskSnapshot)
async def dispatch_node(task_id: str, node_id: str, request: Request) -> TaskSnapshot:
    dispatcher = request.app.state.dispatcher
    service = request.app.state.task_service
    try:
        await dispatcher.dispatch_node(task_id, node_id)
    except InvalidNodeState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await service.finalize_if_complete(task_id)
    return await service.get_snapshot(task_id)
```

- [ ] **Step 6: 实现 `src/agent_hub/api/agents.py`**

```python
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.a2a.registry import DuplicateAgentName
from agent_hub.api.schemas import RegisterAgentIn
from agent_hub.models.domain import AgentRecord

router = APIRouter(tags=["agents"])


@router.post("/agents", status_code=201, response_model=AgentRecord)
async def register_agent(body: RegisterAgentIn, request: Request) -> AgentRecord:
    try:
        return await request.app.state.registry.register(body.name, body.card_url)
    except DuplicateAgentName as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - card 拉取失败统一返回 400
        raise HTTPException(
            status_code=400, detail=f"failed to resolve agent card: {exc}"
        ) from exc


@router.get("/agents", response_model=list[AgentRecord])
async def list_agents(request: Request) -> list[AgentRecord]:
    return await request.app.state.registry.list()


@router.delete("/agents/{agent_id}", status_code=204)
async def delete_agent(agent_id: str, request: Request) -> None:
    if not await request.app.state.registry.delete(agent_id):
        raise HTTPException(status_code=404, detail=f"agent not found: {agent_id}")


@router.post("/agents/{agent_id}/refresh", response_model=AgentRecord)
async def refresh_agent(agent_id: str, request: Request) -> AgentRecord:
    try:
        return await request.app.state.registry.refresh(agent_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
```

- [ ] **Step 7: 实现 `src/agent_hub/main.py`**

```python
from __future__ import annotations

import os

import uvicorn

from agent_hub.api.app import create_app
from agent_hub.config import load_settings


def main() -> None:
    settings = load_settings(os.environ.get("AGENT_HUB_CONFIG"))
    uvicorn.run(
        create_app(settings),
        host=settings.server.host,
        port=settings.server.port,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: 运行确认通过**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: `4 passed`

- [ ] **Step 9: 提交**

```bash
git add src/agent_hub/api/ src/agent_hub/main.py tests/integration/test_api.py
git commit -m "feat: FastAPI 入口（任务/派发/agent 注册）"
```

---

### Task 12: README、全量验证与收尾

**Files:**
- Create: `README.md`

- [ ] **Step 1: 写 `README.md`**

````markdown
# Agent Hub

A2A 多 Agent 编排平台（MVP）。平台不执行业务动作，负责理解需求、拆分任务 DAG、
调度 A2A subagent，并支持暂停协助、断点恢复与平台侧回退。

当前进度：**M1 骨架与单节点派发**（手动指定 agent 的单节点计划；LLM 规划、SSE、
HITL、恢复与回退见后续计划）。

## 开发环境

- Python 3.12（由 uv 管理）
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
uv run pytest
```

## 运行

```bash
cp config.example.yaml config.yaml   # 可选
uv run agent-hub                     # 默认 http://127.0.0.1:8080
```

## M1 接口速览

```bash
# 注册一个 A2A agent（Agent Card 会被拉取并缓存）
curl -X POST localhost:8080/v1/agents \
  -H 'content-type: application/json' \
  -d '{"name":"demo","card_url":"http://127.0.0.1:9001"}'

# 提交任务（M1 直接用 target 指定 agent，单节点计划）
curl -X POST localhost:8080/v1/tasks \
  -H 'content-type: application/json' \
  -d '{"request":"hello","target":{"agent_name":"demo"}}'

# 手动派发节点（M1 暂用手动派发，Plan 2 起由调度器自动派发）
curl -X POST localhost:8080/v1/tasks/<task_id>/nodes/<node_id>/dispatch

# 查询快照
curl localhost:8080/v1/tasks/<task_id>
```

## 设计文档与计划

- 设计：`docs/superpowers/specs/2026-09-12-a2a-orchestration-platform-design.md`
- Plan 1：`docs/superpowers/plans/2026-09-12-a2a-platform-m1-skeleton.md`
````

- [ ] **Step 2: 跑全量测试与 lint**

Run: `uv run pytest -v`
Expected: 全部通过（约 20 个用例）。

Run: `uv run ruff check .`
Expected: `All checks passed!`（如有告警，修复后重跑）

- [ ] **Step 3: 手动冒烟（可选但推荐）**

在一个终端启动假 agent：

```bash
uv run python -c "
import asyncio
from tests.fake_agents.echo_agent import start_fake_agent

async def main():
    agent = await start_fake_agent('echo')
    print('fake agent at', agent.url)
    await asyncio.Event().wait()

asyncio.run(main())
"
```

另开终端启动服务并用 README 的 curl 流程验证返回 `completed`。

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: M1 README 与使用说明"
```

---

## Plan 1 完成标准（对照 spec M1）

- [ ] `POST /v1/tasks` + 手动派发可让单个 A2A 远程任务完成，`GET /v1/tasks/{id}` 显示 `completed` 与产物。
- [ ] 全部状态变化以事件追加到 `events` 表，同一事务内更新投影；`rebuild` 重放结果与现有投影一致（测试覆盖）。
- [ ] `input-required` 被正确记录为节点状态（HITL 响应留待 Plan 3）。
- [ ] 超时与远程失败被记录为 `node error` + `node failed` + 任务 `failed`。
- [ ] `uv run pytest` 与 `uv run ruff check .` 全绿。
