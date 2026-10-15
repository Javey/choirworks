# workflow 图引擎实现计划

> 状态：**实施中**。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）。仅借鉴设计，不复制代码；实现时借鉴点须在文件头
> 标注来源与许可证。

## 一、项目背景

在 `new_choirworks` 项目的 `core/workflow/` 目录构建通用图引擎，为编排 agent
（OrchestratorAgent）提供 DAG 计划调度能力。图引擎借鉴 ADK 的 workflow 机制，
但简化了 replay/resume/HITL/分支隔离等特性。

### ADK 参考路径

| ADK 文件 | 对应我们的文件 | 借鉴点 |
|----------|---------------|--------|
| `workflow/_base_node.py` | `base_node.py` | BaseNode + START + run/_run_impl 分层 |
| `workflow/_graph.py` | `graph.py` | Edge + Graph + get_next_pending_nodes |
| `workflow/utils/_graph_validation.py` | `utils/graph_validation.py` | 7 条校验 |
| `workflow/_join_node.py` | `join_node.py` | JoinNode + _requires_all_predecessors |
| `workflow/_trigger.py` | `trigger.py` | Trigger（简化，去掉 branch） |
| `workflow/_workflow.py` | `workflow.py` | Workflow + 调度循环 |
| `workflow/_errors.py` | `errors.py` | GraphValidationError + WorkflowError |
| `workflow/_node_runner.py` | （不搬） | 我们用 _run_node 替代 |
| `workflow/_node_state.py` | （不搬） | 我们用 set/dict 跟踪状态 |
| `workflow/_node_status.py` | （不搬） | 我们用 set/dict 跟踪状态 |

### 与 ADK 的关键差异

1. **无 per-node Context** — ADK 的 NodeRunner 创建 child Context 存 output/route/error；
   我们没有 per-node Context，`output` 和 `route` 放 `BaseNode` 字段上。
2. **无 NodeRunner** — ADK 用 NodeRunner 驱动 node.run()；我们用 `_run_node` wrapper。
3. **无独立 EventQueue** — ADK 有 `ic._enqueue_event`（A2A SDK EventQueue）传事件 +
   `task.result()` 返回 Context 传完成状态；我们的 `_run_impl` 是 async generator，
   事件和完成信号必须走同一通道，因此引入 `_NodeDone` 信号。
4. **无 trigger buffer replay** — ADK 有 replay manager 从 session 事件恢复；
   我们不做 replay。
5. **无 retry** — 暂不实现，等需要时在 `_run_node` 内部加（和 ADK NodeRunner 一致）。
6. **无 abort/interrupt/resume** — 暂不实现。

## 二、文件布局与当前状态

```
src/new_choirworks/core/workflow/
  __init__.py                  # 空
  base_node.py                 # ✅ BaseNode + START + RouteValue
  errors.py                    # ✅ GraphValidationError, WorkflowError
  graph.py                     # ✅ Edge, Graph, DEFAULT_ROUTE, NodeLike
  join_node.py                 # ✅ JoinNode
  trigger.py                   # ✅ Trigger
  workflow.py                  # 🔧 Workflow（骨架 + _seed_start_triggers）
  utils/
    __init__.py                # 空
    graph_validation.py        # ✅ validate_graph + 7 条校验

tests/next/workflow/
  __init__.py                  # 空
  test_graph.py                # ✅ 25 tests
  test_workflow.py             # 🔧 2 tests（_seed_start_triggers）
```

## 三、已完成的代码（精确状态）

### `base_node.py`（67 行）

```python
type RouteValue = bool | int | str

class BaseNode(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    name: str
    description: str = ""
    output: object | None = None       # 节点 _run_impl 里设，Workflow 读
    route: RouteValue | None = None    # 节点 _run_impl 里设，用于条件路由

    async def run(self, ctx, node_input=None) -> AsyncGenerator[A2AEvent, None]:
        # Aclosing 包装 _run_impl，确保取消时 finally 执行
        async with aclosing(self._run_impl(ctx, node_input)) as agen:
            async for event in agen:
                yield event

    async def _run_impl(self, _ctx, _node_input=None) -> AsyncGenerator[A2AEvent, None]:
        # 默认 raise NotImplementedError；BaseNode 必须可实例化（START 依赖）
        raise NotImplementedError(...)
        yield  # pyright: ignore[reportUnreachable]  ← 让 Python 识别为 async generator

    @property
    def _requires_all_predecessors(self) -> bool:
        return False  # JoinNode 覆写为 True

START = BaseNode(name="__START__")  # 哨兵节点，不执行
```

**设计决策**：
- `RouteValue` 定义在此文件（不放 `graph.py`），消除 `base_node.py` ↔ `graph.py`
  运行时循环 import。`graph.py` 从 `base_node.py` import `RouteValue`。
- `output` / `route` 放 `BaseNode` 字段上（ADK 放 per-node Context 上；我们无
  per-node Context）。节点在 `_run_impl` 里设 `self.output` / `self.route`，
  Workflow 在 task 完成后读取。
- `node_input` 参数加到 `run` / `_run_impl`（和 ADK 一致），前驱 output 通过
  `Trigger.input` → `node.run(ctx, node_input=trigger.input)` 传入。
- `_requires_all_predecessors` 是 `@property`（非字段），类级行为，只能子类覆写。
  不能从外部 `node._requires_all_predecessors = True` 赋值（和 JS getter 一样）。
- `raise` + `yield` 模式：`raise` 先抛异常，`yield` 不可达，但让 Python 识别
  为 async generator。运行时 `return` + `yield` 同理（JoinNode 用这个模式）。

### `errors.py`（6 行）

```python
class GraphValidationError(ValueError):
    """A malformed graph definition."""

class WorkflowError(RuntimeError):
    """Raised when a workflow execution fails."""
```

**设计决策**：所有 error 放一个文件（和 ADK `_errors.py` 一致）。后续按需拆分
（如 `NodeTimeoutError`、`WorkflowConfigurationError` 等）。

### `graph.py`（129 行）

```python
DEFAULT_ROUTE = "__DEFAULT__"
type NodeLike = BaseNode | Literal["START"]

@dataclass(frozen=True, slots=True)
class Edge:
    from_node: BaseNode
    to_node: BaseNode
    route: RouteValue | list[RouteValue] | None = None  # None=无条件, DEFAULT_ROUTE=兜底

@dataclass(slots=True)
class Graph:
    edges: list[Edge] = field(default_factory=list)
    nodes: list[BaseNode] = field(default_factory=list)  # __post_init 从 edges 推导

    def __post_init__(self):
        # 1. 拒绝显式传 nodes
        # 2. 从 edges 按 id() 去重保序推导 nodes

    def validate_graph(self):
        # 委托给 utils/graph_validation.py 的模块级 validate_graph

    @property
    def terminal_node_names(self) -> set[str]:
        # 无出边的非 START 节点

    def get_next_pending_nodes(self, node_name, routes_to_match) -> list[str]:
        # 路由匹配（和 ADK 一致）：
        # 1. route=None 的边总触发
        # 2. 具体 route 命中触发
        # 3. DEFAULT_ROUTE 兜底（无具体命中时）
        # 4. 全未命中 + 有 routing edges → warning + 空 list
```

**设计决策**：
- `Edge` 用 `from_node` / `to_node`（和 ADK 一致），非 `source` / `target`。
- `Graph` 是普通 dataclass（非 pydantic），运行时构造，不需要序列化。
- `__post_init` 不调 `validate_graph`（和 ADK 一致），需显式调用。
- `validate_graph` 方法委托给 `utils/graph_validation.py` 的模块级函数
  （方法内 import 避免循环依赖，和 ADK 一致）。
- `get_next_pending_nodes` 变量名和 ADK 完全一致：
  `next_pending_nodes`、`matched_specific_route`、`default_route_nodes`、
  `has_routing_edges`、`edge_matched`、`edge_routes`。

### `utils/graph_validation.py`（126 行）

模块级函数，按调用顺序排列：

```python
def validate_graph(nodes, edges) -> None:
    node_names = _validate_duplicate_node_names(nodes)     # 1. 节点名唯一
    _validate_start_node(node_names)                       # 2. START 存在
    _validate_start_edges(edges)                           # 3. START 出边无 route
    _validate_connectivity(edges, node_names)             # 4+5. 可达性 + START 无入边
    _validate_duplicate_edges(edges)                      # 6. 无重复边
    _validate_default_routes(edges)                       # 7. DEFAULT 不混 list
    _detect_unconditional_cycles(edges, node_names)       # 8. 无无条件环

def _validate_duplicate_node_names(nodes) -> set[str]: ...
def _validate_start_node(node_names) -> None: ...
def _validate_start_edges(edges) -> None: ...
def _validate_connectivity(edges, node_names) -> None: ...
def _normalize_route(route) -> set[RouteValue | None]: ...
def _validate_duplicate_edges(edges) -> None: ...
def _validate_default_routes(edges) -> None: ...
def _detect_unconditional_cycles(edges, node_names) -> None: ...
```

**校验规则**（对照 ADK 9 条，去掉 2 条）：
1. 节点名唯一 ✓
2. START 存在 ✓
3. START 出边无 route ✓
4. 从 START 可达所有节点 ✓
5. START 无入边 ✓
6. 无重复边（同 from→to + 同 route）✓
7. DEFAULT 不与其他 route 混在 list 里 ✓
8. 无纯无条件环（有 route 的环合法）✓
9. ~~schema 匹配 / chat agent 接线~~ — 不需要

### `join_node.py`（35 行）

```python
class JoinNode(BaseNode):
    @property
    @override
    def _requires_all_predecessors(self) -> bool:
        return True

    @override
    async def _run_impl(self, _ctx, node_input=None) -> AsyncGenerator[A2AEvent, None]:
        self.output = node_input  # 透传聚合 inputs
        return
        yield  # pyright: ignore[reportUnreachable]
```

### `trigger.py`（16 行）

```python
@dataclass
class Trigger:
    input: object | None = None
```

ADK 的 `Trigger` 有 `input`、`use_sub_branch`、`branch`；我们只需要 `input`
（无分支隔离）。

### `workflow.py`（67 行 — 当前状态）

```python
class Workflow(BaseNode):
    edges: list[Edge] = Field(default_factory=list)
    max_concurrency: int | None = None
    timeout: float | None = None
    _graph: Graph | None = PrivateAttr(default=None)

    @override
    def model_post_init(self, context):
        super().model_post_init(context)
        self._graph = Graph(edges=self.edges)
        self._graph.validate_graph()

    def _get_static_node_by_name(self, name) -> BaseNode:
        # 遍历 self._graph.nodes 查找（和 ADK 一致，O(n)）
        assert self._graph is not None
        for node in self._graph.nodes:
            if node.name == name:
                return node
        raise WorkflowError(f"node {name} not found in graph")

    def _seed_start_triggers(self, trigger_buffer):
        # 用 get_next_pending_nodes(START.name, None) 找 START 后继
        # 当前版本：不处理 JoinNode 后继（等 _buffer_barrier_trigger 实现后补）
        assert self._graph is not None
        next_nodes = self._graph.get_next_pending_nodes(START.name, None)
        for name in next_nodes:
            trigger_buffer.setdefault(name, []).append(Trigger())

    @override
    async def _run_impl(self, _ctx, _node_input=None) -> AsyncGenerator[A2AEvent, None]:
        # 占位 — 后续步骤填充
        return
        yield  # pyright: ignore[reportUnreachable]
```

**设计决策**：
- 只支持 `edges` 入口（不支持直接传 `Graph`）。`edges` 是 `list[Edge]`，
  运行时可动态构造，覆盖动态图场景。不需要额外的 `graph` 入口。
- `_graph` 是 `PrivateAttr`（非 pydantic 字段），在 `model_post_init` 里设置。
- `_get_static_node_by_name` 遍历 `graph.nodes` 查找（和 ADK 一致，O(n)），
  不用 `_nodes: dict[str, BaseNode]` 映射。
- `model_post_init` 里调 `validate_graph()`（Workflow 构造即校验），
  虽然和 ADK 的 Graph 不同（Graph 不自动校验），但 Workflow 级别的图必须合法。
- `_seed_start_triggers` 当前不处理 JoinNode 后继 — 等实现
  `_buffer_barrier_trigger` 后补。

### 测试文件

**`test_graph.py`**（271 行，25 tests）：
- helper: `_node(name)` → `BaseNode(name=name)`
- 4 个 valid graph 测试（线性/带 route/带 DEFAULT/routed cycle）
- 11 个 validation error 测试（重复节点名/无 START/START 出边有 route/
  START 有入边/不可达/重复边/同 route 重复/不同 route 重复/DEFAULT 混 list/
  无条件环/空图）
- 3 个 construction 测试（nodes 推导/拒绝显式传 nodes/terminal_node_names）
- 7 个 get_next_pending_nodes 测试（无条件/route 匹配/route 不匹配/
  DEFAULT 兜底/route+DEFAULT/route list/routes_to_match list）

**`test_workflow.py`**（27 行，2 tests）：
- helper: `_node(name)` → `BaseNode(name=name)`
- `test_seed_start_triggers_single` — `START→A`，buffer 有 `{"a": [Trigger()]}`
- `test_seed_start_triggers_multiple` — `START→A, START→B`，buffer 有两个

**测试模式**：
- `_node` helper 构造 `BaseNode(name=name)`
- `Workflow(name="wf", edges=[...])` 构造 workflow
- 访问 `_` 前缀方法需 `# pyright: ignore[reportPrivateUsage]`

## 四、待完成的步骤

### Step 8: `_NodeDone` 信号

**文件**: `workflow.py`（模块级，`Workflow` 类之前）

```python
@dataclass
class _NodeDone:
    """Signal: a node finished (success or failure).

    ADK 不需要这个信号 — 它用独立 EventQueue 传事件 + task.result() 返回
    Context（带 error/output/route）。我们的 _run_impl 是 async generator，
    事件和完成信号必须走同一通道（asyncio.Queue），因此需要 _NodeDone。
    """
    node_name: str
    error: str | None = None
```

**无测试** — 被 Step 9 覆盖。

### Step 9: `_run_node` wrapper

**文件**: `workflow.py`（`Workflow` 类方法）

```python
async def _run_node(
    self,
    name: str,
    ctx: InvocationContext,
    node_input: object | None,
    queue: asyncio.Queue[A2AEvent | _NodeDone],
) -> None:
    """Execute a node, forwarding events and completion to queue."""
    node = self._get_static_node_by_name(name)
    try:
        async with (
            asyncio.timeout(self.timeout) if self.timeout
            else nullcontext()
        ):
            async for event in node.run(ctx, node_input=node_input):
                await queue.put(event)
        await queue.put(_NodeDone(name))
    except Exception as e:
        await queue.put(_NodeDone(name, error=str(e)))
```

**设计点**：
- `node_input` 从 `Trigger.input` 传入
- `timeout` 用 `asyncio.timeout`，`None` 时用 `nullcontext`
- 所有异常 catch，放 `_NodeDone.error`，不 raise（避免 task 异常被 asyncio 吞掉）
- output/route 不在 `_NodeDone` 里传 — 直接从 `node.output` / `node.route` 读取
  （task 完成后节点对象还在）

**测试**（需 fake node）：

```python
class _FakeNode(BaseNode):
    """Yields canned events, optionally fails or times out."""
    def __init__(self, name, events=None, output=None, error=None, delay=0):
        self.name = name
        self._events = events or []
        self.output = output
        self._error = error
        self._delay = delay

    async def _run_impl(self, _ctx, _node_input=None):
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._error:
            raise RuntimeError(self._error)
        for event in self._events:
            yield event
```

| 测试 | 场景 | 预期 |
|------|------|------|
| `test_run_node_success` | fake node yield 2 事件 | queue 有 2 事件 + `_NodeDone` 无 error |
| `test_run_node_failure` | fake node raise | queue 有 `_NodeDone` 有 error |
| `test_run_node_timeout` | fake node sleep > timeout | `_NodeDone` 有 timeout error |
| `test_run_node_events_forwarded` | yield 的事件出现在 queue | 验证事件顺序 |

**ADK 对应**：`_node_runner.py` 的 `NodeRunner.run()` + `_run_node_loop()` +
`_run_node_loop_with_timeout()`。ADK 的 NodeRunner 有 retry 逻辑，我们暂不实现。

### Step 10: `_schedule_ready_nodes` + `_at_concurrency_limit`

**文件**: `workflow.py`（`Workflow` 类方法）

```python
def _at_concurrency_limit(
    self,
    pending_tasks: dict[str, asyncio.Task[None]],
) -> bool:
    return (
        self.max_concurrency is not None
        and self.max_concurrency > 0
        and len(pending_tasks) >= self.max_concurrency
    )

async def _schedule_ready_nodes(
    self,
    ctx: InvocationContext,
    trigger_buffer: dict[str, list[Trigger]],
    pending_tasks: dict[str, asyncio.Task[None]],
    queue: asyncio.Queue[A2AEvent | _NodeDone],
) -> None:
    """Pop triggers from buffer and start node tasks.

    不检查前驱是否完成 — barrier 逻辑在 _buffer_downstream_triggers 里
    由 _requires_all_predecessors 控制（和 ADK 一致）。
    """
    for name in list(trigger_buffer.keys()):
        if name in pending_tasks:
            continue  # already running
        if self._at_concurrency_limit(pending_tasks):
            break
        trigger = trigger_buffer[name].pop(0)
        if not trigger_buffer[name]:
            del trigger_buffer[name]
        task = asyncio.create_task(
            self._run_node(name, ctx, trigger.input, queue)
        )
        pending_tasks[name] = task
```

**设计点**：
- 不检查前驱（`_all_predecessors_completed`）— barrier 逻辑在
  `_buffer_downstream_triggers` 里由 `_requires_all_predecessors` 控制。
  普通节点：前驱完成 → 立即 trigger。JoinNode：等所有前驱完成 →
  `_buffer_barrier_trigger` 才加 trigger。
- `trigger_buffer` 是 `dict[str, list[Trigger]]`（和 ADK 一致），
  key 是 target node name，value 是待处理的 trigger 列表。
- `pending_tasks` 是 `dict[str, asyncio.Task[None]]`，key 是 node name。
- 变量名和 ADK 一致：`trigger_buffer`、`pending_tasks`、`_at_concurrency_limit`。

**ADK 对应**：`_workflow.py:502-541` 的 `_schedule_ready_nodes` +
`_at_concurrency_limit`。ADK 有 `NodeState` / `NodeStatus` 跟踪 RUNNING/WAITING，
我们不需要（DAG 每个节点只跑一次，`pending_tasks` 就够了）。

| 测试 | 场景 | 预期 |
|------|------|------|
| `test_schedule_ready_single` | buffer 有 1 个 trigger | 1 个 task 启动 |
| `test_schedule_ready_skip_running` | 节点已在 pending_tasks | 跳过 |
| `test_schedule_ready_concurrency_limit` | max_concurrency=2，buffer 有 3 个 | 只启动 2 个 |
| `test_schedule_ready_pops_trigger` | trigger 弹出后 buffer 清空 | buffer key 被删除 |

### Step 11: `_buffer_downstream_triggers`

**文件**: `workflow.py`（`Workflow` 类方法）

```python
def _buffer_downstream_triggers(
    self,
    node_name: str,
    trigger_buffer: dict[str, list[Trigger]],
    node_outputs: dict[str, object],
    completed: set[str],
) -> None:
    """Find downstream nodes and add triggers to buffer."""
    assert self._graph is not None
    node = self._get_static_node_by_name(node_name)
    next_nodes = self._graph.get_next_pending_nodes(node_name, node.route)
    for target_name in next_nodes:
        target = self._get_static_node_by_name(target_name)
        if target._requires_all_predecessors:
            self._buffer_barrier_trigger(
                target_name, trigger_buffer, completed, node_outputs
            )
        else:
            trigger_buffer.setdefault(target_name, []).append(
                Trigger(input=node.output)
            )
```

**设计点**：
- `node.route` 从 `BaseNode.route` 字段读取（节点在 `_run_impl` 里设）。
- JoinNode 后继 → `_buffer_barrier_trigger`（等所有前驱）。
- 普通后继 → 直接 `Trigger(input=node.output)`。
- `completed` 参数传给 `_buffer_barrier_trigger` 做前驱检查。

**ADK 对应**：`_workflow.py:843-871` 的 `_buffer_downstream_triggers`。
ADK 有 `use_sub_branch` / `branch`，我们不需要。

| 测试 | 场景 | 预期 |
|------|------|------|
| `test_buffer_downstream_simple` | A→B，A 完成后 | buffer 有 B 的 trigger |
| `test_buffer_downstream_with_route` | A→B(route="x")，A.route="x" | B 入 buffer |
| `test_buffer_downstream_route_no_match` | A→B(route="x")，A.route="y" | B 不入 buffer |
| `test_buffer_downstream_join_node` | A→J, B→J（J=JoinNode），A 完成后 | J 不入 buffer（等 B） |

### Step 12: `_buffer_barrier_trigger`

**文件**: `workflow.py`（`Workflow` 类方法）

```python
def _buffer_barrier_trigger(
    self,
    target_name: str,
    trigger_buffer: dict[str, list[Trigger]],
    completed: set[str],
    node_outputs: dict[str, object],
) -> None:
    """Buffer a trigger for target_name once all predecessors complete.

    No-op while any predecessor is still outstanding.
    """
    assert self._graph is not None
    preds = {
        e.from_node.name for e in self._graph.edges
        if e.to_node.name == target_name
    }
    if not all(p in completed for p in preds):
        return  # 还有前驱未完成
    outputs = {p: node_outputs.get(p) for p in preds}
    trigger_buffer.setdefault(target_name, []).append(
        Trigger(input=outputs)
    )
```

**设计点**：
- 只在所有前驱完成时才加 trigger（input 是前驱 outputs dict）。
- 前驱集合从 `graph.edges` 反查（`e.to_node.name == target_name`）。
- START 也算前驱，但 START 在 `completed` 初始化时就加入。

**ADK 对应**：`_workflow.py:811-841` 的 `_buffer_barrier_trigger`。
ADK 有 `get_common_branch_prefix`，我们不需要。

| 测试 | 场景 | 预期 |
|------|------|------|
| `test_buffer_barrier_not_ready` | A→J, B→J，A 完成 B 没完成 | J 不入 buffer |
| `test_buffer_barrier_ready` | A→J, B→J，A 和 B 都完成 | J 入 buffer，input=`{"a":..., "b":...}` |

### Step 13: `_run_loop` + `_run_impl` 完成

**文件**: `workflow.py`

#### `_run_loop`（async generator，yield A2AEvent）

```python
async def _run_loop(
    self,
    ctx: InvocationContext,
    completed: set[str],
    pending_tasks: dict[str, asyncio.Task[None]],
    node_outputs: dict[str, object],
    trigger_buffer: dict[str, list[Trigger]],
    queue: asyncio.Queue[A2AEvent | _NodeDone],
) -> AsyncGenerator[A2AEvent, None]:
    """Main scheduling loop."""
    while True:
        await self._schedule_ready_nodes(
            ctx, trigger_buffer, pending_tasks, queue
        )

        if not pending_tasks and not trigger_buffer:
            break  # 全部完成

        if not pending_tasks:
            raise WorkflowError("workflow deadlocked")

        item = await queue.get()
        if isinstance(item, _NodeDone):
            pending_tasks.pop(item.node_name)
            if item.error:
                for task in pending_tasks.values():
                    task.cancel()
                raise WorkflowError(
                    f"node {item.node_name} failed: {item.error}"
                )
            else:
                completed.add(item.node_name)
                node = self._get_static_node_by_name(item.node_name)
                if node.output is not None:
                    node_outputs[item.node_name] = node.output
                self._buffer_downstream_triggers(
                    item.node_name, trigger_buffer, node_outputs, completed
                )
        else:
            yield item  # A2AEvent
```

#### `_run_impl` 完成

```python
@override
async def _run_impl(
    self, ctx: InvocationContext, _node_input: object | None = None
) -> AsyncGenerator[A2AEvent, None]:
    """Orchestration loop: SETUP → LOOP → FINALIZE."""
    assert self._graph is not None

    # SETUP
    completed: set[str] = {START.name}
    pending_tasks: dict[str, asyncio.Task[None]] = {}
    node_outputs: dict[str, object] = {}
    trigger_buffer: dict[str, list[Trigger]] = {}
    queue: asyncio.Queue[A2AEvent | _NodeDone] = asyncio.Queue()

    self._seed_start_triggers(trigger_buffer)

    # LOOP
    async for event in self._run_loop(
        ctx, completed, pending_tasks, node_outputs,
        trigger_buffer, queue,
    ):
        yield event

    # FINALIZE
    self._node_outputs = node_outputs
```

**设计点**：
- `_run_loop` 是 async generator（yield events），`_run_impl` 用 `async for`
  转发事件。这是我们的架构决定 — 因为 `_run_impl` 是 async generator，
  事件必须从 `_run_loop` 流出到 `_run_impl` 的 yield 点。
- `completed` 初始化为 `{START.name}` — START 不执行，视为已完成。
- 失败时取消所有运行中的 task（`task.cancel()`），然后 `raise WorkflowError`。
- 死锁检测：`not pending_tasks and trigger_buffer` — buffer 有 trigger
  但无法调度（应该是 JoinNode 前驱未完成的场景，但合法图中不会出现）。
- `self._node_outputs` 存最终输出（terminal 节点的 output）。

**ADK 对应**：`_workflow.py:286-412` 的 `_run_loop` + `_run_impl`。
ADK 有 abort signal、replay、dynamic nodes、interrupt 收集等，我们都不需要。

| 测试 | 场景 | 预期 |
|------|------|------|
| `test_workflow_linear` | START→A→B→C | 所有事件按序 yield |
| `test_workflow_parallel` | START→A, START→B, A→J, B→J(J=JoinNode) | J 在 A 和 B 都完成后跑一次 |
| `test_workflow_conditional_route` | A→B(route="x"), A→C(route=DEFAULT), A.route="x" | 只有 B 跑 |
| `test_workflow_failure` | A 失败 | `WorkflowError` |
| `test_workflow_deadlock` | （理论上合法图不会出现，但可构造 buffer 有 trigger 无法调度的场景） | `WorkflowError` |

## 五、完整数据流

```
SETUP:
    START.name → completed
    _seed_start_triggers → START 后继入 trigger_buffer

LOOP (每轮):
    _schedule_ready_nodes:
        遍历 trigger_buffer:
            跳过已在 pending_tasks 的
            检查 max_concurrency 限制
            弹出 Trigger
            _run_node(name, ctx, trigger.input, queue) → asyncio task
            pending_tasks[name] = task

    item = await queue.get():
        if _NodeDone:
            pending_tasks.pop(node_name)
            if error:
                cancel all pending_tasks
                raise WorkflowError
            else:
                completed.add(node_name)
                node_outputs[node_name] = node.output
                _buffer_downstream_triggers:
                    get_next_pending_nodes(name, node.route):
                        → 普通后继: Trigger(input=node.output) → buffer
                        → JoinNode 后继: _buffer_barrier_trigger:
                            等所有前驱完成
                            → Trigger(input={pred: output, ...}) → buffer
        else:
            yield item  (A2AEvent → _run_impl → 调用方)

    if not pending_tasks and not trigger_buffer:
        break  (全部完成)

FINALIZE:
    self._node_outputs = node_outputs
```

## 六、设计决策汇总

| # | 决策 | 理由 | ADK 对应 |
|---|------|------|----------|
| 1 | `Workflow` 继承 `BaseNode` | 可嵌套 — workflow 本身是图中的节点 | `Workflow(BaseNode)` |
| 2 | 只支持 `edges` 入口 | `edges` 是 `list[Edge]`，运行时可动态构造 | ADK 支持 `edges` + `graph`，我们简化 |
| 3 | `RouteValue` 放 `base_node.py` | 消除 `base_node.py` ↔ `graph.py` 运行时循环 import | ADK 无此问题（RouteValue 在 `_graph.py`） |
| 4 | `output` / `route` 放 `BaseNode` 字段 | ADK 放 per-node Context 上；我们无 per-node Context | ADK: `Context.output` / `Context.route` |
| 5 | `node_input` 参数加到 `run` / `_run_impl` | 前驱 output 通过 `Trigger.input` 传入 | ADK: `node.run(ctx=ctx, node_input=...)` |
| 6 | `_requires_all_predecessors` 是 `@property` | 类级行为，只能子类覆写 | ADK: `@property` |
| 7 | `_NodeDone` 信号 | ADK 用独立 EventQueue + task 返回值；我们的 `_run_impl` 是 async generator，事件和完成信号必须走同一通道 | ADK 无此概念 |
| 8 | `_schedule_ready_nodes` 不检查前驱 | barrier 逻辑在 `_buffer_downstream_triggers` 里 | ADK 一致 |
| 9 | `validate_graph` 在 `model_post_init` 调用 | Workflow 构造即校验（Graph 不自动校验） | ADK: Workflow.model_post_init 调 validate_graph |
| 10 | 暂不实现重试 | 等需要时在 `_run_node` 内部加 | ADK: NodeRunner._attempt_retry |
| 11 | `_seed_start_triggers` 暂不处理 JoinNode 后继 | 等 `_buffer_barrier_trigger` 实现后补 | ADK: _seed_start_triggers 有 JoinNode 分支 |
| 12 | `_get_static_node_by_name` 遍历查找 | 和 ADK 一致，O(n)，图通常很小 | ADK: `_get_static_node_by_name` |

## 七、测试模式

### helper

```python
def _node(name: str) -> BaseNode:
    return BaseNode(name=name)
```

### fake node（Step 9 起）

```python
class _FakeNode(BaseNode):
    def __init__(self, name, events=None, output=None, error=None, delay=0):
        self.name = name
        self._events = events or []
        self.output = output
        self._error = error
        self._delay = delay

    async def _run_impl(self, _ctx, _node_input=None):
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._error:
            raise RuntimeError(self._error)
        for event in self._events:
            yield event
```

### pyright 注意

- 访问 `_` 前缀方法需 `# pyright: ignore[reportPrivateUsage]`
- `Workflow(name="wf", edges=[...])` 构造时必须传 `name`（继承自 BaseNode）

## 八、验证命令

```bash
uv run pytest tests/next/workflow -q
uv run ruff check --fix src tests && uv run ruff format src tests
uv run basedpyright
```

## 九、提交记录

| commit | 内容 |
|--------|------|
| `4ee8e7d` | workflow 图引擎地基 — BaseNode + Edge/Graph + graph_validation + 18 tests |
| `9b56b7c` | workflow 编排层 — BaseNode(output/route/node_input) + JoinNode + Trigger + Workflow(骨架) + RouteValue 提至 base_node |

## 十、修订记录

- 2026-10-15 初始计划 — 从 ADK workflow 借鉴，分 13 步实现。
- 2026-10-15 更新 — 补充完整代码状态、ADK 对应关系、设计决策理由、测试模式。
