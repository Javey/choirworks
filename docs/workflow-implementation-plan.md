# workflow 图引擎实现计划

> 状态：**核心完成**，待后续扩展（A2A 远端节点、编排 Agent）。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）。仅借鉴设计，不复制代码；实现时借鉴点须在文件头
> 标注来源与许可证。

## 一、项目背景

在 `new_choirworks` 项目的 `core/workflow/` 目录构建通用图引擎，为编排 agent
（OrchestratorAgent）提供 DAG 计划调度能力。图引擎借鉴 ADK 的 workflow 机制，
但简化了 replay/resume/HITL/分支隔离等特性。

最终目标：LLM 生成 DAG 计划 → 远端 A2A 调用执行节点 → 流式回收结果。

## 二、ADK 参考路径

| ADK 文件 | 对应我们的文件 | 借鉴点 |
|----------|---------------|--------|
| `workflow/_base_node.py` | `base_node.py` | BaseNode + START + run/_run_impl 分层 |
| `workflow/_graph.py` | `graph.py` | Edge + Graph + get_next_pending_nodes |
| `workflow/utils/_graph_validation.py` | `utils/graph_validation.py` | 7 条校验 |
| `workflow/_join_node.py` | `join_node.py` | JoinNode + _requires_all_predecessors |
| `workflow/_trigger.py` | `trigger.py` | Trigger（简化，去掉 branch） |
| `workflow/_workflow.py` | `workflow.py` | Workflow + 调度循环 |
| `workflow/_node_runner.py` | `node_runner.py` | NodeRunner（简化，去掉 retry/child Context/enrichment） |
| `workflow/_node_state.py` | `node_state.py` | NodeState（简化，只保留 status + input） |
| `workflow/_node_status.py` | `node_status.py` | NodeStatus（只保留 RUNNING/COMPLETED/FAILED） |
| `agents/context.py` | `context.py` | Context（简化，只保留 output/route/error） |
| `workflow/_errors.py` | `errors.py` | GraphValidationError + WorkflowError + NodeTimeoutError |

## 三、与 ADK 的关键差异

1. **双通道设计** — ADK 事件走 `ic._enqueue_event`（EventQueue），完成走 `task.result()`（Context）。
   我们照搬：事件走 `ctx.invocation_context.event_queue.enqueue_event`，完成走 `task.result()`（Context）。
   `_run_impl` 不 yield 事件（`return; yield` 占位，和 ADK 一样）。
2. **Context 简化** — ADK 的 Context 有 20+ 字段（node_path、branch、state、telemetry 等）；
   我们的 Context 只有 5 个字段：`invocation_context`、`node`、`output`、`route`、`error`。
3. **NodeRunner 简化** — ADK 的 NodeRunner 有 retry、child Context 创建、event enrichment、
   output/delta flushing 等；我们的 NodeRunner 只做：遍历 `node.run()` → enqueue 事件 → 返回 Context。
4. **NodeState 简化** — ADK 的 NodeState 有 6 个字段（status、input、attempt_count、interrupts、
   resume_inputs、run_id）；我们的 NodeState 只有 2 个：`status`、`input`。
5. **NodeStatus 对齐** — 枚举值与 ADK 七态一一对应（INACTIVE/PENDING/RUNNING/COMPLETED/WAITING/FAILED/CANCELLED）；
   引擎当前实际只会用到 `INACTIVE`、`RUNNING`、`COMPLETED`、`FAILED`、`CANCELLED`。
6. **`_LoopState` 替代参数展开** — 所有循环状态放 `_LoopState` dataclass，和 ADK 一致。
7. **无 trigger buffer replay** — ADK 有 replay manager 从 session 事件恢复；我们不做。
8. **无 retry** — 暂不实现，等需要时在 `NodeRunner.run` 内部加。
9. **无 abort/interrupt/resume** — 暂不实现。

## 四、文件布局

```
src/new_choirworks/core/workflow/
  __init__.py                  # 空
  base_node.py                 # ✅ BaseNode + START + RouteValue + timeout
  context.py                   # ✅ Context（per-node 状态）
  errors.py                    # ✅ GraphValidationError, WorkflowError, NodeTimeoutError
  graph.py                     # ✅ Edge, Graph, DEFAULT_ROUTE, NodeLike
  join_node.py                 # ✅ JoinNode
  node_runner.py               # ✅ NodeRunner
  node_state.py                # ✅ NodeState
  node_status.py               # ✅ NodeStatus
  trigger.py                   # ✅ Trigger
  workflow.py                  # ✅ Workflow + _LoopState
  utils/
    __init__.py                # 空
    graph_validation.py        # ✅ validate_graph + 7 条校验

tests/next/workflow/
  __init__.py                  # 空
  test_graph.py                # ✅ 25 tests
  test_workflow.py             # ✅ 2 tests（_seed_start_triggers）
  test_workflow_e2e.py         # ✅ 6 tests（端到端）
```

## 五、已完成代码状态

### `base_node.py`

- `type RouteValue = bool | int | str`
- `BaseNode(BaseModel)`: `model_config(extra="forbid")`, `name`, `description`, `timeout: float | None = None`
- `run(ctx: Context, node_input)` — `aclosing` 包裹 `_run_impl`
- `_run_impl(ctx, node_input)` — `raise NotImplementedError; yield`（占位）
- `@property _requires_all_predecessors` — `return False`
- `START = BaseNode(name="__START__")`

**设计决策**：
- `output` / `route` 不在 `BaseNode` 上 — 移到 `Context`（per-node 状态）。
- `timeout` 在 `BaseNode` 上（和 ADK 一致），`NodeRunner` 读取。
- `RouteValue` 定义在此文件，消除 `base_node.py` ↔ `graph.py` 运行时循环 import。
- `run()` / `_run_impl()` 参数是 `Context`（不是 `InvocationContext`）。
- `TYPE_CHECKING` import `Context` 避免循环 import。

### `context.py`

- `Context` dataclass: `invocation_context: InvocationContext`, `node: BaseNode`, `output: object | None = None`, `route: RouteValue | None = None`, `error: Exception | None = None`

**设计决策**：
- ADK 的 Context 有 20+ 字段，我们只有 5 个。
- `invocation_context` 是共享的（所有节点同一个），`output`/`route`/`error` 是 per-node 的。
- `error` 类型是 `Exception | None`（和 ADK 一致），不是 `str | None`。

### `node_runner.py`

- `NodeRunner` dataclass: `node: BaseNode`, `parent_ctx: Context`
- `run(node_input) -> Context` — 创建 child Context，执行节点，catch error 到 `ctx.error`，返回 Context
- `_execute_node(ctx, node_input)` — 读 `node.timeout`，调 `_run_node_loop` 或 `_run_node_loop_with_timeout`
- `_run_node_loop(ctx, node_input)` — `aclosing` 包裹 `node.run()`，事件 enqueue 到 `ctx.invocation_context.event_queue`
- `_run_node_loop_with_timeout(ctx, node_input, timeout)` — `asyncio.timeout` 包裹 `_run_node_loop`，超时 raise `NodeTimeoutError`
- `_enqueue_event(event, ctx)` — `ctx.invocation_context.event_queue.enqueue_event(event)`

**ADK 对应**：`_node_runner.py` 的 `NodeRunner`。砍掉 retry、child Context 创建、event enrichment、output/delta flushing、telemetry。

### `node_state.py`

- `NodeState` dataclass: `status: NodeStatus = NodeStatus.INACTIVE`, `input: object | None = None`

### `node_status.py`

- `NodeStatus(Enum)`: 与 ADK 七态同值 — `INACTIVE = 0`, `PENDING = 1`, `RUNNING = 2`, `COMPLETED = 3`, `WAITING = 4`, `FAILED = 5`, `CANCELLED = 6`

### `errors.py`

- `GraphValidationError(ValueError)`
- `WorkflowError(RuntimeError)`
- `NodeTimeoutError(Exception)`: `node_name: str`, `timeout: float`

### `graph.py`

- `DEFAULT_ROUTE = "__DEFAULT__"`
- `type NodeLike = BaseNode | Literal["START"]`
- `Edge` (frozen dataclass): `from_node`, `to_node`, `route: RouteValue | list[RouteValue] | None = None`
- `Graph` (dataclass): `edges`, `nodes`（从 edges 推导）, `validate_graph()`, `terminal_node_names`, `get_next_pending_nodes()`

### `join_node.py`

- `JoinNode(BaseNode)`: `_requires_all_predecessors = True`, `_run_impl` → `ctx.output = node_input`

### `trigger.py`

- `Trigger` dataclass: `input: object | None = None`

### `workflow.py`

- `_LoopState` dataclass: `trigger_buffer`, `pending_tasks`, `nodes: dict[str, NodeState]`, `node_outputs`
- `Workflow(BaseNode)`: `edges`, `max_concurrency`, `graph: Graph | None = None`
- `model_post_init` — 构造 Graph + validate_graph
- `_get_static_node_by_name(name)` — 遍历 `graph.nodes` 查找
- `_seed_start_triggers(loop_state, node_input)` — START 后继入 trigger_buffer，JoinNode 后继走 _buffer_barrier_trigger
- `_run_impl(ctx, node_input)` — SETUP → _run_loop（try/finally 包 _cleanup_all_tasks）→ _finalize → `return; yield`
- `_run_loop(ctx, loop_state)` — `asyncio.wait(FIRST_COMPLETED)` + `ordered_done` + `_pop_completed_task` + error/completion 处理
- `_schedule_ready_nodes(ctx, loop_state)` — RUNNING 检查 + `_at_concurrency_limit` + `_pop_trigger` + `_prepare_node_state_for_starting` + `_start_node_task`
- `_at_concurrency_limit(loop_state)` — `max_concurrency` 检查
- `_prepare_node_state_for_starting(loop_state, node_name, trigger)` — `NodeState(status=RUNNING, input=trigger.input)`
- `_start_node_task(ctx, loop_state, node_name, trigger)` — `NodeRunner.run(trigger.input)` → asyncio task
- `_buffer_barrier_trigger(loop_state, target_name)` — 前驱完成检查 + 聚合 outputs dict → Trigger
- `_pop_completed_task(loop_state, task)` — 从 pending_tasks 反查 task → node_name
- `_handle_completion(loop_state, node_name, child_ctx)` — `NodeStatus.COMPLETED` + `node_outputs` + `_buffer_downstream_triggers`
- `_buffer_downstream_triggers(loop_state, node_name, child_ctx)` — `get_next_pending_nodes` → 普通后继 Trigger / JoinNode 后继 _buffer_barrier_trigger
- `_finalize(loop_state, ctx)` — 收集 terminal 节点输出；1 个 → `ctx.output`；多个 → raise `WorkflowError`
- `_cleanup_all_tasks(loop_state)` — cancel 遗留 pending_tasks + `gather(return_exceptions=True)` + 被取消节点标 `NodeStatus.CANCELLED`
- `_pop_trigger(trigger_buffer, node_name)` — 模块级函数，pop trigger

### `invocation_context.py`

- `InvocationContext` dataclass: `messages`, `context_id`, `task_id`, `event_queue: EventQueue | None = None`

### `executor.py`

- `LLMAgentExecutor.execute` — 传 `event_queue=event_queue` 给 `InvocationContext`

## 六、完整数据流

```
SETUP:
    _LoopState() 创建（空 trigger_buffer, pending_tasks, nodes, node_outputs）
    _seed_start_triggers → START 后继入 trigger_buffer
        JoinNode 后继 → _buffer_barrier_trigger（START 短路通过）

LOOP (每轮):
    _schedule_ready_nodes:
        遍历 trigger_buffer:
            跳过 RUNNING 状态的节点
            _at_concurrency_limit → break
            _pop_trigger
            _prepare_node_state_for_starting → NodeState(RUNNING)
            _start_node_task → NodeRunner.run(trigger.input) → asyncio task
            pending_tasks[name] = task

    asyncio.wait(FIRST_COMPLETED) → done
    ordered_done = 按 pending_tasks 插入顺序排序

    for task in ordered_done:
        _pop_completed_task → name
        child_ctx = task.result()
        if child_ctx.error:
            NodeStatus.FAILED
            raise WorkflowError
        else:
            _handle_completion:
                NodeStatus.COMPLETED
                node_outputs[name] = child_ctx.output
                _buffer_downstream_triggers:
                    get_next_pending_nodes(name, child_ctx.route):
                        → 普通后继: Trigger(input=child_ctx.output) → buffer
                        → JoinNode 后继: _buffer_barrier_trigger:
                            等所有前驱 COMPLETED
                            → Trigger(input={pred: output, ...}) → buffer

    if not pending_tasks:
        break

CLEANUP (try/finally):
    _run_loop 退出（正常/异常/取消）→ _cleanup_all_tasks:
        cancel 未完成的 pending_tasks
        gather(return_exceptions=True) 等待取消完成
        被取消的节点标 NodeStatus.CANCELLED

FINALIZE:
    _finalize → terminal 节点输出写回 ctx.output（多个 raise WorkflowError）
```

## 七、设计决策汇总

| # | 决策 | 理由 | ADK 对应 |
|---|------|------|----------|
| 1 | `Workflow` 继承 `BaseNode` | 可嵌套 | `Workflow(BaseNode)` |
| 2 | `output`/`route` 在 `Context` 上 | per-node 状态，和 ADK 一致 | ADK: `Context.output` / `Context.route` |
| 3 | `NodeRunner` 独立类 | 和 ADK 同构 | ADK: `NodeRunner` |
| 4 | 双通道：事件走 EventQueue，完成走 task.result() | 和 ADK 一致 | ADK: `ic._enqueue_event` + `task.result()` |
| 5 | `_run_impl` 不 yield 事件 | 事件走 EventQueue 旁路 | ADK: `return; yield` 占位 |
| 6 | `_LoopState` 携带所有循环状态 | 和 ADK 同构 | ADK: `_LoopState` |
| 7 | `NodeState` 只有 status + input | 砍掉 retry/interrupt/resume/run_id | ADK: 6 个字段 |
| 8 | `NodeStatus` 枚举与 ADK 七态同值 | 当前仅用 INACTIVE/RUNNING/COMPLETED/FAILED/CANCELLED | ADK: 7 个状态 |
| 9 | `Context` 只有 5 个字段 | 砍掉 node_path/branch/state/telemetry 等 | ADK: 20+ 字段 |
| 10 | `_buffer_downstream_triggers` 接收 child_ctx | 用户设计选择 | ADK: 接收 output + route |
| 11 | `asyncio.timeout` 替代 `asyncio.wait_for` | ruff ASYNC109 建议 | ADK: `asyncio.wait_for` |
| 12 | 外层 `aclosing` 包裹 `node.run()` | 异常时触发内层 aclosing | ADK: `Aclosing` |
| 13 | `_at_concurrency_limit` 在 RUNNING 检查后 | 和 ADK 一致 | ADK: `:528` |
| 14 | `_cleanup_all_tasks` 取消遗留任务并标 CANCELLED | 防任务泄漏（失败/外部取消） | ADK: `_cleanup_all_tasks`（砍 dynamic tasks） |

## 八、ADK 同构规则

借鉴 ADK 编写 `core/` 时保持同构：方法名、调用链、方法拆分与 ADK 一一对应。不合并、不内联、不简化结构。即使方法再简单也要独立存在，和 ADK 保持一致。除非用户主动提出变更。

## 九、测试

### 当前测试（38 tests）

| 文件 | 测试数 | 内容 |
|------|--------|------|
| `test_graph.py` | 25 | 图校验 + get_next_pending_nodes |
| `test_workflow.py` | 3 | _seed_start_triggers + _cleanup_all_tasks 标 CANCELLED |
| `test_workflow_e2e.py` | 10 | 线性/并行/Join/条件路由/失败/失败取消遗留任务/并发限制/terminal 输出/多 terminal 报错/节点超时 |

### 端到端测试模式

- `_FakeEventQueue` — 继承 `EventQueue`，收集事件到 list
- `_FakeNode` — 继承 `BaseNode`，yield 假事件 + 设 `ctx.output`/`ctx.route`，记录 `received_inputs`
- `_CapturingJoinNode` — 继承 `JoinNode`，记录 `received_inputs`
- 验证方式：事件数量 + `received_inputs` + 异常

## 十、待完成

| # | 内容 | 说明 |
|---|------|------|
| 1 | ~~A2A 远端节点~~ | 已完成：`RemoteA2aAgent`（core/a2a）+ `run_remote_node`（orchestrator） |
| 2 | ~~编排 Agent~~ | 已完成：`OrchestratorAgent`（new_choirworks/orchestrator）+ Server 接线（`BaseAgentExecutor`） |

### Orchestrator v0 限制（当前实现边界）

- 任一 A2A 节点失败 → 整体失败（不自动重试、不交 Planner 重规划）。
- `cancel()` 只取消本地 asyncio 任务并触发 agent 清理，不向远端发 `CancelTask`。
- 无 HITL resume：远端 `INPUT_REQUIRED` / `AUTH_REQUIRED` 暂停态直接判失败（resume TODO）。
- 无进程重启恢复：编排状态仅存于单次 invocation 内存。
- Planner 结构化输出非法时直接失败，无错误反馈重试。

## 十一、验证命令

```bash
uv run pytest tests/next/workflow -q
uv run ruff check --fix src tests && uv run ruff format src tests
uv run basedpyright
```

## 十二、提交记录

| commit | 内容 |
|--------|------|
| `4ee8e7d` | workflow 图引擎地基 — BaseNode + Edge/Graph + graph_validation + 18 tests |
| `9b56b7c` | workflow 编排层 — BaseNode(output/route/node_input) + JoinNode + Trigger + Workflow(骨架) + RouteValue 提至 base_node |
| `b79f35f` | docs: workflow 图引擎实现计划 |
| `7b81616` | feat: workflow NodeRunner + Context + NodeState — 双通道设计 |
| `611a06e` | feat: workflow _run_loop 循环体 + _handle_completion + _buffer_downstream_triggers + _pop_completed_task |
| `885d800` | test: workflow 端到端测试 — 5 场景 |
| `0d2f7e6` | feat: workflow _at_concurrency_limit + 端到端并发限制测试 |
| `c9712f8` | feat: workflow _finalize + 节点超时端到端测试 — 图引擎核心闭环 |
| `41d0f28` | feat: workflow _cleanup_all_tasks — 取消遗留任务+标 CANCELLED；AGENTS.md 禁全量测试；basedpyright tests 放开 reportPrivateUsage |

## 十三、修订记录

- 2026-10-15 初始计划 — 从 ADK workflow 借鉴，分 13 步实现。
- 2026-10-15 更新 — 补充完整代码状态、ADK 对应关系、设计决策理由、测试模式。
- 2026-10-16 重写 — 核心完成，更新为当前状态（Context/NodeRunner/NodeState/NodeStatus/双通道设计）。
- 2026-10-16 补全 `_finalize`（terminal 输出写回 ctx.output，多 terminal raise WorkflowError）+ 节点超时端到端测试，图引擎核心闭环完成。
- 2026-10-16 补 `_cleanup_all_tasks`（_run_loop 外包 try/finally，取消遗留任务 + 标 CANCELLED）；修正文档：NodeStatus 实为 ADK 七态同值、NodeState 默认 INACTIVE。
