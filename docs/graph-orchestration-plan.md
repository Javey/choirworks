# choirworks 编排图化设计方案（定稿）

> 状态：**已定稿，待实施**（2026-09-24）。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）的 workflow 图机制。**仅借鉴设计，不复制代码**；
> 实现时借鉴点注释须标注来源与许可证（仓库先例：`tools/create_plan.py:37-39`）。
> 文中行号为撰写时快照（HEAD≈607906c），实施时以实际代码为准。

## 一、背景与目标

当前编排的「下一步做什么」判断分散在四处，以 if/elif 形式内嵌于控制循环或流程函数中，
新增一个分支（新意图、新失败策略、新求助去向）需要在多处修改循环代码：

| # | 判断簇 | 现状位置 |
|---|---|---|
| ① | 回合路由 | `a2a/executor.py:execute`（166-243）+ `orchestration/routing.py:route_message`（20-113） |
| ② | 任务生命周期 | `orchestration/node_executor.py:execute_node`（43-146）+ `_handle_completed`（149-193） |
| ③ | 计划调度 | `orchestration/runner.py:run_plan`（44-229） |
| ④ | 求助结算与人工介入 | `orchestration/intervention.py:settle_node_input`（46-103）+ `answer_intervention`（197-269） |

目标：引入图（Node + Edge + route）概念，把上述判断改写为显式的节点与路由边；
新增分支 = 新增节点/边，而非修改循环体。行为（事件序列、状态机、A2A 线格式）保持不变。

## 二、已确认决策

| # | 决策 |
|---|------|
| 1 | 图化范围：**全部四处**，分阶段实施，每阶段保持测试全绿 |
| 2 | 引擎来源：**仓库内自研最小图引擎**，借鉴 ADK 设计、不复制代码、不新增依赖 |
| 3 | 计划承载：**编排图静态 + 计划任务作为动态节点**（计划仍是 LLM 生成的动态 DAG） |
| 4 | 持久化：**沿用现有快照机制**（`OrchestrationState` 存 contexts 表 + Task.metadata，恢复重挂远程任务） |

## 三、总体设计

### 3.1 两层结构

```
choirworks/graph/          通用最小图引擎（借鉴 ADK Workflow）
choirworks/orchestration/  四张业务图，取代散落的 if/else
```

引擎不含任何 ChoirWorks 业务语义（不认识 A2A、Plan、Intervention）；
业务图通过 `GraphContext.orch` 访问 `OrchestrationContext` 并自行发事件。

### 3.2 引擎概念与 ADK 对照

| ADK（`src/google/adk/workflow/`） | `choirworks/graph/` | 说明 |
|---|---|---|
| `BaseNode._run_impl` 生成器 | `Node.run(ctx, data) -> NodeResult` | 节点产出 output + route；不走 ADK 的 Event 生成器 |
| `Edge(from,to,route)` + `DEFAULT_ROUTE` | 同 | route 支持 `bool/int/str/list[str]` |
| `Graph.nodes` 由 edges 推导 | 同 | 边引用节点对象，按对象 id 去重、保持顺序 |
| `Graph.get_next_pending_nodes` | 同 | 按 route 匹配出边；无匹配时走 `DEFAULT_ROUTE` 边 |
| `JoinNode._requires_all_predecessors` | 同 | 等待全部前驱完成，输入聚合为 `{前驱名: 输出}` |
| `Workflow._run_loop` | 同 | 触发缓冲 + `asyncio.wait(FIRST_COMPLETED)` + 路由传播，**支持环** |
| `START` 哨兵 | 同 | 入口标记，从不执行 |
| `ctx.run_node()` 动态节点调度 | `GraphContext.spawn/run_node` | 计划任务动态挂载、跟踪、取消 |
| `RequestInput` / interrupt / resume | **不实现** | ChoirWorks 的 HITL 是业务状态 `INPUT_REQUIRED` + Intervention，不是协程中断 |
| 事件重放 / 检查点 / rehydration | **不实现** | 沿用 `OrchestrationState` 快照 |
| `retry_config` / `timeout` | **先不实现** | 重试是调度层路由分支；超时留在远端节点内部；出现第二个使用点再提取 |
| `max_concurrency` | **先不实现** | 计划调度按现有 slos 逻辑在节点内计算槽位 |

### 3.3 引擎接口草案

```python
# graph/types.py
RouteValue: TypeAlias = bool | int | str
DEFAULT_ROUTE = "__DEFAULT__"

@dataclass(frozen=True, slots=True)
class NodeResult:
    output: object | None = None
    route: RouteValue | list[RouteValue] | None = None

# graph/node.py
class Node(ABC):
    name: str
    async def run(self, ctx: GraphContext, data: object) -> NodeResult: ...

class FunctionNode(Node):
    def __init__(self, name: str, fn: Callable[[GraphContext, object], Awaitable[NodeResult | None]]) -> None: ...

class JoinNode(Node): ...   # _requires_all_predecessors = True

# graph/graph.py
class Edge(BaseModel):
    from_node: Node
    to_node: Node
    route: RouteValue | list[RouteValue] | None = None

class Graph:
    nodes: list[Node]      # 由 edges 推导
    edges: list[Edge]
    def next_nodes(self, from_name: str, route: RouteValue | list[RouteValue] | None) -> list[str]: ...

# graph/context.py
class GraphContext:
    orch: OrchestrationContext
    outputs: dict[str, object]          # 本 workflow 内节点输出
    def spawn(self, node: Node, data: object, *, name: str | None = None) -> DynamicHandle: ...
    async def run_node(self, node: Node, data: object, *, name: str | None = None) -> object: ...
    async def wait_any(self, handles: Sequence[DynamicHandle]) -> None: ...
    def cancel_all(self) -> None: ...

# graph/workflow.py
class Workflow(Node):
    def __init__(self, name: str, edges: list[Edge]) -> None: ...
    async def run(self, ctx: GraphContext, data: object) -> NodeResult: ...
```

要点：

- **终端节点** = 无出边的节点；Workflow 收敛终端节点的 output/route 作为自身 `NodeResult`。
- **环**：Graph 校验显式允许环（`validate` 只检查名称唯一、边两端存在于节点集合）。
- **异常**：节点自行捕获预期错误并返回路由（如远端失败 → `route="failed"`）；
  未捕获异常视为引擎级错误，节点标记 FAILED 并向 Workflow 调用方抛出。
- **动态节点**：`spawn` 的任务由当前 Workflow 跟踪（`name` 唯一，重复 spawn 同 name 返回既有 handle），
  Workflow 结束或取消时统一清理；`run_node` 是 `spawn` + `await` 的语法糖。

### 3.4 调度语义

单次 `Workflow.run` 内部循环，语义与 ADK `_run_loop` 一致：

1. `START` 的直接后继入触发缓冲（input = workflow 的 node_input）。
2. 从缓冲按插入序取就绪节点，创建 asyncio task 执行。
3. `asyncio.wait(FIRST_COMPLETED)`；完成节点的 output/route 入 `ctx.outputs`。
4. 按 route 解析出边：无 route 的边总是触发；匹配特定 route 的边触发；
   无任何特定匹配且有 `DEFAULT_ROUTE` 边则触发 default；无匹配且无 default 记录 warning。
5. 目标若为 `JoinNode`：等全部前驱完成后聚合触发。
6. 无 pending task 且缓冲为空时结束，收集终端输出。

### 3.5 持久化与恢复

- 引擎内部状态（触发缓冲、运行中 task、节点 run 状态）**不持久化**，workflow 每次运行都是短生命周期。
- 业务状态继续由 `OrchestrationState`（nodes/members/interventions/queue）快照持久化，
  引擎节点通过 `ctx.orch` 读写并 `persist`。
- 计划调度图设计为**可从 START 幂等重入**：所有判断只读 `OrchestrationState`
  （`recover_retryable` / `spawn_settlements` / `dispatch_ready` 均有天然守卫），
  因此答复后 `start_runner` 重新跑图即可，与现有恢复语义一致。
- 引擎自身的 `NodeStatus`（INACTIVE/PENDING/RUNNING/COMPLETED/FAILED/CANCELLED）仅用于流程控制，
  **不序列化到 wire**；业务 `NodeStatus`（`orchestration/state.py`）与前端契约保持不变。

### 3.6 类型与归属约束

- **禁止 Any**（AGENTS.md）：节点数据流用 `object`，节点内 isinstance / pydantic `TypeAdapter` 收窄。
- 归属注释格式（每个 `graph/*.py` 文件头）：

  ```python
  # 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
  # src/google/adk/workflow/_workflow.py、_graph.py、_node.py
  ```

## 四、四张业务图

### 4.1 回合路由图（替换 ①）

```
TurnWorkflow:
START → classify_message ─┬ "recover"            → RecoverPlanNode
                          ├ "malformed"          → RejectNode
                          ├ "answers"            → ApplyAnswersNode ─┬ "pending" → EmitQuestionsNode
                          │                                           └ "done"    → EnsureRunnerNode
                          ├ "unanswered_pending" → EmitQuestionsNode
                          ├ "active" / "pending_work" → RouteActiveWorkflow
                          ├ "empty"              → CompleteNode
                          └ DEFAULT              → PlanAndLaunchNode

RouteActiveWorkflow:
START → classify_room ─┬ "quote_active"    → EnqueueNode
                       ├ "quote_completed" → FollowupNode
                       ├ "interrupt"       → CancelActiveNode → FollowupNode
                       ├ "target"          → EnqueueNode
                       └ DEFAULT           → PlanAndLaunchNode
```

- `DEFAULT_ROUTE` 承载 `routing.py:107-113`「否则起新计划」的兜底。
- `executor.execute` 只保留协议层：建 Task、解析 question_response、锁、TaskUpdater；
  决策段由 `await turn_workflow.run(...)` 取代。
- `route_message` / `spawn_followup_node` 的逻辑迁入 `turn.py`；
  `spawn_followup_node` 仍被任务图（`_deliver_queued`）复用，保留函数形态。

### 4.2 任务生命周期图（替换 ②）

每个计划节点一个 `TaskWorkflow` 实例（输入：`NodeState` + mode）：

```
START → prepare ─┬ "dispatch" → DispatchNode ─┐
                 ├ "recover"  → RecoverNode  ─┤
                 └ "continue" → ContinueNode ─┤
consume ←──────────────────────────────────────┘
consume ─┬ "completed"       → InterpretNode
         ├ "input_required"  → MarkInputRequiredNode
         ├ "canceled"        → MarkCanceledNode
         └ "failed"          → MarkFailedNode
InterpretNode ─┬ "deliver"   → MarkDeliveredNode → MentionNode → QueuedFollowupNode
               ├ "need_info" → MarkInputRequiredNode
               └ "revise"    → RevisePlanNode → MarkDeliveredNode
所有分支 → FinalizeNode
```

- 三个远端节点内部保留现有 `asyncio.timeout(ctx.config.node_timeout)`、异常捕获，
  失败返回 `route="failed"`；`MarkFailedNode` 不发终态（retry/repair 由调度图裁决）。
- `FinalizeNode` = `expire_cancel_requests` 事件 + `persist` + `emit_pending_questions`，
  保证所有分支收尾顺序与现状一致。
- `MarkDeliveredNode` 包裹现有 `emit_state_delta(... completed ...)`；
  `MentionNode` = `arbitrate_mentions`；`QueuedFollowupNode` = `_deliver_queued`。
- 判定逻辑 `_interpret_outcome`、`markers.parse_marker`、`OutcomeDecision` 原样复用。

### 4.3 计划调度图（替换 ③，带环）

```
PlanWorkflow:
START → recover_retryable      # failed 且 attempt < max → PENDING（含 backoff 等待）
      → spawn_settlements      # spawn 后台 SettleWorkflow 动态节点（幂等）
      → evaluate ─┬ "dispatch"  → dispatch_ready → evaluate
                  ├ "pause"     → pause_for_questions   # 终端
                  ├ "completed" → complete_plan         # 终端
                  ├ "repair"    → RepairNode → evaluate
                  ├ "fail"      → fail_plan             # 终端
                  └ "stalled"   → fail_plan             # 终端
```

- `evaluate` 纯读状态产出路由，对应 `run_plan:151-229` 的终态判断链。
- `dispatch_ready`：`slots = max_parallel - pending`，取 ready 批，
  spawn `TaskWorkflow` 动态节点并 `asyncio.wait(任务批 ∪ settle 任务, FIRST_COMPLETED)`，
  取代 `runner.py:66-148` 的批量派发与等待；完成后回 `evaluate`。
- settle 任务完成即唤醒调度，取代现有 `runtime.wake`（答复路径的 `_start_runner` 兜底保留）。
- `RepairNode` = `repair.repair_plan`（失败节点并入 invalidate），对应 `runner.py:181-207`。
- **幂等重入**是所有恢复路径成立的前提，Phase 4 专门补「重入两次不重复派发」测试。

### 4.4 求助结算与答复图（替换 ④）

```
SettleWorkflow（每个 input_required 节点后台运行）:
START → inspect_helpers ─┬ "already_pending"  → END(noop)
                         ├ "helper_completed" → ResolveFromHelperNode → ResumeNode
                         ├ "helper_active"    → END(noop)
                         └ DEFAULT            → DecideAssistanceNode
DecideAssistanceNode ─┬ "spawn_assist" → SpawnAssistNode ──失败──→ RequestHumanNode
                      └ "human"        → RequestHumanNode

AnswerWorkflow（executor 答复入口）:
START → lookup ─┬ "unknown"        → RejectNode
                ├ "confirm_cancel" → ResolveCancelNode ─┬ "cancel" → CancelNode
                │                                       └ "keep"   → END
                └ "question"       → ValidateNode ─┬ "invalid" → RejectNode
                                                    ├ "stale"   → ExpireNode
                                                    └ DEFAULT   → ResolveAnswerNode
```

- `settle_input` / `answer_intervention` 保留同名薄封装（跑图并返回结果），现有单测改动最小。
- `request_human`、`render_answer`、`_validate_answer`、`cancel_node` 保留为纯函数供节点调用。

## 五、文件布局

```
src/choirworks/graph/
  __init__.py     # 导出 + ADK 来源/许可证注释
  types.py        # RouteValue / DEFAULT_ROUTE / NodeResult
  node.py         # Node / FunctionNode / JoinNode / START
  graph.py        # Edge / Graph（节点推导、next_nodes、环与终端校验）
  context.py      # GraphContext（orch、outputs、spawn/run_node/wait_any/cancel_all）
  workflow.py     # Workflow 调度循环
src/choirworks/orchestration/
  turn.py         # 回合路由图 + RouteActiveWorkflow
  task.py         # 任务生命周期图（取代 node_executor.py）
  settle.py       # 求助结算 + 答复图
  plan.py         # 计划调度图（取代 runner.run_plan 的决策树）
  repair.py       # 保留 repair_plan/revise_plan；apply_patch_locked 改为节点引用
  assist.py       # arbitrate_mentions / spawn_assist 保留为节点函数
tests/unit/test_graph_engine.py
```

预估引擎 300–400 行 + 单测。

## 六、分阶段实施

| Phase | 内容 | 主要文件 | 验收重点 |
|---|---|---|---|
| 0 | 引擎 + 单测（纯新增，不改现有代码） | `graph/*`、`test_graph_engine.py` | 新单测覆盖：多路由匹配、`DEFAULT_ROUTE`、fan-out、Join 屏障、环、并发调度、异常传播、嵌套 Workflow、动态任务取消 |
| 1 | 回合路由图 | `turn.py`、`executor.py`、`routing.py`（删除/迁移） | test_a2a_send / queue / hitl / stream / rewind |
| 2 | 任务生命周期图 | `task.py`、删 `node_executor.py`、`runner.py` 调用点 | test_a2a_announcements / revise / queue / sim_flow |
| 3 | 结算 + 答复图 | `settle.py`、精简 `intervention.py`、`executor.py` | test_interventions、test_a2a_hitl |
| 4 | 计划调度图 | `plan.py`、`runner.py` 变薄、`repair/assist` 改节点 | test_recovery、test_a2a_recovery、幂等重入新测试 |
| 5 | 清理与文档 | 删死代码、README 架构说明 | 全量 + 前端 |

每阶段结束必须全绿后再进入下一阶段。

## 七、验证命令

- 后端测试：`uv run pytest tests -q`
- Lint：`uv run ruff check --fix src tests` && `uv run ruff format src tests`
- 类型检查：`uv run basedpyright`（0 errors，不新增 warning 基线）
- 前端（本改造不动前端，如涉及）：`npm test`、`npm run build`、`npm run lint`

## 八、明确不做

- 不实现 ADK 的 `RequestInput` 协程中断 / `resume_inputs` 恢复（HITL 走业务状态）。
- 不实现事件重放 / 检查点 / rehydration（沿用快照，与 hitl-plan.md「不搬事件溯源重放」一致）。
- 不引入 ADK 的 session / invocation / 合成 function call 体系。
- 不在引擎中提前实现 retry / timeout / max_concurrency 原语（出现第二个使用点再提取）。
- 不改动 A2A 线格式、函数调用 artifact、questions 消息与前端契约。

## 九、风险与熔断条件

1. **行为等价**：事件顺序与状态机是前端契约，每阶段只搬逻辑、不加能力，靠现有集成测试锁死。
2. **环 + 并发 + 动态节点**是引擎最易错处，Phase 0 单测先行覆盖。
3. **幂等重入**：计划调度图可从 START 重复运行是恢复路径成立的前提；
   `dispatch_ready` 必须在节点状态（SUBMITTED/READY/recover）层面防重复派发。
4. **熔断条件**：若引擎膨胀到需要重写 ADK 的 resume/replay/session 体系，
   说明自研选型错误，应回退为「直接依赖 google-adk」并重新评估适配层成本。

## 十、修订记录

- 2026-09-24 定稿：四项决策确认（全范围图化、自研最小引擎、静态编排图+动态计划节点、沿用快照持久化）。
