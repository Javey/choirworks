# choirworks 编排图化设计方案 v2（渐进式，归档备查）

> 状态：**已被取代，保留备查**。实施依据改用 `docs/graph-orchestration-plan-v3.md`
> （本稿为主体的融合定稿）。
> 前版：`docs/graph-orchestration-plan.md`（v1，未实施）。v2 与 v1 的核心差异见「二、与 v1 的差异」。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）的 workflow 图机制。**仅借鉴设计，不复制代码、不引入依赖**；
> 实现时借鉴点注释须标注来源与许可证（仓库先例：`tools/create_plan.py:37-39`）。
> 文中行号为撰写时快照（HEAD=607906c），实施时以实际代码为准。

## 一、背景与决策

「下一步做什么」的判断分散在 4 个 if/elif 决策梯 + 1 个散落的状态机里，
新增分支（新意图、新失败策略、新求助去向）需要修改多处循环代码，
退出与重启条件分居不同文件，liveness 难以验证：

| # | 判断簇 | 现状位置 |
|---|---|---|
| ① | 调度终局判断 | `orchestration/runner.py:151-229`（顺序敏感梯子；`input_required` 时隐式 return，重启条件在 `a2a/executor.py:219-228`） |
| ② | 节点状态转移 | 散落 7+ 文件（`runner.py`、`node_executor.py`、`routing.py`、`intervention.py`、`patch.py`、`repair.py`、`a2a/executor.py`）；`"recover"` 是魔法字符串不在 `NodeStatus` 枚举内；重试谓词在 `runner.py` 内重复 3 次（:59、:144、:153） |
| ③ | 入口路由链 | `a2a/executor.py:148-243`（7 级 if/elif）+ `orchestration/routing.py:20-113` 合起来是一棵决策树却分居两文件 |
| ④ | 派生节点生成 | `routing.py:116-159`（`-f{n}`）、`assist.py:46-71`（`-a{n}`）、`tools/call_subagent.py:92-118`（`-h{n}`）三套实现，各自实现 `max_derived_nodes` 上限与 persist/emit 序列 |

本对话确认的三项决策：

| # | 决策 |
|---|------|
| 1 | **渐进式**：只搬 ADK 的结构层（Route 标签、声明式边、构造期校验），runner 调度循环保留，4 个决策梯逐个改写为声明式流图，可分步验证 |
| 2 | **范围**：上述 4 个痛点全部纳入，迁移顺序 = ① → ② → ③ → ④ |
| 3 | **持久化**：route/trigger 决策不持久化，重启后从 `OrchestrationState` 现状重新推导（与现有 recovery 行为一致，不搬 ADK 的事件重放体系） |

## 二、与 v1 的差异

| 维度 | v1（旧稿） | v2（本稿） |
|---|---|---|
| 引擎 | 自研完整图引擎（trigger buffer、asyncio 图调度、环、JoinNode、动态节点） | 不建引擎；只有「线性流图」原语（约 150 行）：handler 发 route → 匹配边 → 下一个 handler |
| runner 循环 | 重写为 `Workflow.run` | 保留 `run_plan` 循环，仅终局判断段换成流图 |
| 状态机 | 未单独处理 | 先行建设集中转移表（Phase 1，是后续一切的地基） |
| 风险 | 引擎环+并发+动态节点是最易错处 | 无新调度语义，纯结构收敛，每步可独立验证 |
| 熔断 | 若膨胀到需重写 resume/replay 则回退为依赖 adk | v1 引擎方案保留在旧稿，若渐进式流图在某个决策梯上表达力不足（需要真并发分支/环调度），可升级到 v1 |

## 三、ADK 结构层要点（借鉴对象）

ADK 2.0 已废弃 SequentialAgent/LoopAgent，改为 `src/google/adk/workflow/` 的 `Workflow` 图引擎。
本方案搬其**结构层思想**，不搬执行引擎（ADK 的 `_LoopState` 是单次调用内存态，
ChoirWorks 的编排横跨多次进程生命周期，引擎不适用）：

- **Route 标签而非条件闭包**（`_graph.py:37,58-76`）：节点运行时「发 route」（`bool/int/str`），
  边声明自己匹配哪些 route；`DEFAULT_ROUTE`（`_graph.py:90`）兜底。
  决策是数据而非代码路径 → 可枚举、可校验。
- **构造期校验**（`utils/_graph_validation.py`，9 条规则）：handler 引用存在、
  从 start 可达、无重复边、DEFAULT 每源至多一条；坏图永远不会运行。
- **route 匹配语义**（`_graph.py:133-183`）：`route=None` 的边永远触发；
  具体 route 边命中才触发；都未命中走 DEFAULT；有条件边但全未命中 → 分支结束。
- **不借鉴的部分**：trigger buffer、`asyncio.wait` 图调度、JoinNode 屏障、
  checkpoint 事件重放、`RequestInput` 协程中断——ChoirWorks 已有对应机制
  （deps 调度、`OrchestrationState` 快照、业务态 `INPUT_REQUIRED`）。

## 四、总体设计：两层

### 4.1 L1 — 节点生命周期状态机（Phase 1）

1. `orchestration/state.py`：`NodeStatus` 增加 `RECOVER = "recover"` 成员，消灭魔法字符串
   （现在 `state.py:362`、`patch.py:10`、`a2a/executor.py:329`、`runner.py:70-74`、
   `node_executor.py:68-74` 里的裸字符串）；`NodeState.status` 类型从 `str` 收紧为
   `NodeStatus`；状态集合（terminal/active/pending/input，`state.py:45-53`）补上 RECOVER 归属。
2. 新建 `orchestration/transitions.py`：
   - `ALLOWED_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]]` 集中转移表
   - `async def transition(ctx, node_id, to, **fields)`：校验合法性 → 应用变更 →
     persist → emit delta，**唯一状态变更入口**
   - `can_retry(node, max_attempts)` 谓词收敛为一个函数
3. 迁移所有散落变更点（见上表 ②）为 `transition()` 调用。
   persist+emit 样板代码随迁移消灭（现在约 8 处重复）。

状态机快照（迁移目标，非法转移在 `transition()` 内抛错）：

```
pending ──(deps 全 completed)→ ready
ready ──(派发)→ submitted ──(远端 ack)→ working
working ──→ completed | failed | input_required | canceled
input_required ──(helper 输出 / 人答复)→ ready
failed ──(attempt < max)→ pending（重试）
failed / pending / ready / recover ──(patch invalidate / 级联)→ invalidated
任意活跃态 ──(重启恢复)→ recover(有 a2a_task_id) | pending
```

### 4.2 L2 — 声明式流图（Phase 2 建原语，Phase 3-5 迁移决策梯）

新建 `orchestration/graph.py`（与既有 `flows.py`——工具函数执行流——无关，命名已确认无冲突）：

```python
DEFAULT = "__default__"
Route = str   # 每个流用自己的 Literal 集合（见 4.3）

Handler: TypeAlias = Callable[[OrchestrationContext, P], Awaitable[Route]]
# P = 该流的 typed payload dataclass，handler 可 mutate 并沿链传递；全程无 Any

@dataclass(frozen=True, slots=True)
class Edge:
    source: str
    target: str
    routes: frozenset[Route]   # 空集 = 无条件边；含 DEFAULT = 兜底边

@dataclass(slots=True)
class Flow(Generic[P]):
    name: str
    start: str
    handlers: dict[str, Handler[P]]
    edges: tuple[Edge, ...]
    def validate(self) -> None: ...
    async def run(self, ctx, payload: P) -> FlowOutcome: ...
```

- `run()` 语义（线性链，与 ADK 路由匹配规则一致）：执行 start handler →
  按返回 route 匹配出边（无条件边总触发；具体 route 命中触发；无具体命中走 DEFAULT；
  全未命中且无 DEFAULT → 分支结束，返回 `FlowOutcome.END`）→ 执行目标 handler → 重复直到无出边。
- **终端动作**通过 `FlowOutcome` 返回值告知调用方
  （`CONTINUE` 回主循环 / `EXIT_WAIT` / `EXIT_DONE` / `EXIT_FAILED`），
  消灭「退出条件在 runner、重启条件在 executor」的割裂。
- **校验（定义时执行，仿 ADK 构造期校验）**：
  - handler 引用存在、start 存在、所有 handler 从 start 可达、无重复边、DEFAULT 每源至多一条
  - **route 覆盖检查（本方案特色）**：handler 返回注解写
    `-> Literal["retry", "wait_input"]`，`validate()` 用 `typing.get_type_hints`
    提取 Literal 集合，逐一核对每条可发 route 都有出边接住——
    route 打错字在 import 时暴露（比 ADK 的运行时 warning 更强，basedpyright 全程可检）。

### 4.3 四张流图（Phase 3-5 逐个迁移）

**① `plan_flow`（runner 终局梯，`runner.py:151-229`）**——payload: `None`

```
start: check_retryable ──"retry"──→ do_backoff_continue      # 终端: FlowOutcome.CONTINUE
      └─无条件─→ check_input_required ──"wait_input"──→ do_exit_wait    # 终端: EXIT_WAIT
      └─无条件─→ check_all_completed ──"plan_completed"──→ do_emit_completed  # EXIT_DONE
      └─无条件─→ check_repairable ──"repair"──→ do_repair
      │                                 └─"repair_ok"──→ do_backoff_continue
      │                                 └─DEFAULT──────→ do_emit_failed
      └─无条件─→ do_stalled_failed      # EXIT_FAILED
```

**③ `message_flow`（入口路由链，`executor.py:148-243` + `routing.py`）**——payload: 入站消息

```
start: classify_inbound ─┬ "recover"         → do_recover_task
                         ├ "malformed"       → do_reject
                         ├ "answers"         → do_answer_intervention
                         ├ "unanswered_pending" → do_reemit_questions   # 终端
                         ├ "active" | "pending_work" → classify_room
                         ├ "empty"           → do_complete_task
                         └ DEFAULT           → do_plan_and_launch

classify_room ─┬ "quote_active"    → do_enqueue
               ├ "quote_completed" → do_spawn_followup
               ├ "interrupt"       → do_cancel_active → do_spawn_followup
               ├ "target"          → do_enqueue
               └ DEFAULT           → do_plan_and_launch
```

`executor.execute` 收敛为「协议层（建 Task / 解析 question_response / 锁 / TaskUpdater）+
`await message_flow.run(ctx, msg)` 一行」。

**② `outcome_flow`（交付结果分支，`node_executor.py:157-193`）**——payload: `OutcomePayload`

```
start: interpret ─┬ "deliver"   → do_mark_delivered → do_arbitrate_mentions → do_deliver_queued
                  ├ "need_info" → do_mark_input_required
                  └ "revise"    → do_revise_plan → do_mark_delivered → do_arbitrate_mentions → do_deliver_queued
```

`interpret` = 现有 `_interpret_outcome`（marker → LLM → 异常默认 deliver 三层，原样保留）。

**④ `settlement_flow`（求助结算梯，`intervention.py:46-127`）**——payload: node_id

```
start: inspect_helpers ─┬ "already_pending"  → END(noop)
                        ├ "helper_completed" → do_resolve_from_helper → do_resume
                        ├ "helper_active"    → END(noop)
                        └ DEFAULT            → decide_assistance
decide_assistance ─┬ "spawn_assist" → do_spawn_assist ──失败──→ do_request_human
                   └ "human"        → do_request_human
```

### 4.4 持久化与恢复（不变）

- route/trigger 决策**不持久化**。流图每次运行是短生命周期纯函数链，
  重启后由 `recover` / `answer_intervention` 等入口从 `OrchestrationState` 现状重新推导——
  与现有 recovery 语义一致。
- `OrchestrationState` 快照（contexts 表 + Task.metadata）、rewind、事件线格式全部不动。

## 五、Phase 6：统一派生节点生成

新建 `orchestration/derived.py`：

```python
async def spawn_derived_node(
    ctx, parent: NodeState, kind: DerivedKind, *, input_text: str, ...
) -> NodeState
# kind ∈ {followup(-f{n}), assist(-a{n}), helper(-h{n})}
# 统一：id 方案按 kind、join_members、max_derived_nodes 上限、persist+emit 一次完成
```

`routing.py:116`、`assist.py:46`、`tools/call_subagent.py:92` 三处全部改调它。
（注意保持各 kind 现有差异：followup 不 join_members，其余两个 join——
差异收敛为参数而非三套代码。）

## 六、分阶段实施

| Phase | 内容 | 主要文件 | 验收 |
|---|---|---|---|
| 1 | L1 状态机：RECOVER 入枚举 + `transitions.py` + 迁移全部变更点 | `state.py`、`transitions.py`（新）+ 7 个调用方 | 全量测试全绿；非法转移抛错有单测 |
| 2 | L2 原语：`graph.py` + `validate()` + 单测 | `graph.py`（新）、`tests/unit/test_graph.py`（新） | 校验器单测：不可达 handler、重复边、多重 DEFAULT、Literal route 缺边 |
| 3 | ① `plan_flow`（用户标记最高优先） | `runner.py` 决策段 → `plan_flow` | test_recovery、test_a2a_recovery；退出/重启条件同表可见 |
| 4 | ③ `message_flow` | `executor.py`、`routing.py` | test_a2a_send / queue / hitl / stream / rewind |
| 5 | ② `outcome_flow` + ④ `settlement_flow` | `node_executor.py`、`intervention.py` | test_interventions、test_a2a_announcements / revise / sim_flow |
| 6 | 派生节点统一 `derived.py` | `routing.py`、`assist.py`、`call_subagent.py` | 三处行为等价（id 方案、上限、join 差异保留） |

- 每个 Phase 独立提交、测试全绿后才进入下一个。
- **行为等价是硬约束**：事件序列、状态机、A2A 线格式是前端契约，只搬逻辑不加能力，
  靠现有集成测试锁死。
- Phase 1 与 Phase 2 互相独立可并行；Phase 3-6 依赖 1+2。

## 七、验证命令

- 后端测试：`uv run pytest tests -q`
- Lint：`uv run ruff check --fix src tests` && `uv run ruff format src tests`
- 类型检查：`uv run basedpyright`（0 errors；`Literal` route 注解全程可检）
- 前端（本改造不动前端）：如涉及时 `npm test`、`npm run build`、`npm run lint`

## 八、明确不做

- 不建 trigger buffer / asyncio 图调度引擎（runner 循环保留；v1 方案留在旧稿备查）
- 不改 plan DAG 的 deps 调度与并发槽位逻辑
- 不实现 ADK 的 RequestInput 协程中断 / 事件重放 / checkpoint（HITL 走业务态，快照沿用）
- 不持久化 route 决策（重启从状态重推导）
- 不引入 google-adk 库依赖
- 不改动 A2A 线格式、函数调用 artifact、questions 消息与前端契约

## 九、风险与熔断

1. **顺序敏感梯子的等价改写**：runner 终局梯的 if/elif 顺序即优先级，
   改成流图后顺序体现在「谓词链 + DEFAULT」结构里，需逐分支对照现有测试。
2. **`transition()` 集中化可能暴露隐性转移**：现有代码存在理论非法的转移路径
   （如 recover 相关），迁移时以现状行为为准补全转移表，不趁机「修正」行为。
3. **熔断条件**：若某个决策梯在表达上确实需要真并发分支/环调度
   （目前判断：不需要，四个梯子都是线性链），说明渐进式选型不当，
   该梯升级为 v1 引擎方案，其余梯不受影响。

## 十、修订记录

- 2026-09-24 定稿（v2）：三项决策确认——渐进式结构层改造、四痛点按 ①②③④ 顺序全量纳入、
  route 决策不持久化（从状态重推导）。与 v1（完整自研引擎）分歧点见「二」。
