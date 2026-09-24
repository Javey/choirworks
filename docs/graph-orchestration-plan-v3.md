# choirworks 编排图化设计方案 v3（渐进式，融合定稿）

> 状态：**已定稿，待实施**（2026-09-24）。
> 前版：`docs/graph-orchestration-plan.md`（v1，完整自研引擎，未实施）、
> `docs/graph-orchestration-plan-v2.md`（v2，渐进式，未实施）。
> 本稿以 v2 为主体，吸收 v1 的 ADK 对照细节，修正两稿的事实偏差；v1/v2 保留备查（见「十、风险与熔断」）。
> 设计借鉴：google-adk 2.9.0（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）的 workflow 图机制。**仅借鉴设计，不复制代码、不引入依赖**；
> 实现时借鉴点注释须标注来源与许可证（仓库先例：`tools/create_plan.py:37-39`）。
> 文中行号为撰写时快照（HEAD=86a0faa），实施时以实际代码为准。

## 一、背景与痛点

「下一步做什么」的判断分散在 4 个 if/elif 决策梯 + 1 个散落的状态机里，
新增分支（新意图、新失败策略、新求助去向）需要修改多处循环代码，
退出与重启条件分居不同文件，liveness 难以验证：

| # | 判断簇 | 现状位置 |
|---|---|---|
| ① | 调度终局判断 | `orchestration/runner.py:151-229`（顺序敏感梯子；`input_required` 时隐式 return，重启条件在 `a2a/executor.py` 答复分支） |
| ② | 节点状态转移 | 散落 7+ 文件（`runner.py`、`node_executor.py`、`routing.py`、`intervention.py`、`patch.py`、`repair.py`、`a2a/executor.py`）；`"recover"` 是魔法字符串不在 `NodeStatus` 枚举内（`state.py:362`、`patch.py:10`、`executor.py:329`、`runner.py:71-75`、`node_executor.py:68-78`）；重试谓词在 `runner.py` 内重复 3 次（:59、:144、:151） |
| ③ | 入口路由链 | `a2a/executor.py:125` 起（execute 决策段）+ `orchestration/routing.py:20-113` 合起来是一棵决策树却分居两文件 |
| ④ | 派生节点生成 | `routing.py:116-159`（`-f{n}`）、`assist.py:39-60`（`-a{n}`）、`tools/call_subagent.py:84-108`（`-h{n}`）三套实现，各自实现 `max_derived_nodes` 上限与 persist/emit 序列 |

## 二、决策

| # | 决策 |
|---|------|
| 1 | **渐进式**：只搬 ADK 的结构层（Route 标签、声明式边、构造期校验），runner 调度循环保留，决策梯逐个改写为声明式流图，可分步验证 |
| 2 | **范围**：4 个痛点全部纳入，实施顺序见「七、分阶段实施」（L1 状态机先行，流图按 ①→③→②④ 迁移，派生节点收尾） |
| 3 | **持久化**：route/trigger 决策不持久化，重启后从 `OrchestrationState` 现状重新推导（与现有 recovery 行为一致，不搬 ADK 的事件重放体系） |

## 三、ADK 结构层要点（借鉴对象）

ADK 2.9.0 中 SequentialAgent/LoopAgent/ParallelAgent 已 **deprecated（未移除）**，
官方推荐 Workflow 图引擎（`sequential_agent.py:89-93`、`loop_agent.py:62-65`；
注意官方同时注明 Workflow 尚不能作为 LlmAgent sub-agent）。本方案搬其**结构层思想**，不搬执行引擎。

### 3.1 借鉴的三点

- **Route 标签而非条件闭包**：节点运行时「发 route」（`RouteValue = bool | int | str`，
  `_graph.py:38`），边声明自己匹配的 route（`Edge`，`_graph.py:59-77`，单值或 list），
  `DEFAULT_ROUTE`（`_graph.py:91`）兜底。决策是数据而非代码路径 → 可枚举、可校验。
- **route 匹配语义**（`Graph.get_next_pending_nodes`，`_graph.py:138-188`）：
  `route=None` 的边永远触发（:151-154）；具体 route 边命中才触发（:161-175）；
  无任何具体命中时走 DEFAULT（:177-178）；全未命中且无 DEFAULT → 打 warning，分支结束（:180-186）。
- **构造期校验**（`utils/_graph_validation.py:212-223`，共 9 条）：
  节点名唯一、必须含 START、START 出边不带 route、全节点从 START 可达、无重复边、
  DEFAULT 每源至多一条、**禁止纯无条件边环**（含路由边的环才合法，:28-61）、
  边上 schema 匹配、chat agent 接线限制。坏图永远不会运行。

### 3.2 本方案的增量（ADK 没有）

**route 覆盖检查**：handler 返回注解写 `-> Literal["retry", "wait_input"]`，
`validate()` 用 `typing.get_type_hints` 提取 Literal 集合，逐一核对每条可发 route 都有出边接住——
route 打错字在 import 时暴露。ADK 无此检查（route 经 `event.actions.route` 运行时写入，
`_node_runner.py:329-332`，未命中仅 warning），本方案比 ADK 更强，且 basedpyright 全程可检。

### 3.3 不借鉴的部分及理由

| ADK 机制 | 不搬的理由 |
|---|---|
| trigger buffer + `asyncio.wait` 图调度（`_workflow.py:299-389`） | runner 循环保留；调度语义已有 deps/槽位逻辑，重写给不到收益只给风险 |
| JoinNode 屏障、并发分支 | 四个决策梯都是线性链，无真并发分支需求（出现时见熔断条款） |
| 事件 replay / rehydration / checkpoint（`utils/_replay_manager.py` 等） | ADK 的跨进程恢复靠 session 事件重放 + 快进（`_LoopState` 本身确是单次调用内存态，`_workflow.py:78-82`）；ChoirWorks 以 `OrchestrationState` 快照为事实来源，恢复语义已成立，不引入事件溯源 |
| `RequestInput` / interrupt / resume_inputs | ADK 的 interrupt 也非协程挂起（节点跑完置 WAITING、run 结束、下次 replay 快进）；ChoirWorks 的 HITL 已是同构的业务态 `INPUT_REQUIRED` + Intervention，无需第二套 |
| `retry_config` / `timeout` / `max_concurrency` 引擎原语 | 重试是调度层路由分支；超时留在远端节点内部；并发槽位按 slos 在节点内计算。出现第二个使用点再提取 |
| `ctx.run_node()` 动态节点调度（`agents/context.py:422-479`） | 计划任务的动态挂载沿用现有 runner 批量派发；且 ADK 的 detached 动态节点不能 resume（`_workflow.py:426-430`），模型边界不匹配 |

## 四、总体设计：两层

### 4.1 L1 — 节点生命周期状态机（Phase 1）

1. `orchestration/state.py`：`NodeStatus` 增加 `RECOVER = "recover"` 成员，消灭魔法字符串；
   `NodeState.status` 类型从 `str` 收紧为 `NodeStatus`；状态集合
   （terminal/active/pending/input，`state.py:45-53`）补上 RECOVER 归属。
2. 新建 `orchestration/transitions.py`：
   - `ALLOWED_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]]` 集中转移表
   - `async def transition(ctx, node_id, to, **fields)`：校验合法性 → 应用变更 →
     persist → emit delta，**唯一状态变更入口**
   - `can_retry(node, max_attempts)` 谓词收敛为一个函数
3. 迁移所有散落变更点（痛点表 ②）为 `transition()` 调用。
   persist+emit 样板代码随迁移消灭（现约 8 处重复）。

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
Route = str   # 每个流用自己的 Literal 集合

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
- **校验（定义时执行，仿 ADK 构造期校验）**：handler 引用存在、start 存在、
  所有 handler 从 start 可达、无重复边、DEFAULT 每源至多一条、route 覆盖检查（见 3.2）。

### 4.3 类型与归属约束

- **禁止 Any**（AGENTS.md）：payload 用 typed dataclass，节点间数据流用 `object` + isinstance /
  pydantic `TypeAdapter` 收窄。
- 归属注释格式（`orchestration/graph.py` 文件头）：

  ```python
  # 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
  # src/google/adk/workflow/_graph.py、utils/_graph_validation.py
  ```

### 4.4 持久化与恢复（不变）

- route/trigger 决策**不持久化**。流图每次运行是短生命周期纯函数链，
  重启后由 `recover` / `answer_intervention` 等入口从 `OrchestrationState` 现状重新推导。
- `OrchestrationState` 快照（contexts 表 + Task.metadata）、rewind、事件线格式全部不动。

## 五、四张流图（Phase 3-5 逐个迁移）

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

**③ `message_flow`（入口路由链，`executor.py` + `routing.py`）**——payload: `MessagePayload`

```
start: prepare_inbound ─┬ "recover" → do_recover ─→ END   # 协议准备短路
                        └ "ready"   → classify_inbound
classify_inbound ─┬ "malformed"       → do_reject
                  ├ "answers"         → do_answers
                  ├ "unanswered_pending" → do_reemit_questions   # 终端
                  ├ "room"            → classify_room
                  ├ "empty"           → do_complete
                  └ DEFAULT           → do_plan_and_launch

classify_room ─┬ "quote_active"    → do_enqueue
               ├ "quote_completed" → do_spawn_followup
               ├ "interrupt"       → do_interrupt
               ├ "target"          → do_enqueue
               └ DEFAULT           → do_new_plan
```

`prepare_inbound` 承担 inbound 协议准备（`get_user_input`、解析 `question_response`、
`current_task is None` 时建初始 Task、`TaskUpdater`、`room_options`+mentions），并对 recover
**短路**——recover 不解析 message、不判断 `current_task`、不建 updater/room，直达 `do_recover`。
`executor.execute` 只留结构性三样：`ensure_session`（产出 runtime）、`_build_ctx`（图的 ctx）、
`runtime.lock`（作用域），其余一行 `await message_flow.run(ctx, payload)`。

recover 逻辑（协议探测 `is_recover_request` + 领域逻辑 `recover_session`）全部抽离到
`orchestration/recover.py`，executor 不再含任何 recover 代码。

**② `outcome_flow`（交付结果分支，`node_executor.py:149-193`）**——payload: `OutcomePayload`

```
start: interpret ─┬ "deliver"   → do_mark_delivered → do_arbitrate_mentions → do_deliver_queued
                  ├ "need_info" → do_mark_input_required
                  └ "revise"    → do_revise_plan → do_mark_delivered → do_arbitrate_mentions → do_deliver_queued
```

`interpret` = 现有 `_interpret_outcome`（marker → LLM → 异常默认 deliver 三层，原样保留）。

**④ `settlement_flow`（求助结算梯，`settlement.py`）**——payload: `SettlementPayload`

```
start: inspect_helpers ─┬ "already_pending"  → already_pending(noop) ─→ END
                        ├ "helper_active"    → helper_active(noop) ───→ END
                        ├ "helper_completed" → resolve_from_helper ──→ EXIT_DONE
                        └ "decide"           → decide_assistance ─"act"→ act ─→ EXIT_DONE | END
```

`decide_assistance` 后的 `act` 在**同一把 `ctx.lock` 内**完成「spawn_assist 失败则转人工」，
以保持与旧实现一致的锁语义（即 `spawn_assist`/`request_human` 不拆成两个持锁节点）。

> 与 v1 的取舍（已确认放弃，理由见「九、明确不做」）：v1 的 `TaskWorkflow`
> （node_executor 全量图化）与 `AnswerWorkflow`（answer 守卫链图化）不采纳；
> 本稿 `outcome_flow` 只覆盖交付结果分支，`message_flow` 的 `do_answers`
> 直接调用现有 `answer_intervention` 函数。

## 六、Phase 6：统一派生节点生成

新建 `orchestration/derived.py`：

```python
async def spawn_derived_node(
    ctx, parent: NodeState, kind: DerivedKind, *, input_text: str, ...
) -> NodeState
# kind ∈ {followup(-f{n}), assist(-a{n}), helper(-h{n})}
# 统一：id 方案按 kind、join_members、max_derived_nodes 上限、persist+emit 一次完成
```

`routing.py:116`、`assist.py:39`、`tools/call_subagent.py:84` 三处全部改调它。
（注意保持各 kind 现有差异：followup 不 join_members，其余两个 join——
差异收敛为参数而非三套代码。）

## 七、分阶段实施

| Phase | 内容 | 主要文件 | 验收 |
|---|---|---|---|
| 1 | L1 状态机：RECOVER 入枚举 + `transitions.py` + 迁移全部变更点 | `state.py`、`transitions.py`（新）+ 7 个调用方 | 全量测试全绿；非法转移抛错有单测 |
| 2 | L2 原语：`graph.py` + `validate()` + 单测 | `graph.py`（新）、`tests/unit/test_graph.py`（新） | 校验器单测：不可达 handler、重复边、多重 DEFAULT、Literal route 缺边 |
| 3 | ① `plan_flow`（最高优先） | `runner.py` 决策段 → `plan_flow` | test_recovery、test_a2a_recovery；退出/重启条件同表可见 |
| 4 | ③ `message_flow`（含 `prepare_inbound`；recover 逻辑抽到 `recover.py`） | `executor.py`、`message.py`（新）、`routing.py`、`recover.py`（新） | test_a2a_send / queue / hitl / stream / rewind、test_recovery |
| 5 | ② `outcome_flow` + ④ `settlement_flow` | `node_executor.py`、`intervention.py` | test_interventions、test_a2a_announcements / revise / sim_flow |
| 6 | 派生节点统一 `derived.py` | `routing.py`、`assist.py`、`call_subagent.py` | 三处行为等价（id 方案、上限、join 差异保留） |

- 每个 Phase 独立提交、测试全绿后才进入下一个。
- **行为等价是硬约束**：事件序列、状态机、A2A 线格式是前端契约，只搬逻辑不加能力，
  靠现有集成测试锁死。
- Phase 1 与 Phase 2 互相独立可并行；Phase 3-6 依赖 1+2。

## 八、验证命令

- 后端测试：`uv run pytest tests -q`
- Lint：`uv run ruff check --fix src tests` && `uv run ruff format src tests`
- 类型检查：`uv run basedpyright`（0 errors；`Literal` route 注解全程可检）
- 前端（本改造不动前端）：如涉及时 `npm test`、`npm run build`、`npm run lint`

## 九、明确不做

- 不建 trigger buffer / asyncio 图调度引擎（runner 循环保留；v1 完整引擎方案留在旧稿备查）。
- 不改 plan DAG 的 deps 调度与并发槽位逻辑。
- 不搬 ADK 的事件 replay / rehydration / checkpoint：ADK 靠 session 事件重放 + 快进实现
  跨进程恢复；ChoirWorks 以 `OrchestrationState` 快照为事实来源，恢复语义已成立。
- 不实现 ADK 的 `RequestInput` 协程中断 / `resume_inputs`（HITL 走业务态 `INPUT_REQUIRED`）。
- 不实现 retry / timeout / max_concurrency 引擎原语（出现第二个使用点再提取）。
- 不持久化 route 决策（重启从状态重推导）。
- 不引入 google-adk 库依赖。
- 不改动 A2A 线格式、函数调用 artifact、questions 消息与前端契约。
- **不图化 `node_executor` 的远端调用段**（dispatch/recover/continue 是 mode 驱动的线性
  固定序列，无路由扩散，图化无收益；v1 的 `TaskWorkflow` 方案放弃）。
- **不图化 `answer_intervention` 守卫链**（86a0faa 已重写为按 id 应答的线性守卫且有单测；
  v1 的 `AnswerWorkflow` 方案放弃；答复入口已由 `message_flow` 覆盖）。

## 十、风险与熔断

1. **顺序敏感梯子的等价改写**：runner 终局梯的 if/elif 顺序即优先级，
   改成流图后顺序体现在「谓词链 + DEFAULT」结构里，需逐分支对照现有测试。
2. **`transition()` 集中化可能暴露隐性转移**：现有代码存在理论非法的转移路径
   （如 recover 相关），迁移时以现状行为为准补全转移表，不趁机「修正」行为。
3. **熔断条件**：若某个决策梯在表达上确实需要真并发分支/环调度
   （目前判断：不需要，四个梯子都是线性链），说明渐进式选型不当，
   该梯升级为 v1 引擎方案（`docs/graph-orchestration-plan.md` 备查），其余梯不受影响。

## 十一、修订记录

- 2026-09-24 定稿（v3）：以 v2（渐进式）为主体融合 v1 的 ADK 对照细节；
  依据 ADK 2.9.0 源码核实修正事实（deprecated 措辞、`_LoopState` 与 replay 层的关系、
  环的规则、9 条校验出处）；显式放弃 v1 的 `TaskWorkflow`/`AnswerWorkflow` 并记录理由。
