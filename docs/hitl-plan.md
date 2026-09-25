# choirworks HITL 问题交互改造计划（定稿）

> 状态：**已实施**（2026-09-24 完成 Phase 1-4，验证全绿）。本文是两份计划合并后的最终版。
> 生成时间：2026-09-23；合并定稿：2026-09-23；最终修订：2026-09-23（并入 a2a-sdk V2 流语义更正 + 结算即时化）
> 设计借鉴：google-adk 2.9.0（`/home/javey/Workspaces/adk-python`），Apache License 2.0。**仅借鉴设计，不复制代码**；注释引用 ADK 机制名处须标注来源与许可证（仓库先例：`tools/create_plan.py:37-39`）。
> 注：文中行号为撰写时快照（HEAD≈6501aff），实施时以实际代码为准。

## 一、目标

把 HITL 问题从「纯文本、无寻址、靠 runner 布尔标志唤醒」升级为：

1. 问题类型化：`input` / `select`（单选、多选）/ `confirm`
2. 每个问题携带发起人（`requester`：agent_name，或规划层的内部标识 `assistant`）
3. 前端按问题卡片独立回复（不再走聊天输入框的自然语言答复）
4. 多个 agent 的多个问题并存、可分别回答
5. 对外符合标准 A2A：**input-required 状态 + `status.message`** 承载问题；答复是标准 A2A 消息

### 事件面分工（三面收敛为两面）

- ~~面1 function_call artifact~~：**删除**（见决策 8）。ask_user 的 result 只是 ack，问题文本/id/requester/时间分别由面2、`Intervention` 字段覆盖，无独立消费者。
- **面2 `status.message`**：协议应答面——问题以 text part（通用客户端可读）+ data part（结构化规格）呈现，客户端按 `intervention_id` 回复。
- **面3 state_delta**：状态真源——intervention 的 pending/resolved/expired 流转、answer 回填，不落 artifact、可回放。

## 二、已确认的决策

| # | 决策 |
|---|------|
| 1 | 问题类型/选项**仅由本地编排器定义**（远端 A2A agent 不声明；远端提问默认 input） |
| 2 | 答复走 **A2A 消息 + data part**：`{intervention_id, answer}`，part metadata `cw_type:"question_response"` |
| 3 | 多个待答问题用**一条 `status.message` 多 part 聚合**，集合变化（创建/答复/过期）时重发 |
| 4 | `confirm_cancel` **也置 input_required**；节点继续执行，答复/过期后回到工作状态。任何 pending 期间，非终态状态更新统一显示 input_required（§4.3） |
| 5 | 前端：聊天流**内联问题卡片**；有待答问题时**隐藏输入框**；取消「直接打字即答复」 |
| 6 | `Intervention.id` 改用 **UUID**（`uuid.uuid4().hex`），删除 `next_intervention` 计数器 |
| 7 | 无答复 part 的消息在等待期间 → **重发问题消息**，不消费（对齐 ADK A2A `handle_user_input`，long_running_functions.py:159-204） |
| 8 | **ask_user 的 function_call artifact 移除**，机制为 `AgentFunction.emit_artifact: bool` 声明式标记（不按名字特判）；`execute_function` 跳过 artifact 但仍发状态事件。未来编排层 LLM 直调 ask_user 时行为自动一致 |
| 9 | `kind` 与 `question_type` 均用 **StrEnum**（`InterventionKind`、`QuestionType`），不再裸 str / 字面量联合 |
| 10 | select/multi/options **完整实现**：assistance 决策 subagent 可生成选项；前端 radio/checkbox |
| 11 | 删除 `runner_start_requested` 布尔握手：`answer_intervention` 返回 bool，executor 直接 `_start_runner`（幂等，已核实 runner.py:30-33） |
| 12 | 分 Phase 实施，每 Phase 跑完全部验证命令 |
| 13 | **前端 `SETTLED_STATES` 只保留四个终态**；`input_required` 视为非终态——续订（follow）继续、taskId 保留；`answerQuestion` 发往当前 `taskId`。这是决策 4 成立的必要条件（a2a-sdk V2 服务端不会因 input_required 断流，见 §4.4） |
| 14 | **结算即时化**：`settle_input` 拆出后台 `settle_node_input` 任务，问题在决策完成后**立即**通过 `emit_pending_questions` 推送，不等并行节点排空；答复用 `runtime.wake` 唤醒 runner |

## 三、现状缺陷（已在代码中核实）

1. `runner_start_requested` 布尔握手脆弱（`session.py:35`）；唯一读取方 `executor.py:167`，`routing.py:160`、`assist.py:72` 是冗余写（start_runner 幂等，route_message 路径本就会启动 runner）
2. `answer_intervention` 的 question 分支**不 persist**（`intervention.py:172-188`），违背「先落盘再恢复」
3. 答复无寻址：永远消费 `pending[0]`（`intervention.py:144`）；多干预并发串台
4. confirm_cancel 判定靠硬编码关键词 `_is_affirmative`（intervention.py:31-35）
5. 无状态守卫：已取消/作废节点会被答复复活成 `READY`
6. 恢复对账只覆盖 `confirm_cancel`（`state.py:452-466`）
7. `kind` 裸 str（state.py:153/206/219）
8. 前端 `conversationView.ts` 读 artifact 层级错误：`funcResult.intervention_id`（:623）应为 `funcResult.data.intervention_id`；`revise_plan` 的 `funcResult.added_nodes`（:584）同样错位（线格式 `FunctionResult{success, data, error}`）——payload 模型改造后的静默回归
9. 前端 `client.ts:60 answerIntervention` 是死代码（后端无路由，全仓无引用）
10. 前端 `SETTLED_STATES` 把 `input_required` 当结束（`useConversation.ts:19-25`）：send 结束后不再 follow、下一条消息清空 taskId → **真正的断流点**（服务端 V2 不会因 input_required 断流，见 §4.4），也是答复发错 task 的根因
11. 问题结算时机：`settle_input` 只在 runner 排空（`runtime.node_tasks` 为空）后调用（`runner.py:86-135`），ask_user 无法与并行节点同步推流——违背最初设想

## 四、协议设计

### 4.1 出站：标准 input-required 状态消息

```
TaskStatusUpdateEvent(
  status = TaskStatus(
    state = TASK_STATE_INPUT_REQUIRED,
    message = Message(role=ROLE_AGENT, message_id=…, task_id/context_id=…,
      parts = [
        # 问题 1
        Part(text="请选择采用的方案："),
        Part(data={"intervention_id":"<uuid>","node_id":"n1",
                   "requester":"product-manager","kind":"question",
                   "question_type":"select",
                   "options":["方案一","方案二","方案三"],"multi":false,
                   "question":"请选择采用的方案："},
             metadata={"cw_type":"question"}),
        # 问题 2（confirm_cancel）
        Part(text="计划修订建议打断节点 n2，是否打断？"),
        Part(data={"intervention_id":"<uuid>","node_id":"n2",
                   "requester":"assistant","kind":"confirm_cancel",
                   "question_type":"confirm","options":[],"multi":false,
                   "question":"…"},
             metadata={"cw_type":"question"}),
      ])
  )
)
```

规则：

- 一条消息聚合当前**全部 pending 问题**；每问题 = text part（通用客户端可读）+ data part（结构化规格）
- **仅在问题集合变化时重发**该消息；其余状态更新只把 state 保持在 `input_required`，不带 message（避免 SDK 把 `status.message` 反复搬进 `Task.history`）
- 集合变空 → 按正常流程回到 working/终态
- SDK 标准行为（已核实）：`status.message` 在下一次状态更新时移入 `Task.history`（a2a-sdk `task_manager.py:208,332`），回放天然可用

### 4.2 入站：标准 A2A 消息 + 答复 data part

```json
{ "role": "ROLE_USER", "taskId": "...", "contextId": "...",
  "parts": [
    { "content": { "$case": "text", "value": "方案二" } },
    { "content": { "$case": "data", "value": {
        "intervention_id": "<uuid>", "answer": "方案二" } },
      "metadata": { "cw_type": "question_response" } } ] }
```

- 多问题分别回复 = 各自一条消息，`intervention_id` 定位；confirm 的 answer 为布尔
- 等待期间收到不含 `question_response` 的消息 → 重发问题消息，不消费
- 非法答复（未知/已 resolved 的 id、选项不在 options、类型不符）→ emit `intervention.rejected`（metadata 带 id+reason），不消费

### 4.3 状态优先级

存在 pending 问题（含 confirm_cancel）时，所有**非终态**状态更新统一显示 `input_required`；终态（completed/failed/canceled）不受影响。

### 4.4 流与暂停事实（a2a-sdk V2，已核实）

- 应用实际使用 `DefaultRequestHandler = DefaultRequestHandlerV2`（`a2a/server/request_handlers/__init__.py`），绝非 `LegacyRequestHandler`（后者的 `EventConsumer.consume_all` 才会把 input_required 当 final 并关队列）
- V2 仅四个终态触发关流：`TERMINAL_TASK_STATES`（`active_task.py:83-88`）；`input_required`/`auth_required` 属 `INTERRUPTED_TASK_STATES`（`:89-92`），全 SDK 唯一消费点是非流式 `on_message_send` 的提前返回（`default_request_handler_v2.py:265-273`，注释：*"AgentExecutor will continue to run in the background"*）
- 流式与订阅不因 interrupted 中断：`ActiveTask.subscribe()`（`active_task.py:608-700`）基于 tap 队列 fan-out，仅在本请求 `_RequestCompleted`（`execute()` 返回）或终态/清理时结束
- **每条 `sendMessageStream` 订阅都会在 `_RequestCompleted` 结束**（与状态无关）；持续推流靠 `follow`/`resubscribeTask`
- 结论：**断流点在前端 `SETTLED_STATES`**；决策 4 / §4.3 在服务端是安全的，修复点在决策 13

## 五、实施阶段

### Phase 1 — Intervention.id 改 UUID（独立小步）

`src/choirworks/orchestration/state.py`

1. 顶部 `import uuid`
2. `add_intervention`：`id=f"iv{state.next_intervention}"` → `id=uuid.uuid4().hex`，删 `state.next_intervention += 1`
3. `add_cancel_request`：同上
4. `OrchestrationState`：删 `next_intervention: int = 1`
5. `start_new_plan`：删 `state.next_intervention = 1`
6. `state_to_json` / `state_from_json`：删 `next_intervention` 写出/读入

测试：

7. `tests/unit/test_state.py`：删 `state.next_intervention = 5` 及断言
8. `tests/unit/test_interventions.py`：`["iv1"]` → 先拿 `first = add_cancel_request(...)`，断言 `[expired[0].id] == [first.id]`

注：前端测试里的 `"iv1"` 只是构造 wire 数据的字面量，不受影响。`QueuedMessage` 的 `qm{n}` 计数器不动。

### Phase 2 — 前置稳健性清理（不动协议）

9. 删除 `runner_start_requested`：
   - `session.py:35` 删字段
   - `executor.py:163-169` 改为 `if await answer_intervention(...): self._start_runner(runtime)`
   - `routing.py:160`、`assist.py:72` 删冗余写
   - 测试引用：`tests/support/fakes.py:44`、`tests/unit/test_tools.py:87`、`tests/unit/test_interventions.py:43,160`
10. `answer_intervention` 统一「校验 → 改状态 → persist → emit delta → 返回 bool」；question 分支补 persist
11. 状态守卫：
    - question：node 不存在或 status ≠ `INPUT_REQUIRED` → 干预 `EXPIRED`，不复活
    - confirm_cancel：target 不在 `ACTIVE_NODE_STATUSES` → `EXPIRED`
12. `normalize_cancel_requests` → `normalize_interventions`（对所有 pending 对账），更新 `executor._recover` 调用点

### Phase 3 — 问题模型 + 标准 A2A 消息（后端）

13. `state.py`：
    - 新增 `InterventionKind(StrEnum)`：`QUESTION="question"` / `CONFIRM_CANCEL="confirm_cancel"`；`Intervention.kind`、`InterventionDelta.kind`、`InterventionDict` 全部类型化
    - 新增 `QuestionType(StrEnum)`：`INPUT` / `SELECT` / `CONFIRM`
    - `Intervention` 增加：`question_type`（默认 INPUT）、`options: list[str]`、`multi: bool`、`requester: str`、`answer: str | list[str] | bool | None`（`state_from_json` 做类型校验，禁止 Any）
    - 同步 `to_dict` / `from_dict` / `InterventionDelta`
    - `add_cancel_request`：`question_type=CONFIRM`、`requester="assistant"`
14. `tools/base.py`：`AgentFunction` 增加 `emit_artifact: bool = True`；`flows.execute_function` 按标记跳过 `emit_function_call`，但**仍发状态事件**（走 #16 的 questions 事件）
15. `a2a/wire.py`：`status_update(..., message: Message | None = None)`；新增入站解析 `parse_question_response(message) -> list[QuestionResponse]`（模型含 `intervention_id`、`answer`；形状非法 → 解析层拒绝）
16. `orchestration/events.py`：
    - `emit_event(..., message=None)`
    - 新增 `build_questions_message(ctx, pending)`、`emit_pending_questions(ctx)`
    - 非终态事件在 pending 存在时状态置 `input_required`（§4.3）
    - 新增 `intervention.rejected` 事件构造（metadata 带 id+reason）
17. 结算即时化（决策 14，核心；对应用户初衷「ask_user 与并行节点同步推流」）：
    - `session.py`：`SessionRuntime` 增加 `settle_tasks: dict[asyncio.Task, str]`、`wake: asyncio.Event`
    - `intervention.settle_input` 拆出 `settle_node_input(ctx, node)`（单节点：已完成 helper 解析 / 决策 / spawn_assist / request_human）
    - `runner.run_plan`：每轮扫描 `input_required_nodes`，未决且无 settle 任务 → 后台 spawn `settle_node_input`；
      wait 集合 = `node_tasks ∪ settle_tasks`；drain 判定必须等 `settle_tasks` 为空；退出/取消时 cancel `settle_tasks`
    - 问题推送调用点（均**即时**，不等并行节点排空）：
      - `intervention.request_human` 创建干预后 `emit_pending_questions`
      - `repair.apply_patch_locked` 创建 confirm_cancel 后 `emit_pending_questions`
      - `node_executor` `expire_cancel_requests` 后 `emit_pending_questions`
      - runner 暂停点再发一次（保证暂停瞬间 `status.message` 为最新聚合）
    - `answer_intervention` resolve 后 `runtime.wake.set()`；executor `_start_runner` 兜底（runner 已退出时）
18. `orchestration/intervention.py`：
    - `answer_intervention(ctx, *, intervention_id, answer) -> bool`：id 定位、守卫（#11）、类型校验（select 须在 options 内；multi 为列表且子集；confirm 为布尔）、渲染 `node.answer_text`（select→「A、B」；confirm→「确认/取消」）、persist、emit；非法 → 拒绝事件
    - 删 `_is_affirmative` / `_AFFIRMATIVE_ANSWERS`
    - `request_human(ctx, node, decision)` 携带 question spec 并即时推送（见 #17）
    - `settle_input` → `settle_node_input`（#17）；同伴自动答复解析同步新字段
    - `answer_intervention` 成功后 `runtime.wake.set()`
19. 问题类型来源（仅本地编排器）：
    - `tools/ask_user.py`：`AskUserArgs`/`AskUserData` 增加 `question_type/options/multi`；`requester=node.agent_name`；`ask_user_func` 置 `emit_artifact=False`；更新过时 docstring（并注明未来由编排层 LLM 直调，见 §八）
    - `subagents/assistance.py` / `tools/outcome_decision.py`：决策模型加 `question_type/options/multi`，更新 `ASSISTANCE_SYSTEM` 提示词
    - `sim/litellm_mock.py`：assistance 决策 mock 同步新字段
    - `[cw:need_info]` 标记、远端 input-required → 默认 `input`
20. `a2a/executor.py`：
    - 解析 message 里的 `question_response` data parts → 逐个调用 `answer_intervention`
    - 有 pending 且无答复 part → 重发问题消息，不消费（含 confirm_cancel 期间，决策 4）
    - 合法答复 → resolve（内部已 `wake`）后 `_start_runner` 兜底（幂等；runner 已退出时启动）
21. `api/replay.py`：final state delta 输出干预新字段与 `answer`

### Phase 4 — 前端

22. `lib/conversationView.ts`
    - 新增 `questions` 状态（按 `intervention_id` 去重）
    - `statusUpdate` 分支：识别任意状态 `status.message` 中 `cw_type:"question"` 的 part → 问题卡片（§4.3 下通常为 `input_required`；当前约 479-508 行会把 `status.message` 当 thinking，需修）
    - task history 里的问题消息同样识别（回放）
    - `state_delta` 的 interventions 更新 pending/resolved/expired/answer
    - 删除 ask_user artifact 分支（artifact 已移除）
    - 修复 artifact 层级回归：`revise_plan` 等读 `funcResult.data.*`（:584 等）
23. 新组件 `components/room/QuestionCard.tsx`
    - 头部：Avatar + @requester + 问题文本
    - input: textarea；select: radio/checkbox（multi）；confirm: 双按钮（confirm_cancel→「打断/保留」）
    - 已答显示所选答案并禁用；过期灰显；rejected 显示错误
24. `components/ChatPanel.tsx`：渲染问题卡片 timeline 项
25. `hooks/useConversation.ts`（硬前置，决策 13）
    - `SETTLED_STATES` **只保留四个终态**；`input_required` 移出（否则 send 结束后不 follow、下一条消息清 taskId → 前端断流）
    - `send` 结束与 replay 载入后：任务非终态即 `follow`（含暂停中的 input_required，保持后端事件可见）
    - `answerQuestion(interventionId, answer, text)`：消息 parts = [text, data]，**直接发到 `view.taskId`**
    - composer 隐藏条件：存在 pending 问题
26. `App.tsx`：传入 onAnswer；`api/client.ts` 删除死方法 `answerIntervention`

## 六、测试

- 后端 unit：
  - UUID 生成、state 序列化（answer 三型、新枚举字段）
  - `answer_intervention` 按 id / 多问题分别答复 / 非法值不消费 / guard（已取消节点不复活）/ persist 顺序
  - 状态优先级（pending 时非终态事件 → input_required）
  - `execute_function` 对 `emit_artifact=False` 不发 artifact 但仍发状态事件
  - 结算即时化：`settle_node_input` 幂等（已有 settle 任务/pending 不重复派发）、runner 退出清理 `settle_tasks`、`wake` 唤醒
- 后端 integration（`tests/integration/test_a2a_hitl.py`）：
  - 断言 input-required 事件的 `status.message`（多 part、data 规格）
  - **即时推送**：问题在其它节点仍在运行时即达（任务状态 input_required，SSE 不中断）
  - **工作期间答复**：resolve 后 READY 节点被 wake 立即调度，不必等其它节点完成
  - select / confirm 答复数据；多问题聚合与集合变化重发
  - confirm_cancel 期间任务仍 input_required 且节点继续
  - 空闲时暂停；迟答复 → `intervention.rejected` 且不触发新计划
  - 答复后续接远端原 task
  - `tests/support/sdk.py` 增加「发送答复 data part」helper
  - 现有文本应答用例（test_a2a_hitl ×2、test_a2a_send.py::test_send_answers_pending_intervention）改走 data part
- 前端：`conversationView.test.ts` 卡片创建/更新/回放去重/rejected；composer 隐藏逻辑；答复消息构造含 data part
  - `useConversation`：`SETTLED_STATES` 仅终态；send/replay 后 input_required 仍 `follow`；答复发往同一 task
- 验证命令（每 Phase 结束跑）：
  - `uv run ruff check --fix src tests && uv run ruff format src tests`
  - `uv run basedpyright`（0 errors；warnings 基线 412 / explicitAny 15 / any 226，不新增）
  - `uv run pytest tests -q -p no:cacheprovider`（基线 210 + 新增）
  - `uv run python -c "import choirworks.main, choirworks.sim.runner, choirworks.api.app"`
  - 前端：`npm test && npm run build && npm run lint`

## 七、明确不做

- 不搬 ADK 的合成 function call / 事件溯源重放机制
- 不做远端问题类型扩展（仅本地编排器定义）
- 不做 JSON Schema `response_schema` 全量校验
- 不保留「直接打字即答复」的 fallback
- 不做 REST 端点（`client.ts` 死方法删除即可）
- `QueuedMessage.id`（`qm{n}`）不改 UUID
- 本次不把 ask_user 接入编排层 LLM（机制已兼容，见 §八）

## 八、未来方向：编排层 LLM 直调 ask_user

ask_user 将成为编排层 LLM 的工具（LLM 直接发起用户询问）。本计划的兼容性设计：

- 工具调用与内部调用汇聚同一出口 `execute_function` → 状态变更、questions 消息、delta 三个动作一处生效，LLM 接入时零改动；
- `AskUserArgs` 已带 `question_type/options/multi`，LLM 可发起结构化提问；
- `emit_artifact=False` 对 LLM 调用同样生效；
- **届时再做**：`AskUserArgs.node_id` 目前是编排内部概念，LLM 直调时不应由模型填写——接入时改为可选/由绑定层注入（现在不做，用不到不提前实现）。

## 九、已核实事实附录

| 事实 | 出处 |
|---|---|
| 应用使用 V2 handler（`DefaultRequestHandler = DefaultRequestHandlerV2`） | a2a-sdk `server/request_handlers/__init__.py` |
| V2 仅四个终态关流；input_required 属 INTERRUPTED，仅非流式提前返回 | a2a-sdk `server/agent_execution/active_task.py:83-92,282-315`；`server/request_handlers/default_request_handler_v2.py:265-273` |
| 订阅在 `_RequestCompleted`（execute 返回）或终态/清理时结束，input_required 不中断 | a2a-sdk `active_task.py:608-700` |
| SDK 把 `status.message` 移入 `Task.history` | a2a-sdk `server/tasks/task_manager.py:208,332` |
| `start_runner` 幂等 | `orchestration/runner.py:30-33` |
| `runner_start_requested` 唯一读者 | `a2a/executor.py:167`（写：assist.py:72、routing.py:160、intervention.py:170/188） |
| 前端 artifact 层级 bug | `conversationView.ts:623`（ask_user）、`:584`（revise_plan）；线格式 `FunctionResult{success, data, error}`（tools/base.py:13-22） |
| 前端 settled 断流点 | `frontend/src/hooks/useConversation.ts:19-25` |
| 结算只在排空后 | `orchestration/runner.py:86-135` |
| `_is_affirmative` 关键词集合 | `orchestration/intervention.py:31-35` |
| ADK 严格模式先例：input_required 时无 function_response → 拒绝 | adk `a2a/converters/long_running_functions.py:159-204` |
| ADK 请求按 id 关联 + resume 校验 | adk `agents/context.py:687-717`、`flows/llm_flows/request_confirmation.py:75-289` |

## 十、修订记录

- 2026-09-24 实施完成（Phase 1-4）。落地偏差与补充：
  1. `execute_function` 对 `emit_artifact=False` **不发任何事件**（调用方负责状态事件），而非"仍发状态事件"——
     否则会先发一条无问题的 input_required 事件，与聚合问题消息竞态。
  2. `request_human` 先 `persist` 再 `emit_pending_questions`（先落盘再推送）；confirm_cancel 创建与过期同样即时 emit。
  3. `Database.transaction()` 改为 `BEGIN IMMEDIATE`：结算后台任务与节点任务并发 persist 时，
     延迟 BEGIN 升级写会触发 SQLITE_BUSY_SNAPSHOT（database is locked）。
  4. 前端问题 id 从 `status.message` 的 question data part 解析（`question_ids_from_status`），不依赖 state JSON 快照时序。
  5. 测试新增：即时推送（并行节点仍在跑）、多问题聚合/分别作答、数据 part 解析、
     `answer_intervention` 类型校验与拒绝、`SETTLED_STATES` 仅终态。
- 2026-09-23 最终修订：并入 a2a-sdk V2 流语义更正（input_required 不断流，断点在前端 `SETTLED_STATES`；决策 13/§4.4），
  新增结算即时化（决策 14、Phase 3 #17：后台 `settle_tasks` + `wake`，ask_user 与并行节点同步推流），
  缺陷新增 #11，测试补即时推送/工作期间答复/SETTLED_STATES 用例。
  撤回早期基于 `LegacyRequestHandler` 的「input_required 关流丢事件/必须最后 emit」结论。
