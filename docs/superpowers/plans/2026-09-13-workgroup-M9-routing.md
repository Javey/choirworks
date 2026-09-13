# 工作群协作 M9：路由与治理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> 前置：M8 已完成（`docs/superpowers/plans/2026-09-13-workgroup-M8-message-base.md`），当前 144 后端测试全绿。

**Goal:** 让工作群真正「像群」：人类 @ 即派活、引用回复可续接/排队/打断、Agent 互相 @ 由协调者仲裁（复用/扩展/合并/拒绝）、必要时新成员自动入群、人类干预在群里提问与作答、assistant 全程播报。

**Architecture:** 新增 `core/coordinator.py`（RoomCoordinator）作为路由与治理大脑；`core/room.py` 增加 assistant 播报助手；Orchestrator 在计划创建/派发/完成/干预/产出五个时机回调 coordinator；消息数据层与上下文投喂复用 M8。

**Tech Stack:** 同 M8；测试继续使用 `tests/support/fakes.py::FakeLLM`、`tests/sim/fake_agent` 行为脚本。

**权威 spec:** `docs/superpowers/specs/2026-09-13-group-chat-collaboration-design.md` §6、§7、§11.2

---

## 关键接口（锁定签名）

```python
# core/coordinator.py
class HumanMessageResult(BaseModel):
    message: RoomMessage
    task_id: str | None = None
    routed: str  # new_task | direct_agent | follow_up | queued | interrupted | intervention_answer

class RoomCoordinator:
    def __init__(self, db, events, task_service, orchestrator, registry, llm=None): ...
    async def handle_human_message(self, conversation_id: str, *, text: str,
        mentions: list[str], quote_id: str | None, interrupt: bool) -> HumanMessageResult: ...
    async def announce_plan(self, task, draft) -> RoomMessage | None: ...
    async def announce_dispatch(self, task, node) -> RoomMessage | None: ...
    async def announce_completion(self, task, nodes) -> RoomMessage | None: ...
    async def announce_join(self, conversation_id: str, agent_name: str, agent_url: str, reason: str) -> None: ...
    async def join_new_members(self, conversation_id: str, names: list[str]) -> None: ...
    async def arbitrate_message(self, task, message: RoomMessage) -> int: ...
    async def deliver_queued_for_terminal(self, task, nodes) -> int: ...
    async def mark_delivered_for_node(self, node_id: str) -> int: ...
    async def reconcile(self) -> int: ...

# core/room.py 追加
async def post_assistant_message(db, events, *, conversation_id, text, task_id=None,
    node_id=None, intervention_id=None) -> RoomMessage: ...
def extract_mentions(text: str, known_names: set[str]) -> list[str]: ...

# projections 追加
async def fetch_queued_messages(db, node_id: str) -> list[RoomMessage]: ...       # undelivered
async def fetch_undelivered_queued_messages(db) -> list[RoomMessage]: ...          # 全局，reconcile 用
async def fetch_assistant_message_for_node(db, node_id: str) -> RoomMessage | None: ...

# TaskService 变更
async def create_task(..., start: bool = False)  # 保持现状，由调用方 start
# Planner.plan(..., required_agents: list[str] | None = None)  # 提示词约束
# Orchestrator.answer_intervention(intervention_id, text, responder="user", quote_id=None)
```

---

### Task 1: coordinator 骨架 + 人类消息路由（无引用路径）+ 入群

**Files:**
- Create: `src/choirworks/core/coordinator.py`
- Modify: `src/choirworks/core/room.py`（`extract_mentions`、`post_assistant_message`）
- Modify: `src/choirworks/core/planner.py`（`required_agents` 提示）
- Modify: `src/choirworks/api/messages.py`（委托 coordinator，允许 mentions）
- Test: `tests/integration/test_coordinator_routing.py`

实现要点：
1. `join_new_members`：注册表中存在且不在 `room_members` 的名字 → `ROOM_PARTICIPANT_JOINED`（reason=`human_mention`/`agent_mention`）+ assistant 消息「已将 @X 加入群聊」。
2. 无 quote：
   - `len(mentions) == 1` → `task_service.create_task(text, TargetSpec(agent_name=mentions[0]), conversation_id)`，消息带 task_id，`orchestrator.start(task_id)`，routed=`direct_agent`。
   - 其他 → `create_pending_task` + 消息 + `orchestrator.start`；多 mention 时把 names 传入 `required_agents`（在 create_pending 流程中不可用；改为在 handler 中记录 policy 并写 TASK_CREATED 时带上——**简化**：多 mention 时仍走 pending，mention 仅用于入群与播报，`required_agents` 留待后续）。
3. 每条用户消息都写 `message.posted`（role=user, sender=CEO）。
4. 摘要触发：`if self._llm: await maybe_update_summary(...)`，异常吞掉记 error。
5. API `create_message` 改为调用 coordinator；mentions 仍校验注册表。

测试（FakeLLM + echo/ask agent）：
- 单 mention 直接建 task，消息与 task_id 关联，echo 回复入时间线。
- 多 mention 自动入群 + assistant 入群播报。
- 未注册 mention 400。

### Task 2: assistant 播报（拆解/派发/完成/干预提问）

**Files:**
- Modify: `src/choirworks/core/orchestrator.py`
- Modify: `src/choirworks/core/coordinator.py`
- Test: `tests/integration/test_assistant_announcements.py`

实现要点：
- `_initial_plan` 成功后 `await self._coordinator.announce_plan(task, drafted)`（无 coordinator 时跳过，兼容既有测试构造）。播报文本：`任务已拆解：\n- @researcher 负责 调研\n- @writer 负责 写作`。
- run 循环派发 ready 节点前 `announce_dispatch(task, node)`：每个节点首次派发一条（`fetch_assistant_message_for_node` 去重），文本 `已派发 @X：{node.name}`。
- 全部完成后（`finalize_if_complete` 之前）`announce_completion(task, nodes)`：`任务完成：\n- researcher：<输出前80字>\n...`。
- `_create_intervention` 中 policy==human 时：assistant 消息带 `intervention_id` 与问题原文。
- `answer_intervention` 增加 `quote_id` 可选：resolve 后写用户消息（`sender=CEO`, `intervention_id`, `quote_id`）。
- Orchestrator `__init__` 增加可选 `coordinator=None`；`app.py` 构造并注入（coordinator 在 orchestrator 之后创建？循环依赖 → coordinator 持 orchestrator 引用，orchestrator 持 coordinator 引用：先建 orchestrator（coordinator=None），再建 coordinator，再 `orchestrator.set_coordinator(coordinator)`）。

### Task 3: Agent mention 仲裁（复用/扩展/合并/拒绝）

**Files:**
- Modify: `src/choirworks/core/coordinator.py`
- Modify: `src/choirworks/core/orchestrator.py`（run 循环：`post_agent_messages` 返回新消息 → `arbitrate_message`）
- Test: `tests/integration/test_mention_arbitration.py`

实现要点：
- `extract_mentions(text, known)`：正则 `@([A-Za-z0-9_\-]+)`，仅保留 known，保序去重。
- `post_agent_messages` 返回 `list[RoomMessage]`（新消息）。
- 仲裁规则（逐 mention）：
  1. 自引用/未知 → 忽略。
  2. 单条消息 >3 mentions → 只取前 3，assistant 提示已截断。
  3. 派生节点深度 ≤5（task 内 `derived` 节点计数，超限 assistant 拒绝播报）。
  4. 若目标存在**非终态**派生协助节点 → 把触发消息文本作为排队消息挂到该节点（`queued_for_node_id`），assistant 播报「已并入 @X 现有工作」。
  5. 否则 → `extend_plan` 新增 standalone 派生节点（`id=a{uuid8}`, `name=协助 · {target}`, `input={"text": f"来自 @{sender} 的请求：{source_text}"}`，无 edges），自动入群 + assistant 播报「@A 请求 @B 协助，已加入工作」。调度器下一轮自动派发。
- 幂等：只在 `post_agent_messages` 新产生消息时仲裁。

### Task 4: 引用回复路由（续接/排队/打断/干预作答）

**Files:**
- Modify: `src/choirworks/core/coordinator.py`
- Modify: `src/choirworks/api/messages.py`（去除 quote/interrupt 的 400）
- Test: `tests/integration/test_quote_routing.py`

实现要点（`quote` 消息查 `projections.fetch_message`）：
- 404 引用不存在。
- `quote.intervention_id` 且干预 PENDING → `answer_intervention(..., responder="CEO", quote_id=quote.id)`，routed=`intervention_answer`。
- `quote.node_id`：
  - 节点非终态且 `interrupt=False` → 排队：消息 `queued_for_node_id=node.id`，assistant 播报投递时机，routed=`queued`。
  - 节点非终态且 `interrupt=True` → `orchestrator.stop_task(task_id)`（取消在途）后创建 follow-up 任务（单节点给 `quote.sender`），routed=`interrupted`。
  - 节点终态（COMPLETED/FAILED/CANCELED/INVALIDATED）→ follow-up 任务（`TargetSpec(agent_name=quote.sender)`，input 文本含引用原文摘要），routed=`follow_up`。
- `quote.sender == "CEO"` 或非 agent → 视为普通新任务。

### Task 5: 排队投递与启动对账

**Files:**
- Modify: `src/choirworks/core/coordinator.py`
- Modify: `src/choirworks/core/orchestrator.py`（run 循环顶部）
- Modify: `src/choirworks/store/projections.py`
- Modify: `src/choirworks/api/app.py`（lifespan 里 `await coordinator.reconcile()`）
- Test: `tests/integration/test_queued_delivery.py`

实现要点：
- `mark_delivered_for_node(node_id)`：所有未投递排队消息写 `MESSAGE_DELIVERED`（幂等）。
- run 循环取得 nodes 后：对每个 `COMPLETED` 节点，若存在未投递排队消息 → follow-up 新任务（合并文本），标记投递，assistant 播报「已转交 @X 继续处理」。
- ready 派发前：`mark_delivered_for_node(node.id)`（该节点上下文已包含排队消息）。
- `reconcile()`：`fetch_undelivered_queued_messages` → 节点已终态则 follow-up + 标记投递；非终态跳过。启动时恢复窗口由 recovery 保证在 reconcile 前完成。

### Task 6: 集成剧本 + README 回填

**Files:**
- Test: `tests/integration/test_room_scenario.py`
- Modify: `README.md`（工作群查看方式：`GET/POST /v1/conversations/{id}/messages`）

剧本（sim，SimLLM + 6 agents）：
1. `POST /v1/tasks` 建会话 → `POST messages` @researcher 发「请协调多个子代理协作完成这项分析」。
2. 等待 completed；断言时间线包含：用户消息、assistant 拆解播报、agent 消息、协助相关播报。
3. `GET messages?since_seq=` 游标分页一致；重启 `rebuild()` 后时间线不变。
4. 引用一条 agent 消息追问 → follow-up task 完成。
5. 对在途任务发消息 → 排队标记；任务完成后转 follow-up。

---

## 验收

```bash
uv run pytest -p no:warnings -q          # 旧 144 + 新增
uv run ruff check .
cd frontend && npm test && npm run build # 前端不改，保持 25
```

实机：`uv run choirworks-sim --fresh` → 浏览器/curl 走完剧本 1–5。

## Self-Review 备忘

- 循环依赖用 `set_coordinator` 注入解决；无 coordinator 时所有播报静默跳过，存量测试不受影响。
- 派生节点无 edges（standalone）时调度器直接派发；peer assist（input-required）路径完全复用现有 `_create_intervention`。
- 打断 `stop_task` 会取消该任务全部在途节点（v1 语义），follow-up 以新任务承载。
- `required_agents` 提示词约束未纳入 v1（多 mention 仅入群+播报）；如需强约束在 M9 收尾追加。
