# Agent Hub 工作群协作模式设计（v1）

- 日期：2026-09-13
- 状态：已评审通过（待实现）
- 关联：`2026-09-12-a2a-orchestration-platform-design.md`（平台设计）、`2026-09-13-frontend-chat-design.md`（对话前端）、`2026-09-13-peer-assist-dynamic-dag-design.md`（动态 DAG 协助）

## 1. 背景与目标

模拟人类工作方式：人类作为 CEO 发起任务，协调者（assistant）拆解任务并把相关 Agent 拉进一个「工作群」；群内 Agent 可以互相 @，人类可以引用回复任意 Agent；协作过程中可能需要额外 Agent 入群。群聊是人类体验层，底层仍复用现有确定性编排内核（DAG、节点、干预、重试、恢复、回退）。

目标：

1. **群聊即前门**：人类只面对一个持续存在的群；所有消息经 hub 记录，不存在的旁路通道。
2. **共享上下文**：每个被唤醒的 Agent 收到按相关性分级裁剪的群上下文，保证「够用且可扩展」。
3. **类人协作**：人类 @ 即派活；Agent @ 由协调者仲裁；引用回复可续接；中途插话平滑排队，紧急可显式打断。
4. **动态组队**：需要新成员时自动入群并播报，全程可审计。
5. **零回归**：既有 task/plan/node/干预/回退机制与 128 个后端测试、25 个前端测试保持通过。

非目标（v1）：

- 私聊/DM、消息编辑删除、表情回复、附件上传、语音。
- 多人类同群协作与用户系统（沿用无鉴权现状）。
- Agent 自治监听循环（不采用 B 方案；Agent 只在被唤醒时获得上下文）。
- 房间级入群审批策略（预留 policy 字段，后置实现）。
- 移动端专项优化。

## 2. 定稿决策

| # | 决策 | 说明 |
|---|---|---|
| D1 | 群聊作编排前门 | hub 是唯一消息总线与事实源；DAG/节点是执行内核，零改动复用 |
| D2 | 上下文分级投喂 | 房间头＋摘要＋与我相关＋最近窗口＋我的历史，按预算裁剪；小房间自动退化为全量 |
| D3 | 人类 @ 即派发 | 视为已批准指令，经同一调度器与治理层（去重/复用/排队/限流/防循环） |
| D4 | Agent @ 由协调者仲裁 | 可复用已完成协作（`_reusable_helper`）、`plan.extended` 新建、合并或拒绝 |
| D5 | 群＝conversation 容器 | 人类发言默认创建新 task；引用回复路由到被引用消息所属的 Agent/工作链（以 follow-up 任务承载，见 D9） |
| D6 | 中途插话默认排队补投 | 当前工作结束/下次 continue 时随上下文送达；UI 提供显式「打断」 |
| D7 | 协调者是可⻅成员 assistant | hub 的界面人格，内部为确定性逻辑，不是远程 A2A 进程 |
| D8 | 新 Agent 自动入群＋播报 | 沿用 `plan.extended` 授权范围，事后可审计、可打断 |
| D9 | 续接语义不依赖远程终态任务 | 对已完成的 Agent 引用回复＝同一 task 内新建 follow-up 节点，A2A 层发新消息并携带上下文包与原引用；不复用终态 remote task_id（跨实现语义不一致） |
| D10 | 群日志 append-only | 回退不回删消息；作废由 assistant 系统消息与被回退节点状态表达 |

## 3. 总体架构

```
CEO ──▶ POST /v1/conversations/{id}/messages
          │  1. 记录 message.posted（事件日志 = 事实源）
          ▼
   RoomCoordinator（core/room.py，hub 内）
     │  路由：新 task ／ 续接 follow-up ／ 干预作答 ／ 排队 ／ 打断
     │  仲裁：人类@ 直接派发；Agent@ 复用/扩展/合并/拒绝
     │  组队：room.participant_joined + assistant 播报
     │  摘要：room.summary_updated（增量）
     ▼
   既有 Orchestrator / Planner / Scheduler / Dispatcher / Interventions / Rollback
     │  A2A 下发 = 本次指令 + ContextBuilder 渲染的分级上下文包
     ▼
   Agent 产出 ──▶ NODE_ARTIFACT（流式）/ NODE_OUTPUT
     │                        │
     │                        ▼
     │              message.posted（role=agent，含 @mentions 解析）
     ▼
   前端房间 SSE ──▶ 群聊气泡 / 成员状态 / 右侧计划面板
```

组件边界：

- `core/room.py`：RoomCoordinator。消息接收与校验、候选动作决策、assistant 播报，不直接执行 A2A；通过调用既有 `TaskService`/`Orchestrator`/`NodeDispatcher` 完成执行。
- `core/context.py`：ContextBuilder。纯函数式渲染，输入房间数据与指令，输出渲染文本与已包含消息 id 列表（可审计/可测试）。
- `core/summary.py`：SummaryBuilder。增量摘要，事件化存储，sim 下确定性模板。
- 既有 `Orchestrator`：节点完成、失败、干预解析处增加对 coordinator 的显式回调（`post_agent_message`、`arbitrate_mentions`），不做通用事件总线。

## 4. 数据模型与事件

### 4.1 新增表（`store/db.py`，沿用 `_migrate` 模式）

```sql
CREATE TABLE IF NOT EXISTS messages (
  id              TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  seq             INTEGER NOT NULL,           -- 房间内单调递增，从 1 开始
  role            TEXT NOT NULL,              -- user | assistant | agent | system
  sender          TEXT,                       -- 人类为 'CEO'，assistant 为 'assistant'，agent 为 agent_name
  text            TEXT NOT NULL,
  mentions        TEXT NOT NULL DEFAULT '[]', -- JSON 数组，agent_name
  quote_id        TEXT,                       -- 被引用消息 id
  task_id         TEXT,
  node_id         TEXT,
  intervention_id TEXT,
  queued_for_node_id TEXT,                    -- 排队补投目标节点
  delivered_at    TEXT,
  created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_room_seq ON messages(conversation_id, seq);

CREATE TABLE IF NOT EXISTS room_members (
  conversation_id TEXT NOT NULL,
  agent_name      TEXT NOT NULL,
  agent_url       TEXT NOT NULL,
  reason          TEXT,
  joined_at       TEXT NOT NULL,
  PRIMARY KEY (conversation_id, agent_name)
);

CREATE TABLE IF NOT EXISTS room_summaries (
  conversation_id TEXT PRIMARY KEY,
  covers_seq      INTEGER NOT NULL,
  summary         TEXT NOT NULL,              -- JSON：结构化摘要
  updated_at      TEXT NOT NULL
);
```

### 4.2 事件存储扩展（`store/db.py`、`store/event_store.py`）

群消息事件是会话级聚合、可能不属于任何 task，而现有 `events.task_id` 为 NOT NULL。扩展：

- `events` 增加 `conversation_id TEXT` 列；`task_id` 放开为可空（`_migrate` 中按 SQLite 标准流程建新表→复制→改名，保留原 `seq`）。
- `EventStore.append(task_id: str | None, event_type, payload, *, conversation_id: str | None = None)`；`Event` 模型增加 `conversation_id`。
- 既有任务事件查询 `WHERE task_id = ?` 不受影响；房间 SSE 与投影按 `conversation_id` 查询。
- `rebuild()` 仍按全局 `seq` 遍历全部事件，房间事件从 `conversation_id` 取聚合。

### 4.3 新增事件（`models/enums.py::EventType`）

| 事件 | payload | 投影动作 |
|---|---|---|
| `message.posted` | `message_id, conversation_id, seq, role, sender, text, mentions, quote_id, task_id, node_id, intervention_id, created_at` | 插入 `messages` |
| `message.delivered` | `message_id, node_id, task_id, delivered_at` | 更新 `delivered_at` |
| `room.participant_joined` | `conversation_id, agent_name, agent_url, reason, joined_at` | upsert `room_members` |
| `room.summary_updated` | `conversation_id, covers_seq, summary, updated_at` | upsert `room_summaries` |

- `seq` 在事件 payload 中固化（`当前 max(seq)+1` 于写入前计算），保证 `rebuild()` 后房间游标稳定。
- 群聊事件与任务事件写同一事件日志，全局 `events.seq` 保证顺序。
- `rebuild()` 的 DELETE 清单加入 `messages`、`room_members`、`room_summaries`。

### 4.4 投影查询（`store/projections.py`）

- `fetch_messages(db, conversation_id, since_seq, limit)`、`fetch_room_members`、`fetch_room_summary`。
- `fetch_message(db, message_id)`、`fetch_queued_messages(db, node_id)`。
- `fetch_agent_messages_for_node(db, node_id)`（用于把流式输出映射到消息）。

## 5. 上下文投喂（分级策略）

### 5.1 层次与优先级

| 优先级 | 层 | 内容 | 来源 |
|---|---|---|---|
| 1 | 当前指令 | 本次工作项指令原文 | node.input |
| 2 | 房间头 | 群名、总目标（最近人类消息或摘要目标）、成员列表、你的身份、协作规则 | conversation/members |
| 3 | 与我相关 | @我、引用我/回复我、我参与过的链、我依赖节点的产出 | mentions/quote/edges |
| 4 | 任务摘要 | 结构化长期记忆 | room_summaries |
| 5 | 最近窗口 | 最近 K 条完整消息（默认 K=20） | messages |
| 6 | 我的历史 | 我在本群的发言与产出 | messages by sender |

### 5.2 渲染与预算

- `build_agent_context(db, conversation_id, agent_name, instruction, budget=8000) -> ContextPackage`
  - `text`：渲染文本（`[角色] #seq 内容`；引用以 `（引用 #seq [sender]…）` 内联）。
  - `included_message_ids`：已包含消息 id（测试与审计用）。
  - `truncated`：是否发生裁剪。
- 预算按 `len(text) // 4` 估算 token（确定性，测试可预期）；逐层贪心装入，超预算从低优先级层整条丢弃并在末尾标注「更早消息已省略」。
- 最近窗口按 seq 倒序取满即止；与我相关层单独扫描全部消息（上界为房间消息总数，超 500 条时只扫描最近 500 条，更早依赖摘要）。
- 房间消息总数 ≤ K 且总长度 ≤ 预算时，输出等同「全量投喂」，无需摘要。

### 5.3 摘要

- 触发：距上次 `covers_seq` 新增消息 ≥ 12 条，或渲染时预算不足且存在可压缩历史。
- 增量：`新摘要 = LLM(旧摘要 + covers_seq 之后的新消息)`，结构化字段：`{goal, decisions[], artifacts[{message_id|node_id, note}], todos[], open_questions[]}`。
- 写入 `room.summary_updated`（`covers_seq` = 被覆盖的最后一条消息 seq）。
- `core/llm.py` 增加 `summarize(previous, messages) -> dict`；sim 的 ScriptedLLM 返回确定性模板（拼接关键行），保证测试稳定。
- 摘要失败不阻塞消息流：记 `error` 事件并保留旧摘要，下一条消息触发重试。

### 5.4 调度器接入

- `NodeDispatcher` 在下发与 continue 时调用 ContextBuilder；渲染文本作为 A2A 消息正文。
- 兼容：`node.input["text"]` 仍保留指令原文；无 `conversation_id` 的任务沿用旧行为（只发指令），存量测试不受影响。
- 上下文包渲染时间戳不写入事件，避免非确定性；`node.dispatch.intent` payload 中记录 `context_included`（消息 id 列表）用于审计。

## 6. 路由与协调语义

### 6.1 人类消息处理（RoomCoordinator.handle_human_message）

输入：`{text, mentions[], quote_id?}`。步骤：

1. 校验会话存在、mentions 均为已注册 Agent（未注册 → 拒绝，400）。
2. 分配 seq，写 `message.posted`。
3. 解析 Mention：
   - 成员未在群内 → 自动入群（`room.participant_joined`，reason=`human_mention`）+ assistant 系统消息「已将 @X 加入群聊」。
4. 按 quote 路由：
   - **引用 pending 干预** → 写 `intervention.resolved`（`responder=CEO`，answer=消息文本），不创建 task。
   - **引用已完成节点** → 创建新 task（D5），计划为单节点 follow-up：agent=被引用消息的 sender，指令=消息文本，上下文包含引用消息与其所在节点产出；task 的 `conversation_id` 关联本群。
   - **引用在途节点** → 标记 `queued_for_node_id`（D6），assistant 播报「已排队，将在 @X 当前工作结束后投递」。
   - **显式打断**（请求带 `interrupt: true`）→ 记 message.posted 后取消该节点（既有 cancel 信号），失败/取消后按 follow-up 新 task 处理。
   - 无引用 → 创建新 task；若含 mentions，作为 planner 的「指定执行者」提示（`policy.required_agents`），否则 planner 自由拆解。
5. 创建 task 后走既有 `TaskService.create_task` 流程；assistant 在 `plan.created` 投影后发消息：「任务已拆解：@a 负责…，@b 负责…」，并逐节点在有向边就绪时播报「已派发 @X：…」。

### 6.2 Agent 消息与 Mention 仲裁

- 节点 `NODE_OUTPUT` 落库后，Orchestrator 回调 `coordinator.post_agent_message(node, output)`：
  - 文本 = 节点 output artifacts 合并文本（空则「已完成」）。
  - `mentions` = 从文本解析的 @name（词边界匹配，仅识别群成员名/已注册 Agent 名，避免邮箱等误报）。
  - 写 `message.posted`（role=agent，sender=agent_name，node_id）。
- 仲裁（`coordinator.arbitrate_mentions(message)`），逐条 mention 决策：
  1. **自引用或未知** → 忽略，不播报。
  2. **目标有同父任务在途/排队** → 合并：写入排队补投，assistant 播报「已并入 @X 的现有工作」。
  3. **存在同父已完成协助节点** → 复用（既有 `_reusable_helper`），播报「复用了 @X 的既有产出」。
  4. **需要新工作** → `plan.extended`（既有 peer assist 流程），播报「@X 请求 @Y 协助，已加入工作」。
  5. **目标策略为 human** → 走干预（既有）。
- 防循环护栏（v1 固定值，可后续配置）：
  - 单条消息最多 3 个 mentions，超出忽略并播报系统提示。
  - 同一 task 的扩展协助深度 ≤ 5（沿 `derived` 链计数），超限拒绝并播报「协作深度超限」。
  - 同一对 `(parent_node, agent)` 只允许一个派生节点（复用规则已保证）。
- 仲裁产生的所有关键动作以 assistant 消息留痕；拒绝动作写系统消息，不静默。

### 6.3 排队补投的投递时机

- 节点 `input_required` 且后续 continue 时：排队消息文本合并进 continue 正文（追加「（来自 CEO 的补充）#seq …」），写 `message.delivered`。
- 节点直接 `completed`：排队消息转为该 agent 的 follow-up 新 task，写 `message.delivered`（assistant 播报转交）。
- 节点 `failed/canceled`：排队消息随重试后 continue 投递；若任务终态，转为 follow-up 新 task。
- 规则集中在 `RoomCoordinator.on_node_terminal(node)`，由 Orchestrator 在节点终态处理末尾调用；崩溃恢复时，recovery 完成后对仍存在的 `queued_for_node_id` 且节点已终态的消息执行同规则（幂等：`delivered_at` 已置则跳过）。

### 6.4 assistant 播报消息

- 均以 `role=assistant` 写 `message.posted`，不经过 A2A。
- 类型：拆解摘要、派发通知、入群通知、仲裁结果、排队/打断提示、任务完成总结（节点全部完成后汇总各产物）。
- 任务完成总结由既有 `finalize_if_complete` 时机触发，文本为各 completed 节点产出的结构化列表。

## 7. 干预在群里的呈现

- `intervention.requested`（policy=human）时，assistant 写一条消息：`intervention_id` 关联，文本为问题原文，前端渲染为内联干预卡。
- 人类回答方式：直接引用该消息回复（6.1 路由）或在卡片输入框提交（复用既有 `answer_intervention` API）。
- `intervention.resolved/failed` 更新卡片状态；`plan.superseded` 失效时卡片显示「已失效」。
- peer_agent 干预不对人类发消息（由仲裁流程处理），仅记事件。

## 8. API 与 SSE

### 8.1 REST

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/conversations/{id}/messages?since_seq=&limit=` | `{messages, members, summary, last_seq}`，seq 升序 |
| POST | `/v1/conversations/{id}/messages` | body `{text, mentions?, quote_id?, interrupt?}`；返回 `{message_id, task_id?}` |
| GET | `/v1/conversations/{id}` | 既有接口，扩展返回 `members`、`last_seq` |

- 不支持客户端伪造 `role=assistant/agent/system`（服务端固定 `user`）。
- 旧的 `POST /v1/tasks` 保持可用（等价于不带 quote 的消息 + 新会话场景）。

### 8.2 SSE

- 新增 `GET /v1/conversations/{id}/stream?since_seq=`：复用现有事件存储，过滤出该会话相关事件（`message.*`、`room.*`、以及关联 task 的 `plan.*`/`node.*`/`intervention.*`）。
- 事件 id 使用全局 `events.seq`；`since_seq` 回放后转实时。
- 任务级 SSE `/v1/tasks/{id}/stream` 保留，供右侧详情面板订阅（复用现有前端 `useConversation` 逻辑时按需切换）。
- 心跳与断线重连沿用现有实现。

## 9. 前端设计

### 9.1 布局

```
┌ 侧栏（会话列表） ┬ 群聊主区 ────────────────┬ 右侧面板（可折叠）┐
│                  │ 头部：群名+成员状态chips   │ 当前任务计划卡    │
│                  │ 消息流（气泡/干预卡/系统）│ 节点卡列表        │
│                  │ 输入区：@补全/引用/发送    │ 回退/取消按钮     │
└──────────────────┴───────────────────────────┴──────────────────┘
```

### 9.2 消息渲染

- `UserBubble`：CEO 消息；`AgentBubble`：头像色块＋名字＋文本＋流式光标；`AssistantBubble`：协调者消息（含拆解、派发、总结）；`SystemNote`：入群/拒绝/失效等灰条。
- 引用块：被引用消息的发送者与摘要文本，点击跳转。
- @高亮：成员名渲染为高亮 chip。
- 干预卡：内联在 assistant 消息下，pending 时可输入提交（复用 `InterventionCard`）。
- 流式：节点 `node.artifact` 事件到达时，在该节点的 transient 气泡内追加文本；`node.output` 后由服务端 `message.posted` 替换为持久消息。
- 排队消息在自己气泡上加「排队中」角标；被投递后角标消失（`message.delivered`）。

### 9.3 输入区

- @ 自动补全：输入 `@` 弹出成员/已注册 Agent 列表，键盘可选中。
- 引用：消息 hover 出现「引用」按钮，进入引用态后输入框上方显示引用条。
- 打断：在途节点消息气泡旁「打断」按钮（二次确认），发送时带 `interrupt: true`。
- 发送后立即本地插入用户气泡（乐观），以服务端 `message.posted` 为准去重。

### 9.4 状态管理

- 新 `lib/roomView.ts`：房间状态折叠器（messages、members、summary、last_seq、queued 标记），输入 REST 快照与房间 SSE 事件；纯函数，Vitest 单测。
- 右侧面板继续复用 `lib/taskView.ts` 与 `NodeCard`；进入群聊模式后默认折叠，点开显示当前/最近 task。

## 10. 失败、恢复与回退

- 群日志 append-only；`rollback` 不回删消息。回退后 assistant 写系统消息「任务 X 已回退至 checkpoint，之后产物作废」，被回退节点相关气泡由 `taskView` 状态标记为失效。
- 排队消息持久化在 `messages.delivered_at IS NULL`；重启后 recovery 完成后由 `RoomCoordinator.reconcile_pending_deliveries()` 按 6.3 规则幂等处理。
- 摘要失败：记录 error 事件，保留旧摘要，不阻塞。
- 入群/mention 仲裁的拒绝路径都有系统消息，不产生静默丢弃。

## 11. 测试与验收

### 11.1 单元测试

- 投影与迁移：`message.posted/delivered`、`room.participant_joined`、`room.summary_updated`；`rebuild()` 后房间 seq 与成员一致；旧库迁移新列。
- `ContextBuilder`：层次优先级、预算裁剪、@我必含、小房间全量、`included_message_ids` 正确。
- `SummaryBuilder`：12 条触发、增量 covers_seq、sim 确定性输出、失败保留旧摘要。
- Mention 解析：词边界、非成员忽略、邮箱不误报；护栏：>3 拒绝、深度 >5 拒绝。
- 排队规则：input_required→continue 合并投递；completed→转 follow-up；failed→重试后投递；终态→转 follow-up；幂等。
- API：消息创建校验、quote/interrupt 路由、未知 mention 400。

### 11.2 集成测试（sim）

剧本（`tests/integration/test_room_chat.py`）：CEO 发「请协调多个子代理协作完成这项分析」并 @researcher：

1. assistant 拆解消息出现（含 member 列表）；researcher/writer 节点派发、流式消息落库。
2. writer 暂停 → 仲裁路由 researcher 协助 → 入群/扩展播报 → 协助节点完成 → writer 续跑。
3. Agent 产出含 @analyst 的需求 → 仲裁创建协助节点（analyst 自动入群并有入群播报）。
4. CEO 引用 writer 消息追问 → 生成 follow-up task 并完成。
5. CEO 对在途节点发言 → 消息排队，节点 continue 时投递（断言 `message.delivered`）。
6. CEO 打断在途节点 → 节点取消 → follow-up 执行。
7. 群消息 API 返回完整时间线；重启后 `rebuild()` 一致；旧测试全绿。

### 11.3 前端测试

- `roomView` 折叠：消息、成员、摘要、排队角标、引用块。
- 组件：AgentBubble 流式、@高亮、引用条、@补全键盘选择、打断二次确认。
- 既有 25 个前端测试保持通过。

### 11.4 验收命令

- `uv run pytest -p no:warnings -q`（含新增房间测试）
- `uv run ruff check .`
- `cd frontend && npm test && npm run build`
- 实机：`uv run agent-hub-sim --fresh` 后在浏览器完成剧本 1–6。

## 12. 里程碑

### M8 群消息底座（旧流程不动）

- 表/事件/迁移/投影/查询；`GET/POST messages`、房间 SSE；`ContextBuilder` + 单测；`SummaryBuilder` 骨架 + 单测。
- 调度器接入上下文包（无 `conversation_id` 时旧行为）。
- 验收：房间消息可发可看可流式；上下文分层单测通过；既有测试全绿。

### M9 路由与治理

- `RoomCoordinator`：人类消息路由（新 task/干预作答/follow-up/排队/打断）、mention 仲裁、入群、assistant 播报、终态队列 reconcile。
- Orchestrator 回调接入；sim 剧本与集成测试；摘要生效。
- 验收：11.2 集成剧本通过。

### M10 前端群聊

- `roomView` + 群聊组件、@补全、引用、排队角标、打断、成员状态、右侧面板联动。
- 前端测试、README「工作群」章节、设计文档回填实现说明。
- 验收：11.3/11.4 通过，实机剧本可复现。

## 13. 风险与对策

| 风险 | 对策 |
|---|---|
| 上下文爆炸/成本 | 分级投喂＋增量摘要＋预算裁剪；小房间全量 |
| Agent 互 @ 风暴 | mention 上限、协助深度上限、同父同 agent 单一派生节点、拒绝必播报 |
| A2A 终态任务追加语义不一 | D9：follow-up 发新远程消息，上下文由 hub 包保证 |
| 排队消息被用户认为「没反应」 | 前端排队角标＋assistant 明确播报投递时机 |
| 房间 SSE 事件量 | 单房间单流＋`since_seq` 回放；任务级流保留给详情面板 |
| mention 文本误解析 | 词边界＋仅允许成员/已注册名，忽略大小写边界情况（如邮箱） |
| 多写者 seq 竞争 | 房间消息由 coordinator 串行处理（单 asyncio 事件循环内 await 顺序保证） |

## 14. 后置扩展（非 v1）

- 私密消息 `visible_to`、房间级入群审批 policy、多人类协作者、消息搜索、附件、Agent 主动监听订阅、mention 上限可配置化。
