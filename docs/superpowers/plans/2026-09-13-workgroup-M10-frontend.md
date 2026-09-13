# 工作群协作 M10：前端群聊 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`).
> 前置：M8（消息底座）+ M9（路由治理）已完成，后端 159 测试全绿。spec §9 为权威 UI 设计。

**Goal:** 把「工作群」搬进浏览器：群聊主线（人类/Agent/assistant/系统消息 + 引用 + @高亮 + 流式光标）、成员状态、@自动补全、引用回复、排队角标、打断按钮；右侧保留既有任务/节点面板。

**Architecture:** 新增 `lib/roomView.ts`（纯函数折叠）+ `hooks/useRoom.ts`（REST 快照 + 房间 SSE `since_seq` 增量）；复用既有 `taskView` 折叠右侧面板与 `NodeCard`。入口仍是 `?c=<conversation_id>`；「群聊」为主线，任务面板可折叠。

**Tech Stack:** Vite 8 / React 19 / TS / Vitest 5；测试沿用 `frontend/src/test/setup.ts`、Testing Library。

---

### Task 1: API 客户端与类型（含测试）

**Files:**
- Modify: `frontend/src/api/client.ts`（`getRoomMessages`、`postRoomMessage`）
- Modify: `frontend/src/lib/types.ts`（`RoomMessageDto`、`RoomMemberDto`、`RoomSummaryDto`、`RoomMessagesDto`、`PostMessageIn/Out`）
- Test: `frontend/src/api/client.test.ts`

类型对齐后端 `api/schemas.py`：`RoomMessage(id, conversation_id, seq, role, sender, text, mentions, quote_id, task_id, node_id, intervention_id, queued_for_node_id, delivered_at, created_at)`。

- `getRoomMessages(id, sinceSeq=0)` → `GET /v1/conversations/{id}/messages?since_seq=`
- `postRoomMessage(id, {text, mentions?, quote_id?, interrupt?})` → `POST .../messages`
- 测试：fetch mock 断言 URL/方法/JSON；错误路径抛 `ApiError`（复用现有 client 错误类）。

### Task 2: `roomView` 折叠器（TDD）

**Files:**
- Create: `frontend/src/lib/roomView.ts`
- Test: `frontend/src/lib/roomView.test.ts`

```ts
export interface RoomView {
  messages: RoomMessageDto[];
  members: RoomMemberDto[];
  summary: RoomSummaryDto | null;
  lastSeq: number;
}
export function fromRoomSnapshot(snapshot: RoomMessagesDto): RoomView;
export function applyRoomEvent(view: RoomView, event: {seq; type; payload}): RoomView;
```

规则：
- `message.posted` → 按 seq 去重插入并排序；`lastSeq = max`。
- `message.delivered` → 对应消息 `delivered_at` 置位（排队角标消失）。
- `room.participant_joined` → 成员 upsert。
- `room.summary_updated` → 覆盖 summary。
- 其他事件（plan/node/intervention）不影响 roomView（右侧面板处理）。

测试：快照加载、增量追加去重、delivered、成员加入、摘要替换。

### Task 3: 群聊组件（TDD）

**Files:**
- Create: `frontend/src/components/room/{RoomThread,RoomMessage,QuoteBlock,MemberBar}.tsx`
- Modify: `frontend/src/index.css`
- Test: `frontend/src/components/room/room.test.tsx`

- `RoomMessage`：按 role 渲染
  - `user`：右侧浅蓝气泡；`agent`：头像色块（名字首字）+ 名称 + 文本 + `streaming` 时光标；`assistant`：左侧浅紫气泡（拆解/派发/完成）；`system`：居中灰条。
  - `@name` 高亮为 chip（正则切分，仅群成员）。
  - `quote_id` → `QuoteBlock`（发送者 + 摘要 + 点击滚动到源消息）。
  - 排队中（`queued_for_node_id && !delivered_at`）显示「排队中」角标。
  - 在途节点消息（`node_id` 且节点 working）显示「打断」按钮（二次确认）。
- `RoomThread`：列表 + 空态；`MemberBar`：成员 chips + 工作状态（来自 `taskView` 节点状态）。
- 测试：四种角色渲染、@高亮、引用块、排队角标、打断确认回调。

### Task 4: `useRoom` Hook + Composer（TDD）

**Files:**
- Create: `frontend/src/hooks/useRoom.ts`
- Create: `frontend/src/components/room/RoomComposer.tsx`
- Test: `frontend/src/hooks/useRoom.test.tsx`、`room.test.tsx`

- `useRoom(conversationId)`：
  - 初始 `getRoomMessages` 快照；
  - `EventSource('/v1/conversations/{id}/stream?since_seq=lastSeq')` 增量 `applyRoomEvent`；
  - 断线重连（EventSource 自带 retry；`onerror` 关闭后 1s 重连并带 `since_seq`）；
  - 返回 `{view, send, interrupt}`。
  - `send({text, mentions, quoteId?, interrupt?})` 乐观插入（临时 id），成功后用服务端 `message.posted` 去重替换。
- `RoomComposer`：`@` 触发成员自动补全（键盘 ↑↓/Enter/Esc），引用态显示引用条，发送/中断按钮。
- 测试：mock EventSource 与 fetch，断言增量折叠、乐观去重、@补全键盘选择、引用条。

### Task 5: App 集成 + 右侧面板 + 构建

**Files:**
- Modify: `frontend/src/App.tsx`、`Sidebar.tsx`（会话标题不变）
- Modify: `frontend/src/components/Thread.tsx`（保留给任务详情或移除）
- Test: `frontend/src/components/components.test.tsx`（回归保持 25 个）
- Modify: `README.md`

- 主区：`RoomThread` + `RoomComposer`；右侧：现有 `TaskView` 面板折叠（`details` 样式），从 `useConversation` 的 SSE 复用或仅快照轮询（v1 沿用现有 hook）。
- 空会话：显示引导（输入即建群并发送第一条消息）。
- 验收：`npm test`（25 + 新增）、`npm run build`；实机 `agent-hub-sim --fresh` 走：发「请协调多个子代理协作完成这项分析」→ 群内看到拆解/派发/协助/完成播报与两个 Agent 的流式回复；引用追问与打断可操作。

---

## Self-Review 备忘

- `roomView` 只负责群聊；右侧面板继续用 `taskView`，避免一次大重构。
- 流式光标的增量文本来自既有 `node.artifact` 事件：v1 先用「轮询/SSE 任务流渲染 transient 气泡」，持久化消息以 `message.posted` 为准；若实现复杂，v1 可只显示「正在输入…」占位（spec §9.2 允许降级）。
- 现有前端 25 测试与构建必须保持全绿。
