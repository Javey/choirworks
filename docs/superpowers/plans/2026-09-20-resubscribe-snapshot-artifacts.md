# 计划：补齐重订阅缝隙事件（快照 artifacts）

## 现象

派发公告气泡在 live 流里不出现；刷新页面（`/replay`）能看到。后端已发出事件（DB 的 `task.artifacts` 里有 `call_subagent {requested_by: "orchestrator"}`），前端未渲染。

## 根因（代码级时序）

缝隙 = `execute()` 返回 → 重订阅 `tap()` 建立 之间的事件；SDK 只提供"重订阅快照"这一条补齐通道，前端忽略了快照里的 `artifacts`。

1. `executor.execute()`（`src/choirworks/a2a/executor.py:341`）内 `_plan_and_launch`（:600）依次 await：planner 文本、`create_plan` fc（:638）、成员加入、`_persist`（:652）；最后 `_start_runner`（:657-662）只是 `create_task`，不切换事件循环，随即返回。
   → 这些事件都在初始 `message:stream` 里送达（DOM 中有计划通知与入群通知）。
2. SDK 生产者（`.venv/.../active_task.py:540-549`）在 `execute()` 返回后立刻 `enqueue_event(_RequestCompleted)`；`EventQueueLegacy.enqueue_event`（`event_queue.py:113-121`）为 `queue.put`，队列未满时不挂起 → 哨兵在**同一 tick**入队，排在 runner 首个事件之前。
3. 消费者（`active_task.py:166-170,194`）先转发 `_RequestCompleted`；`ActiveTask.subscribe` 收到自己 request 的哨兵即 `return`（`active_task.py:676-685`）→ HTTP 流关闭，前端 `sendMessageStream` 循环退出（`frontend/src/hooks/useConversation.ts:138-154`）。
4. runner 之后才跑第一轮，`_announce_dispatch`（`executor.py:1254`）把 `call_subagent`(requested_by=orchestrator)、节点 `state_delta` 入队：事件被落盘进 `task.artifacts`，但旧订阅者已退出，`tap()` 只接建立之后的事件（`event_queue.py:174-186`）→ live 丢失。
5. 前端随即重订阅（`useConversation.ts:156-157` → `follow()` :60-77）→ 服务端 `on_subscribe_to_task`（`default_request_handler_v2.py:391-405`）→ `ActiveTask.subscribe(include_initial_task=True)`：先 `tap()`（`active_task.py:636`）再 `yield` 带全部 artifacts 的 Task 快照（:645-646），之后才是 live 事件。
6. 前端 `applyStreamEvent` 的 `task` 分支（`frontend/src/lib/conversationView.ts:308` 起）只合并 `history`，**未读 `result.artifacts`** → 缝隙事件被丢弃；节点产出在 tap 之后，所以只有派发公告缺失。

补充窗口：`tap()`（:636）先于快照读取（:645），两者之间的事件会**既在快照又走 live** → 补齐逻辑必须幂等。

## 修复方案（纯前端，`frontend/src/lib/conversationView.ts`）

1. `ConversationView` 新增 `seenArtifactIds: Set<string>`，`emptyConversation` 置空；现有 `applyStreamEvent` 改名 inner，外层包装在 `artifactUpdate` 时记录 `artifactId`（覆盖所有提前 `return`）。
2. `task` 快照分支：合并 `history` 后遍历 `result.artifacts`，对未在 `seenArtifactIds` 的 artifact 合成 `artifactUpdate` 事件（`append: false, lastChunk: true`）递归应用：
   - `cw_thought` / `cw_type=function_call`：原样透传；事件 metadata 取 `artifact.metadata`（带出 `node_id/agent_name`，与 `replay.py` 行为一致）；
   - 其余文本 artifact：先按 `parts.map(text).join("")` 合并为单 part（与 `replay.py` 一致，避免分块被 `textOfParts` 以换行拼接）。
3. `seenArtifactIds` 保证快照里已见过的 `create_plan` 不被二次执行（否则节点状态会被重置回 pending、通知重复）。
4. 后端不改：缝隙是 SDK `message:stream` 在 `execute()` 返回即结束 + 重订阅先发快照的设计固有，快照就是为补齐准备的；此修复对所有缝隙事件通用。

## 测试（`frontend/src/lib/conversationView.test.ts`）

- 任务快照含派发 `call_subagent` + 节点产出 artifact → 生成派发气泡与 agent 消息；
- 同一快照应用两次 → 不重复（`seenArtifactIds` + 既有消息 id 去重）；
- 已见过的 `create_plan` artifact 出现在快照里 → 不覆盖节点状态/不重复通知。

## 验证

- 临时 vitest 用"带 artifacts 的 Task 快照 + 后续 live 事件"序列断言派发气泡出现且不重复；
- 全量前端 `npm test` / `npm run build` / `npm run lint`；后端无改动、无需回归；
- sim 走 live 流跑一轮，确认派发气泡在计划通知后立即出现，刷新结果一致。

## 关联

- 前置改动（未提交）：working 气泡固化、artifact 元数据回放透传、`call_subagent` 统一事件（`docs/superpowers/plans/2026-09-20-unified-subagent-calls.md`）。
