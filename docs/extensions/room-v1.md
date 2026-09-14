# ChoirWorks Room Extension v1

> **状态：已废弃（历史文档）。** 该扩展最初用于旧版事件溯源栈的「合成 Room Task」
> 语义，相关实现（`a2a/server.py`、房间 REST/SSE）已随 A2A SDK 迁移删除。
> 当前实现仅在消息 metadata 中沿用 `room/v1` URI 承载 `mentions` / `quote_id` /
> `interrupt` 等群聊字段；会话语义见 `docs/product.md`。

- **URI:** `https://github.com/Javey/choirworks/extensions/room/v1`
- **状态:** 阶段 1（无鉴权、无分页）

## 1. 概述

ChoirWorks 的「群」是一个长期存在的 A2A `Task`：群里的消息是该 Task 的 `history`，
发送消息即向该 Task 追加 `Message`。感知扩展的 client 可读取 `metadata[room-uri]`
还原成员、引用、排队、摘要等群语义；不感知扩展的 client 看到的是完全合法的
Task/Message/StatusUpdate 序列。

## 2. AgentCard 声明

```json
"capabilities": {
  "streaming": true,
  "extensions": [{
    "uri": "https://github.com/Javey/choirworks/extensions/room/v1",
    "description": "Conversations as long-lived A2A tasks; room messages as A2A Messages",
    "required": false
  }]
}
```

## 3. 激活

client 在 HTTP 头 `A2A-Extensions` 中声明 URI。阶段 1 服务端行为不因激活与否改变
（仅日志），因为合成 Task/Message 对未激活 client 同样合法。

## 4. 合成 Room Task

| 字段 | 值 |
|---|---|
| `id` / `contextId` | `conversation_id` |
| `status.state` | 群内有非终态任务 → `TASK_STATE_WORKING`；否则 `TASK_STATE_INPUT_REQUIRED` |
| `history` | 群消息按 `seq` 升序（阶段 1 最多 1000 条，分页见 §8） |
| `artifacts` | 无 |
| `metadata[room-uri]` | `{kind:"room", title, members:[{agent_name,agent_url,reason,joined_at}], summary:{covers_seq,updated_at,content}, message_count, last_seq}` |

`GetTask(id)` 先按任务投影查，未命中再按会话投影查；都未命中 → `TaskNotFoundError`。
`CancelTask` 只作用于任务；对 Room Task 调用会得到 `TaskNotFoundError`（房间不是可取消任务）。

## 5. Message 映射

| A2A 字段 | 来源 |
|---|---|
| `messageId` | 房间消息 id |
| `contextId` | `conversation_id` |
| `taskId` | 消息关联任务；无关联时为合成 Room Task id |
| `role` | `user` → `ROLE_USER`；`assistant`/`agent` → `ROLE_AGENT` |
| `parts` | `[{text}]` |
| `extensions` | `[room-uri]` |
| `metadata[room-uri]` | `{kind:"message", sender, seq, mentions[], quote_id, node_id, intervention_id, queued_for_node_id}` |

## 6. 事件映射（订阅）

| 内部事件 | A2A 输出 |
|---|---|
| `message.posted` | `StreamResponse.message` |
| `message.delivered` | `status_update`（`kind:"message.delivered"`，`message_id`、`node_id`，供排队角标） |
| `room.participant_joined` | `status_update`（`kind:"room.participant_joined"` + `agent_name/agent_url/reason`） |
| `room.summary_updated` | `status_update`（`kind:"room.summary_updated"` + `covers_seq/summary`） |
| 其他（任务/节点/计划事件） | 不转发 |

> 房间状态变化（WORKING ↔ INPUT_REQUIRED）以重新订阅时的快照为准；订阅期间不随任务
> 生命周期推送状态帧。

## 7. 订阅与发送

- `SubscribeToTask(conversation_id)`：先发合成 Task 快照（含 history），随后转发 live；
  房间流常开，由 client 主动断开。
- `SendMessage` + `message.contextId=conversation_id`：
  - 产生任务（@单人 / 自动拆解 / 作答干预 / 终态续接）→ 返回 `Task`；
  - 排队补投 / 打断转交 → 返回 `Message`（映射该条房间消息）。
- `metadata[room-uri]` 可携带 `mentions`、`quote_id`、`interrupt`，与文本 `@` 解析合并；
  `interrupt=true` 必须同时给 `quote_id`，否则 `InvalidParamsError`。

## 8. 已知限制（阶段 1）

- history 全量返回上限 1000 条；分页（`historyLength`/游标）未实现，`GetTask` 不接受分页参数；
- 无鉴权，仅限内网使用；
- 房间订阅不转发任务类事件；
- Room Task 的 `artifacts` 为空（任务产物在各自任务流中）。
