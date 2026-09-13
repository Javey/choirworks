# ChoirWorks A2A 协议兼容设计（v1）

- 日期：2026-09-13
- 状态：已评审通过（待实现）
- 关联：`2026-09-12-a2a-orchestration-platform-design.md`（平台设计）、`2026-09-13-group-chat-collaboration-design.md`（工作群）、`2026-09-13-peer-assist-dynamic-dag-design.md`（动态 DAG）

## 1. 背景与目标

ChoirWorks 已有内部事件流（自定义 SSE）与南向 A2A 客户端（平台 → 远程 Agent）。本设计把方向反过来：**让 ChoirWorks 自身成为一个标准 A2A v1.0 Server**，使任意 A2A client 能把它当作一个 agent 编排；同时把群聊语义以 A2A Extension 形式暴露，服务自有前端。

直接动机是**群嵌套**：未来一个 subagent 可能本身就是另一个 ChoirWorks 实例。只有北向兼容 A2A，嵌套（外层 hub 把内层 hub 当普通 agent 派发/订阅/取消）才能在协议层同构、递归成立。

目标：

1. **标准 A2A v1.0 Server**：AgentCard + JSON-RPC 端点，支持 `SendMessage` / `SendStreamingMessage` / `GetTask` / `CancelTask` / `SubscribeToTask`，被官方 `a2a-sdk` client 直接消费。
2. **hub task = 1 个 A2A Task**：一次订阅看全貌；节点粒度经 `metadata` 表达；节点产物映射为 A2A artifact 增量流。
3. **Room Extension**：conversation 暴露为合成 A2A Task，群消息映射为 A2A Message；未感知扩展的 client 优雅降级。
4. **前端迁移**：wire 层改走 A2A（POST SSE），UI 视图模型不变（适配层独立）。
5. **零回归**：现有 REST/SSE、内部 EventType、DAG/协调器行为不变；162 后端 / 40 前端测试保持通过。

非目标（v1）：

- **v0.3 兼容**：`enable_v0_3_compat=False`，只支持 v1.0，不做历史负担。
- 外层订阅内层房间、跨层人类参与（嵌套只做 task 级）。
- push notification 配置、gRPC/REST binding（只挂 JSONRPC）、`ListTasks`。
- 认证/多租户（沿用内网/localhost 假设）。
- Room history 分页（阶段 1 全量，标注 TODO）。
- 删除或替换现有 `/v1/tasks/*`、`/v1/conversations/*` 端点。

## 2. 定稿决策

| # | 决策 | 说明 |
|---|---|---|
| A1 | 实现路径：自定义 `RequestHandler` + SDK `create_jsonrpc_routes` | SDK 负责 JSON-RPC 信封/SSE 帧/错误码/扩展头解析；我们只写映射与桥接（对齐 sim 的用法） |
| A2 | hub task = 1 A2A Task | `task_id` 即 A2A id，`conversation_id` 即 `context_id`；节点不驱动任务级 state |
| A3 | 节点细节进 `metadata` | 每次节点变化发 `status_update`，`metadata` 带 node_id/node_name/from/to/agent_name/attempt |
| A4 | 产物用 A2A artifact 增量流 | `artifact_id = "{node_id}:{artifact_id}"`，`append` 透传，节点终态补 `last_chunk=true` |
| A5 | 订阅不平滑回放历史 | `SubscribeToTask` 先发 Task 快照（覆盖累计 artifacts/history），再发 live；靠快照兜底断线 |
| A6 | 终态后发消息 = hub follow-up | 返回**新** Task（文档注明 id 会变）；A2A 规范不强制同 id |
| A7 | 房间 = 合成 A2A Task（Room Extension） | `id = context_id = conversation_id`；`metadata` URI 键标记；`history` = 群消息 |
| A8 | 未感知扩展的 client 优雅降级 | 看到的是合法 Task/Message；扩展语义只在 `metadata` 与 `Message.extensions` |
| A9 | 前端 wire 层换 A2A，UI domain 不变 | `a2a.ts` + 适配层把 StreamResponse 转成现有 `RoomEvent`/`TaskEvent`；roomView/taskView 不重写 |
| A10 | 阶段 1 无鉴权、v1.0-only | 明确写入 AgentCard 与文档 |

## 3. 协议事实基线（已验证）

以下事实来自 `a2a-sdk[http-server]>=1.1,<2` 与本仓库依赖环境，是实现与测试的依据：

1. **JSON-RPC 方法名（v1.0，gRPC 风格）**：`SendMessage`、`SendStreamingMessage`、`GetTask`、`CancelTask`、`SubscribeToTask`、`ListTasks` 等（`.venv/.../server/routes/jsonrpc_dispatcher.py:114-125`）。
2. **流式响应**：每个 SSE `data:` 帧是一个 JSON-RPC 2.0 response，`result` 为 `StreamResponse` 的 proto3 JSON（camelCase 字段），SDK client 用 `json_format.ParseDict` 解析（`.venv/.../client/transports/jsonrpc.py:355-373`）。
3. **StreamResponse oneof**：`task` / `message` / `statusUpdate` / `artifactUpdate`（`a2a.types` protobuf）。
4. **Task 字段**：`id`、`contextId`、`status{state,message,timestamp}`、`artifacts[]`、`history[]`、`metadata`(Struct)；**Task 无 `extensions` 字段**，扩展标记只能进 `metadata`。
5. **Message/Artifact 有 `extensions: repeated string`**（扩展 URI 列表）。
6. **TaskState 枚举**：`TASK_STATE_SUBMITTED/WORKING/INPUT_REQUIRED/COMPLETED/FAILED/CANCELED/REJECTED/AUTH_REQUIRED`。
7. **扩展机制**：AgentCard `capabilities.extensions[]` 声明；client 发 `A2A-Extensions` 头激活；SDK 服务端解析进 `ServerCallContext.requested_extensions`（`a2a/extensions/common.py`）；client 侧 `with_a2a_extensions()` 生成头（`client/service_parameters.py:49`）。
8. **AgentCard v1.0** 用 `supportedInterfaces=[{protocolBinding:"JSONRPC", url, protocolVersion:"1.0"}]`（无独立 `url` 字段）。
9. **错误类**：`a2a.utils.errors` 提供 `TaskNotFoundError` / `TaskNotCancelableError` / `UnsupportedOperationError` / `InvalidParamsError` / `InternalError` 等，SDK 自动映射 JSON-RPC 错误码。
10. **任务创建前置**：hub `TaskService.create_task` 支持 `conversation_id=None`（无群任务）与指定 conversation；`coordinator.handle_human_message` 是群消息的统一入口。

## 4. 总体架构

```
外部 A2A client / 外层 ChoirWorks / 自有前端
        │  GET  /.well-known/agent-card.json      （AgentCard，声明 streaming + room 扩展）
        │  POST /v1/a2a                            （JSON-RPC，SDK 路由）
        ▼
┌─ A2A Facade（新增）──────────────────────────────────────────────────┐
│  a2a/server.py   HubA2AHandler(RequestHandler)                        │
│  a2a/mapping.py  内部 Event/投影 ⇄ A2A 类型（纯函数，可单测）           │
│  a2a/card.py     AgentCard / 扩展声明                                  │
│  桥接：TaskService · EventBus · 房间投影 · Coordinator                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ 只读投影 + 现有服务调用（零改动）
        core/tasks · core/orchestrator · core/coordinator · store
                               │
        现有 REST/SSE（保留，不迁移、不删除）:
          /v1/tasks/{id}/events · /v1/conversations/{id}/stream · 其余 /v1/*
```

- 挂载方式：`create_agent_card_routes(card)` + `create_jsonrpc_routes(request_handler=HubA2AHandler(...), rpc_url="/v1/a2a")`，注册进现有 FastAPI app；AgentCard 路由必须先于前端静态挂载注册。
- 公共地址：新增配置 `a2a.public_url`（默认 `http://127.0.0.1:8000`）用于 card 的 `supportedInterfaces.url`。
- `RemoteAgentClient`（南向）保持不变；北向 facade 与其互不影响。

## 5. 核心映射规格

### 5.1 任务状态映射

| 内部 `TaskStatus` | A2A `TaskState` | 备注 |
|---|---|---|
| `pending` | `TASK_STATE_SUBMITTED` | |
| `planning` | `TASK_STATE_WORKING` | `metadata.substate="planning"` |
| `running` | `TASK_STATE_WORKING` | |
| `awaiting_input` | `TASK_STATE_INPUT_REQUIRED` | `status.message` = 干预问题 |
| `completed` | `TASK_STATE_COMPLETED` | 流终止 |
| `failed` | `TASK_STATE_FAILED` | `status.message` = 错误信息；流终止 |
| `canceled` | `TASK_STATE_CANCELED` | 流终止 |

节点状态（9 态）**不驱动**任务级 state（并行节点会抖动），全部进 `status_update.metadata`。生成器维护 `last_state`，仅在 state 变化时发 status update。

### 5.2 事件 → StreamResponse（21 种任务事件全覆盖）

| 内部事件 | A2A 输出 | 关键字段 |
|---|---|---|
| `task.created` | `StreamResponse.task` | 初始快照（见 5.4） |
| `task.state_changed` | `status_update` | state 映射；`metadata={from,to}` |
| `plan.created` | `artifact_update` | `artifact_id="plan:{plan_id}"`、`name="plan"`、`parts=[{data: dag}]`、`append=false`、`last_chunk=true`、metadata `{version,rationale}` |
| `plan.extended` / `plan.superseded` | `artifact_update` | 同上，全量替换（`append=false`） |
| `node.dispatch.intent` | `status_update` | `metadata={node_id,kind:"node.dispatch.intent",agent_name}` |
| `node.dispatched` | `status_update` | `metadata` 附远程 `a2a_task_id` |
| `node.state_changed` | `status_update` | `metadata={node_id,node_name,from,to,agent_name,attempt,plan_id}` |
| `node.retry.scheduled` | `status_update` | `metadata={node_id,attempt}` |
| `node.invalidated` / `node.cancel.sent` | `status_update` | `metadata={node_id,kind}` |
| `node.artifact` | `artifact_update` | `artifact_id="{node_id}:{artifact_id}"`、`parts=[{text}]`、`append` 透传；`metadata={node_id}` |
| `node.output` | 不发 | artifacts 已增量表达，避免重复；节点终态由 `last_chunk` 收尾 |
| `intervention.requested` | `status_update` | `state=INPUT_REQUIRED`；`status.message`=`{messageId:干预id, role:ROLE_AGENT, parts:[{text:问题}]}`；`metadata={intervention_id,node_id,policy,deadline}` |
| `intervention.resolved` | `status_update` | 回 `WORKING`（或当前 state）；答案文本进 `status.message`；`metadata={intervention_id}` |
| `intervention.failed` | `status_update` | `metadata={intervention_id,kind}`；失败通常随后 Task FAILED |
| `checkpoint.created` / `rollback.performed` | `status_update` | 纯通知，`metadata={kind,...}` |
| `error` | `status_update` | 不改 state；`metadata={kind:"error",message,node_id?}` |
| `task.completed` / `task.failed` | `status_update` | 终态 + 流终止；failed 带错误 message |
| `message.*` / `room.*` / `conversation.created` | Room 通道 | 见第 6 节；不进任务流 |

### 5.3 Artifact 规则

- id 前缀：`"{node_id}:{内部 artifact_id}"`，避免跨节点冲突；
- `parts=[{text: piece}]`（内部 artifact 为文本增量）；
- `append` 直接透传内部语义；`last_chunk` 默认 false；
- 节点进入终态（completed/failed/canceled/invalidated）时，对该节点**已知的全部 artifact** 各补发一条 `artifact_update{append=true, last_chunk=true}`（parts 为空），作为流式结束信号；
- plan artifact 例外：一次性全量（`append=false,last_chunk=true`）。

### 5.4 快照（`GetTask` / 订阅首帧）

```json
{
  "id": "<task_id>",
  "contextId": "<conversation_id>",
  "status": {"state": "TASK_STATE_WORKING", "message": null, "timestamp": "..."},
  "artifacts": [ { "artifactId": "n1:a1", "name": "output", "parts": [{"text": "..."}] } ],
  "history": [ { "messageId": "<task_id>:request", "role": "ROLE_USER", "parts": [{"text": "<request>"}] } ],
  "metadata": {
    "plan_version": 2,
    "nodes": [{"id": "n1", "name": "researcher", "status": "completed",
               "agent_name": "researcher", "attempt": 1}]
  }
}
```

- `artifacts` = 各节点产物的聚合（含终态收尾后的完整文本）；
- `history` 只放初始 request 一条（保持精简，节点产出由 artifacts 表达）；
- `status.message`：仅 INPUT_REQUIRED（问题）与 FAILED（错误）时填充，其余为空；
- `metadata.nodes` 是前端任务面板与嵌套外层了解内部结构的窗口。

### 5.5 订阅语义（`SubscribeToTask`）

1. 发送快照 `StreamResponse.task`（覆盖断线期间的累计状态与 artifacts）；
2. 从当前时刻起转发 live 映射（按 `EventStore` seq 去重，复用 `api/sse.py` 的 `seen` 模式）；
3. **不做历史事件回放**（A2A 无 seq 语义，快照已覆盖）；
4. 自有前端在 SSE 帧中仍保留 `id: <seq>` 字段用于乐观合并与调试（SDK 客户端忽略 `id:`）；
5. 流终止：任务终态 status update 发出后关闭；`message/stream` 与 `subscribe` 同规则；
6. 背压：沿用 `EventSubscription` 队列满即关闭的行为，客户端重连后由快照兜底。

### 5.6 发送语义（`SendMessage` / `SendStreamingMessage`）

| 请求 | hub 行为 | 响应 |
|---|---|---|
| 无 `task_id`（可带 `context_id`） | 新建 hub task；无 `context_id` 时自动建群（标题取消息前 30 字符）并作为 `context_id` 返回 | `Task` |
| 带 `task_id`，任务 `INPUT_REQUIRED` | `answer_intervention`（引用/路由沿用协调器规则） | 同一 `Task` |
| 带 `task_id`，任务运行中 | 排队补投 / 打断（沿用协调器现有策略） | 同一 `Task`（metadata 标注 queued） |
| 带 `task_id`，任务终态 | hub follow-up（同一 conversation 新 task） | **新 `Task`**（A6，文档注明） |
| 带 `context_id`（Room 扩展） | `coordinator.handle_human_message` | 产生任务→`Task`；仅插话/排队→`Message` |

- 文本 `@` 解析保持服务端现有逻辑；扩展 metadata 中的 `mentions`/`quote_id` 与文本解析结果合并；
- `SendMessage` 返回 `SendMessageResponse{task|message}`（SDK 自动包装）；
- `CancelTask` → hub `cancel_task`；终态任务抛 `TaskNotCancelableError`；
- **已知限制（M11）**：运行中追加消息以第一个 `DISPATCHED/WORKING` 节点为目标入队，随现有 `deliver_queued_for_terminal` 机制在节点 COMPLETED 时投递；**无活跃节点时（如任务刚创建、尚未出现活跃节点）消息仅进入房间时间线、不做投递**；队列消息所属节点 FAILED 时依赖启动 `reconcile()` 兜底投递。A2A Task `metadata` 的 queued 标注推迟到 M12。

## 6. Room Extension v1

### 6.1 URI 与声明/激活

- URI：`https://github.com/Javey/choirworks/extensions/room/v1`（可改；规范文档 `docs/extensions/room-v1.md` 随仓库发布）
- AgentCard：

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

- 激活：client 发 `A2A-Extensions` 头；handler 通过 `ServerCallContext.requested_extensions` 判断（阶段 1 行为不因激活与否改变，仅用于日志与将来协商）；
- `required:false`：未请求激活的 client 也能使用（看到的是合法 Task/Message）。

### 6.2 合成 Room Task

| 字段 | 值 |
|---|---|
| `id` / `contextId` | `conversation_id` |
| `status.state` | 房间有 task 在运行 → `WORKING`；否则 `INPUT_REQUIRED`（等人开口） |
| `history` | 房间消息映射后按 seq 升序（阶段 1 全量，标注分页 TODO） |
| `artifacts` | 无 |
| `metadata[room-uri]` | `{kind:"room", title, members:[{agent_name,agent_url,reason,joined_at}], summary:{...}, message_count, last_seq}` |

- `GetTask(conversation_id)`：先按 task_id 查任务投影，未命中再按 conversation 查房间投影；都未命中抛 `TaskNotFoundError`。

### 6.3 消息映射（Room Message → A2A Message）

| A2A 字段 | 来源 |
|---|---|
| `messageId` | 房间消息 id |
| `contextId` | `conversation_id` |
| `taskId` | 消息关联 task（无则 = 合成 Room Task id） |
| `role` | `user`→`ROLE_USER`；`assistant`/`agent`→`ROLE_AGENT` |
| `parts` | `[{text}]` |
| `extensions` | `[room-uri]` |
| `metadata[room-uri]` | `{kind:"message"|"participant_joined"|"summary_updated", sender, seq, mentions[], quote_id, node_id, intervention_id, queued_for_node_id}` |

### 6.4 房间事件映射

| 内部事件 | A2A 输出 |
|---|---|
| `message.posted` | `StreamResponse.message` |
| `message.delivered` | `status_update`（`metadata[room-uri]={kind:"message.delivered",message_id,node_id}`，供排队角标） |
| `room.participant_joined` | `status_update`（`kind:"room.participant_joined"` + 成员信息；协调器另有入群播报消息走 Message 通道） |
| `room.summary_updated` | `status_update`（`kind:"room.summary_updated"` + summary 对象，并同步进合成 Task metadata） |
| `conversation.created` | 不发（建群通过 REST 或首条消息隐式发生） |

### 6.5 订阅与发送

- **订阅**：`SubscribeToTask(conversation_id)` → 先发合成 Task 快照（含 history），再转发 live；房间流**常开**（客户端主动断开）；
- **发送**：`SendMessage` + `message.contextId=conversation_id` → `coordinator.handle_human_message`；
  - 产生任务 → 返回 `Task`；
  - 仅插话/排队/打断 → 返回 `Message`（映射该条房间消息）；
  - `mentions`、`quote_id` 从 `metadata[room-uri]` 读取，与文本 `@` 解析合并。

### 6.6 优雅降级

- 无扩展感知的 client：`GetTask(conversation_id)` 得到一个合法 Task（history 是消息列表）；`SubscribeToTask` 收到 Task/Message 序列——可读、可用，只是不理解房间语义；
- 扩展感知的 client（嵌套 ChoirWorks、自有前端）：按 `metadata[room-uri]` 还原成员、引用、排队、摘要等语义。

## 7. 错误处理 / 背压 / 对账 / 安全

| 场景 | 处理 |
|---|---|
| `GetTask`/`SubscribeToTask` 未知 id | `TaskNotFoundError` |
| `CancelTask` 终态任务 | `TaskNotCancelableError` |
| `CancelTask` 未知 id | `TaskNotFoundError` |
| push notification 配置 / `ListTasks`（阶段 1） | `UnsupportedOperationError` |
| 缺 message / parts 为空 | `InvalidParamsError` |
| handler 内部异常 | `InternalError` + ERROR 日志 |
| 订阅队列满 | 关闭订阅、结束流；重连靠快照兜底 |
| 生成器取消 | `try/finally` 中 `unsubscribe/close`（对齐 `api/sse.py:55-57`） |

- **对账**：现有启动 `coordinator.reconcile()` 不变；facade 无独立状态（纯投影），不新增对账逻辑；
- **安全**：阶段 1 无认证（AgentCard 不声明 securitySchemes），文档明示仅内网使用；
- **顺序**：EventBus 单 key 有序 + event store seq 去重。

## 8. 前端迁移（M13）

新增 `frontend/src/api/a2a.ts`：

- `postSse(url, body, signal)`：`fetch` POST + `ReadableStream` 按 W3C SSE 解析（空行分帧、`data:` 拼接、忽略注释行），每帧 `JSON.parse` 得 JSON-RPC response，取 `result` 的 oneof；
- `sendMessage({text, contextId?, taskId?, roomMeta?})`、`getTask(id)`、`subscribeTask(id, onResponse)`；
- 适配层（与后端映射对称）：
  - `StreamResponse.message` + `metadata[room-uri].kind` → `message.posted` / `room.participant_joined` / `room.summary_updated`；
  - `statusUpdate` room kind `message.delivered` → `message.delivered`；
  - 任务流：`statusUpdate` → `task.state_changed` / `node.state_changed`（metadata 取 node 信息）；`artifactUpdate` → `node.artifact`（node_id 从 `artifactId` 前缀解析）；`task` → 快照合并（复用现有 `mergeRoomSnapshot`/`taskView` 能力）；
- `useRoom` / `useConversation` 切换到 `a2a.ts` 的订阅与发送；快照阶段 1 仍用现有 REST（`GET messages` / `GET task`），旧 SSE 端点保留；
- 断线重连：重新 `subscribe`，以快照首帧做增量合并（现有乐观插入/合并逻辑直接复用）。

## 9. 里程碑

| 里程碑 | 内容 | 验收 |
|---|---|---|
| **M11 核心 A2A facade** | AgentCard（无扩展）+ `HubA2AHandler` + `mapping.py` 任务映射 + 路由挂载 + 配置 | SDK client 全流程集成测试；**双 hub 实例嵌套冒烟**（A 注册 B 为 agent → A 派任务 → B 执行 → 产物回传） |
| **M12 Room Extension** | 合成 Room Task + 消息/事件映射 + 扩展声明 + `docs/extensions/room-v1.md` | SDK client 订阅房间/发消息/两分支响应；优雅降级验证 |
| **M13 前端迁移** | `a2a.ts` + 适配层 + hooks 切换 | 前端 40 测试通过 + A2A 路径回归（房间/任务/断言） |

## 10. 测试策略

- **单测（mapping）**：21 种任务事件 → StreamResponse 全覆盖；状态映射表；artifact 前缀与 `last_chunk` 收尾；Room Message/metadata；发送四分支；
- **集成（真 SDK client）**：
  - card 解析 → `create_client`；`SendMessage` 非流式 → Task；
  - `SendStreamingMessage` 序列断言：task → status_update* → artifact_update(append) → 终态；
  - `GetTask` 快照（artifacts 聚合、metadata.nodes）；`SubscribeToTask` 快照+live；
  - 干预：INPUT_REQUIRED → 带 task_id 回复 → 同 task 恢复完成；
  - `CancelTask`；未知 id 的错误类型；
  - Room：`GetTask(conversation)` history；`SubscribeToTask` 收 message；`SendMessage` 的 Task/Message 两分支；
- **嵌套验收**：双 hub（`free_port` + uvicorn，同 sim 模式）A/B，B 的 card 注册进 A 的 registry，A 派任务 → 断言 A 节点 completed、artifact 含 B 产出、B 侧群叙事完整；
- **前端**：`postSse` 解析单测（录制帧夹具）、适配层映射单测、`useRoom`/`useConversation` 回归。

## 11. 风险与对策

| 风险 | 对策 |
|---|---|
| SDK 升级导致类型/路由破坏 | A2A 类型接触点全部集中 `a2a/server.py`/`a2a/mapping.py`/`a2a/card.py`（对齐 `RemoteAgentClient` 唯一入口模式） |
| Room history 过大 | 阶段 1 接受；spec/扩展文档标注分页 TODO |
| `Task` 无 `extensions` 字段 | 房间标记只走 `metadata` URI 键；Message/Artifact 才用 `extensions` |
| 订阅重复/丢失 | seq 去重 + 快照首帧兜底 |
| 前端 POST SSE 解析细节 | 单独解析器 + 夹具单测；EventSource 不可用于 POST 是已知约束 |
| AgentCard 静态 URL 与部署不符 | `a2a.public_url` 配置；默认 localhost |

## 12. 附录 A：报文样例

**AgentCard 片段**

```json
{
  "name": "ChoirWorks",
  "description": "多 Agent 协作工作群（A2A facade）",
  "version": "0.1.0",
  "supportedInterfaces": [
    {"protocolBinding": "JSONRPC", "url": "http://127.0.0.1:8000/v1/a2a", "protocolVersion": "1.0"}
  ],
  "capabilities": {
    "streaming": true,
    "extensions": [
      {"uri": "https://github.com/Javey/choirworks/extensions/room/v1",
       "description": "Conversations as long-lived A2A tasks", "required": false}
    ]
  },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [{"id": "orchestrate", "name": "多 Agent 编排", "tags": ["orchestration"]}]
}
```

**SendStreamingMessage 请求**

```json
{"jsonrpc": "2.0", "id": 1, "method": "SendStreamingMessage",
 "params": {"message": {"messageId": "m-1", "role": "ROLE_USER",
   "parts": [{"text": "@researcher 请分析 X"}], "contextId": "conv-1"}}}
```

**流式帧序列**

```json
{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"task-1","contextId":"conv-1","status":{"state":"TASK_STATE_SUBMITTED"}}}}
{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"task-1","contextId":"conv-1","status":{"state":"TASK_STATE_WORKING"},"metadata":{"node_id":"n1","node_name":"researcher","from":"ready","to":"working"}}}}
{"jsonrpc":"2.0","id":1,"result":{"artifactUpdate":{"taskId":"task-1","contextId":"conv-1","artifact":{"artifactId":"n1:a1","name":"output","parts":[{"text":"分析…"}]},"append":true,"lastChunk":false}}}
{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"task-1","contextId":"conv-1","status":{"state":"TASK_STATE_COMPLETED"}}}}
```

**Room 消息帧**

```json
{"jsonrpc":"2.0","id":2,"result":{"message":{"messageId":"msg-9","contextId":"conv-1","taskId":"conv-1","role":"ROLE_AGENT","parts":[{"text":"已将 @researcher 加入群聊"}],"extensions":["https://github.com/Javey/choirworks/extensions/room/v1"],"metadata":{"https://github.com/Javey/choirworks/extensions/room/v1":{"kind":"message","sender":"assistant","seq":2,"mentions":["researcher"]}}}}}
```

## 13. 附录 B：SDK 事实索引

| 事实 | 位置 |
|---|---|
| JSON-RPC 方法名映射 | `.venv/.../a2a/server/routes/jsonrpc_dispatcher.py:114-125` |
| 流式响应解析（client） | `.venv/.../a2a/client/transports/jsonrpc.py:355-373` |
| SSE 解析（W3C） | `.venv/.../a2a/client/transports/http_helpers.py:71-120` |
| 扩展头解析 | `.venv/.../a2a/extensions/common.py`；`client/service_parameters.py:49` |
| RequestHandler 接口 | `RequestHandler.on_message_send/on_message_send_stream/on_get_task/on_cancel_task/on_subscribe_to_task/on_list_tasks` |
| 路由注册 | `create_jsonrpc_routes(request_handler, rpc_url, enable_v0_3_compat=False)`；sim 用例 `src/choirworks/sim/fake_agent.py:233` |
| 错误类 | `a2a.utils.errors` |
