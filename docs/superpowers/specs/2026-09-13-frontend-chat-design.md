# Agent Hub 前端对话界面设计（v1）

- 日期：2026-09-13
- 状态：已评审通过（待实现）
- 关联：`2026-09-12-a2a-orchestration-platform-design.md`（后端平台设计）

## 1. 背景与目标

Agent Hub 目前只有 REST/SSE API，无可视化界面。本设计为其增加一个**对话式前端**：用户在一个聊天界面中发起任务，实时看到规划与各 Agent 的执行过程，需要时内联回答干预请求，任务结束后在同一会话中追问，并支持取消、单节点重试与 checkpoint 回退。

目标：

1. 对话体验：同一会话内的消息与任务历史连成一条时间线，追问时携带会话上下文。
2. 过程可见：规划、节点状态、Agent 输出、重试、错误以气泡/卡片形式实时流式呈现。
3. 基本控制：取消任务、重试失败节点、回退到 checkpoint，均可在界面上完成。
4. 可靠：断线自动重连并回放缺失事件；终态后以服务端快照为准。

非目标（v1）：

- 中途 steer 正在运行的任务（中途交互只通过干预机制）。
- Agent 注册/管理页面、DAG 图形化编辑器、事件审计页面。
- 用户系统与鉴权（沿用后端现状：无鉴权）。
- 移动端适配（桌面优先，窄屏可用即可）。
- 会话重命名、删除、归档。

## 2. 用户故事与验收标准

| 编号 | 用户故事 | 验收标准 |
|---|---|---|
| US1 | 我输入请求并发送 | 左侧出现新会话；主区用户气泡出现；任务自动规划并开始执行 |
| US2 | 我观看执行过程 | 规划摘要、节点卡片（agent/状态/输出）随 SSE 实时更新；失败显示红色与错误信息 |
| US3 | 我回答 Agent 的提问 | Agent `input-required` 且策略为 human 时，出现干预卡；输入回答后任务继续，卡片变为已答复 |
| US4 | 我追问 | 上一任务终态后输入框可用；发送后同一会话新增用户气泡与新任务，规划时参考之前结果 |
| US5 | 我取消任务 | 运行中任务点「取消」后任务变为已取消，在途节点收到取消信号 |
| US6 | 我重试失败节点 | 失败节点卡片上点「重试」后节点重新执行；成功后任务完成 |
| US7 | 我回退 | 打开回退对话框，选择 checkpoint 先看到 dry-run 影响面，确认后 checkpoint 之后节点被重置并重跑 |
| US8 | 我断网/刷新 | 刷新后历史仍在；运行中任务恢复实时更新（SSE 带 Last-Event-ID 回放） |
| US9 | 未注册 Agent 时我发起任务 | 任务失败，界面提示「请先注册 Agent」及注册方法 |

## 3. 架构总览

```
┌────────────┐  REST (fetch)   ┌──────────────────────┐
│ 浏览器 SPA │◀───────────────▶│ FastAPI (Agent Hub)  │
│ Vite+React │  SSE (EventSource)                    │
└────────────┘                 │  conversations API    │
     开发: vite :5173           │  tasks / SSE / ...    │
     代理 /v1 → :8080           └──────────────────────┘
     生产: FastAPI 托管 dist 静态文件（同源）
```

- 前端为独立工程 `frontend/`（Vite + React + TypeScript），无 UI 库、无 router 依赖（用 `?c=<conversation_id>` 查询参数）。
- 开发态：`npm run dev` 启动 Vite，`/v1` 与 `/healthz` 代理到 `http://127.0.0.1:8080`。
- 生产态：`npm run build` 产出 `frontend/dist`；FastAPI 在 dist 存在时用 `StaticFiles(html=True)` 挂载 `/`（API 路由优先注册）。
- 前端不重写后端投影逻辑，只维护**展示视图**：静态历史来自快照接口，活跃任务由 SSE 事件增量折叠，终态/回退后重新拉取快照兜底。

## 4. 后端设计

### 4.1 Schema 与迁移

新增独立会话表，任务通过 `conversation_id` 关联：

```sql
CREATE TABLE IF NOT EXISTS conversations (
  id         TEXT PRIMARY KEY,
  title      TEXT NOT NULL,
  created_at TEXT NOT NULL
);
-- 迁移（_migrate 内 ALTER，老库兼容）：
ALTER TABLE orchestration_tasks ADD COLUMN conversation_id TEXT;
CREATE INDEX IF NOT EXISTS idx_tasks_conversation ON orchestration_tasks(conversation_id);
```

- `title` 为会话首条请求截断 60 字符；v1 不支持重命名。
- 老任务 `conversation_id` 为 `NULL`：不出现在会话列表；`GET /v1/tasks/{id}` 仍可用。
- `conversations` 的 `updated_at` 不落列，由列表查询 `MAX(tasks.updated_at)` 计算。

### 4.2 事件与投影

- `TASK_CREATED` 载荷新增（向后兼容，读取用 `.get()`）：
  - `conversation_id: str`
  - `conversation_title: str | None`（仅新建会话的首任务携带）
- 投影 `apply_event(TASK_CREATED)`：
  1. 若 `conversation_id` 存在：`INSERT INTO conversations (...) VALUES (...) ON CONFLICT(id) DO NOTHING`；
  2. 插入任务时写入 `conversation_id`。
- `rebuild()` 清理表列表加入 `conversations`（置于 `orchestration_tasks` 前删除）。
- 无新增 EventType。

### 4.3 API 契约

| 方法 | 路径 | 请求 | 响应 |
|---|---|---|---|
| POST | `/v1/tasks` | `{request, target?, conversation_id?}` | `201 {task_id, plan_id?, node_ids?, conversation_id}` |
| GET | `/v1/conversations` | — | `[{id, title, created_at, updated_at, task_count, last_status}]` |
| GET | `/v1/conversations/{cid}` | — | `{conversation: 上述摘要, tasks: [TaskSnapshot…]}`（按 created_at 升序） |
| GET | `/v1/tasks/{id}/checkpoints` | — | `[Checkpoint…]`（按 seq 升序） |

细节：

- `POST /v1/tasks`：无 `conversation_id` → 新建会话（title=请求截断）并把任务挂入；有 → 校验会话存在，不存在返回 404。响应始终返回 `conversation_id`。
- `last_status` 取该会话**最新任务**的状态；列表按 `updated_at` 倒序。
- `TaskSnapshot.task` 增加 `conversation_id` 字段；`TaskSnapshot` 增加 `last_seq: int`（该任务事件流最大 seq，无事件为 0），供前端订阅时 `?after_seq=` 使用（既有接口字段扩展，向后兼容）。
- `GET /v1/tasks/{id}/checkpoints` 校验任务存在，404 语义与 tasks 接口一致。

### 4.4 会话上下文构造

新增 `core/conversations.py`：

- `build_conversation_context(db, conversation_id, exclude_task_id, *, max_tasks=5, max_chars=4000) -> str | None`
  - 取同会话中**终态**且非当前任务的任务，按 `created_at` 取最近 `max_tasks` 条；
  - 每条渲染为：
    ```
    User: <request>
    Result: <completed 节点输出文本，按节点 id 顺序拼接，单任务截断 1200 字符>
    ```
  - 整体超过 `max_chars` 时从最早的任务开始丢弃；
  - 无历史返回 `None`。
- 输出文本提取：节点 `output.artifacts[*].text` 拼接（`output` 为 `{"artifacts": [{"id","name","text"}]}`）。
- 接入点：`Orchestrator._initial_plan` 调用 `Planner.plan(request, context=conversation_context)`；仅当任务有 `conversation_id` 且存在历史时传入。重规划路径不变（已有自身 context）。

### 4.5 后端错误处理

- 会话不存在：`404 {"detail": "conversation not found: <id>"}`。
- 创建任务时数据库/规划错误沿用现有行为（任务落 `error` 事件并 `task.failed`）。
- 所有新增接口保持现有 JSON 错误格式（FastAPI `detail`）。

## 5. 前端设计

### 5.1 工程结构与技术栈

```
frontend/
  package.json  vite.config.ts  tsconfig.json  index.html
  src/
    main.tsx  App.tsx  styles.css
    api/client.ts          # REST 封装（fetch + 统一错误）
    api/events.ts          # EventSource 封装（命名事件、重连状态）
    lib/taskView.ts        # TaskView 类型 + 事件增量折叠
    lib/timeline.ts        # 纯函数：TaskView[] → ChatItem[]
    hooks/useConversations.ts
    hooks/useConversation.ts   # 详情加载 + 活跃任务 SSE 订阅 + 兜底刷新
    components/
      Sidebar.tsx ConversationList.tsx
      Thread.tsx UserBubble.tsx NodeCard.tsx InterventionCard.tsx
      ResultBubble.tsx SystemNote.tsx PlanSummary.tsx
      StatusBar.tsx Composer.tsx RollbackDialog.tsx ErrorBanner.tsx
  tests/  setup.ts  timeline.test.ts  taskView.test.ts  client.test.ts  components.test.tsx
```

- 依赖：`react`、`react-dom`；开发依赖：`vite`、`@vitejs/plugin-react`、`typescript`、`vitest`、`jsdom`、`@testing-library/react`、`@testing-library/user-event`。
- 状态：`useReducer` + 自定义 hooks，不引入状态管理库；服务端为唯一事实源。
- 路由：无 router。`?c=<conversation_id>` 决定当前会话；`pushState`/`popstate` 同步；无参数时为「新对话」空态。

### 5.2 数据流

1. 启动：`GET /v1/conversations` → 侧边栏。
2. 打开会话：`GET /v1/conversations/{cid}` → `TaskSnapshot[]`（含 `last_seq`）。
3. 对每个 **终态任务**直接用快照构造 `TaskView`；对**活跃任务**从快照取任务头信息，节点/干预等细节以 SSE 事件折叠为准，并以 `?after_seq=<last_seq>` 订阅 `GET /v1/tasks/{task_id}/events`：
   - 事件 → `taskView` 增量更新（节点状态、输出、干预、回退等）；`TaskView` 记录 `lastSeq`，忽略 `seq <= lastSeq` 的重复事件（SSE 的 `id` 即 seq）；
   - 收到终态事件（`task.completed` / `task.failed` / `state_changed → canceled`）→ 拉取一次 `GET /v1/tasks/{id}` 快照替换，作为兜底；
   - 回退 `rollback.performed` 后同样拉取快照兜底（任务可能继续运行）。
4. 渲染：`timeline(taskViews)` 输出 `ChatItem[]` → `Thread` 渲染。
5. 发送：`POST /v1/tasks {request, conversation_id?}` → 立即插入本地用户项并订阅 SSE；无 `conversation_id` 时用响应中的 `conversation_id` 更新 URL 与侧边栏，随后刷新会话列表。
6. 干预答复：`POST .../interventions/{iid}` → 不本地乐观更新，等事件回显。
7. 回退/重试/取消：调用对应接口；由事件与快照刷新（重试是节点级，回退见上）。

### 5.3 视图模型

```ts
type TaskView = {
  task: { id; conversationId; status; request; createdAt; updatedAt };
  nodes: NodeView[];              // 来自快照或事件增量
  interventions: Intervention[];
  rollback?: { checkpointId; resetNodeIds };
  connection: "live" | "reconnecting" | "closed";
};

type NodeView = {
  id; name; agentName?; status; attempt;
  inputText?;                     // input.text
  outputText?; error?;            // artifacts 文本拼接
  retryScheduled?: number;
};

type ChatItem =
  | { kind: "user"; taskId; text; at }
  | { kind: "plan"; taskId; rationale; nodeCount; nodes: {name;agentName}[] }
  | { kind: "node"; taskId; node: NodeView }
  | { kind: "intervention"; taskId; intervention }
  | { kind: "note"; taskId; level: "info"|"warn"; text }   // 重试/回退/取消等
  | { kind: "result"; taskId; status: "completed"|"failed"|"canceled"; text?; error? };
```

- `taskView.ts` 提供 `applyEvent(view, event)`（纯函数，忽略 `seq <= lastSeq`）与 `fromSnapshot(snapshot)`。
- `timeline.ts` 提供 `buildTimeline(views: TaskView[]): ChatItem[]`：按任务顺序输出 user → plan → node* → intervention* → note* → result；节点按当前计划 `plan.dag.nodes` 数组顺序（规划器给定），无计划时按 node id 升序。

### 5.4 事件 → 视图映射（SSE `event:` 名与载荷）

| 事件 | 视图效果 |
|---|---|
| `task.created` | 创建/更新任务头（request、conversation_id） |
| `plan.created` | 设置计划摘要与节点列表（input、agent） |
| `plan.superseded` | 旧节点标记为已被替代（灰显），保留节点历史 |
| `node.dispatch.intent` | attempt 更新，状态标记为派发中 |
| `node.dispatched` | 状态 dispatched |
| `node.state_changed` | 状态/结束时间更新 |
| `node.output` | outputText 更新 |
| `node.retry.scheduled` | note「第 N 次重试将在 Xs 后」，并记录 attempt |
| `node.invalidated` / `node.cancel.sent` | note 提示 |
| `intervention.requested` | 追加/更新干预卡（pending） |
| `intervention.resolved` | 干预卡显示答案与答复人 |
| `checkpoint.created` | 静默记录（供回退对话框使用，不渲染气泡） |
| `rollback.performed` | note「已回退到 checkpoint xxx，重置 N 个节点」，节点状态重置 |
| `error` | note（warn）或节点错误信息 |
| `task.state_changed` / `task.completed` / `task.failed` | 任务状态；终态触发快照兜底 + 连接关闭 |

注：前端只实现上述**展示级**折叠；服务端快照始终可覆盖本地视图。

### 5.5 交互设计

**侧边栏**（宽 260px）

- 「＋ 新对话」按钮：清空当前选择（`/`）。
- 会话列表项：标题（省略号截断）、最后状态色点、相对时间；当前项高亮；按 `updated_at` 倒序。
- 空态：「还没有对话，输入一条消息开始」。

**顶部状态条（StatusBar）**

- 会话标题 + 当前（最新）任务状态徽章。
- 操作按钮（按状态启用）：
  - 取消：任务为 running/planning/awaiting_input 时可用；二次确认。
  - 回退：存在 checkpoint 时可用，打开 `RollbackDialog`。
- 失败节点重试入口在对应的节点卡片上（`NodeCard`），不在状态条重复提供。

**回退对话框**

1. 拉取 `GET /v1/tasks/{id}/checkpoints`；
2. 选择 checkpoint → `POST rollback {mode:"dry_run"}` → 展示「将重置的节点」列表与数量；
3. 「确认回退」→ `POST rollback {mode:"restart"}` → 关闭对话框，等待事件/快照刷新。

**干预卡**

- 展示问题文本（question 载荷已有文案）、来源 agent、截止时间（若有时）。
- 提供 textarea + 「提交」；提交后按钮禁用，显示等待回显。
- auto_llm/peer_agent 策略下通常不会出现 pending 干预；只在 human 时需要人工。

**Composer**

- 多行输入（Enter 发送、Shift+Enter 换行）。
- 最新任务非终态时禁用并提示「任务执行中，可等待完成或处理上方干预」。
- 未注册 agent 时后端会返回任务失败；界面在失败结果气泡中提示「请先注册 Agent（POST /v1/agents）」。

**空态**

- 无会话选中：居中欢迎区 + 输入框。
- 加载中：骨架条。

### 5.6 视觉规范（浅色聊天风）

- CSS 变量定义色板：背景 `#f7f8fa`、面板白、主色蓝 `#2f6fed`、成功 `#16a34a`、失败 `#dc2626`、等待 `#d97706`；文本 `#111827`/`#6b7280`。
- 布局：左侧栏固定 + 主区弹性；消息区 `max-width: 840px` 居中；输入区吸底。
- 气泡：用户消息右对齐主色底白字圆角；节点卡左对齐白底描边，头部为 agent 名 + 状态圆点；系统 note 居中/左对齐小号灰字。
- 无动画依赖；状态变化用 150ms 过渡；仅用系统字体栈。

### 5.7 前端错误与边界

- REST 失败：顶部 `ErrorBanner` 显示消息与「重试」。
- SSE `onerror`：任务内连接状态置 `reconnecting`，状态条显示「连接中断，重连中…」；EventSource 自动携带 `Last-Event-ID`，服务端回放；重连成功置回 `live`。任务终态后主动关闭连接。
- 刷新：按 URL 重新加载会话与快照，活跃任务重新订阅（从头回放，服务端保证幂等展示）。前端不做事件去重以外的特殊处理，重复事件按 seq 幂等应用（`applyEvent` 幂等）。
- 会话不存在：显示「会话不存在」并提供返回新对话按钮。

## 6. 交付与运行

- `.gitignore` 增加：`frontend/node_modules/`、`frontend/dist/`。
- 开发：
  ```
  cd frontend && npm install && npm run dev      # :5173，代理 /v1 → :8080
  uv run agent-hub                               # 后端 :8080
  ```
- 生产：
  ```
  cd frontend && npm run build                   # 产出 frontend/dist
  uv run agent-hub                               # 根路径托管 dist
  ```
- FastAPI 装配：`create_app` 中在 API 路由注册后，若 `frontend/dist/index.html` 存在则 `app.mount("/", StaticFiles(directory=..., html=True))`；不存在时根路径返回 JSON 提示（`{"detail": "frontend not built"}`），不影响 API。
- README 增加「前端对话界面」章节（开发/构建/功能简介）。

## 7. 测试策略

后端（pytest，沿用现有假 agent/FakeLLM 设施）：

- `TaskService.create_pending_task`：新建会话（conversations 行 + 任务 conversation_id）；追问挂到既有会话；不存在会话报 404（API 层）。
- 投影：`TASK_CREATED` 带会话字段后 `fetch_conversation(s)` 正确；`rebuild()` 后 conversations 与任务关联恢复。
- 迁移：旧库（无 conversation_id 列）执行 `initialize()` 后可正常写入新任务。
- `build_conversation_context`：多任务截断、终态过滤、排除自身、>max_chars 丢弃最早。
- API：`GET /v1/conversations` 排序与 last_status；详情返回快照升序；checkpoints 列表 404 与正常。
- `Orchestrator._initial_plan` 传入会话上下文（FakeLLM 捕获 user prompt 断言包含历史）。

前端（Vitest + Testing Library）：

- `taskView`：从快照构造；对各类事件序列增量折叠（含终态、回退、重试、干预）。
- `timeline`：多任务排序、节点顺序、干预与 note 位置、结果项。
- `client`：REST 封装错误映射（mock fetch）。
- 组件冒烟：Composer 禁用态与提交、干预卡提交调用、节点卡重试/取消按钮回调。

不引入端到端浏览器测试（v1 用手动验收替代）。

## 8. 非目标与后续演进

- 运行中 steer、多模态消息、附件上传、Markdown 富渲染（v1 按纯文本 + 换行渲染）、深色主题、移动端优化、会话管理（重命名/删除）、DAG 可视化、Agent 管理页、鉴权。

## 9. 风险与对策

| 风险 | 对策 |
|---|---|
| SSE 事件与快照状态短暂不一致 | 终态/回退后强制快照兜底；展示层允许最终一致 |
| 会话上下文过长导致规划变慢/超限 | 固定 max_tasks=5、max_chars=4000，单任务结果截断 |
| 前端依赖 Node 工具链，CI/新环境需安装 | 文档明确步骤；Python 后端与测试不依赖前端构建 |
| 老库迁移 | `_migrate` ALTER + 默认 NULL；测试覆盖旧库场景 |
