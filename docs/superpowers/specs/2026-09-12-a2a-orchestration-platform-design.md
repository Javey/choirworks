# A2A 多 Agent 编排平台（ChoirWorks）设计文档

- 日期：2026-09-12
- 状态：设计已评审通过，待写实施计划
- 范围：MVP（单机、单进程）

## 1. 背景与目标

平台作为多 agent 架构的统一入口，自身不执行业务动作，职责是：

1. 理解用户的自然语言需求；
2. 用 LLM 将需求拆分为带依赖关系的任务 DAG；
3. 按依赖关系串行或并行调度 A2A subagent；
4. 处理 subagent 中途暂停（A2A `input-required`）与协助请求；
5. 协调多个 subagent 的输入输出流转；
6. 提供全流程可观测（SSE）、可恢复（断点续跑）、可回退（平台侧状态）能力。

平台不关心 subagent 的实现来源，只依赖标准 A2A 协议：Agent Card 发现、JSON-RPC/REST 绑定、Task 状态机、SSE 流式与 `input-required` 多轮交互。

## 2. 范围

### 2.1 MVP 范围内

- REST API + SSE 事件流作为唯一用户入口
- A2A client 与 Agent Card 注册表（静态注册，可选健康检查）
- LLM 动态规划器，输出结构化 DAG
- DAG 调度器：依赖就绪即并行（有并发上限），否则串行
- 协助策略引擎：`auto_llm` / `peer_agent` / `human`
- 追加式事件日志（SQLite），驱动 SSE、恢复、审计
- Checkpoint、崩溃后自动恢复、远程任务周期性对账（reconcile）
- 回退：平台侧状态回滚 + 对在途远程任务发取消信号（不等待、不补偿）
- 用 a2a-sdk 实现的假 agent 测试集，覆盖关键验收场景

### 2.2 明确非目标（本期不做）

- Web 控制台、CLI、IM 集成
- 分布式部署、消息队列、Worker 池、多租户
- 完整认证/授权（仅预留静态 API Key 中间件开关，默认关闭）
- L2 补偿执行（外部副作用撤销）
- subagent 自身的实现与托管、agent 间 P2P 直连
- 数据归档与清理策略

## 3. 需求决策记录

| 编号 | 决策 | 说明 |
|---|---|---|
| D1 | 平台只做编排入口 | subagent 均为标准 A2A agent，不关心来源与实现 |
| D2 | Python 技术栈 | 使用官方 `a2a-sdk` |
| D3 | 协助策略可配置 | 策略链 `auto_llm → peer_agent → human`，支持按 agent/skill/节点覆盖 |
| D4 | LLM 动态规划 + DAG 调度 | 失败或信息不足时可重规划 |
| D5 | MVP 单服务 | SQLite 持久化；SSE；不做分布式 |
| D6 | 回退仅平台侧 | 对远程 agent 只发取消信号，不做补偿，不保证撤销外部副作用 |

## 4. 总体架构

```
                 ┌────────────────────────────────────────────────┐
  用户/脚本 ──►  │  API 层 (FastAPI)                               │
                 │  /tasks  /interventions  /rollback  /agents    │
                 │  /tasks/{id}/events (SSE)                      │
                 └───────────────┬────────────────────────────────┘
                                 │
                 ┌───────────────▼────────────────────────────────┐
                 │  Orchestrator Core                             │
                 │  ┌──────────┐ ┌───────────┐ ┌───────────────┐  │
                 │  │ Planner  │ │ Scheduler │ │ Policy Engine │  │
                 │  └────┬─────┘ └─────┬─────┘ └──────┬────────┘  │
                 │       │             │              │           │
                 │  ┌────▼─────────────▼──────────────▼────────┐  │
                 │  │ Event Bus + 状态投影 (State Projection)   │  │
                 │  └────────────────────┬─────────────────────┘  │
                 └───────────────────────│────────────────────────┘
                                         │ 先追加、后分发
                 ┌───────────────────────▼────────────────────────┐
                 │ Event Store (SQLite WAL, 唯一事实源)            │
                 │ events / tasks / plans / nodes / interventions │
                 │ checkpoints / agent_registry                   │
                 └───────────────────────▲────────────────────────┘
                                         │
                 ┌───────────────────────┴────────────────────────┐
                 │ A2A Layer                                      │
                 │ Registry(Agent Card) / Client(Send/Stream/     │
                 │ Subscribe/Cancel) / Reconcile(对账)            │
                 └───────────────────────┬────────────────────────┘
                                         │ A2A v1.0 (JSON-RPC/REST + SSE)
                                 ┌───────▼────────┐
                                 │ 远程 A2A agents │
                                 └────────────────┘
```

### 4.1 组件职责

| 组件 | 职责 | 依赖 |
|---|---|---|
| API 层 | 提交任务、查询快照、SSE 订阅、干预答复、回退、agent 注册 | Orchestrator、Event Store |
| Planner | 输入用户需求 + agent 能力清单，输出经校验的 DAG（JSON） | LLM Client、Registry |
| Scheduler | 计算就绪节点、并行/串行派发、超时与重试、触发重规划 | A2A Client、Policy Engine、Event Store |
| Policy Engine | 解析协助策略优先级链，决定协助请求的处置方式 | 配置、Registry |
| A2A Client | 封装 SendMessage/SendStreamingMessage/SubscribeToTask/GetTask/CancelTask/ListTasks | a2a-sdk |
| Registry | 保存 Agent Card、能力索引、健康状态 | Event Store |
| Event Store | 追加事件、投影读模型、checkpoint 持久化 | SQLite |
| Recovery | 启动重建内存态、重新挂接远程任务、补建干预等待 | Event Store、A2A Client |
| Reconcile | 周期性对账远程任务状态与本地状态 | Event Store、A2A Client |
| SSE Hub | 回放 + 实时扇出事件，支持断线重连 | Event Store |

### 4.2 主流程

1. `POST /v1/tasks` 提交需求，追加 `task.created`，状态进入 `planning`。
2. Planner 读取 registry 的能力清单与用户需求，生成 DAG，经 schema 与业务规则校验后追加 `plan.created`，节点初始化为 `pending`。
3. Scheduler 循环：依赖全部 `completed` 的节点变为 `ready`；在并发上限内选取节点，**先写 `node.dispatch.intent`（写前日志）**，再调用 A2A Client 发送消息（`contextId = 编排任务 ID`，`messageId` 确定性生成）。
4. 远程任务事件（状态变化、artifact 分片）追加事件并扇出 SSE；节点状态与远程状态保持映射。
5. 远程任务进入 `input-required` 时，Policy Engine 按策略链处置：自动回答、路由给 peer agent、或转人工。人工答复通过 `POST /v1/tasks/{id}/interventions/{iid}` 回送（同一远程 task ID 追加消息）。
6. 所有节点 `completed` 且有输出 → 追加 `task.completed`；节点失败按重试 → 局部重规划 → 任务失败逐级处理。
7. 任意时刻崩溃：重启后 Recovery 重放事件重建状态，重新挂接在途远程任务，恢复调度循环。
8. 任意 checkpoint 可回退：平台状态回滚 + 在途远程任务发取消信号 + 生成新 plan version。

## 5. 核心机制

### 5.1 事件模型与 SSE

**事件结构**

```json
{
  "seq": 42,
  "task_id": "01J...",
  "type": "node.state_changed",
  "payload": {"node_id": "n3", "from": "working", "to": "input_required"},
  "created_at": "2026-09-12T10:00:00Z"
}
```

- `seq` 为 per-task 单调递增整数，实现上可用全局自增主键 + task 过滤。
- **先持久化、后分发**：事件先写入 Event Store，再进入内存 Event Bus。SSE 与恢复共用同一份日志。
- 事件类型（MVP 全集）：
  - `task.created` / `task.state_changed` / `task.completed` / `task.failed`
  - `plan.created` / `plan.superseded`
  - `node.dispatch.intent` / `node.dispatched` / `node.state_changed` / `node.artifact` / `node.output`
  - `node.retry.scheduled` / `node.invalidated` / `node.cancel.sent`
  - `intervention.requested` / `intervention.resolved`
  - `checkpoint.created` / `rollback.performed`
  - `error`（携带错误码与上下文）

**SSE 端点语义**

- `GET /v1/tasks/{task_id}/events`
- 支持 `Last-Event-ID` 头与 `?after_seq=` 查询参数；连接时先从 Event Store 回放 `seq > after_seq` 的事件，再切入实时订阅，按 `seq` 去重。
- SSE `id:` = `seq`，`event:` = 事件类型，`data:` = payload JSON。
- 每 15s 发送心跳注释行保活。
- 慢消费者：单订阅者使用有界队列，溢出则断开连接，客户端凭 `Last-Event-ID` 重连回放，天然不丢事件。
- 多订阅者：同一 task 多个连接各自独立游标；MVP 使用进程内 Event Bus 扇出。

### 5.2 HITL（人工介入）

**触发源**

1. 远程任务进入 `input-required`，携带提问消息；
2. 节点在计划中被标记 `requires_approval`（如高风险动作）；
3. 规划前需求澄清（Planner 判定信息不足，向用户提问）。

**策略解析优先级**（高到低）

```
节点 policy_override > 配置 overrides（按声明顺序，首个命中生效）
> 任务提交时策略 > 全局默认
```

**策略动作**

| 策略 | 行为 | 审计 |
|---|---|---|
| `auto_llm` | 编排器将原始需求、上游节点输出、提问组装为 prompt，请 LLM 作答，回送 A2A 消息 | 记录问答与模型名 |
| `peer_agent` | 按 skill 匹配（LLM 辅助选择）另一个 agent，派发子任务，结果回送原节点 | 记录路由决策与子任务 ID |
| `human` | 追加 `intervention.requested`，节点保持 `input_required`，编排任务进入 `awaiting_input`，等待 REST 答复 | 记录答复人与时间 |

**人工流程**

1. `intervention.requested` 事件经 SSE 推送，包含 `intervention_id`、问题内容、可选答案提示；
2. 用户/操作员 `POST /v1/tasks/{id}/interventions/{iid}`，body 为 A2A Message 的 parts（text/data/file 引用）；
3. 平台以同一远程 task ID 追加消息，节点回到 `working`，追加 `intervention.resolved`；
4. 若远程任务已不可用（被取消/过期），标记干预失效并按节点失败策略处理。

**超时**：按策略配置 `on_timeout`：`escalate`（重复提醒）| `auto`（降级为 auto_llm）| `fail`。默认 `escalate`，绝不静默丢弃。

### 5.3 断点恢复

**写前日志与幂等**

- 派发前先写 `node.dispatch.intent`，包含确定性幂等键：
  - `messageId = "{task_id}:{node_id}:{attempt}"`
  - `contextId = task_id`
- 崩溃后若「已发出、未记录远程 task ID」：用 `ListTasks(contextId=task_id)` 对账，按 `messageId`/时间窗口找到孤儿任务并挂接，避免重复派发。
- 远程 agent 若支持 `messageId` 幂等语义则天然去重；不支持时以对账结果为准。

**Checkpoint**

- 创建时机：每个节点 `completed` 后（MVP 固定策略）、以及人工干预答复后。
- 内容：`{plan_version, 已完成节点前沿, 各节点 artifact 引用, 最后一个事件 seq}`。
- 只引用 artifact，不复制大对象。

**启动恢复算法**

1. 打开 SQLite，重放未归档事件，校验投影一致性（不一致则以事件重建投影）。
2. 找出所有非终态编排任务：
   - 任务状态 `planning` → 若存在未决澄清干预则保持等待，否则重新调用 Planner；
   - 节点状态 `dispatched/working` → `GetTask`；存在则 `SubscribeToTask` 重新挂接，不存在则按 `ListTasks(contextId)` 对账，仍找不到则标记节点失败；
   - 节点状态 `input_required` → 若已有未决干预则重建等待，否则按策略重新处置；
   - 任务状态 `awaiting_input` 且存在未决人工干预 → 重建等待，并重发 `intervention.requested`（`replayed=true`）。
3. 重建 Scheduler 循环并续跑。

**周期性 reconcile**

- 默认每 30s（可配置）对非终态节点调用 `GetTask`/`ListTasks`，修正本地与远程状态漂移；发现远程终态未被捕获时补写事件。

### 5.4 回退（Revert）

**语义**：平台只回滚自己的状态与推导结果；对远程 agent 仅发送取消信号，不执行补偿，不保证撤销已发生的外部副作用。

**`POST /v1/tasks/{task_id}/rollback`**

```json
{"checkpoint_id": "ck_123", "mode": "restart"}
```

- `mode = dry_run`：只计算影响面并返回（将作废的节点、将取消的在途任务、将失效的 artifact），不产生副作用；
- `mode = restart`：
  1. 对 checkpoint 之后处于 `dispatched/working/input_required` 的远程任务发送 `CancelTask`（fire-and-forget，写入 `node.cancel.sent`，不等待结果）；
  2. 将 checkpoint 之后的节点标记 `invalidated`，其 artifact 标记 `stale`（保留审计，不物理删除）；
  3. 相关未决干预标记失效；
  4. 恢复 checkpoint 的前沿与计划版本，追加 `checkpoint.created`（回退后新锚点）与 `rollback.performed`；
  5. 按配置从该点**续跑**或**重新规划**（产生新 plan version，追加 `plan.superseded`）。

**单节点重试**：`POST /v1/tasks/{id}/nodes/{node_id}/retry` 仅重置该节点及其下游（下游需重算时失效），用于远程瞬时故障。

**边界说明**：节点动作应尽量幂等；平台通过 attempt 计数与写前日志保证「至少一次派发、不重复记录」，但不保证外部副作用恰好一次。

## 6. 数据模型

SQLite（WAL 模式），事件表为唯一事实源；其余表是可重建投影，读写在同一事务内完成以保证一致性。

```sql
CREATE TABLE events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT NOT NULL,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,          -- JSON
  created_at TEXT NOT NULL
);
CREATE INDEX idx_events_task_seq ON events(task_id, seq);

CREATE TABLE orchestration_tasks (
  id           TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  request      TEXT NOT NULL,        -- 原始需求 JSON
  policy       TEXT,                 -- 任务级策略覆盖 JSON
  plan_version INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE TABLE plans (
  id         TEXT PRIMARY KEY,
  task_id    TEXT NOT NULL REFERENCES orchestration_tasks(id),
  version    INTEGER NOT NULL,
  dag        TEXT NOT NULL,          -- JSON
  rationale  TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, version)
);

CREATE TABLE nodes (
  id             TEXT PRIMARY KEY,
  task_id        TEXT NOT NULL,
  plan_id        TEXT NOT NULL REFERENCES plans(id),
  name           TEXT NOT NULL,
  agent_url      TEXT,
  skill_id       TEXT,
  deps           TEXT NOT NULL,      -- JSON 数组
  input          TEXT,               -- JSON
  status         TEXT NOT NULL,
  attempt        INTEGER NOT NULL DEFAULT 0,
  requires_approval INTEGER NOT NULL DEFAULT 0,
  a2a_task_id    TEXT,
  a2a_context_id TEXT,
  output         TEXT,               -- JSON（artifact 引用 + 摘要）
  error          TEXT,
  started_at     TEXT,
  ended_at       TEXT
);
CREATE INDEX idx_nodes_task ON nodes(task_id);
CREATE INDEX idx_nodes_a2a ON nodes(a2a_task_id);

CREATE TABLE interventions (
  id          TEXT PRIMARY KEY,
  task_id     TEXT NOT NULL,
  node_id     TEXT,
  source      TEXT NOT NULL,         -- remote_input_required | approval | clarification
  policy      TEXT NOT NULL,         -- auto_llm | peer_agent | human
  question    TEXT NOT NULL,         -- JSON（A2A parts）
  answer      TEXT,                  -- JSON（A2A parts）
  responder   TEXT,                  -- auto | agent_url | user id
  status      TEXT NOT NULL,         -- pending | resolved | expired | invalidated
  deadline_at TEXT,
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX idx_interventions_task_status ON interventions(task_id, status);

CREATE TABLE checkpoints (
  id           TEXT PRIMARY KEY,
  task_id      TEXT NOT NULL,
  seq          INTEGER NOT NULL,     -- 对应事件位置
  plan_version INTEGER NOT NULL,
  frontier     TEXT NOT NULL,        -- JSON：已完成节点集合
  artifacts    TEXT NOT NULL,        -- JSON：节点 -> artifact 引用
  created_at   TEXT NOT NULL
);

CREATE TABLE agent_registry (
  id         TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  card_url   TEXT NOT NULL,
  card       TEXT NOT NULL,          -- Agent Card JSON 快照
  health     TEXT NOT NULL DEFAULT 'unknown',
  last_seen  TEXT,
  created_at TEXT NOT NULL
);
```

- 投影重建：清空投影表后按 `events.seq` 顺序重放即可；恢复时校验 `orchestration_tasks.updated_at` 对应事件位置，必要时重建。
- 非当前 plan 的节点保留用于审计与回退锚点；查询默认按当前 plan_version 过滤。
- MVP 保留全部事件；归档策略为非目标。

## 7. 状态机

**编排任务**

| 当前 | 事件 | 下一状态 |
|---|---|---|
| pending | task.created | planning |
| planning | plan.created | running |
| planning | 规划失败（重试耗尽） | failed |
| planning | 创建需求澄清干预 | awaiting_input |
| running | 存在 `input_required` 或待人工答复的节点 | awaiting_input |
| awaiting_input | 干预全部解决 | 恢复到进入 `awaiting_input` 前的状态（planning 或 running） |
| running | 全部节点 completed | completed |
| running | 不可恢复失败 | failed |
| 任意非终态 | 用户 cancel | canceled |

**节点**

| 当前 | 触发 | 下一状态 |
|---|---|---|
| pending | 依赖全部 completed 且入选调度 | ready |
| ready | dispatch.intent 已写、消息已发 | dispatched |
| dispatched | 远程返回 working/收到状态事件 | working |
| working | 远程 artifact 事件 | working（追加输出） |
| working | 远程 input-required | input_required |
| input_required | 策略处置/人工答复完成 | working |
| working | 远程 completed | completed |
| working | 远程 failed/超时且重试耗尽 | failed |
| pending/ready/dispatched/working/input_required | rollback 到更早 checkpoint | invalidated |
| failed | 单节点 retry | ready |

## 8. API 设计

统一前缀 `/v1`，错误响应 `{code, message, details}`。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交需求：`{request, policy?}` → `{task_id, status}` |
| GET | `/tasks/{id}` | 快照：任务 + 当前 plan + 节点 + 未决干预 + 最近 checkpoint |
| GET | `/tasks/{id}/events` | SSE 事件流（支持 `Last-Event-ID` / `after_seq`） |
| GET | `/tasks/{id}/interventions` | 干预列表（`?status=pending`） |
| POST | `/tasks/{id}/interventions/{iid}` | 答复干预：`{parts:[...], responder?}` |
| GET | `/tasks/{id}/checkpoints` | checkpoint 列表 |
| POST | `/tasks/{id}/rollback` | `{checkpoint_id, mode: restart\|dry_run}` |
| POST | `/tasks/{id}/nodes/{node_id}/retry` | 单节点重试 |
| POST | `/tasks/{id}/cancel` | 取消整个任务（含对所有在途节点发取消信号） |
| GET | `/agents` | 已注册 agent 列表 |
| POST | `/agents` | 注册 agent：`{name, card_url}` 或 `{card_json}` |
| DELETE | `/agents/{id}` | 注销 |
| POST | `/agents/{id}/refresh` | 重新拉取 Agent Card |

SSE 示例：

```
id: 42
event: node.state_changed
data: {"node_id":"n3","from":"working","to":"input_required","intervention_id":"iv_9"}

id: 43
event: intervention.requested
data: {"intervention_id":"iv_9","node_id":"n3","policy":"human","question":{"parts":[{"text":"请提供 API 凭据环境"}]}}
```

## 9. Planner 与 DAG 规范

**输入**：用户需求、agent registry 能力清单（name/skills/描述）、历史干预摘要、重规划原因（如有）。

**输出 schema**（Pydantic 校验，带重试）：

```json
{
  "rationale": "为什么这样拆分",
  "nodes": [
    {
      "id": "n1",
      "title": "检索资料",
      "agent_name": "search-agent",
      "skill_id": "web_search",
      "input": {"query": "..."},
      "deps": [],
      "requires_approval": false,
      "policy_override": null
    }
  ]
}
```

**校验规则**

- 节点 ID 唯一；`deps` 必须引用已存在节点；检测环（拓扑排序）；
- `agent_name` 和 `skill_id` 必须存在于 registry 能力清单，禁止编造；
- 单计划节点数上限（默认 20）、单层并发上限（默认 5）；
- 校验失败将错误反馈给 LLM 重试（默认 2 次）；仍失败则退化为单节点计划或任务失败。

**重规划触发**：节点不可恢复失败、agent 下线、人工干预改变了需求、回退时选择 `replan`。每次重规划产生新 plan version，旧版本保留；已完成且不受影响的节点沿用结果。

## 10. 技术栈与依赖

| 用途 | 选型 | 备注 |
|---|---|---|
| 语言 | Python 3.12 | |
| A2A 协议 | 官方 `a2a-sdk` | 锁定与 A2A v1.0 兼容的版本 |
| Web 框架 | FastAPI + uvicorn | |
| SSE | `sse-starlette` | |
| 数据校验 | Pydantic v2 | 事件/规划/API 全量模型 |
| LLM 接入 | LiteLLM + `instructor` | 结构化输出，provider 无关 |
| 存储 | SQLite WAL + `aiosqlite` | 单写者串行化事件追加 |
| 日志 | `structlog` | JSON 结构化 |
| 测试 | pytest + pytest-asyncio + httpx | 假 agent 用 a2a-sdk 搭建 |

## 11. 模块划分

```
src/choirworks/
  main.py                 # FastAPI 应用装配、生命周期（启动恢复）
  config.py               # 配置模型（YAML + 环境变量）
  api/
    tasks.py              # 任务提交/查询/取消
    interventions.py      # HITL 答复
    rollback.py           # 回退与 retry
    agents.py             # registry 管理
    sse.py                # SSE 端点与 Event Bus 桥接
  core/
    orchestrator.py       # Scheduler 主循环、并发控制、重试
    planner.py            # LLM 规划、schema 校验、重规划
    policy.py             # 策略解析与 auto/peer/human 处置
    state.py              # 状态机定义与投影
    events.py             # 事件类型、Event Bus、SSE Hub
    recovery.py           # 启动恢复与远程重挂接
    rollback.py           # dry_run/restart、失效传播
  a2a/
    registry.py           # Agent Card 注册与能力索引
    client.py             # a2a-sdk 封装（Send/Stream/Subscribe/Get/List/Cancel）
    reconcile.py          # 周期性对账
  store/
    db.py                 # 连接、迁移、事务助手
    event_store.py        # 追加写 + 回放
    projections.py        # 投影更新与重建
  models/
    event.py  task.py  node.py  plan.py  intervention.py  checkpoint.py
tests/
  unit/                   # schema 校验、策略解析、状态机、投影重放
  integration/            # 假 agent 全链路场景
  fake_agents/            # 可控行为的 A2A 测试 agent
```

## 12. 配置示例

```yaml
server:
  host: 0.0.0.0
  port: 8080
  api_key: null            # 预留，null 表示不启用

llm:
  planner_model: "openai/gpt-4.1"     # LiteLLM 格式，可替换
  assist_model: "openai/gpt-4.1-mini"
  timeout_seconds: 60
  max_plan_retries: 2

scheduler:
  max_parallel_nodes: 5
  node_timeout_seconds: 600
  max_node_attempts: 2
  replan_on_failure: true

policies:
  default: auto_llm
  on_timeout: escalate
  timeout_seconds: 900
  overrides:
    - agent_name: "payment-agent"
      policy: human
    - skill_id: "deploy_prod"
      policy: human

recovery:
  reconcile_interval_seconds: 30
  replay_on_startup: true

store:
  db_path: "./data/choirworks.db"
```

## 13. 错误处理

| 场景 | 处理 |
|---|---|
| 远程调用瞬时失败 | 指数退避重试，计入 `attempt`，上限 `max_node_attempts` |
| 节点超时 | 重试；耗尽后标记失败，触发局部重规划或任务失败 |
| agent 不可达/下线 | 派发前跳过并重规划；执行中失败按节点失败处理 |
| 流式断连 | `SubscribeToTask` 重试，失败降级 `GetTask` 轮询对账 |
| Planner 输出非法 | 校验错误反馈给 LLM 重试；耗尽后退化单节点或失败 |
| 事件写库失败 | 快速失败（事实源不可用则停止调度），不产生未记录副作用 |
| SSE 慢消费者 | 有界队列溢出即断开，客户端凭 seq 回放 |
| 干预答复时远程任务已终态 | 干预标记 `invalidated`，追加 `error` 事件，按节点策略处理 |

## 14. 测试策略

**单元**：DAG schema 与环检测、策略优先级解析、状态机非法迁移、事件投影重放确定性（同事件序列 → 同投影）、回退影响面计算。

**集成（假 agent，行为可控）**：

1. 单节点成功链路（提交 → SSE 事件序列 → completed）；
2. 并行扇出：多依赖节点同时派发、合并输出；
3. `input-required` + `auto_llm` 自动回答闭环；
4. `input-required` + `human`：SSE 收到请求、REST 答复后远程续跑完成；
5. `peer_agent`：协助请求路由到第二个假 agent，结果回送；
6. 崩溃恢复：在派发后/等待人工时 kill 进程，重启后任务续跑到完成，无重复派发；
7. 写前日志孤儿对账：模拟发出请求未记录 task ID，靠 `ListTasks(contextId)` 找回；
8. 回退：`dry_run` 只报告；`restart` 后远端收到 CancelTask 信号、下游失效、续跑成功；
9. SSE 重连：`Last-Event-ID` 回放不重不漏；
10. 单节点 retry 与失败重规划。

**验收口径**：场景 1-10 全部通过；崩溃恢复场景中「同一 messageId 不被派发两次」；SSE 事件序列与事件表一致。

## 15. 里程碑

| 里程碑 | 交付物 | 验证 |
|---|---|---|
| M1 骨架 | FastAPI + Event Store + A2A client + registry + 假 agent | 单节点手动派发成功，事件正确落库 |
| M2 规划与调度 | Planner + DAG 并行/串行 | 集成场景 1-2 通过 |
| M3 SSE | 回放、重连、心跳、慢消费者处理 | 集成场景 9 通过 |
| M4 HITL | 策略链 + 干预 API + 超时 | 集成场景 3-5 通过 |
| M5 恢复 | checkpoint + 启动恢复 + reconcile | 集成场景 6-7 通过 |
| M6 回退 | rollback + retry + dry_run | 集成场景 8、10 通过 |
| M7 收尾 | 演示脚本、README、配置样例 | 全量测试通过 |

## 16. 风险与缓解

| 风险 | 缓解 |
|---|---|
| a2a-sdk 版本演进快 | 锁定版本；协议访问封装在 `a2a/client.py` 单点，便于升级 |
| 「发出未记录」的孤儿远程任务 | 确定性 `contextId`/`messageId` + `ListTasks` 对账 + 幂等要求 |
| LLM 规划质量不稳定 | 严格 schema 校验 + 重试反馈 + 节点/并发上限 + 重规划 |
| 远程 agent 不尊重 CancelTask | 取消仅为信号且 fire-and-forget；平台侧照常失效，不阻塞回退 |
| SQLite 单写者瓶颈 | MVP 规模（任务数百级）足够；预留 repository 接口便于换 Postgres |
| SSE 仅进程内 | 单机 MVP 足够；升级路径为 Redis Pub/Sub + 多实例 |

## 17. 后续演进（非本期）

1. 调度内核迁移 Temporal（workflow = 编排任务，signal = 人工答复），协议层代码不动；
2. SSE 跨实例（Redis Pub/Sub）、Postgres 存储、多租户与认证授权；
3. L2 补偿约定：Agent Card 声明 compensating skill，回退时按逆拓扑调用；
4. Web 控制台（DAG 可视化、干预工作台）、IM 集成；
5. 事件归档与保留策略。
