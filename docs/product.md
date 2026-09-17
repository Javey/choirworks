# ChoirWorks 产品文档

> [!WARNING]
> **本仓库目前仍处于概念验证阶段，不具备任何实际使用价值。**
> 所有设计、接口与行为均可能随时大幅变更乃至推翻重做，不承诺任何形式的稳定性、
> 兼容性与安全性，请勿用于生产环境或任何真实业务。

## 一句话定位

ChoirWorks 是一个多 Agent 协作群聊平台 — 人类像 CEO 一样提出目标，编排器作为"群管理员"拆解任务、把专业 Agent 拉进群、分派任务，群内成员可以互相 @、引用、求助，平台负责编排调度但不执行具体业务。

## 与传统 Agent 框架的区别

| | 传统 Agent 框架（如 ADK） | ChoirWorks |
|---|---|---|
| **编排器角色** | 专家执行者，LLM 自主调工具 | 群管理员，拉人、分派、转发 |
| **对结果的态度** | 检查、推理、决定下一步 | 不检查、不汇总结果，各专家产出即最终交付 |
| **动态规划来源** | LLM 自己推理 | subagent 求助（`input_required`）→ 编排器响应 |
| **执行模式** | 阻塞式 — LLM 循环直到完成 | 非阻塞 — 有人工作、有人聊天 |
| **Agent 关系** | 层级式（parent → child） | 群聊式（扁平、可互相对话） |
| **群的生命周期** | 单次任务 | 持续存在，直到所有工作结束 |

## 核心角色

| 角色 | 身份 | 职责 |
|------|------|------|
| **CEO（人类用户）** | 群的创建者 | 提出目标、@指派 Agent、引用追问、中途插话/打断 |
| **编排器（assistant）** | 群管理员 | 分析需求、制定计划、拉人入群、分派任务、转发消息 |
| **Subagent** | 群成员/专家 | 执行专业任务、产出结果、向编排器求助（`input_required`）、互相 @ |

### 编排器的职责边界

| 做 | 不做 |
|---|---|
| 分析需求 → 制定计划 | 检查产出质量 |
| 拉人入群 | 汇总/总结结果 |
| 分派任务 | 评判结果好坏 |
| 转发消息（@、求助） | 代替专家做决策 |
| 处理求助（拉新人或转人类） | 执行具体业务动作 |

编排器连结果都不用汇总 — 各专家的产出本身就是最好的结果，用户直接看各 Agent 的产出即可。

## 群的生命周期

```
CEO 发起目标
  ↓
编排器分析需求 → 制定 DAG 计划
  ↓
编排器拉入相关 Agent（入群播报）
  ↓
编排器分派任务（@agent 请开始...）
  ↓
┌───────────────────────────────────────────────┐
│ 群内并行活动（非阻塞）：                         │
│                                               │
│ • Agent A 正在工作（流式输出）                   │
│ • Agent B 已完成 → 可继续对话                   │
│ • CEO 发新消息 → 编排器分析是否需要新计划          │
│ • Agent A 求助 → 编排器拉入新 Agent 或转给人类    │
│ • Agent B @Agent A → 编排器转发                  │
│ • 编排器派发下一个就绪节点                        │
└───────────────────────────────────────────────┘
  ↓
所有任务完成 → 各 Agent 产出已在群聊中展示
  ↓
群继续存在（CEO 可以发新消息，或开始新计划）
```

群没有显式的"结束"。所有节点完成并不意味着群关闭 — 用户随时可以发新消息、开始新计划。只有当所有工作都结束时，群自然归于安静。

> 实现说明：一个 A2A Task 终态后不再接收新消息；在同一 `contextId`（群）下发送新消息
> 会创建新的接续 Task，编排器会带上已完成产出的上下文重新规划。

## 编排器的核心能力

### 1. 规划

用户发消息后，编排器调用 LLM 将需求拆解为 DAG（有向无环图）：
- 每个节点分配给一个已注册的 Agent
- 节点间用 `deps` 表达依赖关系
- 独立节点并行执行，有依赖的按序执行
- LLM 的推理过程作为 assistant 消息发到群里，让用户看到编排器的思考

### 2. 非阻塞派发

编排器派发任务后**不等待完成**：
- Agent 在后台工作，流式输出实时出现在群聊中
- 已完成的 Agent 可以继续参与对话
- 正在工作的 Agent 不阻塞其他 Agent 或用户
- 节点完成后自动触发下游就绪节点

### 3. 动态组队

群成员不是固定的，编排器可以在运行中动态拉入新 Agent：

| 来源 | 触发条件 | 示例 |
|------|---------|------|
| 用户 @mention | 用户消息中 @ 的新 Agent | "请 @analyst 也来看看" |
| subagent 求助 | Agent 发出 `input_required` | researcher: "我需要 analyst 帮忙" |
| LLM 规划 | 计划中需要的 Agent | 计划自动包含 researcher + writer |

编排器自己不会主动将新 Agent 加入群，但具备这个能力 — 由求助触发。

### 4. 消息路由

编排器是群内消息的中转站：

| 消息来源 | 路由规则 |
|---------|---------|
| 用户消息 | 分析是否需要新计划、是否影响正在工作的 Agent、是否只是普通对话 |
| Agent 产出 | 作为群消息显示，并触发下游就绪节点 |
| Agent @另一 Agent | 编排器仲裁：复用已有产出、创建协助节点、或转给人类 |
| Agent 求助 | 编排器由大模型分析求助内容，决定转交另一个 Agent 或转给人类 |

### 5. 求助处理（`input_required`）

当 subagent 发出 `input_required`（求助信号）时，编排器由大模型统一分析求助内容和群上下文，
做出二选一决策：

```
subagent: "我需要 analyst 帮忙确认技术细节"
  ↓
编排器分析求助内容 + 群上下文
  ↓
大模型决策：
  • 转交另一个 agent → 选择合适的 Agent，拉入群，创建协助节点，完成后回填续跑
  • 转交人类 → 提问等待用户答复，答复后续跑
  ↓
LLM 不可用时 → 降级转人工
```

### 6. 中途插话与打断

用户在 Agent 工作时可以发新消息：

| 场景 | 处理方式 |
|------|---------|
| 新消息与正在工作的 Agent 无关 | 立即处理（可能触发新计划） |
| 新消息需要正在工作的 Agent 变更 | 通知用户：是否打断？ |
| 新消息引用已完成的结果 | 创建 follow-up 任务 |
| 新消息引用在途的工作 | 排队，工作完成后投递 |
| 显式打断 | 取消当前工作，转入新任务 |

## 群聊交互模型

### 消息类型

| 类型 | 发送者 | 示例 |
|------|--------|------|
| 用户消息 | CEO | "帮我调研并写报告" |
| assistant 思考 | 编排器 | "收到请求，分析需求。计划：@researcher 调研 → @writer 撰写" |
| assistant 播报 | 编排器 | "任务已拆解：- @researcher 负责 调研 …" |
| Agent 产出 | subagent | "调研结果：关于「你好」的模拟要点。" |
| Agent 求助 | subagent | "我需要 analyst 帮忙确认技术细节" |
| 系统通知 | 事件 | `room.participant_joined` 事件（前端渲染为"x 加入了群聊"通知） |

不设"任务完成总结"消息 — 最后一条 Agent 产出即为最终结果。

### 时间线示例

```
#1  [CEO]        帮我调研并写报告
#2  [assistant]  收到请求，分析需求。已注册的 agent 有：researcher, writer。
                  计划：n1 → @researcher 调研，n2 → @writer 撰写   ← 思考消息
#3  [assistant]  任务已拆解：- @researcher 负责 调研 / - @writer 负责 撰写
#4  [system]     researcher 加入了群聊 / writer 加入了群聊        ← 入群事件
#5  [researcher] 调研结果：关于「帮我调研并写报告」的模拟要点。（流式输出）
#6  [writer]     文稿：基于调研结果撰写的报告。（流式输出，依赖完成后自动派发）
```

注意：没有"任务完成"的总结消息。writer 的产出就是最终结果，用户直接看。

### 求助链路示例

```
#5  [researcher] 调研中... 遇到技术细节不确定
#6  [researcher] 我需要 analyst 帮忙确认技术细节  ← input_required
#7  [assistant]  @researcher 请求 @analyst 协助，已加入工作
#8  [analyst]    技术细节确认：...（协助节点产出）
#9  [researcher] 调研结果：...（基于 analyst 的确认继续工作）
```

## 技术架构

### 协议层

ChoirWorks 自身是一个标准 A2A v1.0 Server：
- **AgentCard**：`GET /.well-known/agent-card.json`
- **REST API**：`POST /v1/message:send`、`POST /v1/message:stream`、`GET /v1/tasks/{id}` 等
- **SSE 流式**：`message:stream` / `tasks/{id}:subscribe` 实时推送任务与群内事件
- **群嵌套**：一个 ChoirWorks 实例可以注册为另一个实例的 subagent
- **群聊元数据**：`room/v1` 扩展 URI 承载 mentions / quote_id / interrupt 等群聊字段（消息 metadata）

### 执行模型

```
A2A SDK（DefaultRequestHandlerV2 + ActiveTask）
  ├── execute() 被调用 → 路由一条消息 → 立即返回
  ├── ActiveTask 持续运行 → 消费事件队列 → 分发给订阅者
  └── _request_lock → 同一 Task 的消息串行处理

ChoirWorksAgentExecutor（编排器）
  ├── 规划：LLM 生成 DAG（PlanDraft），推理文本作为 assistant 消息
  ├── 派发：后台 runner 非阻塞执行，不阻塞后续消息
  ├── 监听：节点完成 → 派发下游 → 处理求助/排队/仲裁
  └── 状态：Plan/节点/成员/干预/队列写入 Task.metadata，
            由 SDK TaskManager 合并、DatabaseTaskStore 持久化
```

A2A Task 是聚合根，事件（Message / TaskStatusUpdateEvent / TaskArtifactUpdateEvent）
是唯一事实来源。`execute()` 从 `Task.metadata` 重建编排状态，重启后由恢复流程
（`recover_tasks`）重挂非终态任务并继续跟踪在途的远端工作。

### 模拟环境

无需 API Key，一条命令启动完整演示：

```bash
uv run choirworks-sim --port 8567 --fresh
```

模拟在 `litellm` 响应层注入 — 走完整 `instructor` 解析链路，返回包含推理文本（`content`）和结构化数据（`tool_calls`）的 `ModelResponse`，最接近真实 LLM 行为。

## 当前状态

| 能力 | 状态 | 说明 |
|------|------|------|
| LLM 规划（DAG 生成） | 已实现 | `Planner` + `LiteLLMClient`，推理文本经 `structured_with_raw` 返回 |
| 非阻塞派发 | 已实现 | `execute()` 路由后立即返回；后台 runner 派发与续跑 |
| 群状态持久化 | 已实现 | Plan/节点/成员/干预/队列写入 `Task.metadata`，`DatabaseTaskStore` 落库；重启 `recover_tasks` 恢复 |
| assistant 思考消息 | 已实现 | 规划推理文本作为 assistant 群消息发出；无「任务完成」总结 |
| subagent 求助处理 | 已实现 | 大模型统一决策：转交 agent（协助节点 + 续跑）或转交人类（等待答复后续跑）；LLM 不可用时降级转人工 |
| 动态组队 | 已实现 | 计划自动入群、`@mention` 入群、Agent 间 `@` 仲裁（协助节点，带防环上限） |
| 中途插话/打断 | 部分 | 在途节点排队补投、显式打断转新节点、引用已完成产出建接续节点；多干预歧义时按首个处理 |
| 群嵌套 | 已实现 | 双实例嵌套集成测试通过；对端非阻塞返回时自动 `GetTask` + `SubscribeToTask` 跟随 |

> 旧版事件溯源编排栈（`core/orchestrator`、`core/coordinator`、`store/event_store`、
> `a2a/server.py`、房间 REST）已按 A2A SDK 迁移计划删除，不再作为实现依据。

## 非目标

- 编排器不执行具体业务动作
- 编排器不检查、不汇总 Agent 产出（各专家的产出即最终结果）
- 编排器不主动将新 Agent 加入群（但具备此能力，由求助触发）
- 不是 LLM-driven 的自主 agent（确定性调度 + LLM 辅助规划）
- 暂时不做分布式部署（MVP 单机单进程）
