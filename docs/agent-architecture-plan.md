# choirworks Agent 架构重构方案（ADK 同构 × A2A 母语）

> 状态：**实施依据**。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）。仅借鉴设计，不复制代码；实现时借鉴点须在文件头
> 标注来源与许可证（仓库先例：`orchestration/flows/`、`core/agents/base.py`）。
> 前置已完成：`core/agents/base.py`（BaseAgent：身份 + agent 树）。
> 本方案为彻底重构，不受现有结构约束（AGENTS.md：开发阶段，不考虑兼容与迁移）。

## 一、背景与目标

现状三者没有共同的 agent 抽象：

| 角色 | 现状 |
|---|---|
| 编排器 | `ChoirWorksAgentExecutor(AgentExecutor)`（`a2a/executor.py`），A2A 适配与编排引擎融合 |
| LLM 决策件 | `Subagent` 数据类 + `run_subagent` 循环（`subagents/base.py`），静默运行，思考不推事件 |
| 远端 peer | registry 记录 + `remote_caller` 的 A2A 调用，游离于 agent 体系之外 |

目标：借鉴 ADK 的 agents / Runner / InvocationContext 分层，三者统一进 agent
体系——编排器是根 agent，LLM 决策件是 `LlmAgent` 子类，远端 peer 收编为
`RemoteAgent`；子 agent 的思考与结果边跑边推 SSE 事件。

## 二、已确认决策

| # | 决策 |
|---|------|
| 1 | **session 持久化保持快照**（`OrchestrationState` + `SessionManager` + `ContextStore`），不搬 ADK 的事件重放 / rehydration |
| 2 | **不引入内部 Event 对象、不做 Event→A2A 转换**：产品本身就是 A2A agent，事件词汇表 = A2A 标准事件（`TaskStatusUpdateEvent` / `TaskArtifactUpdateEvent` / `Message`），agent 经回合上下文的 queue 直推 |
| 3 | **wire 协议自由重设计**：自定义内容统一为 `cw_type` data part 约定，agent card 注册 AgentExtension（`room` extension 先例）；状态增量从 status metadata 挪进 data part |
| 4 | **core/ 只放与 ADK 同构、业务无关的框架件**；业务编排留在 `orchestration/`；`a2a/` 是纯壳桥 |
| 5 | **契约分家**：BaseAgent 只管身份 + 树；根家族 `run_async(ctx)`，LlmAgent 家族 `decide(ctx, user) -> T`（流式推事件）——见 3.4 的理由 |
| 6 | `Subagent` / `run_subagent` 退役：outcome / assistance / repair 迁为 LlmAgent 实例；planner 的 `plan()` 循环迁为 PlannerAgent；`emit_*` 体系退役为 core 事件构造器 + 单点 `emit` |
| 7 | **分期提交，每期测试全绿**（pytest / ruff / basedpyright 0 errors） |

## 三、目标架构

### 3.1 总览

```
A2A 桥（a2a/executor.py，纯壳）
   RequestContext + EventQueue
      → Runner（core/runner.py）
          持服务（registry / llm / remote / sessions / brief / config）
          ensure session → 装配 TurnContext（服务 + 入站消息解析 + queue + lock + state）
             → OrchestratorAgent.run_async(ctx)      ← 根 agent（orchestration/orchestrator.py）
                  消息路由 / 计划 DAG 调度 / settlement / HITL / 恢复（impl，逻辑照搬）
                  ├─ PlannerAgent（LlmAgent 特化：多轮重试 + validate_plan + PlanRetry）
                  ├─ OutcomeAgent / AssistanceAgent / RepairAgent（LlmAgent 实例）
                  └─ RemoteAgent × N（remote_caller 收编：远端 peer 的 A2A 调用）

事件：只有 A2A 标准事件三件套，agent 经 TurnContext.queue 直推
session：快照持久化不变
```

### 3.2 core / 业务 分层对照

| core/（ADK 同构，业务无关） | ADK 对应 | 业务侧落点 |
|---|---|---|
| `agents/base.py`（已建） | `agents/base_agent.py` | 身份 + 树 |
| `agents/llm_agent.py` | `agents/llm_agent.py`（SingleFlow 语义） | PlannerAgent、四件套实例定义在 `orchestration/` |
| `agents/context.py`（TurnContext） | `agents/invocation_context.py` | 由桥 / Runner 装配，吸收现 `MessagePayload` |
| `runner.py` | `runners.py` | 持业务服务实例，驱动根 agent |
| `events.py`（事件构造器 + `emit` 单点） | `events/`（Event——**不搬**） | `cw_type` data part 约定（替代内部 Event） |
| `flows/` | `workflow/_graph.py` | 现 `orchestration/flows/engine.py` 挪入（本就标注 ADK 参考） |

现 `core/context.py` 的 prompt 构造、引用围栏说明、brief builder 是业务内容，迁
`orchestration/`；core 只留框架件。

**不搬清单**：ADK 的回调体系（before/after agent/model/tool）、clone、
find_agent/root_agent、`run_live`、事件重放 / rehydration、agent name 的
identifier 校验（本仓 agent 名带连字符，如 `qa-engineer`）、pydantic
model_config 体系、ADK 的 agentic 工具循环（AutoFlow——本仓 LLM 件是单次
结构化决策）。

### 3.3 事件词汇表（cw 扩展，第 1 期定稿）

自定义内容的载体规则（第 1 期实测定稿）：

- **status.message 是问答卡（question data part）的独占槽位**——状态事件的
  message 会顶掉任务快照里的问题卡，非问答内容一律走事件 metadata 或
  artifact，不碰 message。
- part 级自定义内容：discriminator 在 part metadata（`cw_type`）；
- 事件级自定义内容：挂在事件 metadata（`cw_delta` / `intervention_id`）。

| 内容 | 载体 | 相对现状 |
|---|---|---|
| `thought` / `text` 分块 | `TaskArtifactUpdateEvent`，part metadata `{cw_type, author}` | 替代 part metadata `cw_thought` + artifact metadata author（author 收进 part metadata） |
| `function_call` | `TaskArtifactUpdateEvent`，data payload + part metadata `cw_type` | `wire.function_call_part` 原样，实现迁 `core/events.py` |
| `question` | `TaskStatusUpdateEvent` 的 status.message data part | 沿用 QUESTION_PART |
| 状态增量 | `TaskStatusUpdateEvent` 事件 metadata `cw_delta` | **沿用**（第 1 期曾试挂 message，实测会顶掉问答卡，回退） |
| 答题拒绝 | `TaskStatusUpdateEvent` 事件 metadata `{intervention_id, reason}` | 沿用 |

`a2a/card.py` 注册 AgentExtension 描述以上约定；构造器全部在
`core/events.py`（`status_event` / `chunk_event` / `function_call_event` /
`state_delta_event`），`emit(ctx, ev)` 单点推队列。

### 3.4 契约（为什么分家）

```
A2A 桥 (a2a/executor.py)
  (RequestContext, EventQueue)          ← 只有桥层见得到这两个
      │  Runner: ensure session → 装配
      ▼
  TurnContext                           ← queue 已在 ctx 里（共享层）
      ├─ 根家族 OrchestratorAgent.run_async(ctx)      ← 回合驱动，事件经 ctx.queue 推
      └─ LlmAgent 家族 decide(ctx, user, **kw) -> T   ← 单次结构化 LLM 决策
```

- `RequestContext`（A2A 原始入站消息：room 元数据、`question_response` data
  part、quote/interrupt）只在桥层存在，内部 agent 收到的是拼好的提示词——
  统一签名的唯一途径是 ADK 的内部 Event 管线，而它已被决策 2 排除。
- 共享层是 `TurnContext`（含 queue），两个家族经它推 A2A 标准事件；BaseAgent
  只管身份 + 树，run 契约归各家族声明，避免不合身的强契约。

```python
# core/agents/base.py（已建）：身份 + 树，不声明 run 契约

# 根家族（orchestration/orchestrator.py）
class OrchestratorAgent(BaseAgent):
    async def run_async(self, ctx: TurnContext) -> None: ...

# LlmAgent 家族（core/agents/llm_agent.py）
class LlmAgent[T](BaseAgent):
    system_prompt: str
    tool_name: str
    build_tools: Callable[..., Awaitable[list[AgentFunction]]]
    process: Callable[[ToolCallResult | None], T]
    max_retries: int = 2

    async def decide(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> T:
        # 思考/正文 chunk 事件边跑边推（ctx.queue）；无调用/解析失败 → 追加反馈重试；
        # 耗尽 → process(None) 兜底；成功 → process(tool_call)
```

### 3.5 TurnContext

现 `OrchestrationContext` 吸收 `MessagePayload`（入站消息解析结果：text /
room / responses / updater）而成；`emit_*` 的构造逻辑归并进
`core/events.py`，调用点经 `emit(ctx, ev)` 单点推队列。

## 四、分期计划

每期独立提交、全量验证（`uv run pytest tests -q` 全绿 + ruff +
basedpyright 0 errors）。测试随期迁移，不留旧断言。

| 期 | 内容 | 主要动到 |
|---|---|---|
| 1 | `core/events.py` 事件词汇表 + extension 注册；全仓 `emit_*` 调用点迁移；`orchestration/events.py` 收缩为业务组合层（全部经 core 构造器 + `emit`，第 4 期随根 agent 重组） | events / wire / card + 全部调用点 + 测试 |
| 2 | TurnContext + Runner；`a2a/executor.py` 瘦身纯壳；`core/flows/` 就位；`core/context.py` 业务内容迁出 | context / session / executor + 测试 |
| 3 | `LlmAgent`（`decide` 流式推思考/结果事件）；outcome / assistance / repair 迁为实例；`Subagent` / `run_subagent` 退役 | `subagents/` + settlement / outcome / repair 调用点 |
| 4 | `OrchestratorAgent` 根：message_flow、DAG 调度、settlement、HITL 移植为根 impl | `orchestration/flows/` `execution/` `hitl/` |
| 5 | PlannerAgent：`core/planner.py` 的 `plan()` 循环迁为 LlmAgent 特化（只迁现状，不夹带新功能） | `core/planner.py` + `orchestration/planning/` |
| 6 | RemoteAgent：`remote_caller` 收编，节点执行走 agent | `execution/remote_caller.py` + `node_executor.py` |

## 五、约束

- 借鉴 ADK 的文件头一律标注来源与许可证（先例：`orchestration/flows/`、`core/agents/base.py`）。
- 禁 `Any`；basedpyright 0 errors（规则见 `pyproject.toml`）。
- `docs/superpowers/` 历史文档不动。
- 不夹带无关功能（如编排层 ask-user 等此前讨论过但未立项的能力）。
