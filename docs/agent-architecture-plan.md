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
| 3 | **wire 协议自由重设计**：自定义内容按 3.3 定稿的载体规则重整（`cw_type` part metadata / `cw_delta` 事件 metadata / 问答卡独占 status.message）；agent card 注册 AgentExtension |
| 4 | **core/ 只放与 ADK 同构、业务无关的框架件**；业务编排留在 `orchestration/`；`a2a/` 是纯壳桥 |
| 5 | **契约统一为 `run_async`**（ADK 形状，挂 BaseAgent）：`run_async(ctx, user, **tool_kwargs) -> object`——根返回 `None`、LlmAgent 返回 `T`；返回 `object` 是决策 2（砍内部 Event）的代价，替代 ADK 的 `AsyncGenerator[Event]` |
| 6 | `Subagent` / `run_subagent` 退役：outcome / assistance / repair 迁为 LlmAgent 实例；planner 的 `plan()` 循环迁为 PlannerAgent；`emit_*` 体系退役为 core 事件构造器 + 单点 `emit` |
| 7 | **地基与接线分离**：地基件纯新增（或只动地基自己的文件），接线/迁移动现有代码、**每项单独等指令再动**；每步测试全绿 |

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
| `agents/llm_agent.py` | `agents/llm_agent.py`（双态：终态工具决策 / 文字回复） | PlannerAgent、四件套实例定义在 `orchestration/` |
| `agents/context.py`（TurnContext） | `agents/invocation_context.py` | 由桥 / Runner 装配，吸收现 `MessagePayload` |
| `runner.py` | `runners.py` | 持业务服务实例，驱动根 agent |
| `events.py`（事件构造器 + `emit` 单点） | `events/`（Event——**不搬**） | `cw_type` data part 约定（替代内部 Event） |
| `flows/` | `workflow/_graph.py` | 现 `orchestration/flows/engine.py` 挪入（本就标注 ADK 参考） |

现 `core/context.py` 的 prompt 构造、引用围栏说明、brief builder 是业务内容，迁
`orchestration/`；core 只留框架件。

**不搬清单**：ADK 的回调体系（before/after agent/model/tool）、clone、
find_agent/root_agent、`run_live`、事件重放 / rehydration、agent name 的
identifier 校验（本仓 agent 名带连字符，如 `qa-engineer`）、pydantic
model_config 体系、ADK 的 AutoFlow 多工具自由循环（本仓 LLM 件为双态：终态
工具决策或文字回复，多工具循环按需再议）。

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

### 3.4 契约（统一 run_async，ADK 形状）

```
A2A 桥 (a2a/executor.py)
  (RequestContext, EventQueue)          ← 只有桥层见得到这两个
      │  Runner: ensure session → 装配 TurnContext（业务子类，含入站解析）
      ▼
  TurnContext（core 基座 ← 业务子类）   ← queue 已在 ctx 里（共享层）
      ├─ OrchestratorAgent.run_async(ctx, user) -> None      ← 回合驱动
      └─ LlmAgent.run_async(ctx, user, **kw) -> T            ← LLM 双态：决策 / 文字
```

```python
# core/agents/base.py：身份 + 树 + run_async（ADK 形状，非泛型 → 树无 Any 问题）
@dataclass(slots=True)
class BaseAgent(abc.ABC):
    name: str
    description: str = ""
    sub_agents: list[BaseAgent] = field(default_factory=list)
    parent_agent: BaseAgent | None = field(default=None, init=False, repr=False, compare=False)

    @abc.abstractmethod
    async def run_async(
        self, ctx: TurnContext, user: str, **tool_kwargs: object
    ) -> object:
        """跑一次调用：吃一条用户消息，产出输出；事件经 ctx 推出。"""

# core/agents/llm_agent.py：LlmAgent[T](BaseAgent)——双态
class LlmAgent[T](BaseAgent):
    system_prompt: str
    # 决策态：三字段同现——强制调用终态工具，参数即输出
    build_tools: Callable[..., Awaitable[list[AgentFunction]]] | None = None
    final_tool: str | None = None
    process: Callable[[ToolCallResult | None], T] | None = None
    max_retries: int = 2
    # 文字态：三字段同缺——不声明工具的单次文字回复，返回拼接文本（LlmAgent[str]）

    @override
    async def run_async(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> T:
        # 决策态：思考/正文 chunk 边跑边推；无调用/解析失败 → 追加反馈重试；
        # 耗尽 → process(None) 兜底；成功 → process(tool_call)
        # 文字态：单次调用、思考/正文照流，返回拼接文本
```

- 返回 `object`：ADK 靠 `AsyncGenerator[Event]` 统一输出，本仓砍了内部 Event
  （决策 2），根返回 `None`、LlmAgent 返回 `T`，协变合法、不破禁 Any。
- 根的 `user` = 入站消息文本；LlmAgent 的 `user` = 编排层拼好的提示词。

### 3.5 TurnContext（基座 + 业务子类）

- `core/agents/context.py`：`TurnContext` = 回合上下文基座（ADK
  InvocationContext 对应物）——`task_id` / `context_id` / `queue` / `lock` /
  `llm`。非泛型具体类，天然满足 `core/events.EventSink`。
- 业务子类（接线期由现 `OrchestrationContext` 演化）扩展 `registry` /
  `state` / `sessions` / 入站解析（吸收现 `MessagePayload`）；LlmAgent 收
  `TurnContext` 即同时收得下业务子类。

## 四、推进方式（地基与接线分离）

每步独立提交、全量验证（`uv run pytest tests -q` 全绿 + ruff +
basedpyright 0 errors）。

### 已完成

| 步 | 内容 |
|---|---|
| ✓ | `core/agents/base.py`：BaseAgent（身份 + 树） |
| ✓ | `core/events.py` 事件词汇表 + extension 注册 + 全仓 `emit_*` 调用点迁移（`orchestration/events.py` 收缩为业务组合层） |
| ✓ | `tests/unit/test_core_events.py`、方案文档重组 |
| ✓ | `BaseAgent` 挂统一 `run_async`（ADK 形状）+ `core/agents/context.py`（TurnContext 基座） |
| ✓ | `core/agents/llm_agent.py`：`LlmAgent[T]`（SingleFlow 循环 + 流式推事件）+ `test_llm_agent.py` |
| ✓ | `core/runner.py`：`Runner(root, prepare)` 持锁驱动根 agent + `test_runner.py` |
| ✓ | `LlmAgent` 双态：`build_tools` / `final_tool`（原 `tool_name`）/ `process` 可选化，无终态工具 = 文字态（返回拼接文本） |

**地基已完成**（core/：agents/base、agents/context、agents/llm_agent、events、runner）。

### 接线 / 迁移待办（动现有代码，**每项单独等指令**）

| 项 | 内容 |
|---|---|
| events 收敛 | 问答卡（`build_questions_message` / `emit_pending_questions` / `QUESTION_PART`）在 `hitl/intervention.py`；`a2a/executor.py` cancel 直推；`remote_caller.py` 三处远端转发直推——收敛为「core 造词、orchestration 组句」两层 |
| Subagent 迁移 | outcome / assistance / repair 迁为 `LlmAgent` 实例；`Subagent` / `run_subagent` 退役；`build_tools` 的 ctx 类型随业务回合子类 |
| TurnContext 业务子类 | 现 `OrchestrationContext` 演化（吸收 `MessagePayload`，扩展 registry/state/sessions） |
| 编排器根 | `OrchestratorAgent`（message_flow / DAG 调度 / settlement / HITL 移植为根 impl） |
| PlannerAgent | `core/planner.py` 的 `plan()` 循环迁为 LlmAgent 特化（只迁现状） |
| RemoteAgent | `remote_caller` 收编，节点执行走 agent |
| core/context.py 业务迁出 | prompt 构造 / 围栏 / brief builder 迁业务层 |
| `core/flows/` | `orchestration/flows/engine.py` 挪入 |

## 五、约束

- 借鉴 ADK 的文件头一律标注来源与许可证（先例：`orchestration/flows/`、`core/agents/base.py`）。
- 禁 `Any`；basedpyright 0 errors（规则见 `pyproject.toml`）。
- `docs/superpowers/` 历史文档不动。
- 不夹带无关功能（如编排层 ask-user 等此前讨论过但未立项的能力）。
