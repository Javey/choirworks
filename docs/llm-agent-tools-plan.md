# LlmAgent 工具传入方案（ADK `tools` + `output_schema`）

> 状态：**已实施**。
> 设计借鉴：google-adk（`/home/javey/Workspaces/adk-python`，Apache License 2.0，
> Copyright 2026 Google LLC）。仅借鉴设计，不复制代码。实现时文件头标注来源与
> 许可证（先例：`core/agents/llm_agent.py`、`core/agents/base.py`）。
> 对照点：`agents/llm_agent.py` 的 `ToolUnion` / `canonical_tools`；
> `flows/llm_flows/prompt/_schema.py` 与 `tools/set_model_response_tool.py`
> 的 `set_model_response`；`flows/llm_flows/basic.py` 的「无工具则原生
> response schema」。
> 前置：`docs/agent-architecture-plan.md` 地基已完成。本方案替换该文档 3.4 中
> 尚未改写的 `final_tool` / `build_tools` / `process` 目标形状；落地前以本文为准。
> 开发阶段，不考虑历史兼容（AGENTS.md）。`docs/superpowers/` 不动。

## 一、结论

可以移植，但只移植「构造时传入工具、运行时解析、模型能调用、结构化输出用
schema」这一层。

ADK 没有用户可见的 `final_tool`。对外是 `tools` 加可选的 `output_schema`。
终态工具名和强制 `tool_choice` 是本仓在「只有 tool calling、没有 response
schema」时加的。本方案删掉 `final_tool`、`build_tools`、`process`。

目标形状：

```python
agent = LlmAgent(
    name="helper",
    system_prompt="...",
    output_schema=OutcomeResult,  # 可选；没有就是文字或工具循环
    tools=[AskUserTool(), my_fn, MyToolset()],
)
```

## 二、不搬什么

不搬 ADK 的 processor 流水线、AutoFlow / `transfer_to_agent`、回调、工具确认、
鉴权、并行 tool call、`response_format`、500 次 LLM 上限、toolset 加载失败后
吞掉。不把 `PlannerAgent` 纳入本次。不改现有 `FunctionTool` 的
`OrchestrationContext` 签名。不把 `orchestration.execute_function` 拉进 core。

一次模型响应仍只接受一个 tool call。`LiteLLMClient` 仍不执行工具。

## 三、运行

`set_model_response` 是框架内部工具名，用户不命名、不放进 `tools`。有
`output_schema` 时由运行时注入，schema 即其参数。

| 条件 | 行为 |
|---|---|
| 无 schema、无 tools | 现有文字态：单次调用，思考/正文照流，返回拼接文本 |
| 有 schema、无其它工具 | 注入 `set_model_response`，强制 `tool_choice`。成功则 `schema.model_validate`。这是对 ADK 原生 response schema 的有意偏离：本仓 LiteLLM 路径没有 response schema，现有 tool calling 已够用，不先做 `response_format` |
| 有 tools、无 schema | `tool_choice=auto`。调到工具就 `run_async`，把结果塞回消息，直到模型只回文本。上限 `max_tool_rounds`（默认 8） |
| 两者都有 | 注入 `set_model_response`，`tool_choice=auto`（不能强制，否则别的工具调不到）。调到它就校验并返回。模型只回文字视为失败，走现有重试；耗尽走 `fallback()`。这是相对 ADK 的有意收紧：调用方要的是 `T`，不能把未校验文本当结果 |

校验失败、未调用 `set_model_response`、解析失败：沿用现在的反馈重试
（`max_retries`，默认 2）。成功的旁路工具调用不消耗这次重试，只消耗
`max_tool_rounds`。工具循环打满上限：无 schema 时抛错；有 schema 时走
`fallback()`。

`process()` 删除。成功路径只有校验。耗尽兜底收成可覆写的 `fallback()`，
默认抛错。三个子 agent 只保留兜底：

| Agent | `output_schema` | `fallback()` |
|---|---|---|
| `OutcomeAgent` | `OutcomeResult` | `OutcomeResult(intent="deliver")` |
| `AssistanceAgent` | 每回合 `resolve_output_schema` | `AssistanceResult()` |
| `RepairAgent` | `RepairResult`，`max_retries = 0` | `None`（`LlmAgent[RepairResult \| None]`） |

思考/正文 chunk 仍按现有词汇表边跑边推，每轮尝试独立 artifact，结束时 seal。
旁路工具的执行不在 core 发 function-call 产物；那仍是
`orchestration.execute_function` 的事。agent 循环只调用 `run_async`。工具抛错
时包成 `FunctionResult(success=False, error=...)` 喂回模型，不中断循环；不吞
`BaseException`。

## 四、解析

`ToolUnion = FunctionTool | BaseToolset | Callable[..., object]`，只放在
`LlmAgent`，不放 `BaseAgent`。

`canonical_tools(ctx, **kwargs)` 默认解析 `self.tools`，子类可覆写。解析顺序：

1. `BaseAgent` → `TypeError`（ADK 同样拒绝；agent 走 `sub_agents`）。
2. `FunctionTool` 实例 → 原样。传入工具类而不是实例 → `TypeError`。
3. `BaseToolset` → `await get_tools(ctx)`。加载失败直接抛，不学 ADK 吞掉后继续跑。
4. 可调用对象 → 包一层 `FunctionTool`（见下）。
5. 其它 → `TypeError`。

展开后工具名重复，或在有 schema 时用户自带名为 `set_model_response` 的工具，
都是 `TypeError`。该名字在有 schema 时保留给框架。

`__init__` 复制 `tools` 列表。类属性可作缺省，构造参数可覆盖；实例不共享、
不改类上的列表。`output_schema` / `max_retries` / `max_tool_rounds` 同一套
「类属性缺省 + 构造覆盖」。

### 4.1 函数包装

公开签名不用 `Any`。包装时用 `inspect` 校验，不合格即 `TypeError`：

- 每个参数都有注解。
- 禁止 `*args` / `**kwargs`。
- 名为 `ctx` 且注解为 `TurnContext` 的参数注入、不进 schema。不认 `tool_context` 别名。
- 返回注解必须是 `FunctionResult`、`BaseModel` 子类或 `str`。
- 名称取 `__name__`。描述取 docstring 首行；没有 docstring 则拒绝。
- 同步函数就地调用，不建线程池。异步函数 await。
- schema 用 pydantic `create_model` 从注解生成。若动态字段把 `Any` 漏进公开
  类型，只允许在这一处构造调用上处理，不把 `Any` 写进 `ToolUnion`。

### 4.2 动态 schema

ADK 的 `output_schema` 是静态的。本仓 `AssistanceAgent` 的候选 agent 每次调用
才知道，静态列表和静态 schema 都表达不了。保留一个钩子，而不是把决策工具再
做回用户命名的 tool：

```python
async def resolve_output_schema(
    self, ctx: TurnContext, **kwargs: object
) -> type[BaseModel] | None:
    return self.output_schema
```

只有 `AssistanceAgent` 覆写它：按 `exclude_agent`（仍由
`run_async(..., exclude_agent=)` 传入）和 `ctx.registry` 调用现有
`assistance_schema`。`OutcomeAgent` / `RepairAgent` 只设类属性
`output_schema`，删掉 `build_tools`。

## 五、消息回灌

`LiteLLMClient.stream` 增加可选 `messages`。不传时仍是现在的 `system` + `user`，
`core/planner.py` 不动。

`ToolCallResult` 增加 `call_id`。流式 delta 里没有 id 就生成 uuid。回灌两条：

- assistant：`tool_calls`（id、name、arguments JSON）
- `role=tool`：`tool_call_id` + `FunctionResult` 的 JSON

消息用 `TypedDict`，不用 `dict[str, Any]`。

## 六、子 agent 落地形状

```python
class OutcomeAgent(LlmAgent[OutcomeResult]):
    name = "outcome"
    system_prompt = OUTCOME_SYSTEM
    output_schema = OutcomeResult

    def fallback(self) -> OutcomeResult:
        return OutcomeResult(intent="deliver")


class RepairAgent(LlmAgent[RepairResult | None]):
    name = "repair"
    system_prompt = REPAIR_SYSTEM
    output_schema = RepairResult
    max_retries = 0

    def fallback(self) -> RepairResult | None:
        return None


class AssistanceAgent(LlmAgent[AssistanceResult]):
    name = "assistance"
    system_prompt = ASSISTANCE_SYSTEM

    async def resolve_output_schema(self, ctx: TurnContext, **kwargs: object) -> type[BaseModel]:
        ...

    def fallback(self) -> AssistanceResult:
        return AssistanceResult()
```

无其它工具时仍是强制 `set_model_response`，所以现有「模型必须交出结构化结果」
的行为不变，只是工具名不再由子类声明。

## 七、模拟器

`src/choirworks/sim/litellm_mock.py` 现在用 `OutcomeDecision` /
`AssistanceDecision` 分发。改名后，工具名为 `set_model_response` 时用
declaration 的 schema `title` 区分 `OutcomeResult` / `AssistanceResult`。
旧工具名分支保留：`tests/unit/test_sim_llm.py` 直接构造的
`StructuredOutputTool(name="AssistanceDecision")` 不改。

`tests/integration/test_sim_flow.py` 走真实 agent，必须能在新工具名下仍走出
outcome / assistance。`create_plan` 仍由 `planner.py` 直接调 `stream`，本次不改。

## 八、改哪些文件

| 文件 | 改动 |
|---|---|
| `src/choirworks/core/tool.py` | `BaseToolset`、函数包装、`ToolCallResult.call_id`、常量 `set_model_response`。`StructuredOutputTool` 复用为内部终态工具 |
| `src/choirworks/core/llm.py` | 可选 `messages`；捕获 call id |
| `src/choirworks/core/agents/llm_agent.py` | `tools` / `canonical_tools` / `output_schema` / `resolve_output_schema` / `fallback` / 四态循环。改文件头注释 |
| `src/choirworks/subagents/outcome/agent.py` | 删 `final_tool` / `build_tools` / `process` |
| `src/choirworks/subagents/assistance/agent.py` | 同上，改为覆写 `resolve_output_schema` |
| `src/choirworks/subagents/repair/agent.py` | 同上 |
| `src/choirworks/sim/litellm_mock.py` | `set_model_response` + schema title 分发 |
| `tests/unit/test_llm_agent.py` | 见下 |
| `docs/agent-architecture-plan.md` | 落地时把 3.4 和「已完成」行改成本文形状。落地前不要改那份已实施记录 |

不改：`core/planner.py`、各 `tools/*.py` 的上下文类型、`docs/superpowers/`。

## 九、测试

`tests/unit/test_llm_agent.py`：

- 文字态不变：无 schema、无 tools，不声明工具，返回拼接文本。
- 仅 schema：强制 `tool_choice` 名为 `set_model_response`，返回校验后的模型，不再经过 `process` 抽字段。
- 校验失败重试后成功；耗尽走 `fallback()`。
- `tools=[实例]` 被送进 `stream`。
- 有注解的函数包成工具，schema 不含 `ctx`；无注解、无 docstring、`*args` 拒绝。
- toolset 的 `get_tools(ctx)` 被调用；抛错则 agent 失败，不静默丢工具。
- 工具循环：第一次 tool call，第二次纯文本；第二次请求含 tool result。
- schema 与旁路工具并存：旁路工具执行并回灌；`set_model_response` 结束并校验；中途纯文本触发重试。
- `BaseAgent`、重名、保留名冲突均为 `TypeError`。
- 类属性 `tools` 被实例复制，改实例列表不影响类。

然后：`uv run pytest tests -q`、`uv run ruff check --fix src tests`、
`uv run ruff format src tests`、`uv run basedpyright`（0 errors）。

## 十、约束

- 禁 `Any`。
- core 不新增对 `orchestration` 的运行时导入。
- 有 schema 时不得返回未校验数据。
- 不静默丢掉工具或 toolset。
- 不夹带 `PlannerAgent`、原生 `response_format`、并行 tool call。
