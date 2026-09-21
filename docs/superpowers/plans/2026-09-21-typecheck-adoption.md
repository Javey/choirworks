# 计划：类型检查纳入常规验证（basedpyright 清零）

## 背景

`registry.py` 的 `"isoformat" is not a known attribute of None` 在编辑器 Pylance 里可见，但项目验证命令（pytest + ruff）不跑类型检查，`basedpyright` 只是临时 `uvx` 运行且 45 个既有 error 被当作噪声放过。

## 决策

- 将 `basedpyright` 加入 dev 依赖（`uv add --dev basedpyright`，1.40.1）。
- `pyproject.toml` 增加 `[tool.basedpyright]`：`include = ["src"]`；`reportImportCycles = "warning"`（项目有意使用 TYPE_CHECKING + 函数内延迟导入，运行时无循环）。
- `AGENTS.md` 增加验证命令：`uv run basedpyright`，要求 0 errors。

## 修复清单（45 errors → 0）

| 类别 | 处理 |
|---|---|
| `reportImportCycles`（11） | 修复 `tools/base.py` 误从 `a2a.executor` 导入 `SessionRuntime`（改为 `orchestration.session`）；其余类型层循环保留为 warning |
| `TaskState` 注解（3） | `events.emit_function_call/error`、`flows.execute_function` 的 `state_name: int` → `TaskState`；`intervention.request_human` 的 `state_name=4` → 枚举 |
| `registry` Optional 访问（2） | 直接用局部 `now.isoformat()`，避免 `model_copy(update=...)` 的类型盲区 |
| JsonValue 不可迭代（4） | `planner._skill_ids` / `context._skill_descriptions` 显式 `isinstance` 收窄 `card["skills"]` |
| `replay.py` 快照收窄（4） | `nodes/members/interventions` 先判 `isinstance(list)`；`task_state: TaskState` |
| `asyncio.Task` 泛型（3） | `session.SessionRuntime` / `fake_agent.FakeAgent` 补 `[None]`；`node_tasks` 值类型 `NodeState`（去掉 Any） |
| `create_model` 展开（7） | 动态字段 dict 标注 `dict[str, Any]` |
| 动态 `Literal` union（1） | `outcome_decision` 用局部变量 + `# pyright: ignore[reportOperatorIssue]` |
| 其他收窄（4） | `as_model` 分层校验；`plan` 工具参数 `isinstance(PlanDraft)`；`call_subagent` 求助文案 `or ""`；`sim/runner` 用 `ServerConfig/StoreConfig/A2AConfig/SchedulerConfig` 构造 |

行为说明：`room_options` 用 `cast(RoomOptions, cast(object, raw))` 绕过 pyright 对 dict→TypedDict 直接 cast 的 `reportInvalidCast`（不做运行时校验，行为与原来一致）；其余均为类型收窄，行为不变。

## 验证

- `uv run ruff check src tests`：全绿。
- `uv run pytest tests -q`：`201 passed`（与基线一致）。
- `uv run basedpyright`：`0 errors, 474 warnings`（warning 为既有的 reportAny/reportUnknown*/reportUnusedParameter 等，暂不阻断，后续可逐类清理）。
- `import choirworks.main / sim.runner / api.app` 冒烟通过。
