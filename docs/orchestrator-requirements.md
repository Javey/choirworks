# 编排 Agent 需求

## 倾向的图结构

```python
Workflow(
    edges=[
        Edge(START, Planner),
        Edge(Planner, Executor),                     # 总是 → Executor 执行计划
        Edge(Executor, Helper, route="judge"),       # 节点完成 → Helper 规则解析
        Edge(Executor, Done, route=DEFAULT_ROUTE),   # 所有节点完成 → Done
        Edge(Helper, Executor, route="execute"),     # @mention 命中 → 加 assist 节点 → 继续 Executor
        Edge(Helper, Planner, route=DEFAULT_ROUTE),  # 无 @mention → Planner 评估
    ]
)
```

## 节点角色

| 节点 | 类型 | 职责 |
|------|------|------|
| Planner | 本地 LLMAgent | 唯一 LLM 决策点：评估全量上下文，输出完整新计划。空计划 = 完成 |
| Executor | 本地 subagent | 管理 A2A 节点执行 DAG：并行派发、事件流、完成追踪。无 pending 无 running → Done |
| A2ANode | 远端 A2A 节点 | 执行实际子任务（被 Executor 管理） |
| Helper | 本地 BaseNode | 纯规则解析节点 output 中的 `@agent_name`，命中→加 assist 节点，未命中→交 Planner |
| Done | 终点 | 标记完成 |

## 核心流程

```
用户输入任务
    ↓
Planner 分析任务，生成 DAG 图
    ↓
Executor 管理 A2A 节点执行 DAG
  ├─ 有 pending 节点 → 派发执行
  ├─ 有 running 节点 → 等待完成
  ├─ 节点完成 → route="judge" → Helper 解析
  └─ 无 pending + 无 running → Done
    ↓
Helper 规则解析节点 output
  ├─ @agent_name 命中 → 加 assist 节点 → route="execute" → Executor 继续
  └─ 无 @mention → DEFAULT_ROUTE → Planner 评估 → 输出新计划 → Executor
    ↓
循环直到 Executor 无 pending 无 running → Done
```

## 需求点

1. **用户输入一个任务**
2. **编排 Agent 有一个 Workflow**——声明式图结构
3. **Planner 分析任务，生成 DAG 图**
4. **Executor 是本地 subagent**——管理 A2A 节点执行 DAG 任务
5. **A2A 节点执行实际子任务**——远端 A2A 调用
6. **A2A 节点间可通过编排层通信**——A 执行中需要 B 协助，B 完成后结果交还给 A
7. **后台执行 DAG，持续输出事件**——子任务并行执行，事件流式输出
8. **每个节点完成触发 Helper 解析**——纯规则，非 LLM
9. **Helper 职责**：解析 `@agent_name`，命中→加 assist 节点，未命中→交 Planner
10. **协助走 Helper → Executor 统一执行**——不在运行时即时触发
11. **直到所有节点完成**——Executor 无 pending 无 running → Done
12. **评估期间停止派发**——节点完成触发 Helper 时，停止后续节点派发，已运行节点继续跑完，结果留给新计划
13. **Planner 只输出完整新计划**——不输出变更记录，编排层 diff 新旧 DagPlan 确定取消/新增/保留
14. **replan 是默认动作**——无 @mention 即交 Planner，不需要 LLM 判定是否 replan
15. **Planner 输出空计划 = 完成**——无需更新计划即为完成
16. **失败整体失败**——任一 A2A 节点失败（FAILED/REJECTED/CANCELED）即终止整个编排，
    不自动重试、不交 Planner 决策
17. **远端节点消息 = 自包含 instruction + deps output**——不携带原始用户任务；
    Planner 必须把任务所需上下文完整写进每个节点的 instruction

## 并行 + 重规划场景

- A2A 节点 **A、B 并行**运行
- **A 先完成** → Helper 解析 → 无 @mention → Planner 输出新计划
- 新 DAG 发现 **B 不需要了** → B 继续跑完（已派发不取消），结果留存
- 新计划不引用 B → B 的结果被忽略；新计划引用 B → B 的 output 传递给新节点
- 整个过程是**并行**的

## 重规划场景

- **停止派发**：新计划不需要的未开始节点不再派发
- **已运行不取消**：已派发节点继续跑完，结果留存
- **新增节点**：新计划新增的 C → A 的 output 传递给 C
- **已完成节点不重复执行**：A 已完成，新 DAG 不重跑 A
- **已完成节点的 output 传递**给新节点
- **Planner 只输出完整新计划**：编排层 diff 新旧 DagPlan 自行确定取消/新增/保留

## 评估暂停

- 节点完成触发 Helper 解析时：停止后续节点派发，已运行节点继续跑完
- Helper route="execute" → Executor 收集已运行节点完成结果，继续派发
- Helper DEFAULT_ROUTE → Planner → 新计划 → Executor diff 新旧计划，只派发未开始的节点

## A2A 节点间协作场景

- A 执行过程中 output 表明需要 B 协助
- Helper 解析 A 的 output 中的 `@B` → 更新计划，加一个 B 的 assist 节点
- route="execute" → Executor 执行 B
- B 完成后 output 交还给 A（通过计划中 A 的 instruction 引用 B 的 output）
- 通信通过**编排层**（计划 + 节点 output 传递），不是 A2A 节点直接通信

## 数据传递需求

- 已完成 A2A 节点的 **output 要能传递**给新 DAG 中的节点（含新增节点）
- 已完成的节点**不重复执行**
- 节点消息 = instruction（自包含）+ 依赖节点 output 注入；原始用户任务不随消息下发（需求 17）

## 验证目标

- 判定动作是**节点级**（每个 A2A 节点完成后 Helper 解析），非事件级
- Planner 只输出完整新计划，编排层 diff
- 评估期间停止派发，不取消已运行节点
- 完成判定：Executor 无 pending 无 running → Done
