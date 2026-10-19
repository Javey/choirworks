# 编排 Agent 需求

## 倾向的图结构

```python
Workflow(
    edges=[
        Edge(START, Planner),
        Edge(Planner, Executor),               # Executor 是本地 subagent（管理 A2A 节点）
        Edge(Executor, Judge),
        Edge(Judge, Helper, route="assist"),   # 协助动作（subagent，流式）
        Edge(Judge, Done, route="done"),        # 完成
        Edge(Judge, Planner, route="revise"),  # 修订计划（回边，重规划）
        Edge(Helper, Done),
    ]
)
```

## 节点角色

| 节点 | 类型 | 职责 |
|------|------|------|
| Planner | 本地 LLMAgent | 分析任务，生成 DAG 图 |
| Executor | 本地 subagent | 管理 A2A 节点执行 DAG 任务 |
| A2ANode | 远端 A2A 节点 | 执行实际子任务（被 Executor 管理） |
| Judge | 本地 LLMAgent | 判定子任务结果 |
| Helper | 本地 subagent | 协助动作（流式 LLM 推理） |
| Done | 终点 | 标记完成 |

## 核心流程

```
用户输入任务
    ↓
Planner 分析任务，生成 DAG 图
    ↓
Executor（本地 subagent）管理 A2A 节点执行 DAG
  ├─ A2A 节点并行执行子任务
  ├─ 持续输出事件
  └─ 任意子任务完成即判定
    ↓
Judge 判定：协助 / 完成 / 修订计划
    ↓
根据判定结果执行相应动作（需流式输出）
    ↓
直到所有任务完成
```

## 需求点

1. **用户输入一个任务**
2. **编排 Agent 有一个 Workflow**——声明式图结构
3. **Planner 分析任务，生成 DAG 图**
4. **Executor 是本地 subagent**——管理 A2A 节点执行 DAG 任务
5. **A2A 节点执行实际子任务**——远端 A2A 调用
6. **A2A 节点间可通过编排层通信**——A 执行中需要 B 协助，B 完成后结果交还给 A
7. **后台执行 DAG，持续输出事件**——子任务并行执行，事件流式输出
8. **任意子任务完成即判定**——每个子任务完成后 Judge 判定（非批量）
9. **判定结果三种分支**：
   - **协助**：调 Helper subagent 流式输出协助过程
   - **完成**：标记该子任务完成
   - **修订计划**：重新生成 DAG（回边到 Planner）
10. **协助动作需要流式输出**——用 subagent（LLM 推理），不是 tool
11. **直到所有任务完成**——循环直到结束

## 并行 + 重规划场景

- A2A 节点 **A、B 并行**运行
- **A 先完成** → Judge 判定为"修订计划"
- 新 DAG 发现 **B 不需要了** → 取消正在运行的 B（不等 B 完成）
- 整个过程是**并行**的

## 重规划场景

- **取消节点**：B 不需要了 → 取消正在运行的 B
- **新增节点**：新 DAG 新增了 C → A 的结果要交给 C
- **已完成节点不重复执行**：A 已完成，新 DAG 不重跑 A
- **已完成节点的 output 传递给新节点**：A 的 output 交给新增的 C

## A2A 节点间协作场景

- A 执行过程中，**需要 B 的协助**
- B 协助完成后，**结果交还给 A**
- A 继续执行
- 通信通过**编排层**（不是 A2A 节点直接通信）

## 数据传递需求

- 已完成 A2A 节点的 **output 要能传递**给新 DAG 中的节点（含新增节点）
- 已完成的节点**不重复执行**

## 验证目标

- 判定动作是**节点级**（A2A 节点完成后判定），非事件级
