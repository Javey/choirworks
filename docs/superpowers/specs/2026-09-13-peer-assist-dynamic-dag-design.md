# 动态 DAG 协作设计：Peer 协助升级为一等节点

- 日期：2026-09-13
- 状态：已实现
- 关联：`2026-09-12-a2a-orchestration-platform-design.md`（§5.2 HITL）、`2026-09-13-frontend-chat-design.md`

## 1. 目标与场景

人类工作模式的多 Agent 协作：A 请求协助 → 编排器决策（LLM/人工）→ 指派 B/C 干活 →
B 完成后编排让 A 继续，A 的上下文不丢；多轮协调后任务完成。

关键约束：**协助工作必须与普通节点同等可见、可管、可回退**，且不得重建正在运行的父节点。

## 2. 核心设计：计划扩展（`plan.extended`）

- 不新建计划版本。协助节点以 `plan.extended` 事件追加到**同一 plan**：
  - `added_nodes`：新节点（`derived: true` 仅作 UI 徽章标记）
  - `added_edges`：`helper → parent`，即把 helper 追加进 parent 的 `deps`
- 父节点行原封不动（`a2a_task_id` / `context_id` 保留）→ 上下文不丢失。
- 投影把扩展节点物化进 `nodes` 表并同步 `plans.dag` JSON；`rebuild()` 按事件顺序重放。

调度与普通节点一致：扩展节点 `deps=[]`，下一轮调度循环按 ready 规则**自动派发**，
不存在旁路调用。

## 3. 干预状态机（`peer_agent`）

1. 父节点 `input-required` → 读问题原文，LLM `PeerChoice`（提示词含问题原文与 `Worker node 'X' asks:`，排除提问者）。
2. 复用检查：同父、同 agent、非 invalidated 的已有协助节点：
   - `completed` → 直接以输出 resolve，不新建；
   - 其他 → 挂到既有节点；
   - 否则 → `plan.extended` 新建。
3. 写 `intervention.requested`（`assigned_node_id` / `assigned_to`）。
4. 调度器派发协助节点；其完成 → `intervention.resolved`（`responder` = agent 名，
   `answer.text` = 输出）→ `continue_node(parent)` 向父节点**同一远程任务**追加消息。
5. 递归：协助节点自身 `input-required` 时同样触发本流程。

等待语义：父节点停车且协助节点在途时任务保持 `running`；仅当无在途协助且干预未指派时才进入 `awaiting_input`。

失败语义（统一）：协助节点失败走普通 `_handle_failure`（重试/退避 → `replan_on_failure` 重规划或任务失败）；
重试耗尽时把关联干预标记 `failed`（`intervention.failed`）；`plan.superseded` 时 pending 干预置 `invalidated`。

## 4. 回退：精确图恢复

回滚 = 将任务投影回 `checkpoint.seq`：

1. `fold_graph(events ≤ seq, checkpoint.plan_version)` 折叠出当时的图（节点、deps、derived）。
2. diff 当前 DB：
   - 当时不存在的节点（checkpoint 后的扩展节点、后续计划节点）→ `invalidated`（清远程标识）；
   - 当时存在的节点 → 还原 `deps`；非 frontier 节点重置为 `pending`（清 attempt/output/远程标识）；
   - `plans.dag` 写回折叠图；task `plan_version` 与状态还原；
   - 受影响节点（reset ∪ invalidated）的所有干预置 `invalidated`（防止旧答案被复用）。
3. 对被重置/失效且在途的节点发送 `CancelTask` 信号（`node.cancel.sent`）。
4. `ROLLBACK_PERFORMED` 载荷携带 `deps_restore` / `reset_node_ids` / `invalidate_node_ids` / `dag`，
   投影纯应用，`rebuild()` 可精确重放。

调度、完成判定（`finalize_if_complete`）、recovery 全部忽略 `invalidated` 节点。

## 5. 前端

- `plan.extended` → 动态节点实时出现在线程中；`derived` 节点显示「协助」徽章；
  新事件类型加入 SSE 监听（`plan.extended` / `intervention.failed`）。
- 干预卡：已指派 → 「已指派 {agent} 处理中…」（无输入框）；resolved 显示答复人；failed 显示协助失败。
- 其余（流式、重试、回退对话框）复用既有机制。

## 6. 模拟剧本（`choirworks-sim`）

6 个脚本化 Agent：`researcher`(collaborate)、`writer`(inquire)、`critic`(review→human)、
`analyst`(assist)、`flaky`、`broken`；策略覆盖 `researcher/writer → peer_agent`。

请求 `请协调多个子代理协作完成这项分析`：
1. Planner 产出 `researcher ∥ writer` 并行；
2. researcher 暂停 → 编排器路由 analyst → 扩展节点执行；
3. writer 暂停 → 编排器路由 researcher → 扩展节点执行；
4. 两个协助节点完成后分别回填父节点并续跑 → 任务完成。

演示可见：4 张节点卡（2 主 + 2 协助且带徽章）、2 张干预卡由不同 agent 答复、计划卡含动态边。

## 7. 测试覆盖

- 投影/迁移：`plan.extended` 物化、`rebuild`、`assigned_node_id` 列、`intervention.failed`、
  `plan.superseded` 失效 pending 干预。
- 编排：扩展+自动派发+续跑、协助在途时任务保持 running、协助失败标记干预、复用已完成协助、
  递归/多轮路径由 sim 场景覆盖。
- 回退：扩展后回退失效扩展节点并还原 deps/dag；回退到扩展后 checkpoint 复用已完成协助；
  `rebuild` 重放一致；回退后可重新完成。
- 前端：`plan.extended` 折叠、指派/失败展示、协助徽章。
- 集成：sim 协作剧本断言 4 节点、2 干预（responder = analyst/researcher）、2 次 `plan.extended`。

## 8. 已知边界（后续演进）

- 全量重规划（`plan.superseded`）仍会替换整图，父节点上下文按设计丢失（仅回退路径保证图恢复）。
- 协助节点重试耗尽后不自动降级人工（当前统一走重规划/任务失败）。
- 计划 DAG 无环校验仅覆盖 `plan.created`；扩展边由编排器生成（helper→parent），不会成环。
