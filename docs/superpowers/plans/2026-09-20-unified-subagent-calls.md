# 计划：统一 call_subagent 事件（派发公告 + 协助请求气泡）

## 目标

前端可见两件此前看不到的事：

1. **派发公告**：编排层把"接下来要执行"的节点派给 agent 时，发一条 `@agent 任务名` 消息；串联任务只在轮到它时才公告；同一批并行派发合并成一个气泡（多个 `@`）。
2. **协助请求**：agent 请求协助时，发一条以请求者为发送者的气泡：`@helper 协助干嘛`。

## 事件契约

派发与求助复用同一个 A 类事件 `call_subagent`（function call，落盘于 `task.artifacts`，回放原样透传），只差 `requested_by`：

```json
{
  "function_name": "call_subagent",
  "function_args": {
    "requested_by": "orchestrator" | "<发起节点id>",
    "target_agent": "code-reviewer",
    "instruction": "代码审查与质量把控" | "评估该方案的可行性…"
  },
  "function_result": { "success": true, ... }
}
```

- 编排层派发：`requested_by="orchestrator"`，`instruction=node.name`，只 emit 不 execute（节点已由 `create_plan` 建好）。
- 求助：`requested_by=node.id`，`instruction=decision.instruction`，`execute()` 创建 derived 节点。

## 后端

1. `tools/call_subagent.py`：`requester_node_id` → `requested_by`；`execute()` 遇到 `orchestrator` 直接返回失败（防御）。
2. `a2a/executor.py`
   - `_run_plan()`：先收集本批 `mode=="dispatch" and not derived and attempt==0` 的节点并逐个 `emit_function_call`（在创建 node task 之前，保证同批事件相邻），再创建任务；重试/resume/continue 不重复公告。
   - `_spawn_assist()`：改传 `requested_by=node.id`。
3. `replay.py`、`state_delta`、`state.py` 不动。

## 前端（conversationView.ts）

`function_name === "call_subagent"` 分支：

- `requested_by === "orchestrator"`：若最后一条消息是派发气泡（标记 `group: "dispatch"`）则追加一行，否则新建 assistant 气泡（规划大脑）；每行 `- @{target_agent} {instruction}`；行文本去重（幂等）。
- 否则：保留 `assist.dispatched` 通知 + pending 节点；新增 agent 气泡，sender=请求者（`function_result.requester` → 回落 `view.nodes` 查找 → 回落 `requested_by`），text `@{helper} {instruction}`。

## 测试

- 新增 `tests/integration/test_a2a_announcements.py`：串联两批、并行一批、重试不重复、求助事件字段、`/replay` 可见。
- `frontend/src/lib/conversationView.test.ts`：连续编排层事件合并、非连续新建、求助气泡 sender、重复事件幂等。

## 验证

后端 `uv run pytest tests -q -p no:cacheprovider`（基线：2 个既有失败，勿新增）、ruff、basedpyright（基线 39 个既有错误）；前端 `npm test` / `npm run build` / `npm run lint`；sim 抓真实事件用前端 reducer 复放。

## 前置

- 上一轮未提交改动（working 气泡固化、artifact 元数据回放透传、`SystemNotification.seq` 编译修复）保持在工作区。
- 既有失败 `test_llm_routes_to_human` / `test_send_answers_pending_intervention` 属并行会话改动范围（`ask_user` 清 `a2a_task_id` + fake agent 重复提问），本计划不处理。
