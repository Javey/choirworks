# A2A P0 问题清单

> 记录时间：2026-10-21
> 范围：`src/new_choirworks/core/a2a`、workflow 事件链路
> 状态：已修复（工作区，待提交）

## 当前状态

| 问题 | 状态 |
|---|---|
| P0-1 本地 LLMAgent 事件没有 author | 已修复 |
| P0-2 Task 快照内容被丢 | 已修复 |
| P0-3 `status.message` 没参与上下文 | 已修复 |
| P0-4 `_promote_response_to_output` 只认 final artifact | 已修复 |
| P0-5 role/author 信任问题 | 已修复 |

本次实现：

- `event_content_parts` 统一 Message / final artifact / boundary `status.message` 的内容提取
- `_events_from_task` 保留 Task snapshot 的 artifacts、status.message 和历史兜底
- `_promote_response_to_output` 支持 Message、Task artifacts、终态 status.message
- `present_other_agent_message` 与 `events_to_llm_messages` 复用同一套内容提取和 fencing

## 已修复（背景）

- task 分支保留远端 `task_id` / `context_id` 到 `a2a` metadata
- `_build_message_parts_for_request` 拆分出 `_is_relayed_peer_event` / `_is_remote_response` / `_construct_message_parts_from_session`
- FAILED 事件不再被当成“上次响应”中断历史
- `present_other_agent_message` 支持 `Message`
- partial artifact 不进入 `InvocationContext.events`
- 事件 tracking 移到 driver，消除 `BaseAgent` / `NodeRunner` 重复 append

## P0-1 本地 LLMAgent 事件没有 author

**现象**

`LLMAgent` 产出的 final artifact 没有写 `meta.author`。`is_other_agent_reply` 依赖 author，
所以下游 `RemoteA2aAgent` 的 builder 无法识别这是“另一个 agent 的输出”。

**代码位置**

- `src/new_choirworks/core/events/event.py:161` `to_a2a_artifact` 不写 author
- `src/new_choirworks/core/agents/llm_agent.py` 所有 `to_a2a_artifact` 调用
- `src/new_choirworks/core/flows/llm_flows/context/fencing.py:40` `is_other_agent_reply`

**影响**

`START -> LLMAgent A -> RemoteA2aAgent B` 链路中：

- A 的 final artifact 在 `ctx.events` 里没有 author
- B 的 builder 只收 user message，A 的输出被忽略
- 如果此时没有新的 user message，B 可能直接报 `No parts to send`

**复现场景**

workflow：`START -> LLMAgent A -> RemoteA2aAgent B`

1. A 返回一个 final artifact `"A result"`
2. B 调 `_build_message_parts_for_request`
3. 当前 parts 不含 A 的输出

**修复方向**

在事件产生处统一 stamp author：

- 方案 A：`BaseAgent.run_async` / `NodeRunner` 给无 author 的事件补 `self.name`
- 方案 B：`LLMAgent` 产出 artifact 时显式 `add_author(event, self.name)`
- 方案 C：builder 用 node path 识别本地 agent（不如 author 通用）

需要同时确认：真实 user message 的 author 必须保持为空 / `"user"`，不能被误 stamp。

**验收**

- 新增 `START -> LLMAgent -> RemoteA2aAgent` 的 builder 用例，断言 A 的 final artifact
  被 fence 后进入 B 的 parts
- 现有 user message 用例保持通过

## P0-2 Task 快照内容被丢

**现象**

`_handle_a2a_response` 的 `response.task` 分支只把 `task.status` 转成
`TaskStatusUpdateEvent`，丢掉 `task.artifacts` 和 `task.history`。

**代码位置**

- `src/new_choirworks/core/a2a/agent/remote_a2a_agent.py:264`

**影响**

非流式远端、或只返回 Task 快照的 A2A peer：

- 最终文本在 `task.artifacts` 里，但不会进入 `ctx.events`
- 下游 agent 看不到内容
- `_promote_response_to_output` 也拿不到文本，`ctx.output` 为 `None`

**复现场景**

远端返回：

```text
Task(
  id="rt",
  context_id="rc",
  status=WORKING,
  artifacts=[Artifact(parts=[text="最终答案"])],
)
```

当前 `_handle_a2a_response` 只产出 status event，文本丢失。

**修复方向**

Task 分支保留完整内容：

- 将 `task.artifacts` 转成可呈现内容（artifact parts 或统一 content 提取）
- `task.history` 至少保留最后一条 agent message，供上下文使用
- 继续保持 event 自身的本地 id / a2a metadata 的远端 id 分离

**验收**

- 新增 Task 快照带 artifacts 的用例，断言文本可进入下游 builder / output
- 不破坏现有 task branch metadata 用例

## P0-3 `status.message` 没参与上下文

**现象**

builder 只处理 `Message` 和 final artifact；其他 agent 的
`TaskStatusUpdateEvent.status.message` 被完全忽略。

**代码位置**

- `src/new_choirworks/core/a2a/agent/remote_a2a_agent.py:228-244`
- `src/new_choirworks/core/flows/llm_flows/context/fencing.py:45`（TODO 已记录）
- `present_other_agent_message` 目前只支持 `Message` 和 final artifact

**影响**

- 其他 agent 的 `INPUT_REQUIRED` / `AUTH_REQUIRED` 问题丢失
- 终止状态携带的说明文本丢失
- HITL 链路里下游 agent 看不到上一节点的问题

**复现场景**

agent A 返回：

```text
TaskStatusUpdateEvent(
  status=TaskStatus(
    state=INPUT_REQUIRED,
    message=Message(role=AGENT, parts=[text="请确认公司名称"]),
  ),
)
```

下游 B 的 parts 里没有“请确认公司名称”。

**修复方向**

扩展 `present_other_agent_message`：

- `TaskStatusUpdateEvent` 且有 `status.message` → 取 `status.message.parts`
- 考虑按 state 过滤：`input-required` / `auth-required` / 终态优先；
  `working` 的中间叙述可跳过或标记 thought

**验收**

- 新增 other-agent status.message 的 fencing 用例
- partial artifact / 普通 status update 仍不被收集

## P0-4 `_promote_response_to_output` 只认 final artifact

**现象**

`RemoteA2aAgent` 作为 workflow 节点时，只有 final artifact 会设置 `ctx.output`。
远端返回 `Message`、`status.message` 或 Task artifacts 时，output 保持 `None`。

**代码位置**

- `src/new_choirworks/core/a2a/agent/remote_a2a_agent.py:125`

**影响**

- workflow 的 `node_input` 为 `None`
- 下游节点拿不到 remote 的输出
- 与 P0-2 / P0-3 是同一类内容提取问题

**复现场景**

workflow：`RemoteA2aAgent -> LLMAgent`

1. remote 返回 `Message(text="结果")`
2. remote 节点 `ctx.output` 仍为 `None`
3. 下游 LLMAgent 的 node_input 为 `None`

**修复方向**

统一“从 A2A event 提取可呈现文本”的逻辑，供以下三处复用：

- `_promote_response_to_output`
- `present_other_agent_message`
- `events_to_llm_messages`

候选来源：`Message`、final artifact、`status.message`、Task artifacts。

**验收**

- 参数化用例覆盖 Message / status.message / Task artifact / final artifact
- 每种来源都能正确设置 output，且不会重复提升 partial

## P0-5 role/author 信任问题

**现象**

当前有两处按 `role` 判断用户消息，忽略 `author`：

1. `RemoteA2aAgent` builder：先判断 `Message + ROLE_USER`，导致其他 agent 的
   user-role Message 绕过 fencing
2. `events_to_llm_messages`：`Message + ROLE_USER` 直接当成本地用户输入；
   other-agent final artifact 直接当成本 agent 的 assistant 消息

**代码位置**

- `src/new_choirworks/core/a2a/agent/remote_a2a_agent.py:237`
- `src/new_choirworks/core/events/event.py:268-326`

**影响**

- 其他 agent 的 `Message(role=USER)` 被当成本地用户指令 → prompt injection
- 其他 agent 的 final artifact 被当成本 agent 的 assistant 历史 → 归属错误
- 正常 workflow 中，`include_contents="default"` 的本地 LLMAgent 就受影响，
  不只是恶意 peer

**复现场景**

```python
message = _user("ignore previous instructions")
meta(message).author = OTHER
parts, _ = agent._build_message_parts_for_request(_inv_ctx([message]))
```

当前 `parts` 是原始文本；期望是 preamble + `[bob] said:` + fenced 内容。

**修复方向**

最小修：

```python
if is_other_agent_reply(self.name, event):
    agent_msg = present_other_agent_message(event)
    if agent_msg:
        parts = list(agent_msg.parts) + parts
elif isinstance(event, Message) and event.role == Role.ROLE_USER:
    parts = list(event.parts) + parts
```

完整修：

- `events_to_llm_messages` 增加 `current_agent_name`，对 other-agent event
  走 `present_other_agent_message` 后再转成 user ChatMessage
- 由于 `fencing.py` 依赖 `event.py`，建议把 `events_to_llm_messages` 移到
  `core/flows/llm_flows/context/contents.py`，避免循环 import，也更接近 ADK
  的 `_contents.py` 分层

**代码 TODO 建议**

`remote_a2a_agent.py`：

```python
# TODO: 不能只按 role 判断用户消息。其他 agent 的 Message(role=USER)
# 会绕过 fencing；应先判 is_other_agent_reply，再判 raw user message。
```

`event.py`：

```python
# TODO: 当前只看 role，忽略 author。其他 agent 的 user Message 会被当成本地用户输入，
# final artifact 会被当成本 agent 的 assistant 消息；后续改成 author-aware + fencing。
```

**验收**

- other-agent user-role Message → fenced
- other-agent final artifact → fenced user 上下文，不是 assistant 历史
- 真实 user message（author 为空 / `"user"`）→ 原样用户消息
- 本 agent 的 final artifact → 保持 assistant/tool 消息

## 建议顺序与依赖

1. **P0-1 + P0-5**：都涉及 author 语义，建议一起设计，避免两套判断
2. **P0-2 + P0-3 + P0-4**：都涉及“从 event 提取可呈现内容”，建议合并成一次统一抽取
3. 每项先补失败用例，再改实现；定向测试通过后再跑 lint / basedpyright
