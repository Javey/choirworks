from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from typing import override

from a2a.helpers.proto_helpers import new_message, new_text_part
from a2a.types.a2a_pb2 import Role, TaskState, TaskStatusUpdateEvent

from new_choirworks.core.a2a.agent.remote_a2a_agent import RemoteA2aAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import (
    A2AEvent,
    extract_text,
    is_partial_event,
    meta,
)
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import DagPlan, PlanNode, resolve_instruction
from new_choirworks.orchestrator.runtime import (
    CompletionRecord,
    DagRuntime,
    NodeStatus,
)

type NodeRunFn = Callable[[Context, PlanNode, AgentInfo, str], Awaitable[str]]

FAILURE_STATES = frozenset(
    {
        TaskState.TASK_STATE_FAILED,
        TaskState.TASK_STATE_REJECTED,
        TaskState.TASK_STATE_CANCELED,
    }
)
PAUSE_STATES = frozenset(
    {
        TaskState.TASK_STATE_INPUT_REQUIRED,
        TaskState.TASK_STATE_AUTH_REQUIRED,
    }
)


class RemoteNodeError(RuntimeError):
    """远端 A2A 节点以失败/不支持的暂停态收尾。"""


def build_node_message(runtime: DagRuntime, node: PlanNode) -> str:
    """构造派发消息：instruction 占位符解析 + deps output 注入（需求：output 传递）。"""
    text = resolve_instruction(node.instruction, runtime.node_outputs)
    dep_outputs = [
        f"[{dep} output]\n{runtime.node_outputs[dep]}"
        for dep in node.deps
        if dep in runtime.node_outputs
    ]
    return "\n\n".join([text, *dep_outputs])


async def run_remote_node(ctx: Context, node: PlanNode, info: AgentInfo, message: str) -> str:
    """默认节点执行器：经 RemoteA2aAgent 调远端 agent。

    - 以 ``node.agent_name`` 为 agent 身份（事件 author / 日志）；
    - 子 invocation 沿用父 task/context id，远端 id 保留 metadata（事件关联一致）；
    - 终态语义：FAILED/REJECTED/CANCELED → 抛错（节点 FAILED）；
      INPUT_REQUIRED/AUTH_REQUIRED → v0 不支持续跑，抛明确错误；
    - ``finally`` 清理 RemoteA2aAgent（httpx client / artifact buffer）。

    TODO(resume): INPUT_REQUIRED/AUTH_REQUIRED 暂停态应以 Message.task_id 续跑原
    远端任务（B 的 output 作为补充消息），待 RemoteA2aAgent 支持 resume 后接入
    （见 core/a2a/agent/remote_a2a_agent.py 的 resume TODO）；当前统一新建远端任务下发。
    假设远端 agent stateful，消息不重放历史。
    """
    parent_ic = ctx.invocation_context
    child_ic = InvocationContext(
        event_queue=parent_ic.event_queue,
        task_id=parent_ic.task_id or str(uuid.uuid4()),
        context_id=parent_ic.context_id or str(uuid.uuid4()),
    )
    user_msg = new_message(parts=[new_text_part(text=message)], role=Role.ROLE_USER)
    meta(user_msg).author = "user"
    child_ic.events.append(user_msg)

    agent = RemoteA2aAgent(name=node.agent_name, agent_url=info.url)
    node_ctx = Context(invocation_context=child_ic, node=agent)
    failure: str | None = None
    pause: str | None = None

    try:
        async with aclosing(agent.run(node_ctx, node_input=None)) as agen:
            async for event in agen:
                if not is_partial_event(event):
                    parent_ic.events.append(event)
                if parent_ic.event_queue is not None:
                    await parent_ic.event_queue.enqueue_event(event)
                state = _status_state(event)
                if state is None:
                    continue
                text = extract_text(event)
                if state in FAILURE_STATES and failure is None:
                    failure = text or f"remote task ended in {_state_name(state)}"
                elif state in PAUSE_STATES and pause is None:
                    pause = (
                        f"remote task paused in {_state_name(state)}"
                        + (f": {text}" if text else "")
                        + " (resume not supported in v0)"
                    )
    finally:
        await agent.cleanup()

    if failure is not None:
        raise RemoteNodeError(f"remote agent {node.agent_name} failed: {failure}")
    if pause is not None:
        raise RemoteNodeError(f"remote agent {node.agent_name}: {pause}")
    output = node_ctx.output
    return output if isinstance(output, str) else ""


def _state_name(state: TaskState) -> str:
    return TaskState.Name(state)


def _status_state(event: A2AEvent) -> TaskState | None:
    if isinstance(event, TaskStatusUpdateEvent):
        return event.status.state
    return None


class ExecutorNode(BaseNode):
    """确定性 DAG 执行节点：消费优先循环（评估暂停语义，需求 12）。

    1. Planner 产出的新计划（node_input）→ apply_plan diff；
    2. done_requested（空计划=完成）→ drain 在途任务 → DEFAULT → Done；
    3. 边界队列非空 → 逐个 pop → route="judge"（评估期间不派发）；
    4. 队列空 → 派发就绪节点；无 pending 无 running → DEFAULT → Done；
    5. 否则等第一个完成 → route="judge"。

    在途任务跨调用存活（已运行不取消）；FAILED → 整体失败。
    """

    runtime: DagRuntime
    node_runner: NodeRunFn | None = None

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        runtime = self.runtime
        self._apply_plan_input(node_input)
        self._collect_done()

        if runtime.done_requested:
            # 空计划 = 完成（需求 15）：不再判定，drain 在途任务后收口（不取消）
            while runtime.running_tasks:
                await self._wait_one()
                self._collect_done()
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        await self._abort_if_failed()

        rec = runtime.pop_completion()
        if rec is not None:
            self._route_completion(ctx, rec)
            return
            yield  # pyright: ignore[reportUnreachable]

        await self._dispatch_ready(ctx)
        if not runtime.has_work():
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        await self._wait_one()
        self._collect_done()
        await self._abort_if_failed()

        rec = runtime.pop_completion()
        if rec is None:
            raise WorkflowError(f"Executor {self.name}: dispatch made no progress")
        self._route_completion(ctx, rec)

        return
        yield  # pyright: ignore[reportUnreachable]

    def _apply_plan_input(self, node_input: object | None) -> None:
        if isinstance(node_input, dict) and "nodes" in node_input:
            self.runtime.apply_plan(DagPlan.model_validate(node_input))
        elif isinstance(node_input, DagPlan):
            self.runtime.apply_plan(node_input)

    def _route_completion(self, ctx: Context, rec: CompletionRecord) -> None:
        ctx.output = rec.node_id
        ctx.route = "judge"

    async def _abort_if_failed(self) -> None:
        failed = self.runtime.failed_completion()
        if failed is not None:
            await self.runtime.abort(f"node {failed.node_id} failed: {failed.error}")

    async def _dispatch_ready(self, ctx: Context) -> None:
        runner = self.node_runner or run_remote_node
        for node_id in self.runtime.dispatchable():
            node = self.runtime.get_node(node_id)
            info = self.runtime.agents[node.agent_name]
            message = build_node_message(self.runtime, node)
            self.runtime.node_status[node_id] = NodeStatus.RUNNING
            self.runtime.running_tasks[node_id] = asyncio.create_task(
                self._invoke_runner(runner, ctx, node, info, message)
            )

    async def _invoke_runner(
        self, runner: NodeRunFn, ctx: Context, node: PlanNode, info: AgentInfo, message: str
    ) -> str:
        return await runner(ctx, node, info, message)

    async def _wait_one(self) -> None:
        tasks = list(self.runtime.running_tasks.values())
        if not tasks:
            return
        _done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    def _collect_done(self) -> None:
        """把已结束的在途任务收进边界队列（结果留存，逐个判定）。"""
        for node_id, task in list(self.runtime.running_tasks.items()):
            if not task.done():
                continue
            del self.runtime.running_tasks[node_id]
            try:
                output = task.result()
            except Exception as e:
                self.runtime.record_failure(node_id, str(e))
            else:
                self.runtime.record_completion(node_id, output)
