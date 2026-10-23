from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import override

from a2a.helpers.proto_helpers import new_message, new_text_part
from a2a.types.a2a_pb2 import Role

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
    """默认节点执行器：经 A2A 调远端 agent，事件流出到共享 event_queue。

    TODO(resume): A 续轮 / INPUT_REQUIRED 暂停态应以 Message.task_id 续跑原远端任务
    （B 的 output 作为补充消息），待 RemoteA2aAgent 支持 resume 后接入
    （见 core/a2a/agent/remote_a2a_agent.py 的 resume TODO）；当前统一新建远端任务下发。
    假设远端 agent stateful，消息不重放历史。
    """
    child_ic = InvocationContext(
        event_queue=ctx.invocation_context.event_queue,
        task_id=str(uuid.uuid4()),
        context_id=str(uuid.uuid4()),
    )
    user_msg = new_message(parts=[new_text_part(text=message)], role=Role.ROLE_USER)
    meta(user_msg).author = "user"
    child_ic.events.append(user_msg)

    agent = RemoteA2aAgent(name=node.name, agent_url=info.url)
    output = ""
    async for event in agent.run_async(child_ic):
        if not is_partial_event(event):
            ctx.invocation_context.events.append(event)
        if ctx.invocation_context.event_queue is not None:
            await ctx.invocation_context.event_queue.enqueue_event(event)
        text = extract_text(event)
        if text and not is_partial_event(event):
            output = text
    return output


class ExecutorNode(BaseNode):
    """确定性 DAG 执行节点：消费优先循环（评估暂停语义，需求 12）。

    1. Planner 产出的新计划（node_input）→ apply_plan diff；
    2. 边界队列非空 → 逐个 pop → route="judge"（评估期间不派发）；
    3. 队列空 → 派发就绪节点；无 pending 无 running → DEFAULT → Done；
    4. 否则等第一个完成 → route="judge"。

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
