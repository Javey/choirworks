from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

from a2a.types.a2a_pb2 import Message, Role

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, extract_text
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE, Edge
from new_choirworks.core.workflow.workflow import Workflow
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.executor_node import ExecutorNode, NodeRunFn
from new_choirworks.orchestrator.helper import Helper
from new_choirworks.orchestrator.planner import PlannerNode, build_planner
from new_choirworks.orchestrator.runtime import DagRuntime


class DoneNode(BaseNode):
    """终点节点：无 pending 无 running 后到达（需求 11）。

    输出为整个 outputs registry（含 ignored 节点），定位是内部可观测性数据；
    最终交付物以节点事件流里的产出为准（编排器不检查、不总结，见 product.md）。
    """

    runtime: DagRuntime

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        ctx.output = dict(self.runtime.node_outputs)

        return
        yield  # pyright: ignore[reportUnreachable]


def build_workflow(
    runtime: DagRuntime, llm: BaseLLM, node_runner: NodeRunFn | None = None
) -> Workflow:
    """声明式编排图（需求 2；轮次图语义）。

    Executor→Helper("judge")：每个子任务完成 → 节点级判定；
    Helper→Executor("execute")：@mention 命中 → assist 接线后继续；
    Helper→Planner(DEFAULT)：无 @mention → Planner 全量评估（replan 默认动作）；
    Executor→Done(DEFAULT)：无 pending 无 running。
    """
    planner = PlannerNode(
        name="planner",
        planner=build_planner(list(runtime.agents.values()), llm),
        runtime=runtime,
    )
    executor = ExecutorNode(name="executor", runtime=runtime, node_runner=node_runner)
    helper = Helper(name="helper", runtime=runtime)
    done = DoneNode(name="done", runtime=runtime)

    return Workflow(
        name="orchestrator",
        edges=[
            Edge(START, planner),
            Edge(planner, executor),
            Edge(executor, helper, route="judge"),
            Edge(executor, done, route=DEFAULT_ROUTE),
            Edge(helper, executor, route="execute"),
            Edge(helper, planner, route=DEFAULT_ROUTE),
        ],
    )


def extract_user_task(ic: InvocationContext) -> str:
    for event in reversed(ic.events):
        if isinstance(event, Message) and event.role == Role.ROLE_USER:
            return extract_text(event)
    return ""


class OrchestratorAgent(BaseAgent):
    """编排 Agent：持有声明式 Workflow，管理 A2A 节点执行 DAG 任务（需求 1/2/4）。"""

    agents: list[AgentInfo] = []
    llm: BaseLLM
    node_runner: NodeRunFn | None = None

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        runtime = DagRuntime(
            agents={a.name: a for a in self.agents},
            user_task=extract_user_task(ctx),
        )
        workflow = build_workflow(runtime, self.llm, node_runner=self.node_runner)
        wf_ctx = Context(invocation_context=ctx, node=workflow)
        try:
            async for event in workflow.run(wf_ctx, node_input=runtime.user_task):
                yield event
        finally:
            # 异常/取消/正常收口统一清理后台任务（评审 P0-4）
            await runtime.shutdown()
