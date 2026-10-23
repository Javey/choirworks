from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import final

import pytest

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.executor_node import ExecutorNode, build_node_message
from new_choirworks.orchestrator.plan import DagPlan, PlanNode, output_ref
from new_choirworks.orchestrator.runtime import DagRuntime, NodeStatus

AGENTS = {
    "writer": AgentInfo(name="writer", url="http://writer"),
    "reviewer": AgentInfo(name="reviewer", url="http://reviewer"),
}


def _node(nid: str, deps: list[str] | None = None, agent: str = "writer") -> PlanNode:
    return PlanNode(
        id=nid,
        name=nid,
        agent_name=agent,
        instruction=f"task {nid}",
        deps=deps or [],
    )


def _plan_dict(*nodes: PlanNode) -> dict[str, object]:
    return DagPlan(nodes=list(nodes)).model_dump()


@final
class _FakeRunner:
    """脚本化节点执行器：记录派发顺序与消息，按节点 id 返回输出。"""

    def __init__(
        self,
        outputs: dict[str, str] | None = None,
        delays: dict[str, float] | None = None,
        failures: dict[str, str] | None = None,
    ) -> None:
        self.outputs = outputs or {}
        self.delays = delays or {}
        self.failures = failures or {}
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, ctx: Context, node: PlanNode, info: AgentInfo, message: str) -> str:
        self.calls.append((node.id, message))
        await asyncio.sleep(self.delays.get(node.id, 0.01))
        if node.id in self.failures:
            raise RuntimeError(self.failures[node.id])
        return self.outputs.get(node.id, f"output_{node.id}")


def _runtime() -> DagRuntime:
    return DagRuntime(agents=AGENTS, user_task="do something")


def _make_ctx(rt: DagRuntime, runner: _FakeRunner) -> Context:
    executor = ExecutorNode(name="executor", runtime=rt, node_runner=runner)
    return Context(invocation_context=InvocationContext(), node=executor)


async def _run(rt: DagRuntime, runner: _FakeRunner, node_input: object | None = None) -> Context:
    ctx = _make_ctx(rt, runner)
    executor = ctx.node
    assert isinstance(executor, ExecutorNode)
    agen: AsyncGenerator[A2AEvent, None] = executor.run(ctx, node_input=node_input)
    async for _ in agen:
        pass
    return ctx


async def test_single_node_dispatches_and_routes_judge():
    rt = _runtime()
    runner = _FakeRunner({"n1": "hello"})
    ctx = await _run(rt, runner, _plan_dict(_node("n1")))

    assert ctx.route == "judge"
    assert ctx.output == "n1"
    assert rt.node_status["n1"] == NodeStatus.COMPLETED
    assert rt.node_outputs["n1"] == "hello"


async def test_empty_plan_routes_done():
    rt = _runtime()
    ctx = await _run(rt, _FakeRunner(), _plan_dict())
    assert ctx.route == DEFAULT_ROUTE


async def test_no_work_routes_done_after_completion():
    rt = _runtime()
    runner = _FakeRunner({"n1": "done"})
    _ = await _run(rt, runner, _plan_dict(_node("n1")))
    ctx = await _run(rt, runner, None)
    assert ctx.route == DEFAULT_ROUTE


async def test_consumption_first_one_completion_per_round():
    rt = _runtime()
    runner = _FakeRunner({"a": "out_a", "b": "out_b"})
    ctx0 = await _run(rt, runner, _plan_dict(_node("a"), _node("b")))
    assert ctx0.route == "judge"
    await asyncio.sleep(0.05)  # b 的完成也进入边界队列

    ctx1 = await _run(rt, runner, None)
    assert ctx1.route == "judge"
    assert {ctx0.output, ctx1.output} == {"a", "b"}
    assert rt.pop_completion() is None

    ctx2 = await _run(rt, runner, None)
    assert ctx2.route == DEFAULT_ROUTE


async def test_parallel_first_completion_routes_judge():
    rt = _runtime()
    runner = _FakeRunner(delays={"a": 0.01, "b": 0.2})
    ctx = await _run(rt, runner, _plan_dict(_node("a"), _node("b")))

    assert ctx.route == "judge"
    assert ctx.output == "a"
    assert rt.node_status["b"] == NodeStatus.RUNNING


async def test_running_tasks_not_cancelled_on_return():
    rt = _runtime()
    runner = _FakeRunner(delays={"a": 0.01, "b": 0.2})
    _ = await _run(rt, runner, _plan_dict(_node("a"), _node("b")))

    assert "b" in rt.running_tasks
    assert not rt.running_tasks["b"].done()
    await asyncio.sleep(0.25)
    assert rt.running_tasks["b"].done()


async def test_deps_respected_and_message_carries_outputs():
    rt = _runtime()
    runner = _FakeRunner({"a": "out_a", "b": "out_b"})
    ctx = await _run(rt, runner, _plan_dict(_node("a"), _node("b", deps=["a"])))

    assert ctx.output == "a"
    assert rt.node_status["b"] == NodeStatus.PENDING
    assert len(runner.calls) == 1


async def test_build_node_message_resolves_refs_and_deps():
    rt = _runtime()
    rt.init_plan(DagPlan(nodes=[_node("a")]))
    rt.record_completion("a", "out_a")

    node = _node("b", deps=["a"])
    node.instruction = f"use {output_ref('a')}"
    message = build_node_message(rt, node)

    assert "use out_a" in message
    assert "[a output]\nout_a" in message


async def test_plan_input_applies_diff():
    rt = _runtime()
    runner = _FakeRunner({"a": "out_a", "c": "out_c"})
    _ = await _run(rt, runner, _plan_dict(_node("a")))
    ctx = await _run(rt, runner, _plan_dict(_node("a"), _node("c", deps=["a"])))

    assert rt.node_status["a"] == NodeStatus.COMPLETED
    assert rt.node_outputs["c"] == "out_c"
    assert ctx.route == "judge"


async def test_failed_node_aborts_and_cancels_local_tasks():
    rt = _runtime()
    runner = _FakeRunner(delays={"bad": 0.01, "slow": 0.5}, failures={"bad": "boom"})

    with pytest.raises(WorkflowError, match="node bad failed: boom"):
        _ = await _run(rt, runner, _plan_dict(_node("bad"), _node("slow")))

    assert rt.running_tasks == {}
    assert rt.node_status["bad"] == NodeStatus.FAILED
