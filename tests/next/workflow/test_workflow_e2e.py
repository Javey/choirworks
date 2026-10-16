from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

import pytest
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import TaskState, TaskStatus, TaskStatusUpdateEvent
from pydantic import Field

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import START, BaseNode, RouteValue
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE, Edge
from new_choirworks.core.workflow.join_node import JoinNode
from new_choirworks.core.workflow.workflow import Workflow


class _FakeEventQueue(EventQueue):
    """收集事件用于断言。"""

    def __init__(self) -> None:
        self._events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self._events.append(event)

    @property
    def events(self) -> list[object]:
        return self._events


def _fake_event() -> TaskStatusUpdateEvent:
    return TaskStatusUpdateEvent(
        task_id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )


class _FakeNode(BaseNode):
    fake_output: object | None = None
    fake_route: RouteValue | None = None
    fake_error: str | None = None
    received_inputs: list[object | None] = Field(default_factory=list)

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        self.received_inputs.append(node_input)
        if self.fake_error:
            raise RuntimeError(self.fake_error)
        assert ctx.invocation_context.event_queue is not None
        await ctx.invocation_context.event_queue.enqueue_event(_fake_event())
        ctx.output = self.fake_output
        ctx.route = self.fake_route
        return
        yield  # pyright: ignore[reportUnreachable]


class _CapturingJoinNode(JoinNode):
    received_inputs: list[object | None] = Field(default_factory=list)

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        self.received_inputs.append(node_input)
        ctx.output = node_input
        return
        yield  # pyright: ignore[reportUnreachable]


def _make_ctx(workflow: Workflow) -> tuple[Context, _FakeEventQueue]:
    queue = _FakeEventQueue()
    ic = InvocationContext(event_queue=queue)
    return Context(invocation_context=ic, node=workflow), queue


async def _run(workflow: Workflow, ctx: Context) -> None:
    async for _ in workflow.run(ctx):
        pass


@pytest.mark.asyncio
async def test_workflow_linear():
    a = _FakeNode(name="a", fake_output="hello")
    b = _FakeNode(name="b", fake_output="world")
    wf = Workflow(name="wf", edges=[Edge(START, a), Edge(a, b)])
    ctx, queue = _make_ctx(wf)
    await _run(wf, ctx)

    assert len(queue.events) == 2
    assert a.received_inputs == [None]
    assert b.received_inputs == ["hello"]


@pytest.mark.asyncio
async def test_workflow_parallel():
    a = _FakeNode(name="a", fake_output="A")
    b = _FakeNode(name="b", fake_output="B")
    wf = Workflow(name="wf", edges=[Edge(START, a), Edge(START, b)])
    ctx, queue = _make_ctx(wf)
    await _run(wf, ctx)

    assert len(queue.events) == 2
    assert a.received_inputs == [None]
    assert b.received_inputs == [None]


@pytest.mark.asyncio
async def test_workflow_join():
    a = _FakeNode(name="a", fake_output="A")
    b = _FakeNode(name="b", fake_output="B")
    j = _CapturingJoinNode(name="j")
    wf = Workflow(
        name="wf",
        edges=[Edge(START, a), Edge(START, b), Edge(a, j), Edge(b, j)],
    )
    ctx, queue = _make_ctx(wf)
    await _run(wf, ctx)

    assert len(queue.events) == 2  # a + b 事件，j 不产事件
    assert len(j.received_inputs) == 1
    received = j.received_inputs[0]
    assert isinstance(received, dict)
    assert received == {"a": "A", "b": "B"}


@pytest.mark.asyncio
async def test_workflow_conditional_route():
    a = _FakeNode(name="a", fake_output="data", fake_route="x")
    b = _FakeNode(name="b", fake_output="B")
    c = _FakeNode(name="c", fake_output="C")
    wf = Workflow(
        name="wf",
        edges=[
            Edge(START, a),
            Edge(a, b, route="x"),
            Edge(a, c, route=DEFAULT_ROUTE),
        ],
    )
    ctx, queue = _make_ctx(wf)
    await _run(wf, ctx)

    assert len(queue.events) == 2  # a + b 事件，c 不跑
    assert b.received_inputs == ["data"]
    assert c.received_inputs == []


@pytest.mark.asyncio
async def test_workflow_failure():
    a = _FakeNode(name="a", fake_error="boom")
    b = _FakeNode(name="b", fake_output="B")
    wf = Workflow(name="wf", edges=[Edge(START, a), Edge(a, b)])
    ctx, _queue = _make_ctx(wf)

    with pytest.raises(WorkflowError):
        await _run(wf, ctx)

    assert b.received_inputs == []


@pytest.mark.asyncio
async def test_workflow_concurrency_limit():
    a = _FakeNode(name="a", fake_output="A")
    b = _FakeNode(name="b", fake_output="B")
    c = _FakeNode(name="c", fake_output="C")
    wf = Workflow(
        name="wf",
        max_concurrency=2,
        edges=[Edge(START, a), Edge(START, b), Edge(START, c)],
    )
    ctx, queue = _make_ctx(wf)
    await _run(wf, ctx)

    # 3 个节点都最终执行，但每次最多 2 个并行
    assert len(queue.events) == 3
    assert a.received_inputs == [None]
    assert b.received_inputs == [None]
    assert c.received_inputs == [None]
