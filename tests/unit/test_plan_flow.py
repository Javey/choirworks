from __future__ import annotations

import asyncio
from types import SimpleNamespace

from a2a.types.a2a_pb2 import TaskState, TaskStatusUpdateEvent

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.graph import FlowOutcome
from choirworks.orchestration.plan import plan_flow
from choirworks.orchestration.state import NodeState, NodeStatus, OrchestrationState
from tests.support.fakes import FakeRegistry, FakeSessions


class _Queue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def _ctx(
    state: OrchestrationState,
    *,
    max_node_attempts: int = 2,
    replan_on_failure: bool = False,
) -> tuple[OrchestrationContext, _Queue, FakeSessions]:
    queue = _Queue()
    sessions = FakeSessions()
    runtime = SimpleNamespace(
        state=state,
        task_id="t1",
        context_id="c1",
        queue=queue,
        lock=asyncio.Lock(),
        node_tasks={},
        settle_tasks={},
    )
    ctx = OrchestrationContext(
        runtime=runtime,
        registry=FakeRegistry([]),
        remote=SimpleNamespace(),
        llm=SimpleNamespace(),
        sessions=sessions,
        config=SimpleNamespace(
            max_node_attempts=max_node_attempts,
            replan_on_failure=replan_on_failure,
            max_revisions=3,
        ),
        brief_builder=SimpleNamespace(),
        effects=SimpleNamespace(),
    )
    return ctx, queue, sessions


def _node(node_id: str, status: NodeStatus, **kwargs: object) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name="a",
        agent_url="http://a",
        status=status,
        **kwargs,
    )


async def test_completed_plan_reports_done():
    state = OrchestrationState()
    state.nodes["n1"] = _node("n1", NodeStatus.COMPLETED)
    ctx, queue, sessions = _ctx(state)

    outcome = await plan_flow.run(ctx, None)

    assert outcome is FlowOutcome.EXIT_DONE
    assert sessions.evicted == ["c1"]
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    assert delta.status.state == TaskState.TASK_STATE_COMPLETED


async def test_waiting_plan_reports_wait():
    state = OrchestrationState()
    state.nodes["n1"] = _node("n1", NodeStatus.INPUT_REQUIRED, question="继续？")
    ctx, queue, sessions = _ctx(state)

    outcome = await plan_flow.run(ctx, None)

    assert outcome is FlowOutcome.EXIT_WAIT
    assert sessions.persist_count == 1
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    assert delta.status.state == TaskState.TASK_STATE_INPUT_REQUIRED


async def test_retryable_failure_returns_continue():
    state = OrchestrationState()
    state.nodes["n1"] = _node("n1", NodeStatus.FAILED, attempt=0)
    ctx, queue, _ = _ctx(state, max_node_attempts=2)

    outcome = await plan_flow.run(ctx, None)

    assert outcome is FlowOutcome.CONTINUE
    assert queue.events == []


async def test_exhausted_failure_reports_failed():
    state = OrchestrationState()
    state.nodes["n1"] = _node("n1", NodeStatus.FAILED, attempt=2)
    ctx, queue, sessions = _ctx(state, max_node_attempts=2, replan_on_failure=False)

    outcome = await plan_flow.run(ctx, None)

    assert outcome is FlowOutcome.EXIT_FAILED
    assert sessions.evicted == ["c1"]
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    assert delta.status.state == TaskState.TASK_STATE_FAILED


async def test_blocked_pending_work_reports_stalled_failure():
    state = OrchestrationState()
    state.nodes["n1"] = _node("n1", NodeStatus.PENDING, deps=["ghost"])
    ctx, queue, sessions = _ctx(state)

    outcome = await plan_flow.run(ctx, None)

    assert outcome is FlowOutcome.EXIT_FAILED
    assert sessions.evicted == ["c1"]
    assert isinstance(queue.events[-1], TaskStatusUpdateEvent)
