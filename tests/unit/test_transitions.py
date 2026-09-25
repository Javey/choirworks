from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from a2a.types.a2a_pb2 import TaskStatusUpdateEvent
from google.protobuf.json_format import MessageToDict

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.state import (
    InterventionKind,
    NodeState,
    NodeStatus,
    OrchestrationState,
)
from choirworks.orchestration.transitions import (
    InvalidTransition,
    apply_transition,
    can_retry,
    can_transition,
    transition,
)
from tests.support.fakes import FakeRegistry, FakeSessions


class _Queue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def make_ctx(state: OrchestrationState) -> tuple[OrchestrationContext, _Queue]:
    queue = _Queue()
    runtime = SimpleNamespace(
        state=state,
        task_id="t1",
        context_id="c1",
        queue=queue,
        lock=asyncio.Lock(),
    )
    ctx = OrchestrationContext(
        runtime=runtime,
        registry=FakeRegistry([]),
        remote=SimpleNamespace(),
        llm=SimpleNamespace(),
        sessions=FakeSessions(),
        config=SimpleNamespace(),
        brief_builder=SimpleNamespace(),
    )
    return ctx, queue


def node(node_id: str, status: NodeStatus = NodeStatus.PENDING) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=node_id,
        agent_url=f"http://{node_id}",
        status=status,
    )


def test_can_transition_allows_table_entries_and_self():
    pending = node("n1", NodeStatus.PENDING)
    assert can_transition(pending, NodeStatus.SUBMITTED)
    assert can_transition(pending, NodeStatus.INVALIDATED)
    assert can_transition(pending, NodeStatus.PENDING)

    completed = node("n1", NodeStatus.COMPLETED)
    assert not can_transition(completed, NodeStatus.PENDING)
    assert not can_transition(completed, NodeStatus.CANCELED)


def test_apply_transition_raises_and_leaves_status_untouched():
    completed = node("n1", NodeStatus.COMPLETED)
    with pytest.raises(InvalidTransition):
        apply_transition(completed, NodeStatus.FAILED)
    assert completed.status == NodeStatus.COMPLETED


def test_apply_transition_mutates_status():
    working = node("n1", NodeStatus.WORKING)
    apply_transition(working, NodeStatus.CANCELED)
    assert working.status == NodeStatus.CANCELED


def test_can_retry_requires_failed_and_attempts_left():
    failed = node("n1", NodeStatus.FAILED)
    failed.attempt = 1
    assert can_retry(failed, 2)
    failed.attempt = 2
    assert not can_retry(failed, 2)

    pending = node("n2", NodeStatus.PENDING)
    assert not can_retry(pending, 2)


async def test_transition_emits_status_delta():
    state = OrchestrationState()
    working = node("n1", NodeStatus.WORKING)
    state.nodes["n1"] = working
    ctx, queue = make_ctx(state)

    await transition(ctx, working, NodeStatus.COMPLETED)

    assert working.status == NodeStatus.COMPLETED
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    meta = MessageToDict(delta.metadata)
    assert "cw_delta" in meta
    assert meta["cw_delta"]["nodes"]["n1"]["status"] == "completed"


async def test_transition_delta_override_and_interventions_share_one_event():
    state = OrchestrationState()
    asked = node("n1", NodeStatus.INPUT_REQUIRED)
    asked.question = "继续吗？"
    state.nodes["n1"] = asked
    ctx, queue = make_ctx(state)

    await transition(
        ctx,
        asked,
        NodeStatus.READY,
        delta={"question": asked.question},
        interventions={
            "iv1": {
                "status": "resolved",
                "node_id": "n1",
                "kind": InterventionKind.QUESTION,
                "answer": True,
            }
        },
    )

    assert len(queue.events) == 1
    delta = queue.events[0]
    assert isinstance(delta, TaskStatusUpdateEvent)
    meta = MessageToDict(delta.metadata)
    assert meta["cw_delta"]["nodes"]["n1"]["status"] == "ready"
    assert meta["cw_delta"]["nodes"]["n1"]["question"] == "继续吗？"
    assert meta["cw_delta"]["interventions"]["iv1"]["answer"] is True


async def test_transition_emit_false_skips_wire_event():
    state = OrchestrationState()
    pending = node("n1", NodeStatus.PENDING)
    state.nodes["n1"] = pending
    ctx, queue = make_ctx(state)

    await transition(ctx, pending, NodeStatus.SUBMITTED, emit=False)

    assert pending.status == NodeStatus.SUBMITTED
    assert queue.events == []
