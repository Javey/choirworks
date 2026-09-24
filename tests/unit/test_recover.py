from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from a2a.types.a2a_pb2 import TaskStatusUpdateEvent
from google.protobuf.json_format import MessageToDict

from choirworks.a2a import recovery as recover
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.state import (
    InterventionStatus,
    NodeState,
    NodeStatus,
    OrchestrationState,
    add_cancel_request,
)
from tests.support.fakes import FakeRegistry, FakeSessions


class _Queue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def _ctx(state: OrchestrationState) -> tuple[OrchestrationContext, _Queue]:
    queue = _Queue()
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
        sessions=FakeSessions(),
        config=SimpleNamespace(),
        brief_builder=SimpleNamespace(),
    )
    return ctx, queue


def _node(node_id: str, status: NodeStatus, **kwargs: object) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name="a",
        agent_url="http://a",
        status=status,
        **kwargs,
    )


async def test_recover_session_resets_active_nodes_and_expires_stale_interventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = OrchestrationState()
    state.nodes["resubscribe"] = _node("resubscribe", NodeStatus.WORKING, a2a_task_id="remote-1")
    state.nodes["redispatch"] = _node("redispatch", NodeStatus.SUBMITTED)
    state.nodes["done"] = _node("done", NodeStatus.COMPLETED)
    stale = add_cancel_request(state, "done", "打断？")
    assert stale is not None

    started: list[OrchestrationContext] = []
    monkeypatch.setattr(recover, "start_runner", lambda ctx: started.append(ctx))
    ctx, queue = _ctx(state)

    await recover.recover_session(ctx)

    assert state.nodes["resubscribe"].status == NodeStatus.RECOVER
    assert state.nodes["redispatch"].status == NodeStatus.PENDING
    assert stale.status == InterventionStatus.EXPIRED
    assert started == [ctx]
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    meta = MessageToDict(delta.metadata)
    assert meta["interventions"][stale.id]["status"] == "expired"
