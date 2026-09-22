from __future__ import annotations

import asyncio
from types import SimpleNamespace

from a2a.types.a2a_pb2 import TaskStatusUpdateEvent
from google.protobuf.json_format import MessageToDict

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.intervention import answer_intervention, settle_input
from choirworks.orchestration.state import (
    NodeState,
    OrchestrationState,
    add_cancel_request,
    add_intervention,
    expire_cancel_requests,
    normalize_cancel_requests,
    pending_interventions,
    state_from_json,
    state_to_json,
)
from tests.support.fakes import FakeRegistry


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
        runner_start_requested=False,
    )
    deps = SimpleNamespace(registry=FakeRegistry([]))
    ctx = OrchestrationContext(runtime=runtime, deps=deps, effects=SimpleNamespace())
    return ctx, queue


def node(node_id: str, status: str = "pending") -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=node_id,
        agent_url=f"http://{node_id}",
        status=status,
    )


def test_cancel_request_dedupes_per_node():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    first = add_cancel_request(state, "n1", "打断？")
    again = add_cancel_request(state, "n1", "打断？")
    assert first is not None
    assert again is None
    assert len(state.interventions) == 1


def test_expire_cancel_requests_on_node_settle():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    add_cancel_request(state, "n1", "打断？")
    expired = expire_cancel_requests(state, "n1")
    assert [iv.id for iv in expired] == ["iv1"]
    assert pending_interventions(state) == []


def test_normalize_expires_when_target_missing_or_settled():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "completed")
    state.nodes["n2"] = node("n2", "working")
    add_cancel_request(state, "n1", "打断？")
    add_cancel_request(state, "n2", "打断？")
    add_cancel_request(state, "ghost", "打断？")
    expired = normalize_cancel_requests(state)
    assert {iv.target_node_id for iv in expired} == {"n1", "ghost"}
    pending = pending_interventions(state)
    assert [iv.target_node_id for iv in pending] == ["n2"]


def test_intervention_serialization_roundtrip():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    add_cancel_request(state, "n1", "打断？")
    loaded = state_from_json(state_to_json(state))
    intervention = next(iter(loaded.interventions.values()))
    assert intervention.kind == "confirm_cancel"
    assert intervention.target_node_id == "n1"
    assert pending_interventions(loaded)[0].kind == "confirm_cancel"


async def test_settle_input_resolves_via_completed_helper_without_human():
    state = OrchestrationState()
    blocked = node("3", "input_required")
    blocked.question = "请确认是否采用该方案？"
    state.nodes["3"] = blocked
    state.nodes["3-h1"] = NodeState(
        id="3-h1",
        name="",
        agent_name="product-manager",
        agent_url="http://pm",
        status="completed",
        output="PM 的评估结论",
        derived=True,
        assist_requested_by="3",
    )
    ctx, queue = make_ctx(state)

    progress = await settle_input(ctx)

    assert progress is True
    assert state.nodes["3"].status == "ready"
    intervention = next(iter(state.interventions.values()))
    assert intervention.status == "resolved"
    assert intervention.responder == "3-h1"
    assert intervention.answer == "PM 的评估结论"
    assert len(queue.events) == 1
    delta = queue.events[0]
    assert isinstance(delta, TaskStatusUpdateEvent)
    meta = MessageToDict(delta.metadata)
    assert meta["kind"] == "state_delta"
    assert meta["interventions"][intervention.id]["responder"] == "3-h1"
    assert meta["nodes"]["3"]["status"] == "ready"


async def test_answer_intervention_marks_human_responder_and_readies_node():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "input_required")
    add_intervention(state, "n1", "请确认是否采用该方案？")
    ctx, queue = make_ctx(state)

    await answer_intervention(ctx, "按方案二执行")

    intervention = next(iter(state.interventions.values()))
    assert intervention.status == "resolved"
    assert intervention.responder == "human"
    assert intervention.answer == "按方案二执行"
    assert state.nodes["n1"].status == "ready"
    assert state.nodes["n1"].answer_text == "按方案二执行"
    assert ctx.runtime.runner_start_requested is True
    delta = queue.events[-1]
    assert isinstance(delta, TaskStatusUpdateEvent)
    meta = MessageToDict(delta.metadata)
    assert meta["interventions"][intervention.id]["responder"] == "human"
