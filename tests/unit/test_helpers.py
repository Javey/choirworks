from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent, TaskStatusUpdateEvent
from google.protobuf.json_format import MessageToDict

from choirworks.models.domain import AgentRecord
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.helpers import join_members
from choirworks.orchestration.state import OrchestrationState
from tests.support.fakes import FakeRegistry


class FakeQueue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def agent(name: str) -> AgentRecord:
    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={},
        created_at=datetime.now(UTC),
    )


def make_ctx(agents: list[AgentRecord]) -> tuple[OrchestrationContext, FakeQueue]:
    queue = FakeQueue()
    runtime = SimpleNamespace(
        state=OrchestrationState(),
        task_id="t1",
        context_id="c1",
        queue=queue,
        lock=asyncio.Lock(),
    )
    deps = SimpleNamespace(registry=FakeRegistry(agents))
    ctx = OrchestrationContext(
        runtime=runtime,
        registry=deps.registry,
        remote=SimpleNamespace(),
        llm=SimpleNamespace(),
        sessions=SimpleNamespace(),
        config=SimpleNamespace(),
        brief_builder=SimpleNamespace(),
    )
    return ctx, queue


async def test_join_members_emits_function_call_artifact_and_delta() -> None:
    ctx, queue = make_ctx([agent("writer")])

    await join_members(ctx, ["writer"], "human_mention")

    # artifact -> plain WORKING status -> member state_delta
    assert len(queue.events) == 3
    artifact_event = queue.events[0]
    assert isinstance(artifact_event, TaskArtifactUpdateEvent)
    part = artifact_event.artifact.parts[0]
    data = MessageToDict(part.data)
    assert data["function_name"] == "join_members"
    assert data["function_args"] == {"names": ["writer"], "reason": "human_mention"}
    assert data["function_result"] == {
        "success": True,
        "data": {"joined": ["writer"]},
        "error": None,
    }
    assert MessageToDict(part.metadata) == {"cw_type": "function_call"}

    delta_event = queue.events[2]
    assert isinstance(delta_event, TaskStatusUpdateEvent)
    metadata = MessageToDict(delta_event.metadata)
    assert metadata["kind"] == "state_delta"
    assert metadata["members"][0]["agent_name"] == "writer"


async def test_join_members_dedupes_and_skips_unknown() -> None:
    ctx, queue = make_ctx([agent("writer")])

    await join_members(ctx, ["writer", "ghost", "writer"], "human_mention")
    assert len(queue.events) == 3
    assert list(ctx.state.members) == ["writer"]

    await join_members(ctx, ["writer"], "human_mention")
    assert len(queue.events) == 3
