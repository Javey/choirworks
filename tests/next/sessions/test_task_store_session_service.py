from __future__ import annotations

from a2a.helpers.proto_helpers import new_text_artifact_update_event
from a2a.server.context import ServerCallContext
from a2a.server.tasks import InMemoryTaskStore
from a2a.types.a2a_pb2 import (
    Message,
    Role,
    Task,
    TaskArtifactUpdateEvent,
)

from new_choirworks.core.a2a.events.event import a2a_meta
from new_choirworks.core.events.event import NodeInfo, meta
from new_choirworks.core.sessions.task_store_session_service import (
    TaskStoreSessionService,
)


def _artifact_event(text: str, *, author: str) -> TaskArtifactUpdateEvent:
    event = new_text_artifact_update_event(
        task_id="t1",
        context_id="c1",
        name="art",
        text=text,
        last_chunk=True,
    )
    meta(event).author = author
    return event


def _task_with(*events: TaskArtifactUpdateEvent) -> Task:
    task = Task(id="t1", context_id="c1")
    for event in events:
        task.artifacts.append(event.artifact)
    return task


def _service(store: InMemoryTaskStore) -> TaskStoreSessionService:
    return TaskStoreSessionService(store, ServerCallContext())


def _text(message: Message) -> str:
    return "".join(part.text for part in message.parts if part.text)


async def test_get_session_restores_order_and_types() -> None:
    store = InMemoryTaskStore()
    user = _artifact_event("hello", author="user")
    agent = _artifact_event("draft", author="writer")
    decision = _artifact_event("", author="planner")
    meta(decision).actions.state_delta = {"done": True}

    await store.save(_task_with(user, agent, decision), ServerCallContext())

    session = await _service(store).get_session("c1")

    assert session is not None
    assert len(session.events) == 3
    assert isinstance(session.events[0], Message)
    assert session.events[0].role == Role.ROLE_USER
    assert _text(session.events[0]) == "hello"
    assert isinstance(session.events[1], TaskArtifactUpdateEvent)
    assert isinstance(session.events[2], TaskArtifactUpdateEvent)
    assert session.state == {"done": True}


async def test_get_session_preserves_metadata() -> None:
    store = InMemoryTaskStore()
    event = _artifact_event("x", author="writer")
    meta(event).node_info = NodeInfo(path="wf@1/a@2")
    meta(event).branch = "a.b"
    meta(event).isolation_scope = "fc-1"
    a2a_meta(event).task_id = "remote-1"

    await store.save(_task_with(event), ServerCallContext())

    session = await _service(store).get_session("c1")

    assert session is not None
    restored = session.events[0]
    assert meta(restored).author == "writer"
    assert meta(restored).node_info.path == "wf@1/a@2"
    assert meta(restored).branch == "a.b"
    assert meta(restored).isolation_scope == "fc-1"
    assert a2a_meta(restored).task_id == "remote-1"


async def test_get_session_folds_multiple_state_deltas() -> None:
    store = InMemoryTaskStore()
    first = _artifact_event("a", author="writer")
    meta(first).actions.state_delta = {"k": 1, "only_first": True}
    second = _artifact_event("b", author="writer")
    meta(second).actions.state_delta = {"k": 2}

    await store.save(_task_with(first, second), ServerCallContext())

    session = await _service(store).get_session("c1")

    assert session is not None
    assert session.state == {"k": 2, "only_first": True}


async def test_get_session_returns_none_when_missing() -> None:
    store = InMemoryTaskStore()
    assert await _service(store).get_session("missing") is None
