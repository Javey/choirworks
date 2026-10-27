from __future__ import annotations

from typing import override

from a2a.helpers.proto_helpers import (
    new_text_artifact_update_event,
    new_text_message,
)
from a2a.types.a2a_pb2 import Message, Role, TaskArtifactUpdateEvent

from new_choirworks.core.events.event import meta
from new_choirworks.core.sessions.base_session_service import BaseSessionService
from new_choirworks.core.sessions.session import Session


class _StubSessionService(BaseSessionService):
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}

    @override
    async def get_session(self, session_id: str) -> Session | None:
        return self.sessions.get(session_id)


def _message() -> Message:
    return new_text_message("hi", role=Role.ROLE_AGENT)


def _partial() -> TaskArtifactUpdateEvent:
    return new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="x", last_chunk=False
    )


async def test_partial_is_dropped() -> None:
    service = _StubSessionService()
    session = Session(id="c1")
    event = _partial()

    returned = await service.append_event(session, event)

    assert returned is event
    assert session.events == []


async def test_partial_state_delta_not_applied() -> None:
    service = _StubSessionService()
    session = Session(id="c1")
    event = _partial()
    meta(event).actions.state_delta = {"should": "not apply"}

    _ = await service.append_event(session, event)

    assert session.events == []
    assert session.state == {}


async def test_non_partial_appended_in_order() -> None:
    service = _StubSessionService()
    session = Session(id="c1")
    first = _message()
    second = _message()

    _ = await service.append_event(session, first)
    _ = await service.append_event(session, second)

    assert session.events == [first, second]


async def test_state_delta_applied_and_folded() -> None:
    service = _StubSessionService()
    session = Session(id="c1")

    first = _message()
    meta(first).actions.state_delta = {"nodes": {"a": "running"}}
    second = _message()
    meta(second).actions.state_delta = {"nodes": {"a": "completed"}, "done": False}

    _ = await service.append_event(session, first)
    _ = await service.append_event(session, second)

    assert session.state == {"nodes": {"a": "completed"}, "done": False}
    assert session.events == [first, second]


async def test_get_session_returns_registered_session() -> None:
    service = _StubSessionService()
    session = Session(id="c1")
    service.sessions["c1"] = session

    assert await service.get_session("c1") is session
    assert await service.get_session("missing") is None
