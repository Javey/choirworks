from __future__ import annotations

from types import SimpleNamespace

from a2a.types.a2a_pb2 import TaskState
from google.protobuf.json_format import MessageToDict

from choirworks.core.events import (
    CwType,
    chunk_event,
    emit,
    function_call_event,
    state_delta_event,
    status_event,
)


class _Queue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def _sink() -> tuple[SimpleNamespace, _Queue]:
    queue = _Queue()
    sink = SimpleNamespace(task_id="t1", context_id="c1", queue=queue)
    return sink, queue


async def test_emit_pushes_single_event():
    sink, queue = _sink()
    event = status_event("t1", "c1", TaskState.TASK_STATE_WORKING)

    await emit(sink, event)  # type: ignore[arg-type]

    assert queue.events == [event]


def test_status_event_passes_state_and_metadata():
    event = status_event("t1", "c1", TaskState.TASK_STATE_COMPLETED, metadata={"k": "v"})

    assert event.task_id == "t1"
    assert event.context_id == "c1"
    assert event.status.state == TaskState.TASK_STATE_COMPLETED
    assert MessageToDict(event.metadata) == {"k": "v"}


def test_chunk_event_carries_kind_and_author_in_part_metadata():
    event = chunk_event(
        "t1",
        "c1",
        text="思考",
        kind=CwType.THOUGHT,
        author="assistant",
        artifact_id="a1",
        append=True,
        last_chunk=False,
    )

    part = event.artifact.parts[0]
    assert part.text == "思考"
    assert MessageToDict(part.metadata) == {"cw_type": CwType.THOUGHT, "author": "assistant"}
    assert event.artifact.artifact_id == "a1"
    assert event.append is True
    assert event.last_chunk is False


def test_function_call_event_carries_payload():
    event = function_call_event(
        "t1",
        "c1",
        function_name="ask_user",
        args={"node_id": "n1"},
        result={"success": True},
    )

    part = event.artifact.parts[0]
    payload = MessageToDict(part.data, preserving_proto_field_name=True)
    assert payload["function_name"] == "ask_user"
    assert payload["function_args"] == {"node_id": "n1"}
    assert payload["function_result"] == {"success": True}
    assert MessageToDict(part.metadata) == {"cw_type": CwType.FUNCTION_CALL}
    assert event.append is False
    assert event.last_chunk is True


def test_state_delta_event_keeps_message_slot_free():
    event = state_delta_event("t1", "c1", {"nodes": {"n1": {"status": "ready"}}})

    assert event.status.state == TaskState.TASK_STATE_WORKING
    assert MessageToDict(event.metadata) == {"cw_delta": {"nodes": {"n1": {"status": "ready"}}}}
    assert not event.status.HasField("message")
