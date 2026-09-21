from __future__ import annotations

import pytest
from a2a.types.a2a_pb2 import Message, Role, Task, TaskState, TaskStatus
from google.protobuf.json_format import ParseDict

from choirworks.a2a.rewind import (
    RewindMarker,
    RewindUnavailable,
    checkpoint_before,
    hidden_task_ids,
    is_human_turn,
    parse_markers,
    restore_state,
)
from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.a2a.state import OrchestrationState, state_to_json


def _marker(before: str, cut: str) -> RewindMarker:
    return RewindMarker(before_task_id=before, cut_task_id=cut)


def _task(task_id: str, *, state: OrchestrationState | None = None) -> Task:
    task = Task(
        id=task_id,
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    if state is not None:
        ParseDict({"choirworks.state": state_to_json(state)}, task.metadata)
    return task


def test_hidden_task_ids_covers_marker_range():
    ids = ["a", "b", "c", "d"]
    assert hidden_task_ids(ids, []) == set()
    assert hidden_task_ids(ids, [_marker("b", "b")]) == {"b"}
    assert hidden_task_ids(ids, [_marker("b", "c")]) == {"b", "c"}


def test_hidden_task_ids_unions_markers():
    ids = ["a", "b", "c", "d", "e"]
    markers = [_marker("b", "c"), _marker("a", "e")]
    assert hidden_task_ids(ids, markers) == {"a", "b", "c", "d", "e"}


def test_checkpoint_before_returns_previous_live_task():
    ids = ["a", "b", "c"]
    assert checkpoint_before(ids, [], "c") == "b"
    assert checkpoint_before(ids, [], "a") is None


def test_checkpoint_before_skips_hidden_range():
    ids = ["a", "b", "c", "d"]
    markers = [_marker("b", "c")]
    assert checkpoint_before(ids, markers, "d") == "a"


def test_checkpoint_before_jumps_across_chained_rewinds():
    ids = ["a", "b", "c", "d", "e"]
    markers = [_marker("c", "c"), _marker("b", "d")]
    assert checkpoint_before(ids, markers, "e") == "a"


def test_checkpoint_before_none_when_chain_reaches_start():
    ids = ["a", "b", "c", "d", "e"]
    markers = [_marker("c", "c"), _marker("a", "d")]
    assert checkpoint_before(ids, markers, "e") is None


def test_restore_state_returns_predecessor_snapshot():
    first = _task("a", state=OrchestrationState(plan_id="p1"))
    second = _task("b")
    state = restore_state([first, second], [], "b")
    assert state.plan_id == "p1"


def test_restore_state_without_predecessor_is_empty():
    assert restore_state([_task("a")], [], "a").plan_id == ""


def test_restore_state_raises_without_snapshot():
    with pytest.raises(RewindUnavailable):
        restore_state([_task("a"), _task("b")], [], "b")


def test_parse_markers_ignores_invalid_entries():
    raw = '[{"before_task_id": "b", "cut_task_id": "c"}, 3, {"x": 1}]'
    markers = parse_markers(raw)
    assert len(markers) == 1
    assert markers[0].before_task_id == "b"
    assert parse_markers("not json") == []


def test_is_human_turn_rejects_internal_messages():
    def task_with(message: Message) -> Task:
        task = Task(id="t1", context_id="c1")
        task.history.append(message)
        return task

    human = Message(message_id="m1", role=Role.ROLE_USER)
    assert is_human_turn(task_with(human)) is True

    resume = Message(message_id="m2", role=Role.ROLE_USER)
    ParseDict({"choirworks.resume": {"kind": "resume"}}, resume.metadata)
    assert is_human_turn(task_with(resume)) is False

    agent = Message(message_id="m3", role=Role.ROLE_USER)
    ParseDict({A2A_ROOM_URI: {"sender": "echo"}}, agent.metadata)
    assert is_human_turn(task_with(agent)) is False

    assistant = Message(message_id="m4", role=Role.ROLE_AGENT)
    assert is_human_turn(task_with(assistant)) is False

    assert is_human_turn(Task(id="t2", context_id="c1")) is False
