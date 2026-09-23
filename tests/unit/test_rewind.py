from __future__ import annotations

import pytest
from a2a.types.a2a_pb2 import Message, Role, Task, TaskState, TaskStatus
from google.protobuf.json_format import ParseDict

from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.orchestration.rewind import (
    REWIND_KEY,
    RewindUnavailable,
    apply_rewinds,
    extract_rewind,
    is_human_turn,
    restore_state,
)
from choirworks.orchestration.state import OrchestrationState, state_to_json


def _task(
    task_id: str, *, state: OrchestrationState | None = None
) -> Task:
    task = Task(
        id=task_id,
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    if state is not None:
        ParseDict({"choirworks.state": state_to_json(state)}, task.metadata)
    return task


def _rewind_task(before_task_id: str, context_id: str = "c1") -> Task:
    task = Task(
        id=f"rewind-{before_task_id}",
        context_id=context_id,
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    ParseDict({REWIND_KEY: before_task_id}, task.metadata)
    return task


def test_extract_rewind_returns_before_task_id():
    task = _rewind_task("t3")
    assert extract_rewind(task) == "t3"


def test_extract_rewind_returns_none_for_ordinary_task():
    assert extract_rewind(_task("t1")) is None


def test_apply_rewinds_no_markers():
    tasks = [_task("a"), _task("b"), _task("c")]
    assert [t.id for t in apply_rewinds(tasks)] == ["a", "b", "c"]


def test_apply_rewinds_hides_range_inclusive():
    # [a, b, c, REWIND(before=b), d] — b, c, and the marker are hidden;
    # d is a new turn after the rewind, so it stays visible.
    tasks = [_task("a"), _task("b"), _task("c"), _rewind_task("b"), _task("d")]
    assert [t.id for t in apply_rewinds(tasks)] == ["a", "d"]


def test_apply_rewinds_drops_marker_task():
    tasks = [_task("a"), _task("b"), _rewind_task("a")]
    assert [t.id for t in apply_rewinds(tasks)] == []


def test_apply_rewinds_chained_markers():
    # [a, b, c, REWIND(c), d, REWIND(d), e]
    # First REWIND(c) hides c + marker; second REWIND(d) hides d + marker;
    # e is a new turn after the second rewind, stays visible.
    tasks = [
        _task("a"),
        _task("b"),
        _task("c"),
        _rewind_task("c"),
        _task("d"),
        _rewind_task("d"),
        _task("e"),
    ]
    assert [t.id for t in apply_rewinds(tasks)] == ["a", "b", "e"]


def test_restore_state_returns_predecessor_snapshot():
    first = _task("a", state=OrchestrationState(plan_id="p1"))
    second = _task("b")
    state = restore_state([first, second], "b")
    assert state.plan_id == "p1"


def test_restore_state_without_predecessor_is_empty():
    assert restore_state([_task("a")], "a").plan_id == ""


def test_restore_state_raises_without_snapshot():
    with pytest.raises(RewindUnavailable):
        restore_state([_task("a"), _task("b")], "b")


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
