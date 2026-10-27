from __future__ import annotations

import asyncio

from a2a.helpers.proto_helpers import new_text_artifact_update_event
from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

from new_choirworks.core.events.event import A2AEvent, NodeInfo, meta
from new_choirworks.core.workflow.utils.replay_manager import ReplayManager


def _event(
    path: str,
    *,
    output: str | None = None,
    route: str | None = None,
) -> TaskArtifactUpdateEvent:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="x", last_chunk=True
    )
    meta(event).node_info = NodeInfo(path=path)
    if output is not None:
        meta(event).output = output
    if route is not None:
        meta(event).actions.route = route
    return event


def test_scan_workflow_events_states_and_sequence() -> None:
    events: list[A2AEvent] = [
        _event("wf@1/a@1", output="a"),
        _event("wf@1/b@1", output="b"),
        _event("wf@1/a@2", output="a2"),
    ]
    manager = ReplayManager()

    states, sequence = manager.scan_workflow_events(events, "wf@1")

    assert set(states) == {"a@1", "b@1", "a@2"}
    assert sequence == ["a@1", "b@1", "a@2"]
    assert manager.sequence_barrier is not None


def test_non_terminal_event_not_in_sequence() -> None:
    events: list[A2AEvent] = [_event("wf@1/a@1")]
    manager = ReplayManager()

    _, sequence = manager.scan_workflow_events(events, "wf@1")

    assert sequence == []


def test_completed_echo_ignored() -> None:
    events: list[A2AEvent] = [
        _event("wf@1/a@1", output="first"),
        _event("wf@1/a@1", output="second"),
    ]
    manager = ReplayManager()

    _, sequence = manager.scan_workflow_events(events, "wf@1")

    assert sequence == ["a@1"]


def test_delegated_output_enters_sequence() -> None:
    event = _event("wf@1/a@1/leaf@1", output="deep")
    meta(event).node_info = NodeInfo(path="wf@1/a@1/leaf@1", output_for=["wf@1/a@1"])
    manager = ReplayManager()

    states, sequence = manager.scan_workflow_events([event], "wf@1")

    assert sequence == ["a@1"]
    assert states["a@1"].output == "deep"


def test_strict_parent_barrier_ignores_deeper_descendants() -> None:
    events: list[A2AEvent] = [
        _event("wf@1/a@1", output="a"),
        _event("wf@1/b@1/leaf@1", output="deep"),
    ]
    manager = ReplayManager()

    barrier = manager.prepare_parent_sequence_barrier(events, "wf@1")

    assert barrier.sequence == ["a@1"]


async def test_wait_and_advance_delegate_to_barrier() -> None:
    events: list[A2AEvent] = [
        _event("wf@1/a@1", output="a"),
        _event("wf@1/b@1", output="b"),
    ]
    manager = ReplayManager()
    _ = manager.prepare_parent_sequence_barrier(events, "wf@1")

    waiter = asyncio.create_task(manager.wait_sequence("wf@1", "b@1"))
    await asyncio.sleep(0)
    assert not waiter.done()

    await manager.advance_sequence("wf@1", "a@1")
    await asyncio.wait_for(waiter, timeout=0.1)


def test_empty_events() -> None:
    manager = ReplayManager()

    states, sequence = manager.scan_workflow_events([], "wf@1")

    assert states == {}
    assert sequence == []
