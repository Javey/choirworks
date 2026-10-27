from __future__ import annotations

from a2a.helpers.proto_helpers import new_text_artifact_update_event
from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

from new_choirworks.core.events.event import NodeInfo, meta
from new_choirworks.core.workflow.utils.rehydration_utils import (
    ChildScanState,
    reconstruct_node_states,
)


def _event(path: str) -> TaskArtifactUpdateEvent:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="x", last_chunk=True
    )
    meta(event).node_info = NodeInfo(path=path)
    return event


def test_direct_output_route_branch_scope() -> None:
    event = _event("wf@1")
    meta(event).output = "done"
    meta(event).actions.route = "next"
    meta(event).branch = "a.b"
    meta(event).isolation_scope = "fc-1"

    states = reconstruct_node_states([event], "wf@1")

    assert states["wf@1"] == ChildScanState(
        run_id="1",
        output="done",
        route="next",
        branch="a.b",
        isolation_scope="fc-1",
    )


def test_last_non_none_wins() -> None:
    first = _event("wf@1")
    meta(first).output = "first"
    meta(first).actions.route = "r1"
    second = _event("wf@1")
    meta(second).output = "second"
    meta(second).isolation_scope = "fc-2"

    state = reconstruct_node_states([first, second], "wf@1")["wf@1"]

    assert state.output == "second"
    assert state.route == "r1"
    assert state.isolation_scope == "fc-2"


def test_ignores_other_paths() -> None:
    event = _event("wf@2")
    meta(event).output = "other"

    assert reconstruct_node_states([event], "wf@1") == {}


def test_ignores_events_without_path() -> None:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="x", last_chunk=True
    )

    assert reconstruct_node_states([event], "wf@1") == {}


def test_base_without_run_id() -> None:
    event = _event("wf")
    meta(event).output = "done"

    state = reconstruct_node_states([event], "wf")["wf"]

    assert state.run_id is None
    assert state.output == "done"


def test_group_by_direct_child() -> None:
    a = _event("wf@1/a@1")
    meta(a).output = "a"
    b = _event("wf@1/b@2")
    meta(b).output = "b"

    states = reconstruct_node_states([a, b], "wf@1", group_by_direct_child=True)

    assert set(states) == {"a@1", "b@2"}
    assert states["a@1"].output == "a"
    assert states["a@1"].run_id == "1"
    assert states["b@2"].output == "b"
    assert states["b@2"].run_id == "2"


def test_group_ignores_base_path_events() -> None:
    event = _event("wf@1")
    meta(event).output = "base"

    assert reconstruct_node_states([event], "wf@1", group_by_direct_child=True) == {}


def test_group_ignores_non_descendants() -> None:
    event = _event("wf@2/a@1")
    meta(event).output = "other"

    assert reconstruct_node_states([event], "wf@1", group_by_direct_child=True) == {}


def test_group_deeper_descendant_has_no_output() -> None:
    event = _event("wf@1/a@1/leaf@1")
    meta(event).output = "deep"

    state = reconstruct_node_states([event], "wf@1", group_by_direct_child=True)["a@1"]

    assert state.run_id == "1"
    assert state.output is None


def test_group_last_non_none_wins() -> None:
    first = _event("wf@1/a@1")
    meta(first).output = "first"
    meta(first).actions.route = "r1"
    second = _event("wf@1/a@1")
    meta(second).output = "second"

    state = reconstruct_node_states([first, second], "wf@1", group_by_direct_child=True)["a@1"]

    assert state.output == "second"
    assert state.route == "r1"


def test_base_delegated_output_from_descendant() -> None:
    event = _event("wf@1/a@1")
    meta(event).node_info = NodeInfo(path="wf@1/a@1", output_for=["wf@1"])
    meta(event).output = "delegated"

    state = reconstruct_node_states([event], "wf@1")["wf@1"]

    assert state.output == "delegated"


def test_group_delegated_output_from_deeper_descendant() -> None:
    event = _event("wf@1/a@1/leaf@1")
    meta(event).node_info = NodeInfo(path="wf@1/a@1/leaf@1", output_for=["wf@1/a@1"])
    meta(event).output = "deep"

    state = reconstruct_node_states([event], "wf@1", group_by_direct_child=True)["a@1"]

    assert state.output == "deep"


def test_message_as_output() -> None:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="hello", last_chunk=True
    )
    meta(event).node_info = NodeInfo(path="wf@1", message_as_output=True)

    state = reconstruct_node_states([event], "wf@1")["wf@1"]

    assert state.output == "hello"


def test_partial_event_does_not_set_output_or_route() -> None:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="partial", last_chunk=False
    )
    meta(event).node_info = NodeInfo(path="wf@1")
    meta(event).output = "partial"
    meta(event).actions.route = "r"

    state = reconstruct_node_states([event], "wf@1")["wf@1"]

    assert state.output is None
    assert state.route is None


def test_delegated_without_output_does_not_apply() -> None:
    event = _event("wf@1/a@1")
    meta(event).node_info = NodeInfo(path="wf@1/a@1", output_for=["wf@1"])

    state = reconstruct_node_states([event], "wf@1")["wf@1"]

    assert state.output is None


def test_delegated_sets_route() -> None:
    event = _event("wf@1/a@1")
    meta(event).node_info = NodeInfo(path="wf@1/a@1", output_for=["wf@1"])
    meta(event).output = "x"
    meta(event).actions.route = "next"

    state = reconstruct_node_states([event], "wf@1")["wf@1"]

    assert state.output == "x"
    assert state.route == "next"
