from __future__ import annotations

from typing import cast

from a2a.helpers.proto_helpers import (
    new_text_artifact_update_event,
    new_text_message,
    new_text_status_update_event,
)
from a2a.types.a2a_pb2 import Role, Task, TaskState
from google.protobuf import struct_pb2

from new_choirworks.core.a2a.events.event import a2a_meta
from new_choirworks.core.events.event import NodeInfo, meta


def _message():
    return new_text_message("hi", role=Role.ROLE_AGENT)


def _artifact(*, last_chunk: bool):
    return new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text="x", last_chunk=last_chunk
    )


def test_node_info_default() -> None:
    info = meta(_message()).node_info
    assert info.path == ""
    assert info.output_for is None
    assert info.message_as_output is None


def test_node_info_run_id() -> None:
    info = NodeInfo(path="wf@1/node_a@2")
    assert info.run_id == "2"
    assert info.parent_run_id == "1"
    assert NodeInfo(path="wf").run_id == ""
    assert NodeInfo(path="wf").parent_run_id is None


def test_node_info_roundtrip() -> None:
    event = _message()
    output_for = ["wf@1/node_a@2", "wf@1"]
    meta(event).node_info = NodeInfo(
        path="wf@1/node_a@2",
        output_for=output_for,
        message_as_output=True,
    )

    info = meta(event).node_info
    assert info.path == "wf@1/node_a@2"
    assert info.output_for == output_for
    assert info.message_as_output is True
    assert info.run_id == "2"


def test_node_info_replaces_and_clears_optional_fields() -> None:
    event = _message()
    m = meta(event)
    m.node_info = NodeInfo(path="wf@1", output_for=["wf@1"], message_as_output=True)

    m.node_info = NodeInfo(path="wf@1")

    info = m.node_info
    assert info.path == "wf@1"
    assert info.output_for is None
    assert info.message_as_output is None


def test_branch_and_isolation_scope() -> None:
    event = _message()
    m = meta(event)
    assert m.branch is None
    assert m.isolation_scope is None

    m.branch = "a.b"
    m.isolation_scope = "fc-1"
    assert m.branch == "a.b"
    assert m.isolation_scope == "fc-1"

    m.branch = None
    m.isolation_scope = None
    assert m.branch is None
    assert m.isolation_scope is None


def test_node_info_is_stored_as_nested_struct() -> None:
    event = _message()
    meta(event).node_info = NodeInfo(path="wf@1", output_for=["wf@1"], message_as_output=True)

    node = event.metadata["node_info"]
    assert isinstance(node, struct_pb2.Struct)
    assert node["path"] == "wf@1"
    assert list(cast(struct_pb2.ListValue, node["output_for"])) == ["wf@1"]
    assert node["message_as_output"] is True


def test_empty_node_info_removes_key() -> None:
    event = _message()
    meta(event).node_info = NodeInfo(path="wf@1")
    meta(event).node_info = NodeInfo()
    assert "node_info" not in event.metadata


def test_artifact_meta_uses_artifact_metadata() -> None:
    event = _artifact(last_chunk=True)
    meta(event).author = "writer"
    meta(event).node_info = NodeInfo(path="wf@1/a@2")
    meta(event).actions.state_delta = {"k": "v"}
    a2a_meta(event).task_id = "remote-1"

    assert "author" not in event.metadata
    assert event.artifact.metadata["author"] == "writer"
    assert "node_info" in event.artifact.metadata
    assert "actions.state_delta" in event.artifact.metadata
    assert event.artifact.metadata["a2a:task_id"] == "remote-1"


def test_message_meta_uses_event_metadata() -> None:
    event = _message()
    meta(event).author = "user"

    assert event.metadata["author"] == "user"


def test_status_meta_uses_event_metadata() -> None:
    event = new_text_status_update_event(
        task_id="t",
        context_id="c",
        state=TaskState.TASK_STATE_WORKING,
        text="working",
    )
    meta(event).author = "writer"

    assert event.metadata["author"] == "writer"


def test_artifact_metadata_survives_task_persistence() -> None:
    event = _artifact(last_chunk=True)
    meta(event).author = "writer"
    meta(event).actions.state_delta = {"k": "v"}
    a2a_meta(event).task_id = "remote-1"

    task = Task(id="t", context_id="c")
    task.artifacts.append(event.artifact)

    kept = task.artifacts[0]
    assert kept.metadata["author"] == "writer"
    assert "actions.state_delta" in kept.metadata
    assert kept.metadata["a2a:task_id"] == "remote-1"


def test_partial_is_derived_from_artifact_flags() -> None:
    assert meta(_artifact(last_chunk=False)).partial is True
    assert meta(_artifact(last_chunk=True)).partial is False
    assert meta(_message()).partial is False
