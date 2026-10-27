from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from a2a.helpers.proto_helpers import (
    new_text_artifact_update_event,
    new_text_message,
    new_text_status_update_event,
)
from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf import json_format

from new_choirworks.core.events.event import (
    PART_METADATA_TYPE,
    PART_TYPE_FUNCTION_CALL,
    PART_TYPE_FUNCTION_RESPONSE,
    NodeInfo,
    meta,
)
from new_choirworks.core.flows.llm_flows.context.contents import get_contents


def _user(text: str):
    return new_text_message(text, role=Role.ROLE_USER)


def _artifact(
    text: str,
    *,
    author: str = "writer",
    path: str | None = None,
    branch: str | None = None,
    isolation_scope: str | None = None,
):
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text=text, last_chunk=True
    )
    meta(event).author = author
    if path is not None:
        meta(event).node_info = NodeInfo(path=path)
    if branch is not None:
        meta(event).branch = branch
    if isolation_scope is not None:
        meta(event).isolation_scope = isolation_scope
    return event


def _status(state: TaskState, text: str):
    return new_text_status_update_event(task_id="t", context_id="c", state=state, text=text)


def _as_dict(message: object) -> dict[str, object]:
    return cast(dict[str, object], message)


def _assistant_contents(messages: Sequence[object]) -> list[object]:
    return [
        _as_dict(message).get("content")
        for message in messages
        if _as_dict(message).get("role") == "assistant"
    ]


def test_user_and_agent_messages() -> None:
    messages = get_contents([_user("hello"), _artifact("draft")], agent_name="writer")

    assert _as_dict(messages[0])["role"] == "user"
    assert _as_dict(messages[0])["content"] == "hello"
    assert _as_dict(messages[1])["role"] == "assistant"
    assert _as_dict(messages[1])["content"] == "draft"


def test_branch_filter() -> None:
    included = get_contents(
        [_artifact("a", branch="a"), _artifact("b", branch="x")],
        agent_name="writer",
        branch="a.b",
    )

    assert _assistant_contents(included) == ["a"]


def test_isolation_scope_filter() -> None:
    included = get_contents(
        [
            _artifact("a", isolation_scope="s1"),
            _artifact("b", isolation_scope="s2"),
        ],
        agent_name="writer",
        isolation_scope="s1",
    )

    assert _assistant_contents(included) == ["a"]


def test_node_path_filter() -> None:
    included = get_contents(
        [
            _artifact("a", path="wf@1/a@1"),
            _artifact("b", path="wf@2/b@1"),
        ],
        agent_name="writer",
        node_path="wf@1",
    )

    assert _assistant_contents(included) == ["a"]


def test_status_boundary_message_included() -> None:
    messages = get_contents(
        [_status(TaskState.TASK_STATE_COMPLETED, "done")],
        agent_name="writer",
    )

    assert _as_dict(messages[0])["role"] == "assistant"
    assert _as_dict(messages[0])["content"] == "done"


def test_working_status_message_ignored() -> None:
    messages = get_contents(
        [_status(TaskState.TASK_STATE_WORKING, "working")],
        agent_name="writer",
    )

    assert messages == []


def test_function_call_and_response_pairing() -> None:
    call = Part()
    call.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_CALL
    _ = json_format.ParseDict({"id": "c1", "name": "tool", "args": {"x": 1}}, call.data)
    response = Part()
    response.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_RESPONSE
    _ = json_format.ParseDict({"id": "c1", "name": "tool", "response": {"ok": True}}, response.data)
    event = TaskArtifactUpdateEvent(
        task_id="t",
        context_id="c",
        artifact=Artifact(artifact_id="a1", parts=[call, response]),
        append=False,
        last_chunk=True,
    )
    meta(event).author = "writer"

    messages = get_contents([event], agent_name="writer")

    assistant = _as_dict(messages[0])
    tool_calls = cast(list[dict[str, object]], assistant["tool_calls"])
    assert assistant["role"] == "assistant"
    assert tool_calls[0]["id"] == "c1"
    tool_message = _as_dict(messages[1])
    assert tool_message["role"] == "tool"
    assert tool_message["tool_call_id"] == "c1"
