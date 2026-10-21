from collections.abc import AsyncGenerator, Sequence

from a2a.helpers.proto_helpers import (
    new_message,
    new_text_artifact_update_event,
    new_text_part,
    new_text_status_update_event,
)
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    StreamResponse,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import json_format

from new_choirworks.core.a2a.agent.remote_a2a_agent import RemoteA2aAgent
from new_choirworks.core.a2a.events.event import a2a_meta
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import (
    PART_METADATA_TYPE,
    PART_TYPE_FUNCTION_CALL,
    PART_TYPE_FUNCTION_RESPONSE,
    A2AEvent,
    add_author,
    extract_text,
    meta,
)
from new_choirworks.core.flows.llm_flows.context.fencing import (
    OTHER_AGENT_CONTEXT_PREAMBLE,
    QUOTED_CONTENT_BEGIN,
    QUOTED_CONTENT_END,
)
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context

AGENT = "remote"
OTHER = "bob"
TASK_ID = "t1"
CTX_ID = "c1"
REMOTE_TASK_ID = "rt"
REMOTE_CTX_ID = "rc"


# --- Helpers ---


def _user(text: str, context_id: str = "") -> Message:
    return new_message(
        parts=[new_text_part(text=text)],
        role=Role.ROLE_USER,
        context_id=context_id or None,
    )


def _final_artifact(
    author: str,
    text: str,
    *,
    remote_task_id: str = REMOTE_TASK_ID,
    remote_ctx: str | None = REMOTE_CTX_ID,
) -> TaskArtifactUpdateEvent:
    """A final artifact as it appears in ctx.events after _handle_a2a_response."""
    event = new_text_artifact_update_event(
        task_id=remote_task_id,
        context_id=remote_ctx or "",
        name="art",
        text=text,
        last_chunk=True,
    )
    a2a_meta(event).task_id = remote_task_id
    if remote_ctx:
        a2a_meta(event).context_id = remote_ctx
    meta(event).author = author
    event.task_id = TASK_ID
    event.context_id = CTX_ID
    return event


def _partial_artifact(author: str, text: str) -> TaskArtifactUpdateEvent:
    event = new_text_artifact_update_event(
        task_id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        name="art",
        text=text,
        last_chunk=False,
    )
    meta(event).author = author
    return event


def _function_call_only_artifact(author: str) -> TaskArtifactUpdateEvent:
    p = Part()
    p.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_CALL
    _ = json_format.ParseDict({"name": "tool", "args": {}}, p.data)
    event = TaskArtifactUpdateEvent(
        task_id=TASK_ID,
        context_id=CTX_ID,
        artifact=Artifact(artifact_id="aid", parts=[p]),
        last_chunk=True,
    )
    meta(event).author = author
    return event


def _agent() -> RemoteA2aAgent:
    return RemoteA2aAgent(name=AGENT, agent_url="http://example.com")


def _inv_ctx(events: Sequence[A2AEvent]) -> InvocationContext:
    return InvocationContext(events=list(events), task_id=TASK_ID, context_id=CTX_ID)


def _node_ctx(inv_ctx: InvocationContext | None = None) -> Context:
    return Context(invocation_context=inv_ctx or _inv_ctx([]), node=BaseNode(name="test"))


def _stream_message(msg: Message) -> StreamResponse:
    resp = StreamResponse()
    resp.message.CopyFrom(msg)
    return resp


def _stream_status_update(ev: TaskStatusUpdateEvent) -> StreamResponse:
    resp = StreamResponse()
    resp.status_update.CopyFrom(ev)
    return resp


def _stream_artifact_update(ev: TaskArtifactUpdateEvent) -> StreamResponse:
    resp = StreamResponse()
    resp.artifact_update.CopyFrom(ev)
    return resp


def _stream_task(task: Task) -> StreamResponse:
    resp = StreamResponse()
    resp.task.CopyFrom(task)
    return resp


def _remote_message(
    text: str = "hi",
    task_id: str = REMOTE_TASK_ID,
    context_id: str = REMOTE_CTX_ID,
) -> Message:
    return new_message(
        parts=[new_text_part(text=text)],
        role=Role.ROLE_AGENT,
        task_id=task_id,
        context_id=context_id,
    )


async def _collect(gen: AsyncGenerator[A2AEvent, None]) -> list[A2AEvent]:
    return [e async for e in gen]


# --- _build_message_parts_for_request ---


def test_build_single_user_message():
    agent = _agent()
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx([_user("hi")]))
    assert [p.text for p in parts] == ["hi"]
    assert context_id is None


def test_build_multiple_user_messages_preserve_order():
    agent = _agent()
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx([_user("a"), _user("b")]))
    assert [p.text for p in parts] == ["a", "b"]
    assert context_id is None


def test_build_break_at_own_response_skips_older():
    agent = _agent()
    events = [_user("old"), _final_artifact(AGENT, "x", remote_ctx=None), _user("new")]
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx(events))
    assert [p.text for p in parts] == ["new"]
    assert context_id is None


async def test_build_skips_failed_status_event_and_keeps_history():
    agent = _agent()
    failure = add_author(
        new_text_status_update_event(
            task_id=TASK_ID,
            context_id=CTX_ID,
            state=TaskState.TASK_STATE_FAILED,
            text="Failed to send message to remote A2A agent remote",
        ),
        AGENT,
    )
    events = [
        _user("first"),
        _final_artifact(AGENT, "ok", remote_ctx="R"),
        _user("second"),
        failure,
    ]

    parts, context_id = agent._build_message_parts_for_request(_inv_ctx(events))

    assert [p.text for p in parts] == ["second"]
    assert context_id == "R"


def test_is_remote_response_true_for_relayed_artifact():
    agent = _agent()
    assert agent._is_remote_response(_final_artifact(AGENT, "ok")) is True


def test_is_remote_response_false_for_failed_status():
    agent = _agent()
    failure = add_author(
        new_text_status_update_event(
            task_id=TASK_ID,
            context_id=CTX_ID,
            state=TaskState.TASK_STATE_FAILED,
            text="boom",
        ),
        AGENT,
    )
    assert agent._is_remote_response(failure) is False


def test_is_remote_response_false_for_other_agent():
    agent = _agent()
    assert agent._is_remote_response(_final_artifact(OTHER, "ok")) is False


def test_build_other_agent_message_fenced():
    agent = _agent()
    message = _remote_message(text="peer result")
    meta(message).author = OTHER
    events = [_user("a"), message]

    parts, _ = agent._build_message_parts_for_request(_inv_ctx(events))

    assert len(parts) == 3
    assert parts[0].text == "a"
    assert parts[1].text == OTHER_AGENT_CONTEXT_PREAMBLE
    assert "[bob] said:" in parts[2].text
    assert "peer result" in parts[2].text
    assert QUOTED_CONTENT_BEGIN in parts[2].text
    assert QUOTED_CONTENT_END in parts[2].text


def test_build_other_agent_final_artifact_fenced():
    agent = _agent()
    events = [_user("a"), _final_artifact(OTHER, "result")]
    parts, _ = agent._build_message_parts_for_request(_inv_ctx(events))
    assert len(parts) == 3
    assert parts[0].text == "a"
    assert parts[1].text == OTHER_AGENT_CONTEXT_PREAMBLE
    assert "[bob] said:" in parts[2].text
    assert "result" in parts[2].text
    assert QUOTED_CONTENT_BEGIN in parts[2].text
    assert QUOTED_CONTENT_END in parts[2].text


def test_build_other_agent_partial_artifact_not_collected():
    agent = _agent()
    events = [_user("a"), _partial_artifact(OTHER, "x")]
    parts, _ = agent._build_message_parts_for_request(_inv_ctx(events))
    assert [p.text for p in parts] == ["a"]


def test_build_empty_events_returns_empty():
    agent = _agent()
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx([]))
    assert parts == []
    assert context_id is None


def test_build_only_other_agent_artifact_no_user():
    agent = _agent()
    events = [_final_artifact(OTHER, "x")]
    parts, _ = agent._build_message_parts_for_request(_inv_ctx(events))
    assert len(parts) == 2
    assert parts[0].text == OTHER_AGENT_CONTEXT_PREAMBLE
    assert "[bob] said:" in parts[1].text


def test_build_context_id_from_own_response():
    agent = _agent()
    events = [
        _user("old"),
        _final_artifact(AGENT, "x", remote_ctx="R"),
        _user("new"),
    ]
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx(events))
    assert [p.text for p in parts] == ["new"]
    assert context_id == "R"


def test_build_context_id_none_when_remote_empty():
    agent = _agent()
    events = [
        _user("old"),
        _final_artifact(AGENT, "x", remote_ctx=None),
        _user("new"),
    ]
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx(events))
    assert [p.text for p in parts] == ["new"]
    assert context_id is None


def test_build_first_call_no_prior_response():
    agent = _agent()
    parts, context_id = agent._build_message_parts_for_request(_inv_ctx([_user("a")]))
    assert [p.text for p in parts] == ["a"]
    assert context_id is None


# --- _handle_a2a_response ---


async def test_handle_message_branch():
    agent = _agent()
    resp = _stream_message(_remote_message(text="hi"))
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, Message)
    assert event.task_id == TASK_ID
    assert event.context_id == CTX_ID
    assert meta(event).author == AGENT
    assert a2a_meta(event).task_id == REMOTE_TASK_ID
    assert a2a_meta(event).context_id == REMOTE_CTX_ID


async def test_handle_status_update_branch():
    agent = _agent()
    status_update = TaskStatusUpdateEvent(
        task_id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    resp = _stream_status_update(status_update)
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TaskStatusUpdateEvent)
    assert event.task_id == TASK_ID
    assert event.context_id == CTX_ID
    assert event.status.state == TaskState.TASK_STATE_WORKING
    assert meta(event).author == AGENT
    assert a2a_meta(event).task_id == REMOTE_TASK_ID
    assert a2a_meta(event).context_id == REMOTE_CTX_ID


async def test_handle_artifact_update_branch():
    agent = _agent()
    artifact_ev = new_text_artifact_update_event(
        task_id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        name="art",
        text="result",
        last_chunk=True,
    )
    resp = _stream_artifact_update(artifact_ev)
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TaskArtifactUpdateEvent)
    assert event.task_id == TASK_ID
    assert event.context_id == CTX_ID
    assert meta(event).author == AGENT
    assert a2a_meta(event).task_id == REMOTE_TASK_ID
    assert a2a_meta(event).context_id == REMOTE_CTX_ID


async def test_handle_task_branch_streaming():
    agent = _agent()
    task = Task(
        id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    resp = _stream_task(task)
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TaskStatusUpdateEvent)
    assert event.task_id == TASK_ID
    assert event.context_id == CTX_ID
    assert event.status.state == TaskState.TASK_STATE_WORKING
    assert meta(event).author == AGENT


async def test_handle_task_branch_keeps_remote_ids_in_metadata():
    agent = _agent()
    task = Task(
        id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    resp = _stream_task(task)
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, TaskStatusUpdateEvent)
    assert event.task_id == TASK_ID
    assert event.context_id == CTX_ID
    assert a2a_meta(event).task_id == REMOTE_TASK_ID
    assert a2a_meta(event).context_id == REMOTE_CTX_ID


async def test_build_uses_remote_context_id_after_task_response():
    agent = _agent()
    task = Task(
        id=REMOTE_TASK_ID,
        context_id=REMOTE_CTX_ID,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    response_events = await _collect(agent._handle_a2a_response(_stream_task(task), _inv_ctx([])))

    parts, context_id = agent._build_message_parts_for_request(
        _inv_ctx([*response_events, _user("next")])
    )

    assert [p.text for p in parts] == ["next"]
    assert context_id == REMOTE_CTX_ID


async def test_handle_empty_response_no_yield():
    agent = _agent()
    resp = StreamResponse()
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert events == []


async def test_handle_empty_context_id_not_stored():
    agent = _agent()
    msg = new_message(
        parts=[new_text_part(text="hi")],
        role=Role.ROLE_AGENT,
        task_id=REMOTE_TASK_ID,
        context_id="",
    )
    resp = _stream_message(msg)
    events = await _collect(agent._handle_a2a_response(resp, _inv_ctx([])))
    assert len(events) == 1
    event = events[0]
    assert a2a_meta(event).task_id == REMOTE_TASK_ID
    assert a2a_meta(event).context_id is None


# --- _promote_response_to_output ---


def test_promote_final_artifact_with_text():
    agent = _agent()
    event = _final_artifact(AGENT, "hello")
    ctx = _node_ctx()
    assert agent._promote_response_to_output(event, ctx) is True
    assert ctx.output == "hello"


def test_promote_partial_artifact_returns_false():
    agent = _agent()
    event = _partial_artifact(AGENT, "x")
    ctx = _node_ctx()
    assert agent._promote_response_to_output(event, ctx) is False
    assert ctx.output is None


def test_promote_artifact_without_text_returns_false():
    agent = _agent()
    event = _function_call_only_artifact(AGENT)
    ctx = _node_ctx()
    assert agent._promote_response_to_output(event, ctx) is False
    assert ctx.output is None


def test_promote_non_artifact_returns_false():
    agent = _agent()
    event = _user("hi")
    ctx = _node_ctx()
    assert agent._promote_response_to_output(event, ctx) is False
    assert ctx.output is None


# --- extract_text ---


def test_extract_text_pure_text():
    event = new_text_artifact_update_event(
        task_id=TASK_ID, context_id=CTX_ID, name="art", text="hello", last_chunk=True
    )
    assert extract_text(event) == "hello"


def test_extract_text_skips_function_call():
    text_part = new_text_part("hi")
    fc_part = Part()
    fc_part.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_CALL
    _ = json_format.ParseDict({"name": "tool", "args": {}}, fc_part.data)
    event = TaskArtifactUpdateEvent(
        task_id=TASK_ID,
        context_id=CTX_ID,
        artifact=Artifact(artifact_id="aid", parts=[text_part, fc_part]),
        last_chunk=True,
    )
    assert extract_text(event) == "hi"


def test_extract_text_skips_function_response():
    text_part = new_text_part("hi")
    fr_part = Part()
    fr_part.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_RESPONSE
    _ = json_format.ParseDict({"name": "tool", "response": {}}, fr_part.data)
    event = TaskArtifactUpdateEvent(
        task_id=TASK_ID,
        context_id=CTX_ID,
        artifact=Artifact(artifact_id="aid", parts=[text_part, fr_part]),
        last_chunk=True,
    )
    assert extract_text(event) == "hi"


def test_extract_text_only_function_call_returns_empty():
    fc_part = Part()
    fc_part.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_CALL
    _ = json_format.ParseDict({"name": "tool", "args": {}}, fc_part.data)
    event = TaskArtifactUpdateEvent(
        task_id=TASK_ID,
        context_id=CTX_ID,
        artifact=Artifact(artifact_id="aid", parts=[fc_part]),
        last_chunk=True,
    )
    assert extract_text(event) == ""


def test_extract_text_only_function_response_returns_empty():
    fr_part = Part()
    fr_part.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_RESPONSE
    _ = json_format.ParseDict({"name": "tool", "response": {}}, fr_part.data)
    event = TaskArtifactUpdateEvent(
        task_id=TASK_ID,
        context_id=CTX_ID,
        artifact=Artifact(artifact_id="aid", parts=[fr_part]),
        last_chunk=True,
    )
    assert extract_text(event) == ""
