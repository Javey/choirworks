from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, TypeGuard, cast

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from google.protobuf import json_format
from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
)

from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import LLMResponse

if TYPE_CHECKING:
    from new_choirworks.core.agents.invocation_context import InvocationContext

A2AEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Message

# Must come after A2AEvent is defined to break the event_actions → base_node → event cycle.
from .event_actions import EventActions  # noqa: E402

PART_TYPE_FUNCTION_CALL = "function_call"
PART_TYPE_FUNCTION_RESPONSE = "function_response"
PART_METADATA_TYPE = "type"
PART_METADATA_THOUGHT = "thought"
ARTIFACT_METADATA_STRUCTURED_OUTPUT = "structured_output"

# metadata
EVENT_META_AUTHOR = "author"
EVENT_META_OUTPUT = "output"
EVENT_META_NODE_PATH = "node_path"

# Task states that finish the current interaction, including deliberate pauses.
TASK_RESPONSE_BOUNDARY_STATES = frozenset(
    {
        TaskState.TASK_STATE_INPUT_REQUIRED,
        TaskState.TASK_STATE_AUTH_REQUIRED,
        TaskState.TASK_STATE_COMPLETED,
        TaskState.TASK_STATE_FAILED,
        TaskState.TASK_STATE_CANCELED,
        TaskState.TASK_STATE_REJECTED,
    }
)

# States whose content is provisional and must not be promoted to node output.
NON_FINAL_TASK_STATES = frozenset(
    {
        TaskState.TASK_STATE_UNSPECIFIED,
        TaskState.TASK_STATE_SUBMITTED,
        TaskState.TASK_STATE_WORKING,
        TaskState.TASK_STATE_INPUT_REQUIRED,
        TaskState.TASK_STATE_AUTH_REQUIRED,
    }
)


class EventMeta:
    _event: A2AEvent
    _actions: EventActions

    def __init__(self, event: A2AEvent):
        self._event = event
        self._actions = EventActions(event)

    @property
    def author(self) -> str:
        if EVENT_META_AUTHOR not in self._event.metadata:
            return ""
        return cast(str, self._event.metadata[EVENT_META_AUTHOR])

    @author.setter
    def author(self, value: str) -> None:
        self._event.metadata[EVENT_META_AUTHOR] = value

    @property
    def output(self) -> object | None:
        if EVENT_META_OUTPUT not in self._event.metadata:
            return None
        return cast(object, json.loads(cast(str, self._event.metadata[EVENT_META_OUTPUT])))

    @output.setter
    def output(self, value: object) -> None:
        self._event.metadata[EVENT_META_OUTPUT] = json.dumps(value, default=str)

    @property
    def node_path(self) -> str:
        if EVENT_META_NODE_PATH not in self._event.metadata:
            return ""
        return cast(str, self._event.metadata[EVENT_META_NODE_PATH])

    @node_path.setter
    def node_path(self, value: str) -> None:
        self._event.metadata[EVENT_META_NODE_PATH] = value

    @property
    def actions(self) -> EventActions:
        return self._actions


def meta(event: A2AEvent) -> EventMeta:
    """Get typed EventMeta view for an A2A event."""
    return EventMeta(event)


def add_author[T: A2AEvent](event: T, author: str) -> T:
    """Add author metadata to an A2A event."""
    meta(event).author = author
    return event


def to_a2a_message(response: LLMResponse) -> Message:
    """Convert LLMResponse to A2A Message."""
    msg = Message()
    msg.role = Role.ROLE_AGENT
    if response.content:
        for part in response.content.parts:
            msg.parts.append(Part(text=part.text))
    return msg


def to_chat_message(response: LLMResponse) -> ChatMessage | None:
    """Convert LLMResponse to ChatMessage for LLM context.

    Filters out thought parts — thought is preserved in A2A artifacts for replay,
    not passed to the LLM.
    """
    if not response.content:
        return None

    text = "".join(
        part.text for part in response.content.parts if part.text is not None and not part.thought
    )
    function_calls = [
        part.function_call for part in response.content.parts if part.function_call is not None
    ]
    function_responses = [
        part.function_response
        for part in response.content.parts
        if part.function_response is not None
    ]

    if function_responses:
        fr = function_responses[0]
        return ChatCompletionToolMessage(
            role="tool",
            content=json.dumps(fr.response),
            tool_call_id=fr.id,
        )

    if text or function_calls:
        return OpenAIChatCompletionAssistantMessage(
            role="assistant",
            content=text or None,
            tool_calls=[
                ChatCompletionAssistantToolCall(
                    id=fc.id,
                    type="function",
                    function=ChatCompletionToolCallFunctionChunk(
                        name=fc.name,
                        arguments=json.dumps(fc.args),
                    ),
                )
                for fc in function_calls
            ]
            or None,
        )

    return None


def to_a2a_artifact(
    ctx: InvocationContext,
    response: LLMResponse,
    *,
    artifact_id: str,
    append: bool = False,
    last_chunk: bool = False,
) -> TaskArtifactUpdateEvent:
    """Convert LLMResponse to A2A TaskArtifactUpdateEvent.

    When ``not append and last_chunk``, automatically appends the corresponding
    ChatMessage to ``ctx.messages`` for LLM context.
    """
    assert response.content

    parts: list[Part] = []
    for part in response.content.parts:
        if part.text is not None:
            p = Part(text=part.text)
            if part.thought:
                p.metadata[PART_METADATA_THOUGHT] = True
            parts.append(p)
        if part.function_call is not None:
            p = Part()
            p.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_CALL
            _ = json_format.ParseDict(
                asdict(part.function_call),
                p.data,
            )
            parts.append(p)
        if part.function_response is not None:
            p = Part()
            p.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_RESPONSE
            _ = json_format.ParseDict(
                asdict(part.function_response),
                p.data,
            )
            parts.append(p)

    event = TaskArtifactUpdateEvent(
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        artifact=Artifact(artifact_id=artifact_id, parts=parts),
        append=append,
        last_chunk=last_chunk,
    )

    return event


def is_final_artifact(event: A2AEvent) -> TypeGuard[TaskArtifactUpdateEvent]:
    """True if event is a final (non-streaming, non-append) artifact event."""
    return isinstance(event, TaskArtifactUpdateEvent) and not event.append and event.last_chunk


def is_partial_event(event: A2AEvent) -> bool:
    """True if event is an incomplete artifact snapshot."""
    return isinstance(event, TaskArtifactUpdateEvent) and not is_final_artifact(event)


def event_content_parts(event: A2AEvent) -> list[Part]:
    """Return parts that carry presentable content for an A2A event.

    Handles Message, final artifacts, and boundary-status messages. Working
    status messages are progress updates and are intentionally excluded.
    """
    # TODO: append=True 的 artifact 需要按 artifact_id 聚合；聚合完成前按
    # partial 处理，这里不返回内容。
    if isinstance(event, Message):
        return list(event.parts)
    if is_final_artifact(event):
        return list(event.artifact.parts)
    if isinstance(event, TaskStatusUpdateEvent):
        if event.status.state in TASK_RESPONSE_BOUNDARY_STATES:
            return list(event.status.message.parts)
    return []


def has_function_calls(event: A2AEvent) -> bool:
    """True if event's presentable content contains any function_call parts."""
    return any(is_function_call_part(p) for p in event_content_parts(event))


def extract_text(event: A2AEvent, include_thought: bool = False) -> str:
    """Concatenate text parts from an event's presentable content.

    Skips thoughts (unless include_thought), function calls, and function responses.
    """
    return "".join(
        p.text
        for p in event_content_parts(event)
        if p.text
        and not is_function_call_part(p)
        and not is_function_response_part(p)
        and (include_thought or not is_thought_part(p))
    )


def is_thought_part(part: Part) -> bool:
    """True if part is a thought."""
    return PART_METADATA_THOUGHT in part.metadata and cast(
        bool, part.metadata[PART_METADATA_THOUGHT]
    )


def is_function_call_part(part: Part) -> bool:
    """True if part is a function_call."""
    return (
        PART_METADATA_TYPE in part.metadata
        and part.metadata[PART_METADATA_TYPE] == PART_TYPE_FUNCTION_CALL
    )


def is_function_response_part(part: Part) -> bool:
    """True if part is a function_response."""
    return (
        PART_METADATA_TYPE in part.metadata
        and part.metadata[PART_METADATA_TYPE] == PART_TYPE_FUNCTION_RESPONSE
    )
