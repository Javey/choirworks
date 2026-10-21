from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeGuard, cast

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from google.protobuf import json_format
from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import FunctionCall, FunctionResponse, LLMResponse

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


def add_author(event: A2AEvent, author: str) -> A2AEvent:
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
                {
                    "name": part.function_call.name,
                    "args": part.function_call.args or {},
                    "id": part.function_call.id,
                },
                p.data,
            )
            parts.append(p)
        if part.function_response is not None:
            p = Part()
            p.metadata[PART_METADATA_TYPE] = PART_TYPE_FUNCTION_RESPONSE
            _ = json_format.ParseDict(
                {
                    "name": part.function_response.name,
                    "id": part.function_response.id,
                    "response": part.function_response.response,
                },
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


def has_function_calls(event: A2AEvent) -> bool:
    """True if event's artifact contains any function_call parts."""
    if not isinstance(event, TaskArtifactUpdateEvent):
        return False
    return any(is_function_call_part(p) for p in event.artifact.parts)


def extract_text(event: A2AEvent, include_thought: bool = False) -> str:
    """Concatenate text parts from an artifact event, skipping thoughts by default."""
    if not isinstance(event, TaskArtifactUpdateEvent):
        return ""
    return "".join(
        p.text
        for p in event.artifact.parts
        if p.text and (include_thought or not is_thought_part(p))
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


def events_to_llm_messages(events: list[A2AEvent]) -> list[ChatMessage]:
    messages: list[ChatMessage] = []

    for event in events:
        if isinstance(event, Message) and event.role == Role.ROLE_USER:
            text = "".join(p.text for p in event.parts if p.text)
            if text:
                messages.append(
                    OpenAIChatCompletionUserMessage(
                        role="user",
                        content=text,
                    )
                )
        elif is_final_artifact(event):
            text_buf: list[str] = []
            function_calls: list[FunctionCall] = []
            function_responses: list[FunctionResponse] = []

            for part in event.artifact.parts:
                if part.text and not is_thought_part(part):
                    text_buf.append(part.text)
                elif is_function_call_part(part):
                    fc_data = json_format.MessageToDict(part.data)
                    function_calls.append(FunctionCall(**fc_data))
                elif is_function_response_part(part):
                    fr_data = json_format.MessageToDict(part.data)
                    function_responses.append(FunctionResponse(**fr_data))

            text = "".join(text_buf)
            if text or function_calls:
                messages.append(
                    OpenAIChatCompletionAssistantMessage(
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
                )
            if function_responses:
                for fr in function_responses:
                    messages.append(
                        ChatCompletionToolMessage(
                            role="tool",
                            content=json.dumps(fr.response),
                            tool_call_id=fr.id,
                        )
                    )

    return messages
