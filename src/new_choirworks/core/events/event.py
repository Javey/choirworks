from __future__ import annotations

import json
from dataclasses import asdict, dataclass
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
from google.protobuf import json_format, struct_pb2
from litellm.types.llms.openai import (
    ChatCompletionAssistantToolCall,
    ChatCompletionToolCallFunctionChunk,
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
)

from new_choirworks.core.events.node_path_builder import NodePathBuilder
from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import LLMResponse

from .event_actions import EventActions

if TYPE_CHECKING:
    from new_choirworks.core.agents.invocation_context import InvocationContext

A2AEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Message


PART_TYPE_FUNCTION_CALL = "function_call"
PART_TYPE_FUNCTION_RESPONSE = "function_response"
PART_METADATA_TYPE = "type"
PART_METADATA_THOUGHT = "thought"
ARTIFACT_METADATA_STRUCTURED_OUTPUT = "structured_output"

# metadata
EVENT_META_AUTHOR = "author"
EVENT_META_OUTPUT = "output"
EVENT_META_NODE_INFO = "node_info"
EVENT_META_BRANCH = "branch"
EVENT_META_ISOLATION_SCOPE = "isolation_scope"

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


@dataclass
class NodeInfo:
    """Workflow node metadata attached to an event（ADK 同构，扁平存储）。

    ``path`` 形如 ``wf@1/node_a@2``；``output_for`` 表示这条 output 事件
    同时属于哪些节点路径（嵌套 workflow / use_as_output 场景）；
    ``message_as_output`` 表示事件内容本身就是节点 output。
    """

    path: str = ""
    output_for: list[str] | None = None
    message_as_output: bool | None = None

    @property
    def run_id(self) -> str:
        return NodePathBuilder.from_string(self.path).run_id or ""

    @property
    def parent_run_id(self) -> str | None:
        parent = NodePathBuilder.from_string(self.path).parent
        return parent.run_id if parent else None


class EventMeta:
    _event: A2AEvent

    def __init__(self, event: A2AEvent):
        self._event = event

    @property
    def _metadata(self) -> struct_pb2.Struct:
        """Returns the persistence-safe metadata carrier for an A2A event.

        ``TaskArtifactUpdateEvent`` is persisted by the A2A SDK from
        ``event.artifact`` only, so its event-level metadata is dropped. Keep the
        metadata on ``artifact.metadata`` so author/node_info/actions/output and
        A2A ids survive persistence.
        """
        event = self._event
        if isinstance(event, TaskArtifactUpdateEvent):
            return event.artifact.metadata
        return event.metadata

    @property
    def author(self) -> str:
        if EVENT_META_AUTHOR not in self._metadata:
            return ""
        return cast(str, self._metadata[EVENT_META_AUTHOR])

    @author.setter
    def author(self, value: str) -> None:
        self._metadata[EVENT_META_AUTHOR] = value

    @property
    def output(self) -> object | None:
        if EVENT_META_OUTPUT not in self._metadata:
            return None
        return cast(object, json.loads(cast(str, self._metadata[EVENT_META_OUTPUT])))

    @output.setter
    def output(self, value: object) -> None:
        self._metadata[EVENT_META_OUTPUT] = json.dumps(value, default=str)

    @property
    def node_info(self) -> NodeInfo:
        if EVENT_META_NODE_INFO not in self._metadata:
            return NodeInfo()
        node = self._metadata[EVENT_META_NODE_INFO]
        if not isinstance(node, struct_pb2.Struct):
            return NodeInfo()
        path = cast(str, node["path"]) if "path" in node else ""
        output_for: list[str] | None = None
        if "output_for" in node:
            output_for = [
                cast(str, item) for item in cast(struct_pb2.ListValue, node["output_for"])
            ]
        message_as_output = (
            cast(bool, node["message_as_output"]) if "message_as_output" in node else None
        )
        return NodeInfo(
            path=path,
            output_for=output_for,
            message_as_output=message_as_output,
        )

    @node_info.setter
    def node_info(self, value: NodeInfo) -> None:
        node = struct_pb2.Struct()
        if value.path:
            node["path"] = value.path
        if value.output_for is not None:
            node["output_for"] = value.output_for
        if value.message_as_output is not None:
            node["message_as_output"] = value.message_as_output
        if not node.fields:
            if EVENT_META_NODE_INFO in self._metadata:
                del self._metadata[EVENT_META_NODE_INFO]
            return
        self._metadata[EVENT_META_NODE_INFO] = node

    @property
    def branch(self) -> str | None:
        if EVENT_META_BRANCH not in self._metadata:
            return None
        return cast(str, self._metadata[EVENT_META_BRANCH])

    @branch.setter
    def branch(self, value: str | None) -> None:
        if value is None:
            if EVENT_META_BRANCH in self._metadata:
                del self._metadata[EVENT_META_BRANCH]
            return
        self._metadata[EVENT_META_BRANCH] = value

    @property
    def isolation_scope(self) -> str | None:
        if EVENT_META_ISOLATION_SCOPE not in self._metadata:
            return None
        return cast(str, self._metadata[EVENT_META_ISOLATION_SCOPE])

    @isolation_scope.setter
    def isolation_scope(self, value: str | None) -> None:
        if value is None:
            if EVENT_META_ISOLATION_SCOPE in self._metadata:
                del self._metadata[EVENT_META_ISOLATION_SCOPE]
            return
        self._metadata[EVENT_META_ISOLATION_SCOPE] = value

    @property
    def partial(self) -> bool:
        """派生：A2A 用 artifact 的 append/last_chunk 表达 partial。"""
        return is_partial_event(self._event)

    @property
    def actions(self) -> EventActions:
        return EventActions(self._metadata)


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
