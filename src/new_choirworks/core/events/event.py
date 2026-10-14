from __future__ import annotations

import json

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
)

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.models.llm_request import ChatMessage
from new_choirworks.core.models.llm_response import LLMResponse

A2AEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Message


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
                p.metadata["thought"] = "true"
            parts.append(p)
        if part.function_call is not None:
            p = Part()
            p.metadata["type"] = "function_call"
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
            p.metadata["type"] = "function_response"
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

    if not append and last_chunk:
        msg = to_chat_message(response)
        if msg is not None:
            ctx.messages.append(msg)

    return event
