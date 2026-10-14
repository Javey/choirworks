from __future__ import annotations

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from google.protobuf import json_format

from new_choirworks.core.agents.invocation_context import InvocationContext
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


def to_a2a_artifact(
    ctx: InvocationContext,
    response: LLMResponse,
    *,
    artifact_id: str,
    append: bool = False,
    last_chunk: bool = False,
) -> TaskArtifactUpdateEvent:
    """Convert LLMResponse to A2A TaskArtifactUpdateEvent."""
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
            json_format.ParseDict(
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
            json_format.ParseDict(
                {
                    "name": part.function_response.name,
                    "id": part.function_response.id,
                    "response": part.function_response.response,
                },
                p.data,
            )
            parts.append(p)
    return TaskArtifactUpdateEvent(
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        artifact=Artifact(artifact_id=artifact_id, parts=parts),
        append=append,
        last_chunk=last_chunk,
    )
