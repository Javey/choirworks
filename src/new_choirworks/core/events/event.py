from __future__ import annotations

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)

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
    ctx: InvocationContext, response: LLMResponse, *, artifact_id: str
) -> TaskArtifactUpdateEvent:
    """Convert LLMResponse to A2A TaskArtifactUpdateEvent."""
    parts: list[Part] = []
    if response.content:
        for part in response.content.parts:
            parts.append(Part(text=part.text))
    return TaskArtifactUpdateEvent(
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        artifact=Artifact(artifact_id=artifact_id, parts=parts),
        append=False,
        last_chunk=False,
    )
