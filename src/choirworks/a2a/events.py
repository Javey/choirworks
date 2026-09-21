from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel

from choirworks.a2a.helpers import function_call_part, status_update, struct
from choirworks.a2a.state import MemberDelta

if TYPE_CHECKING:
    from choirworks.a2a.context import OrchestrationContext
    from choirworks.tools.base import AgentFunction, FunctionResult


async def emit_event(
    ctx: OrchestrationContext,
    kind: str,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
    **metadata: Any,
) -> None:
    await ctx.queue.enqueue_event(
        status_update(
            ctx.task_id,
            ctx.context_id,
            state_name,
            kind=kind,
            **metadata,
        )
    )


async def emit_state_delta(
    ctx: OrchestrationContext,
    *,
    nodes: dict[str, dict[str, Any]] | None = None,
    members: list[MemberDelta] | None = None,
    interventions: dict[str, dict[str, Any]] | None = None,
    state_name: int = TaskState.TASK_STATE_WORKING,
) -> None:
    delta: dict[str, Any] = {}
    if nodes:
        delta["nodes"] = nodes
    if members:
        delta["members"] = members
    if interventions:
        delta["interventions"] = interventions
    if not delta:
        return
    await emit_event(ctx, "state_delta", state_name, **delta)


async def emit_thought_chunk(
    ctx: OrchestrationContext,
    *,
    text: str,
    author: str,
    append: bool,
    last_chunk: bool,
    artifact_id: str,
) -> None:
    part = Part(text=text)
    ParseDict({"cw_thought": True}, part.metadata)
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=Artifact(
                artifact_id=artifact_id,
                parts=[part],
                metadata=struct({"author": author}),
            ),
            append=append,
            last_chunk=last_chunk,
        )
    )


async def emit_text_chunk(
    ctx: OrchestrationContext,
    *,
    text: str,
    append: bool,
    last_chunk: bool,
    artifact_id: str,
) -> None:
    part = Part(text=text)
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=Artifact(
                artifact_id=artifact_id,
                parts=[part],
                metadata=struct({"author": "assistant"}),
            ),
            append=append,
            last_chunk=last_chunk,
        )
    )


async def emit_function_call(
    ctx: OrchestrationContext,
    func: AgentFunction,
    args: BaseModel,
    result: FunctionResult,
    *,
    state_name: int = TaskState.TASK_STATE_WORKING,
) -> None:
    part = function_call_part(
        func.name, args.model_dump(), result.model_dump()
    )
    artifact = Artifact(
        artifact_id=uuid.uuid4().hex,
        parts=[part],
    )
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=artifact,
            append=False,
            last_chunk=True,
        )
    )
    await emit_event(ctx, "", state_name)


async def emit_function_error(
    ctx: OrchestrationContext,
    func: AgentFunction,
    error: str,
    *,
    state_name: int = TaskState.TASK_STATE_FAILED,
) -> None:
    part = function_call_part(
        func.name, {}, {"success": False, "error": error}
    )
    artifact = Artifact(
        artifact_id=uuid.uuid4().hex,
        parts=[part],
    )
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=artifact,
            append=False,
            last_chunk=True,
        )
    )
    await emit_event(ctx, "", state_name)
