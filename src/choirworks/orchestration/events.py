from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel

from choirworks.a2a.wire import function_call_part, status_update, struct
from choirworks.orchestration.state import (
    InterventionDelta,
    MemberDelta,
    NodeDelta,
)

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext
    from choirworks.tools.base import AgentFunction, FunctionResult

logger = structlog.get_logger(__name__)


async def emit_event(
    ctx: OrchestrationContext,
    kind: str,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
    *,
    metadata: Mapping[str, object] | None = None,
) -> None:
    logger.info(
        "emit_event",
        task_id=ctx.task_id, kind=kind or "(none)", state=state_name,
    )
    await ctx.queue.enqueue_event(
        status_update(
            ctx.task_id,
            ctx.context_id,
            state_name,
            kind=kind,
            metadata=metadata,
        )
    )


async def emit_state_delta(
    ctx: OrchestrationContext,
    *,
    nodes: dict[str, NodeDelta] | None = None,
    members: list[MemberDelta] | None = None,
    interventions: dict[str, InterventionDelta] | None = None,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
) -> None:
    delta: dict[str, object] = {}
    if nodes:
        delta["nodes"] = nodes
    if members:
        delta["members"] = members
    if interventions:
        delta["interventions"] = interventions
    if not delta:
        return
    logger.info(
        "emit_state_delta", task_id=ctx.task_id, keys=list(delta.keys()),
    )
    await emit_event(ctx, "state_delta", state_name, metadata=delta)


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
    logger.info(
        "emit_thought_chunk",
        task_id=ctx.task_id, artifact_id=artifact_id, length=len(text),
        append=append, last_chunk=last_chunk,
    )
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
    logger.info(
        "emit_text_chunk",
        task_id=ctx.task_id, artifact_id=artifact_id, length=len(text),
        append=append, last_chunk=last_chunk,
    )
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
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
) -> None:
    logger.info(
        "emit_function_call",
        task_id=ctx.task_id, function=func.name, success=result.success,
    )
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
    state_name: TaskState = TaskState.TASK_STATE_FAILED,
) -> None:
    logger.warning(
        "emit_function_error",
        task_id=ctx.task_id, function=func.name, error=error,
    )
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
