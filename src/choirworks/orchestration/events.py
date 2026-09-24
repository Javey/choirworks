from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel

from choirworks.a2a.wire import data_part, function_call_part, status_update, struct
from choirworks.orchestration.state import (
    Intervention,
    InterventionDelta,
    MemberDelta,
    NodeDelta,
    pending_interventions,
)

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext
    from choirworks.tools.base import AgentFunction, FunctionResult

logger = structlog.get_logger(__name__)

_TERMINAL_TASK_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
}


def _effective_state(ctx: OrchestrationContext, state_name: TaskState) -> TaskState:
    """Pending questions keep the task in input_required (§4.3)."""
    if state_name in _TERMINAL_TASK_STATES:
        return state_name
    if pending_interventions(ctx.state):
        return TaskState.TASK_STATE_INPUT_REQUIRED
    return state_name


async def emit_event(
    ctx: OrchestrationContext,
    kind: str,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
    *,
    metadata: Mapping[str, object] | None = None,
    message: Message | None = None,
) -> None:
    effective = _effective_state(ctx, state_name)
    logger.info(
        "emit_event",
        task_id=ctx.task_id,
        kind=kind or "(none)",
        state=effective,
    )
    await ctx.queue.enqueue_event(
        status_update(
            ctx.task_id,
            ctx.context_id,
            effective,
            kind=kind,
            metadata=metadata,
            message=message,
        )
    )


def build_questions_message(
    ctx: OrchestrationContext,
    pending: list[Intervention],
) -> Message:
    """Aggregate pending questions into one agent message (text + data parts)."""
    parts: list[Part] = []
    for intervention in pending:
        parts.append(Part(text=intervention.question))
        parts.append(
            data_part(
                {
                    "intervention_id": intervention.id,
                    "node_id": intervention.node_id,
                    "requester": intervention.requester,
                    "kind": intervention.kind,
                    "question_type": intervention.question_type,
                    "options": list(intervention.options),
                    "multi": intervention.multi,
                    "question": intervention.question,
                },
                {"cw_type": "question"},
            )
        )
    return Message(
        role=Role.ROLE_AGENT,
        message_id=uuid.uuid4().hex,
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        parts=parts,
    )


async def emit_pending_questions(ctx: OrchestrationContext) -> None:
    """Emit the aggregated input-required question message (set changed)."""
    pending = pending_interventions(ctx.state)
    if not pending:
        return
    await emit_event(
        ctx,
        "questions",
        TaskState.TASK_STATE_INPUT_REQUIRED,
        message=build_questions_message(ctx, pending),
    )


async def emit_intervention_rejected(
    ctx: OrchestrationContext,
    intervention_id: str,
    reason: str,
) -> None:
    """Report an answer that could not be applied (stale / malformed)."""
    await emit_event(
        ctx,
        "intervention.rejected",
        metadata={"intervention_id": intervention_id, "reason": reason},
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
        "emit_state_delta",
        task_id=ctx.task_id,
        keys=list(delta.keys()),
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
        task_id=ctx.task_id,
        artifact_id=artifact_id,
        length=len(text),
        append=append,
        last_chunk=last_chunk,
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
        task_id=ctx.task_id,
        artifact_id=artifact_id,
        length=len(text),
        append=append,
        last_chunk=last_chunk,
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
        task_id=ctx.task_id,
        function=func.name,
        success=result.success,
    )
    part = function_call_part(func.name, args.model_dump(), result.model_dump())
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
        task_id=ctx.task_id,
        function=func.name,
        error=error,
    )
    part = function_call_part(func.name, {}, {"success": False, "error": error})
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
