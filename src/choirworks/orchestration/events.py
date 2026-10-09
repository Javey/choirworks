from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import (
    Message,
    TaskState,
)

from choirworks.core.events import (
    TEXT,
    THOUGHT,
    chunk_event,
    emit,
    function_call_event,
    state_delta_event,
    status_event,
)
from choirworks.orchestration.state import (
    Intervention,
    InterventionDelta,
    MemberDelta,
    NodeDelta,
    pending_interventions,
)

if TYPE_CHECKING:
    from pydantic import BaseModel

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
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
    *,
    metadata: Mapping[str, object] | None = None,
    message: Message | None = None,
) -> None:
    effective = _effective_state(ctx, state_name)
    logger.info(
        "emit_event",
        task_id=ctx.task_id,
        state=TaskState.Name(effective),
        keys=list(metadata) if metadata else None,
    )
    await emit(
        ctx,
        status_event(ctx.task_id, ctx.context_id, effective, metadata=metadata, message=message),
    )


async def emit_intervention_rejected(
    ctx: OrchestrationContext,
    intervention_id: str,
    reason: str,
) -> None:
    """Report an answer that could not be applied (stale / malformed)."""
    await emit_event(
        ctx,
        metadata={"intervention_id": intervention_id, "reason": reason},
    )


async def emit_interventions_expired(
    ctx: OrchestrationContext, expired: list[Intervention]
) -> None:
    """Push an expired delta so the client tears down the reply affordance."""
    if not expired:
        return
    await emit_state_delta(
        ctx,
        interventions={
            iv.id: {
                "status": "expired",
                "node_id": iv.node_id,
                "kind": iv.kind,
            }
            for iv in expired
        },
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
        delta=delta,
    )
    await emit(
        ctx,
        state_delta_event(
            ctx.task_id,
            ctx.context_id,
            delta,
            state=_effective_state(ctx, state_name),
        ),
    )


async def emit_thought_chunk(
    ctx: OrchestrationContext,
    *,
    text: str,
    author: str,
    append: bool,
    last_chunk: bool,
    artifact_id: str,
) -> None:
    logger.info(
        "emit_thought_chunk",
        task_id=ctx.task_id,
        artifact_id=artifact_id,
        text=text,
        append=append,
        last_chunk=last_chunk,
    )
    await emit(
        ctx,
        chunk_event(
            ctx.task_id,
            ctx.context_id,
            text=text,
            kind=THOUGHT,
            author=author,
            artifact_id=artifact_id,
            append=append,
            last_chunk=last_chunk,
        ),
    )


async def emit_text_chunk(
    ctx: OrchestrationContext,
    *,
    text: str,
    append: bool,
    last_chunk: bool,
    artifact_id: str,
) -> None:
    logger.info(
        "emit_text_chunk",
        task_id=ctx.task_id,
        artifact_id=artifact_id,
        text=text,
        append=append,
        last_chunk=last_chunk,
    )
    await emit(
        ctx,
        chunk_event(
            ctx.task_id,
            ctx.context_id,
            text=text,
            kind=TEXT,
            author="assistant",
            artifact_id=artifact_id,
            append=append,
            last_chunk=last_chunk,
        ),
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
        args=args.model_dump(),
    )
    await emit(
        ctx,
        function_call_event(
            ctx.task_id,
            ctx.context_id,
            function_name=func.name,
            args=args.model_dump(),
            result=result.model_dump(),
        ),
    )
    await emit_event(ctx, state_name)


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
    await emit(
        ctx,
        function_call_event(
            ctx.task_id,
            ctx.context_id,
            function_name=func.name,
            args={},
            result={"success": False, "error": error},
        ),
    )
    await emit_event(ctx, state_name)
