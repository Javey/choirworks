from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

import structlog
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf import struct_pb2
from litellm.types.llms.openai import (
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

from choirworks.core.events import (
    CwType,
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

    from choirworks.core.tool import FunctionResult, FunctionTool
    from choirworks.orchestration.context import OrchestrationContext

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
            kind=CwType.THOUGHT,
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
            kind=CwType.TEXT,
            author="assistant",
            artifact_id=artifact_id,
            append=append,
            last_chunk=last_chunk,
        ),
    )


def _record_tool_exchange(
    ctx: OrchestrationContext,
    *,
    function_name: str,
    call_id: str,
    args_json: str,
    result_json: str,
) -> None:
    """Append the assistant tool call and its tool result to runtime messages.

    Mirrors the A2A function-call artifact one-to-one so ``rebuild`` can
    replay the exact same messages from the task store.
    """
    ctx.runtime.messages.append(
        OpenAIChatCompletionAssistantMessage(
            role="assistant",
            content=None,
            tool_calls=[
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": function_name, "arguments": args_json},
                }
            ],
        )
    )
    ctx.runtime.messages.append(
        ChatCompletionToolMessage(role="tool", tool_call_id=call_id, content=result_json)
    )


async def emit_function_call(
    ctx: OrchestrationContext,
    func: FunctionTool,
    args: BaseModel,
    result: FunctionResult,
    *,
    call_id: str = "",
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
) -> None:
    call_id = call_id or uuid.uuid4().hex
    args_json = args.model_dump_json()
    result_json = result.model_dump_json()
    logger.info(
        "emit_function_call",
        task_id=ctx.task_id,
        function=func.name,
        success=result.success,
        args=args.model_dump(),
    )
    _record_tool_exchange(
        ctx,
        function_name=func.name,
        call_id=call_id,
        args_json=args_json,
        result_json=result_json,
    )
    await emit(
        ctx,
        function_call_event(
            ctx.task_id,
            ctx.context_id,
            function_name=func.name,
            args=args.model_dump(),
            result=result.model_dump(),
            call_id=call_id,
            args_json=args_json,
            result_json=result_json,
        ),
    )
    await emit_event(ctx, state_name)


async def emit_function_error(
    ctx: OrchestrationContext,
    func: FunctionTool,
    error: str,
    *,
    call_id: str = "",
    args_json: str = "",
    state_name: TaskState = TaskState.TASK_STATE_FAILED,
) -> None:
    call_id = call_id or uuid.uuid4().hex
    args_json = args_json or "{}"
    result = {"success": False, "error": error}
    result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    logger.warning(
        "emit_function_error",
        task_id=ctx.task_id,
        function=func.name,
        error=error,
    )
    _record_tool_exchange(
        ctx,
        function_name=func.name,
        call_id=call_id,
        args_json=args_json,
        result_json=result_json,
    )
    await emit(
        ctx,
        function_call_event(
            ctx.task_id,
            ctx.context_id,
            function_name=func.name,
            args={},
            result=result,
            call_id=call_id,
            args_json=args_json,
            result_json=result_json,
        ),
    )
    await emit_event(ctx, state_name)


async def _record_message(
    ctx: OrchestrationContext,
    content: str,
    *,
    cw_type: CwType,
    role: Literal["user", "assistant"],
) -> None:
    """Append one message to runtime.messages and persist it as an artifact.

    The artifact carries ``cw_type`` and ``role`` in part metadata so
    ``rebuild`` can replay the exact same message from the task store.
    """
    if role == "user":
        ctx.runtime.messages.append(OpenAIChatCompletionUserMessage(role="user", content=content))
    else:
        ctx.runtime.messages.append(
            OpenAIChatCompletionAssistantMessage(role="assistant", content=content)
        )
    part = Part(text=content)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": cw_type, "role": role})
    part.metadata.CopyFrom(meta)
    logger.info(
        "record_message",
        task_id=ctx.task_id,
        cw_type=str(cw_type),
        role=role,
        content=content[:200],
    )
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=Artifact(
                artifact_id=uuid.uuid4().hex,
                parts=[part],
            ),
            append=False,
            last_chunk=True,
        )
    )


async def record_decision(ctx: OrchestrationContext, content: str) -> None:
    """Record a UI-visible orchestration decision (assistant message)."""
    await _record_message(ctx, content, cw_type=CwType.DECISION, role="assistant")


async def record_message(
    ctx: OrchestrationContext,
    content: str,
    *,
    role: Literal["user", "assistant"] = "assistant",
) -> None:
    """Record a history-only message: LLM context, hidden from the UI.

    Used for user turns and question/answer text that already has a dedicated
    UI surface (task history snapshot / question cards).
    """
    await _record_message(ctx, content, cw_type=CwType.HISTORY, role=role)


async def emit_compaction(
    ctx: OrchestrationContext,
    *,
    compacted_count: int,
    summary: str,
) -> None:
    """Persist a compaction artifact so ``rebuild`` can recover it on restart."""
    part = Part(text=summary)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": CwType.COMPACTION, "compacted_count": compacted_count})
    part.metadata.CopyFrom(meta)
    logger.info(
        "emit_compaction",
        task_id=ctx.task_id,
        compacted_count=compacted_count,
        summary_len=len(summary),
    )
    await ctx.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            artifact=Artifact(
                artifact_id=uuid.uuid4().hex,
                parts=[part],
            ),
            append=False,
            last_chunk=True,
        )
    )
