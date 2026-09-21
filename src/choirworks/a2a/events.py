from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import ParseDict

from choirworks.a2a.context import OrchestrationContext
from choirworks.tools.base import AgentFunction, FunctionResult

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _struct(data: dict[str, Any]) -> struct_pb2.Struct:
    result = struct_pb2.Struct()
    ParseDict(_strip_none(data), result)
    return result


def _join_text(parts: Any) -> str:
    return "\n".join(p.text for p in parts if p.HasField("text"))


def _strip_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_strip_none(item) for item in value]
    return value


def _status_update(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    kind: str | None = None,
    **metadata: Any,
) -> TaskStatusUpdateEvent:
    meta: dict[str, Any] = {}
    if kind:
        meta["kind"] = kind
    meta.update(metadata)
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()
    return TaskStatusUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        status=TaskStatus(state=state, timestamp=timestamp),
        metadata=_struct(meta) if meta else None,
    )


def _function_call_part(
    func_name: str,
    args: dict[str, Any],
    result: dict[str, Any],
) -> Part:
    data_value = struct_pb2.Value()
    ParseDict(
        {
            "function_name": func_name,
            "function_args": args,
            "function_result": result,
        },
        data_value,
    )
    part_meta = struct_pb2.Struct()
    part_meta.update({"cw_type": "function_call"})
    part = Part()
    part.data.CopyFrom(data_value)
    part.metadata.CopyFrom(part_meta)
    return part


class EventEmitter:
    """Event emission layer — all components emit through this.

    Replaces the executor's ``_emit_event`` / ``_emit_thought_chunk`` /
    ``_emit_text_chunk`` methods and the module-level ``emit_function_call`` /
    ``emit_function_error`` / ``emit_state_delta`` helpers.
    """

    async def emit_event(
        self,
        ctx: OrchestrationContext,
        kind: str,
        state_name: TaskState = TaskState.TASK_STATE_WORKING,
        **metadata: Any,
    ) -> None:
        await ctx.queue.enqueue_event(
            _status_update(
                ctx.task_id,
                ctx.context_id,
                state_name,
                kind=kind,
                **metadata,
            )
        )

    async def emit_state_delta(
        self,
        ctx: OrchestrationContext,
        *,
        nodes: dict[str, dict[str, Any]] | None = None,
        members: list[dict[str, Any]] | None = None,
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
        await self.emit_event(ctx, "state_delta", state_name, **delta)

    async def emit_thought_chunk(
        self,
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
                    metadata=_struct({"author": author}),
                ),
                append=append,
                last_chunk=last_chunk,
            )
        )

    async def emit_text_chunk(
        self,
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
                    metadata=_struct({"author": "assistant"}),
                ),
                append=append,
                last_chunk=last_chunk,
            )
        )

    async def emit_function_call(
        self,
        ctx: OrchestrationContext,
        func: AgentFunction,
        args: BaseModel,
        result: FunctionResult,
        *,
        state_name: int = TaskState.TASK_STATE_WORKING,
    ) -> None:
        part = _function_call_part(
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
        await self.emit_event(ctx, "", state_name)

    async def emit_function_error(
        self,
        ctx: OrchestrationContext,
        func: AgentFunction,
        error: str,
        *,
        state_name: int = TaskState.TASK_STATE_FAILED,
    ) -> None:
        part = _function_call_part(
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
        await self.emit_event(ctx, "", state_name)
