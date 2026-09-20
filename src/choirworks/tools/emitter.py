from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel

if TYPE_CHECKING:
    from choirworks.a2a.executor import ChoirWorksAgentExecutor, SessionRuntime
    from choirworks.tools.base import AgentFunction, FunctionResult


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


async def emit_function_call(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
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
    await runtime.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=runtime.task_id,
            context_id=runtime.context_id,
            artifact=artifact,
            append=False,
            last_chunk=True,
        )
    )
    await executor._emit_event(runtime, "", state_name)


async def emit_function_error(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
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
    await runtime.queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=runtime.task_id,
            context_id=runtime.context_id,
            artifact=artifact,
            append=False,
            last_chunk=True,
        )
    )
    await executor._emit_event(runtime, "", state_name)


async def emit_state_delta(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
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
    await executor._emit_event(
        runtime,
        "state_delta",
        state_name,
        **delta,
    )
