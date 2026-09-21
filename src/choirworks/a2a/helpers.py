from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from a2a.types.a2a_pb2 import (
    Part,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel

if TYPE_CHECKING:
    from choirworks.a2a.context import OrchestrationContext
    from choirworks.a2a.state import MemberDelta
    from choirworks.tools.base import AgentFunction, FunctionResult
MAX_METADATA_OUTPUT = 2000


def now_iso() -> str:
    """Current UTC timestamp in ISO format."""
    return datetime.now(UTC).isoformat()


def truncate(
    text: str | None, limit: int = MAX_METADATA_OUTPUT
) -> str | None:
    """Truncate text to *limit* characters, returning None for None input."""
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit]


def strip_none(value: Any) -> Any:
    """Recursively remove None values from dicts and lists."""
    if isinstance(value, dict):
        return {
            key: strip_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [strip_none(item) for item in value]
    return value


def struct(data: dict[str, Any]) -> struct_pb2.Struct:
    """Build a protobuf Struct from a dict, stripping None values first."""
    result = struct_pb2.Struct()
    ParseDict(strip_none(data), result)
    return result


def join_text(parts: Any) -> str:
    """Join protobuf parts' text fields with newlines."""
    return "\n".join(p.text for p in parts if p.HasField("text"))


def status_update(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    kind: str | None = None,
    **metadata: Any,
) -> TaskStatusUpdateEvent:
    """Build a TaskStatusUpdateEvent with optional kind and metadata."""
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
        metadata=struct(meta) if meta else None,
    )


def function_call_part(
    func_name: str,
    args: dict[str, Any],
    result: dict[str, Any],
) -> Part:
    """Build a protobuf Part carrying a function_call data payload."""
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


def as_model[T: BaseModel](item: object, model: type[T]) -> T:
    """Extract a typed model from a ToolCallResult, validating if needed."""
    args = getattr(item, "args", item)
    return args if isinstance(args, model) else model.model_validate(
        args.model_dump()
    )


async def join_members(
    ctx: OrchestrationContext,
    names: list[str],
    reason: str,
) -> None:
    """Add agents to the room state (de-duplicated) and emit a member delta."""
    from choirworks.a2a.events import emit_state_delta
    from choirworks.a2a.state import add_member

    state = ctx.state
    records = await ctx.registry.list()
    known = {record.name: record for record in records}
    new_members: list[MemberDelta] = []
    for name in dict.fromkeys(names):
        record = known.get(name)
        if record is None:
            continue
        if not add_member(state, name, record.card_url, reason):
            continue
        new_members.append({
            "agent_name": name,
            "agent_url": record.card_url,
            "reason": reason,
        })
    if new_members:
        await emit_state_delta(ctx, members=new_members)


async def execute_function(
    ctx: OrchestrationContext,
    func: AgentFunction,
    args: BaseModel,
    *,
    state_name: int = TaskState.TASK_STATE_WORKING,
) -> FunctionResult:
    """Execute an AgentFunction and emit the function-call event.

    Builds the FunctionContext, calls execute, emits the result artifact,
    and returns the FunctionResult for the caller to inspect.
    """
    from choirworks.a2a.events import emit_function_call
    from choirworks.tools.base import FunctionContext

    func_ctx = FunctionContext(
        runtime=ctx.runtime,
        registry=ctx.registry,
        effects=ctx.effects,
    )
    result = await func.execute(func_ctx, args)
    await emit_function_call(ctx, func, args, result, state_name=state_name)
    return result
