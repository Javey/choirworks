from __future__ import annotations

from typing import Any

from a2a.types.a2a_pb2 import (
    Part,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import ParseDict


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
