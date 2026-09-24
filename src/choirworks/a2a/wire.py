from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import cast

from a2a.types.a2a_pb2 import (
    Message,
    Part,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import MessageToDict, ParseDict
from pydantic import BaseModel, ValidationError


def strip_none(value: object) -> object:
    """Recursively remove None values from dicts and lists."""
    if isinstance(value, dict):
        mapping = cast("dict[object, object]", value)
        return {key: strip_none(item) for key, item in mapping.items() if item is not None}
    if isinstance(value, list):
        return [strip_none(item) for item in value]
    return value


def struct(data: Mapping[str, object]) -> struct_pb2.Struct:
    """Build a protobuf Struct from a mapping, stripping None values first."""
    result = struct_pb2.Struct()
    ParseDict(cast("dict[str, object]", strip_none(data)), result)
    return result


def join_text(parts: Collection[Part]) -> str:
    """Join protobuf parts' text fields with newlines."""
    return "\n".join(p.text for p in parts if p.HasField("text"))


def status_update(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    kind: str | None = None,
    metadata: Mapping[str, object] | None = None,
    message: Message | None = None,
) -> TaskStatusUpdateEvent:
    """Build a TaskStatusUpdateEvent with optional kind, metadata and message."""
    meta: dict[str, object] = {}
    if kind:
        meta["kind"] = kind
    if metadata:
        meta.update(metadata)
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()
    status = TaskStatus(state=state, timestamp=timestamp)
    if message is not None:
        status.message.CopyFrom(message)
    return TaskStatusUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        status=status,
        metadata=struct(meta) if meta else None,
    )


def function_call_part(
    func_name: str,
    args: Mapping[str, object],
    result: Mapping[str, object],
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


def data_part(data: Mapping[str, object], metadata: Mapping[str, object]) -> Part:
    """Build a protobuf Part carrying a data payload plus part metadata."""
    data_value = struct_pb2.Value()
    ParseDict(cast("dict[str, object]", strip_none(data)), data_value)
    part = Part()
    part.data.CopyFrom(data_value)
    part.metadata.CopyFrom(struct(metadata))
    return part


class QuestionResponse(BaseModel):
    """A user's answer to one pending question."""

    intervention_id: str
    answer: str | list[str] | bool


def parse_question_response(message: Message) -> list[QuestionResponse]:
    """Extract ``question_response`` data parts from a user message.

    Returns an empty list when the message carries no such part; raises
    ``ValueError`` when a part is malformed (missing id / bad answer type).
    """
    responses: list[QuestionResponse] = []
    for part in message.parts:
        if part.WhichOneof("content") != "data":
            continue
        kind_field = part.metadata.fields.get("cw_type")
        if kind_field is None or kind_field.string_value != "question_response":
            continue
        payload = MessageToDict(part.data, preserving_proto_field_name=True)
        try:
            responses.append(QuestionResponse.model_validate(payload))
        except ValidationError as exc:
            raise ValueError(f"malformed question_response: {payload!r}") from exc
    return responses
