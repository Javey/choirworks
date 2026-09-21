from __future__ import annotations

from typing import Any, TypedDict

from a2a.types.a2a_pb2 import Message
from google.protobuf.json_format import MessageToDict

A2A_ROOM_URI = "https://github.com/Javey/choirworks/extensions/room/v1"


class RoomOptions(TypedDict, total=False):
    mentions: list[str]
    quote_id: str
    interrupt: bool
    sender: str
    role: str
    kind: str
    node_id: str


def room_options(message: Message | None) -> RoomOptions:
    if message is None or not message.metadata.fields:
        return {}
    room = message.metadata.fields.get(A2A_ROOM_URI)
    if room is None or not room.HasField("struct_value"):
        return {}
    raw: dict[str, Any] = MessageToDict(
        room.struct_value, preserving_proto_field_name=True
    )
    options: RoomOptions = {}
    mentions = raw.get("mentions")
    if isinstance(mentions, list):
        options["mentions"] = [str(item) for item in mentions]
    quote_id = raw.get("quote_id")
    if isinstance(quote_id, str):
        options["quote_id"] = quote_id
    interrupt = raw.get("interrupt")
    if isinstance(interrupt, bool):
        options["interrupt"] = interrupt
    sender = raw.get("sender")
    if isinstance(sender, str):
        options["sender"] = sender
    role = raw.get("role")
    if isinstance(role, str):
        options["role"] = role
    kind = raw.get("kind")
    if isinstance(kind, str):
        options["kind"] = kind
    node_id = raw.get("node_id")
    if isinstance(node_id, str):
        options["node_id"] = node_id
    return options


def message_text(msg: Message) -> str:
    parts: list[str] = []
    for part in msg.parts or []:
        if part.WhichOneof("content") == "text":
            parts.append(part.text)
    return "\n".join(parts)
