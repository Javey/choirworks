from __future__ import annotations

from typing import TypedDict, cast

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
    return cast(RoomOptions, MessageToDict(room.struct_value, preserving_proto_field_name=True))


def message_text(msg: Message) -> str:
    parts: list[str] = []
    for part in msg.parts or []:
        if part.WhichOneof("content") == "text":
            parts.append(part.text)
    return "\n".join(parts)
