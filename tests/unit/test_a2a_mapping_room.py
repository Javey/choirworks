from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    room_message_from_event,
    room_to_task,
)
from choirworks.models.domain import (
    Conversation,
    RoomMember,
    RoomMessage,
    RoomSummary,
)
from choirworks.models.enums import EventType
from choirworks.store.event_store import Event

_NOW = datetime(2026, 9, 14, tzinfo=UTC)


def _conversation() -> Conversation:
    return Conversation(id="c1", title="测试群", created_at=_NOW)


def _message(
    seq: int = 1,
    role: str = "user",
    sender: str = "CEO",
    text: str = "大家好",
    **overrides,
) -> RoomMessage:
    data = {
        "id": f"msg-{seq}",
        "conversation_id": "c1",
        "seq": seq,
        "role": role,
        "sender": sender,
        "text": text,
        "mentions": ["echo"],
        "quote_id": None,
        "task_id": None,
        "node_id": None,
        "intervention_id": None,
        "queued_for_node_id": None,
        "created_at": _NOW,
    }
    data.update(overrides)
    return RoomMessage(**data)


def _member() -> RoomMember:
    return RoomMember(
        conversation_id="c1",
        agent_name="echo",
        agent_url="http://agent",
        reason="human_mention",
        joined_at=_NOW,
    )


def _summary() -> RoomSummary:
    return RoomSummary(
        conversation_id="c1",
        covers_seq=1,
        summary={"topics": ["问候"]},
        updated_at=_NOW,
    )


def test_room_to_task_maps_metadata_and_history():
    room = room_to_task(
        _conversation(), [_message()], [_member()], _summary(), running=False
    )
    assert room.id == "c1"
    assert room.context_id == "c1"
    assert room.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "room"
    assert fields["title"].string_value == "测试群"
    assert fields["message_count"].number_value == 1
    assert fields["last_seq"].number_value == 1
    member = fields["members"].list_value.values[0].struct_value.fields
    assert member["agent_name"].string_value == "echo"
    assert member["agent_url"].string_value == "http://agent"
    assert member["reason"].string_value == "human_mention"
    summary = fields["summary"].struct_value.fields
    assert summary["covers_seq"].number_value == 1
    assert summary["content"].struct_value.fields["topics"].list_value.values[
        0
    ].string_value == "问候"
    assert room.history[0].message_id == "msg-1"
    assert room.history[0].role is Role.ROLE_USER
    assert room.history[0].parts[0].text == "大家好"
    assert room.history[0].extensions == [A2A_ROOM_URI]


def test_room_to_task_working_without_messages_or_summary():
    room = room_to_task(_conversation(), [], [], None, running=True)
    assert room.status.state is TaskState.TASK_STATE_WORKING
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["message_count"].number_value == 0
    assert fields["last_seq"].number_value == 0
    assert not fields["members"].list_value.values
    assert not fields["summary"].struct_value.fields
    assert not room.history


def test_room_message_from_event_rebuilds_message():
    event = Event(
        seq=7,
        task_id="t1",
        conversation_id="c1",
        type=EventType.MESSAGE_POSTED,
        payload={
            "message_id": "msg-9",
            "conversation_id": "c1",
            "seq": 3,
            "role": "assistant",
            "sender": "assistant",
            "text": "已派发 @echo",
            "mentions": ["echo"],
            "quote_id": "msg-1",
            "task_id": "t1",
            "node_id": "p1:n1",
            "queued_for_node_id": None,
        },
        created_at=_NOW,
    )
    message = room_message_from_event(event)
    assert message.id == "msg-9"
    assert message.conversation_id == "c1"
    assert message.seq == 3
    assert message.role == "assistant"
    assert message.sender == "assistant"
    assert message.text == "已派发 @echo"
    assert message.mentions == ["echo"]
    assert message.quote_id == "msg-1"
    assert message.task_id == "t1"
    assert message.node_id == "p1:n1"
    assert message.created_at == _NOW
