from datetime import UTC, datetime

import pytest
from a2a.types import Role, TaskState

from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    TASK_STATE_MAP,
    room_message_to_a2a,
    snapshot_to_task,
)
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask, RoomMessage
from choirworks.models.enums import NodeStatus, TaskStatus


def _snapshot(status: TaskStatus, node_status: NodeStatus = NodeStatus.COMPLETED):
    task = OrchestrationTask(
        id="task-1",
        status=status,
        request="分析 X",
        conversation_id="conv-1",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    node = Node(
        id="plan1:n1",
        task_id="task-1",
        plan_id="plan1",
        name="researcher",
        agent_name="researcher",
        status=node_status,
        attempt=2,
        output={"artifacts": [{"id": "a1", "name": "output", "text": "结论"}]},
    )
    return TaskSnapshot(task=task, plan=None, nodes=[node], last_seq=7)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (TaskStatus.PENDING, TaskState.TASK_STATE_SUBMITTED),
        (TaskStatus.PLANNING, TaskState.TASK_STATE_WORKING),
        (TaskStatus.RUNNING, TaskState.TASK_STATE_WORKING),
        (TaskStatus.AWAITING_INPUT, TaskState.TASK_STATE_INPUT_REQUIRED),
        (TaskStatus.COMPLETED, TaskState.TASK_STATE_COMPLETED),
        (TaskStatus.FAILED, TaskState.TASK_STATE_FAILED),
        (TaskStatus.CANCELED, TaskState.TASK_STATE_CANCELED),
    ],
)
def test_task_state_map_maps_each_status(status: TaskStatus, expected: TaskState):
    assert TASK_STATE_MAP[status] is expected


def test_task_state_map_covers_all_statuses():
    assert set(TASK_STATE_MAP) == set(TaskStatus)


def test_snapshot_maps_state_and_artifacts():
    mapped = snapshot_to_task(_snapshot(TaskStatus.RUNNING))
    assert mapped.id == "task-1"
    assert mapped.context_id == "conv-1"
    assert mapped.status.state is TaskState.TASK_STATE_WORKING
    assert mapped.artifacts[0].artifact_id == "plan1:n1:a1"
    assert mapped.artifacts[0].parts[0].text == "结论"


def test_snapshot_history_contains_request():
    mapped = snapshot_to_task(_snapshot(TaskStatus.COMPLETED))
    assert mapped.history[0].message_id == "task-1:request"
    assert mapped.history[0].role is Role.ROLE_USER
    assert mapped.history[0].parts[0].text == "分析 X"


def test_snapshot_metadata_lists_nodes():
    mapped = snapshot_to_task(_snapshot(TaskStatus.RUNNING))
    nodes = mapped.metadata.fields["nodes"].list_value.values
    assert nodes[0].struct_value.fields["id"].string_value == "plan1:n1"
    assert nodes[0].struct_value.fields["agent_name"].string_value == "researcher"
    assert nodes[0].struct_value.fields["attempt"].number_value == 2


def test_snapshot_without_conversation_maps_to_empty_context():
    snapshot = _snapshot(TaskStatus.RUNNING)
    snapshot.task.conversation_id = None
    mapped = snapshot_to_task(snapshot)
    assert mapped.context_id == ""


def test_artifact_without_name_or_text_uses_defaults():
    snapshot = _snapshot(TaskStatus.RUNNING)
    snapshot.nodes[0].output = {"artifacts": [{"id": "a1"}]}
    mapped = snapshot_to_task(snapshot)
    assert mapped.artifacts[0].artifact_id == "plan1:n1:a1"
    assert mapped.artifacts[0].name == "a1"
    assert mapped.artifacts[0].parts[0].text == ""


def test_snapshot_without_output_has_no_artifacts():
    snapshot = _snapshot(TaskStatus.RUNNING)
    snapshot.nodes[0].output = None
    mapped = snapshot_to_task(snapshot)
    assert len(mapped.artifacts) == 0


def test_failed_snapshot_carries_error_message():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED)
    snapshot.nodes[0].error = "boom"
    mapped = snapshot_to_task(snapshot)
    assert mapped.status.state is TaskState.TASK_STATE_FAILED
    assert mapped.status.message.parts[0].text == "boom"


def test_failed_snapshot_without_node_error_has_no_message():
    mapped = snapshot_to_task(_snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED))
    assert not mapped.status.HasField("message")


def test_failed_snapshot_uses_last_error_among_failed_nodes():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED)
    snapshot.nodes[0].error = "first"
    snapshot.nodes.append(
        snapshot.nodes[0].model_copy(update={"id": "plan1:n2", "error": "second"})
    )
    mapped = snapshot_to_task(snapshot)
    assert mapped.status.message.parts[0].text == "second"


def test_failed_snapshot_ignores_invalidated_node_error():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.INVALIDATED)
    snapshot.nodes[0].error = "stale"
    failed = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED)
    failed.nodes[0].error = "boom"
    snapshot.nodes.extend(failed.nodes)
    mapped = snapshot_to_task(snapshot)
    assert mapped.status.message.parts[0].text == "boom"


def test_failed_snapshot_with_only_invalidated_error_has_no_message():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.INVALIDATED)
    snapshot.nodes[0].error = "stale"
    mapped = snapshot_to_task(snapshot)
    assert not mapped.status.HasField("message")


def test_input_required_question_message():
    snapshot = _snapshot(TaskStatus.AWAITING_INPUT, node_status=NodeStatus.INPUT_REQUIRED)
    mapped = snapshot_to_task(snapshot, question="请补充预算口径")
    assert mapped.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert mapped.status.message.parts[0].text == "请补充预算口径"


def test_input_required_without_question_has_no_message():
    snapshot = _snapshot(TaskStatus.AWAITING_INPUT, node_status=NodeStatus.INPUT_REQUIRED)
    mapped = snapshot_to_task(snapshot)
    assert not mapped.status.HasField("message")


def _room_message(**overrides) -> RoomMessage:
    values = {
        "id": "msg-1",
        "conversation_id": "conv-1",
        "seq": 3,
        "role": "user",
        "sender": "CEO",
        "text": "请评估",
        "mentions": ["ask"],
        "quote_id": "msg-0",
        "node_id": "plan1:n1",
        "intervention_id": "int-1",
        "created_at": datetime.now(UTC),
    }
    values.update(overrides)
    return RoomMessage(**values)


def test_room_message_maps_identity_role_and_parts():
    mapped = room_message_to_a2a(_room_message())
    assert mapped.message_id == "msg-1"
    assert mapped.context_id == "conv-1"
    assert mapped.task_id == "conv-1"
    assert mapped.role is Role.ROLE_USER
    assert mapped.parts[0].text == "请评估"
    assert A2A_ROOM_URI in mapped.extensions


def test_room_message_task_id_overrides_context_fallback():
    mapped = room_message_to_a2a(_room_message(task_id="task-9"))
    assert mapped.task_id == "task-9"


def test_room_message_assistant_maps_to_agent_role():
    mapped = room_message_to_a2a(_room_message(role="assistant"))
    assert mapped.role is Role.ROLE_AGENT


def test_room_message_metadata_carries_room_fields():
    mapped = room_message_to_a2a(_room_message())
    fields = mapped.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"
    assert fields["sender"].string_value == "CEO"
    assert fields["seq"].number_value == 3
    assert [item.string_value for item in fields["mentions"].list_value.values] == [
        "ask"
    ]
    assert fields["quote_id"].string_value == "msg-0"
    assert fields["node_id"].string_value == "plan1:n1"
    assert fields["intervention_id"].string_value == "int-1"
    assert fields["queued_for_node_id"].WhichOneof("kind") == "null_value"


def test_room_message_metadata_carries_queued_node():
    mapped = room_message_to_a2a(_room_message(queued_for_node_id="plan1:n2"))
    fields = mapped.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["queued_for_node_id"].string_value == "plan1:n2"
