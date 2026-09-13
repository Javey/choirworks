from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import TASK_STATE_MAP, snapshot_to_task
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask
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


def test_failed_snapshot_carries_error_message():
    snapshot = _snapshot(TaskStatus.FAILED, node_status=NodeStatus.FAILED)
    snapshot.nodes[0].error = "boom"
    mapped = snapshot_to_task(snapshot)
    assert mapped.status.state is TaskState.TASK_STATE_FAILED
    assert mapped.status.message.parts[0].text == "boom"


def test_input_required_question_message():
    snapshot = _snapshot(TaskStatus.AWAITING_INPUT, node_status=NodeStatus.INPUT_REQUIRED)
    mapped = snapshot_to_task(snapshot, question="请补充预算口径")
    assert mapped.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert mapped.status.message.parts[0].text == "请补充预算口径"
