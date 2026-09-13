from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import TaskStreamMapper
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store.event_store import Event


def _snapshot() -> TaskSnapshot:
    task = OrchestrationTask(
        id="task-1",
        status=TaskStatus.RUNNING,
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
        status=NodeStatus.WORKING,
    )
    return TaskSnapshot(task=task, plan=None, nodes=[node], last_seq=0)


def _event(event_type: EventType, payload: dict, seq: int = 1) -> Event:
    return Event(
        seq=seq,
        task_id="task-1",
        conversation_id="conv-1",
        type=event_type,
        payload=payload,
        created_at=datetime.now(UTC),
    )


def test_task_state_change_maps_and_dedupes():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.TASK_STATE_CHANGED,
            {"from": "running", "to": "awaiting_input"},
        )
    )
    assert responses[0].status_update.status.state is (
        TaskState.TASK_STATE_INPUT_REQUIRED
    )
    assert mapper.terminal is False
    assert (
        mapper.map_event(
            _event(
                EventType.TASK_STATE_CHANGED,
                {"from": "running", "to": "awaiting_input"},
                seq=2,
            )
        )
        == []
    )


def test_task_completed_marks_terminal():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(_event(EventType.TASK_COMPLETED, {}, seq=3))
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_COMPLETED
    assert mapper.terminal is True


def test_node_state_change_carries_metadata():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {
                "node_id": "plan1:n1",
                "node_name": "researcher",
                "agent_name": "researcher",
                "attempt": 1,
                "from": "ready",
                "to": "working",
            },
        )
    )
    meta = responses[0].status_update.metadata
    assert meta.fields["node_id"].string_value == "plan1:n1"
    assert meta.fields["kind"].string_value == "node.state_changed"
    assert meta.fields["to"].string_value == "working"


def test_node_artifact_maps_append_and_prefix():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.NODE_ARTIFACT,
            {
                "node_id": "plan1:n1",
                "artifact_id": "a1",
                "name": "output",
                "text": "增量",
                "append": True,
            },
        )
    )
    update = responses[0].artifact_update
    assert update.artifact.artifact_id == "plan1:n1:a1"
    assert update.append is True
    assert update.last_chunk is False
    assert update.artifact.parts[0].text == "增量"


def test_node_terminal_emits_last_chunk_for_known_artifacts():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(
        _event(
            EventType.NODE_ARTIFACT,
            {
                "node_id": "plan1:n1",
                "artifact_id": "a1",
                "name": "output",
                "text": "增量",
                "append": True,
            },
        )
    )
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {"node_id": "plan1:n1", "from": "working", "to": "completed"},
            seq=2,
        )
    )
    assert len(responses) == 2
    last = responses[1].artifact_update
    assert last.artifact.artifact_id == "plan1:n1:a1"
    assert last.append is True
    assert last.last_chunk is True


def test_plan_created_maps_to_data_artifact():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_CREATED,
            {
                "plan_id": "plan1",
                "version": 1,
                "rationale": "因为",
                "dag": {"nodes": [{"id": "n1"}]},
            },
        )
    )
    update = responses[0].artifact_update
    assert update.artifact.artifact_id == "plan:plan1"
    assert update.artifact.parts[0].data.struct_value.fields["nodes"].list_value
    assert update.last_chunk is True


def test_intervention_requested_sets_input_required():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "plan1:n1",
                "policy": "human",
                "question": {"text": "请确认口径"},
            },
        )
    )
    update = responses[0].status_update
    assert update.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert update.status.message.role is Role.ROLE_AGENT
    assert update.status.message.parts[0].text == "请确认口径"


def test_notification_events_do_not_change_state():
    mapper = TaskStreamMapper(_snapshot())
    for event_type in (
        EventType.NODE_DISPATCH_INTENT,
        EventType.NODE_DISPATCHED,
        EventType.NODE_RETRY_SCHEDULED,
        EventType.CHECKPOINT_CREATED,
        EventType.ROLLBACK_PERFORMED,
        EventType.ERROR,
    ):
        mapper = TaskStreamMapper(_snapshot())
        responses = mapper.map_event(
            _event(event_type, {"node_id": "plan1:n1", "message": "m"})
        )
        assert responses[0].status_update.status.state is (
            TaskState.TASK_STATE_WORKING
        )
        assert responses[0].status_update.metadata.fields["kind"].string_value == (
            event_type.value
        )


def test_node_output_is_not_emitted():
    mapper = TaskStreamMapper(_snapshot())
    assert mapper.map_event(_event(EventType.NODE_OUTPUT, {"node_id": "plan1:n1"})) == []
