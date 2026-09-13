from datetime import UTC, datetime

from a2a.types import Role, TaskState

from choirworks.a2a.mapping import TaskStreamMapper
from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node, OrchestrationTask
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store.event_store import Event


def _snapshot(status: TaskStatus = TaskStatus.RUNNING) -> TaskSnapshot:
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


def test_intervention_resolved_restores_working_state():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "plan1:n1",
                "policy": "auto_llm",
                "question": {"text": "请确认口径"},
            },
        )
    )
    responses = mapper.map_event(
        _event(
            EventType.INTERVENTION_RESOLVED,
            {"intervention_id": "iv1", "answer": {"text": "口径是 X"}},
            seq=2,
        )
    )
    update = responses[0].status_update
    assert update.status.state is TaskState.TASK_STATE_WORKING
    assert update.metadata.fields["kind"].string_value == "intervention.resolved"
    assert update.status.message.role is Role.ROLE_AGENT
    assert update.status.message.parts[0].text == "口径是 X"


def test_intervention_resolved_without_pending_keeps_state():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.INTERVENTION_RESOLVED,
            {"intervention_id": "iv1", "answer": {"text": "口径是 X"}},
        )
    )
    update = responses[0].status_update
    assert update.status.state is TaskState.TASK_STATE_WORKING
    assert update.metadata.fields["kind"].string_value == "intervention.resolved"
    assert not update.status.HasField("message")


def test_intervention_resolved_empty_answer_has_no_message():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {"intervention_id": "iv1", "node_id": "plan1:n1", "policy": "auto_llm"},
        )
    )
    responses = mapper.map_event(
        _event(EventType.INTERVENTION_RESOLVED, {"intervention_id": "iv1"}, seq=2)
    )
    update = responses[0].status_update
    assert update.status.state is TaskState.TASK_STATE_WORKING
    assert not update.status.HasField("message")


def test_intervention_resolved_then_notification_stays_working():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {"intervention_id": "iv1", "node_id": "plan1:n1", "policy": "auto_llm"},
        )
    )
    mapper.map_event(
        _event(
            EventType.INTERVENTION_RESOLVED,
            {"intervention_id": "iv1", "answer": {"text": "ok"}},
            seq=2,
        )
    )
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {"node_id": "plan1:n1", "from": "ready", "to": "working"},
            seq=3,
        )
    )
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_WORKING


def test_plan_superseded_emits_status_only():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_SUPERSEDED,
            {"plan_id": "plan0", "superseded_by_version": 2},
        )
    )
    assert len(responses) == 1
    assert not responses[0].HasField("artifact_update")
    meta = responses[0].status_update.metadata
    assert meta.fields["kind"].string_value == "plan.superseded"
    assert meta.fields["plan_id"].string_value == "plan0"
    assert meta.fields["superseded_by_version"].number_value == 2


def test_plan_superseded_without_previous_plan_has_no_artifact():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_SUPERSEDED,
            {"plan_id": None, "superseded_by_version": 2},
        )
    )
    assert len(responses) == 1
    assert not responses[0].HasField("artifact_update")
    plan_id = responses[0].status_update.metadata.fields["plan_id"]
    assert plan_id.WhichOneof("kind") == "null_value"


def test_plan_extended_without_dag_emits_status_only():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_EXTENDED,
            {
                "plan_id": "plan1",
                "version": 1,
                "rationale": "加一个节点",
                "added_nodes": [{"id": "n2"}],
                "added_edges": [{"from": "n1", "to": "n2"}],
            },
        )
    )
    assert len(responses) == 1
    assert not responses[0].HasField("artifact_update")
    meta = responses[0].status_update.metadata
    assert meta.fields["kind"].string_value == "plan.extended"
    assert meta.fields["plan_id"].string_value == "plan1"
    assert meta.fields["version"].number_value == 1
    assert meta.fields["rationale"].string_value == "加一个节点"


def test_plan_extended_with_dag_emits_artifact():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.PLAN_EXTENDED,
            {
                "plan_id": "plan1",
                "version": 1,
                "rationale": "扩展",
                "dag": {"nodes": [{"id": "n1"}, {"id": "n2"}]},
            },
            seq=2,
        )
    )
    update = responses[0].artifact_update
    assert update.artifact.artifact_id == "plan:plan1"
    assert update.append is False
    assert update.last_chunk is True


def test_notification_kind_cannot_be_overridden_by_payload():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(EventType.NODE_DISPATCHED, {"kind": "spoofed", "node_id": "plan1:n1"})
    )
    assert responses[0].status_update.metadata.fields["kind"].string_value == (
        "node.dispatched"
    )


def test_task_failed_carries_last_error():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(_event(EventType.ERROR, {"message": "boom"}, seq=2))
    responses = mapper.map_event(_event(EventType.TASK_FAILED, {}, seq=3))
    status = responses[0].status_update.status
    assert status.state is TaskState.TASK_STATE_FAILED
    assert status.message.role is Role.ROLE_AGENT
    assert status.message.message_id == "task-1:error"
    assert status.message.parts[0].text == "boom"


def test_task_failed_without_error_has_no_message():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(_event(EventType.TASK_FAILED, {}, seq=3))
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_FAILED
    assert not responses[0].status_update.status.HasField("message")


def test_checkpoint_notification_metadata_is_whitelisted():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": "ck1",
                "seq": 4,
                "plan_version": 1,
                "artifacts": {"big": "payload"},
                "frontier": ["plan1:n1"],
            },
        )
    )
    meta = responses[0].status_update.metadata
    assert meta.fields["kind"].string_value == "checkpoint.created"
    assert meta.fields["checkpoint_id"].string_value == "ck1"
    assert meta.fields["seq"].number_value == 4
    assert meta.fields["plan_version"].number_value == 1
    assert meta.fields["frontier"].list_value.values[0].string_value == "plan1:n1"
    assert "artifacts" not in meta.fields


def test_rollback_notification_metadata_is_whitelisted():
    mapper = TaskStreamMapper(_snapshot())
    responses = mapper.map_event(
        _event(
            EventType.ROLLBACK_PERFORMED,
            {
                "checkpoint_id": "ck1",
                "plan_id": "plan1",
                "plan_version": 1,
                "dag": {"nodes": [{"id": "n1"}]},
                "deps_restore": {},
                "reset_node_ids": ["plan1:n1"],
                "invalidate_node_ids": ["plan1:n2"],
                "cancelled_remote_task_ids": ["remote-1"],
            },
        )
    )
    meta = responses[0].status_update.metadata
    assert meta.fields["kind"].string_value == "rollback.performed"
    assert meta.fields["reset_node_ids"].list_value.values[0].string_value == "plan1:n1"
    assert meta.fields["invalidate_node_ids"].list_value.values[0].string_value == (
        "plan1:n2"
    )
    assert meta.fields["cancelled_remote_task_ids"].list_value.values[0].string_value == (
        "remote-1"
    )
    assert "dag" not in meta.fields
    assert "artifacts" not in meta.fields


def test_last_chunk_not_repeated_for_same_artifact():
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
    mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {"node_id": "plan1:n1", "from": "working", "to": "completed"},
            seq=2,
        )
    )
    responses = mapper.map_event(
        _event(
            EventType.NODE_STATE_CHANGED,
            {"node_id": "plan1:n1", "from": "completed", "to": "completed"},
            seq=3,
        )
    )
    assert len(responses) == 1
    assert not responses[0].HasField("artifact_update")


def test_last_chunk_reemitted_after_reappend():
    mapper = TaskStreamMapper(_snapshot())
    artifact = {
        "node_id": "plan1:n1",
        "artifact_id": "a1",
        "name": "output",
        "text": "v1",
        "append": False,
    }
    terminal = {"node_id": "plan1:n1", "from": "working", "to": "completed"}
    mapper.map_event(_event(EventType.NODE_ARTIFACT, artifact))
    first = mapper.map_event(_event(EventType.NODE_STATE_CHANGED, terminal, seq=2))
    assert len(first) == 2
    assert first[1].artifact_update.last_chunk is True
    mapper.map_event(_event(EventType.NODE_ARTIFACT, {**artifact, "text": "v2"}, seq=3))
    second = mapper.map_event(_event(EventType.NODE_STATE_CHANGED, terminal, seq=4))
    assert len(second) == 2
    assert second[1].artifact_update.last_chunk is True


def test_task_failed_after_state_advance_has_no_stale_error():
    mapper = TaskStreamMapper(_snapshot(TaskStatus.AWAITING_INPUT))
    mapper.map_event(_event(EventType.ERROR, {"message": "boom"}, seq=2))
    mapper.map_event(
        _event(
            EventType.TASK_STATE_CHANGED,
            {"from": "awaiting_input", "to": "running"},
            seq=3,
        )
    )
    responses = mapper.map_event(_event(EventType.TASK_FAILED, {}, seq=4))
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_FAILED
    assert not responses[0].status_update.status.HasField("message")


def test_task_failed_after_intervention_recovery_has_no_stale_error():
    mapper = TaskStreamMapper(_snapshot())
    mapper.map_event(_event(EventType.ERROR, {"message": "boom"}, seq=2))
    mapper.map_event(
        _event(
            EventType.INTERVENTION_REQUESTED,
            {"intervention_id": "iv1", "node_id": "plan1:n1", "policy": "auto_llm"},
            seq=3,
        )
    )
    mapper.map_event(
        _event(
            EventType.INTERVENTION_RESOLVED,
            {"intervention_id": "iv1", "answer": {"text": "ok"}},
            seq=4,
        )
    )
    responses = mapper.map_event(_event(EventType.TASK_FAILED, {}, seq=5))
    assert responses[0].status_update.status.state is TaskState.TASK_STATE_FAILED
    assert not responses[0].status_update.status.HasField("message")
