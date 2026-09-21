from __future__ import annotations

from choirworks.orchestration.state import (
    Member,
    NodeState,
    OrchestrationState,
    active_nodes,
    add_cancel_request,
    add_intervention,
    add_member,
    all_completed,
    assist_nodes_for,
    blocked_nodes,
    enqueue,
    expire_cancel_requests,
    failed_nodes,
    has_failures,
    has_pending_work,
    input_required_nodes,
    load_state,
    normalize_cancel_requests,
    pending_intervention_for,
    pending_interventions,
    ready_nodes,
    start_new_plan,
    state_from_json,
    state_to_json,
    take_queued,
)


def _state_with_session_data() -> OrchestrationState:
    state = OrchestrationState(plan_id="p1")
    state.nodes["n1"] = NodeState(
        id="n1", name="n1", agent_name="a", agent_url="http://a"
    )
    add_intervention(state, "n1", "q")
    enqueue(state, "n1", "hi", sender="user")
    state.derived_count = 2
    state.members["a"] = Member(name="a", url="http://a", reason="plan")
    return state


def test_start_new_plan_resets_plan_scope_keeps_members():
    state = _state_with_session_data()
    start_new_plan(state, "p2")

    assert state.plan_id == "p2"
    assert state.plan_version == 2
    assert state.nodes == {}
    assert state.interventions == {}
    assert state.queue == {}
    assert state.derived_count == 0
    assert state.revision_count == 0
    assert set(state.members) == {"a"}


def test_full_json_round_trips_everything():
    state = OrchestrationState(plan_id="p2", plan_version=3, derived_count=1)
    state.revision_count = 2
    state.nodes["n1"] = NodeState(
        id="n1",
        name="n1",
        agent_name="a",
        agent_url="http://a",
        status="completed",
        attempt=2,
        a2a_task_id="remote-1",
        output="结果",
        deps=["n0"],
        input_text="问题",
        derived=True,
        question="q?",
        source_message_id="m1",
        assist_requested_by="n9",
    )
    state.members["a"] = Member(name="a", url="http://a", reason="plan")
    pending = add_intervention(state, "n1", "q")
    resolved = add_intervention(state, "n2", "q2")
    resolved.status = "resolved"
    resolved.answer = "答复"
    resolved.responder = "human"
    queued = enqueue(state, "n1", "hi", sender="user", quote_id="n1")
    state.next_intervention = 5
    state.next_message = 5

    loaded = state_from_json(state_to_json(state))

    assert loaded.plan_id == "p2"
    assert loaded.plan_version == 3
    assert loaded.derived_count == 1
    assert loaded.revision_count == 2
    assert loaded.next_intervention == 5
    assert loaded.next_message == 5
    node = loaded.nodes["n1"]
    assert node.status == "completed"
    assert node.attempt == 2
    assert node.a2a_task_id == "remote-1"
    assert node.output == "结果"
    assert node.deps == ["n0"]
    assert node.input_text == "问题"
    assert node.derived is True
    assert node.question == "q?"
    assert node.source_message_id == "m1"
    assert node.assist_requested_by == "n9"
    assert loaded.members["a"].url == "http://a"
    assert loaded.interventions[pending.id].status == "pending"
    assert loaded.interventions[resolved.id].answer == "答复"
    assert loaded.queue["n1"][0].id == queued.id
    assert loaded.queue["n1"][0].quote_id == "n1"


def test_load_state_reads_task_metadata():
    from a2a.types.a2a_pb2 import Task
    from google.protobuf.json_format import ParseDict

    state = _state_with_session_data()
    start_new_plan(state, "p2")
    task = Task(id="t1", context_id="c1")
    ParseDict({"choirworks.state": state_to_json(state)}, task.metadata)

    loaded = load_state(task)

    assert loaded is not None
    assert loaded.plan_id == "p2"
    assert set(loaded.members) == {"a"}


def _node(state: OrchestrationState, node_id: str, **kwargs) -> NodeState:
    kwargs.setdefault("agent_name", "a")
    node = NodeState(
        id=node_id,
        name=node_id,
        agent_url="http://a",
        **kwargs,
    )
    state.nodes[node_id] = node
    return node


def test_ready_nodes_requires_all_deps_completed():
    state = OrchestrationState()
    _node(state, "upstream", status="working")
    _node(state, "dep", status="completed")
    _node(state, "downstream", status="pending", deps=["dep"])
    _node(state, "waiting", status="pending", deps=["upstream", "dep"])
    _node(state, "root", status="pending")
    _node(state, "resuming", status="resume", deps=["dep"])

    ready_ids = {node.id for node in ready_nodes(state)}

    assert ready_ids == {"downstream", "root", "resuming"}


def test_ready_nodes_ignores_non_pending_statuses_and_missing_deps():
    state = OrchestrationState()
    _node(state, "done", status="completed")
    _node(state, "active", status="dispatched")
    _node(state, "orphan", status="pending", deps=["ghost"])

    assert ready_nodes(state) == []


def test_blocked_nodes_are_pending_with_terminal_dep():
    state = OrchestrationState()
    _node(state, "failed_dep", status="failed")
    _node(state, "canceled_dep", status="canceled")
    _node(state, "blocked", status="pending", deps=["failed_dep"])
    _node(state, "blocked2", status="pending", deps=["canceled_dep"])
    _node(state, "fine", status="pending", deps=["working_dep"])
    _node(state, "working_dep", status="working")

    assert {node.id for node in blocked_nodes(state)} == {"blocked", "blocked2"}


def test_active_and_input_required_and_failed_nodes():
    state = OrchestrationState()
    _node(state, "dispatched", status="dispatched")
    _node(state, "working", status="working")
    _node(state, "asked", status="input_required")
    _node(state, "failed", status="failed")
    _node(state, "pending", status="pending")

    assert {n.id for n in active_nodes(state)} == {"dispatched", "working"}
    assert [n.id for n in input_required_nodes(state)] == ["asked"]
    assert [n.id for n in failed_nodes(state)] == ["failed"]


def test_all_completed_ignores_invalidated_and_rejects_empty():
    empty = OrchestrationState()
    assert all_completed(empty) is False

    state = OrchestrationState()
    _node(state, "done", status="completed")
    _node(state, "canceled", status="canceled")
    _node(state, "stale", status="invalidated")
    assert all_completed(state) is True

    _node(state, "pending", status="pending")
    assert all_completed(state) is False


def test_has_failures_and_has_pending_work():
    state = OrchestrationState()
    _node(state, "done", status="completed")
    assert has_failures(state) is False
    assert has_pending_work(state) is False

    _node(state, "queued", status="ready")
    assert has_pending_work(state) is True

    _node(state, "bad", status="failed")
    assert has_failures(state) is True


def test_assist_nodes_for_filters_by_requester_and_agent():
    state = OrchestrationState()
    _node(state, "n1", status="working")
    _node(
        state, "helper", status="pending", derived=True,
        assist_requested_by="n1", agent_name="b",
    )
    _node(
        state, "other", status="pending", derived=True,
        assist_requested_by="n2", agent_name="b",
    )
    _node(state, "plain", status="pending", agent_name="b")

    helpers = assist_nodes_for(state, "b", "n1")

    assert [node.id for node in helpers] == ["helper"]


def test_pending_interventions_and_lookup():
    state = OrchestrationState()
    _node(state, "n1", status="input_required")
    pending = add_intervention(state, "n1", "q")
    resolved = add_intervention(state, "n2", "q2")
    resolved.status = "resolved"

    assert [iv.id for iv in pending_interventions(state)] == [pending.id]
    assert pending_intervention_for(state, "n1") is pending
    assert pending_intervention_for(state, "n2") is None


def test_add_member_is_idempotent():
    state = OrchestrationState()

    assert add_member(state, "a", "http://a", "plan") is True
    assert add_member(state, "a", "http://a", "again") is False
    assert state.members["a"].reason == "plan"


def test_enqueue_take_queued_round_trip():
    state = OrchestrationState()
    message = enqueue(state, "n1", "hi", sender="user", quote_id="n1")

    assert message.id == "qm1"
    assert [qm.id for qm in take_queued(state, "n1")] == ["qm1"]
    assert take_queued(state, "n1") == []


def test_cancel_requests_expire_and_deduplicate():
    state = OrchestrationState()
    _node(state, "n1", status="working")

    first = add_cancel_request(state, "n1", "打断？")
    assert first is not None
    assert add_cancel_request(state, "n1", "打断？") is None

    expired = expire_cancel_requests(state, "n1")
    assert [iv.id for iv in expired] == [first.id]
    assert first.status == "expired"
    assert add_cancel_request(state, "n1", "再问？") is not None


def test_normalize_cancel_requests_expires_when_target_settled():
    state = OrchestrationState()
    _node(state, "n1", status="completed")
    expired = add_cancel_request(state, "n1", "打断？")
    assert expired is not None

    normalized = normalize_cancel_requests(state)

    assert [iv.id for iv in normalized] == [expired.id]
    assert expired.status == "expired"
