from __future__ import annotations

from choirworks.a2a.state import (
    Member,
    NodeState,
    OrchestrationState,
    load_state,
)


def _state_with_session_data() -> OrchestrationState:
    state = OrchestrationState(plan_id="p1")
    state.nodes["n1"] = NodeState(
        id="n1", name="n1", agent_name="a", agent_url="http://a"
    )
    state.add_intervention("n1", "q")
    state.enqueue("n1", "hi", sender="user")
    state.derived_count = 2
    state.members["a"] = Member(name="a", url="http://a", reason="plan")
    return state


def test_start_new_plan_resets_plan_scope_keeps_members():
    state = _state_with_session_data()
    state.start_new_plan("p2")

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
    pending = state.add_intervention("n1", "q")
    resolved = state.add_intervention("n2", "q2")
    resolved.status = "resolved"
    resolved.answer = "答复"
    resolved.responder = "human"
    queued = state.enqueue("n1", "hi", sender="user", quote_id="n1")
    state.next_intervention = 5
    state.next_message = 5

    loaded = OrchestrationState.from_json(state.to_json())

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
    state.start_new_plan("p2")
    task = Task(id="t1", context_id="c1")
    ParseDict({"choirworks.state": state.to_json()}, task.metadata)

    loaded = load_state(task)

    assert loaded is not None
    assert loaded.plan_id == "p2"
    assert set(loaded.members) == {"a"}
