from choirworks.a2a.state import Member, NodeState, OrchestrationState


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
    assert set(state.members) == {"a"}


def test_minimal_json_round_trips_plan_version_and_members():
    state = _state_with_session_data()
    state.start_new_plan("p2")
    state.nodes["n2"] = NodeState(
        id="n2", name="n2", agent_name="a", agent_url="http://a"
    )

    loaded = OrchestrationState.from_minimal_json(state.to_minimal_json())

    assert loaded.plan_version == 2
    assert set(loaded.members) == {"a"}
    assert set(loaded.nodes) == {"n2"}
