from __future__ import annotations

from choirworks.a2a.state import (
    NodeState,
    OrchestrationState,
    add_cancel_request,
    expire_cancel_requests,
    normalize_cancel_requests,
    pending_interventions,
    state_from_json,
    state_to_json,
)


def node(node_id: str, status: str = "pending") -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=node_id,
        agent_url=f"http://{node_id}",
        status=status,
    )


def test_cancel_request_dedupes_per_node():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    first = add_cancel_request(state, "n1", "打断？")
    again = add_cancel_request(state, "n1", "打断？")
    assert first is not None
    assert again is None
    assert len(state.interventions) == 1


def test_expire_cancel_requests_on_node_settle():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    add_cancel_request(state, "n1", "打断？")
    expired = expire_cancel_requests(state, "n1")
    assert [iv.id for iv in expired] == ["iv1"]
    assert pending_interventions(state) == []


def test_normalize_expires_when_target_missing_or_settled():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "completed")
    state.nodes["n2"] = node("n2", "working")
    add_cancel_request(state, "n1", "打断？")
    add_cancel_request(state, "n2", "打断？")
    add_cancel_request(state, "ghost", "打断？")
    expired = normalize_cancel_requests(state)
    assert {iv.target_node_id for iv in expired} == {"n1", "ghost"}
    pending = pending_interventions(state)
    assert [iv.target_node_id for iv in pending] == ["n2"]


def test_intervention_serialization_roundtrip():
    state = OrchestrationState()
    state.nodes["n1"] = node("n1", "working")
    add_cancel_request(state, "n1", "打断？")
    loaded = state_from_json(state_to_json(state))
    intervention = next(iter(loaded.interventions.values()))
    assert intervention.kind == "confirm_cancel"
    assert intervention.target_node_id == "n1"
    assert pending_interventions(loaded)[0].kind == "confirm_cancel"
