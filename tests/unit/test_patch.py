from __future__ import annotations

from choirworks.a2a.patch import PatchNode, PlanPatch, apply_patch
from choirworks.a2a.state import NodeState, OrchestrationState


def node(node_id: str, **kwargs) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=kwargs.pop("agent_name", node_id),
        agent_url=f"http://{node_id}",
        **kwargs,
    )


def state_with(*nodes: NodeState) -> OrchestrationState:
    state = OrchestrationState(plan_id="plan-1")
    for item in nodes:
        state.nodes[item.id] = item
    return state


AGENTS = {"writer": "http://writer", "reviewer": "http://reviewer"}


def test_add_node_gets_patch_id_and_agent_url():
    state = state_with(node("n1", status="completed"))
    patch = PlanPatch(
        add=[PatchNode(agent_name="writer", instruction="写报告", deps=["n1"])],
        reason="需要撰写",
    )
    result = apply_patch(state, patch, AGENTS)
    assert result.added == ["x1"]
    added = state.nodes["x1"]
    assert added.status == "pending"
    assert added.agent_url == "http://writer"
    assert added.input_text == "写报告"
    assert added.deps == ["n1"]
    assert state.patch_count == 1


def test_patch_ids_increment():
    state = state_with(node("n1", status="completed"))
    apply_patch(
        state,
        PlanPatch(add=[PatchNode(agent_name="writer", instruction="a")]),
        AGENTS,
    )
    apply_patch(
        state,
        PlanPatch(add=[PatchNode(agent_name="reviewer", instruction="b")]),
        AGENTS,
    )
    assert sorted(state.nodes) == ["n1", "x1", "x2"]


def test_unknown_agent_rejected():
    state = state_with(node("n1", status="completed"))
    result = apply_patch(
        state,
        PlanPatch(add=[PatchNode(agent_name="ghost", instruction="x")]),
        AGENTS,
    )
    assert result.added == []
    assert "ghost" in result.rejected[0]


def test_unknown_dep_rejected():
    state = state_with(node("n1", status="completed"))
    result = apply_patch(
        state,
        PlanPatch(
            add=[PatchNode(agent_name="writer", instruction="x", deps=["nope"])]
        ),
        AGENTS,
    )
    assert result.added == []
    assert "nope" in result.rejected[0]


def test_invalidate_pending_cascades_downstream():
    state = state_with(
        node("n1", status="pending"),
        node("n2", status="pending", deps=["n1"]),
        node("n3", status="completed"),
    )
    result = apply_patch(state, PlanPatch(invalidate=["n1"]), AGENTS)
    assert result.invalidated == ["n1", "n2"]
    assert state.nodes["n1"].status == "invalidated"
    assert state.nodes["n2"].status == "invalidated"
    assert state.nodes["n3"].status == "completed"


def test_invalidate_in_flight_is_skipped():
    state = state_with(node("n1", status="working"))
    result = apply_patch(state, PlanPatch(invalidate=["n1"]), AGENTS)
    assert result.skipped_in_flight == ["n1"]
    assert result.invalidated == []
    assert state.nodes["n1"].status == "working"


def test_invalidate_completed_is_skipped():
    state = state_with(node("n1", status="completed"))
    result = apply_patch(state, PlanPatch(invalidate=["n1"]), AGENTS)
    assert result.invalidated == []
    assert state.nodes["n1"].status == "completed"


def test_invalidate_failed_is_allowed():
    state = state_with(
        node("n1", status="failed"),
        node("n2", status="pending", deps=["n1"]),
    )
    result = apply_patch(state, PlanPatch(invalidate=["n1"]), AGENTS)
    assert result.invalidated == ["n1", "n2"]
    assert state.nodes["n1"].status == "invalidated"
    assert state.nodes["n2"].status == "invalidated"


def test_invalidated_nodes_do_not_block_completion():
    state = state_with(
        node("n1", status="completed"),
        node("n2", status="invalidated"),
    )
    assert state.all_completed()
    assert not state.has_pending_work()


def test_canceled_nodes_settle_the_plan():
    state = state_with(
        node("n1", status="completed"),
        node("n2", status="canceled"),
    )
    assert state.all_completed()
    assert not state.has_pending_work()
