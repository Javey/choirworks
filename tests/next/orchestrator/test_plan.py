from __future__ import annotations

import pytest

from new_choirworks.orchestrator.plan import (
    DagPlan,
    PlanNode,
    PlanValidationError,
    output_ref,
    resolve_instruction,
    validate_plan,
)


def _node(nid: str, deps: list[str] | None = None, agent: str = "writer") -> PlanNode:
    return PlanNode(id=nid, name=nid, agent_name=agent, instruction=f"task {nid}", deps=deps or [])


def _plan(*nodes: PlanNode) -> DagPlan:
    return DagPlan(nodes=list(nodes))


def test_valid_plan():
    validate_plan(_plan(_node("a"), _node("b", deps=["a"])))


def test_empty_plan_ok():
    validate_plan(_plan())


def test_duplicate_id_rejected():
    with pytest.raises(PlanValidationError, match="duplicate"):
        validate_plan(_plan(_node("a"), _node("a")))


def test_unknown_dep_rejected():
    with pytest.raises(PlanValidationError, match="unknown dependency"):
        validate_plan(_plan(_node("a", deps=["ghost"])))


def test_self_dep_rejected():
    with pytest.raises(PlanValidationError, match="itself"):
        validate_plan(_plan(_node("a", deps=["a"])))


def test_cycle_rejected():
    with pytest.raises(PlanValidationError, match="cycle"):
        validate_plan(_plan(_node("a", deps=["b"]), _node("b", deps=["a"])))


def test_cycle_of_three_rejected():
    with pytest.raises(PlanValidationError, match="cycle"):
        validate_plan(_plan(_node("a", deps=["c"]), _node("b", deps=["a"]), _node("c", deps=["b"])))


def test_too_many_nodes_rejected():
    with pytest.raises(PlanValidationError, match="too many"):
        validate_plan(_plan(*(_node(f"n{i}") for i in range(21))))


def test_custom_max_nodes():
    validate_plan(_plan(_node("a"), _node("b")), max_nodes=2)
    with pytest.raises(PlanValidationError):
        validate_plan(_plan(_node("a"), _node("b")), max_nodes=1)


def test_parallel_nodes_ok():
    validate_plan(_plan(_node("a"), _node("b"), _node("c", deps=["a", "b"])))


def test_output_ref_roundtrip():
    assert output_ref("b1") == "{{output:b1}}"
    assert resolve_instruction(f"see {output_ref('b1')}", {"b1": "data"}) == "see data"


def test_resolve_instruction_missing_id_empty():
    assert resolve_instruction(f"see {output_ref('ghost')}", {}) == "see "


def test_resolve_instruction_multiple():
    text = f"{output_ref('a')} and {output_ref('b')}"
    assert resolve_instruction(text, {"a": "1", "b": "2"}) == "1 and 2"
