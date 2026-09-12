import pytest

from agent_hub.core.planner import (
    PlanDraft,
    PlanNodeDraft,
    PlanValidationError,
    validate_plan,
)
from agent_hub.models.domain import AgentRecord


def make_agent(name: str, skills: list[str]) -> AgentRecord:
    from datetime import UTC, datetime

    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={
            "name": name,
            "description": f"{name} agent",
            "skills": [{"id": s, "name": s, "description": s} for s in skills],
        },
        created_at=datetime.now(UTC),
    )


AGENTS = [make_agent("research", ["search"]), make_agent("writer", ["write"])]


def node(node_id: str, agent: str, deps: list[str] | None = None, skill: str | None = None):
    return PlanNodeDraft(
        id=node_id, name=node_id, agent_name=agent, deps=deps or [], skill_id=skill
    )


def test_valid_plan_passes():
    draft = PlanDraft(
        rationale="two steps",
        nodes=[
            node("n1", "research", skill="search"),
            node("n2", "writer", deps=["n1"], skill="write"),
        ],
    )
    validate_plan(draft, AGENTS, max_nodes=10)


def test_unknown_agent_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "ghost")])
    with pytest.raises(PlanValidationError, match="unknown agent"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_unknown_skill_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research", skill="nope")])
    with pytest.raises(PlanValidationError, match="unknown skill"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_duplicate_node_id_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research"), node("n1", "writer")])
    with pytest.raises(PlanValidationError, match="duplicate node id"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_missing_dependency_rejected():
    draft = PlanDraft(rationale="x", nodes=[node("n1", "research", deps=["n9"])])
    with pytest.raises(PlanValidationError, match="unknown dependency"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_cycle_rejected():
    draft = PlanDraft(
        rationale="x",
        nodes=[node("n1", "research", deps=["n2"]), node("n2", "writer", deps=["n1"])],
    )
    with pytest.raises(PlanValidationError, match="cycle"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_too_many_nodes_rejected():
    draft = PlanDraft(rationale="x", nodes=[node(f"n{i}", "research") for i in range(4)])
    with pytest.raises(PlanValidationError, match="too many nodes"):
        validate_plan(draft, AGENTS, max_nodes=3)


def test_empty_plan_rejected():
    draft = PlanDraft(rationale="x", nodes=[])
    with pytest.raises(PlanValidationError, match="no nodes"):
        validate_plan(draft, AGENTS, max_nodes=10)
