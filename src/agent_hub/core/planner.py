from __future__ import annotations

from typing import Any, Sequence

from pydantic import BaseModel, Field

from agent_hub.models.domain import AgentRecord


class PlanNodeDraft(BaseModel):
    id: str
    name: str
    agent_name: str
    skill_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)
    requires_approval: bool = False
    policy_override: str | None = None


class PlanDraft(BaseModel):
    rationale: str
    nodes: list[PlanNodeDraft]


class PlanValidationError(ValueError):
    pass


def validate_plan(
    draft: PlanDraft, agents: Sequence[AgentRecord], max_nodes: int = 20
) -> None:
    if not draft.nodes:
        raise PlanValidationError("plan has no nodes")
    if len(draft.nodes) > max_nodes:
        raise PlanValidationError(f"too many nodes: {len(draft.nodes)} > {max_nodes}")

    ids = [node.id for node in draft.nodes]
    if len(set(ids)) != len(ids):
        raise PlanValidationError("duplicate node id in plan")
    id_set = set(ids)
    agents_by_name = {agent.name: agent for agent in agents}

    for node in draft.nodes:
        record = agents_by_name.get(node.agent_name)
        if record is None:
            raise PlanValidationError(f"unknown agent: {node.agent_name}")
        if node.skill_id is not None:
            skills = {skill.get("id") for skill in record.card.get("skills", [])}
            if node.skill_id not in skills:
                raise PlanValidationError(
                    f"unknown skill '{node.skill_id}' for agent {node.agent_name}"
                )
        for dep in node.deps:
            if dep not in id_set:
                raise PlanValidationError(f"unknown dependency: {dep}")
            if dep == node.id:
                raise PlanValidationError(f"node {node.id} depends on itself")

    indegree = {node.id: len(set(node.deps)) for node in draft.nodes}
    children: dict[str, list[str]] = {node.id: [] for node in draft.nodes}
    for node in draft.nodes:
        for dep in set(node.deps):
            children[dep].append(node.id)
    queue = [node_id for node_id, degree in indegree.items() if degree == 0]
    visited = 0
    while queue:
        current = queue.pop()
        visited += 1
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited != len(ids):
        raise PlanValidationError("plan contains a cycle")


def draft_to_dag(draft: PlanDraft, agent_urls: dict[str, str]) -> dict[str, Any]:
    return {
        "nodes": [
            {
                "id": node.id,
                "name": node.name,
                "agent_url": agent_urls[node.agent_name],
                "skill_id": node.skill_id,
                "deps": node.deps,
                "input": node.input,
                "requires_approval": node.requires_approval,
                "policy_override": node.policy_override,
            }
            for node in draft.nodes
        ]
    }
