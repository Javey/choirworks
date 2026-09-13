from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, Field

from choirworks.a2a.registry import AgentRegistry
from choirworks.core.llm import LLMClient
from choirworks.models.domain import AgentRecord


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
                "agent_name": node.agent_name,
                "skill_id": node.skill_id,
                "deps": node.deps,
                "input": node.input,
                "requires_approval": node.requires_approval,
                "policy_override": node.policy_override,
            }
            for node in draft.nodes
        ]
    }


class PlanningFailed(RuntimeError):
    pass


SYSTEM_PROMPT = """You are the planning brain of a multi-agent orchestration platform.
Decompose the user's request into a DAG of tasks, each assigned to one registered agent.
Return only JSON matching the required schema. Rules:
- Every node must reference an existing agent_name and, when provided, an existing skill_id.
- Use deps to express ordering; independent nodes run in parallel.
- Keep the plan minimal: only nodes required to fulfill the request.
- Put the exact instruction for the agent in each node's input.text."""


class Planner:
    def __init__(
        self,
        llm: LLMClient,
        registry: AgentRegistry,
        *,
        max_nodes: int = 20,
        max_retries: int = 2,
    ):
        self._llm = llm
        self._registry = registry
        self._max_nodes = max_nodes
        self._max_retries = max_retries

    async def plan(
        self,
        request: str,
        *,
        reason: str | None = None,
        context: str | None = None,
    ) -> PlanDraft:
        agents = await self._registry.list()
        if not agents:
            raise PlanningFailed("no agents registered; register at least one A2A agent first")
        capabilities = self._capabilities_text(agents)
        user = f"User request:\n{request}\n\nAvailable agents:\n{capabilities}"
        if reason:
            user += f"\n\nReason for replanning:\n{reason}"
        if context:
            user += f"\n\nCompleted work so far:\n{context}"

        last_error: Exception | None = None
        for _ in range(self._max_retries + 1):
            draft = await self._llm.structured(
                system=SYSTEM_PROMPT, user=user, schema=PlanDraft
            )
            try:
                validate_plan(draft, agents, self._max_nodes)
                return draft
            except PlanValidationError as exc:
                last_error = exc
                user += f"\n\nPrevious plan was invalid: {exc}. Return a corrected plan."
        raise PlanningFailed(
            f"planner failed after {self._max_retries + 1} attempts: {last_error}"
        )

    @staticmethod
    def _capabilities_text(agents: Sequence[AgentRecord]) -> str:
        lines = []
        for agent in agents:
            skills = agent.card.get("skills", [])
            skill_text = "; ".join(
                f"{skill.get('id')} ({skill.get('description', '')})" for skill in skills
            )
            lines.append(
                f"- {agent.name}: {agent.card.get('description', '')} skills=[{skill_text}]"
            )
        return "\n".join(lines)
