from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, create_model

from choirworks.a2a.registry import AgentRegistry
from choirworks.core.context import build_planner_capabilities, build_planner_user_message
from choirworks.core.llm import LiteLLMClient
from choirworks.models.domain import AgentRecord

logger = logging.getLogger(__name__)


class PlanNodeDraft(BaseModel):
    id: str
    name: str
    agent_name: str
    skill_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)


class PlanDraft(BaseModel):
    nodes: list[PlanNodeDraft]


# Enum-pinned structured output mirrors google-adk's TransferToAgentTool
# (src/google/adk/tools/transfer_to_agent_tool.py, Apache-2.0), which
# constrains agent_name to a JSON-Schema enum so hallucinated names cannot
# pass validation.
def constrained_plan_schema(agent_names: Sequence[str]) -> type[PlanDraft]:
    node = create_model(
        "PlanNodeDraft",
        __base__=PlanNodeDraft,
        agent_name=(Literal[*agent_names], ...),
    )
    return create_model(
        "PlanDraft",
        __base__=PlanDraft,
        nodes=(list[node], ...),
    )


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
        if node.skill_id is not None:
            record = agents_by_name[node.agent_name]
            skills = {
                skill.get("id") for skill in record.card.get("skills", [])
            }
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


class PlanningFailed(RuntimeError):
    pass


SYSTEM_PROMPT = """You are the planning brain of a multi-agent orchestration platform.
Decompose the user's request into a DAG of tasks, each assigned to one registered agent.

First explain your decomposition briefly in your response text, then call the
PlanDraft tool with the final plan.

Rules:
- agent_name is enum-constrained to the registered agents listed in the user message.
- skill_id, when set, must be an existing skill id of the assigned agent.
- Use deps to express ordering; independent nodes run in parallel.
- Keep the plan minimal: only nodes required to fulfill the request.
- Put the exact instruction for the agent in each node's input.text."""


class Planner:
    def __init__(
        self,
        llm: LiteLLMClient,
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
    ) -> AsyncIterator[str | PlanDraft]:
        """Stream the planning thought process, then yield the final draft.

        Yields:
            Plain-text thinking chunks followed by exactly one ``PlanDraft``.
        """
        agents = await self._registry.list()
        if not agents:
            raise PlanningFailed(
                "no agents registered; register at least one A2A agent first"
            )
        schema = constrained_plan_schema([agent.name for agent in agents])
        capabilities = build_planner_capabilities(agents)
        user = build_planner_user_message(
            request, capabilities, reason=reason, context=context
        )

        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            if attempt > 0:
                logger.warning(
                    "plan validation failed (attempt %d/%d): %s",
                    attempt,
                    self._max_retries + 1,
                    last_error,
                )

            draft: PlanDraft | None = None
            try:
                async for item in self._llm.stream_structured(
                    system=SYSTEM_PROMPT,
                    user=user,
                    schema=schema,
                    tool_name="PlanDraft",
                ):
                    if isinstance(item, PlanDraft):
                        draft = item
                    else:
                        yield item
                if draft is None:
                    raise ValueError("model did not call the PlanDraft tool")
                validate_plan(draft, agents, self._max_nodes)
            except (PlanValidationError, ValidationError, ValueError) as exc:
                last_error = exc
                user += (
                    f"\n\nPrevious plan was invalid: {exc}."
                    " Return a corrected plan."
                )
                continue

            yield draft
            return

        raise PlanningFailed(
            f"planner failed after {self._max_retries + 1} attempts: {last_error}"
        )
