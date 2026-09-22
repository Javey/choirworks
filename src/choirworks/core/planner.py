from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING

import structlog
from litellm.types.utils import Delta
from pydantic import BaseModel, Field, ValidationError

from choirworks.core.context import build_planner_capabilities, build_planner_user_message
from choirworks.core.llm import LiteLLMClient, ToolParseError
from choirworks.models.domain import AgentRecord
from choirworks.orchestration.registry import AgentRegistry

if TYPE_CHECKING:
    from choirworks.tools.base import FunctionContext, ToolCallResult

logger = structlog.get_logger(__name__)


class PlanNodeDraft(BaseModel):
    id: str
    name: str
    agent_name: str
    skill_id: str | None = None
    input: dict[str, str] = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)


class PlanDraft(BaseModel):
    nodes: list[PlanNodeDraft]


class PlanValidationError(ValueError):
    pass


def _skill_ids(record: AgentRecord) -> set[str]:
    raw = record.card.get("skills")
    if not isinstance(raw, list):
        return set()
    ids: set[str] = set()
    for skill in raw:
        if not isinstance(skill, dict):
            continue
        skill_id = skill.get("id")
        if isinstance(skill_id, str):
            ids.add(skill_id)
    return ids


def validate_plan(
    draft: PlanDraft, agents: Sequence[AgentRecord], max_nodes: int = 20
) -> None:
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
            skills = _skill_ids(record)
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
create_plan tool with the final plan.

Rules:
- agent_name is enum-constrained to the registered agents listed in the user message.
- skill_id, when set, must be an existing skill id of the assigned agent.
- Use deps to express ordering; independent nodes run in parallel.
- Keep the plan minimal: only nodes required to fulfill the request.
- Put the exact instruction for the agent in each node's input.text.
- If the request is a greeting, chitchat, or anything that does not need
  multi-agent decomposition, reply directly in your response text and call
  create_plan with an empty nodes list."""


async def plan(
    llm: LiteLLMClient,
    registry: AgentRegistry,
    request: str,
    *,
    ctx: FunctionContext,
    reason: str | None = None,
    context: str | None = None,
    max_nodes: int = 20,
    max_retries: int = 2,
) -> AsyncIterator[Delta | ToolCallResult]:
    """Stream the planning thought process, then yield the tool call.

    Yields:
        ``Delta`` objects (streaming chunks) followed by exactly one
        ``ToolCallResult`` whose ``args`` is a :class:`PlanDraft`.
        The caller validates and executes the tool.
    """
    agents = await registry.list()
    if not agents:
        raise PlanningFailed(
            "no agents registered; register at least one A2A agent first"
        )
    capabilities = build_planner_capabilities(agents)
    user = build_planner_user_message(
        request, capabilities, reason=reason, context=context
    )

    last_error: Exception | None = None
    from choirworks.tools.base import ToolCallResult  # runtime: avoid circular import

    for attempt in range(max_retries + 1):
        if attempt > 0:
            logger.warning(
                "plan validation failed",
                attempt=attempt,
                max_attempts=max_retries + 1,
                error=last_error,
            )

        tool_call: ToolCallResult | None = None
        try:
            from choirworks.tools.create_plan import create_plan_func
            async for item in llm.stream(
                system=SYSTEM_PROMPT,
                user=user,
                tools=[create_plan_func],
                ctx=ctx,
                tool_choice={"type": "function", "function": {"name": "create_plan"}},
            ):
                if isinstance(item, ToolCallResult):
                    tool_call = item
                else:
                    yield item
            if tool_call is None:
                raise ValueError("model did not call the create_plan tool")
            if not isinstance(tool_call.args, PlanDraft):
                raise ValueError("model did not return a plan draft")
            validate_plan(tool_call.args, agents, max_nodes)
        except (PlanValidationError, ValidationError, ToolParseError, ValueError) as exc:
            last_error = exc
            if isinstance(exc, ToolParseError):
                prev_payload = exc.payload
            elif tool_call is not None:
                prev_payload = tool_call.args.model_dump_json()
            else:
                prev_payload = ""
            user += (
                f"\n\nYour previous tool call was invalid.\n"
                f"Tool call args:\n{prev_payload}\n\n"
                f"Error: {exc}\n"
                "Return a corrected tool call."
            )
            continue

        yield tool_call
        return

    raise PlanningFailed(
        f"planner failed after {max_retries + 1} attempts: {last_error}"
    )
