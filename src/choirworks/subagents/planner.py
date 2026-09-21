from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from choirworks.a2a.context import OrchestrationContext
from choirworks.core.llm import LiteLLMClient
from choirworks.subagents.base import InternalSubagent
from choirworks.tools.base import AgentFunction

if TYPE_CHECKING:
    pass


class PlannerSubagent(InternalSubagent):
    """``planner`` — decompose a user request into a DAG of tasks.

    Uses the ``create_plan`` tool with an enum-constrained schema so the
    model cannot hallucinate agent names.  Validation (cycle detection,
    skill checks, etc.) is handled by the :class:`Planner` class which
    wraps this subagent.
    """

    name = "planner"
    system_prompt = """You are the planning brain of a multi-agent orchestration platform.
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
    tool_name = "create_plan"

    def __init__(
        self,
        llm: LiteLLMClient,
        create_plan_tool: AgentFunction,
        *,
        max_nodes: int = 20,
        max_retries: int = 2,
    ):
        super().__init__(llm, max_retries=max_retries)
        self._create_plan = create_plan_tool
        self._max_nodes = max_nodes

    async def _build_tools(
        self, ctx: OrchestrationContext, **kwargs: object
    ) -> list[AgentFunction]:
        return [self._create_plan]

    def _validate(self, args: BaseModel, ctx: OrchestrationContext) -> None:
        pass
