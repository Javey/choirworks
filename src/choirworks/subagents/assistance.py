"""``assistance`` — decide how to handle a blocked agent.

When a node is ``input_required``, this agent decides whether another
registered agent could help (``target_agent`` set) or a human must
answer (``target_agent`` empty).  Falls back to an empty
``AssistanceResult`` (escalate to human) on failure.
"""

from __future__ import annotations

from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    ASSISTANCE_SYSTEM,
    AssistanceResult,
    assistance_schema,
    decision_tool,
)


async def build_assistance_tools(
    ctx: OrchestrationContext,
    *,
    exclude_agent: str = "",
    **kwargs: object,
) -> list[AgentFunction]:
    agents = await ctx.registry.list()
    candidates = [a for a in agents if a.name != exclude_agent] if exclude_agent else agents
    schema = assistance_schema([a.name for a in candidates])
    return [decision_tool("AssistanceDecision", "Decide how to handle a blocked agent.", schema)]


def _process_assistance(tool_call: ToolCallResult | None) -> AssistanceResult:
    if tool_call is None:
        return AssistanceResult()
    return as_model(tool_call, AssistanceResult)


ASSISTANCE_AGENT: LlmAgent[AssistanceResult] = LlmAgent(
    name="assistance",
    system_prompt=ASSISTANCE_SYSTEM,
    final_tool="AssistanceDecision",
    build_tools=build_assistance_tools,
    process=_process_assistance,
)
