"""``assistance`` — decide how to handle a blocked agent.

When a node is ``input_required``, this subagent decides whether another
registered agent could help (``target_agent`` set) or a human must
answer (``target_agent`` empty).  Falls back to ``need_info`` on failure.
"""

from __future__ import annotations

from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.subagents.base import Subagent
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    ASSISTANCE_SYSTEM,
    OutcomeDecision,
    outcome_decision_schema,
    outcome_decision_tool,
)


async def build_assistance_tools(
    ctx: OrchestrationContext,
    *,
    exclude_agent: str = "",
    **kwargs: object,
) -> list[AgentFunction]:
    agents = await ctx.registry.list()
    candidates = [a for a in agents if a.name != exclude_agent] if exclude_agent else agents
    schema = outcome_decision_schema([a.name for a in candidates])
    return [outcome_decision_tool(schema)]


def _process_assistance(tool_call: ToolCallResult | None) -> OutcomeDecision:
    if tool_call is None:
        return OutcomeDecision(intent="need_info")
    return as_model(tool_call, OutcomeDecision)


ASSISTANCE_SUBAGENT: Subagent[OutcomeDecision] = Subagent(
    name="assistance",
    system_prompt=ASSISTANCE_SYSTEM,
    tool_name="OutcomeDecision",
    build_tools=build_assistance_tools,
    process=_process_assistance,
)
