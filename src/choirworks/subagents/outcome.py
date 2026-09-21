"""``outcome`` — interpret a completed node's reply.

Decides whether the agent's output is a finished deliverable, a request
for more information, or a structural plan revision.  The
``outcome_decision_tool`` schema is built dynamically per call with an
enum-constrained ``target_agent`` based on currently registered agents.
"""

from __future__ import annotations

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.helpers import as_model
from choirworks.subagents.base import Subagent
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    OUTCOME_SYSTEM,
    OutcomeDecision,
    outcome_decision_schema,
    outcome_decision_tool,
)


async def build_outcome_tools(
    ctx: OrchestrationContext,
    *,
    exclude_agent: str = "",
    **kwargs: object,
) -> list[AgentFunction]:
    agents = await ctx.registry.list()
    candidates = (
        [a for a in agents if a.name != exclude_agent]
        if exclude_agent else agents
    )
    schema = outcome_decision_schema([a.name for a in candidates])
    return [outcome_decision_tool(schema)]


def _process_outcome(tool_call: ToolCallResult | None) -> OutcomeDecision:
    if tool_call is None:
        return OutcomeDecision(intent="deliver")
    return as_model(tool_call, OutcomeDecision)


OUTCOME_SUBAGENT: Subagent[OutcomeDecision] = Subagent(
    name="outcome",
    system_prompt=OUTCOME_SYSTEM,
    tool_name="OutcomeDecision",
    build_tools=build_outcome_tools,
    process=_process_outcome,
)
