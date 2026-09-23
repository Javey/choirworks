"""``repair`` — produce an incremental patch for a failed plan.

Given the set of failed nodes, the LLM returns an ``OutcomeDecision``
with ``intent="revise"`` and a :class:`PlanPatch`.  No retries —
repair is best-effort, and a missing tool call yields ``None``.
"""

from __future__ import annotations

from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.subagents.base import Subagent
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    REPAIR_SYSTEM,
    OutcomeDecision,
    outcome_decision_schema,
    outcome_decision_tool,
)


async def build_repair_tools(ctx: OrchestrationContext, **kwargs: object) -> list[AgentFunction]:
    agents = await ctx.registry.list()
    schema = outcome_decision_schema([a.name for a in agents])
    return [outcome_decision_tool(schema)]


def _process_repair(tool_call: ToolCallResult | None) -> OutcomeDecision | None:
    if tool_call is None:
        return None
    return as_model(tool_call, OutcomeDecision)


REPAIR_SUBAGENT: Subagent[OutcomeDecision | None] = Subagent(
    name="repair",
    system_prompt=REPAIR_SYSTEM,
    tool_name="OutcomeDecision",
    build_tools=build_repair_tools,
    process=_process_repair,
    max_retries=0,
)
