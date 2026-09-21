from __future__ import annotations

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.helpers import as_model
from choirworks.subagents.base import InternalSubagent
from choirworks.tools.base import AgentFunction
from choirworks.tools.outcome_decision import (
    REPAIR_SYSTEM,
    OutcomeDecision,
    OutcomeDecisionTool,
    outcome_decision_schema,
)


class RepairSubagent(InternalSubagent):
    """``repair`` — produce an incremental patch for a failed plan.

    Given the set of failed nodes, the LLM returns an ``OutcomeDecision``
    with ``intent="revise"`` and a :class:`PlanPatch`.  No retries —
    repair is best-effort.
    """

    name = "repair"
    system_prompt = REPAIR_SYSTEM
    tool_name = "OutcomeDecision"
    max_retries = 0

    async def _build_tools(
        self, ctx: OrchestrationContext, **kwargs: object
    ) -> list[AgentFunction]:
        agents = await ctx.registry.list()
        schema = outcome_decision_schema([a.name for a in agents])
        return [OutcomeDecisionTool(schema)]

    async def run(
        self,
        ctx: OrchestrationContext,
        user: str,
        **kwargs: object,
    ) -> OutcomeDecision | None:
        tool_call = await super().run(ctx, user, **kwargs)
        if tool_call is None:
            return None
        return as_model(tool_call, OutcomeDecision)
