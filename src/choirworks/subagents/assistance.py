from __future__ import annotations

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.helpers import as_model
from choirworks.subagents.base import InternalSubagent
from choirworks.tools.base import AgentFunction
from choirworks.tools.outcome_decision import (
    ASSISTANCE_SYSTEM,
    OutcomeDecision,
    OutcomeDecisionTool,
    outcome_decision_schema,
)


class AssistanceSubagent(InternalSubagent):
    """``assistance`` — decide how to handle a blocked agent.

    When a node is ``input_required``, this subagent decides whether another
    registered agent could help (``target_agent`` set) or a human must
    answer (``target_agent`` empty).  Falls back to ``need_info`` on
    failure.
    """

    name = "assistance"
    system_prompt = ASSISTANCE_SYSTEM
    tool_name = "OutcomeDecision"

    async def _build_tools(
        self, ctx: OrchestrationContext,
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
        return [OutcomeDecisionTool(schema)]

    async def run(
        self,
        ctx: OrchestrationContext,
        user: str,
        *,
        exclude_agent: str = "",
        **kwargs: object,
    ) -> OutcomeDecision | None:
        tool_call = await super().run(
            ctx, user, exclude_agent=exclude_agent, **kwargs
        )
        if tool_call is None:
            return OutcomeDecision(intent="need_info")
        return as_model(tool_call, OutcomeDecision)
