from __future__ import annotations

from choirworks.a2a.context import OrchestrationContext
from choirworks.subagents.base import InternalSubagent
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    OUTCOME_SYSTEM,
    OutcomeDecision,
    OutcomeDecisionTool,
    outcome_decision_schema,
)


def _as_model(item: ToolCallResult, model: type[OutcomeDecision]) -> OutcomeDecision:
    return item.args if isinstance(item.args, model) else model.model_validate(
        item.args.model_dump()
    )


class OutcomeSubagent(InternalSubagent):
    """``outcome`` — interpret a completed node's reply.

    Decides whether the agent's output is a finished deliverable, a request
    for more information, or a structural plan revision.  The
    ``OutcomeDecisionTool`` schema is built dynamically per call with an
    enum-constrained ``target_agent`` based on currently registered agents.
    """

    name = "outcome"
    system_prompt = OUTCOME_SYSTEM
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
    ) -> OutcomeDecision:
        tool_call = await super().run(
            ctx, user, exclude_agent=exclude_agent, **kwargs
        )
        if tool_call is None:
            return OutcomeDecision(intent="deliver")
        return _as_model(tool_call, OutcomeDecision)
