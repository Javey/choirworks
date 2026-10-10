"""``assistance`` — decide how to handle a blocked agent.

When a node is ``input_required``, this agent decides whether another
registered agent could help (``target_agent`` set) or a human must
answer (``target_agent`` empty).  Falls back to an empty
``AssistanceResult`` (escalate to human) on failure.
"""

from __future__ import annotations

from typing import override

from choirworks.core.agents.context import TurnContext
from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.tool import FunctionTool, StructuredOutputTool, ToolCallResult
from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.subagents.assistance.model import AssistanceResult, assistance_schema
from choirworks.subagents.assistance.prompt import ASSISTANCE_SYSTEM

_ASSISTANCE_TOOL_DESCRIPTION = "Decide how to handle a blocked agent."


class AssistanceAgent(LlmAgent[AssistanceResult]):
    name = "assistance"
    system_prompt = ASSISTANCE_SYSTEM
    final_tool = "AssistanceDecision"

    @override
    async def build_tools(self, ctx: TurnContext, **kwargs: object) -> list[FunctionTool]:
        assert isinstance(ctx, OrchestrationContext)
        exclude_agent = str(kwargs.get("exclude_agent", ""))
        agents = await ctx.registry.list()
        candidates = [a for a in agents if a.name != exclude_agent] if exclude_agent else agents
        schema = assistance_schema([a.name for a in candidates])
        return [
            StructuredOutputTool(
                name="AssistanceDecision",
                description=_ASSISTANCE_TOOL_DESCRIPTION,
                schema=schema,
            )
        ]

    @override
    def process(self, tool_call: ToolCallResult | None) -> AssistanceResult:
        if tool_call is None:
            return AssistanceResult()
        return as_model(tool_call, AssistanceResult)


assistance_agent = AssistanceAgent()
