"""``outcome`` — interpret a completed node's reply.

Decides whether the agent's output is a finished deliverable, a request
for more information, or a structural plan revision.
"""

from __future__ import annotations

from typing import override

from choirworks.core.agents.context import TurnContext
from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.tool import FunctionTool, StructuredOutputTool, ToolCallResult
from choirworks.core.util import as_model
from choirworks.subagents.outcome.model import OutcomeResult
from choirworks.subagents.outcome.prompt import OUTCOME_SYSTEM

_OUTCOME_TOOL_DESCRIPTION = "Decide what an agent's final reply means for the plan."


class OutcomeAgent(LlmAgent[OutcomeResult]):
    name = "outcome"
    system_prompt = OUTCOME_SYSTEM
    final_tool = "OutcomeDecision"

    @override
    async def build_tools(self, ctx: TurnContext, **kwargs: object) -> list[FunctionTool]:
        return [
            StructuredOutputTool(
                name="OutcomeDecision",
                description=_OUTCOME_TOOL_DESCRIPTION,
                schema=OutcomeResult,
            )
        ]

    @override
    def process(self, tool_call: ToolCallResult | None) -> OutcomeResult:
        if tool_call is None:
            return OutcomeResult(intent="deliver")
        return as_model(tool_call, OutcomeResult)


outcome_agent = OutcomeAgent()
