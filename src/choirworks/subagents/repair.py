"""``repair`` — produce an incremental patch for a failed plan.

Given the set of failed nodes, the LLM returns a :class:`RepairResult`
with a :class:`PlanPatch`.  No retries — repair is best-effort, and a
missing tool call yields ``None``.
"""

from __future__ import annotations

from typing import override

from choirworks.core.agents.context import TurnContext
from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.util import as_model
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    REPAIR_SYSTEM,
    RepairResult,
    decision_tool,
)

_REPAIR_TOOL_DESCRIPTION = "Produce an incremental repair patch for a failed plan."


class RepairAgent(LlmAgent[RepairResult | None]):
    name = "repair"
    system_prompt = REPAIR_SYSTEM
    final_tool = "RepairDecision"
    max_retries = 0

    @override
    async def build_tools(self, ctx: TurnContext, **kwargs: object) -> list[AgentFunction]:
        return [decision_tool("RepairDecision", _REPAIR_TOOL_DESCRIPTION, RepairResult)]

    @override
    def process(self, tool_call: ToolCallResult | None) -> RepairResult | None:
        if tool_call is None:
            return None
        return as_model(tool_call, RepairResult)


repair_agent = RepairAgent()
