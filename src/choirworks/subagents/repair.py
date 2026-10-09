"""``repair`` — produce an incremental patch for a failed plan.

Given the set of failed nodes, the LLM returns a :class:`RepairResult`
with a :class:`PlanPatch`.  No retries — repair is best-effort, and a
missing tool call yields ``None``.
"""

from __future__ import annotations

from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    REPAIR_SYSTEM,
    RepairResult,
    decision_tool,
)


async def build_repair_tools(ctx: OrchestrationContext, **kwargs: object) -> list[AgentFunction]:
    return [
        decision_tool(
            "RepairDecision",
            "Produce an incremental repair patch for a failed plan.",
            RepairResult,
        )
    ]


def _process_repair(tool_call: ToolCallResult | None) -> RepairResult | None:
    if tool_call is None:
        return None
    return as_model(tool_call, RepairResult)


REPAIR_AGENT: LlmAgent[RepairResult | None] = LlmAgent(
    name="repair",
    system_prompt=REPAIR_SYSTEM,
    final_tool="RepairDecision",
    build_tools=build_repair_tools,
    process=_process_repair,
    max_retries=0,
)
