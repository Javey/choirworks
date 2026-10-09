"""``outcome`` — interpret a completed node's reply.

Decides whether the agent's output is a finished deliverable, a request
for more information, or a structural plan revision.
"""

from __future__ import annotations

from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.util import as_model
from choirworks.orchestration.context import OrchestrationContext
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.outcome_decision import (
    OUTCOME_SYSTEM,
    OutcomeResult,
    decision_tool,
)


async def build_outcome_tools(
    ctx: OrchestrationContext,
    **kwargs: object,
) -> list[AgentFunction]:
    return [
        decision_tool(
            "OutcomeDecision",
            "Decide what an agent's final reply means for the plan.",
            OutcomeResult,
        )
    ]


def _process_outcome(tool_call: ToolCallResult | None) -> OutcomeResult:
    if tool_call is None:
        return OutcomeResult(intent="deliver")
    return as_model(tool_call, OutcomeResult)


OUTCOME_AGENT: LlmAgent[OutcomeResult] = LlmAgent(
    name="outcome",
    system_prompt=OUTCOME_SYSTEM,
    final_tool="OutcomeDecision",
    build_tools=build_outcome_tools,
    process=_process_outcome,
)
