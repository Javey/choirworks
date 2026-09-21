from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, create_model

from choirworks.orchestration.patch import PlanPatch
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult

OUTCOME_SYSTEM = """You are the orchestrator of a multi-agent group.
Read an agent's final reply and decide what it means for the plan:
- intent="deliver": the reply is the finished work (default when unsure)
- intent="need_info": the reply asks for information, help from a member, or a human decision
- intent="revise": the reply reveals new information that structurally changes the plan

Rules:
- intent="deliver" is the default. The agent completed its task. Do NOT judge whether the
  output is good, complete, or matches the instructions — quality is the agent's responsibility.
- intent="need_info": the agent explicitly requests help, information, or a human decision.
- intent="revise": use ONLY when the agent's reply contains information that changes what work
  the plan needs (e.g., "this is a static site, no backend needed" or "we also need a design
  step"). Do NOT use revise because the output is low quality, incomplete, or doesn't match
  instructions — that is the agent's responsibility, not the orchestrator's.

When intent="need_info", put what is needed into question and set target_agent to the
listed candidate who can help; leave target_agent empty when a human must answer.
When intent="deliver" or intent="revise", leave question, target_agent and instruction empty.
Return only JSON matching the schema."""

ASSISTANCE_SYSTEM = """You are the orchestrator of a multi-agent group.
An agent is blocked and needs help. Decide how to handle it:
- set target_agent to another registered agent that can help
- leave target_agent empty to escalate to a human

Set intent="need_info". When target_agent is set, instruction should describe the
task. Return only JSON matching the schema.
- reasoning: one short sentence explaining your decision."""

REPAIR_SYSTEM = """You are the orchestrator of a multi-agent group.
Some tasks in the plan failed after retries. Produce an incremental repair patch:
- intent="revise" with a patch that adds replacement tasks and/or invalidates tasks
- added tasks may only depend on existing task ids
- do not repeat work that is already completed; keep the plan minimal
Return only JSON matching the schema."""


class OutcomeDecision(BaseModel):
    """What an agent's final reply means for the plan."""

    intent: Literal["deliver", "need_info", "revise"]
    question: str = ""
    target_agent: str | None = None
    instruction: str = ""
    patch: PlanPatch | None = None
    reasoning: str = ""


def outcome_decision_schema(
    candidate_names: Sequence[str],
) -> type[OutcomeDecision]:
    fields: dict[str, Any] = {}
    if candidate_names:
        fields["target_agent"] = (Literal[*candidate_names] | None, None)
    return create_model("OutcomeDecision", __base__=OutcomeDecision, **fields)


def outcome_decision_tool(schema: type[OutcomeDecision]) -> AgentFunction:
    """``OutcomeDecision`` — structured-output tool for interpreting agent replies.

    Has no side effects; the caller reads the validated ``OutcomeDecision``
    from :class:`ToolCallResult.args` and acts on it.  The schema is captured
    in a closure so the dynamic enum constraint stays per-call.
    """

    async def args_model(ctx: FunctionContext) -> type[BaseModel]:
        return schema

    async def execute(ctx: FunctionContext, args: BaseModel) -> FunctionResult:
        return FunctionResult(success=True)

    return AgentFunction(
        name="OutcomeDecision",
        description="Decide what an agent's final reply means for the plan.",
        args_model=args_model,
        execute=execute,
    )
