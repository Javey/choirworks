from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field, create_model

from choirworks.orchestration.planning.patch import PlanPatch
from choirworks.orchestration.state import QuestionType
from choirworks.tools.base import AgentFunction, FunctionResult

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

OUTCOME_SYSTEM = """You are the assistant of a multi-agent group.
Read an agent's final reply and decide what it means for the plan:
- intent="deliver": the reply is the finished work (default when unsure)
- intent="need_info": the reply asks for information, help from a member, or a human decision
- intent="revise": the reply reveals new information that structurally changes the plan

Rules:
- intent="deliver" is the default. The agent completed its task. Do NOT judge whether the
  output is good, complete, or matches the instructions — quality is the agent's responsibility.
- intent="need_info": the agent explicitly requests help, information, or a human decision.
  Put what is needed into question.
- intent="revise": use ONLY when the agent's reply contains information that changes what work
  the plan needs (e.g., "this is a static site, no backend needed" or "we also need a design
  step"). Do NOT use revise because the output is low quality, incomplete, or doesn't match
  instructions — that is the agent's responsibility, not the assistant's. When revise, set
  patch with the incremental plan patch.

When intent="deliver" or intent="revise", leave question empty.
Return only JSON matching the schema."""

ASSISTANCE_SYSTEM = """You are the assistant of a multi-agent group.
An agent is blocked and needs help. Decide how to handle it:
- set target_agent to another registered agent that can help
- leave target_agent empty to escalate to a human

When target_agent is set, instruction should describe the task.
Return only JSON matching the schema.
- reasoning: one short sentence explaining your decision.

When escalating to a human, shape the question interface:
- question_type="confirm" when it is a yes/no decision
- question_type="select" with options when the choices are enumerable; set multi=true
  when more than one option can be chosen
- question_type="input" otherwise (default)
Leave question_type/options/multi at their defaults when a peer agent is chosen."""

REPAIR_SYSTEM = """You are the assistant of a multi-agent group.
Some tasks in the plan failed after retries. Produce an incremental repair patch:
- a patch that adds replacement tasks and/or invalidates tasks
- added tasks may only depend on existing task ids
- do not repeat work that is already completed; keep the plan minimal
Return only JSON matching the schema."""


class OutcomeResult(BaseModel):
    """What an agent's final reply means for the plan."""

    intent: Literal["deliver", "need_info", "revise"]
    question: str = ""
    patch: PlanPatch | None = None
    reasoning: str = ""


class AssistanceResult(BaseModel):
    """How to handle a blocked agent."""

    target_agent: str | None = None
    instruction: str = ""
    reasoning: str = ""
    question_type: QuestionType = QuestionType.INPUT
    options: list[str] = Field(default_factory=list)
    multi: bool = False


class RepairResult(BaseModel):
    """An incremental repair patch for a failed plan."""

    patch: PlanPatch
    reasoning: str = ""


def assistance_schema(candidate_names: Sequence[str]) -> type[AssistanceResult]:
    """Dynamically constrain ``target_agent`` to the registered candidate agents."""
    if not candidate_names:
        return AssistanceResult
    target = Literal[*candidate_names] | None  # pyright: ignore[reportOperatorIssue]
    return create_model(
        "AssistanceResult",
        __base__=AssistanceResult,
        target_agent=(target, None),
    )


def decision_tool[T: BaseModel](
    name: str,
    description: str,
    schema: type[T],
) -> AgentFunction:
    """Structured-output tool with no side effects.

    The caller reads the validated model from :class:`ToolCallResult.args`
    and acts on it.  The schema is captured in a closure so dynamic
    constraints (e.g. ``assistance_schema``) stay per-call.
    """

    async def args_model(ctx: OrchestrationContext) -> type[BaseModel]:
        return schema

    async def execute(ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
        return FunctionResult(success=True)

    return AgentFunction(
        name=name,
        description=description,
        args_model=args_model,
        execute=execute,
    )
