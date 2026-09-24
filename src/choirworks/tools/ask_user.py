from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, Field

from choirworks.orchestration.state import NodeStatus, QuestionType, add_intervention
from choirworks.orchestration.transitions import apply_transition
from choirworks.tools.base import AgentFunction, FunctionResult

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)


class AskUserArgs(BaseModel):
    """Arguments for ``ask_user`` — request human input for a blocked node."""

    node_id: str
    question: str
    question_type: QuestionType = QuestionType.INPUT
    options: list[str] = Field(default_factory=list)
    multi: bool = False


class AskUserData(BaseModel):
    """Result payload of ``ask_user``."""

    intervention_id: str
    node_id: str
    agent_name: str
    requester: str
    question: str
    question_type: QuestionType
    options: list[str]
    multi: bool


async def ask_user_args_model(ctx: OrchestrationContext) -> type[BaseModel]:
    return AskUserArgs


async def execute_ask_user(ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
    """``ask_user`` — the orchestrator requests human input to unblock a node.

    Creates an intervention record and returns an ack; the real answer arrives
    later as a user message carrying a ``question_response`` data part, keyed
    by ``intervention_id``.  The question itself is delivered separately via
    the aggregated input-required ``status.message`` (see
    :func:`choirworks.orchestration.events.emit_pending_questions`).
    """
    ask_args = (
        args if isinstance(args, AskUserArgs) else AskUserArgs.model_validate(args.model_dump())
    )
    state = ctx.state
    node = state.nodes.get(ask_args.node_id)
    if node is None:
        logger.warning("ask_user: unknown node", node_id=ask_args.node_id)
        return FunctionResult(success=False, error=f"unknown node: {ask_args.node_id}")

    logger.info(
        "ask_user",
        node_id=node.id,
        agent=node.agent_name,
        question_len=len(ask_args.question),
        question_type=ask_args.question_type,
    )
    apply_transition(node, NodeStatus.INPUT_REQUIRED)
    node.question = ask_args.question

    intervention = add_intervention(
        state,
        node.id,
        ask_args.question,
        question_type=ask_args.question_type,
        options=ask_args.options,
        multi=ask_args.multi,
        requester=node.agent_name,
    )

    return FunctionResult(
        success=True,
        data=AskUserData(
            intervention_id=intervention.id,
            node_id=node.id,
            agent_name=node.agent_name,
            requester=node.agent_name,
            question=intervention.question,
            question_type=intervention.question_type,
            options=list(intervention.options),
            multi=intervention.multi,
        ),
    )


ask_user_func = AgentFunction(
    name="ask_user",
    description="Ask the human a question to unblock a stalled agent node.",
    args_model=ask_user_args_model,
    execute=execute_ask_user,
    is_long_running=True,
    emit_artifact=False,
)
