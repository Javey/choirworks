from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from choirworks.orchestration.state import NodeStatus, add_intervention
from choirworks.tools.base import AgentFunction, FunctionResult

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)


class AskUserArgs(BaseModel):
    """Arguments for ``ask_user`` — request human input for a blocked node."""

    node_id: str
    question: str


class AskUserData(BaseModel):
    """Result payload of ``ask_user``."""

    intervention_id: str
    node_id: str
    agent_name: str
    question: str


async def ask_user_args_model(ctx: OrchestrationContext) -> type[BaseModel]:
    return AskUserArgs


async def execute_ask_user(ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
    """``ask_user`` — the model requests human input to unblock a node.

    The orchestrator's outcome-interpretation layer decides an agent's reply
    means "I need more info".  Instead of emitting an ``intervention.requested``
    kind event, the model calls this function.  The function creates an
    intervention record and returns an ack; the real answer arrives later as a
    user message (B-class state transition ``intervention.resolved``).
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
    )
    node.status = NodeStatus.INPUT_REQUIRED
    node.question = ask_args.question

    intervention = add_intervention(state, node.id, ask_args.question)

    return FunctionResult(
        success=True,
        data=AskUserData(
            intervention_id=intervention.id,
            node_id=node.id,
            agent_name=node.agent_name,
            question=intervention.question,
        ),
    )


ask_user_func = AgentFunction(
    name="ask_user",
    description="Ask the human a question to unblock a stalled agent node.",
    args_model=ask_user_args_model,
    execute=execute_ask_user,
    is_long_running=True,
)
