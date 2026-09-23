from __future__ import annotations

from typing import Literal

import structlog
from pydantic import BaseModel, create_model

from choirworks.core.context import build_peer_fallback_input
from choirworks.orchestration.state import NodeState
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult

logger = structlog.get_logger(__name__)


def _call_subagent_schema(candidate_names: list[str]) -> type[BaseModel]:
    if not candidate_names:
        return CallSubagentArgs
    return create_model(
        "CallSubagentArgs",
        __base__=CallSubagentArgs,
        target_agent=(Literal[*candidate_names], ...),  # type: ignore[valid-type]
    )


class CallSubagentData(BaseModel):
    """Result payload of ``call_subagent``."""

    helper_node_id: str
    helper: str
    requester: str


class CallSubagentArgs(BaseModel):
    """Arguments for ``call_subagent`` — delegate a sub-task to a peer agent.

    ``requested_by`` is either ``"orchestrator"`` (a plan node is being
    dispatched) or the id of the node asking for help (a derived helper node
    is spawned).
    """

    requested_by: str
    target_agent: str
    instruction: str = ""


async def call_subagent_args_model(ctx: FunctionContext) -> type[BaseModel]:
    agents = await ctx.registry.list()
    candidates = [agent.name for agent in agents]
    return _call_subagent_schema(candidates)


async def execute_call_subagent(ctx: FunctionContext, args: BaseModel) -> FunctionResult:
    """``call_subagent`` — spawn a derived helper node handled by a peer agent.

    When the orchestrator decides a node needs assistance from another agent,
    it calls this function instead of emitting an ``assist.dispatched`` kind
    event.  The function creates a derived :class:`NodeState`, joins the helper
    agent as a room member, and returns the helper node id.  Execution starts
    asynchronously via the runner — the model is not kept waiting.
    """
    call_args = (
        args
        if isinstance(args, CallSubagentArgs)
        else CallSubagentArgs.model_validate(args.model_dump())
    )
    state = ctx.state

    logger.info(
        "call_subagent",
        requested_by=call_args.requested_by,
        target=call_args.target_agent,
        instruction_len=len(call_args.instruction),
    )

    if call_args.requested_by == "orchestrator":
        return FunctionResult(
            success=False,
            error="orchestrator dispatch does not create helper nodes",
        )

    if state.derived_count >= ctx.effects.max_derived_nodes:
        return FunctionResult(success=False, error="max derived nodes reached")

    agents = await ctx.registry.list()
    agent = next((item for item in agents if item.name == call_args.target_agent), None)
    if agent is None:
        return FunctionResult(success=False, error=f"unknown agent: {call_args.target_agent}")

    state.derived_count += 1
    requester_node = state.nodes.get(call_args.requested_by)
    helper_id = f"{call_args.requested_by}-h{state.derived_count}"

    helper = NodeState(
        id=helper_id,
        name="",
        agent_name=agent.name,
        agent_url=agent.card_url,
        deps=[],
        input_text=call_args.instruction
        or build_peer_fallback_input((requester_node.question if requester_node else None) or ""),
        derived=True,
        assist_requested_by=call_args.requested_by,
    )
    state.nodes[helper_id] = helper
    await ctx.effects.join_members([agent.name], "peer_assist")
    await ctx.effects.persist()

    return FunctionResult(
        success=True,
        data=CallSubagentData(
            helper_node_id=helper_id,
            helper=agent.name,
            requester=requester_node.agent_name if requester_node else "",
        ),
    )


call_subagent_func = AgentFunction(
    name="call_subagent",
    description="Delegate a sub-task to a peer agent for assistance.",
    args_model=call_subagent_args_model,
    execute=execute_call_subagent,
    is_long_running=True,
)
