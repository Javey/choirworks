from __future__ import annotations

from typing import TYPE_CHECKING, Literal, override

import structlog
from pydantic import BaseModel, create_model

from choirworks.core.prompts import build_peer_fallback_input
from choirworks.core.tool import FunctionResult, FunctionTool
from choirworks.orchestration.planning.derived import DerivedKind, spawn_derived_node

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

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

    ``requested_by`` is either ``"assistant"`` (the planning layer dispatches a
    plan node) or the id of the node asking for help (a derived helper node is
    spawned).
    """

    requested_by: str
    target_agent: str
    instruction: str = ""


class CallSubagentTool(FunctionTool):
    name = "call_subagent"
    description = "Delegate a sub-task to a peer agent for assistance."

    @override
    async def _get_declaration(self, ctx: OrchestrationContext) -> type[BaseModel]:
        agents = await ctx.registry.list()
        candidates = [agent.name for agent in agents]
        return _call_subagent_schema(candidates)

    @override
    async def run_async(self, ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
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
            instruction=call_args.instruction,
        )

        if call_args.requested_by == "assistant":
            return FunctionResult(
                success=False,
                error="assistant dispatch does not create helper nodes",
            )

        agents = await ctx.registry.list()
        agent = next((item for item in agents if item.name == call_args.target_agent), None)
        if agent is None:
            return FunctionResult(success=False, error=f"unknown agent: {call_args.target_agent}")

        requester_node = state.nodes.get(call_args.requested_by)
        helper = await spawn_derived_node(
            ctx,
            DerivedKind.HELPER,
            parent_id=call_args.requested_by,
            agent_name=agent.name,
            input_text=call_args.instruction
            or build_peer_fallback_input(
                (requester_node.question if requester_node else None) or ""
            ),
            assist_requested_by=call_args.requested_by,
            emit=False,
        )
        if helper is None:
            return FunctionResult(success=False, error="max derived nodes reached")

        return FunctionResult(
            success=True,
            data=CallSubagentData(
                helper_node_id=helper.id,
                helper=agent.name,
                requester=requester_node.agent_name if requester_node else "",
            ),
        )


call_subagent_func = CallSubagentTool()
