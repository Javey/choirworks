from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, create_model

from choirworks.a2a.state import NodeState
from choirworks.core.context import build_peer_fallback_input
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult


def _call_subagent_schema(candidate_names: list[str]) -> type[BaseModel]:
    fields: dict[str, object] = {}
    if candidate_names:
        fields["target_agent"] = (Literal[*candidate_names], ...)  # type: ignore[valid-type]
    return create_model("CallSubagentArgs", __base__=CallSubagentArgs, **fields)


class CallSubagentArgs(BaseModel):
    """Arguments for ``call_subagent`` — delegate a sub-task to a peer agent."""

    requester_node_id: str
    target_agent: str
    instruction: str = ""


class CallSubagentFunction(AgentFunction):
    """``call_subagent`` — spawn a derived helper node handled by a peer agent.

    When the orchestrator decides a node needs assistance from another agent,
    it calls this function instead of emitting an ``assist.dispatched`` kind
    event.  The function creates a derived :class:`NodeState`, joins the helper
    agent as a room member, and returns the helper node id.  Execution starts
    asynchronously via the runner — the model is not kept waiting.
    """

    name = "call_subagent"
    description = "Delegate a sub-task to a peer agent for assistance."
    is_long_running = True

    async def args_model(self, ctx: FunctionContext) -> type[BaseModel]:
        agents = await ctx.registry.list()
        candidates = [agent.name for agent in agents]
        return _call_subagent_schema(candidates)

    async def execute(
        self, ctx: FunctionContext, args: BaseModel
    ) -> FunctionResult:
        call_args = args if isinstance(args, CallSubagentArgs) else CallSubagentArgs.model_validate(
            args.model_dump()
        )
        state = ctx.state
        executor = ctx.executor
        runtime = ctx.runtime

        if state.derived_count >= executor._max_derived_nodes:
            return FunctionResult(success=False, error="max derived nodes reached")

        agents = await ctx.registry.list()
        agent = next(
            (item for item in agents if item.name == call_args.target_agent), None
        )
        if agent is None:
            return FunctionResult(
                success=False, error=f"unknown agent: {call_args.target_agent}"
            )

        state.derived_count += 1
        requester_node = state.nodes.get(call_args.requester_node_id)
        helper_id = f"{call_args.requester_node_id}-h{state.derived_count}"

        helper = NodeState(
            id=helper_id,
            name="",
            agent_name=agent.name,
            agent_url=agent.card_url,
            deps=[],
            input_text=call_args.instruction
            or build_peer_fallback_input(
                requester_node.question if requester_node else ""
            ),
            derived=True,
            assist_requested_by=call_args.requester_node_id,
        )
        state.nodes[helper_id] = helper
        await executor._join_members(runtime, [agent.name], "peer_assist")
        await executor._persist(runtime)

        return FunctionResult(
            success=True,
            data={
                "helper_node_id": helper_id,
                "helper": agent.name,
                "requester": requester_node.agent_name if requester_node else "",
            },
        )
