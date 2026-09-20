from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, create_model

from choirworks.a2a.state import NodeState
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult


def _constrained_plan_schema(agent_names: list[str]) -> type[PlanDraft]:
    node = create_model(
        "PlanNodeDraft",
        __base__=PlanNodeDraft,
        agent_name=(Literal[*agent_names], ...),  # type: ignore[valid-type]
    )
    return create_model(
        "PlanDraft",
        __base__=PlanDraft,
        nodes=(list[node], ...),
    )


class CreatePlanFunction(AgentFunction):
    """``create_plan`` — turn the model's plan draft into live orchestration state.

    The model calls this function (via forced ``stream_structured`` schema) with
    a DAG of nodes.  The function creates :class:`NodeState` entries, joins the
    plan agents as room members, and returns an ack.  Execution of the nodes
    starts asynchronously — the model is **not** kept waiting.
    """

    name = "create_plan"
    description = "Create an execution plan: a DAG of tasks assigned to registered agents."
    is_long_running = True

    async def args_model(self, ctx: FunctionContext) -> type[BaseModel]:
        agents = await ctx.registry.list()
        return _constrained_plan_schema([agent.name for agent in agents])

    async def execute(
        self, ctx: FunctionContext, args: BaseModel
    ) -> FunctionResult:
        draft = args if isinstance(args, PlanDraft) else PlanDraft.model_validate(
            args.model_dump()
        )
        state = ctx.state
        executor = ctx.executor
        runtime = ctx.runtime

        agents = await ctx.registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}

        for node_draft in draft.nodes:
            node = NodeState(
                id=node_draft.id,
                name=node_draft.name,
                agent_name=node_draft.agent_name,
                agent_url=agent_urls.get(node_draft.agent_name, ""),
                deps=list(node_draft.deps),
                input_text=str(node_draft.input.get("text", "")),
            )
            state.nodes[node.id] = node

        await executor._join_members(
            runtime,
            [n.agent_name for n in state.nodes.values() if n.agent_name],
            "plan",
        )

        return FunctionResult(
            success=True,
            data={
                "plan_id": state.plan_id,
                "plan_version": state.plan_version,
                "nodes": [
                    {
                        "id": n.id,
                        "name": n.name,
                        "agent_name": n.agent_name,
                        "deps": n.deps,
                    }
                    for n in state.nodes.values()
                ],
            },
        )
