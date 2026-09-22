from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import structlog
from pydantic import BaseModel, create_model

from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.orchestration.state import NodeState
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult

logger = structlog.get_logger(__name__)


class PlanNodeData(BaseModel):
    """Wire payload for one plan node in a tool result."""

    id: str
    name: str
    agent_name: str
    deps: list[str]
    input_text: str


class CreatePlanData(BaseModel):
    """Result payload of ``create_plan``."""

    plan_id: str
    plan_version: int
    nodes: list[PlanNodeData]


# Enum-pinned structured output mirrors google-adk's TransferToAgentTool
# (src/google/adk/tools/transfer_to_agent_tool.py, Apache-2.0), which
# constrains agent_name to a JSON-Schema enum so hallucinated names cannot
# pass validation.
def constrained_plan_schema(agent_names: Sequence[str]) -> type[PlanDraft]:
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


async def create_plan_args_model(ctx: FunctionContext) -> type[BaseModel]:
    agents = await ctx.registry.list()
    return constrained_plan_schema([agent.name for agent in agents])


async def execute_create_plan(
    ctx: FunctionContext, args: BaseModel
) -> FunctionResult:
    """``create_plan`` — turn the model's plan draft into live orchestration state.

    The model calls this function (via :meth:`LiteLLMClient.stream` with a
    forced tool choice) with a DAG of nodes.  The function creates
    :class:`NodeState` entries, joins the plan agents as room members, and
    returns an ack.  Execution of the nodes starts asynchronously — the model
    is **not** kept waiting.
    """
    draft = args if isinstance(args, PlanDraft) else PlanDraft.model_validate(
        args.model_dump()
    )
    state = ctx.state

    logger.info(
        "create_plan",
        nodes=len(draft.nodes), agents=[n.agent_name for n in draft.nodes],
    )

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

    await ctx.effects.join_members(
        [n.agent_name for n in state.nodes.values() if n.agent_name],
        "plan",
    )

    return FunctionResult(
        success=True,
        data=CreatePlanData(
            plan_id=state.plan_id,
            plan_version=state.plan_version,
            nodes=[
                PlanNodeData(
                    id=n.id,
                    name=n.name,
                    agent_name=n.agent_name,
                    deps=n.deps,
                    input_text=n.input_text,
                )
                for n in state.nodes.values()
            ],
        ),
    )


create_plan_func = AgentFunction(
    name="create_plan",
    description="Create an execution plan: a DAG of tasks assigned to registered agents.",
    args_model=create_plan_args_model,
    execute=execute_create_plan,
    is_long_running=True,
)
