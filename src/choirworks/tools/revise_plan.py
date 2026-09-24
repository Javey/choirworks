from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel

from choirworks.orchestration.planning.patch import PlanPatch, apply_patch_locked
from choirworks.tools.base import AgentFunction, FunctionResult
from choirworks.tools.create_plan import PlanNodeData

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)


class RevisePlanArgs(BaseModel):
    """Arguments for ``revise_plan`` — an incremental patch to the active plan."""

    patch: PlanPatch


class RevisePlanData(BaseModel):
    """Result payload of ``revise_plan``."""

    plan_id: str
    plan_version: int
    reason: str
    added: list[str]
    added_nodes: list[PlanNodeData]
    invalidated: list[str]
    skipped_in_flight: list[str]
    rejected: list[str]


async def revise_plan_args_model(ctx: OrchestrationContext) -> type[BaseModel]:
    return RevisePlanArgs


async def execute_revise_plan(ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
    """``revise_plan`` — apply an incremental patch to the running plan.

    The model calls this when an agent's outcome suggests the plan needs
    revising (adding nodes, invalidating stale ones).  The function applies
    the patch, emits confirm-cancel interventions for in-flight nodes that
    the patch wants to invalidate, and returns a summary of what changed.

    Side-effect events that remain as B-class ``kind`` strings:

    * ``intervention.requested`` (confirm_cancel) — state transition on the
      intervention, not a model intent.
    * ``node.invalidated`` — state transition on the node.
    """
    plan_args = (
        args
        if isinstance(args, RevisePlanArgs)
        else RevisePlanArgs.model_validate(args.model_dump())
    )
    logger.info(
        "revise_plan",
        add_count=len(plan_args.patch.add),
        invalidate_count=len(plan_args.patch.invalidate),
        reason=plan_args.patch.reason or "(none)",
    )
    result = await apply_patch_locked(ctx, plan_args.patch)

    logger.info(
        "revise_plan done",
        added=len(result.added),
        invalidated=len(result.invalidated),
        skipped=len(result.skipped_in_flight),
        rejected=len(result.rejected),
    )
    return FunctionResult(
        success=True,
        data=RevisePlanData(
            plan_id=ctx.state.plan_id,
            plan_version=ctx.state.plan_version,
            reason=plan_args.patch.reason,
            added=result.added,
            added_nodes=[
                PlanNodeData(
                    id=node_id,
                    name=ctx.state.nodes[node_id].name,
                    agent_name=ctx.state.nodes[node_id].agent_name,
                    deps=ctx.state.nodes[node_id].deps,
                    input_text=ctx.state.nodes[node_id].input_text,
                )
                for node_id in result.added
            ],
            invalidated=result.invalidated,
            skipped_in_flight=result.skipped_in_flight,
            rejected=result.rejected,
        ),
    )


revise_plan_func = AgentFunction(
    name="revise_plan",
    description="Revise the active execution plan by adding and/or invalidating nodes.",
    args_model=revise_plan_args_model,
    execute=execute_revise_plan,
    is_long_running=True,
)
