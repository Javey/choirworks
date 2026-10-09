from __future__ import annotations

from typing import TYPE_CHECKING, override

import structlog
from pydantic import BaseModel

from choirworks.core.tool import FunctionResult, FunctionTool
from choirworks.orchestration.planning.patch import PlanPatch, apply_patch_locked
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


class RevisePlanTool(FunctionTool):
    name = "revise_plan"
    description = "Revise the active execution plan by adding and/or invalidating nodes."

    @override
    async def _get_declaration(self, ctx: OrchestrationContext) -> type[BaseModel]:
        return RevisePlanArgs

    @override
    async def run_async(self, ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
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


revise_plan_func = RevisePlanTool()
