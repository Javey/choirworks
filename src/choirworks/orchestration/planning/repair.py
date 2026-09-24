from __future__ import annotations

import structlog

from choirworks.core.context import build_repair_user
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.functions import execute_function
from choirworks.orchestration.planning.patch import PatchResult, PlanPatch
from choirworks.orchestration.state import failed_nodes
from choirworks.subagents.base import run_subagent
from choirworks.subagents.repair import REPAIR_SUBAGENT
from choirworks.tools.revise_plan import RevisePlanArgs, RevisePlanData, revise_plan_func

logger = structlog.get_logger(__name__)


async def repair_plan(ctx: OrchestrationContext) -> bool:
    """Ask the repair subagent for a patch and apply it."""
    state = ctx.state
    failed_ids = [node.id for node in failed_nodes(state)]
    logger.info(
        "repair_plan",
        context=ctx.context_id,
        failed_nodes=failed_ids,
    )
    agents = await ctx.registry.list()
    if not agents:
        return False
    user = build_repair_user(state.nodes.values(), agents)
    try:
        decision = await run_subagent(REPAIR_SUBAGENT, ctx, user)
    except Exception:
        logger.exception("plan repair failed", context=ctx.context_id)
        return False
    if decision is None or decision.patch is None:
        logger.info("repair_plan no patch returned", context=ctx.context_id)
        return False
    patch = decision.patch
    patch.invalidate = list(dict.fromkeys([*patch.invalidate, *failed_ids]))
    result = await revise_plan(ctx, patch)
    logger.info(
        "repair_plan",
        context=ctx.context_id,
        added=len(result.added),
        invalidated=len(result.invalidated),
    )
    return bool(result.added or result.invalidated)


async def revise_plan(ctx: OrchestrationContext, patch: PlanPatch) -> PatchResult:
    args = RevisePlanArgs(patch=patch)
    fn_result = await execute_function(ctx, revise_plan_func, args)
    data = fn_result.data
    result = (
        PatchResult(
            added=data.added,
            invalidated=data.invalidated,
            skipped_in_flight=data.skipped_in_flight,
            rejected=data.rejected,
        )
        if isinstance(data, RevisePlanData)
        else PatchResult()
    )
    if result.added or result.invalidated:
        ctx.state.revision_count += 1
    return result
