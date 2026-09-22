from __future__ import annotations

import structlog

from choirworks.core.context import build_repair_user
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.flows import execute_function, join_members
from choirworks.orchestration.patch import PatchResult, PlanPatch, apply_patch
from choirworks.orchestration.state import (
    InterventionDelta,
    add_cancel_request,
    failed_nodes,
)
from choirworks.subagents import REPAIR_SUBAGENT, run_subagent
from choirworks.tools import revise_plan_func
from choirworks.tools.revise_plan import RevisePlanArgs, RevisePlanData

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
        decision = await run_subagent(ctx.llm, REPAIR_SUBAGENT, ctx, user)
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


async def apply_patch_locked(
    ctx: OrchestrationContext, patch: PlanPatch
) -> PatchResult:
    agents = await ctx.registry.list()
    agent_urls = {agent.name: agent.card_url for agent in agents}
    state = ctx.state
    result = apply_patch(state, patch, agent_urls)
    for rejected in result.rejected:
        logger.warning("Patch rejected", context=ctx.context_id, rejected=rejected)
    new_interventions: dict[str, InterventionDelta] = {}
    for node_id in result.skipped_in_flight:
        node = state.nodes.get(node_id)
        if node is None:
            continue
        question = (
            f"计划修订建议作废进行中的任务 @{node.agent_name}"
            f"（{patch.reason or '无说明'}）。是否打断？"
            "回复「确认」打断，回复其他内容则保留。"
        )
        intervention = add_cancel_request(state, node_id, question)
        if intervention is None:
            continue
        new_interventions[intervention.id] = {
            "status": "pending",
            "node_id": node_id,
            "kind": "confirm_cancel",
            "question": intervention.question,
        }
    if new_interventions:
        await emit_state_delta(ctx, interventions=new_interventions)
    added_agents = [
        draft.agent_name for draft in patch.add if draft.agent_name in agent_urls
    ]
    await join_members(ctx, added_agents, "plan_revision")
    if result.invalidated:
        await emit_state_delta(ctx, nodes={
            node_id: {"status": "invalidated"}
            for node_id in result.invalidated
        })
    await ctx.sessions.persist(ctx)
    return result
