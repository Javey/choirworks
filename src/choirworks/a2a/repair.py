from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.patch import PatchResult, PlanPatch, apply_patch
from choirworks.core.context import build_repair_user
from choirworks.subagents.repair import RepairSubagent
from choirworks.tools.base import FunctionContext
from choirworks.tools.revise_plan import RevisePlanArgs

if TYPE_CHECKING:
    from choirworks.a2a.events import EventEmitter
    from choirworks.a2a.session import SessionManager
    from choirworks.tools import RevisePlanFunction

logger = logging.getLogger(__name__)


class RepairManager:
    """Plan repair and revision: apply patches, emit events."""

    def __init__(
        self,
        emitter: EventEmitter,
        session_mgr: SessionManager,
        repair_subagent: RepairSubagent,
        revise_plan_func: RevisePlanFunction,
    ):
        self._emitter = emitter
        self._session_mgr = session_mgr
        self._repair_subagent = repair_subagent
        self._revise_plan_func = revise_plan_func

    async def repair_plan(self, ctx: OrchestrationContext) -> bool:
        state = ctx.state
        failed_ids = [node.id for node in state.failed_nodes()]
        agents = await ctx.registry.list()
        if not agents:
            return False
        user = build_repair_user(state.nodes.values(), agents)
        try:
            decision = await self._repair_subagent.run(ctx, user)
        except Exception:
            logger.exception("plan repair failed for %s", ctx.context_id)
            return False
        if decision is None or decision.patch is None:
            return False
        patch = decision.patch
        patch.invalidate = list(dict.fromkeys([*patch.invalidate, *failed_ids]))
        result = await self.revise_plan(ctx, patch)
        return bool(result.added or result.invalidated)

    async def revise_plan(
        self, ctx: OrchestrationContext, patch: PlanPatch
    ) -> PatchResult:
        args = RevisePlanArgs(patch=patch)
        func_ctx = FunctionContext(executor=ctx.executor, runtime=ctx.runtime)  # type: ignore[arg-type]
        fn_result = await self._revise_plan_func.execute(func_ctx, args)
        await self._emitter.emit_function_call(ctx, self._revise_plan_func, args, fn_result)
        data = fn_result.data or {}
        result = PatchResult(
            added=list(data.get("added", [])),
            invalidated=list(data.get("invalidated", [])),
            skipped_in_flight=list(data.get("skipped_in_flight", [])),
            rejected=list(data.get("rejected", [])),
        )
        if result.added or result.invalidated:
            ctx.state.revision_count += 1
        return result

    async def apply_patch_locked(
        self, ctx: OrchestrationContext, patch: PlanPatch
    ) -> PatchResult:
        agents = await ctx.registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}
        state = ctx.state
        result = apply_patch(state, patch, agent_urls)
        for rejected in result.rejected:
            logger.warning("Patch rejected for %s: %s", ctx.context_id, rejected)
        new_interventions: dict[str, dict[str, Any]] = {}
        for node_id in result.skipped_in_flight:
            node = state.nodes.get(node_id)
            if node is None:
                continue
            question = (
                f"计划修订建议作废进行中的任务 @{node.agent_name}"
                f"（{patch.reason or '无说明'}）。是否打断？"
                "回复「确认」打断，回复其他内容则保留。"
            )
            intervention = state.add_cancel_request(node_id, question)
            if intervention is None:
                continue
            new_interventions[intervention.id] = {
                "status": "pending",
                "node_id": node_id,
                "kind": "confirm_cancel",
                "question": intervention.question,
            }
        if new_interventions:
            await self._emitter.emit_state_delta(ctx, interventions=new_interventions)
        added_agents = [
            draft.agent_name for draft in patch.add if draft.agent_name in agent_urls
        ]
        await self._join_members(ctx, added_agents, "plan_revision")
        if result.invalidated:
            await self._emitter.emit_state_delta(ctx, nodes={
                node_id: {"status": "invalidated"}
                for node_id in result.invalidated
            })
        await self._session_mgr.persist(ctx)
        return result

    async def _join_members(
        self,
        ctx: OrchestrationContext,
        names: list[str],
        reason: str,
    ) -> None:
        from choirworks.a2a.assist import join_members
        await join_members(ctx, names, reason)
