from __future__ import annotations

from pydantic import BaseModel

from choirworks.a2a.patch import PlanPatch
from choirworks.tools.base import AgentFunction, FunctionContext, FunctionResult


class RevisePlanArgs(BaseModel):
    """Arguments for ``revise_plan`` — an incremental patch to the active plan."""

    patch: PlanPatch


class RevisePlanFunction(AgentFunction):
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

    name = "revise_plan"
    description = "Revise the active execution plan by adding and/or invalidating nodes."
    is_long_running = True

    async def args_model(self, ctx: FunctionContext) -> type[BaseModel]:
        return RevisePlanArgs

    async def execute(
        self, ctx: FunctionContext, args: BaseModel
    ) -> FunctionResult:
        plan_args = args if isinstance(args, RevisePlanArgs) else RevisePlanArgs.model_validate(
            args.model_dump()
        )
        executor = ctx.executor
        runtime = ctx.runtime
        result = await executor._apply_patch_locked(runtime, plan_args.patch)

        return FunctionResult(
            success=True,
            data={
                "plan_id": ctx.state.plan_id,
                "plan_version": ctx.state.plan_version,
                "reason": plan_args.patch.reason,
                "added": result.added,
                "added_nodes": [
                    {
                        "id": node_id,
                        "name": ctx.state.nodes[node_id].name,
                        "agent_name": ctx.state.nodes[node_id].agent_name,
                        "deps": ctx.state.nodes[node_id].deps,
                    }
                    for node_id in result.added
                ],
                "invalidated": result.invalidated,
                "skipped_in_flight": result.skipped_in_flight,
                "rejected": result.rejected,
            },
        )


revise_plan_func = RevisePlanFunction()
