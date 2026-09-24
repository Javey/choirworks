from __future__ import annotations

import structlog
from a2a.server.agent_execution import RequestContext

from choirworks.a2a.tasks import RECOVER_KEY
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.execution.runner import start_runner
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    NodeStatus,
    normalize_interventions,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)


def is_recover_request(context: RequestContext) -> bool:
    """True when this request is the internal restart-recovery trigger."""
    return context.call_context.state.get(RECOVER_KEY) is True


async def recover_session(ctx: OrchestrationContext) -> None:
    """Re-attach an in-flight conversation after a process restart.

    Plan revisions (``repair.py``) create ``confirm_cancel`` interventions for
    active nodes, awaiting user confirmation.  After a crash:

    - a target node that had settled before the crash is no longer active, so
      its intervention is meaningless and is expired immediately;
    - a still-active target keeps its ``pending`` intervention, handled later
      by ``expire_cancel_requests`` once the runner re-attaches the remote task.

    Active nodes are reset to ``recover`` (remote task can be re-subscribed) or
    ``pending`` (re-dispatched), then the runner is started.
    """
    expired = normalize_interventions(ctx.state)
    if expired:
        logger.info(
            "recover expired interventions",
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            expired_interventions=len(expired),
        )
        await emit_state_delta(
            ctx,
            interventions={
                iv.id: {
                    "status": "expired",
                    "node_id": iv.node_id,
                    "kind": iv.kind,
                }
                for iv in expired
            },
        )
    for node in ctx.state.nodes.values():
        if node.status in ACTIVE_NODE_STATUSES:
            apply_transition(
                node,
                NodeStatus.RECOVER if node.a2a_task_id else NodeStatus.PENDING,
            )
    logger.info(
        "recover nodes",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        nodes={n.id: n.status for n in ctx.state.nodes.values()},
    )
    await ctx.sessions.persist(ctx)
    start_runner(ctx)
