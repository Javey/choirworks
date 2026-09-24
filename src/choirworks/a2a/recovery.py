from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from a2a.helpers import new_data_message
from a2a.server.agent_execution import RequestContext
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskState,
)

from choirworks.a2a.tasks import iter_all_tasks
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.execution.runner import start_runner
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    NodeStatus,
    load_state,
    normalize_interventions,
)
from choirworks.orchestration.transitions import apply_transition
from choirworks.store.contexts import ContextStore

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)

RECOVER_KEY = "choirworks.recover"

TERMINAL_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
}


async def recover_tasks(
    request_handler: DefaultRequestHandler,
    task_store: TaskStore,
    context_store: ContextStore,
) -> int:
    """Re-attach to non-terminal work after a process restart.

    Tasks are grouped by conversation (context_id); each conversation receives
    one synthetic internal message on its newest non-terminal visible task.
    The SDK only starts :meth:`AgentExecutor.execute` from an ActiveTask
    request, and ``message/send`` is the only way to enqueue one, so recovery
    must go through the protocol entry.  The request is marked as internal via
    ``ServerCallContext.state`` (unforgeable by HTTP clients) and the executor
    reloads the conversation state from the contexts store, then re-subscribes
    to remote work that was in flight.
    """
    recovered = 0
    seen_contexts: set[str] = set()

    # Single DB query; iter_all_tasks filters rewind-hidden tasks in-stream.
    async for task in iter_all_tasks(task_store):
        if task.status.state in TERMINAL_STATES:
            continue
        context_id = task.context_id
        if context_id in seen_contexts:
            continue
        if load_state(task) is None:
            if await context_store.get(context_id) is None:
                continue
        seen_contexts.add(context_id)
        message = new_data_message(
            {},
            role=Role.ROLE_USER,
            task_id=task.id,
            context_id=task.context_id,
        )
        request = SendMessageRequest(
            message=message,
            configuration=SendMessageConfiguration(return_immediately=True),
        )
        try:
            await request_handler.on_message_send(
                request, ServerCallContext(state={RECOVER_KEY: True})
            )
            recovered += 1
        except Exception:  # noqa: BLE001 - one bad task must not stop recovery
            logger.exception("Failed to recover task", task_id=task.id, context_id=task.context_id)
    if recovered:
        logger.info("Recovered in-flight task(s)", count=recovered)
    return recovered


def is_recover_request(context: RequestContext) -> bool:
    """True when this request is the internal restart-recovery trigger."""
    return context.call_context.state.get(RECOVER_KEY) is True


async def recover_session(ctx: OrchestrationContext) -> None:
    """Re-attach an in-flight conversation after a process restart.

    Plan revisions (``planning/repair.py``) create ``confirm_cancel``
    interventions for active nodes, awaiting user confirmation.  After a crash:

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
