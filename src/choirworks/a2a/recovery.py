from __future__ import annotations

import structlog
from a2a.helpers import new_data_message
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskState,
)
from google.protobuf.json_format import ParseDict

from choirworks.a2a.tasks import iter_all_tasks
from choirworks.orchestration.state import load_state
from choirworks.store.contexts import ContextStore

logger = structlog.get_logger(__name__)

TERMINAL_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
}


async def recover_tasks(
    request_handler: DefaultRequestHandler,
    task_store: TaskStore,
    context_store: ContextStore | None = None,
) -> int:
    """Re-attach to non-terminal work after a process restart.

    Tasks are grouped by conversation (context_id); each conversation receives
    one synthetic resume message on its newest non-terminal visible task. The
    executor reloads the conversation state from the contexts store (or the
    persisted Task snapshot for pre-contexts data) and re-subscribes to remote
    work that was in flight.
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
            if context_store is None or await context_store.get(context_id) is None:
                continue
        seen_contexts.add(context_id)
        message = new_data_message(
            {"kind": "resume"},
            role=Role.ROLE_USER,
            task_id=task.id,
            context_id=task.context_id,
        )
        ParseDict({"choirworks.resume": {"kind": "resume"}}, message.metadata)
        request = SendMessageRequest(
            message=message,
            configuration=SendMessageConfiguration(return_immediately=True),
        )
        try:
            await request_handler.on_message_send(
                request, ServerCallContext()
            )
            recovered += 1
        except Exception:  # noqa: BLE001 - one bad task must not stop recovery
            logger.exception("Failed to recover task", task_id=task.id)
    if recovered:
        logger.info("Recovered in-flight task(s)", count=recovered)
    return recovered
