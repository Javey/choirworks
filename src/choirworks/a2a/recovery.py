from __future__ import annotations

import logging

from a2a.helpers import new_data_message
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    ListTasksRequest,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskState,
)
from google.protobuf.json_format import ParseDict

from choirworks.a2a.state import load_state

logger = logging.getLogger(__name__)

TERMINAL_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_CANCELED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_REJECTED,
}


async def recover_tasks(
    request_handler: DefaultRequestHandler, task_store: TaskStore
) -> int:
    """Re-attach to non-terminal tasks after a process restart.

    Each recovered task receives a synthetic resume message. The executor
    reloads the plan from the persisted Task metadata and re-subscribes to
    remote work that was in flight.
    """
    recovered = 0
    page_token = ""
    while True:
        page = await task_store.list(
            ListTasksRequest(page_size=100, page_token=page_token),
            ServerCallContext(),
        )
        for task in page.tasks:
            if task.status.state in TERMINAL_STATES:
                continue
            if load_state(task) is None:
                continue
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
                logger.exception("Failed to recover task %s", task.id)
        page_token = page.next_page_token
        if not page_token:
            break
    if recovered:
        logger.info("Recovered %d in-flight task(s)", recovered)
    return recovered
