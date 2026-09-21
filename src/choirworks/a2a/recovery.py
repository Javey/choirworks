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

from choirworks.orchestration.rewind import hidden_task_ids, parse_markers
from choirworks.orchestration.state import load_state
from choirworks.store.contexts import ContextStore

logger = logging.getLogger(__name__)

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
    one synthetic resume message on its newest non-terminal task. The executor
    reloads the conversation state from the contexts store (or the persisted
    Task snapshot for pre-contexts data) and re-subscribes to remote work that
    was in flight.
    """
    recovered = 0
    seen_contexts: set[str] = set()
    hidden = (
        await _hidden_by_context(task_store, context_store)
        if context_store is not None
        else {}
    )
    page_token = ""
    while True:
        page = await task_store.list(
            ListTasksRequest(page_size=100, page_token=page_token),
            ServerCallContext(),
        )
        for task in page.tasks:
            if task.status.state in TERMINAL_STATES:
                continue
            context_id = task.context_id or task.id
            if context_id in seen_contexts:
                continue
            if task.id in hidden.get(context_id, set()):
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
                logger.exception("Failed to recover task %s", task.id)
        page_token = page.next_page_token
        if not page_token:
            break
    if recovered:
        logger.info("Recovered %d in-flight task(s)", recovered)
    return recovered


async def _hidden_by_context(
    task_store: TaskStore, context_store: ContextStore
) -> dict[str, set[str]]:
    records = {
        record.context_id: record for record in await context_store.list()
    }
    ids_by_context: dict[str, list[str]] = {}
    page_token = ""
    while True:
        page = await task_store.list(
            ListTasksRequest(page_size=100, page_token=page_token),
            ServerCallContext(),
        )
        for task in page.tasks:
            context_id = task.context_id or task.id
            ids_by_context.setdefault(context_id, []).append(task.id)
        page_token = page.next_page_token
        if not page_token:
            break
    return {
        context_id: hidden_task_ids(
            list(reversed(ids)),
            parse_markers(records[context_id].rewind_markers)
            if context_id in records
            else [],
        )
        for context_id, ids in ids_by_context.items()
    }
