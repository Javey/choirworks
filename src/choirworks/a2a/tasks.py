from __future__ import annotations

from collections.abc import AsyncIterator

from a2a.server.context import ServerCallContext
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import ListTasksRequest, Task

from choirworks.orchestration.rewind import extract_rewind

_PAGE_SIZE = 100


async def iter_all_tasks(
    task_store: TaskStore,
    *,
    context_id: str = "",
) -> AsyncIterator[Task]:
    """Stream every visible task, auto-paginating.

    Yields tasks in SDK default order (newest first), with rewind-hidden
    tasks filtered out.  Pass ``context_id`` to filter to a single
    conversation.
    """
    skip_stack: dict[str, list[str]] = {}
    page_token = ""
    while True:
        params = ListTasksRequest(page_size=_PAGE_SIZE, page_token=page_token)
        if context_id:
            params.context_id = context_id
        page = await task_store.list(params, ServerCallContext())
        for task in page.tasks:
            ctx = task.context_id
            before = extract_rewind(task)
            if before is not None:
                skip_stack.setdefault(ctx, []).append(before)
                continue
            stack = skip_stack.get(ctx)
            if stack and task.id == stack[-1]:
                stack.pop()
                continue
            if stack:
                continue
            yield task
        page_token = page.next_page_token
        if not page_token:
            break


async def list_all_tasks(
    task_store: TaskStore,
    *,
    context_id: str = "",
    reverse: bool = False,
) -> list[Task]:
    """Collect every visible task into a list.

    ``reverse=True`` returns oldest first (useful for timeline/replay/rewind).
    """
    tasks = [t async for t in iter_all_tasks(task_store, context_id=context_id)]
    if reverse:
        tasks.reverse()
    return tasks
